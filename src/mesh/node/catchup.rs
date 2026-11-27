use std::sync::Arc;

use parking_lot::Mutex as FastMutex;
use tokio::sync::watch;
use tokio::task::JoinHandle;
use zenoh::query::{ConsolidationMode, QueryTarget};

use crate::mesh::protocol::{
    validate_node_id, CatchupRequest, CatchupResponse, MeshDeltaEnvelope,
};
use crate::storage::StorageEngine;
use crate::types::MembusError;

use super::envelope::{apply_envelope_blocking, build_envelope_from_log_entry};
use super::state::{wait_shutdown, CatchupStats, MeshNode, MAX_CATCHUP_ENTRIES};
use super::watermarks::{
    read_peer_watermark, record_peer_outbound_ack, reset_peer_inbound_epoch,
    update_peer_inbound_watermark,
};

pub(super) struct CatchupQueryableContext {
    pub(super) node_id: String,
    pub(super) catchup_key: String,
    pub(super) max_payload: usize,
    pub(super) storage: Arc<dyn StorageEngine>,
    pub(super) watermark_lock: Arc<FastMutex<()>>,
    pub(super) shutdown_rx: watch::Receiver<bool>,
}

pub(super) fn spawn_catchup_queryable_task(
    catchup_q: zenoh::query::Queryable<zenoh::handlers::FifoChannelHandler<zenoh::query::Query>>,
    mut ctx: CatchupQueryableContext,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = wait_shutdown(&mut ctx.shutdown_rx) => break,
                query = catchup_q.recv_async() => {
                    let Ok(query) = query else { break; };
                    let req_bytes = query.payload().map(|p| p.to_bytes().into_owned());
                    let st = Arc::clone(&ctx.storage);
                    let nid = ctx.node_id.clone();
                    let wml = Arc::clone(&ctx.watermark_lock);
                    let max_payload = ctx.max_payload;
                    let resp = tokio::task::spawn_blocking(move || {
                        handle_catchup_payload(st.as_ref(), &nid, req_bytes, max_payload, &wml)
                    })
                    .await
                    .unwrap_or_else(|e| {
                        CatchupResponse::Error(format!("catchup task panicked: {e}"))
                    });

                    if let Ok(payload) = serde_json::to_vec(&resp) {
                        let _ = query.reply(&ctx.catchup_key, payload).await;
                    }
                }
            }
        }
    })
}

fn handle_catchup_payload(
    storage: &dyn StorageEngine,
    node_id: &str,
    req_bytes: Option<Vec<u8>>,
    max_payload: usize,
    watermark_lock: &Arc<FastMutex<()>>,
) -> CatchupResponse {
    let Some(raw) = req_bytes else {
        return CatchupResponse::Error("missing catchup request payload".to_owned());
    };
    if raw.len() > max_payload {
        return CatchupResponse::Error("catchup request exceeds max payload size".to_owned());
    }
    let req: CatchupRequest = match serde_json::from_slice(&raw) {
        Ok(r) => r,
        Err(err) => return CatchupResponse::Error(format!("invalid catchup request: {err}")),
    };
    build_catchup_response(storage, node_id, &req, max_payload, watermark_lock)
}

fn build_catchup_response(
    storage: &dyn StorageEngine,
    node_id: &str,
    req: &CatchupRequest,
    default_max_bytes: usize,
    watermark_lock: &Arc<FastMutex<()>>,
) -> CatchupResponse {
    let current_epoch = storage.epoch();
    if let Some(expected) = req.expected_epoch {
        if expected != current_epoch {
            return CatchupResponse::EpochMismatch {
                origin_node_id: node_id.to_owned(),
                current_epoch,
            };
        }
    }

    // Record peer's explicit outbound acknowledgment on OUR local `CF_LOG` (M4).
    if let Some(req_peer) = &req.requester_node_id {
        if validate_node_id(req_peer).is_ok() && req_peer != node_id {
            let _ = record_peer_outbound_ack(
                storage,
                req_peer,
                current_epoch,
                req.since_origin_seq,
                watermark_lock,
            );
        }
    }

    let limit = req.max_entries.clamp(1, MAX_CATCHUP_ENTRIES);
    let max_bytes = req
        .max_bytes
        .unwrap_or(default_max_bytes)
        .clamp(1024, default_max_bytes);
    // Reserve >= 512 bytes for the outer `CatchupResponse::Batch` JSON wrapper and count
    // `+ 1` byte comma separator per envelope so replies never exceed `max_mesh_payload_bytes`.
    let budget_bytes = max_bytes.saturating_sub(512);

    match storage.scan_deltas_after(req.since_origin_seq, limit) {
        Ok(page) => {
            let mut envelopes = Vec::new();
            let mut accum_bytes = 0usize;
            let mut scanned_through_seq = req.since_origin_seq;
            let mut stopped_early_for_bytes = false;

            for entry in &page.entries {
                match build_envelope_from_log_entry(node_id, entry) {
                    Ok(Some(env)) => {
                        let env_bytes = serde_json::to_vec(&env)
                            .map(|v| v.len())
                            .unwrap_or(usize::MAX);
                        let item_cost = env_bytes.saturating_add(1);
                        if !envelopes.is_empty()
                            && accum_bytes.saturating_add(item_cost) > budget_bytes
                        {
                            stopped_early_for_bytes = true;
                            break;
                        }
                        if item_cost > budget_bytes {
                            return CatchupResponse::Error(format!(
                                "single catchup envelope ({env_bytes} bytes) exceeds max_bytes ({max_bytes})"
                            ));
                        }
                        accum_bytes = accum_bytes.saturating_add(item_cost);
                        envelopes.push(env);
                        scanned_through_seq = entry.seq;
                    }
                    Ok(None) => {
                        scanned_through_seq = entry.seq;
                    }
                    Err(err) => return CatchupResponse::Error(err.to_string()),
                }
            }

            if !stopped_early_for_bytes && page.scanned_through_seq > scanned_through_seq {
                scanned_through_seq = page.scanned_through_seq;
            }
            let has_more = scanned_through_seq < storage.latest_seq();
            CatchupResponse::Batch {
                origin_node_id: node_id.to_owned(),
                origin_epoch: current_epoch,
                envelopes,
                scanned_through_seq,
                has_more,
            }
        }
        Err(MembusError::Gap {
            earliest_available_seq,
            current_epoch,
        }) => CatchupResponse::Gap {
            earliest_available_seq,
            current_epoch,
        },
        Err(err) => CatchupResponse::Error(err.to_string()),
    }
}

impl MeshNode {
    /// Pull missing ordered deltas from a remote peer's gap catch-up `Queryable`,
    /// applying each delta deterministically under LWW and advancing persisted peer watermarks.
    ///
    /// - Handles `EpochMismatch` by resetting `last_origin_seq = 0`, updating `remote_epoch`,
    ///   and catching up cleanly from `0`.
    /// - Continues across empty batches while `has_more == true` while enforcing strict forward
    ///   progress (`scanned_through_seq > cursor`) and bounded `max_catchup_rounds`.
    /// - Fails closed with [`MembusError::Gap`] if the peer trimmed `CF_LOG` past the requested cursor.
    pub async fn catchup_from_peer(
        &self,
        peer_id: &str,
        since_origin_seq: Option<u64>,
    ) -> Result<CatchupStats, MembusError> {
        validate_node_id(peer_id)?;
        let existing_wm = read_peer_watermark(self.storage.as_ref(), peer_id)?;
        let mut cursor = match since_origin_seq {
            Some(seq) => seq,
            None => existing_wm.as_ref().map_or(0, |wm| wm.last_origin_seq),
        };
        let mut expected_epoch = if since_origin_seq.is_none() {
            existing_wm
                .as_ref()
                .map(|wm| wm.remote_epoch)
                .filter(|&ep| ep != 0)
        } else {
            None
        };
        let ke = format!("{}/catchup/{peer_id}", self.cluster_prefix);
        let mut stats = CatchupStats {
            through_origin_seq: cursor,
            ..Default::default()
        };
        let mut epoch_resets = 0usize;

        for _round in 0..self.max_catchup_rounds {
            let req = CatchupRequest {
                since_origin_seq: cursor,
                max_entries: self.max_batch_entries,
                max_bytes: Some(self.max_mesh_payload_bytes),
                expected_epoch,
                requester_node_id: Some(self.node_id.clone()),
            };
            let resp = self.send_catchup_request(&ke, peer_id, &req).await?;

            match resp {
                CatchupResponse::Gap {
                    earliest_available_seq,
                    current_epoch,
                } => {
                    return Err(MembusError::Gap {
                        earliest_available_seq,
                        current_epoch,
                    });
                }
                CatchupResponse::EpochMismatch {
                    origin_node_id,
                    current_epoch,
                } => {
                    self.handle_epoch_mismatch(peer_id, &origin_node_id, current_epoch, &mut epoch_resets)
                        .await?;
                    cursor = 0;
                    expected_epoch = Some(current_epoch);
                    stats.through_origin_seq = 0;
                }
                CatchupResponse::Error(msg) => {
                    return Err(MembusError::Storage(format!(
                        "peer '{peer_id}' catchup error: {msg}"
                    )));
                }
                CatchupResponse::Batch {
                    origin_node_id,
                    origin_epoch,
                    envelopes,
                    scanned_through_seq,
                    has_more,
                } => {
                    self.validate_catchup_batch(
                        peer_id,
                        cursor,
                        &origin_node_id,
                        &envelopes,
                        scanned_through_seq,
                        has_more,
                    )?;
                    let env_count = envelopes.len();
                    let (applied, skipped) = self
                        .apply_catchup_batch(
                            origin_node_id,
                            origin_epoch,
                            envelopes,
                            scanned_through_seq,
                        )
                        .await?;
                    stats.envelopes_received = stats.envelopes_received.saturating_add(env_count);
                    stats.deltas_applied = stats.deltas_applied.saturating_add(applied);
                    stats.deltas_skipped = stats.deltas_skipped.saturating_add(skipped);
                    expected_epoch = Some(origin_epoch);
                    if scanned_through_seq > cursor {
                        cursor = scanned_through_seq;
                        stats.through_origin_seq = cursor;
                    }
                    if !has_more {
                        return Ok(stats);
                    }
                }
            }
        }

        Err(MembusError::Protocol(format!(
            "catchup from peer '{peer_id}' exceeded max_catchup_rounds ({})",
            self.max_catchup_rounds
        )))
    }

    async fn send_catchup_request(
        &self,
        ke: &str,
        peer_id: &str,
        req: &CatchupRequest,
    ) -> Result<CatchupResponse, MembusError> {
        let req_bytes = serde_json::to_vec(req)
            .map_err(|err| MembusError::Protocol(format!("encode catchup request failed: {err}")))?;
        let replies = self
            .session
            .get(ke)
            .payload(req_bytes)
            .target(QueryTarget::BestMatching)
            .consolidation(ConsolidationMode::None)
            .timeout(self.query_timeout)
            .await
            .map_err(|err| MembusError::Io(format!("zenoh catchup query failed: {err}")))?;

        while let Ok(reply) = replies.recv_async().await {
            if let Ok(sample) = reply.result() {
                let raw = sample.payload().to_bytes();
                if raw.len() > self.max_mesh_payload_bytes {
                    return Err(MembusError::FrameTooLarge {
                        len: raw.len(),
                        max_len: self.max_mesh_payload_bytes,
                    });
                }
                let parsed: CatchupResponse = serde_json::from_slice(&raw).map_err(|err| {
                    MembusError::Protocol(format!("decode catchup response failed: {err}"))
                })?;
                return Ok(parsed);
            }
        }
        Err(MembusError::NotFound(format!(
            "no catchup reply received from peer '{peer_id}'"
        )))
    }

    async fn handle_epoch_mismatch(
        &self,
        peer_id: &str,
        origin_node_id: &str,
        current_epoch: u64,
        epoch_resets: &mut usize,
    ) -> Result<(), MembusError> {
        if origin_node_id != peer_id {
            return Err(MembusError::Protocol(format!(
                "catchup epoch mismatch sender '{origin_node_id}' != expected '{peer_id}'"
            )));
        }
        if *epoch_resets >= 1 {
            return Err(MembusError::Protocol(format!(
                "peer '{peer_id}' repeatedly changed epoch during catchup"
            )));
        }
        *epoch_resets += 1;
        let st = Arc::clone(&self.storage);
        let wml = Arc::clone(&self.watermark_lock);
        let pid = peer_id.to_owned();
        tokio::task::spawn_blocking(move || {
            reset_peer_inbound_epoch(st.as_ref(), &pid, current_epoch, &wml)
        })
        .await
        .map_err(|err| MembusError::Storage(format!("reset watermark panicked: {err}")))?
    }

    fn validate_catchup_batch(
        &self,
        peer_id: &str,
        cursor: u64,
        origin_node_id: &str,
        envelopes: &[MeshDeltaEnvelope],
        scanned_through_seq: u64,
        has_more: bool,
    ) -> Result<(), MembusError> {
        if origin_node_id != peer_id {
            return Err(MembusError::Protocol(format!(
                "catchup batch sender '{origin_node_id}' != expected '{peer_id}'"
            )));
        }
        if has_more && scanned_through_seq <= cursor {
            return Err(MembusError::Protocol(format!(
                "peer '{peer_id}' catchup made no forward progress (cursor={cursor}, scanned_through={scanned_through_seq})"
            )));
        }
        for env in envelopes {
            if env.origin_node_id != peer_id {
                return Err(MembusError::Protocol(format!(
                    "catchup envelope sender '{}' != expected '{peer_id}'",
                    env.origin_node_id
                )));
            }
            if env.deltas.len() > self.max_deltas_per_envelope
                || env.standalone_search_ops.len() > self.max_deltas_per_envelope
            {
                return Err(MembusError::Protocol(
                    "catchup envelope exceeds max_deltas_per_envelope".to_owned(),
                ));
            }
        }
        Ok(())
    }

    async fn apply_catchup_batch(
        &self,
        origin_node_id: String,
        origin_epoch: u64,
        envelopes: Vec<MeshDeltaEnvelope>,
        scanned_through_seq: u64,
    ) -> Result<(usize, usize), MembusError> {
        let st = Arc::clone(&self.storage);
        let sr = self.search.clone();
        let nid = self.node_id.clone();
        let wml = Arc::clone(&self.watermark_lock);
        tokio::task::spawn_blocking(move || {
            let mut applied = 0usize;
            let mut skipped = 0usize;
            for env in &envelopes {
                let (a, s) = apply_envelope_blocking(
                    st.as_ref(),
                    sr.as_deref(),
                    &nid,
                    env,
                    &wml,
                    false, // ordered catchup batch
                )?;
                applied = applied.saturating_add(a);
                skipped = skipped.saturating_add(s);
            }
            if scanned_through_seq > 0 {
                update_peer_inbound_watermark(
                    st.as_ref(),
                    &origin_node_id,
                    origin_epoch,
                    scanned_through_seq,
                    false,
                    &wml,
                )?;
            }
            Ok::<_, MembusError>((applied, skipped))
        })
        .await
        .map_err(|err| MembusError::Storage(format!("catchup apply panicked: {err}")))?
    }
}
