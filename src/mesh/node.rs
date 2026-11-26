use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex as FastMutex;
use sha2::{Digest, Sha256};
use tokio::sync::{watch, Mutex as AsyncMutex};
use tokio::task::JoinHandle;
use zenoh::query::{ConsolidationMode, QueryTarget};

use crate::mesh::protocol::{
    peer_watermark_meta_key, validate_cluster_prefix, validate_node_id, CatchupRequest,
    CatchupResponse, DigestReply, DigestRequest, DigestResponse, MeshDeltaEnvelope,
    MeshPeerWatermark, WireReplicatedDelta, DEFAULT_MAX_CATCHUP_ROUNDS,
    DEFAULT_MAX_DELTAS_PER_ENVELOPE, DEFAULT_MAX_MESH_PAYLOAD_BYTES, META_KEY_MESH_PUB_SEQ,
};
use crate::search::SearchEngine;
use crate::storage::{MetaCasGuard, StorageEngine};
use crate::tiering::TieringManager;
use crate::types::{
    now_unix_ms, sanitize_search_log_op, validate_namespace, Ack, LogDeltaEntry, MembusError,
    MutationOp, ReplicatedDelta, ReplicationStamp, ScanOptions, SearchLogOp, MAX_KEY_BYTES,
};

const MAX_CATCHUP_ENTRIES: usize = 512;

/// Configuration for an embedded Zenoh multi-node replication peer.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MeshConfig {
    pub node_id: String,
    pub cluster_prefix: String,
    pub listen_endpoints: Vec<String>,
    pub connect_endpoints: Vec<String>,
    pub enable_multicast_scouting: bool,
    pub publish_poll_interval_ms: u64,
    pub max_batch_entries: usize,
    pub query_timeout_ms: u64,
    pub max_mesh_payload_bytes: usize,
    pub max_deltas_per_envelope: usize,
    pub max_catchup_rounds: usize,
}

impl Default for MeshConfig {
    fn default() -> Self {
        Self {
            node_id: "node-1".to_owned(),
            cluster_prefix: "membus/v2".to_owned(),
            listen_endpoints: Vec::new(),
            connect_endpoints: Vec::new(),
            enable_multicast_scouting: false,
            publish_poll_interval_ms: 25,
            max_batch_entries: 128,
            query_timeout_ms: 5_000,
            max_mesh_payload_bytes: DEFAULT_MAX_MESH_PAYLOAD_BYTES,
            max_deltas_per_envelope: DEFAULT_MAX_DELTAS_PER_ENVELOPE,
            max_catchup_rounds: DEFAULT_MAX_CATCHUP_ROUNDS,
        }
    }
}

/// Counters returned by [`MeshNode::catchup_from_peer`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CatchupStats {
    pub envelopes_received: usize,
    pub deltas_applied: usize,
    pub deltas_skipped: usize,
    pub through_origin_seq: u64,
}

/// Summary returned by [`MeshNode::reconcile_with_peer`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReconcileReport {
    pub local_digest_before: [u8; 32],
    pub remote_digest: [u8; 32],
    pub local_digest_after: [u8; 32],
    pub was_divergent: bool,
    pub converged: bool,
    pub catchup: CatchupStats,
}

/// Embedded Zenoh multi-node mesh runtime providing:
/// - Off-path live delta `Pub/Sub` (draining settled `CF_LOG` entries without blocking writes)
/// - Bounded ordered gap catch-up `Queryable` (failing closed with `Gap` / `EpochMismatch`)
/// - Deterministic XOR support-digest anti-entropy `Queryable`
/// - Idempotent LWW convergence, 3+ node transitive relay, contiguous inbound watermarks,
///   and payload size bounds enforced before deserialization.
pub struct MeshNode {
    node_id: String,
    cluster_prefix: String,
    query_timeout: Duration,
    max_batch_entries: usize,
    max_mesh_payload_bytes: usize,
    max_deltas_per_envelope: usize,
    max_catchup_rounds: usize,
    storage: Arc<dyn StorageEngine>,
    search: Option<Arc<SearchEngine>>,
    session: zenoh::Session,
    published_seq: Arc<AtomicU64>,
    publish_lock: Arc<AsyncMutex<()>>,
    watermark_lock: Arc<FastMutex<()>>,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    tasks: Vec<JoinHandle<()>>,
}

impl MeshNode {
    /// Open an embedded Zenoh session and launch the off-path publisher, subscriber,
    /// gap catch-up queryable, and XOR support-digest queryable tasks.
    pub async fn start(
        config: MeshConfig,
        storage: Arc<dyn StorageEngine>,
        search: Option<Arc<SearchEngine>>,
    ) -> Result<Self, MembusError> {
        validate_node_id(&config.node_id)?;
        validate_cluster_prefix(&config.cluster_prefix)?;
        if config.max_batch_entries == 0 || config.max_batch_entries > MAX_CATCHUP_ENTRIES {
            return Err(MembusError::InvalidArgument(format!(
                "mesh max_batch_entries must be in 1..={MAX_CATCHUP_ENTRIES}"
            )));
        }
        if config.max_mesh_payload_bytes < 1024 {
            return Err(MembusError::InvalidArgument(
                "mesh max_mesh_payload_bytes must be >= 1024".to_owned(),
            ));
        }
        if config.max_deltas_per_envelope == 0 || config.max_catchup_rounds == 0 {
            return Err(MembusError::InvalidArgument(
                "mesh max_deltas_per_envelope and max_catchup_rounds must be > 0".to_owned(),
            ));
        }

        let zenoh_cfg = build_zenoh_config(&config)?;
        let session = zenoh::open(zenoh_cfg)
            .await
            .map_err(|err| MembusError::Storage(format!("zenoh open failed: {err}")))?;

        let initial_pub_seq = read_or_init_published_seq(storage.as_ref())?;
        let published_seq = Arc::new(AtomicU64::new(initial_pub_seq));
        let publish_lock = Arc::new(AsyncMutex::new(()));
        let watermark_lock = Arc::new(FastMutex::new(()));
        let (shutdown_tx, shutdown_rx) = watch::channel(false);

        let delta_sub_key = format!("{}/delta/*", config.cluster_prefix);
        let delta_prefix = format!("{}/delta/", config.cluster_prefix);
        let catchup_key = format!("{}/catchup/{}", config.cluster_prefix, config.node_id);
        let digest_key = format!("{}/digest/{}", config.cluster_prefix, config.node_id);

        let subscriber = session
            .declare_subscriber(&delta_sub_key)
            .await
            .map_err(|err| MembusError::Storage(format!("zenoh declare_subscriber failed: {err}")))?;
        let catchup_q = session
            .declare_queryable(&catchup_key)
            .await
            .map_err(|err| MembusError::Storage(format!("zenoh catchup queryable failed: {err}")))?;
        let digest_q = session
            .declare_queryable(&digest_key)
            .await
            .map_err(|err| MembusError::Storage(format!("zenoh digest queryable failed: {err}")))?;

        let mut tasks = Vec::with_capacity(4);
        let max_payload = config.max_mesh_payload_bytes;
        let max_deltas = config.max_deltas_per_envelope;

        // 1. Live delta subscriber task (bounds-checked before deserialization; strictly contiguous watermark).
        {
            let node_id = config.node_id.clone();
            let storage_ref = Arc::clone(&storage);
            let search_ref = search.clone();
            let wm_lock = Arc::clone(&watermark_lock);
            let mut rx = shutdown_rx.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = wait_shutdown(&mut rx) => break,
                        sample = subscriber.recv_async() => {
                            let Ok(sample) = sample else { break; };
                            let ke_str = sample.key_expr().as_str().to_owned();
                            let Some(key_sender) = ke_str.strip_prefix(&delta_prefix) else {
                                continue;
                            };
                            let raw = sample.payload().to_bytes();
                            if raw.len() > max_payload {
                                continue;
                            }
                            let Ok(envelope) = serde_json::from_slice::<MeshDeltaEnvelope>(&raw) else {
                                continue;
                            };
                            if envelope.origin_node_id != key_sender
                                || envelope.origin_node_id == node_id
                                || envelope.deltas.len() > max_deltas
                                || envelope.standalone_search_ops.len() > max_deltas
                            {
                                continue;
                            }
                            let st = Arc::clone(&storage_ref);
                            let sr = search_ref.clone();
                            let nid = node_id.clone();
                            let wml = Arc::clone(&wm_lock);
                            let _ = tokio::task::spawn_blocking(move || {
                                apply_envelope_blocking(
                                    st.as_ref(),
                                    sr.as_deref(),
                                    &nid,
                                    &envelope,
                                    &wml,
                                    true, // live_pubsub => strictly contiguous watermark only
                                )
                            })
                            .await;
                        }
                    }
                }
            }));
        }

        // 2. Gap catch-up Queryable task (bounded by entries & bytes; checks expected_epoch).
        {
            let node_id = config.node_id.clone();
            let storage_ref = Arc::clone(&storage);
            let reply_ke = catchup_key.clone();
            let wm_lock = Arc::clone(&watermark_lock);
            let mut rx = shutdown_rx.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = wait_shutdown(&mut rx) => break,
                        query = catchup_q.recv_async() => {
                            let Ok(query) = query else { break; };
                            let req_bytes = query.payload().map(|p| p.to_bytes().into_owned());
                            let st = Arc::clone(&storage_ref);
                            let nid = node_id.clone();
                            let wml = Arc::clone(&wm_lock);
                            let resp = tokio::task::spawn_blocking(move || {
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
                                build_catchup_response(st.as_ref(), &nid, &req, max_payload, &wml)
                            })
                            .await
                            .unwrap_or_else(|e| CatchupResponse::Error(format!("catchup task panicked: {e}")));

                            if let Ok(payload) = serde_json::to_vec(&resp) {
                                let _ = query.reply(&reply_ke, payload).await;
                            }
                        }
                    }
                }
            }));
        }

        // 3. XOR support-digest anti-entropy Queryable task (always replies cleanly on error).
        {
            let node_id = config.node_id.clone();
            let storage_ref = Arc::clone(&storage);
            let reply_ke = digest_key.clone();
            let mut rx = shutdown_rx.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        biased;
                        _ = wait_shutdown(&mut rx) => break,
                        query = digest_q.recv_async() => {
                            let Ok(query) = query else { break; };
                            let req_bytes = query.payload().map(|p| p.to_bytes().into_owned());
                            let st = Arc::clone(&storage_ref);
                            let nid = node_id.clone();
                            let reply = tokio::task::spawn_blocking(move || {
                                let Some(raw) = req_bytes else {
                                    return DigestReply::Error("missing digest request payload".to_owned());
                                };
                                if raw.len() > max_payload {
                                    return DigestReply::Error("digest request exceeds max payload size".to_owned());
                                }
                                let req: DigestRequest = match serde_json::from_slice(&raw) {
                                    Ok(r) => r,
                                    Err(err) => return DigestReply::Error(format!("invalid digest request: {err}")),
                                };
                                if req.key_prefix.len() > MAX_KEY_BYTES {
                                    return DigestReply::Error("digest key_prefix exceeds MAX_KEY_BYTES".to_owned());
                                }
                                match compute_namespace_xor_digest(st.as_ref(), &nid, &req.namespace, &req.key_prefix) {
                                    Ok(resp) => DigestReply::Ok(resp),
                                    Err(err) => DigestReply::Error(err.to_string()),
                                }
                            })
                            .await
                            .unwrap_or_else(|e| DigestReply::Error(format!("digest task panicked: {e}")));
                            if let Ok(payload) = serde_json::to_vec(&reply) {
                                let _ = query.reply(&reply_ke, payload).await;
                            }
                        }
                    }
                }
            }));
        }

        // 4. Off-path live delta publisher task (serialized via publish_lock; recovers from Gap).
        {
            let node_id = config.node_id.clone();
            let pub_ke = format!("{}/delta/{}", config.cluster_prefix, config.node_id);
            let storage_ref = Arc::clone(&storage);
            let session_ref = session.clone();
            let pub_seq_ref = Arc::clone(&published_seq);
            let pub_lock_ref = Arc::clone(&publish_lock);
            let max_batch = config.max_batch_entries;
            let poll_interval = Duration::from_millis(config.publish_poll_interval_ms.max(5));
            let mut rx = shutdown_rx.clone();
            tasks.push(tokio::spawn(async move {
                loop {
                    let _ = drain_and_publish_once(
                        &session_ref,
                        &pub_ke,
                        &node_id,
                        &storage_ref,
                        &pub_seq_ref,
                        &pub_lock_ref,
                        max_batch,
                        max_payload,
                        &rx,
                    )
                    .await;
                    tokio::select! {
                        biased;
                        _ = wait_shutdown(&mut rx) => break,
                        _ = tokio::time::sleep(poll_interval) => {}
                    }
                }
            }));
        }

        Ok(Self {
            node_id: config.node_id,
            cluster_prefix: config.cluster_prefix,
            query_timeout: Duration::from_millis(config.query_timeout_ms.max(100)),
            max_batch_entries: config.max_batch_entries,
            max_mesh_payload_bytes: max_payload,
            max_deltas_per_envelope: max_deltas,
            max_catchup_rounds: config.max_catchup_rounds,
            storage,
            search,
            session,
            published_seq,
            publish_lock,
            watermark_lock,
            shutdown_tx,
            shutdown_rx,
            tasks,
        })
    }

    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn published_seq(&self) -> u64 {
        self.published_seq.load(Ordering::Acquire)
    }

    /// Immediately drain and publish any settled `CF_LOG` entries (serialized with background publisher).
    pub async fn flush_outbound(&self) -> Result<u64, MembusError> {
        let pub_ke = format!("{}/delta/{}", self.cluster_prefix, self.node_id);
        drain_and_publish_once(
            &self.session,
            &pub_ke,
            &self.node_id,
            &self.storage,
            &self.published_seq,
            &self.publish_lock,
            self.max_batch_entries,
            self.max_mesh_payload_bytes,
            &self.shutdown_rx,
        )
        .await?;
        Ok(self.published_seq())
    }

    /// Compute the deterministic local XOR support-digest for `(namespace, key_prefix)`.
    pub fn local_digest(
        &self,
        namespace: &str,
        key_prefix: &[u8],
    ) -> Result<DigestResponse, MembusError> {
        compute_namespace_xor_digest(self.storage.as_ref(), &self.node_id, namespace, key_prefix)
    }

    /// Query a remote peer's XOR support-digest via its Zenoh anti-entropy `Queryable`.
    pub async fn query_peer_digest(
        &self,
        peer_id: &str,
        namespace: &str,
        key_prefix: &[u8],
    ) -> Result<DigestResponse, MembusError> {
        validate_node_id(peer_id)?;
        validate_namespace(namespace)?;
        if key_prefix.len() > MAX_KEY_BYTES {
            return Err(MembusError::InvalidArgument(
                "digest key_prefix exceeds MAX_KEY_BYTES".to_owned(),
            ));
        }
        let ke = format!("{}/digest/{peer_id}", self.cluster_prefix);
        let req_bytes = serde_json::to_vec(&DigestRequest {
            namespace: namespace.to_owned(),
            key_prefix: key_prefix.to_vec(),
        })
        .map_err(|err| MembusError::Protocol(format!("encode digest request failed: {err}")))?;

        let replies = self
            .session
            .get(&ke)
            .payload(req_bytes)
            .target(QueryTarget::BestMatching)
            .consolidation(ConsolidationMode::None)
            .timeout(self.query_timeout)
            .await
            .map_err(|err| MembusError::Io(format!("zenoh digest query failed: {err}")))?;

        while let Ok(reply) = replies.recv_async().await {
            if let Ok(sample) = reply.result() {
                let raw = sample.payload().to_bytes();
                if raw.len() > self.max_mesh_payload_bytes {
                    return Err(MembusError::FrameTooLarge {
                        len: raw.len(),
                        max_len: self.max_mesh_payload_bytes,
                    });
                }
                let parsed: DigestReply = serde_json::from_slice(&raw).map_err(|err| {
                    MembusError::Protocol(format!("decode digest reply failed: {err}"))
                })?;
                return match parsed {
                    DigestReply::Ok(resp) => Ok(resp),
                    DigestReply::Error(msg) => Err(MembusError::Storage(format!(
                        "peer '{peer_id}' digest error: {msg}"
                    ))),
                };
            }
        }
        Err(MembusError::NotFound(format!(
            "no digest reply received from peer '{peer_id}'"
        )))
    }

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
        let existing_wm = self.peer_watermark(peer_id)?;
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
            let req_bytes = serde_json::to_vec(&CatchupRequest {
                since_origin_seq: cursor,
                max_entries: self.max_batch_entries,
                max_bytes: Some(self.max_mesh_payload_bytes),
                expected_epoch,
                requester_node_id: Some(self.node_id.clone()),
            })
            .map_err(|err| MembusError::Protocol(format!("encode catchup request failed: {err}")))?;

            let replies = self
                .session
                .get(&ke)
                .payload(req_bytes)
                .target(QueryTarget::BestMatching)
                .consolidation(ConsolidationMode::None)
                .timeout(self.query_timeout)
                .await
                .map_err(|err| MembusError::Io(format!("zenoh catchup query failed: {err}")))?;

            let mut got_resp: Option<CatchupResponse> = None;
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
                    got_resp = Some(parsed);
                    break;
                }
            }

            let Some(resp) = got_resp else {
                return Err(MembusError::NotFound(format!(
                    "no catchup reply received from peer '{peer_id}'"
                )));
            };

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
                    if origin_node_id != peer_id {
                        return Err(MembusError::Protocol(format!(
                            "catchup epoch mismatch sender '{origin_node_id}' != expected '{peer_id}'"
                        )));
                    }
                    if epoch_resets >= 1 {
                        return Err(MembusError::Protocol(format!(
                            "peer '{peer_id}' repeatedly changed epoch during catchup"
                        )));
                    }
                    epoch_resets += 1;
                    cursor = 0;
                    expected_epoch = Some(current_epoch);
                    stats.through_origin_seq = 0;
                    let st = Arc::clone(&self.storage);
                    let wml = Arc::clone(&self.watermark_lock);
                    let pid = peer_id.to_owned();
                    tokio::task::spawn_blocking(move || {
                        reset_peer_inbound_epoch(st.as_ref(), &pid, current_epoch, &wml)
                    })
                    .await
                    .map_err(|err| MembusError::Storage(format!("reset watermark panicked: {err}")))??;
                    continue;
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
                    for env in &envelopes {
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

                    let env_count = envelopes.len();
                    let st = Arc::clone(&self.storage);
                    let sr = self.search.clone();
                    let nid = self.node_id.clone();
                    let wml = Arc::clone(&self.watermark_lock);
                    let peer_owned = origin_node_id.clone();
                    let (applied, skipped) = tokio::task::spawn_blocking(move || {
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
                                &peer_owned,
                                origin_epoch,
                                scanned_through_seq,
                                false,
                                &wml,
                            )?;
                        }
                        Ok::<_, MembusError>((applied, skipped))
                    })
                    .await
                    .map_err(|err| MembusError::Storage(format!("catchup apply panicked: {err}")))??;

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

    /// Compare local and remote XOR support-digests (AND `entry_count`); if divergent, run gap
    /// catch-up from `peer_id` and re-query remote digest to verify accurate convergence.
    pub async fn reconcile_with_peer(
        &self,
        peer_id: &str,
        namespace: &str,
        key_prefix: &[u8],
    ) -> Result<ReconcileReport, MembusError> {
        let local_before = self.local_digest(namespace, key_prefix)?;
        let remote_before = self.query_peer_digest(peer_id, namespace, key_prefix).await?;
        let was_divergent = local_before.xor_digest != remote_before.xor_digest
            || local_before.entry_count != remote_before.entry_count;

        let catchup = if was_divergent {
            self.catchup_from_peer(peer_id, None).await?
        } else {
            CatchupStats::default()
        };

        let local_after = self.local_digest(namespace, key_prefix)?;
        let remote_after = if was_divergent {
            self.query_peer_digest(peer_id, namespace, key_prefix)
                .await
                .unwrap_or(remote_before)
        } else {
            remote_before
        };
        let converged = local_after.xor_digest == remote_after.xor_digest
            && local_after.entry_count == remote_after.entry_count;

        Ok(ReconcileReport {
            local_digest_before: local_before.xor_digest,
            remote_digest: remote_after.xor_digest,
            local_digest_after: local_after.xor_digest,
            was_divergent,
            converged,
            catchup,
        })
    }

    /// Read the persisted peer watermark from `CF_META`.
    pub fn peer_watermark(&self, peer_id: &str) -> Result<Option<MeshPeerWatermark>, MembusError> {
        validate_node_id(peer_id)?;
        let key = peer_watermark_meta_key(peer_id);
        let Some(raw) = self.storage.meta_get(&key)? else {
            return Ok(None);
        };
        let wm: MeshPeerWatermark = serde_json::from_slice(&raw)
            .map_err(|err| MembusError::Corruption(format!("corrupt peer watermark: {err}")))?;
        Ok(Some(wm))
    }

    /// Gracefully stop background tasks and close the embedded Zenoh session.
    pub async fn shutdown(self) -> Result<(), MembusError> {
        let _ = self.shutdown_tx.send(true);
        for task in self.tasks {
            let _ = task.await;
        }
        self.session
            .close()
            .await
            .map_err(|err| MembusError::Io(format!("zenoh close failed: {err}")))?;
        Ok(())
    }
}

#[allow(clippy::too_many_arguments)]
async fn drain_and_publish_once(
    session: &zenoh::Session,
    pub_ke: &str,
    node_id: &str,
    storage: &Arc<dyn StorageEngine>,
    published_seq: &Arc<AtomicU64>,
    publish_lock: &Arc<AsyncMutex<()>>,
    max_batch: usize,
    max_payload_bytes: usize,
    shutdown_rx: &watch::Receiver<bool>,
) -> Result<(), MembusError> {
    let _guard = publish_lock.lock().await;
    loop {
        if *shutdown_rx.borrow() {
            break;
        }
        let after_seq = published_seq.load(Ordering::Acquire);
        if storage.latest_seq() <= after_seq {
            break;
        }
        let st = Arc::clone(storage);
        let nid = node_id.to_owned();
        let scan_res = tokio::task::spawn_blocking(move || {
            let page = st.scan_deltas_after(after_seq, max_batch)?;
            let mut envelopes = Vec::new();
            for entry in &page.entries {
                if let Some(env) = build_envelope_from_log_entry(&nid, entry)? {
                    envelopes.push(env);
                }
            }
            Ok::<_, MembusError>((envelopes, page.scanned_through_seq))
        })
        .await
        .map_err(|err| MembusError::Storage(format!("mesh publish scan panicked: {err}")))?;

        let (envelopes, scanned_through) = match scan_res {
            Ok(pair) => pair,
            Err(MembusError::Gap {
                earliest_available_seq,
                ..
            }) => {
                // Forced log trim jumped ahead of publisher cursor: advance to the new horizon
                // so the publisher never wedges permanently.
                let recovered_seq = earliest_available_seq.saturating_sub(1);
                published_seq.store(recovered_seq, Ordering::Release);
                let st = Arc::clone(storage);
                let _ = tokio::task::spawn_blocking(move || {
                    persist_published_seq(st.as_ref(), recovered_seq)
                })
                .await;
                continue;
            }
            Err(err) => return Err(err),
        };

        for env in &envelopes {
            if *shutdown_rx.borrow() {
                return Ok(());
            }
            let bytes = serde_json::to_vec(env)
                .map_err(|err| MembusError::Protocol(format!("encode mesh envelope failed: {err}")))?;
            if bytes.len() > max_payload_bytes {
                return Err(MembusError::FrameTooLarge {
                    len: bytes.len(),
                    max_len: max_payload_bytes,
                });
            }
            session
                .put(pub_ke, bytes)
                .await
                .map_err(|err| MembusError::Io(format!("zenoh publish failed: {err}")))?;
        }

        if scanned_through > after_seq {
            published_seq.store(scanned_through, Ordering::Release);
            let st = Arc::clone(storage);
            let _ = tokio::task::spawn_blocking(move || {
                persist_published_seq(st.as_ref(), scanned_through)
            })
            .await;
        } else {
            break;
        }
    }
    Ok(())
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

/// Convert one settled `CF_LOG` entry into an outbound [`MeshDeltaEnvelope`].
///
/// - Skips local tier-migration swaps (`is_tier_swap == true`) so ColdRef archival and
///   rehydration never enter the mesh.
/// - Relays replicated entries (`entry.replicated == true`) while preserving their original
///   `origin_stamp` so 3+ node topologies (`A <-> B <-> C`) converge cleanly without loops.
/// - Double-sanitizes every `SearchLogOp` so `SearchVisibility::Sealed` plaintext can NEVER
///   leave the node.
fn build_envelope_from_log_entry(
    sender_node_id: &str,
    entry: &LogDeltaEntry,
) -> Result<Option<MeshDeltaEnvelope>, MembusError> {
    let mut deltas = Vec::with_capacity(entry.mutations.len());

    for m in &entry.mutations {
        if m.is_tier_swap {
            continue;
        }
        let value_sha256: [u8; 32] = match &m.op {
            MutationOp::Put { value } => Sha256::digest(value).into(),
            MutationOp::Remove => [0u8; 32],
        };
        let stamp = if entry.replicated {
            let Some(orig) = &m.origin_stamp else {
                continue;
            };
            orig.clone()
        } else {
            ReplicationStamp {
                timestamp_ms: entry.ts_ms,
                origin_node_id: sender_node_id.to_owned(),
                origin_seq: entry.seq,
                value_sha256,
            }
        };
        let search_op = match &m.search_op {
            Some(op) => Some(sanitize_search_log_op(op)?),
            None => None,
        };
        deltas.push(WireReplicatedDelta::from(ReplicatedDelta {
            stamp,
            namespace: m.namespace.clone(),
            key: m.key.clone(),
            op: m.op.clone(),
            indexes: m.indexes.clone(),
            search_op,
        }));
    }

    let mut standalone_search_ops = Vec::with_capacity(entry.search_ops.len());
    for op in &entry.search_ops {
        standalone_search_ops.push(sanitize_search_log_op(op)?);
    }

    let standalone_origin = if !standalone_search_ops.is_empty() {
        if entry.replicated {
            entry.standalone_origin.clone()
        } else {
            Some(ReplicationStamp {
                timestamp_ms: entry.ts_ms,
                origin_node_id: sender_node_id.to_owned(),
                origin_seq: entry.seq,
                value_sha256: [0u8; 32],
            })
        }
    } else {
        None
    };

    if deltas.is_empty() && standalone_search_ops.is_empty() {
        return Ok(None);
    }

    Ok(Some(MeshDeltaEnvelope {
        origin_node_id: sender_node_id.to_owned(),
        origin_epoch: entry.epoch,
        origin_seq: entry.seq,
        relayed: entry.replicated,
        deltas,
        standalone_search_ops,
        standalone_origin,
    }))
}

fn apply_envelope_blocking(
    storage: &dyn StorageEngine,
    search: Option<&SearchEngine>,
    local_node_id: &str,
    envelope: &MeshDeltaEnvelope,
    watermark_lock: &Arc<FastMutex<()>>,
    require_contiguous_watermark: bool,
) -> Result<(usize, usize), MembusError> {
    if envelope.origin_node_id == local_node_id {
        return Ok((0, envelope.deltas.len()));
    }
    validate_node_id(&envelope.origin_node_id)?;

    let mut applied = 0usize;
    let mut skipped = 0usize;

    for wire_delta in &envelope.deltas {
        let mut clean_delta: ReplicatedDelta = wire_delta.clone().into();
        validate_node_id(&clean_delta.stamp.origin_node_id)?;

        // For non-relayed origin envelopes, enforce strict sender == origin_node_id consistency.
        if !envelope.relayed && clean_delta.stamp.origin_node_id != envelope.origin_node_id {
            return Err(MembusError::Protocol(format!(
                "non-relayed envelope from '{}' carries mismatched delta origin '{}'",
                envelope.origin_node_id, clean_delta.stamp.origin_node_id
            )));
        }
        // Never re-apply our own authored mutation bounced back via a transitive relay.
        if clean_delta.stamp.origin_node_id == local_node_id {
            skipped = skipped.saturating_add(1);
            continue;
        }
        if let Some(op) = &clean_delta.search_op {
            clean_delta.search_op = Some(sanitize_search_log_op(op)?);
        }
        let res = storage.apply_replicated_delta(local_node_id, &clean_delta, Ack::Fast)?;
        if res.applied {
            applied = applied.saturating_add(1);
            if let (Some(engine), Some(local_seq), Some(op)) =
                (search, res.local_seq, clean_delta.search_op)
            {
                engine.ingest_committed_op(local_seq, 0, storage.epoch(), op)?;
            }
        } else {
            skipped = skipped.saturating_add(1);
        }
    }

    if !envelope.standalone_search_ops.is_empty() {
        let origin_stamp = envelope
            .standalone_origin
            .clone()
            .unwrap_or_else(|| ReplicationStamp {
                timestamp_ms: now_unix_ms(),
                origin_node_id: envelope.origin_node_id.clone(),
                origin_seq: envelope.origin_seq,
                value_sha256: [0u8; 32],
            });
        validate_node_id(&origin_stamp.origin_node_id)?;
        if !envelope.relayed && origin_stamp.origin_node_id != envelope.origin_node_id {
            return Err(MembusError::Protocol(format!(
                "non-relayed envelope from '{}' carries mismatched standalone search origin '{}'",
                envelope.origin_node_id, origin_stamp.origin_node_id
            )));
        }
        if origin_stamp.origin_node_id == local_node_id {
            skipped = skipped.saturating_add(envelope.standalone_search_ops.len());
        } else {
            let mut sanitized_ops = Vec::with_capacity(envelope.standalone_search_ops.len());
            for op in &envelope.standalone_search_ops {
                sanitized_ops.push(sanitize_search_log_op(op)?);
            }
            let res =
                storage.append_replicated_search_ops(&origin_stamp, &sanitized_ops, Ack::Fast)?;
            if res.applied {
                applied = applied.saturating_add(sanitized_ops.len());
                if let (Some(engine), Some(local_seq)) = (search, res.local_seq) {
                    for (idx, op) in sanitized_ops.into_iter().enumerate() {
                        engine.ingest_committed_op(
                            local_seq,
                            u32::try_from(idx).unwrap_or(u32::MAX),
                            storage.epoch(),
                            op,
                        )?;
                    }
                }
            } else {
                skipped = skipped.saturating_add(sanitized_ops.len());
            }
        }
    }

    update_peer_inbound_watermark(
        storage,
        &envelope.origin_node_id,
        envelope.origin_epoch,
        envelope.origin_seq,
        require_contiguous_watermark,
        watermark_lock,
    )?;

    Ok((applied, skipped))
}

/// Compute a deterministic 32-byte XOR support-digest over `(key, value_sha256)` for all
/// keys in `(namespace, key_prefix)` under an O(1) MVCC snapshot.
pub fn compute_namespace_xor_digest(
    storage: &dyn StorageEngine,
    node_id: &str,
    namespace: &str,
    key_prefix: &[u8],
) -> Result<DigestResponse, MembusError> {
    validate_namespace(namespace)?;
    let snap = storage.open_snapshot()?;
    let mut after_key: Option<Vec<u8>> = None;
    let mut xor_digest = [0u8; 32];
    let mut entry_count = 0u64;

    loop {
        let page = storage.scan_prefix(
            &snap,
            namespace,
            key_prefix,
            &ScanOptions {
                after_key: after_key.clone(),
                limit: 512,
                max_bytes: 4 * 1024 * 1024,
            },
        )?;
        if page.records.is_empty() {
            break;
        }
        for rec in &page.records {
            let value_sha256: [u8; 32] = if TieringManager::is_cold_ref(rec) {
                TieringManager::decode_cold_ref(rec)?.cold_sha256
            } else {
                Sha256::digest(&rec.value).into()
            };
            let mut hasher = Sha256::new();
            hasher.update((rec.key.len() as u32).to_be_bytes());
            hasher.update(&rec.key);
            hasher.update(value_sha256);
            let item_hash: [u8; 32] = hasher.finalize().into();
            for i in 0..32 {
                xor_digest[i] ^= item_hash[i];
            }
            entry_count = entry_count.saturating_add(1);
        }
        if !page.truncated {
            break;
        }
        after_key = page.next_cursor;
        if after_key.is_none() {
            break;
        }
    }

    Ok(DigestResponse {
        node_id: node_id.to_owned(),
        epoch: storage.epoch(),
        latest_seq: snap.snapshot_seq(),
        entry_count,
        xor_digest,
    })
}

fn read_or_init_published_seq(storage: &dyn StorageEngine) -> Result<u64, MembusError> {
    let current_epoch = storage.epoch();
    if let Some(raw) = storage.meta_get(META_KEY_MESH_PUB_SEQ)? {
        if raw.len() != 16 {
            return Err(MembusError::Corruption(format!(
                "corrupt mesh:pub_seq watermark: expected 16 bytes, got {}",
                raw.len()
            )));
        }
        let saved_epoch = u64::from_be_bytes(raw[0..8].try_into().unwrap());
        let saved_seq = u64::from_be_bytes(raw[8..16].try_into().unwrap());
        if saved_epoch == current_epoch {
            return Ok(saved_seq);
        }
    }
    // Persist initial `mesh:pub_seq` on startup so safe log-trimming honors an active mesh node.
    persist_published_seq(storage, 0)?;
    Ok(0)
}

fn persist_published_seq(storage: &dyn StorageEngine, seq: u64) -> Result<(), MembusError> {
    let mut raw = [0u8; 16];
    raw[0..8].copy_from_slice(&storage.epoch().to_be_bytes());
    raw[8..16].copy_from_slice(&seq.to_be_bytes());
    storage.meta_put(META_KEY_MESH_PUB_SEQ, &raw, false)
}

fn update_peer_inbound_watermark(
    storage: &dyn StorageEngine,
    peer_id: &str,
    remote_epoch: u64,
    origin_seq: u64,
    require_contiguous: bool,
    watermark_lock: &Arc<FastMutex<()>>,
) -> Result<(), MembusError> {
    let _guard = watermark_lock.lock();
    let key = peer_watermark_meta_key(peer_id);

    for _ in 0..8 {
        let prev_raw = storage.meta_get(&key)?;
        let prev: Option<MeshPeerWatermark> = match &prev_raw {
            Some(bytes) => Some(serde_json::from_slice(bytes).map_err(|err| {
                MembusError::Corruption(format!("corrupt peer watermark '{peer_id}': {err}"))
            })?),
            None => None,
        };

        let (next_epoch, next_seq) = match &prev {
            Some(wm) if wm.remote_epoch == remote_epoch => {
                if require_contiguous {
                    if origin_seq == wm.last_origin_seq.saturating_add(1) {
                        (remote_epoch, origin_seq)
                    } else {
                        // Out-of-order or duplicate live sample: preserve contiguous floor!
                        return Ok(());
                    }
                } else {
                    (remote_epoch, wm.last_origin_seq.max(origin_seq))
                }
            }
            _ => {
                if require_contiguous {
                    if origin_seq == 1 {
                        (remote_epoch, 1)
                    } else {
                        // First observed live sample skipped seq 1..origin_seq-1: hold floor at 0.
                        (remote_epoch, 0)
                    }
                } else {
                    (remote_epoch, origin_seq)
                }
            }
        };

        let wm = MeshPeerWatermark {
            peer_id: peer_id.to_owned(),
            remote_epoch: next_epoch,
            last_origin_seq: next_seq,
            // Preserve existing outbound ack untouched (NEVER overwrite with local latest_seq!).
            local_epoch: prev.as_ref().and_then(|p| p.local_epoch),
            outbound_acked_seq: prev.as_ref().and_then(|p| p.outbound_acked_seq),
            updated_at_ms: now_unix_ms(),
        };
        let encoded = serde_json::to_vec(&wm)
            .map_err(|err| MembusError::Storage(format!("encode peer watermark failed: {err}")))?;
        match storage.meta_cas_batch(
            &[MetaCasGuard {
                key: key.clone(),
                expected: prev_raw,
            }],
            &[(key.clone(), encoded)],
            &[],
            false,
        ) {
            Ok(()) => return Ok(()),
            Err(MembusError::TxConflict(_)) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(MembusError::TxConflict(format!(
        "peer watermark update contention for '{peer_id}'"
    )))
}

fn reset_peer_inbound_epoch(
    storage: &dyn StorageEngine,
    peer_id: &str,
    new_remote_epoch: u64,
    watermark_lock: &Arc<FastMutex<()>>,
) -> Result<(), MembusError> {
    let _guard = watermark_lock.lock();
    let key = peer_watermark_meta_key(peer_id);
    for _ in 0..8 {
        let prev_raw = storage.meta_get(&key)?;
        let prev: Option<MeshPeerWatermark> = match &prev_raw {
            Some(bytes) => Some(serde_json::from_slice(bytes).map_err(|err| {
                MembusError::Corruption(format!("corrupt peer watermark '{peer_id}': {err}"))
            })?),
            None => None,
        };
        let wm = MeshPeerWatermark {
            peer_id: peer_id.to_owned(),
            remote_epoch: new_remote_epoch,
            last_origin_seq: 0,
            local_epoch: prev.as_ref().and_then(|p| p.local_epoch),
            outbound_acked_seq: prev.as_ref().and_then(|p| p.outbound_acked_seq),
            updated_at_ms: now_unix_ms(),
        };
        let encoded = serde_json::to_vec(&wm)
            .map_err(|err| MembusError::Storage(format!("encode peer watermark failed: {err}")))?;
        match storage.meta_cas_batch(
            &[MetaCasGuard {
                key: key.clone(),
                expected: prev_raw,
            }],
            &[(key.clone(), encoded)],
            &[],
            false,
        ) {
            Ok(()) => return Ok(()),
            Err(MembusError::TxConflict(_)) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(MembusError::TxConflict(format!(
        "peer watermark epoch reset contention for '{peer_id}'"
    )))
}

fn record_peer_outbound_ack(
    storage: &dyn StorageEngine,
    peer_id: &str,
    local_epoch: u64,
    acked_seq: u64,
    watermark_lock: &Arc<FastMutex<()>>,
) -> Result<(), MembusError> {
    let _guard = watermark_lock.lock();
    let clamped_ack = acked_seq.min(storage.latest_seq());
    let key = peer_watermark_meta_key(peer_id);
    for _ in 0..8 {
        let prev_raw = storage.meta_get(&key)?;
        let prev: Option<MeshPeerWatermark> = match &prev_raw {
            Some(bytes) => Some(serde_json::from_slice(bytes).map_err(|err| {
                MembusError::Corruption(format!("corrupt peer watermark '{peer_id}': {err}"))
            })?),
            None => None,
        };
        let next_outbound = match &prev {
            Some(wm) if wm.local_epoch == Some(local_epoch) => {
                wm.outbound_acked_seq.unwrap_or(0).max(clamped_ack)
            }
            _ => clamped_ack,
        };
        let wm = MeshPeerWatermark {
            peer_id: peer_id.to_owned(),
            remote_epoch: prev.as_ref().map_or(0, |p| p.remote_epoch),
            last_origin_seq: prev.as_ref().map_or(0, |p| p.last_origin_seq),
            local_epoch: Some(local_epoch),
            outbound_acked_seq: Some(next_outbound),
            updated_at_ms: now_unix_ms(),
        };
        let encoded = serde_json::to_vec(&wm)
            .map_err(|err| MembusError::Storage(format!("encode peer watermark failed: {err}")))?;
        match storage.meta_cas_batch(
            &[MetaCasGuard {
                key: key.clone(),
                expected: prev_raw,
            }],
            &[(key.clone(), encoded)],
            &[],
            false,
        ) {
            Ok(()) => return Ok(()),
            Err(MembusError::TxConflict(_)) => continue,
            Err(err) => return Err(err),
        }
    }
    Err(MembusError::TxConflict(format!(
        "peer outbound ack contention for '{peer_id}'"
    )))
}

fn build_zenoh_config(cfg: &MeshConfig) -> Result<zenoh::Config, MembusError> {
    let json = serde_json::json!({
        "mode": "peer",
        "scouting": {
            "delay": 0,
            "multicast": {
                "enabled": cfg.enable_multicast_scouting
            },
            "gossip": {
                "enabled": false
            }
        },
        "listen": {
            "endpoints": cfg.listen_endpoints,
            "timeout_ms": 0
        },
        "connect": {
            "endpoints": cfg.connect_endpoints,
            "timeout_ms": 0,
            "exit_on_failure": false,
            "retry": {
                "period_init_ms": 100,
                "period_max_ms": 500,
                "period_increase_factor": 1.5
            }
        }
    });
    zenoh::Config::from_json5(&json.to_string())
        .map_err(|err| MembusError::InvalidArgument(format!("invalid zenoh mesh config: {err}")))
}

async fn wait_shutdown(rx: &mut watch::Receiver<bool>) {
    if *rx.borrow() {
        return;
    }
    let _ = rx.changed().await;
}

/// Ensure a search op is sanitized before transmission (exposed for tests).
pub fn sanitize_for_mesh(op: &SearchLogOp) -> Result<SearchLogOp, MembusError> {
    sanitize_search_log_op(op)
}
