use std::sync::Arc;

use tokio::sync::watch;
use tokio::task::JoinHandle;
use zenoh::query::{ConsolidationMode, QueryTarget};

use crate::mesh::protocol::{validate_node_id, DigestReply, DigestRequest, DigestResponse};
use crate::storage::{compute_namespace_xor_digest, StorageEngine};
use crate::types::{validate_namespace, MembusError, MAX_KEY_BYTES};

use super::state::{wait_shutdown, CatchupStats, MeshNode, ReconcileReport};

pub(super) struct DigestQueryableContext {
    pub(super) node_id: String,
    pub(super) digest_key: String,
    pub(super) max_payload: usize,
    pub(super) storage: Arc<dyn StorageEngine>,
    pub(super) shutdown_rx: watch::Receiver<bool>,
}

pub(super) fn spawn_digest_queryable_task(
    digest_q: zenoh::query::Queryable<zenoh::handlers::FifoChannelHandler<zenoh::query::Query>>,
    mut ctx: DigestQueryableContext,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = wait_shutdown(&mut ctx.shutdown_rx) => break,
                query = digest_q.recv_async() => {
                    let Ok(query) = query else { break; };
                    let req_bytes = query.payload().map(|p| p.to_bytes().into_owned());
                    let st = Arc::clone(&ctx.storage);
                    let nid = ctx.node_id.clone();
                    let max_payload = ctx.max_payload;
                    let reply = tokio::task::spawn_blocking(move || {
                        handle_digest_payload(st.as_ref(), &nid, req_bytes, max_payload)
                    })
                    .await
                    .unwrap_or_else(|e| DigestReply::Error(format!("digest task panicked: {e}")));
                    if let Ok(payload) = serde_json::to_vec(&reply) {
                        let _ = query.reply(&ctx.digest_key, payload).await;
                    }
                }
            }
        }
    })
}

fn handle_digest_payload(
    storage: &dyn StorageEngine,
    node_id: &str,
    req_bytes: Option<Vec<u8>>,
    max_payload: usize,
) -> DigestReply {
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
    match compute_namespace_xor_digest(storage, node_id, &req.namespace, &req.key_prefix) {
        Ok(resp) => DigestReply::Ok(resp),
        Err(err) => DigestReply::Error(err.to_string()),
    }
}

impl MeshNode {
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
}
