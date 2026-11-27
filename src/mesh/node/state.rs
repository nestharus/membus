use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex as FastMutex;
use tokio::sync::{watch, Mutex as AsyncMutex};
use tokio::task::JoinHandle;

use crate::mesh::protocol::{
    validate_cluster_prefix, validate_node_id, DEFAULT_MAX_CATCHUP_ROUNDS,
    DEFAULT_MAX_DELTAS_PER_ENVELOPE, DEFAULT_MAX_MESH_PAYLOAD_BYTES,
};
use crate::search::SearchEngine;
use crate::storage::StorageEngine;
use crate::types::{sanitize_search_log_op, MembusError, SearchLogOp};

pub(super) const MAX_CATCHUP_ENTRIES: usize = 512;

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
    /// Break-glass override permitting non-loopback Zenoh listen endpoints without transport TLS+auth.
    pub allow_insecure_non_loopback: bool,
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
            allow_insecure_non_loopback: false,
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
    pub(super) node_id: String,
    pub(super) cluster_prefix: String,
    pub(super) query_timeout: Duration,
    pub(super) max_batch_entries: usize,
    pub(super) max_mesh_payload_bytes: usize,
    pub(super) max_deltas_per_envelope: usize,
    pub(super) max_catchup_rounds: usize,
    pub(super) storage: Arc<dyn StorageEngine>,
    pub(super) search: Option<Arc<SearchEngine>>,
    pub(super) session: zenoh::Session,
    pub(super) published_seq: Arc<AtomicU64>,
    pub(super) publish_lock: Arc<AsyncMutex<()>>,
    pub(super) watermark_lock: Arc<FastMutex<()>>,
    pub(super) shutdown_tx: watch::Sender<bool>,
    pub(super) shutdown_rx: watch::Receiver<bool>,
    pub(super) tasks: Vec<JoinHandle<()>>,
}

impl MeshNode {
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    pub fn published_seq(&self) -> u64 {
        self.published_seq.load(Ordering::Acquire)
    }
}

pub(super) fn validate_mesh_config(config: &MeshConfig) -> Result<(), MembusError> {
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
    validate_mesh_listen_endpoints(
        &config.listen_endpoints,
        config.allow_insecure_non_loopback,
    )?;
    Ok(())
}

pub(super) fn build_zenoh_config(cfg: &MeshConfig) -> Result<zenoh::Config, MembusError> {
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

pub(super) async fn wait_shutdown(rx: &mut watch::Receiver<bool>) {
    if *rx.borrow() {
        return;
    }
    let _ = rx.changed().await;
}

/// Validate that Zenoh mesh listen endpoints are loopback-only unless the explicit
/// break-glass override `allow_insecure_non_loopback` is set.
pub fn validate_mesh_listen_endpoints(
    endpoints: &[String],
    allow_insecure_non_loopback: bool,
) -> Result<(), MembusError> {
    for ep in endpoints {
        if is_loopback_zenoh_endpoint(ep) {
            continue;
        }
        if allow_insecure_non_loopback {
            eprintln!(
                "WARNING: binding Zenoh mesh listener on non-loopback endpoint '{ep}' with allow_insecure_non_loopback override"
            );
            continue;
        }
        return Err(MembusError::InvalidArgument(format!(
            "refusing to bind Zenoh mesh listener on non-loopback endpoint '{ep}' without transport auth+TLS; pass --allow-insecure-non-loopback or set allow_insecure_non_loopback = true to override"
        )));
    }
    Ok(())
}

fn is_loopback_zenoh_endpoint(endpoint: &str) -> bool {
    let Some((proto, rest)) = endpoint.split_once('/') else {
        return false;
    };
    if proto.is_empty()
        || rest.is_empty()
        || rest.contains('#')
        || rest.contains('?')
        || rest.chars().any(char::is_whitespace)
    {
        return false;
    }
    if let Ok(sock) = rest.parse::<std::net::SocketAddr>() {
        return sock.ip().is_loopback();
    }
    if let Some((host, port_str)) = rest.rsplit_once(':') {
        if host.eq_ignore_ascii_case("localhost") && port_str.parse::<u16>().is_ok() {
            return true;
        }
    }
    false
}

/// Ensure a search op is sanitized before transmission (exposed for tests).
pub fn sanitize_for_mesh(op: &SearchLogOp) -> Result<SearchLogOp, MembusError> {
    sanitize_search_log_op(op)
}
