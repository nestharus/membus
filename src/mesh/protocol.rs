use serde::{Deserialize, Serialize};

use crate::types::{
    base64_bytes, GraphEdgeEntry, IndexMutations, MembusError, MutationOp, OrderedIndexEntry,
    ReplicatedDelta, ReplicationStamp, SearchLogOp,
};

/// `CF_META` key storing `(db_epoch_be8 || published_local_seq_be8)` for the outbound mesh publisher.
pub const META_KEY_MESH_PUB_SEQ: &[u8] = b"mesh:pub_seq";

/// `CF_META` prefix storing persisted per-peer replication watermarks (`mesh:peer:{peer_id}`).
pub const META_PREFIX_MESH_PEER: &[u8] = b"mesh:peer:";

/// Default hard cap on incoming/outgoing serialized Zenoh mesh payloads (4 MiB).
pub const DEFAULT_MAX_MESH_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

/// Default maximum deltas permitted inside a single [`MeshDeltaEnvelope`].
pub const DEFAULT_MAX_DELTAS_PER_ENVELOPE: usize = 1_024;

/// Default maximum catch-up pagination rounds per [`crate::mesh::MeshNode::catchup_from_peer`] call.
pub const DEFAULT_MAX_CATCHUP_ROUNDS: usize = 1_024;

/// Validate a mesh node identifier (ASCII alphanumeric, `-`, `_`; 1..=64 bytes).
/// Rejects Zenoh key-expression wildcards and path separators.
pub fn validate_node_id(node_id: &str) -> Result<(), MembusError> {
    if node_id.is_empty() || node_id.len() > 64 {
        return Err(MembusError::InvalidArgument(
            "mesh node_id must be 1..=64 ASCII characters".to_owned(),
        ));
    }
    if !node_id
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(MembusError::InvalidArgument(format!(
            "mesh node_id '{node_id}' contains disallowed characters"
        )));
    }
    Ok(())
}

/// Validate a Zenoh cluster prefix (slash-separated ASCII segments, no wildcards or leading/trailing `/`).
pub fn validate_cluster_prefix(prefix: &str) -> Result<(), MembusError> {
    if prefix.is_empty() || prefix.len() > 128 || prefix.starts_with('/') || prefix.ends_with('/') {
        return Err(MembusError::InvalidArgument(
            "mesh cluster_prefix must be 1..=128 chars without leading/trailing '/'".to_owned(),
        ));
    }
    for seg in prefix.split('/') {
        if seg.is_empty()
            || !seg
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(MembusError::InvalidArgument(format!(
                "invalid segment '{seg}' in mesh cluster_prefix '{prefix}'"
            )));
        }
    }
    Ok(())
}

/// Build the `CF_META` key for a peer watermark: `mesh:peer:{peer_id}`.
pub fn peer_watermark_meta_key(peer_id: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(META_PREFIX_MESH_PEER.len() + peer_id.len());
    out.extend_from_slice(META_PREFIX_MESH_PEER);
    out.extend_from_slice(peer_id.as_bytes());
    out
}

/// Persisted peer cursor watermark stored in `CF_META`.
///
/// Strictly separates inbound receive progress (`remote_epoch`, `last_origin_seq` received FROM
/// `peer_id`) from outbound acknowledgment progress (`local_epoch`, `outbound_acked_seq` pulled BY
/// `peer_id` from our own local `CF_LOG`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshPeerWatermark {
    pub peer_id: String,
    pub remote_epoch: u64,
    pub last_origin_seq: u64,
    #[serde(default)]
    pub local_epoch: Option<u64>,
    #[serde(default)]
    pub outbound_acked_seq: Option<u64>,
    pub updated_at_ms: u64,
}

/// Compact base64 wire representation of [`OrderedIndexEntry`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireOrderedIndexEntry {
    pub index_id: String,
    #[serde(with = "base64_bytes")]
    pub sort_key: Vec<u8>,
    #[serde(with = "base64_bytes")]
    pub summary: Vec<u8>,
}

/// Compact base64 wire representation of [`GraphEdgeEntry`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireGraphEdgeEntry {
    pub index_id: String,
    #[serde(with = "base64_bytes")]
    pub src_id: Vec<u8>,
    #[serde(with = "base64_bytes")]
    pub rel_label: Vec<u8>,
    #[serde(with = "base64_bytes")]
    pub dst_id: Vec<u8>,
    #[serde(with = "base64_bytes")]
    pub meta: Vec<u8>,
}

/// Compact base64 wire representation of [`IndexMutations`].
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireIndexMutations {
    #[serde(default)]
    pub ordered: Vec<WireOrderedIndexEntry>,
    #[serde(default)]
    pub edges: Vec<WireGraphEdgeEntry>,
}

impl From<&IndexMutations> for WireIndexMutations {
    fn from(src: &IndexMutations) -> Self {
        Self {
            ordered: src
                .ordered
                .iter()
                .map(|o| WireOrderedIndexEntry {
                    index_id: o.index_id.clone(),
                    sort_key: o.sort_key.clone(),
                    summary: o.summary.clone(),
                })
                .collect(),
            edges: src
                .edges
                .iter()
                .map(|e| WireGraphEdgeEntry {
                    index_id: e.index_id.clone(),
                    src_id: e.src_id.clone(),
                    rel_label: e.rel_label.clone(),
                    dst_id: e.dst_id.clone(),
                    meta: e.meta.clone(),
                })
                .collect(),
        }
    }
}

impl From<WireIndexMutations> for IndexMutations {
    fn from(src: WireIndexMutations) -> Self {
        Self {
            ordered: src
                .ordered
                .into_iter()
                .map(|o| OrderedIndexEntry {
                    index_id: o.index_id,
                    sort_key: o.sort_key,
                    summary: o.summary,
                })
                .collect(),
            edges: src
                .edges
                .into_iter()
                .map(|e| GraphEdgeEntry {
                    index_id: e.index_id,
                    src_id: e.src_id,
                    rel_label: e.rel_label,
                    dst_id: e.dst_id,
                    meta: e.meta,
                })
                .collect(),
        }
    }
}

/// Compact base64 wire representation of [`MutationOp`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireMutationOp {
    Put {
        #[serde(with = "base64_bytes")]
        value: Vec<u8>,
    },
    Remove,
}

impl From<MutationOp> for WireMutationOp {
    fn from(src: MutationOp) -> Self {
        match src {
            MutationOp::Put { value } => Self::Put { value },
            MutationOp::Remove => Self::Remove,
        }
    }
}

impl From<WireMutationOp> for MutationOp {
    fn from(src: WireMutationOp) -> Self {
        match src {
            WireMutationOp::Put { value } => Self::Put { value },
            WireMutationOp::Remove => Self::Remove,
        }
    }
}

/// Compact base64 wire representation of [`ReplicatedDelta`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireReplicatedDelta {
    pub stamp: ReplicationStamp,
    pub namespace: String,
    #[serde(with = "base64_bytes")]
    pub key: Vec<u8>,
    pub op: WireMutationOp,
    #[serde(default)]
    pub indexes: Option<WireIndexMutations>,
    #[serde(default)]
    pub search_op: Option<SearchLogOp>,
}

impl From<ReplicatedDelta> for WireReplicatedDelta {
    fn from(d: ReplicatedDelta) -> Self {
        Self {
            stamp: d.stamp,
            namespace: d.namespace,
            key: d.key,
            op: d.op.into(),
            indexes: d.indexes.as_ref().map(WireIndexMutations::from),
            search_op: d.search_op,
        }
    }
}

impl From<WireReplicatedDelta> for ReplicatedDelta {
    fn from(d: WireReplicatedDelta) -> Self {
        Self {
            stamp: d.stamp,
            namespace: d.namespace,
            key: d.key,
            op: d.op.into(),
            indexes: d.indexes.map(IndexMutations::from),
            search_op: d.search_op,
        }
    }
}

/// Live delta batch published by a node onto `{cluster}/delta/{origin_node_id}`
/// and served by gap catch-up queryables on `{cluster}/catchup/{origin_node_id}`.
///
/// For transitive propagation (`relayed == true`), `origin_node_id`/`origin_seq` identify
/// the immediate relaying node's `CF_LOG` position while each delta's `stamp.origin_node_id`
/// and `stamp.origin_seq` preserve the original author's identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MeshDeltaEnvelope {
    pub origin_node_id: String,
    pub origin_epoch: u64,
    pub origin_seq: u64,
    #[serde(default)]
    pub relayed: bool,
    #[serde(default)]
    pub deltas: Vec<WireReplicatedDelta>,
    #[serde(default)]
    pub standalone_search_ops: Vec<SearchLogOp>,
    #[serde(default)]
    pub standalone_origin: Option<ReplicationStamp>,
}

/// Query payload sent to `{cluster}/catchup/{peer_id}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatchupRequest {
    pub since_origin_seq: u64,
    pub max_entries: usize,
    #[serde(default)]
    pub max_bytes: Option<usize>,
    #[serde(default)]
    pub expected_epoch: Option<u64>,
    #[serde(default)]
    pub requester_node_id: Option<String>,
}

/// Reply payload returned by `{cluster}/catchup/{peer_id}`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatchupResponse {
    Batch {
        origin_node_id: String,
        origin_epoch: u64,
        envelopes: Vec<MeshDeltaEnvelope>,
        scanned_through_seq: u64,
        has_more: bool,
    },
    Gap {
        earliest_available_seq: u64,
        current_epoch: u64,
    },
    EpochMismatch {
        origin_node_id: String,
        current_epoch: u64,
    },
    Error(String),
}

/// Query payload sent to `{cluster}/digest/{peer_id}` for XOR anti-entropy comparison.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DigestRequest {
    pub namespace: String,
    #[serde(with = "base64_bytes")]
    pub key_prefix: Vec<u8>,
}

/// Deterministic XOR support-digest reply for fast partition detection and reconciliation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DigestResponse {
    pub node_id: String,
    pub epoch: u64,
    pub latest_seq: u64,
    pub entry_count: u64,
    pub xor_digest: [u8; 32],
}

/// Wire reply envelope for `{cluster}/digest/{peer_id}` so errors reply promptly instead of timing out.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DigestReply {
    Ok(DigestResponse),
    Error(String),
}
