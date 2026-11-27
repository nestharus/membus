use serde::{Deserialize, Serialize};

use super::core::{MembusError, MutationOp, SlotRole};
use super::index_search::{IndexMutations, SearchLogOp};

/// Single mutation delta recorded durably in `CF_LOG` (`LOG_FORMAT_V4`) for
/// incremental backup replay and off-path multi-node mesh replication.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogMutationDelta {
    pub namespace: String,
    pub key: Vec<u8>,
    pub op: MutationOp,
    pub flags: u8,
    #[serde(default)]
    pub preserve_indexes: bool,
    #[serde(default)]
    pub is_tier_swap: bool,
    pub indexes: Option<IndexMutations>,
    #[serde(default)]
    pub search_op: Option<SearchLogOp>,
    #[serde(default)]
    pub origin_stamp: Option<ReplicationStamp>,
}

/// Full committed log record decoded from `CF_LOG` for backup & mesh replication.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogDeltaEntry {
    pub seq: u64,
    pub epoch: u64,
    pub ts_ms: u64,
    pub replicated: bool,
    pub mutations: Vec<LogMutationDelta>,
    /// Standalone search ops not paired with a specific KV mutation.
    pub search_ops: Vec<SearchLogOp>,
    /// Original replication stamp when this entry is a replicated standalone search commit.
    #[serde(default)]
    pub standalone_origin: Option<ReplicationStamp>,
}

/// Paginated delta log scan result reporting the exact contiguous sequence scanned through.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeltaLogPage {
    pub entries: Vec<LogDeltaEntry>,
    pub scanned_through_seq: u64,
}

/// Deterministic LWW ordering stamp for replicated multi-node convergence.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicationStamp {
    pub timestamp_ms: u64,
    pub origin_node_id: String,
    pub origin_seq: u64,
    pub value_sha256: [u8; 32],
}

impl ReplicationStamp {
    /// Total deterministic LWW comparison tuple:
    /// `(timestamp_ms, origin_node_id, origin_seq, value_sha256)`.
    pub fn cmp_lww(&self, other: &Self) -> std::cmp::Ordering {
        self.timestamp_ms
            .cmp(&other.timestamp_ms)
            .then_with(|| self.origin_node_id.cmp(&other.origin_node_id))
            .then_with(|| self.origin_seq.cmp(&other.origin_seq))
            .then_with(|| self.value_sha256.cmp(&other.value_sha256))
    }
}

/// One replicated key/search delta exchanged between mesh peers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatedDelta {
    pub stamp: ReplicationStamp,
    pub namespace: String,
    pub key: Vec<u8>,
    pub op: MutationOp,
    pub indexes: Option<IndexMutations>,
    pub search_op: Option<SearchLogOp>,
}

/// Outcome of applying a replicated delta under deterministic LWW rules.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatedApplyResult {
    pub applied: bool,
    pub local_seq: Option<u64>,
}

/// Named active consumer watermark consulted by safe log-trimming.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LogWatermark {
    pub name: String,
    pub seq: u64,
}

/// Kind of change emitted into the signal ring and commit log.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum SignalKind {
    Put = 1,
    Delete = 2,
}

impl SignalKind {
    pub fn from_u8(raw: u8) -> Result<Self, MembusError> {
        match raw {
            1 => Ok(Self::Put),
            2 => Ok(Self::Delete),
            other => Err(MembusError::Corruption(format!(
                "unknown signal kind: {other}"
            ))),
        }
    }
}

/// Immutable change event stored in the commit log and published to the RAM ring.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignalEvent {
    pub seq: u64,
    pub epoch: u64,
    pub namespace: String,
    pub key: Vec<u8>,
    pub version: u64,
    pub kind: SignalKind,
    pub value_hint: Option<Vec<u8>>,
}

/// Paginated commit-log scan result reporting the exact contiguous sequence scanned through.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogScanPage {
    pub events: Vec<SignalEvent>,
    pub scanned_through_seq: u64,
}

/// Fine-grained authorization permission.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Permission {
    Read,
    Write,
    Admin,
}

/// One namespace/prefix grant assigned to a [`Principal`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AclGrant {
    pub namespace_pattern: String,
    pub key_prefix: Vec<u8>,
    pub permissions: Vec<Permission>,
}

/// Authenticated caller identity carrying ACL grants.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Principal {
    pub id: String,
    pub grants: Vec<AclGrant>,
}

/// Explicit authentication enforcement mode.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum AuthMode {
    #[default]
    Disabled,
    Enforced,
}

/// Granted fenced resource lease stored atomically in `lease` CF (includes secret `token`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseGrant {
    pub namespace: String,
    pub resource: String,
    pub holder: String,
    pub generation: u64,
    pub token: String,
    pub expires_at_ms: u64,
}

/// Public inspection view of an active resource lease (deliberately omits secret `token`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseInfo {
    pub namespace: String,
    pub resource: String,
    pub holder: String,
    pub generation: u64,
    pub expires_at_ms: u64,
}

/// Declared store profile for a collection in [`crate::catalog::CollectionCatalog`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum StoreProfile {
    Kv,
    Log,
    Blob,
    Graph,
    Mixed,
}

/// Declared secondary index type in a collection descriptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexKind {
    Ordered,
    Graph,
}

/// Declarative index specification recorded in the collection catalog.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexSpec {
    pub index_id: String,
    pub kind: IndexKind,
    pub description: String,
}

/// Optional retention policy recorded in a collection descriptor.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionPolicy {
    pub max_age_secs: Option<u64>,
    pub max_entries: Option<u64>,
}

/// Schema compatibility policy active for a collection during normal operation or A/B rollout.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompatibilityMode {
    #[default]
    Strict,
    ExpandDual,
    ReverseShadow,
}

/// Explicit schema migration lifecycle state for a collection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationState {
    Active,
    Migrating {
        target_version: u32,
        target_sha256: [u8; 32],
        verified_through: u64,
    },
    ReverseShadow {
        rollback_version: u32,
        rollback_sha256: [u8; 32],
        verified_through: u64,
    },
}

pub(crate) mod base64_bytes {
    use base64::engine::general_purpose::STANDARD;
    use base64::Engine as _;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&STANDARD.encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let encoded = String::deserialize(deserializer)?;
        STANDARD.decode(encoded).map_err(serde::de::Error::custom)
    }
}

/// Versioned collection descriptor persisted atomically in `meta` CF.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionDescriptor {
    pub namespace: String,
    pub store_profile: StoreProfile,
    pub schema_version: u32,
    pub schema_sha256: [u8; 32],
    #[serde(with = "base64_bytes")]
    pub schema_bytes: Vec<u8>,
    pub declared_indexes: Vec<IndexSpec>,
    pub retention_policy: RetentionPolicy,
    pub migration_state: MigrationState,
    #[serde(default)]
    pub compatible_versions: Vec<u32>,
    #[serde(default)]
    pub compatibility_mode: CompatibilityMode,
}

/// Hashing mode for namespace parity digests.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ParityDigestMode {
    #[default]
    Full,
    KeysOnly,
}

/// First-class namespace parity digest returned by `POST /v2/namespaces/{ns}/digest`
/// and binary `Request::NamespaceDigest`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NamespaceParityDigest {
    pub node_id: String,
    pub epoch: u64,
    pub latest_seq: u64,
    pub log_horizon_seq: u64,
    pub entry_count: u64,
    pub xor_digest_hex: String,
    pub catalog_schema_version: Option<u32>,
}

/// Point-in-time server and storage health report.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthStatus {
    pub epoch: u64,
    pub latest_seq: u64,
    pub log_horizon_seq: u64,
    pub in_flight_ops: usize,
    pub in_flight_bytes: usize,
    pub active_connections: usize,
    pub active_subscriptions: usize,
    #[serde(default)]
    pub active_snapshots: usize,
    #[serde(default)]
    pub active_streams: usize,
    #[serde(default)]
    pub search_ingest_deferred_total: u64,
    #[serde(default)]
    pub search_dirty: bool,
    #[serde(default)]
    pub slot_role: SlotRole,
    #[serde(default)]
    pub role_epoch: u64,
}
