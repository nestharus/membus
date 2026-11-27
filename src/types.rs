use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Maximum permitted namespace length in bytes.
pub const MAX_NAMESPACE_BYTES: usize = 256;

/// Maximum permitted key length in bytes.
pub const MAX_KEY_BYTES: usize = 64 * 1024;

/// Default maximum permitted value payload in bytes (16 MiB).
pub const DEFAULT_MAX_VALUE_BYTES: usize = 16 * 1024 * 1024;

/// Maximum permitted client `X-Membus-Idempotency-Key` length in bytes.
pub const MAX_IDEMPOTENCY_KEY_BYTES: usize = 256;

/// Default upper bound on retained transactional idempotency records in `CF_META`.
pub const DEFAULT_MAX_IDEMPOTENCY_ENTRIES: usize = 16_384;

/// Durability contract requested by a write operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum Ack {
    /// Immediate RAM MemTable + OS WAL buffer commit (`sync = false`).
    /// Immediately visible to all readers and signal subscribers.
    /// Does not promise survival across sudden host power loss.
    Fast = 0,
    /// Synchronized WAL commit (`sync = true`), participating in RocksDB's
    /// native lock-free `WriteThread` leader-follower group commit.
    Flush = 1,
}

impl Ack {
    pub fn from_u8(raw: u8) -> Result<Self, MembusError> {
        match raw {
            0 => Ok(Self::Fast),
            1 => Ok(Self::Flush),
            other => Err(MembusError::Protocol(format!("unknown ack mode: {other}"))),
        }
    }
}

/// Compare-and-swap precondition on a single key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExpectedVersion {
    /// Unconditional mutation.
    Any,
    /// Key must not currently exist.
    Absent,
    /// Key must exist with this exact version.
    Version(u64),
}

/// Stored metadata associated with a committed record.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordMeta {
    pub version: u64,
    pub flags: u8,
    pub ts_ms: u64,
    pub crc32: u32,
}

/// Read policy controlling how hot vs cold-tiered records are returned.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadMode {
    /// Return only the bytes stored in hot `CF_KV` without performing cold-tier I/O.
    #[default]
    HotOnly,
    /// Transparently hydrate `ColdRef` tombstones from the configured `ColdStore`,
    /// verifying SHA-256 on every read and failing closed with `MembusError::Corruption`.
    HydrateCold,
}

/// Compact deterministic lineage tombstone stored in hot `CF_KV` when a record's
/// value payload has been archived to a cold tier (`RECORD_FLAG_COLD_REF`).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColdRef {
    pub cold_sha256: [u8; 32],
    pub original_bytes: u64,
    pub archived_at_ms: u64,
    pub tier_uri: String,
    pub lineage_seq: u64,
}

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
    /// Standalone search ops not paired with a specific KV mutation
    /// (e.g. `index_document`, `delete_document`, `revoke_search_prefix`).
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

/// A decoded record returned from point reads or keyset scans.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub meta: RecordMeta,
}

/// An optimistic precondition checked inside a scoped transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct VersionGuard {
    pub namespace: String,
    pub key: Vec<u8>,
    pub expected: ExpectedVersion,
}

/// Single key mutation payload inside a transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationOp {
    Put { value: Vec<u8> },
    Remove,
}

/// A named mutation targeting `(namespace, key)`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WriteMutation {
    pub namespace: String,
    pub key: Vec<u8>,
    pub op: MutationOp,
}

/// Client-emitted ordered secondary index entry stored in `idx_ord`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderedIndexEntry {
    pub index_id: String,
    pub sort_key: Vec<u8>,
    pub summary: Vec<u8>,
}

/// Client-emitted labeled directed graph edge stored bidirectionally in `idx_edge`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphEdgeEntry {
    pub index_id: String,
    pub src_id: Vec<u8>,
    pub rel_label: Vec<u8>,
    pub dst_id: Vec<u8>,
    pub meta: Vec<u8>,
}

/// Optional declarative index postings emitted atomically alongside a primary KV mutation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexMutations {
    pub ordered: Vec<OrderedIndexEntry>,
    pub edges: Vec<GraphEdgeEntry>,
}

impl IndexMutations {
    pub fn is_empty(&self) -> bool {
        self.ordered.is_empty() && self.edges.is_empty()
    }
}

/// A transaction mutation optionally carrying secondary index postings and search ops.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexedMutation {
    pub mutation: WriteMutation,
    pub indexes: Option<IndexMutations>,
    pub search_op: Option<SearchLogOp>,
}

/// Traversal direction for graph neighbor queries and multi-hop BFS.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum GraphDirection {
    Out = 1,
    In = 2,
    Both = 3,
}

impl GraphDirection {
    pub fn from_u8(raw: u8) -> Result<Self, MembusError> {
        match raw {
            1 => Ok(Self::Out),
            2 => Ok(Self::In),
            3 => Ok(Self::Both),
            other => Err(MembusError::Protocol(format!(
                "unknown graph direction: {other}"
            ))),
        }
    }
}

/// A decoded entry from an ordered secondary index query.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderedIndexHit {
    pub sort_key: Vec<u8>,
    pub primary_key: Vec<u8>,
    pub summary: Vec<u8>,
}

/// Stateless keyset-paginated ordered index scan result pinned to an MVCC snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderedIndexPage {
    pub entries: Vec<OrderedIndexHit>,
    pub next_cursor: Option<Vec<u8>>,
    pub truncated: bool,
    pub snapshot_seq: u64,
}

/// A decoded bidirectional adjacency edge returned from graph queries.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphEdgeRecord {
    pub src_id: Vec<u8>,
    pub rel_label: Vec<u8>,
    pub dst_id: Vec<u8>,
    pub direction: GraphDirection,
    pub meta: Vec<u8>,
}

/// Keyset-paginated single-hop graph neighbor query result.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NeighborPage {
    pub edges: Vec<GraphEdgeRecord>,
    pub next_cursor: Option<Vec<u8>>,
    pub truncated: bool,
    pub snapshot_seq: u64,
}

/// Bounded multi-hop BFS traversal result executed strictly inside `idx_edge`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BfsResult {
    pub visited_nodes: Vec<Vec<u8>>,
    pub traversed_edges: Vec<GraphEdgeRecord>,
    pub frontier_nodes: Vec<Vec<u8>>,
    pub truncated: bool,
    pub snapshot_seq: u64,
}

/// Outcome of a committed write or multi-key transaction.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitResult {
    pub seq: u64,
    pub epoch: u64,
    pub versions: Vec<u64>,
}

/// Pagination parameters for keyset prefix/range scans.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanOptions {
    pub after_key: Option<Vec<u8>>,
    pub limit: usize,
    pub max_bytes: usize,
}

impl Default for ScanOptions {
    fn default() -> Self {
        Self {
            after_key: None,
            limit: 256,
            max_bytes: 1024 * 1024,
        }
    }
}

/// Stateless keyset-paginated scan result pinned to a specific MVCC snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScanPage {
    pub records: Vec<StoredRecord>,
    pub next_cursor: Option<Vec<u8>>,
    pub truncated: bool,
    pub snapshot_seq: u64,
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

/// Visibility policy controlling what fields may enter the embedded search index.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum SearchVisibility {
    Public = 0,
    MetadataOnly = 1,
    Sealed = 2,
}

impl SearchVisibility {
    pub fn from_u8(raw: u8) -> Result<Self, MembusError> {
        match raw {
            0 => Ok(Self::Public),
            1 => Ok(Self::MetadataOnly),
            2 => Ok(Self::Sealed),
            other => Err(MembusError::Protocol(format!(
                "unknown search visibility: {other}"
            ))),
        }
    }
}

/// Searchable document payload submitted to the embedded Tantivy engine.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchDocument {
    pub namespace: String,
    pub doc_id: String,
    pub kind: String,
    pub title: String,
    pub tags: Vec<String>,
    pub body: String,
    pub visibility: SearchVisibility,
}

/// Search mutation logged durably in `CF_LOG` for deterministic restart replay.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SearchLogOp {
    Index(SearchDocument),
    Delete { namespace: String, doc_id: String },
    RevokePrefix { namespace: String, prefix: String },
}

/// One replayed search mutation with its durable commit sequence, intra-tx index, and DB epoch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchLogRecord {
    pub seq: u64,
    pub op_idx: u32,
    pub epoch: u64,
    pub op: SearchLogOp,
}

/// Paginated search log scan page returned by `StorageEngine::scan_search_log_after`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchLogPage {
    pub records: Vec<SearchLogRecord>,
    pub scanned_through_seq: u64,
}

/// Search execution mode supported by [`crate::search::SearchEngine`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum SearchMode {
    Bm25 = 1,
    LiteralSubstring = 2,
}

impl SearchMode {
    pub fn from_u8(raw: u8) -> Result<Self, MembusError> {
        match raw {
            1 => Ok(Self::Bm25),
            2 => Ok(Self::LiteralSubstring),
            other => Err(MembusError::Protocol(format!(
                "unknown search mode: {other}"
            ))),
        }
    }
}

/// Structured query submitted to [`crate::search::SearchEngine`].
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SearchQuery {
    pub query: String,
    pub mode: SearchMode,
    pub namespace: Option<String>,
    pub kind: Option<String>,
    pub limit: usize,
}

/// One ranked search hit returned across Tier-0 RAM overlay and Tantivy segments.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SearchHit {
    pub namespace: String,
    pub doc_id: String,
    pub kind: String,
    pub title: String,
    pub tags: Vec<String>,
    pub score: f32,
}

/// Search response page with Tantivy committed watermark.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SearchPage {
    pub hits: Vec<SearchHit>,
    pub total_hits_estimate: usize,
    pub committed_seq: u64,
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
    /// Single active schema version (`[schema_version]`).
    #[default]
    Strict,
    /// Expand phase accepting both current (`vN`) and target (`vN+1`) schema versions.
    ExpandDual,
    /// Reverse-shadow rollback window accepting both promoted (`vN+1`) and rollback (`vN`) versions.
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
    /// Hash `key_len_be4 || key || value_sha256` (resolving `ColdRef`s to `cold_sha256`).
    #[default]
    Full,
    /// Hash `key_len_be4 || key` only for additive `vN+1` expand-phase key-set parity.
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

/// Explicit runtime slot role controlling write fencing during live A/B deployments.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotRole {
    #[default]
    Active,
    Shadow,
    ReadOnly,
    Draining,
}

impl SlotRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Shadow => "shadow",
            Self::ReadOnly => "read_only",
            Self::Draining => "draining",
        }
    }

    pub fn parse(raw: &str) -> Result<Self, MembusError> {
        if raw.eq_ignore_ascii_case("active") {
            Ok(Self::Active)
        } else if raw.eq_ignore_ascii_case("shadow") {
            Ok(Self::Shadow)
        } else if raw.eq_ignore_ascii_case("read_only") || raw.eq_ignore_ascii_case("readonly") {
            Ok(Self::ReadOnly)
        } else if raw.eq_ignore_ascii_case("draining") {
            Ok(Self::Draining)
        } else {
            Err(MembusError::InvalidArgument(format!(
                "unknown slot role '{raw}' (expected active, shadow, read_only, or draining)"
            )))
        }
    }

    pub(crate) fn as_u8(self) -> u8 {
        match self {
            Self::Active => 0,
            Self::Shadow => 1,
            Self::ReadOnly => 2,
            Self::Draining => 3,
        }
    }

    pub(crate) fn from_u8(raw: u8) -> Result<Self, MembusError> {
        match raw {
            0 => Ok(Self::Active),
            1 => Ok(Self::Shadow),
            2 => Ok(Self::ReadOnly),
            3 => Ok(Self::Draining),
            other => Err(MembusError::Protocol(format!("unknown slot role: {other}"))),
        }
    }
}

/// Persisted slot role and monotonic fencing epoch stored in `CF_META` (`sys:slot_role`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SlotRoleState {
    pub role: SlotRole,
    pub role_epoch: u64,
}

/// Caller-supplied idempotency key and canonical request fingerprint enforced atomically
/// inside a single storage transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IdempotencyContext {
    pub scope_namespace: String,
    pub principal_id: Option<String>,
    pub is_tx: bool,
    pub key: String,
    pub request_fingerprint_sha256: [u8; 32],
}

/// Raw WAL batch returned by `read_wal_since`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalBatch {
    pub rocksdb_seq: u64,
    pub raw_bytes: Vec<u8>,
    pub op_count: usize,
}

/// Secondary search-index synchronization status reported after an authoritative storage commit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SearchSyncStatus {
    /// Post-commit search ingestion into the Tier-0 overlay succeeded synchronously.
    #[default]
    Synced,
    /// Authoritative storage committed, but secondary search ingestion failed and was
    /// deferred for catch-up replay from `CF_LOG`.
    Deferred,
}

impl SearchSyncStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Synced => "synced",
            Self::Deferred => "deferred",
        }
    }
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

/// Unified error type across storage, index, auth, catalog, search, signals, protocol, transport, and client boundaries.
#[derive(Debug, Error, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MembusError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("version conflict on {namespace}: expected {expected:?}, actual {actual:?}")]
    Conflict {
        namespace: String,
        key: Vec<u8>,
        expected: ExpectedVersion,
        actual: Option<u64>,
    },

    #[error("lease '{resource}' held by '{holder}' (generation {generation}, expires {expires_at_ms})")]
    LeaseHeld {
        resource: String,
        holder: String,
        generation: u64,
        expires_at_ms: u64,
    },

    #[error("optimistic transaction conflict: {0}")]
    TxConflict(String),

    #[error("slot role fenced (role={role:?}, role_epoch={role_epoch}): {reason}")]
    RoleFenced {
        role: SlotRole,
        role_epoch: u64,
        reason: String,
    },

    #[error("idempotency key conflict: {0}")]
    IdempotencyConflict(String),

    #[error("server busy (retry after {retry_after_ms}ms): {reason}")]
    Busy { retry_after_ms: u32, reason: String },

    #[error("subscription gap: earliest available seq {earliest_available_seq}, epoch {current_epoch}")]
    Gap {
        earliest_available_seq: u64,
        current_epoch: u64,
    },

    #[error("not found: {0}")]
    NotFound(String),

    #[error("data corruption: {0}")]
    Corruption(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("frame payload {len} bytes exceeds max_frame_body {max_len}")]
    FrameTooLarge { len: usize, max_len: usize },

    #[error("io error: {0}")]
    Io(String),

    #[error("connection or runtime closed")]
    Closed,
}

impl From<std::io::Error> for MembusError {
    fn from(err: std::io::Error) -> Self {
        Self::Io(err.to_string())
    }
}

/// Validate a search document's namespace and doc_id without mutating input.
pub fn validate_search_document(doc: &SearchDocument) -> Result<(), MembusError> {
    validate_namespace(&doc.namespace)?;
    if doc.doc_id.is_empty() {
        return Err(MembusError::InvalidArgument(
            "search doc_id must not be empty".to_owned(),
        ));
    }
    if doc.doc_id.len() > MAX_KEY_BYTES {
        return Err(MembusError::InvalidArgument(format!(
            "search doc_id length {} exceeds maximum {MAX_KEY_BYTES}",
            doc.doc_id.len()
        )));
    }
    if doc.doc_id.as_bytes().contains(&0) {
        return Err(MembusError::InvalidArgument(
            "search doc_id must not contain NUL bytes".to_owned(),
        ));
    }
    Ok(())
}

/// Validate and visibility-sanitize a [`SearchLogOp`] BEFORE it enters `CF_LOG`, WAL, or search.
///
/// - `Index` of `SearchVisibility::Sealed` is rewritten to `SearchLogOp::Delete` so sealed
///   plaintext never reaches `CF_LOG`, WAL, SSTs, RAM overlay, or Tantivy.
/// - `Index` of `SearchVisibility::MetadataOnly` clears `body` before persistence.
pub fn sanitize_search_log_op(op: &SearchLogOp) -> Result<SearchLogOp, MembusError> {
    match op {
        SearchLogOp::Index(doc) => {
            validate_search_document(doc)?;
            match doc.visibility {
                SearchVisibility::Sealed => Ok(SearchLogOp::Delete {
                    namespace: doc.namespace.clone(),
                    doc_id: doc.doc_id.clone(),
                }),
                SearchVisibility::MetadataOnly => {
                    let mut clean = doc.clone();
                    clean.body.clear();
                    Ok(SearchLogOp::Index(clean))
                }
                SearchVisibility::Public => Ok(SearchLogOp::Index(doc.clone())),
            }
        }
        SearchLogOp::Delete { namespace, doc_id } => {
            validate_namespace(namespace)?;
            if doc_id.is_empty() {
                return Err(MembusError::InvalidArgument(
                    "search doc_id must not be empty".to_owned(),
                ));
            }
            if doc_id.len() > MAX_KEY_BYTES || doc_id.as_bytes().contains(&0) {
                return Err(MembusError::InvalidArgument(
                    "invalid search doc_id".to_owned(),
                ));
            }
            Ok(SearchLogOp::Delete {
                namespace: namespace.clone(),
                doc_id: doc_id.clone(),
            })
        }
        SearchLogOp::RevokePrefix { namespace, prefix } => {
            validate_namespace(namespace)?;
            if prefix.len() > MAX_KEY_BYTES || prefix.as_bytes().contains(&0) {
                return Err(MembusError::InvalidArgument(
                    "invalid search revoke prefix".to_owned(),
                ));
            }
            Ok(SearchLogOp::RevokePrefix {
                namespace: namespace.clone(),
                prefix: prefix.clone(),
            })
        }
    }
}

/// Sanitize a floating-point search score so NaN/Inf can never poison JSON serialization.
pub fn sanitize_score(score: f32) -> f32 {
    if score.is_finite() {
        score
    } else {
        0.0
    }
}

/// Validate a namespace name without mutating input.
pub fn validate_namespace(namespace: &str) -> Result<(), MembusError> {
    if namespace.is_empty() {
        return Err(MembusError::InvalidArgument(
            "namespace must not be empty".to_owned(),
        ));
    }
    if namespace.len() > MAX_NAMESPACE_BYTES {
        return Err(MembusError::InvalidArgument(format!(
            "namespace length {} exceeds maximum {MAX_NAMESPACE_BYTES}",
            namespace.len()
        )));
    }
    if namespace.as_bytes().contains(&0) {
        return Err(MembusError::InvalidArgument(
            "namespace must not contain NUL bytes".to_owned(),
        ));
    }
    Ok(())
}

/// Validate a key byte slice without mutating input.
pub fn validate_key(key: &[u8]) -> Result<(), MembusError> {
    if key.is_empty() {
        return Err(MembusError::InvalidArgument(
            "key must not be empty".to_owned(),
        ));
    }
    if key.len() > MAX_KEY_BYTES {
        return Err(MembusError::InvalidArgument(format!(
            "key length {} exceeds maximum {MAX_KEY_BYTES}",
            key.len()
        )));
    }
    Ok(())
}

/// Validate a value byte slice against a configured limit.
pub fn validate_value(value: &[u8], max_value_bytes: usize) -> Result<(), MembusError> {
    if value.len() > max_value_bytes {
        return Err(MembusError::InvalidArgument(format!(
            "value length {} exceeds maximum {max_value_bytes}",
            value.len()
        )));
    }
    Ok(())
}

/// Current wall-clock timestamp in Unix milliseconds.
pub fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
