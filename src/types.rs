use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Maximum permitted namespace length in bytes.
pub const MAX_NAMESPACE_BYTES: usize = 256;

/// Maximum permitted key length in bytes.
pub const MAX_KEY_BYTES: usize = 64 * 1024;

/// Default maximum permitted value payload in bytes (16 MiB).
pub const DEFAULT_MAX_VALUE_BYTES: usize = 16 * 1024 * 1024;

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

/// Explicit schema migration lifecycle state for a collection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MigrationState {
    Active,
    Migrating {
        target_version: u32,
        target_sha256: [u8; 32],
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
}

/// Raw WAL batch returned by `read_wal_since`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalBatch {
    pub rocksdb_seq: u64,
    pub raw_bytes: Vec<u8>,
    pub op_count: usize,
}

/// Point-in-time server and storage health report.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct HealthStatus {
    pub epoch: u64,
    pub latest_seq: u64,
    pub log_horizon_seq: u64,
    pub in_flight_ops: usize,
    pub in_flight_bytes: usize,
    pub active_connections: usize,
    pub active_subscriptions: usize,
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
