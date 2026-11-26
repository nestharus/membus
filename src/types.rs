use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// Maximum permitted namespace length in bytes.
pub const MAX_NAMESPACE_BYTES: usize = 256;

/// Maximum permitted key length in bytes.
pub const MAX_KEY_BYTES: usize = 64 * 1024;

/// Default maximum permitted value payload in bytes (16 MiB).
pub const DEFAULT_MAX_VALUE_BYTES: usize = 16 * 1024 * 1024;

/// Durability contract requested by a write operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpectedVersion {
    /// Unconditional mutation.
    Any,
    /// Key must not currently exist.
    Absent,
    /// Key must exist with this exact version.
    Version(u64),
}

/// Stored metadata associated with a committed record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordMeta {
    pub version: u64,
    pub flags: u8,
    pub ts_ms: u64,
    pub crc32: u32,
}

/// A decoded record returned from point reads or keyset scans.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub meta: RecordMeta,
}

/// An optimistic precondition checked inside a scoped transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VersionGuard {
    pub namespace: String,
    pub key: Vec<u8>,
    pub expected: ExpectedVersion,
}

/// Single key mutation payload inside a transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MutationOp {
    Put { value: Vec<u8> },
    Remove,
}

/// A named mutation targeting `(namespace, key)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteMutation {
    pub namespace: String,
    pub key: Vec<u8>,
    pub op: MutationOp,
}

/// Outcome of a committed write or multi-key transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitResult {
    pub seq: u64,
    pub epoch: u64,
    pub versions: Vec<u64>,
}

/// Pagination parameters for keyset prefix/range scans.
#[derive(Clone, Debug, PartialEq, Eq)]
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanPage {
    pub records: Vec<StoredRecord>,
    pub next_cursor: Option<Vec<u8>>,
    pub truncated: bool,
    pub snapshot_seq: u64,
}

/// Kind of change emitted into the signal ring and commit log.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
#[derive(Clone, Debug, PartialEq, Eq)]
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

/// Raw WAL batch returned by `read_wal_since`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WalBatch {
    pub rocksdb_seq: u64,
    pub raw_bytes: Vec<u8>,
    pub op_count: usize,
}

/// Point-in-time server and storage health report.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HealthStatus {
    pub epoch: u64,
    pub latest_seq: u64,
    pub log_horizon_seq: u64,
    pub in_flight_ops: usize,
    pub in_flight_bytes: usize,
    pub active_connections: usize,
    pub active_subscriptions: usize,
}

/// Unified error type across storage, signals, protocol, transport, and client boundaries.
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum MembusError {
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("version conflict on {namespace}: expected {expected:?}, actual {actual:?}")]
    Conflict {
        namespace: String,
        key: Vec<u8>,
        expected: ExpectedVersion,
        actual: Option<u64>,
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
