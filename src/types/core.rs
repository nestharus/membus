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

/// An atomic lease precondition checked inside a scoped transaction (`T_L`).
///
/// Verified against `CF_LEASE` under RocksDB optimistic conflict detection and the
/// resident's monotonic lease clock in the same atomic commit as all KV mutations,
/// eliminating TOCTOU windows between `validate_lease` and `commit_tx`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseGuard {
    pub namespace: String,
    pub resource: String,
    pub holder: String,
    pub generation: u64,
    pub token: String,
}

impl LeaseGuard {
    pub fn from_grant(grant: &crate::types::LeaseGrant) -> Self {
        Self {
            namespace: grant.namespace.clone(),
            resource: grant.resource.clone(),
            holder: grant.holder.clone(),
            generation: grant.generation,
            token: grant.token.clone(),
        }
    }
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

/// Encode a 32-byte SHA-256 digest as a 64-character lowercase hexadecimal string.
pub fn encode_sha256_hex(digest: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for &b in digest {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}
