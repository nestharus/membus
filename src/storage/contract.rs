use std::path::Path;
use std::time::Duration;

use crate::index::{BfsOptions, NeighborQueryOptions, OrderedQueryOptions};
use crate::storage::admission::AdmissionConfig;
use crate::storage::snapshot::OwnedSnapshot;
use crate::types::{
    Ack, BfsResult, CommitResult, DeltaLogPage, ExpectedVersion, IdempotencyContext,
    IndexMutations, IndexedMutation, LogScanPage, LogWatermark, MembusError, NeighborPage,
    OrderedIndexPage, ReplicatedApplyResult, ReplicatedDelta, ReplicationStamp, ScanOptions,
    ScanPage, SearchLogOp, SearchLogPage, StoredRecord, VersionGuard, WalBatch, WriteMutation,
    DEFAULT_MAX_IDEMPOTENCY_ENTRIES, DEFAULT_MAX_VALUE_BYTES,
};

pub use crate::storage::publisher::{NoopPublisher, SignalPublisher};

/// Raw `(key, value)` byte pair returned by low-level `CF_META` prefix scans.
pub type RawKeyValue = (Vec<u8>, Vec<u8>);

/// Atomic compare-and-swap precondition on a raw `CF_META` key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetaCasGuard {
    pub key: Vec<u8>,
    pub expected: Option<Vec<u8>>,
}

/// Configuration for [`crate::storage::RocksDbStorage`].
#[derive(Clone, Debug)]
pub struct StorageConfig {
    pub write_buffer_size: usize,
    pub max_write_buffer_number: i32,
    pub level0_slowdown_writes_trigger: i32,
    pub level0_stop_writes_trigger: i32,
    pub block_cache_bytes: usize,
    pub total_write_buffer_bytes: usize,
    pub blob_min_size: u64,
    pub max_value_bytes: usize,
    pub max_signal_hint_bytes: usize,
    pub wal_ttl_seconds: u64,
    pub wal_size_limit_mb: u64,
    pub bg_sync_interval: Option<Duration>,
    pub admission: AdmissionConfig,
    /// Global maximum number of concurrently pinned MVCC [`OwnedSnapshot`] handles.
    pub max_active_snapshots: usize,
    /// Upper bound on requested items/nodes per scan, index query, or graph traversal page.
    pub max_scan_limit: usize,
    /// Optional cooperative execution deadline for scan, index query, and graph traversal loops.
    pub query_timeout: Option<Duration>,
    /// Maximum retained transactional idempotency dedup records in `CF_META` before pruning oldest.
    pub max_idempotency_entries: usize,
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            write_buffer_size: 64 * 1024 * 1024,
            max_write_buffer_number: 4,
            level0_slowdown_writes_trigger: 20,
            level0_stop_writes_trigger: 36,
            block_cache_bytes: 128 * 1024 * 1024,
            total_write_buffer_bytes: 256 * 1024 * 1024,
            blob_min_size: 16 * 1024,
            max_value_bytes: DEFAULT_MAX_VALUE_BYTES,
            max_signal_hint_bytes: 4096,
            wal_ttl_seconds: 3600,
            wal_size_limit_mb: 256,
            bg_sync_interval: Some(Duration::from_millis(250)),
            admission: AdmissionConfig::default(),
            max_active_snapshots: 1024,
            max_scan_limit: 10_000,
            query_timeout: Some(Duration::from_secs(30)),
            max_idempotency_entries: DEFAULT_MAX_IDEMPOTENCY_ENTRIES,
        }
    }
}

/// Core storage engine contract implemented by [`crate::storage::RocksDbStorage`].
pub trait StorageEngine: Send + Sync {
    fn data_dir(&self) -> Option<&Path>;
    fn epoch(&self) -> u64;
    fn latest_seq(&self) -> u64;
    fn log_horizon_seq(&self) -> u64;
    fn rocksdb_latest_sequence(&self) -> u64;
    fn in_flight_ops(&self) -> usize;
    fn in_flight_bytes(&self) -> usize;
    /// Number of currently pinned MVCC snapshots held against this storage instance.
    fn active_snapshots(&self) -> usize {
        0
    }
    /// Current monotonic HLC timestamp in milliseconds.
    fn hlc_now_ms(&self) -> u64;

    fn get(&self, namespace: &str, key: &[u8]) -> Result<Option<StoredRecord>, MembusError>;
    fn get_many(
        &self,
        namespace: &str,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<StoredRecord>>, MembusError>;

    fn put(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
    ) -> Result<CommitResult, MembusError>;

    /// Put a record with explicit header `flags` (e.g. during backup delta replay),
    /// optionally preserving existing secondary index postings untouched.
    // Public trait signature preserved for API compatibility.
    #[allow(clippy::too_many_arguments)]
    fn put_with_flags(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        flags: u8,
        preserve_indexes: bool,
        ack: Ack,
        expected: ExpectedVersion,
    ) -> Result<CommitResult, MembusError>;

    /// Atomically transition a record between hot bytes and a compact `ColdRef` tombstone:
    /// - Bitwise sets or clears `RECORD_FLAG_COLD_REF` only (preserving other flag bits)
    /// - Preserves original logical `timestamp_ms`, LWW stamp, and secondary indexes
    /// - Emits NO signal events and marks the `CF_LOG` delta `is_tier_swap = true` so
    ///   it is never broadcast over the mesh.
    // Public trait signature preserved for API compatibility.
    #[allow(clippy::too_many_arguments)]
    fn tier_swap_record(
        &self,
        namespace: &str,
        key: &[u8],
        new_value: &[u8],
        set_cold_flag: bool,
        logical_value_sha256: [u8; 32],
        ack: Ack,
        expected: ExpectedVersion,
    ) -> Result<CommitResult, MembusError>;

    /// Atomically apply one replicated delta under deterministic LWW tie-breaking
    /// (`(timestamp_ms, origin_node_id, origin_seq, value_sha256)`), advancing the
    /// monotonic HLC and persisting `origin_stamp` in `CF_LOG` for transitive relay.
    fn apply_replicated_delta(
        &self,
        local_node_id: &str,
        delta: &ReplicatedDelta,
        ack: Ack,
    ) -> Result<ReplicatedApplyResult, MembusError>;

    /// Atomically append replicated standalone search ops (e.g. `index_document`,
    /// `delete_document`, `revoke_search_prefix`), deduplicated by `(origin_node_id, origin_seq)`.
    fn append_replicated_search_ops(
        &self,
        origin_stamp: &ReplicationStamp,
        ops: &[SearchLogOp],
        ack: Ack,
    ) -> Result<ReplicatedApplyResult, MembusError>;

    // Public trait signature preserved for API compatibility.
    #[allow(clippy::too_many_arguments)]
    fn put_indexed(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
        indexes: Option<&IndexMutations>,
        search_op: Option<&SearchLogOp>,
    ) -> Result<CommitResult, MembusError>;

    fn remove(
        &self,
        namespace: &str,
        key: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
    ) -> Result<CommitResult, MembusError>;

    fn remove_indexed(
        &self,
        namespace: &str,
        key: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
        search_op: Option<&SearchLogOp>,
    ) -> Result<CommitResult, MembusError>;

    fn commit_tx(
        &self,
        guards: &[VersionGuard],
        mutations: &[WriteMutation],
        ack: Ack,
    ) -> Result<CommitResult, MembusError>;

    fn commit_indexed_tx(
        &self,
        guards: &[VersionGuard],
        mutations: &[IndexedMutation],
        ack: Ack,
    ) -> Result<CommitResult, MembusError>;

    /// Atomically commit an indexed `PUT` with optional transactional idempotency enforcement.
    /// Returns `(commit, replayed)` where `replayed == true` indicates an idempotent replay.
    // Public trait signature preserved for API compatibility.
    #[allow(clippy::too_many_arguments)]
    fn put_indexed_idempotent(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
        indexes: Option<&IndexMutations>,
        search_op: Option<&SearchLogOp>,
        idempotency: Option<&IdempotencyContext>,
    ) -> Result<(CommitResult, bool), MembusError> {
        if idempotency.is_some() {
            return Err(MembusError::InvalidArgument(
                "idempotent writes are not supported by this storage backend".to_owned(),
            ));
        }
        Ok((
            self.put_indexed(namespace, key, value, ack, expected, indexes, search_op)?,
            false,
        ))
    }

    /// Atomically commit an indexed `REMOVE` with optional transactional idempotency enforcement.
    // Public trait signature preserved for API compatibility.
    #[allow(clippy::too_many_arguments)]
    fn remove_indexed_idempotent(
        &self,
        namespace: &str,
        key: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
        search_op: Option<&SearchLogOp>,
        idempotency: Option<&IdempotencyContext>,
    ) -> Result<(CommitResult, bool), MembusError> {
        if idempotency.is_some() {
            return Err(MembusError::InvalidArgument(
                "idempotent writes are not supported by this storage backend".to_owned(),
            ));
        }
        Ok((
            self.remove_indexed(namespace, key, ack, expected, search_op)?,
            false,
        ))
    }

    /// Atomically commit a multi-key indexed transaction with optional transactional idempotency enforcement.
    fn commit_indexed_tx_idempotent(
        &self,
        guards: &[VersionGuard],
        mutations: &[IndexedMutation],
        ack: Ack,
        idempotency: Option<&IdempotencyContext>,
    ) -> Result<(CommitResult, bool), MembusError> {
        if idempotency.is_some() {
            return Err(MembusError::InvalidArgument(
                "idempotent writes are not supported by this storage backend".to_owned(),
            ));
        }
        Ok((self.commit_indexed_tx(guards, mutations, ack)?, false))
    }

    fn open_snapshot(&self) -> Result<OwnedSnapshot, MembusError>;

    fn get_in_snapshot(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        key: &[u8],
    ) -> Result<Option<StoredRecord>, MembusError>;

    fn get_many_in_snapshot(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<StoredRecord>>, MembusError>;

    fn scan_prefix(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        prefix: &[u8],
        options: &ScanOptions,
    ) -> Result<ScanPage, MembusError>;

    fn scan_range(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        start_key: &[u8],
        end_key: &[u8],
        options: &ScanOptions,
    ) -> Result<ScanPage, MembusError>;

    fn query_ordered_index(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        index_id: &str,
        options: &OrderedQueryOptions,
    ) -> Result<OrderedIndexPage, MembusError>;

    fn query_neighbors(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        index_id: &str,
        node_id: &[u8],
        options: &NeighborQueryOptions,
    ) -> Result<NeighborPage, MembusError>;

    fn bfs_traverse(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        index_id: &str,
        seed_node: &[u8],
        options: &BfsOptions,
    ) -> Result<BfsResult, MembusError>;

    fn put_blob(&self, digest: &[u8], data: &[u8], ack: Ack) -> Result<(), MembusError>;
    fn get_blob(&self, digest: &[u8]) -> Result<Option<Vec<u8>>, MembusError>;

    fn put_cas_blob(
        &self,
        namespace: &str,
        data: &[u8],
        ack: Ack,
    ) -> Result<[u8; 32], MembusError>;
    fn get_cas_blob(
        &self,
        namespace: &str,
        sha256: &[u8; 32],
    ) -> Result<Option<Vec<u8>>, MembusError>;
    fn has_cas_blob(&self, namespace: &str, sha256: &[u8; 32]) -> Result<bool, MembusError>;

    fn scan_log_after(
        &self,
        after_seq: u64,
        limit: usize,
    ) -> Result<LogScanPage, MembusError>;

    fn append_search_log_op(
        &self,
        op: SearchLogOp,
        ack: Ack,
    ) -> Result<CommitResult, MembusError>;

    fn scan_search_log_after(
        &self,
        after_seq: u64,
        limit: usize,
    ) -> Result<SearchLogPage, MembusError>;

    fn scan_deltas_after(
        &self,
        after_seq: u64,
        limit: usize,
    ) -> Result<DeltaLogPage, MembusError>;

    fn trim_log_before(&self, keep_from_seq: u64) -> Result<(), MembusError>;

    /// List active consumer log watermarks persisted in `CF_META` (search, backup, mesh).
    fn list_log_watermarks(&self) -> Result<Vec<LogWatermark>, MembusError>;

    /// Coordinate log trimming against active search, backup, and mesh watermarks unless `force == true`.
    fn safe_trim_log_before(&self, keep_from_seq: u64, force: bool) -> Result<(), MembusError>;

    fn meta_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MembusError>;
    fn meta_put(&self, key: &[u8], value: &[u8], sync: bool) -> Result<(), MembusError>;
    fn meta_scan_prefix(&self, prefix: &[u8]) -> Result<Vec<RawKeyValue>, MembusError>;
    fn meta_cas_batch(
        &self,
        guards: &[MetaCasGuard],
        puts: &[(Vec<u8>, Vec<u8>)],
        deletes: &[Vec<u8>],
        sync: bool,
    ) -> Result<(), MembusError>;

    fn lease_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MembusError>;
    fn lease_cas(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        new_value: &[u8],
    ) -> Result<(), MembusError>;

    fn create_checkpoint(&self, dest_dir: &Path) -> Result<u64, MembusError>;
    fn read_wal_since(&self, rocksdb_seq: u64, limit: usize) -> Result<Vec<WalBatch>, MembusError>;
    fn flush_wal(&self, sync: bool) -> Result<(), MembusError>;
}
