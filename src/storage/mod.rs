pub mod admission;
pub mod codec;
pub mod rocksdb_engine;

use std::path::Path;
use std::time::Duration;

use crate::index::{BfsOptions, NeighborQueryOptions, OrderedQueryOptions};
use crate::types::{
    Ack, BfsResult, CommitResult, ExpectedVersion, IndexMutations, IndexedMutation, LogScanPage,
    MembusError, NeighborPage, OrderedIndexPage, ScanOptions, ScanPage, SearchLogOp, SearchLogPage,
    SignalEvent, StoredRecord, VersionGuard, WalBatch, WriteMutation, DEFAULT_MAX_VALUE_BYTES,
};

pub use admission::{AdmissionConfig, AdmissionGate, AdmissionPermit};
pub use rocksdb_engine::{OwnedSnapshot, RocksDbStorage};

/// Reserved column family names provisioned on open.
pub const CF_KV: &str = "kv";
pub const CF_LOG: &str = "log";
pub const CF_BLOB: &str = "blob";
pub const CF_IDX_ORD: &str = "idx_ord";
pub const CF_IDX_EDGE: &str = "idx_edge";
pub const CF_IDX_REV: &str = "idx_rev";
pub const CF_LEASE: &str = "lease";
pub const CF_META: &str = "meta";

/// All named column families provisioned by Membus v2 storage.
pub const ALL_COLUMN_FAMILIES: [&str; 8] = [
    CF_KV,
    CF_LOG,
    CF_BLOB,
    CF_IDX_ORD,
    CF_IDX_EDGE,
    CF_IDX_REV,
    CF_LEASE,
    CF_META,
];

/// Callback boundary invoked in strict commit-sequence order after each durable commit.
pub trait SignalPublisher: Send + Sync {
    fn publish_ordered(&self, events: &[SignalEvent]);
}

/// No-op publisher for standalone storage callers that do not attach a signal ring.
pub struct NoopPublisher;

impl SignalPublisher for NoopPublisher {
    fn publish_ordered(&self, _events: &[SignalEvent]) {}
}

/// Raw `(key, value)` byte pair returned by low-level `CF_META` prefix scans.
pub type RawKeyValue = (Vec<u8>, Vec<u8>);

/// Atomic compare-and-swap precondition on a raw `CF_META` key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MetaCasGuard {
    pub key: Vec<u8>,
    pub expected: Option<Vec<u8>>,
}

/// Configuration for [`RocksDbStorage`].
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
        }
    }
}

/// Core storage engine contract implemented by [`RocksDbStorage`].
pub trait StorageEngine: Send + Sync {
    fn epoch(&self) -> u64;
    fn latest_seq(&self) -> u64;
    fn log_horizon_seq(&self) -> u64;
    fn rocksdb_latest_sequence(&self) -> u64;
    fn in_flight_ops(&self) -> usize;
    fn in_flight_bytes(&self) -> usize;

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

    fn trim_log_before(&self, keep_from_seq: u64) -> Result<(), MembusError>;

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
