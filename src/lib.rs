//! Membus v2: RocksDB optimistic transactional storage, declarative secondary ordered
//! and graph indexes, content-addressed blobs, authentication/ACLs & fenced leases,
//! collection schema catalog, embedded Tantivy search with 0 ms Tier-0 RAM overlay,
//! embedded Zenoh multi-node mesh, zero-downtime backup/restore, hot/warm/cold tiering,
//! sharded in-memory signal ring, and unified HTTP2/WS/SSE + TCP/mTLS/Named-Pipe transports.

pub mod auth;
pub mod backup;
pub mod catalog;
pub mod cli;
pub mod client;
pub mod index;
pub mod mesh;
pub mod protocol;
pub mod search;
pub mod signals;
pub mod storage;
pub mod tiering;
pub mod transport;
pub mod types;

pub use auth::{AuthConfig, LeaseManager, TokenBinding};
pub use backup::{
    create_full_backup, create_full_backup_with_search, export_incremental_delta,
    restore_clean_room, verify_backup_dir, verify_delta_bundle, BackupCounts, BackupManifest,
    DeltaBundle, FileChecksum, RestoreReport,
};
pub use catalog::CollectionCatalog;
pub use client::{Client, ClientSnapshot, ClientSubscription};
pub use index::{BfsOptions, NeighborQueryOptions, OrderedKeyFilter, OrderedQueryOptions};
pub use mesh::{
    CatchupRequest, CatchupResponse, CatchupStats, DigestReply, DigestRequest, DigestResponse,
    MeshConfig, MeshDeltaEnvelope, MeshNode, MeshPeerWatermark, ReconcileReport,
};
pub use search::{SearchConfig, SearchEngine};
pub use signals::{
    EachChangeSubscription, LiveCoalescedSubscription, SignalBroker, SignalConfig, SignalRing,
    WILDCARD_NAMESPACE,
};
pub use storage::{
    AdmissionConfig, AdmissionGate, MetaCasGuard, NoopPublisher, OwnedSnapshot, RocksDbStorage,
    SignalPublisher, StorageConfig, StorageEngine,
};
pub use tiering::{ArchiveReport, ColdStore, FileColdStore, TieringManager, TieringPolicy};
pub use transport::{
    Endpoint, MembusServer, ServerConfig, ServerHandle, TlsClientConfig, TlsServerConfig,
};
pub use types::{
    sanitize_search_log_op, Ack, AclGrant, AuthMode, BfsResult, ColdRef, CollectionDescriptor,
    CommitResult, DeltaLogPage, ExpectedVersion, GraphDirection, GraphEdgeEntry, GraphEdgeRecord,
    HealthStatus, IndexKind, IndexMutations, IndexSpec, IndexedMutation, LeaseGrant, LeaseInfo,
    LogDeltaEntry, LogMutationDelta, LogScanPage, LogWatermark, MembusError, MigrationState,
    MutationOp, NeighborPage, OrderedIndexEntry, OrderedIndexHit, OrderedIndexPage, Permission,
    Principal, ReadMode, RecordMeta, ReplicatedApplyResult, ReplicatedDelta, ReplicationStamp,
    RetentionPolicy, ScanOptions, ScanPage, SearchDocument, SearchHit, SearchLogOp, SearchLogPage,
    SearchLogRecord, SearchMode, SearchPage, SearchQuery, SearchVisibility, SignalEvent,
    SignalKind, StoreProfile, StoredRecord, VersionGuard, WalBatch, WriteMutation,
};
