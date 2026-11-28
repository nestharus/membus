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
pub mod router;
pub mod search;
pub mod signals;
pub mod storage;
pub mod tiering;
pub mod transport;
pub mod types;

pub use auth::{
    extract_tenant_id, tenant_grant, tenant_namespace, tenant_namespace_pattern,
    tenant_token_binding, validate_tenant_id, validate_workspace_slug, AuthConfig, AuthStore,
    LeaseManager, TokenBinding,
};
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
    compute_namespace_parity_digest, compute_namespace_parity_digest_with_timeout,
    compute_namespace_xor_digest, compute_namespace_xor_digest_with_mode, AdmissionConfig,
    AdmissionGate, MetaCasGuard, NoopPublisher, OwnedSnapshot, RocksDbStorage, SignalPublisher,
    StorageConfig, StorageEngine,
};
pub use tiering::{ArchiveReport, ColdStore, FileColdStore, TieringManager, TieringPolicy};
pub use transport::{
    validate_listener_bind_security, CoordinatedCommit, Endpoint, MembusServer, ServerConfig,
    ServerHandle, TlsClientConfig, TlsServerConfig, META_KEY_SLOT_ROLE,
};
pub use types::{
    sanitize_search_log_op, Ack, AclGrant, AuthMode, BfsResult, ColdRef, CollectionDescriptor,
    CommitResult, CompatibilityMode, DeltaLogPage, ExpectedVersion, GraphDirection,
    GraphEdgeEntry, GraphEdgeRecord, HealthStatus, IdempotencyContext, IndexKind, IndexMutations,
    IndexSpec, IndexedMutation, LeaseGrant, LeaseGuard, LeaseInfo, LogDeltaEntry, LogMutationDelta,
    LogScanPage, LogWatermark, MembusError, MigrationState, MutationOp, NamespaceParityDigest,
    NeighborPage, OrderedIndexEntry, OrderedIndexHit, OrderedIndexPage, ParityDigestMode,
    Permission, Principal, ReadMode, RecordMeta, ReplicatedApplyResult, ReplicatedDelta,
    ReplicationStamp, RetentionPolicy, ScanOptions, ScanPage, SearchDocument, SearchHit,
    SearchLogOp, SearchLogPage, SearchLogRecord, SearchMode, SearchPage, SearchQuery,
    SearchSyncStatus, SearchVisibility, SignalEvent, SignalKind, SlotRole, SlotRoleState,
    StoreProfile, StoredRecord, VersionGuard, WalBatch, WriteMutation,
    DEFAULT_MAX_IDEMPOTENCY_ENTRIES, MAX_IDEMPOTENCY_KEY_BYTES,
};
