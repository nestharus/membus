//! Membus v2: RocksDB optimistic transactional storage, declarative secondary ordered
//! and graph indexes, content-addressed blobs, authentication/ACLs & fenced leases,
//! collection schema catalog, embedded Tantivy search with 0 ms Tier-0 RAM overlay,
//! sharded in-memory signal ring, and multiplexed TCP/mTLS/Named-Pipe/HTTP2/WS/SSE transports.

pub mod auth;
pub mod catalog;
pub mod cli;
pub mod client;
pub mod index;
pub mod protocol;
pub mod search;
pub mod signals;
pub mod storage;
pub mod transport;
pub mod types;

pub use auth::{AuthConfig, LeaseManager, TokenBinding};
pub use catalog::CollectionCatalog;
pub use client::{Client, ClientSnapshot, ClientSubscription};
pub use index::{BfsOptions, NeighborQueryOptions, OrderedKeyFilter, OrderedQueryOptions};
pub use search::{SearchConfig, SearchEngine};
pub use signals::{
    EachChangeSubscription, LiveCoalescedSubscription, SignalBroker, SignalConfig, SignalRing,
    WILDCARD_NAMESPACE,
};
pub use storage::{
    AdmissionConfig, AdmissionGate, MetaCasGuard, NoopPublisher, OwnedSnapshot, RocksDbStorage,
    SignalPublisher, StorageConfig, StorageEngine,
};
pub use transport::{
    Endpoint, MembusServer, ServerConfig, ServerHandle, TlsClientConfig, TlsServerConfig,
};
pub use types::{
    sanitize_search_log_op, Ack, AclGrant, AuthMode, BfsResult, CollectionDescriptor, CommitResult,
    ExpectedVersion, GraphDirection, GraphEdgeEntry, GraphEdgeRecord, HealthStatus, IndexKind,
    IndexMutations, IndexSpec, IndexedMutation, LeaseGrant, LeaseInfo, LogScanPage, MembusError,
    MigrationState, MutationOp, NeighborPage, OrderedIndexEntry, OrderedIndexHit, OrderedIndexPage,
    Permission, Principal, RecordMeta, RetentionPolicy, ScanOptions, ScanPage, SearchDocument,
    SearchHit, SearchLogOp, SearchLogPage, SearchLogRecord, SearchMode, SearchPage, SearchQuery,
    SearchVisibility, SignalEvent, SignalKind, StoreProfile, StoredRecord, VersionGuard, WalBatch,
    WriteMutation,
};
