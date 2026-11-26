//! Membus v2 core: RocksDB optimistic transactional storage, sharded in-memory
//! signal ring with durable commit-log catch-up, and multiplexed `membus/2` async RPC.

pub mod cli;
pub mod client;
pub mod protocol;
pub mod signals;
pub mod storage;
pub mod transport;
pub mod types;

pub use client::{Client, ClientSnapshot, ClientSubscription};
pub use signals::{
    EachChangeSubscription, LiveCoalescedSubscription, SignalBroker, SignalConfig, SignalRing,
    WILDCARD_NAMESPACE,
};
pub use storage::{
    AdmissionConfig, AdmissionGate, NoopPublisher, OwnedSnapshot, RocksDbStorage, SignalPublisher,
    StorageConfig, StorageEngine,
};
pub use transport::{Endpoint, MembusServer, ServerConfig, ServerHandle};
pub use types::{
    Ack, CommitResult, ExpectedVersion, HealthStatus, LogScanPage, MembusError, MutationOp,
    RecordMeta, ScanOptions, ScanPage, SignalEvent, SignalKind, StoredRecord, VersionGuard,
    WalBatch, WriteMutation,
};
