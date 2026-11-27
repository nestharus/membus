pub mod admission;
pub mod codec;
pub mod constants;
pub mod contract;
pub mod hlc;
pub mod mesh_replication;
pub mod parity;
pub mod publisher;
pub mod rocksdb_kv;
pub mod sequencer;
pub mod snapshot;
pub mod wal_sync;

/// Compatibility re-export module preserving `membus::storage::rocksdb_engine::*` paths.
pub mod rocksdb_engine {
    pub use super::rocksdb_kv::RocksDbStorage;
    pub use super::snapshot::OwnedSnapshot;
}

pub use admission::{AdmissionConfig, AdmissionGate, AdmissionPermit};
pub use constants::{
    ALL_COLUMN_FAMILIES, CF_BLOB, CF_IDX_EDGE, CF_IDX_ORD, CF_IDX_REV, CF_KV, CF_LEASE, CF_LOG,
    CF_META,
};
pub use contract::{
    MetaCasGuard, NoopPublisher, RawKeyValue, SignalPublisher, StorageConfig, StorageEngine,
};
pub use parity::{
    compute_namespace_parity_digest, compute_namespace_parity_digest_with_timeout,
    compute_namespace_xor_digest, compute_namespace_xor_digest_with_mode, encode_sha256_hex,
};
pub use rocksdb_kv::RocksDbStorage;
pub use snapshot::OwnedSnapshot;
