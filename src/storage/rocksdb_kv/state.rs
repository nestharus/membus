use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize};
use std::sync::Arc;
use std::time::Instant;

use parking_lot::{Mutex as FastMutex, MutexGuard as FastMutexGuard};
use rocksdb::{BoundColumnFamily, OptimisticTransactionDB};

use crate::storage::admission::AdmissionGate;
use crate::storage::constants::{
    ALL_COLUMN_FAMILIES, CF_IDX_EDGE, CF_IDX_ORD, CF_IDX_REV, CF_KV, CF_LOG,
};
use crate::storage::contract::StorageConfig;
use crate::storage::hlc::HlcClock;
use crate::storage::sequencer::PublishSequencer;
use crate::storage::snapshot::{OwnedSnapshot, SnapshotAdmissionGate};
use crate::storage::wal_sync::BackgroundWalSync;
use crate::types::MembusError;

/// Number of lock stripes serializing per-resource `CF_LEASE` mutations against
/// lease-guarded transactions (`T_L`) in canonical ascending stripe order.
pub(crate) const LEASE_LOCK_STRIPES: usize = 64;

/// Multi-threaded RocksDB storage engine backed by `OptimisticTransactionDB`.
pub struct RocksDbStorage {
    pub(crate) data_dir: PathBuf,
    pub(crate) db: Arc<OptimisticTransactionDB>,
    pub(crate) config: StorageConfig,
    pub(crate) epoch: u64,
    pub(crate) log_horizon: AtomicU64,
    pub(crate) trim_lock: FastMutex<()>,
    pub(crate) idem_prune_lock: FastMutex<()>,
    pub(crate) lease_stripes: Vec<FastMutex<()>>,
    pub(crate) boot_wall_ms: u64,
    pub(crate) boot_instant: Instant,
    pub(crate) idempotency_entry_count: AtomicUsize,
    pub(crate) hlc: HlcClock,
    pub(crate) sequencer: Arc<PublishSequencer>,
    pub(crate) admission: AdmissionGate,
    pub(crate) snapshot_gate: SnapshotAdmissionGate,
    pub(crate) bg_sync: Option<BackgroundWalSync>,
}

impl RocksDbStorage {
    pub fn column_family_names(&self) -> Vec<&'static str> {
        ALL_COLUMN_FAMILIES
            .iter()
            .copied()
            .filter(|name| self.db.cf_handle(name).is_some())
            .collect()
    }

    /// Number of background WAL sync failures observed since opening this instance.
    pub fn background_wal_sync_errors(&self) -> u64 {
        self.bg_sync
            .as_ref()
            .map_or(0, BackgroundWalSync::error_count)
    }

    /// Trigger manual RocksDB flush + full range compaction across `CF_KV` and `CF_LOG`.
    pub fn compact_all(&self) -> Result<(), MembusError> {
        for cf_name in [CF_KV, CF_LOG, CF_IDX_ORD, CF_IDX_EDGE, CF_IDX_REV] {
            let cf = self.cf(cf_name)?;
            self.db
                .flush_cf(&cf)
                .map_err(|e| MembusError::Storage(e.into_string()))?;
            self.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
        }
        Ok(())
    }

    pub(crate) fn cf(&self, name: &str) -> Result<Arc<BoundColumnFamily<'_>>, MembusError> {
        self.db
            .cf_handle(name)
            .ok_or_else(|| MembusError::Storage(format!("missing column family '{name}'")))
    }

    pub(crate) fn build_value_hint(&self, value: &[u8]) -> Option<Vec<u8>> {
        if value.len() <= self.config.max_signal_hint_bytes {
            Some(value.to_vec())
        } else {
            None
        }
    }

    pub(crate) fn verify_snapshot_owner(&self, snapshot: &OwnedSnapshot) -> Result<(), MembusError> {
        if !snapshot.belongs_to(&self.db) {
            return Err(MembusError::InvalidArgument(
                "snapshot belongs to a different RocksDbStorage instance".to_owned(),
            ));
        }
        Ok(())
    }

    pub(crate) fn query_deadline(&self) -> Option<Instant> {
        self.config
            .query_timeout
            .and_then(|d| Instant::now().checked_add(d))
    }

    pub(crate) fn check_query_deadline(deadline: Option<Instant>) -> Result<(), MembusError> {
        if let Some(dl) = deadline {
            if Instant::now() >= dl {
                return Err(MembusError::Busy {
                    retry_after_ms: 50,
                    reason: "query timeout exceeded".to_owned(),
                });
            }
        }
        Ok(())
    }

    pub(crate) fn monotonic_now_ms_impl(&self) -> u64 {
        let elapsed_ms = u64::try_from(self.boot_instant.elapsed().as_millis()).unwrap_or(u64::MAX);
        self.boot_wall_ms.saturating_add(elapsed_ms)
    }

    pub(crate) fn lease_stripe_index(encoded_lease_key: &[u8]) -> usize {
        // FNV-1a 64-bit hash for deterministic, fast lock-stripe partitioning.
        let mut h: u64 = 0xcbf2_9ce4_8422_2325;
        for &b in encoded_lease_key {
            h ^= u64::from(b);
            h = h.wrapping_mul(0x0100_0000_01b3);
        }
        (h as usize) % LEASE_LOCK_STRIPES
    }

    pub(crate) fn lock_lease_stripe(&self, encoded_lease_key: &[u8]) -> FastMutexGuard<'_, ()> {
        self.lease_stripes[Self::lease_stripe_index(encoded_lease_key)].lock()
    }

    /// Acquire lease-serialization locks for all referenced lease keys in canonical
    /// ascending stripe-index order (deadlock-free for multi-lease transactions).
    pub(crate) fn lock_lease_stripes(
        &self,
        encoded_lease_keys: &[Vec<u8>],
    ) -> Vec<FastMutexGuard<'_, ()>> {
        let mut indices = BTreeSet::new();
        for key in encoded_lease_keys {
            indices.insert(Self::lease_stripe_index(key));
        }
        indices
            .into_iter()
            .map(|idx| self.lease_stripes[idx].lock())
            .collect()
    }
}
