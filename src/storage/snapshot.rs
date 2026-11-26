use std::mem::ManuallyDrop;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use rocksdb::{OptimisticTransactionDB, SnapshotWithThreadMode};

use crate::types::MembusError;

struct SnapshotGateInner {
    max_active: usize,
    active: AtomicUsize,
}

/// Lock-free global admission gate bounding live MVCC RocksDB snapshots so clients
/// cannot pin unbounded SST files and prevent RocksDB compaction.
#[derive(Clone)]
pub(crate) struct SnapshotAdmissionGate {
    inner: Arc<SnapshotGateInner>,
}

impl SnapshotAdmissionGate {
    pub(crate) fn new(max_active: usize) -> Self {
        Self {
            inner: Arc::new(SnapshotGateInner {
                max_active: max_active.max(1),
                active: AtomicUsize::new(0),
            }),
        }
    }

    pub(crate) fn try_acquire(&self) -> Result<SnapshotPermit, MembusError> {
        let mut cur = self.inner.active.load(Ordering::Relaxed);
        loop {
            if cur >= self.inner.max_active {
                return Err(MembusError::Busy {
                    retry_after_ms: 10,
                    reason: format!(
                        "global active snapshot limit reached ({cur}/{})",
                        self.inner.max_active
                    ),
                });
            }
            match self.inner.active.compare_exchange_weak(
                cur,
                cur + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    return Ok(SnapshotPermit {
                        inner: Arc::clone(&self.inner),
                    });
                }
                Err(actual) => cur = actual,
            }
        }
    }

    pub(crate) fn active(&self) -> usize {
        self.inner.active.load(Ordering::Acquire)
    }
}

/// RAII permit releasing one active snapshot slot when dropped.
pub(crate) struct SnapshotPermit {
    inner: Arc<SnapshotGateInner>,
}

impl Drop for SnapshotPermit {
    fn drop(&mut self) {
        self.inner.active.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Owned, `'static` handle to an O(1) MVCC RocksDB snapshot.
///
/// # Drop Order Invariant
///
/// The RocksDB [`SnapshotWithThreadMode`] borrows the underlying [`OptimisticTransactionDB`]
/// C++ pointer (`rocksdb_t*`). Both explicit [`ManuallyDrop::drop`] in [`Drop::drop`] AND
/// Rust's RFC 1857 declaration-order drop semantics guarantee that `snapshot` is released
/// strictly BEFORE `_db` drops its `Arc<OptimisticTransactionDB>` reference.
pub struct OwnedSnapshot {
    snapshot: ManuallyDrop<SnapshotWithThreadMode<'static, OptimisticTransactionDB>>,
    _permit: Option<SnapshotPermit>,
    _db: Arc<OptimisticTransactionDB>,
    snapshot_seq: u64,
}

impl OwnedSnapshot {
    /// Pin a RocksDB MVCC snapshot backed by `db` while holding `permit`.
    pub(crate) fn new(
        db: Arc<OptimisticTransactionDB>,
        snapshot_seq: u64,
        permit: SnapshotPermit,
    ) -> Self {
        let db_ptr: *const OptimisticTransactionDB = Arc::as_ptr(&db);
        // SAFETY:
        // 1. `db` is an `Arc<OptimisticTransactionDB>` whose pointee lives at a fixed heap
        //    address for as long as at least one `Arc` clone remains alive. Moving `db` into
        //    `Self::_db` does NOT move the heap-allocated `OptimisticTransactionDB`.
        // 2. `SnapshotWithThreadMode` holds only a shared reference `&OptimisticTransactionDB`
        //    (and its underlying C `rocksdb_snapshot_t*` handle); `OptimisticTransactionDB`
        //    is `Sync`, so shared access through `&OptimisticTransactionDB` is sound.
        // 3. `Self::drop` unconditionally executes `ManuallyDrop::drop(&mut self.snapshot)`
        //    BEFORE `self._db` can ever be dropped, and `snapshot` is never exposed with a
        //    `'static` lifetime outside this struct (`inner(&self)` ties the returned borrow
        //    strictly to `&self`). Therefore the extended `'static` lifetime cannot outlive
        //    `self._db`.
        let db_ref: &'static OptimisticTransactionDB = unsafe { &*db_ptr };
        let snapshot = SnapshotWithThreadMode::new(db_ref);
        Self {
            snapshot: ManuallyDrop::new(snapshot),
            _permit: Some(permit),
            _db: db,
            snapshot_seq,
        }
    }

    pub fn snapshot_seq(&self) -> u64 {
        self.snapshot_seq
    }

    pub(crate) fn inner<'a>(&'a self) -> &'a SnapshotWithThreadMode<'a, OptimisticTransactionDB> {
        &self.snapshot
    }

    pub(crate) fn belongs_to(&self, db: &Arc<OptimisticTransactionDB>) -> bool {
        Arc::ptr_eq(&self._db, db)
    }
}

impl Drop for OwnedSnapshot {
    fn drop(&mut self) {
        // SAFETY:
        // 1. `Drop::drop` runs at most once, so `self.snapshot` is dropped exactly once and
        //    is never accessed again afterwards.
        // 2. `self._db` is still alive during the body of `Drop::drop`, so the underlying
        //    `OptimisticTransactionDB` remains valid while RocksDB releases the snapshot
        //    handle (`rocksdb_release_snapshot`).
        // 3. Only after `Drop::drop` returns does Rust drop `_permit` and then `_db` (in
        //    RFC 1857 field order), guaranteeing `_db` outlives `snapshot`.
        unsafe {
            ManuallyDrop::drop(&mut self.snapshot);
        }
        // Release admission permit immediately after RocksDB snapshot handle is freed.
        self._permit.take();
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rocksdb::{OptimisticTransactionDB, Options};
    use tempfile::TempDir;

    use super::{OwnedSnapshot, SnapshotAdmissionGate};
    use crate::types::MembusError;

    #[test]
    fn snapshot_gate_enforces_cap_and_releases_on_drop() {
        let gate = SnapshotAdmissionGate::new(2);
        assert_eq!(gate.active(), 0);
        let p1 = gate.try_acquire().expect("permit 1");
        let p2 = gate.try_acquire().expect("permit 2");
        assert_eq!(gate.active(), 2);

        let err = gate.try_acquire().err().expect("3rd permit must fail");
        assert!(matches!(err, MembusError::Busy { .. }));

        drop(p1);
        assert_eq!(gate.active(), 1);
        let p3 = gate.try_acquire().expect("permit 3 after release");
        assert_eq!(gate.active(), 2);
        drop(p2);
        drop(p3);
        assert_eq!(gate.active(), 0);
    }

    #[test]
    fn owned_snapshot_outlives_caller_db_arc_and_drops_safely() {
        let dir = TempDir::new().expect("tempdir");
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = Arc::new(OptimisticTransactionDB::open(&opts, dir.path()).expect("open db"));
        db.put(b"k1", b"v1").expect("put v1");

        let gate = SnapshotAdmissionGate::new(4);
        let permit = gate.try_acquire().expect("permit");
        let snap = OwnedSnapshot::new(Arc::clone(&db), 1, permit);
        assert!(snap.belongs_to(&db));
        assert_eq!(gate.active(), 1);
        assert_eq!(Arc::strong_count(&db), 2);

        // Overwrite key after snapshot and then drop caller's original `Arc<DB>`.
        db.put(b"k1", b"v2").expect("put v2");
        drop(db);

        // Snapshot still holds the sole remaining `Arc<DB>` and reads the pinned MVCC view.
        let val = snap.inner().get(b"k1").expect("snap get").expect("present");
        assert_eq!(val.as_slice(), b"v1");

        // Dropping `snap` drops `snapshot` strictly before `_db` and frees the gate slot.
        drop(snap);
        assert_eq!(gate.active(), 0);
    }
}
