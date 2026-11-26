use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use rocksdb::OptimisticTransactionDB;

use crate::types::MembusError;

/// Periodic background WAL flusher for `Ack::Fast` writes.
///
/// Holds only a [`Weak`] reference to the underlying RocksDB instance and joins its
/// worker thread deterministically on drop.
pub(crate) struct BackgroundWalSync {
    stop_flag: Arc<(Mutex<bool>, Condvar)>,
    error_count: Arc<AtomicU64>,
    handle: Option<JoinHandle<()>>,
}

impl BackgroundWalSync {
    pub(crate) fn spawn(
        db: &Arc<OptimisticTransactionDB>,
        interval: Duration,
    ) -> Result<Self, MembusError> {
        let stop_flag = Arc::new((Mutex::new(false), Condvar::new()));
        let error_count = Arc::new(AtomicU64::new(0));
        let stop_ref = Arc::clone(&stop_flag);
        let err_ref = Arc::clone(&error_count);
        let weak_db: Weak<OptimisticTransactionDB> = Arc::downgrade(db);

        let handle = thread::Builder::new()
            .name("membus-wal-sync".to_owned())
            .spawn(move || {
                let (lock, condvar) = &*stop_ref;
                let mut stopped = lock.lock().unwrap_or_else(|e| e.into_inner());
                while !*stopped {
                    let (guard, _) = condvar
                        .wait_timeout(stopped, interval)
                        .unwrap_or_else(|e| e.into_inner());
                    stopped = guard;
                    if *stopped {
                        break;
                    }
                    drop(stopped);
                    if let Some(db) = weak_db.upgrade() {
                        if let Err(err) = db.flush_wal(true) {
                            err_ref.fetch_add(1, Ordering::Relaxed);
                            eprintln!("membus background WAL sync error: {err}");
                        }
                    } else {
                        break;
                    }
                    stopped = lock.lock().unwrap_or_else(|e| e.into_inner());
                }
            })
            .map_err(|err| {
                MembusError::Storage(format!("failed to spawn wal sync thread: {err}"))
            })?;

        Ok(Self {
            stop_flag,
            error_count,
            handle: Some(handle),
        })
    }

    pub(crate) fn error_count(&self) -> u64 {
        self.error_count.load(Ordering::Acquire)
    }
}

impl Drop for BackgroundWalSync {
    fn drop(&mut self) {
        let (lock, condvar) = &*self.stop_flag;
        if let Ok(mut stopped) = lock.lock() {
            *stopped = true;
            condvar.notify_all();
        }
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
