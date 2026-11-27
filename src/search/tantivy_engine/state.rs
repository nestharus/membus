use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;

use tantivy::{Index, IndexReader};

use crate::search::overlay::Tier0Overlay;
use crate::storage::StorageEngine;

use super::schema::{SearchSchemaFields, WRITER_HEAP_BYTES};

/// Configuration for the embedded disk-persisted Tantivy engine + Tier-0 RAM overlay.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SearchConfig {
    pub commit_interval_ms: u64,
    pub max_buffered_docs: usize,
    pub max_overlay_docs: usize,
    pub writer_heap_bytes: usize,
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            commit_interval_ms: 150,
            max_buffered_docs: 256,
            max_overlay_docs: 16_384,
            writer_heap_bytes: WRITER_HEAP_BYTES,
        }
    }
}

pub(super) struct WorkerState {
    pub(super) pending_hint_seq: u64,
    pub(super) dirty_through_seq: u64,
    pub(super) pending_count: usize,
    pub(super) flush_requested_seq: u64,
    pub(super) committed_seq: u64,
    pub(super) last_error: Option<String>,
    pub(super) shutdown: bool,
}

pub(super) fn maybe_clear_dirty(state: &WorkerState, search_dirty: &AtomicBool, latest_seq: u64) {
    if state.last_error.is_none()
        && state.committed_seq >= state.dirty_through_seq
        && state.committed_seq >= state.pending_hint_seq
        && state.committed_seq >= latest_seq
    {
        search_dirty.store(false, Ordering::Release);
    }
}

pub(super) fn consume_fault(counter: &AtomicUsize) -> bool {
    let mut cur = counter.load(Ordering::Acquire);
    while cur > 0 {
        match counter.compare_exchange_weak(cur, cur - 1, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return true,
            Err(actual) => cur = actual,
        }
    }
    false
}

/// Embedded disk-persisted Tantivy search engine with 0 ms Tier-0 RAM live delta overlay,
/// deterministic contiguous `CF_LOG` replay, visibility isolation, and prefix revocation.
pub struct SearchEngine {
    pub(super) storage: Arc<dyn StorageEngine>,
    pub(super) index: Index,
    pub(super) reader: IndexReader,
    pub(super) fields: SearchSchemaFields,
    pub(super) epoch: u64,
    pub(super) overlay: Arc<Tier0Overlay>,
    pub(super) committed_seq: Arc<AtomicU64>,
    pub(super) deferred_ingest_total: Arc<AtomicU64>,
    pub(super) search_dirty: Arc<AtomicBool>,
    pub(super) fail_ingest_remaining: Arc<AtomicUsize>,
    pub(super) fail_flush_remaining: Arc<AtomicUsize>,
    pub(super) worker_state: Arc<(Mutex<WorkerState>, Condvar)>,
    pub(super) worker_handle: Mutex<Option<JoinHandle<()>>>,
}

impl SearchEngine {
    /// Last contiguous commit sequence durably flushed to Tantivy and checkpointed in `meta` CF.
    pub fn committed_seq(&self) -> u64 {
        self.committed_seq.load(Ordering::Acquire)
    }

    /// Number of uncommitted live/tombstone docs currently held in the Tier-0 RAM overlay.
    pub fn overlay_len(&self) -> usize {
        self.overlay.len()
    }

    /// Total number of post-commit search ingestion operations that failed and were deferred
    /// for `CF_LOG` catch-up replay.
    pub fn deferred_ingest_total(&self) -> u64 {
        self.deferred_ingest_total.load(Ordering::Acquire)
    }

    /// Whether the search index currently has deferred post-commit operations pending `CF_LOG` replay.
    pub fn is_dirty(&self) -> bool {
        self.search_dirty.load(Ordering::Acquire)
    }

    /// Inject deterministic post-commit ingest failures for testing deferred search recovery.
    #[doc(hidden)]
    pub fn inject_ingest_failure(&self, count: usize) {
        self.fail_ingest_remaining.store(count, Ordering::Release);
    }

    /// Inject deterministic `flush_sync` failures for testing observable shutdown/flush error handling.
    #[doc(hidden)]
    pub fn inject_flush_failure(&self, count: usize) {
        self.fail_flush_remaining.store(count, Ordering::Release);
    }
}

impl Drop for SearchEngine {
    fn drop(&mut self) {
        {
            let (lock, cv) = &*self.worker_state;
            if let Ok(mut state) = lock.lock() {
                state.shutdown = true;
                cv.notify_all();
            }
        }
        if let Ok(mut guard) = self.worker_handle.lock() {
            if let Some(handle) = guard.take() {
                let _ = handle.join();
            }
        }
    }
}
