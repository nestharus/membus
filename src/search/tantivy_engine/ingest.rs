use std::sync::atomic::Ordering;

use crate::types::{
    sanitize_search_log_op, Ack, CommitResult, MembusError, SearchDocument, SearchLogOp,
    SearchSyncStatus,
};

use super::state::{consume_fault, maybe_clear_dirty, SearchEngine};

impl SearchEngine {
    /// Record a post-commit secondary search ingestion failure without rejecting the authoritative
    /// storage commit, marking the search index dirty and waking the background `CF_LOG` committer.
    pub fn record_deferred_ingest(&self, seq: u64, err: &MembusError) {
        self.deferred_ingest_total.fetch_add(1, Ordering::AcqRel);
        eprintln!(
            "membus search ingest deferred for committed seq={seq} (epoch={}): {err}; scheduled CF_LOG catch-up",
            self.epoch
        );
        let (lock, cv) = &*self.worker_state;
        let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
        state.dirty_through_seq = state.dirty_through_seq.max(seq);
        state.pending_hint_seq = state.pending_hint_seq.max(seq);
        state.pending_count = state.pending_count.saturating_add(1);
        self.search_dirty.store(true, Ordering::Release);
        cv.notify_all();
    }

    /// Replay any deferred or uncommitted `CF_LOG` search operations synchronously into Tantivy.
    pub fn catch_up_from_log(&self) -> Result<u64, MembusError> {
        self.flush_sync()
    }

    /// Durably log a visibility-sanitized search document in `CF_LOG` and ingest it into search,
    /// returning [`SearchSyncStatus::Deferred`] instead of rejecting if post-commit ingest fails.
    pub fn index_document_coordinated(
        &self,
        doc: SearchDocument,
        ack: Ack,
    ) -> Result<(CommitResult, SearchSyncStatus), MembusError> {
        self.append_and_ingest_op(SearchLogOp::Index(doc), ack)
    }

    /// Durably log a visibility-sanitized search document in `CF_LOG` and make it searchable at 0 ms lag.
    pub fn index_document(
        &self,
        doc: SearchDocument,
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        self.index_document_coordinated(doc, ack).map(|(c, _)| c)
    }

    /// Durably delete a search document and return its secondary search synchronization status.
    pub fn delete_document_coordinated(
        &self,
        namespace: &str,
        doc_id: &str,
        ack: Ack,
    ) -> Result<(CommitResult, SearchSyncStatus), MembusError> {
        self.append_and_ingest_op(
            SearchLogOp::Delete {
                namespace: namespace.to_owned(),
                doc_id: doc_id.to_owned(),
            },
            ack,
        )
    }

    /// Durably delete a search document and immediately tombstone it in Tier-0 overlay.
    pub fn delete_document(
        &self,
        namespace: &str,
        doc_id: &str,
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        self.delete_document_coordinated(namespace, doc_id, ack)
            .map(|(c, _)| c)
    }

    /// Atomically revoke all documents matching `(namespace, prefix)` and report secondary sync status.
    pub fn revoke_search_prefix_coordinated(
        &self,
        namespace: &str,
        prefix: &str,
        ack: Ack,
    ) -> Result<(CommitResult, SearchSyncStatus), MembusError> {
        self.append_and_ingest_op(
            SearchLogOp::RevokePrefix {
                namespace: namespace.to_owned(),
                prefix: prefix.to_owned(),
            },
            ack,
        )
    }

    /// Atomically revoke all documents matching `(namespace, prefix)` in Tier-0 + Tantivy,
    /// fenced by DB `epoch` and durably logged in `CF_LOG`.
    pub fn revoke_search_prefix(
        &self,
        namespace: &str,
        prefix: &str,
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        self.revoke_search_prefix_coordinated(namespace, prefix, ack)
            .map(|(c, _)| c)
    }

    fn append_and_ingest_op(
        &self,
        op: SearchLogOp,
        ack: Ack,
    ) -> Result<(CommitResult, SearchSyncStatus), MembusError> {
        let sanitized = sanitize_search_log_op(&op)?;
        let commit = self.storage.append_search_log_op(sanitized.clone(), ack)?;
        let sync = match self.ingest_committed_op(commit.seq, 0, commit.epoch, sanitized) {
            Ok(()) => SearchSyncStatus::Synced,
            Err(err) => {
                self.record_deferred_ingest(commit.seq, &err);
                SearchSyncStatus::Deferred
            }
        };
        Ok((commit, sync))
    }

    /// Ingest an already-logged `SearchLogOp` (tagged with `(seq, op_idx)`) into the 0 ms
    /// Tier-0 RAM overlay and notify the background Tantivy committer.
    ///
    /// Durability & ordering are strictly driven by contiguous `CF_LOG` replay up to
    /// `storage.latest_seq()`, so out-of-order calls to `ingest_committed_op` can never
    /// lose ops, skip lower sequences, or resurrect revoked docs.
    pub fn ingest_committed_op(
        &self,
        seq: u64,
        op_idx: u32,
        epoch: u64,
        op: SearchLogOp,
    ) -> Result<(), MembusError> {
        if epoch != self.epoch {
            return Ok(());
        }
        if consume_fault(&self.fail_ingest_remaining) {
            return Err(MembusError::Storage(
                "injected search ingest failure".to_owned(),
            ));
        }
        let sanitized = sanitize_search_log_op(&op)?;
        self.overlay.apply(seq, op_idx, epoch, &sanitized);

        let should_wait_flush = self.overlay.is_over_budget();
        {
            let (lock, cv) = &*self.worker_state;
            let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
            state.pending_hint_seq = state.pending_hint_seq.max(seq);
            state.pending_count = state.pending_count.saturating_add(1);
            if should_wait_flush {
                state.flush_requested_seq = state.flush_requested_seq.max(seq);
            }
            cv.notify_all();
        }
        if should_wait_flush {
            self.flush_sync()?;
        }
        Ok(())
    }

    /// Synchronously flush all settled `CF_LOG` search mutations into Tantivy and persist checkpoint.
    pub fn flush_sync(&self) -> Result<u64, MembusError> {
        if consume_fault(&self.fail_flush_remaining) {
            return Err(MembusError::Storage(
                "injected search flush failure".to_owned(),
            ));
        }
        let (lock, cv) = &*self.worker_state;
        let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
        let target = state
            .pending_hint_seq
            .max(state.dirty_through_seq)
            .max(self.storage.latest_seq());
        if target <= state.committed_seq {
            if let Some(err) = state.last_error.take() {
                return Err(MembusError::Storage(err));
            }
            maybe_clear_dirty(&state, &self.search_dirty, self.storage.latest_seq());
            return Ok(state.committed_seq);
        }
        state.last_error = None;
        state.flush_requested_seq = state.flush_requested_seq.max(target);
        cv.notify_all();
        while state.committed_seq < target && !state.shutdown {
            if let Some(err) = state.last_error.take() {
                return Err(MembusError::Storage(err));
            }
            state = cv.wait(state).unwrap_or_else(|e| e.into_inner());
        }
        if let Some(err) = state.last_error.take() {
            return Err(MembusError::Storage(err));
        }
        maybe_clear_dirty(&state, &self.search_dirty, self.storage.latest_seq());
        Ok(state.committed_seq)
    }
}
