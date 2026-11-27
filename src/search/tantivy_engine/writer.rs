use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use tantivy::query::TermQuery;
use tantivy::schema::IndexRecordOption;
use tantivy::{IndexReader, IndexWriter, Term};

use crate::search::overlay::{make_doc_key, SanitizedDoc, Tier0Overlay};
use crate::storage::StorageEngine;
use crate::types::{MembusError, SearchLogOp};

use super::collectors::RevokePrefixCollector;
use super::schema::{
    build_tantivy_document, map_tantivy_err, persist_search_checkpoint, SearchSchemaFields,
    LOG_REPLAY_PAGE_LIMIT,
};
use super::state::{maybe_clear_dirty, SearchConfig, WorkerState};

pub(super) struct StagedDoc {
    namespace: String,
    doc_id: String,
    seq: u64,
    op_idx: u32,
}

pub(super) struct OpWriteContext<'a> {
    pub(super) writer: &'a mut IndexWriter,
    pub(super) reader: &'a IndexReader,
    pub(super) fields: SearchSchemaFields,
    pub(super) staged_docs: &'a mut HashMap<String, StagedDoc>,
}

#[derive(Clone, Copy)]
pub(super) struct OpStamp {
    pub(super) seq: u64,
    pub(super) op_idx: u32,
    pub(super) epoch: u64,
}

pub(super) struct CommitWorkerContext {
    pub(super) writer: IndexWriter,
    pub(super) reader: IndexReader,
    pub(super) fields: SearchSchemaFields,
    pub(super) storage: Arc<dyn StorageEngine>,
    pub(super) overlay: Arc<Tier0Overlay>,
    pub(super) committed_seq_atomic: Arc<AtomicU64>,
    pub(super) search_dirty: Arc<AtomicBool>,
    pub(super) worker_state: Arc<(Mutex<WorkerState>, Condvar)>,
    pub(super) epoch: u64,
    pub(super) config: SearchConfig,
}

pub(super) fn spawn_commit_worker(
    mut ctx: CommitWorkerContext,
) -> Result<JoinHandle<()>, MembusError> {
    let commit_interval = Duration::from_millis(ctx.config.commit_interval_ms.max(10));
    let max_buffered = ctx.config.max_buffered_docs.max(1);

    thread::Builder::new()
        .name("membus-tantivy-commit".to_owned())
        .spawn(move || {
            let mut last_commit = Instant::now();
            loop {
                let (want_commit, shutting_down) = wait_for_commit_trigger(
                    &ctx.worker_state,
                    &ctx.search_dirty,
                    max_buffered,
                    commit_interval,
                    last_commit,
                );
                if want_commit {
                    run_commit_step(&mut ctx, shutting_down, &mut last_commit);
                }
                if shutting_down {
                    break;
                }
            }
        })
        .map_err(|err| {
            MembusError::Storage(format!("failed to spawn tantivy commit thread: {err}"))
        })
}

fn wait_for_commit_trigger(
    worker_state: &(Mutex<WorkerState>, Condvar),
    search_dirty: &AtomicBool,
    max_buffered: usize,
    commit_interval: Duration,
    last_commit: Instant,
) -> (bool, bool) {
    let (lock, cv) = worker_state;
    let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
    while !state.shutdown {
        let dirty = search_dirty.load(Ordering::Acquire);
        let has_pending = state.pending_hint_seq > state.committed_seq || dirty;
        let explicit_flush = state.flush_requested_seq > state.committed_seq;
        let count_ready = state.pending_count >= max_buffered;
        let time_ready = has_pending && last_commit.elapsed() >= commit_interval;
        if count_ready || explicit_flush || time_ready {
            break;
        }
        if !has_pending {
            state = cv.wait(state).unwrap_or_else(|e| e.into_inner());
        } else {
            let wait_for = commit_interval.saturating_sub(last_commit.elapsed());
            let (guard, _) = cv
                .wait_timeout(state, wait_for)
                .unwrap_or_else(|e| e.into_inner());
            state = guard;
        }
    }
    let want = state.pending_hint_seq > state.committed_seq
        || state.flush_requested_seq > state.committed_seq
        || state.pending_count > 0
        || search_dirty.load(Ordering::Acquire);
    (want, state.shutdown)
}

fn run_commit_step(
    ctx: &mut CommitWorkerContext,
    shutting_down: bool,
    last_commit: &mut Instant,
) {
    let (lock, cv) = &*ctx.worker_state;
    let current_committed = ctx.committed_seq_atomic.load(Ordering::Acquire);
    let settled_seq = ctx.storage.latest_seq();

    if settled_seq <= current_committed {
        if !shutting_down {
            let state = lock.lock().unwrap_or_else(|e| e.into_inner());
            maybe_clear_dirty(&state, &ctx.search_dirty, settled_seq);
            drop(state);
            // Yield cleanly while an in-flight lower-seq writer finishes committing.
            thread::sleep(Duration::from_millis(1));
        }
        return;
    }

    // Drain contiguous settled `CF_LOG` records strictly in `(seq, op_idx)` order.
    let batch_res = commit_settled_log_batch(
        &mut ctx.writer,
        &ctx.reader,
        ctx.fields,
        ctx.storage.as_ref(),
        ctx.epoch,
        current_committed,
        settled_seq,
    );
    *last_commit = Instant::now();

    match batch_res {
        Ok(new_committed) => {
            // Prune Tier-0 ONLY AFTER `reader.reload()` & checkpoint succeed!
            ctx.committed_seq_atomic
                .store(new_committed, Ordering::Release);
            ctx.overlay.prune_committed(new_committed);

            let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
            state.committed_seq = new_committed;
            if new_committed >= state.pending_hint_seq {
                state.pending_count = 0;
            }
            state.last_error = None;
            maybe_clear_dirty(&state, &ctx.search_dirty, ctx.storage.latest_seq());
            cv.notify_all();
        }
        Err(err) => {
            // Roll back uncommitted Tantivy ops; do NOT advance committed_seq or prune Tier-0!
            let _ = ctx.writer.rollback();
            ctx.search_dirty.store(true, Ordering::Release);
            let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
            state.last_error = Some(err.to_string());
            state.flush_requested_seq = state.committed_seq;
            cv.notify_all();
        }
    }
}

fn commit_settled_log_batch(
    writer: &mut IndexWriter,
    reader: &IndexReader,
    fields: SearchSchemaFields,
    storage: &dyn StorageEngine,
    epoch: u64,
    start_after_seq: u64,
    settled_seq: u64,
) -> Result<u64, MembusError> {
    let mut cursor = start_after_seq;
    let mut staged_docs: HashMap<String, StagedDoc> = HashMap::new();
    let mut applied_any = false;

    while cursor < settled_seq {
        let page = storage.scan_search_log_after(cursor, LOG_REPLAY_PAGE_LIMIT)?;
        for rec in &page.records {
            if rec.epoch != epoch || rec.seq > settled_seq {
                continue;
            }
            let mut op_ctx = OpWriteContext {
                writer,
                reader,
                fields,
                staged_docs: &mut staged_docs,
            };
            let stamp = OpStamp {
                seq: rec.seq,
                op_idx: rec.op_idx,
                epoch: rec.epoch,
            };
            apply_op_to_writer(&mut op_ctx, stamp, &rec.op)?;
            applied_any = true;
        }
        let next_cursor = page.scanned_through_seq.min(settled_seq);
        if next_cursor <= cursor {
            break;
        }
        cursor = next_cursor;
    }

    if applied_any {
        // Durability order: flush RocksDB WAL BEFORE committing Tantivy segments.
        storage.flush_wal(true)?;
        writer.commit().map_err(map_tantivy_err)?;
        reader.reload().map_err(map_tantivy_err)?;
    }
    persist_search_checkpoint(storage, epoch, cursor)?;
    Ok(cursor)
}

pub(super) fn apply_op_to_writer(
    ctx: &mut OpWriteContext<'_>,
    stamp: OpStamp,
    op: &SearchLogOp,
) -> Result<(), MembusError> {
    match op {
        SearchLogOp::Index(doc) => {
            let doc_key = make_doc_key(&doc.namespace, &doc.doc_id);
            ctx.writer
                .delete_term(Term::from_field_text(ctx.fields.doc_key, &doc_key));

            // Strict visibility isolation: Sealed is deleted & never indexed;
            // MetadataOnly strips `body` completely.
            let Some(sanitized) = SanitizedDoc::from_document(doc) else {
                ctx.staged_docs.remove(&doc_key);
                return Ok(());
            };
            let tdoc = build_tantivy_document(
                &sanitized,
                ctx.fields,
                stamp.seq,
                stamp.op_idx,
                stamp.epoch,
            );
            ctx.writer.add_document(tdoc).map_err(map_tantivy_err)?;
            ctx.staged_docs.insert(
                doc_key,
                StagedDoc {
                    namespace: sanitized.namespace,
                    doc_id: sanitized.doc_id,
                    seq: stamp.seq,
                    op_idx: stamp.op_idx,
                },
            );
        }
        SearchLogOp::Delete { namespace, doc_id } => {
            let doc_key = make_doc_key(namespace, doc_id);
            ctx.writer
                .delete_term(Term::from_field_text(ctx.fields.doc_key, &doc_key));
            ctx.staged_docs.remove(&doc_key);
        }
        SearchLogOp::RevokePrefix { namespace, prefix } => {
            // 1. Delete matching docs already committed in Tantivy segments without mid-batch commit.
            let searcher = ctx.reader.searcher();
            let ns_term = Term::from_field_text(ctx.fields.namespace, namespace);
            let ns_query = TermQuery::new(ns_term, IndexRecordOption::Basic);
            let collector = RevokePrefixCollector {
                fields: ctx.fields,
                prefix: prefix.clone(),
                revoke_seq: stamp.seq,
                revoke_op_idx: stamp.op_idx,
            };
            let keys_to_delete = searcher
                .search(&ns_query, &collector)
                .map_err(map_tantivy_err)?;
            for doc_key in keys_to_delete {
                ctx.writer
                    .delete_term(Term::from_field_text(ctx.fields.doc_key, &doc_key));
            }

            // 2. Delete matching docs staged earlier in this uncommitted batch (strict `(seq, op_idx)` order).
            let mut staged_matches = Vec::new();
            for (doc_key, staged) in ctx.staged_docs.iter() {
                if staged.namespace == *namespace
                    && staged.doc_id.starts_with(prefix.as_str())
                    && (staged.seq, staged.op_idx) < (stamp.seq, stamp.op_idx)
                {
                    staged_matches.push(doc_key.clone());
                }
            }
            for doc_key in staged_matches {
                ctx.writer
                    .delete_term(Term::from_field_text(ctx.fields.doc_key, &doc_key));
                ctx.staged_docs.remove(&doc_key);
            }
        }
    }
    Ok(())
}
