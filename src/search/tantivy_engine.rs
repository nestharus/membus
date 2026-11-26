use std::cmp::Ordering as CmpOrdering;
use std::collections::{BinaryHeap, HashMap};
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use tantivy::collector::{Collector, SegmentCollector, TopDocs};
use tantivy::directory::MmapDirectory;
use tantivy::query::{AllQuery, BooleanQuery, Occur, Query, QueryParser, TermQuery};
use tantivy::schema::{
    Field, IndexRecordOption, Schema, TantivyDocument, Value, INDEXED, STORED, STRING, TEXT,
};
use tantivy::store::StoreReader;
use tantivy::{DocId, Index, IndexReader, IndexWriter, ReloadPolicy, Score, SegmentOrdinal, SegmentReader, Term};

use crate::search::overlay::{make_doc_key, OverlaySnapshot, SanitizedDoc, Tier0Overlay};
use crate::search::trigram::{contains_exact_substring, extract_trigrams, query_trigrams};
use crate::storage::StorageEngine;
use crate::types::{
    sanitize_score, sanitize_search_log_op, validate_namespace, Ack, CommitResult, MembusError,
    SearchDocument, SearchHit, SearchLogOp, SearchMode, SearchPage, SearchQuery, SearchSyncStatus,
};

const SEARCH_CHECKPOINT_META_KEY: &[u8] = b"search:checkpoint";
const WRITER_HEAP_BYTES: usize = 32 * 1024 * 1024;
const MAX_SEARCH_LIMIT: usize = 1_000;
const LOG_REPLAY_PAGE_LIMIT: usize = 512;

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

#[derive(Clone, Copy)]
struct SearchSchemaFields {
    doc_key: Field,
    namespace: Field,
    doc_id: Field,
    kind: Field,
    title: Field,
    tags: Field,
    tags_raw: Field,
    body: Field,
    trigrams: Field,
    trigram_overflow: Field,
    doc_seq: Field,
    doc_op_idx: Field,
    doc_epoch: Field,
}

fn build_schema() -> (Schema, SearchSchemaFields) {
    let mut builder = Schema::builder();
    let doc_key = builder.add_text_field("doc_key", STRING | STORED);
    let namespace = builder.add_text_field("namespace", STRING | STORED);
    let doc_id = builder.add_text_field("doc_id", STRING | STORED);
    let kind = builder.add_text_field("kind", STRING | STORED);
    let title = builder.add_text_field("title", TEXT | STORED);
    let tags = builder.add_text_field("tags", TEXT);
    let tags_raw = builder.add_text_field("tags_raw", STORED);
    let body = builder.add_text_field("body", TEXT | STORED);
    let trigrams = builder.add_text_field("trigrams", STRING);
    let trigram_overflow = builder.add_u64_field("trigram_overflow", INDEXED | STORED);
    let doc_seq = builder.add_u64_field("doc_seq", INDEXED | STORED);
    let doc_op_idx = builder.add_u64_field("doc_op_idx", INDEXED | STORED);
    let doc_epoch = builder.add_u64_field("doc_epoch", STORED);
    let schema = builder.build();
    let fields = SearchSchemaFields {
        doc_key,
        namespace,
        doc_id,
        kind,
        title,
        tags,
        tags_raw,
        body,
        trigrams,
        trigram_overflow,
        doc_seq,
        doc_op_idx,
        doc_epoch,
    };
    (schema, fields)
}

struct WorkerState {
    pending_hint_seq: u64,
    dirty_through_seq: u64,
    pending_count: usize,
    flush_requested_seq: u64,
    committed_seq: u64,
    last_error: Option<String>,
    shutdown: bool,
}

fn maybe_clear_dirty(state: &WorkerState, search_dirty: &AtomicBool, latest_seq: u64) {
    if state.last_error.is_none()
        && state.committed_seq >= state.dirty_through_seq
        && state.committed_seq >= state.pending_hint_seq
        && state.committed_seq >= latest_seq
    {
        search_dirty.store(false, Ordering::Release);
    }
}

/// Embedded disk-persisted Tantivy search engine with 0 ms Tier-0 RAM live delta overlay,
/// deterministic contiguous `CF_LOG` replay, visibility isolation, and prefix revocation.
pub struct SearchEngine {
    storage: Arc<dyn StorageEngine>,
    index: Index,
    reader: IndexReader,
    fields: SearchSchemaFields,
    epoch: u64,
    overlay: Arc<Tier0Overlay>,
    committed_seq: Arc<AtomicU64>,
    deferred_ingest_total: Arc<AtomicU64>,
    search_dirty: Arc<AtomicBool>,
    fail_ingest_remaining: Arc<AtomicUsize>,
    fail_flush_remaining: Arc<AtomicUsize>,
    worker_state: Arc<(Mutex<WorkerState>, Condvar)>,
    worker_handle: Mutex<Option<JoinHandle<()>>>,
}

impl SearchEngine {
    /// Open or create `<state_dir>/search`, verify log horizon continuity, replay
    /// uncommitted `CF_LOG` search ops from `(epoch, last_indexed_seq)` in strict
    /// `(seq, op_idx)` order, and launch the owned committer.
    pub fn open(
        state_dir: &Path,
        storage: Arc<dyn StorageEngine>,
        config: SearchConfig,
    ) -> Result<Self, MembusError> {
        let search_dir = state_dir.join("search");
        std::fs::create_dir_all(&search_dir)?;

        let (schema, fields) = build_schema();
        let mmap_dir = MmapDirectory::open(&search_dir)
            .map_err(|err| MembusError::Storage(format!("open tantivy dir failed: {err}")))?;
        let index = Index::open_or_create(mmap_dir, schema)
            .map_err(|err| MembusError::Storage(format!("open tantivy index failed: {err}")))?;

        let mut writer = index
            .writer(config.writer_heap_bytes.max(15 * 1024 * 1024))
            .map_err(|err| MembusError::Storage(format!("open tantivy writer failed: {err}")))?;
        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::Manual)
            .try_into()
            .map_err(|err| MembusError::Storage(format!("open tantivy reader failed: {err}")))?;

        let current_epoch = storage.epoch();
        let mut last_indexed_seq = match read_search_checkpoint(storage.as_ref())? {
            Some((saved_epoch, saved_seq)) if saved_epoch == current_epoch => {
                // Fail closed if `CF_LOG` was trimmed past the persisted search checkpoint.
                let horizon = storage.log_horizon_seq();
                if saved_seq < horizon {
                    return Err(MembusError::Gap {
                        earliest_available_seq: horizon.saturating_add(1),
                        current_epoch,
                    });
                }
                saved_seq
            }
            _ => {
                // Fresh DB or epoch change: clear any stale Tantivy state cleanly.
                writer.delete_all_documents().map_err(map_tantivy_err)?;
                writer.commit().map_err(map_tantivy_err)?;
                reader.reload().map_err(map_tantivy_err)?;
                persist_search_checkpoint(storage.as_ref(), current_epoch, 0)?;
                0
            }
        };

        // Deterministically catch up from `CF_LOG` starting strictly after `last_indexed_seq`.
        let mut replayed_any = false;
        let mut staged_docs: HashMap<String, StagedDoc> = HashMap::new();
        loop {
            let page = storage.scan_search_log_after(last_indexed_seq, LOG_REPLAY_PAGE_LIMIT)?;
            if page.records.is_empty() && page.scanned_through_seq <= last_indexed_seq {
                break;
            }
            for rec in &page.records {
                if rec.epoch == current_epoch {
                    apply_op_to_writer(
                        &mut writer,
                        &reader,
                        fields,
                        &mut staged_docs,
                        rec.seq,
                        rec.op_idx,
                        rec.epoch,
                        &rec.op,
                    )?;
                    replayed_any = true;
                }
            }
            if page.scanned_through_seq > last_indexed_seq {
                last_indexed_seq = page.scanned_through_seq;
            } else {
                break;
            }
        }
        if replayed_any {
            // Flush RocksDB WAL BEFORE committing Tantivy so Tantivy never outpaces RocksDB WAL.
            storage.flush_wal(true)?;
            writer.commit().map_err(map_tantivy_err)?;
            reader.reload().map_err(map_tantivy_err)?;
        }
        persist_search_checkpoint(storage.as_ref(), current_epoch, last_indexed_seq)?;

        let overlay = Arc::new(Tier0Overlay::new(current_epoch, config.max_overlay_docs));
        let committed_seq = Arc::new(AtomicU64::new(last_indexed_seq));
        let deferred_ingest_total = Arc::new(AtomicU64::new(0));
        let search_dirty = Arc::new(AtomicBool::new(false));
        let fail_ingest_remaining = Arc::new(AtomicUsize::new(0));
        let fail_flush_remaining = Arc::new(AtomicUsize::new(0));
        let worker_state = Arc::new((
            Mutex::new(WorkerState {
                pending_hint_seq: last_indexed_seq,
                dirty_through_seq: 0,
                pending_count: 0,
                flush_requested_seq: 0,
                committed_seq: last_indexed_seq,
                last_error: None,
                shutdown: false,
            }),
            Condvar::new(),
        ));

        let handle = spawn_commit_worker(
            writer,
            reader.clone(),
            fields,
            Arc::clone(&storage),
            Arc::clone(&overlay),
            Arc::clone(&committed_seq),
            Arc::clone(&search_dirty),
            Arc::clone(&worker_state),
            current_epoch,
            config,
        )?;

        Ok(Self {
            storage,
            index,
            reader,
            fields,
            epoch: current_epoch,
            overlay,
            committed_seq,
            deferred_ingest_total,
            search_dirty,
            fail_ingest_remaining,
            fail_flush_remaining,
            worker_state,
            worker_handle: Mutex::new(Some(handle)),
        })
    }

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

    /// Durably log a visibility-sanitized search document in `CF_LOG` and ingest it into search,
    /// returning [`SearchSyncStatus::Deferred`] instead of rejecting if post-commit ingest fails.
    pub fn index_document_coordinated(
        &self,
        doc: SearchDocument,
        ack: Ack,
    ) -> Result<(CommitResult, SearchSyncStatus), MembusError> {
        let sanitized = sanitize_search_log_op(&SearchLogOp::Index(doc))?;
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
        let sanitized = sanitize_search_log_op(&SearchLogOp::Delete {
            namespace: namespace.to_owned(),
            doc_id: doc_id.to_owned(),
        })?;
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
        let sanitized = sanitize_search_log_op(&SearchLogOp::RevokePrefix {
            namespace: namespace.to_owned(),
            prefix: prefix.to_owned(),
        })?;
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

    /// Execute a BM25 or exact case-sensitive literal substring search merging Tier-0 RAM
    /// live delta entries and tombstones over committed Tantivy segments (0 ms read-your-writes).
    pub fn search(&self, query: &SearchQuery) -> Result<SearchPage, MembusError> {
        if query.limit == 0 || query.limit > MAX_SEARCH_LIMIT {
            return Err(MembusError::InvalidArgument(format!(
                "search limit {} must be in 1..={MAX_SEARCH_LIMIT}",
                query.limit
            )));
        }
        if let Some(ns) = &query.namespace {
            validate_namespace(ns)?;
        }
        if query.query.is_empty() {
            return Ok(SearchPage {
                hits: Vec::new(),
                total_hits_estimate: 0,
                committed_seq: self.committed_seq(),
            });
        }

        // Capture Tier-0 overlay snapshot BEFORE acquiring Tantivy searcher so any doc pruned
        // from overlay has already been reloaded into the searcher (zero commit-boundary gap).
        let overlay_snap = Arc::new(self.overlay.snapshot());
        let searcher = self.reader.searcher();
        let committed_seq = self.committed_seq();

        let mut merged: HashMap<String, SearchHit> = HashMap::new();

        // 1. Evaluate committed Tantivy segments.
        let tantivy_matched_estimate: usize = match query.mode {
            SearchMode::Bm25 => {
                let mut parser = QueryParser::for_index(
                    &self.index,
                    vec![self.fields.title, self.fields.tags, self.fields.body],
                );
                parser.set_field_boost(self.fields.title, 3.0);
                parser.set_field_boost(self.fields.tags, 2.0);
                parser.set_field_boost(self.fields.body, 1.0);
                let (text_query, _parse_errs) = parser.parse_query_lenient(&query.query);
                let combined = wrap_filters(text_query, self.fields, query);

                if overlay_snap.is_empty() {
                    let top_docs = searcher
                        .search(&combined, &TopDocs::with_limit(query.limit))
                        .map_err(map_tantivy_err)?;
                    let matched = top_docs.len();
                    for (score, addr) in top_docs {
                        let doc: TantivyDocument = searcher.doc(addr).map_err(map_tantivy_err)?;
                        let Some(extracted) = extract_stored_doc(&doc, self.fields) else {
                            continue;
                        };
                        merged.insert(
                            extracted.doc_key,
                            SearchHit {
                                namespace: extracted.namespace,
                                doc_id: extracted.doc_id,
                                kind: extracted.kind,
                                title: extracted.title,
                                tags: extracted.tags,
                                score: sanitize_score(score),
                            },
                        );
                    }
                    matched
                } else {
                    // Filtered streaming collector prevents recall collapse under RevokePrefix / tombstones.
                    let collector = FilteredBm25Collector {
                        fields: self.fields,
                        overlay: Arc::clone(&overlay_snap),
                        limit: query.limit,
                    };
                    let (hits_vec, total_unshadowed) =
                        searcher.search(&combined, &collector).map_err(map_tantivy_err)?;
                    for (doc_key, hit) in hits_vec {
                        merged.insert(doc_key, hit);
                    }
                    total_unshadowed
                }
            }
            SearchMode::LiteralSubstring => {
                let grams = query_trigrams(&query.query);
                let base_query: Box<dyn Query> = if !grams.is_empty() {
                    let gram_clauses: Vec<(Occur, Box<dyn Query>)> = grams
                        .into_iter()
                        .map(|g| {
                            let term = Term::from_field_text(self.fields.trigrams, &g);
                            (
                                Occur::Must,
                                Box::new(TermQuery::new(term, IndexRecordOption::Basic))
                                    as Box<dyn Query>,
                            )
                        })
                        .collect();
                    let all_grams = BooleanQuery::new(gram_clauses);
                    let overflow_term = Term::from_field_u64(self.fields.trigram_overflow, 1);
                    let overflow_query =
                        TermQuery::new(overflow_term, IndexRecordOption::Basic);
                    Box::new(BooleanQuery::new(vec![
                        (Occur::Should, Box::new(all_grams)),
                        (Occur::Should, Box::new(overflow_query)),
                    ]))
                } else {
                    Box::new(AllQuery)
                };
                let combined = wrap_filters(base_query, self.fields, query);

                // Streaming exact-substring verification collector (ZERO 10k unverified candidate cap).
                let collector = LiteralSubstringCollector {
                    fields: self.fields,
                    overlay: Arc::clone(&overlay_snap),
                    needle: query.query.clone(),
                    limit: query.limit,
                };
                let (hits_vec, total_verified) =
                    searcher.search(&combined, &collector).map_err(map_tantivy_err)?;
                for (doc_key, hit) in hits_vec {
                    merged.insert(doc_key, hit);
                }
                total_verified
            }
        };

        // 2. Merge live Tier-0 RAM hits (overriding any stale Tantivy version).
        let live_hits = overlay_snap.evaluate_live_hits(query);
        let live_extra = live_hits
            .iter()
            .filter(|(k, _)| !merged.contains_key(k))
            .count();
        for (doc_key, hit) in live_hits {
            merged.insert(doc_key, hit);
        }

        let total_hits_estimate = tantivy_matched_estimate.saturating_add(live_extra).max(merged.len());
        let mut hits: Vec<SearchHit> = merged.into_values().collect();
        hits.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.namespace.cmp(&b.namespace))
                .then_with(|| a.doc_id.cmp(&b.doc_id))
        });
        hits.truncate(query.limit);

        Ok(SearchPage {
            hits,
            total_hits_estimate,
            committed_seq,
        })
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

struct StagedDoc {
    namespace: String,
    doc_id: String,
    seq: u64,
    op_idx: u32,
}

fn consume_fault(counter: &AtomicUsize) -> bool {
    let mut cur = counter.load(Ordering::Acquire);
    while cur > 0 {
        match counter.compare_exchange_weak(cur, cur - 1, Ordering::AcqRel, Ordering::Acquire) {
            Ok(_) => return true,
            Err(actual) => cur = actual,
        }
    }
    false
}

#[allow(clippy::too_many_arguments)]
fn spawn_commit_worker(
    mut writer: IndexWriter,
    reader: IndexReader,
    fields: SearchSchemaFields,
    storage: Arc<dyn StorageEngine>,
    overlay: Arc<Tier0Overlay>,
    committed_seq_atomic: Arc<AtomicU64>,
    search_dirty: Arc<AtomicBool>,
    worker_state: Arc<(Mutex<WorkerState>, Condvar)>,
    epoch: u64,
    config: SearchConfig,
) -> Result<JoinHandle<()>, MembusError> {
    let commit_interval = Duration::from_millis(config.commit_interval_ms.max(10));
    let max_buffered = config.max_buffered_docs.max(1);

    thread::Builder::new()
        .name("membus-tantivy-commit".to_owned())
        .spawn(move || {
            let (lock, cv) = &*worker_state;
            let mut last_commit = Instant::now();
            loop {
                let (want_commit, shutting_down) = {
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
                            let wait_for =
                                commit_interval.saturating_sub(last_commit.elapsed());
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
                };

                if want_commit {
                    let current_committed = committed_seq_atomic.load(Ordering::Acquire);
                    let settled_seq = storage.latest_seq();

                    if settled_seq <= current_committed {
                        if !shutting_down {
                            let state = lock.lock().unwrap_or_else(|e| e.into_inner());
                            maybe_clear_dirty(&state, &search_dirty, settled_seq);
                            drop(state);
                            // Yield cleanly while an in-flight lower-seq writer finishes committing.
                            thread::sleep(Duration::from_millis(1));
                            continue;
                        }
                    } else {
                        // Drain contiguous settled `CF_LOG` records strictly in `(seq, op_idx)` order.
                        let batch_res = commit_settled_log_batch(
                            &mut writer,
                            &reader,
                            fields,
                            storage.as_ref(),
                            epoch,
                            current_committed,
                            settled_seq,
                        );
                        last_commit = Instant::now();

                        match batch_res {
                            Ok(new_committed) => {
                                // Prune Tier-0 ONLY AFTER `reader.reload()` & checkpoint succeed!
                                committed_seq_atomic.store(new_committed, Ordering::Release);
                                overlay.prune_committed(new_committed);

                                let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
                                state.committed_seq = new_committed;
                                if new_committed >= state.pending_hint_seq {
                                    state.pending_count = 0;
                                }
                                state.last_error = None;
                                maybe_clear_dirty(&state, &search_dirty, storage.latest_seq());
                                cv.notify_all();
                            }
                            Err(err) => {
                                // Roll back uncommitted Tantivy ops; do NOT advance committed_seq or prune Tier-0!
                                let _ = writer.rollback();
                                search_dirty.store(true, Ordering::Release);
                                let mut state = lock.lock().unwrap_or_else(|e| e.into_inner());
                                state.last_error = Some(err.to_string());
                                state.flush_requested_seq = state.committed_seq;
                                cv.notify_all();
                            }
                        }
                    }
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
            apply_op_to_writer(
                writer,
                reader,
                fields,
                &mut staged_docs,
                rec.seq,
                rec.op_idx,
                rec.epoch,
                &rec.op,
            )?;
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

#[allow(clippy::too_many_arguments)]
fn apply_op_to_writer(
    writer: &mut IndexWriter,
    reader: &IndexReader,
    fields: SearchSchemaFields,
    staged_docs: &mut HashMap<String, StagedDoc>,
    seq: u64,
    op_idx: u32,
    epoch: u64,
    op: &SearchLogOp,
) -> Result<(), MembusError> {
    match op {
        SearchLogOp::Index(doc) => {
            let doc_key = make_doc_key(&doc.namespace, &doc.doc_id);
            writer.delete_term(Term::from_field_text(fields.doc_key, &doc_key));

            // Strict visibility isolation: Sealed is deleted & never indexed;
            // MetadataOnly strips `body` completely.
            let Some(sanitized) = SanitizedDoc::from_document(doc) else {
                staged_docs.remove(&doc_key);
                return Ok(());
            };
            let trigrams = extract_trigrams(&[
                &sanitized.title,
                &sanitized.tags_joined,
                &sanitized.body,
            ]);

            let mut tdoc = TantivyDocument::default();
            tdoc.add_text(fields.doc_key, &sanitized.doc_key);
            tdoc.add_text(fields.namespace, &sanitized.namespace);
            tdoc.add_text(fields.doc_id, &sanitized.doc_id);
            tdoc.add_text(fields.kind, &sanitized.kind);
            tdoc.add_text(fields.title, &sanitized.title);
            tdoc.add_text(fields.tags, &sanitized.tags_joined);
            let tags_json = serde_json::to_string(&sanitized.tags).unwrap_or_else(|_| "[]".into());
            tdoc.add_text(fields.tags_raw, &tags_json);
            tdoc.add_text(fields.body, &sanitized.body);
            for gram in &trigrams.grams {
                tdoc.add_text(fields.trigrams, gram);
            }
            tdoc.add_u64(fields.trigram_overflow, u64::from(trigrams.overflow));
            tdoc.add_u64(fields.doc_seq, seq);
            tdoc.add_u64(fields.doc_op_idx, u64::from(op_idx));
            tdoc.add_u64(fields.doc_epoch, epoch);
            writer.add_document(tdoc).map_err(map_tantivy_err)?;
            staged_docs.insert(
                doc_key,
                StagedDoc {
                    namespace: sanitized.namespace,
                    doc_id: sanitized.doc_id,
                    seq,
                    op_idx,
                },
            );
        }
        SearchLogOp::Delete { namespace, doc_id } => {
            let doc_key = make_doc_key(namespace, doc_id);
            writer.delete_term(Term::from_field_text(fields.doc_key, &doc_key));
            staged_docs.remove(&doc_key);
        }
        SearchLogOp::RevokePrefix { namespace, prefix } => {
            // 1. Delete matching docs already committed in Tantivy segments without mid-batch commit.
            let searcher = reader.searcher();
            let ns_term = Term::from_field_text(fields.namespace, namespace);
            let ns_query = TermQuery::new(ns_term, IndexRecordOption::Basic);
            let collector = RevokePrefixCollector {
                fields,
                prefix: prefix.clone(),
                revoke_seq: seq,
                revoke_op_idx: op_idx,
            };
            let keys_to_delete = searcher.search(&ns_query, &collector).map_err(map_tantivy_err)?;
            for doc_key in keys_to_delete {
                writer.delete_term(Term::from_field_text(fields.doc_key, &doc_key));
            }

            // 2. Delete matching docs staged earlier in this uncommitted batch (strict `(seq, op_idx)` order).
            let mut staged_matches = Vec::new();
            for (doc_key, staged) in staged_docs.iter() {
                if staged.namespace == *namespace
                    && staged.doc_id.starts_with(prefix.as_str())
                    && (staged.seq, staged.op_idx) < (seq, op_idx)
                {
                    staged_matches.push(doc_key.clone());
                }
            }
            for doc_key in staged_matches {
                writer.delete_term(Term::from_field_text(fields.doc_key, &doc_key));
                staged_docs.remove(&doc_key);
            }
        }
    }
    Ok(())
}

/// Min-heap wrapper ordering the *worst* hit at the top so a bounded heap of size `limit`
/// retains the top-K hits by `(score desc, namespace asc, doc_id asc)`.
struct HeapHit {
    doc_key: String,
    hit: SearchHit,
}

impl PartialEq for HeapHit {
    fn eq(&self, other: &Self) -> bool {
        self.hit.score.to_bits() == other.hit.score.to_bits()
            && self.hit.namespace == other.hit.namespace
            && self.hit.doc_id == other.hit.doc_id
    }
}

impl Eq for HeapHit {}

impl PartialOrd for HeapHit {
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapHit {
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // Reversed so BinaryHeap (max-heap) pops the *worst* candidate first.
        other
            .hit
            .score
            .total_cmp(&self.hit.score)
            .then_with(|| self.hit.namespace.cmp(&other.hit.namespace))
            .then_with(|| self.hit.doc_id.cmp(&other.hit.doc_id))
    }
}

fn push_bounded_hit(heap: &mut BinaryHeap<HeapHit>, limit: usize, candidate: HeapHit) {
    if heap.len() < limit {
        heap.push(candidate);
    } else if let Some(worst) = heap.peek() {
        // Candidate is better than worst when `candidate < worst` under reversed `Ord`.
        if candidate < *worst {
            heap.pop();
            heap.push(candidate);
        }
    }
}

/// Streaming collector for `RevokePrefix` with ZERO hard document cap.
struct RevokePrefixCollector {
    fields: SearchSchemaFields,
    prefix: String,
    revoke_seq: u64,
    revoke_op_idx: u32,
}

struct RevokePrefixSegmentCollector {
    fields: SearchSchemaFields,
    prefix: String,
    revoke_seq: u64,
    revoke_op_idx: u32,
    store_reader: StoreReader,
    matched_keys: Vec<String>,
    error: Option<String>,
}

impl Collector for RevokePrefixCollector {
    type Fruit = Vec<String>;
    type Child = RevokePrefixSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
        segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let store_reader = segment.get_store_reader(8)?;
        Ok(RevokePrefixSegmentCollector {
            fields: self.fields,
            prefix: self.prefix.clone(),
            revoke_seq: self.revoke_seq,
            revoke_op_idx: self.revoke_op_idx,
            store_reader,
            matched_keys: Vec::new(),
            error: None,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<Result<Vec<String>, String>>,
    ) -> tantivy::Result<Self::Fruit> {
        let mut out = Vec::new();
        for res in segment_fruits {
            match res {
                Ok(mut keys) => out.append(&mut keys),
                Err(msg) => return Err(tantivy::TantivyError::SystemError(msg)),
            }
        }
        Ok(out)
    }
}

impl SegmentCollector for RevokePrefixSegmentCollector {
    type Fruit = Result<Vec<String>, String>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        if self.error.is_some() {
            return;
        }
        match self.store_reader.get::<TantivyDocument>(doc) {
            Ok(tdoc) => {
                if let Some(extracted) = extract_stored_doc(&tdoc, self.fields) {
                    if extracted.doc_id.starts_with(self.prefix.as_str())
                        && (extracted.doc_seq, extracted.doc_op_idx)
                            < (self.revoke_seq, self.revoke_op_idx)
                    {
                        self.matched_keys.push(extracted.doc_key);
                    }
                }
            }
            Err(err) => {
                self.error = Some(err.to_string());
            }
        }
    }

    fn harvest(self) -> Self::Fruit {
        if let Some(err) = self.error {
            Err(err)
        } else {
            Ok(self.matched_keys)
        }
    }
}

/// Streaming collector for `SearchMode::Bm25` when Tier-0 overlay has pending updates,
/// tombstones, or prefix revocations — prevents recall collapse with O(limit) memory.
struct FilteredBm25Collector {
    fields: SearchSchemaFields,
    overlay: Arc<OverlaySnapshot>,
    limit: usize,
}

struct FilteredBm25SegmentCollector {
    fields: SearchSchemaFields,
    overlay: Arc<OverlaySnapshot>,
    limit: usize,
    store_reader: StoreReader,
    heap: BinaryHeap<HeapHit>,
    matched_count: usize,
    error: Option<String>,
}

impl Collector for FilteredBm25Collector {
    type Fruit = (Vec<(String, SearchHit)>, usize);
    type Child = FilteredBm25SegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
        segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let store_reader = segment.get_store_reader(8)?;
        Ok(FilteredBm25SegmentCollector {
            fields: self.fields,
            overlay: Arc::clone(&self.overlay),
            limit: self.limit,
            store_reader,
            heap: BinaryHeap::with_capacity(self.limit.min(1024)),
            matched_count: 0,
            error: None,
        })
    }

    fn requires_scoring(&self) -> bool {
        true
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<Result<(Vec<HeapHit>, usize), String>>,
    ) -> tantivy::Result<Self::Fruit> {
        let mut merged_heap: BinaryHeap<HeapHit> = BinaryHeap::with_capacity(self.limit.min(1024));
        let mut total = 0usize;
        for res in segment_fruits {
            match res {
                Ok((items, count)) => {
                    total = total.saturating_add(count);
                    for item in items {
                        push_bounded_hit(&mut merged_heap, self.limit, item);
                    }
                }
                Err(msg) => return Err(tantivy::TantivyError::SystemError(msg)),
            }
        }
        let hits = merged_heap
            .into_iter()
            .map(|h| (h.doc_key, h.hit))
            .collect();
        Ok((hits, total))
    }
}

impl SegmentCollector for FilteredBm25SegmentCollector {
    type Fruit = Result<(Vec<HeapHit>, usize), String>;

    fn collect(&mut self, doc: DocId, raw_score: Score) {
        if self.error.is_some() {
            return;
        }
        let score = sanitize_score(raw_score);
        // Fast lower-bound prune BEFORE reading stored document when heap is already full.
        if self.heap.len() >= self.limit {
            if let Some(worst) = self.heap.peek() {
                if score < worst.hit.score {
                    self.matched_count = self.matched_count.saturating_add(1);
                    return;
                }
            }
        }
        match self.store_reader.get::<TantivyDocument>(doc) {
            Ok(tdoc) => {
                let Some(extracted) = extract_stored_doc(&tdoc, self.fields) else {
                    return;
                };
                if self.overlay.shadows_tantivy_hit(
                    &extracted.doc_key,
                    &extracted.namespace,
                    &extracted.doc_id,
                    extracted.doc_seq,
                    extracted.doc_op_idx,
                ) {
                    return;
                }
                self.matched_count = self.matched_count.saturating_add(1);
                push_bounded_hit(
                    &mut self.heap,
                    self.limit,
                    HeapHit {
                        doc_key: extracted.doc_key,
                        hit: SearchHit {
                            namespace: extracted.namespace,
                            doc_id: extracted.doc_id,
                            kind: extracted.kind,
                            title: extracted.title,
                            tags: extracted.tags,
                            score,
                        },
                    },
                );
            }
            Err(err) => {
                self.error = Some(err.to_string());
            }
        }
    }

    fn harvest(self) -> Self::Fruit {
        if let Some(err) = self.error {
            Err(err)
        } else {
            Ok((self.heap.into_vec(), self.matched_count))
        }
    }
}

/// Streaming exact-substring collector for `SearchMode::LiteralSubstring` with ZERO 10k cap.
struct LiteralSubstringCollector {
    fields: SearchSchemaFields,
    overlay: Arc<OverlaySnapshot>,
    needle: String,
    limit: usize,
}

struct LiteralSubstringSegmentCollector {
    fields: SearchSchemaFields,
    overlay: Arc<OverlaySnapshot>,
    needle: String,
    limit: usize,
    store_reader: StoreReader,
    heap: BinaryHeap<HeapHit>,
    matched_count: usize,
    error: Option<String>,
}

impl Collector for LiteralSubstringCollector {
    type Fruit = (Vec<(String, SearchHit)>, usize);
    type Child = LiteralSubstringSegmentCollector;

    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
        segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        let store_reader = segment.get_store_reader(8)?;
        Ok(LiteralSubstringSegmentCollector {
            fields: self.fields,
            overlay: Arc::clone(&self.overlay),
            needle: self.needle.clone(),
            limit: self.limit,
            store_reader,
            heap: BinaryHeap::with_capacity(self.limit.min(1024)),
            matched_count: 0,
            error: None,
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<Result<(Vec<HeapHit>, usize), String>>,
    ) -> tantivy::Result<Self::Fruit> {
        let mut merged_heap: BinaryHeap<HeapHit> = BinaryHeap::with_capacity(self.limit.min(1024));
        let mut total = 0usize;
        for res in segment_fruits {
            match res {
                Ok((items, count)) => {
                    total = total.saturating_add(count);
                    for item in items {
                        push_bounded_hit(&mut merged_heap, self.limit, item);
                    }
                }
                Err(msg) => return Err(tantivy::TantivyError::SystemError(msg)),
            }
        }
        let hits = merged_heap
            .into_iter()
            .map(|h| (h.doc_key, h.hit))
            .collect();
        Ok((hits, total))
    }
}

impl SegmentCollector for LiteralSubstringSegmentCollector {
    type Fruit = Result<(Vec<HeapHit>, usize), String>;

    fn collect(&mut self, doc: DocId, _score: Score) {
        if self.error.is_some() {
            return;
        }
        match self.store_reader.get::<TantivyDocument>(doc) {
            Ok(tdoc) => {
                let Some(extracted) = extract_stored_doc(&tdoc, self.fields) else {
                    return;
                };
                if self.overlay.shadows_tantivy_hit(
                    &extracted.doc_key,
                    &extracted.namespace,
                    &extracted.doc_id,
                    extracted.doc_seq,
                    extracted.doc_op_idx,
                ) {
                    return;
                }
                let tags_joined = extracted.tags.join(" ");
                if !contains_exact_substring(
                    &self.needle,
                    &[&extracted.title, &tags_joined, &extracted.body],
                ) {
                    return;
                }
                let mut score = 0.0f32;
                if extracted.title.contains(&self.needle) {
                    score += 3.0;
                }
                if tags_joined.contains(&self.needle) {
                    score += 2.0;
                }
                if extracted.body.contains(&self.needle) {
                    score += 1.0;
                }
                self.matched_count = self.matched_count.saturating_add(1);
                push_bounded_hit(
                    &mut self.heap,
                    self.limit,
                    HeapHit {
                        doc_key: extracted.doc_key,
                        hit: SearchHit {
                            namespace: extracted.namespace,
                            doc_id: extracted.doc_id,
                            kind: extracted.kind,
                            title: extracted.title,
                            tags: extracted.tags,
                            score: sanitize_score(score),
                        },
                    },
                );
            }
            Err(err) => {
                self.error = Some(err.to_string());
            }
        }
    }

    fn harvest(self) -> Self::Fruit {
        if let Some(err) = self.error {
            Err(err)
        } else {
            Ok((self.heap.into_vec(), self.matched_count))
        }
    }
}

fn wrap_filters(
    inner: Box<dyn Query>,
    fields: SearchSchemaFields,
    query: &SearchQuery,
) -> BooleanQuery {
    let mut clauses: Vec<(Occur, Box<dyn Query>)> = vec![(Occur::Must, inner)];
    if let Some(ns) = &query.namespace {
        let term = Term::from_field_text(fields.namespace, ns);
        clauses.push((
            Occur::Must,
            Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
        ));
    }
    if let Some(kind) = &query.kind {
        let term = Term::from_field_text(fields.kind, kind);
        clauses.push((
            Occur::Must,
            Box::new(TermQuery::new(term, IndexRecordOption::Basic)),
        ));
    }
    BooleanQuery::new(clauses)
}

struct StoredSearchDoc {
    doc_key: String,
    namespace: String,
    doc_id: String,
    kind: String,
    title: String,
    tags: Vec<String>,
    body: String,
    doc_seq: u64,
    doc_op_idx: u32,
}

fn extract_stored_doc(
    doc: &TantivyDocument,
    fields: SearchSchemaFields,
) -> Option<StoredSearchDoc> {
    let doc_key = doc.get_first(fields.doc_key)?.as_str()?.to_owned();
    let namespace = doc.get_first(fields.namespace)?.as_str()?.to_owned();
    let doc_id = doc.get_first(fields.doc_id)?.as_str()?.to_owned();
    let kind = doc
        .get_first(fields.kind)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_owned();
    let title = doc
        .get_first(fields.title)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_owned();
    let tags: Vec<String> = doc
        .get_first(fields.tags_raw)
        .and_then(|v| v.as_str())
        .and_then(|raw| serde_json::from_str(raw).ok())
        .unwrap_or_default();
    let body = doc
        .get_first(fields.body)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_owned();
    let doc_seq = doc
        .get_first(fields.doc_seq)
        .and_then(|v| v.as_u64())
        .unwrap_or(0);
    let doc_op_idx = doc
        .get_first(fields.doc_op_idx)
        .and_then(|v| v.as_u64())
        .and_then(|v| u32::try_from(v).ok())
        .unwrap_or(0);
    Some(StoredSearchDoc {
        doc_key,
        namespace,
        doc_id,
        kind,
        title,
        tags,
        body,
        doc_seq,
        doc_op_idx,
    })
}

fn read_search_checkpoint(storage: &dyn StorageEngine) -> Result<Option<(u64, u64)>, MembusError> {
    let Some(bytes) = storage.meta_get(SEARCH_CHECKPOINT_META_KEY)? else {
        return Ok(None);
    };
    if bytes.len() != 16 {
        return Err(MembusError::Corruption(format!(
            "search checkpoint expected 16 bytes, got {}",
            bytes.len()
        )));
    }
    let epoch = u64::from_be_bytes(bytes[0..8].try_into().unwrap());
    let seq = u64::from_be_bytes(bytes[8..16].try_into().unwrap());
    Ok(Some((epoch, seq)))
}

fn persist_search_checkpoint(
    storage: &dyn StorageEngine,
    epoch: u64,
    seq: u64,
) -> Result<(), MembusError> {
    let mut buf = [0u8; 16];
    buf[0..8].copy_from_slice(&epoch.to_be_bytes());
    buf[8..16].copy_from_slice(&seq.to_be_bytes());
    storage.meta_put(SEARCH_CHECKPOINT_META_KEY, &buf, true)
}

fn map_tantivy_err(err: tantivy::TantivyError) -> MembusError {
    MembusError::Storage(format!("tantivy error: {err}"))
}
