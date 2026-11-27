mod collectors;
mod ingest;
mod query;
mod schema;
mod state;
mod writer;

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize};
use std::sync::{Arc, Condvar, Mutex};

use tantivy::directory::MmapDirectory;
use tantivy::{Index, IndexReader, IndexWriter, ReloadPolicy};

use crate::search::overlay::Tier0Overlay;
use crate::storage::StorageEngine;
use crate::types::MembusError;

pub use state::{SearchConfig, SearchEngine};

use writer::{
    apply_op_to_writer, spawn_commit_worker, CommitWorkerContext, OpStamp, OpWriteContext,
    StagedDoc,
};
use schema::{
    build_schema, map_tantivy_err, persist_search_checkpoint, read_search_checkpoint,
    SearchSchemaFields, LOG_REPLAY_PAGE_LIMIT,
};
use state::WorkerState;

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
        let start_seq =
            resolve_initial_checkpoint(storage.as_ref(), &mut writer, &reader, current_epoch)?;
        let last_indexed_seq = replay_startup_log(
            storage.as_ref(),
            &mut writer,
            &reader,
            fields,
            current_epoch,
            start_seq,
        )?;

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

        let handle = spawn_commit_worker(CommitWorkerContext {
            writer,
            reader: reader.clone(),
            fields,
            storage: Arc::clone(&storage),
            overlay: Arc::clone(&overlay),
            committed_seq_atomic: Arc::clone(&committed_seq),
            search_dirty: Arc::clone(&search_dirty),
            worker_state: Arc::clone(&worker_state),
            epoch: current_epoch,
            config,
        })?;

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
}

fn resolve_initial_checkpoint(
    storage: &dyn StorageEngine,
    writer: &mut IndexWriter,
    reader: &IndexReader,
    current_epoch: u64,
) -> Result<u64, MembusError> {
    match read_search_checkpoint(storage)? {
        Some((saved_epoch, saved_seq)) if saved_epoch == current_epoch => {
            // Fail closed if `CF_LOG` was trimmed past the persisted search checkpoint.
            let horizon = storage.log_horizon_seq();
            if saved_seq < horizon {
                return Err(MembusError::Gap {
                    earliest_available_seq: horizon.saturating_add(1),
                    current_epoch,
                });
            }
            Ok(saved_seq)
        }
        _ => {
            // Fresh DB or epoch change: clear any stale Tantivy state cleanly.
            writer.delete_all_documents().map_err(map_tantivy_err)?;
            writer.commit().map_err(map_tantivy_err)?;
            reader.reload().map_err(map_tantivy_err)?;
            persist_search_checkpoint(storage, current_epoch, 0)?;
            Ok(0)
        }
    }
}

fn replay_startup_log(
    storage: &dyn StorageEngine,
    writer: &mut IndexWriter,
    reader: &IndexReader,
    fields: SearchSchemaFields,
    current_epoch: u64,
    start_seq: u64,
) -> Result<u64, MembusError> {
    let mut last_indexed_seq = start_seq;
    let mut replayed_any = false;
    let mut staged_docs: HashMap<String, StagedDoc> = HashMap::new();
    loop {
        let page = storage.scan_search_log_after(last_indexed_seq, LOG_REPLAY_PAGE_LIMIT)?;
        if page.records.is_empty() && page.scanned_through_seq <= last_indexed_seq {
            break;
        }
        for rec in &page.records {
            if rec.epoch == current_epoch {
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
    persist_search_checkpoint(storage, current_epoch, last_indexed_seq)?;
    Ok(last_indexed_seq)
}
