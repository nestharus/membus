use std::sync::Arc;

use crate::search::SearchEngine;
use crate::storage::StorageEngine;
use crate::types::{
    Ack, CommitResult, ExpectedVersion, IndexMutations, IndexedMutation, MembusError,
    SearchDocument, SearchLogOp, SearchSyncStatus, VersionGuard,
};

/// Result of an authoritative storage write paired with its secondary search synchronization state.
///
/// Once RocksDB commits authoritatively, secondary search-index ingestion failures NEVER
/// convert the write into a client-visible rejection; instead they are logged, counted,
/// marked dirty for `CF_LOG` catch-up replay, and reported as [`SearchSyncStatus::Deferred`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoordinatedCommit {
    pub commit: CommitResult,
    pub search_sync: SearchSyncStatus,
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn coordinated_put_indexed(
    storage: &Arc<dyn StorageEngine>,
    search: Option<&Arc<SearchEngine>>,
    namespace: &str,
    key: &[u8],
    value: &[u8],
    ack: Ack,
    expected: ExpectedVersion,
    indexes: Option<&IndexMutations>,
    search_op: Option<SearchLogOp>,
) -> Result<CoordinatedCommit, MembusError> {
    let commit = storage.put_indexed(
        namespace,
        key,
        value,
        ack,
        expected,
        indexes,
        search_op.as_ref(),
    )?;
    let search_sync = match (search, search_op) {
        (Some(engine), Some(op)) => {
            ingest_or_defer(engine, commit.seq, 0, commit.epoch, op)
        }
        _ => SearchSyncStatus::Synced,
    };
    Ok(CoordinatedCommit {
        commit,
        search_sync,
    })
}

pub(crate) fn coordinated_remove_indexed(
    storage: &Arc<dyn StorageEngine>,
    search: Option<&Arc<SearchEngine>>,
    namespace: &str,
    key: &[u8],
    ack: Ack,
    expected: ExpectedVersion,
    search_op: Option<SearchLogOp>,
) -> Result<CoordinatedCommit, MembusError> {
    let commit = storage.remove_indexed(namespace, key, ack, expected, search_op.as_ref())?;
    let search_sync = match (search, search_op) {
        (Some(engine), Some(op)) => {
            ingest_or_defer(engine, commit.seq, 0, commit.epoch, op)
        }
        _ => SearchSyncStatus::Synced,
    };
    Ok(CoordinatedCommit {
        commit,
        search_sync,
    })
}

pub(crate) fn coordinated_commit_indexed_tx(
    storage: &Arc<dyn StorageEngine>,
    search: Option<&Arc<SearchEngine>>,
    guards: &[VersionGuard],
    mutations: Vec<IndexedMutation>,
    ack: Ack,
) -> Result<CoordinatedCommit, MembusError> {
    let commit = storage.commit_indexed_tx(guards, &mutations, ack)?;
    let mut search_sync = SearchSyncStatus::Synced;
    if let Some(engine) = search {
        let mut op_idx = 0u32;
        for m in mutations {
            if let Some(op) = m.search_op {
                if ingest_or_defer(engine, commit.seq, op_idx, commit.epoch, op)
                    == SearchSyncStatus::Deferred
                {
                    search_sync = SearchSyncStatus::Deferred;
                }
                op_idx = op_idx.saturating_add(1);
            }
        }
    }
    Ok(CoordinatedCommit {
        commit,
        search_sync,
    })
}

pub(crate) fn coordinated_index_document(
    search: &Arc<SearchEngine>,
    doc: SearchDocument,
    ack: Ack,
) -> Result<CoordinatedCommit, MembusError> {
    let (commit, search_sync) = search.index_document_coordinated(doc, ack)?;
    Ok(CoordinatedCommit {
        commit,
        search_sync,
    })
}

pub(crate) fn coordinated_delete_search_document(
    search: &Arc<SearchEngine>,
    namespace: &str,
    doc_id: &str,
    ack: Ack,
) -> Result<CoordinatedCommit, MembusError> {
    let (commit, search_sync) = search.delete_document_coordinated(namespace, doc_id, ack)?;
    Ok(CoordinatedCommit {
        commit,
        search_sync,
    })
}

pub(crate) fn coordinated_revoke_search_prefix(
    search: &Arc<SearchEngine>,
    namespace: &str,
    prefix: &str,
    ack: Ack,
) -> Result<CoordinatedCommit, MembusError> {
    let (commit, search_sync) = search.revoke_search_prefix_coordinated(namespace, prefix, ack)?;
    Ok(CoordinatedCommit {
        commit,
        search_sync,
    })
}

fn ingest_or_defer(
    engine: &SearchEngine,
    seq: u64,
    op_idx: u32,
    epoch: u64,
    op: SearchLogOp,
) -> SearchSyncStatus {
    match engine.ingest_committed_op(seq, op_idx, epoch, op) {
        Ok(()) => SearchSyncStatus::Synced,
        Err(err) => {
            engine.record_deferred_ingest(seq, &err);
            SearchSyncStatus::Deferred
        }
    }
}
