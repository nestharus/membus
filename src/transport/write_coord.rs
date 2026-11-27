use std::sync::Arc;

use crate::search::SearchEngine;
use crate::storage::StorageEngine;
use crate::types::{
    Ack, CommitResult, ExpectedVersion, IdempotencyContext, IndexMutations, IndexedMutation,
    MembusError, ReplicatedApplyResult, ReplicatedDelta, SearchDocument, SearchLogOp,
    SearchSyncStatus, VersionGuard,
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
    pub idempotent_replay: bool,
}

/// Result of applying a replicated delta paired with echo-loop and secondary search status.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CoordinatedReplicatedApply {
    pub result: ReplicatedApplyResult,
    pub echo_rejected: bool,
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
    coordinated_put_indexed_idempotent(
        storage, search, namespace, key, value, ack, expected, indexes, search_op, None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn coordinated_put_indexed_idempotent(
    storage: &Arc<dyn StorageEngine>,
    search: Option<&Arc<SearchEngine>>,
    namespace: &str,
    key: &[u8],
    value: &[u8],
    ack: Ack,
    expected: ExpectedVersion,
    indexes: Option<&IndexMutations>,
    search_op: Option<SearchLogOp>,
    idempotency: Option<&IdempotencyContext>,
) -> Result<CoordinatedCommit, MembusError> {
    let (commit, replayed) = storage.put_indexed_idempotent(
        namespace,
        key,
        value,
        ack,
        expected,
        indexes,
        search_op.as_ref(),
        idempotency,
    )?;
    let search_sync = if replayed {
        SearchSyncStatus::Synced
    } else {
        match (search, search_op) {
            (Some(engine), Some(op)) => ingest_or_defer(engine, commit.seq, 0, commit.epoch, op),
            _ => SearchSyncStatus::Synced,
        }
    };
    Ok(CoordinatedCommit {
        commit,
        search_sync,
        idempotent_replay: replayed,
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
    coordinated_remove_indexed_idempotent(
        storage, search, namespace, key, ack, expected, search_op, None,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn coordinated_remove_indexed_idempotent(
    storage: &Arc<dyn StorageEngine>,
    search: Option<&Arc<SearchEngine>>,
    namespace: &str,
    key: &[u8],
    ack: Ack,
    expected: ExpectedVersion,
    search_op: Option<SearchLogOp>,
    idempotency: Option<&IdempotencyContext>,
) -> Result<CoordinatedCommit, MembusError> {
    let (commit, replayed) = storage.remove_indexed_idempotent(
        namespace,
        key,
        ack,
        expected,
        search_op.as_ref(),
        idempotency,
    )?;
    let search_sync = if replayed {
        SearchSyncStatus::Synced
    } else {
        match (search, search_op) {
            (Some(engine), Some(op)) => ingest_or_defer(engine, commit.seq, 0, commit.epoch, op),
            _ => SearchSyncStatus::Synced,
        }
    };
    Ok(CoordinatedCommit {
        commit,
        search_sync,
        idempotent_replay: replayed,
    })
}

pub(crate) fn coordinated_commit_indexed_tx(
    storage: &Arc<dyn StorageEngine>,
    search: Option<&Arc<SearchEngine>>,
    guards: &[VersionGuard],
    mutations: Vec<IndexedMutation>,
    ack: Ack,
) -> Result<CoordinatedCommit, MembusError> {
    coordinated_commit_indexed_tx_idempotent(storage, search, guards, mutations, ack, None)
}

pub(crate) fn coordinated_commit_indexed_tx_idempotent(
    storage: &Arc<dyn StorageEngine>,
    search: Option<&Arc<SearchEngine>>,
    guards: &[VersionGuard],
    mutations: Vec<IndexedMutation>,
    ack: Ack,
    idempotency: Option<&IdempotencyContext>,
) -> Result<CoordinatedCommit, MembusError> {
    let (commit, replayed) =
        storage.commit_indexed_tx_idempotent(guards, &mutations, ack, idempotency)?;
    let mut search_sync = SearchSyncStatus::Synced;
    if !replayed {
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
    }
    Ok(CoordinatedCommit {
        commit,
        search_sync,
        idempotent_replay: replayed,
    })
}

/// Apply one replicated delta with echo-loop detection and post-commit secondary search sync.
pub(crate) fn coordinated_apply_replicated_delta(
    storage: &Arc<dyn StorageEngine>,
    search: Option<&Arc<SearchEngine>>,
    local_node_id: &str,
    route_origin: Option<&str>,
    delta: &ReplicatedDelta,
    ack: Ack,
) -> Result<CoordinatedReplicatedApply, MembusError> {
    crate::storage::mesh_replication::validate_replication_node_id(
        local_node_id,
        "local_node_id",
    )?;
    crate::storage::mesh_replication::validate_replication_node_id(
        &delta.stamp.origin_node_id,
        "origin_node_id",
    )?;
    if delta.stamp.origin_node_id == local_node_id
        || route_origin.is_some_and(|origin| is_local_route_origin(origin, local_node_id))
    {
        return Ok(CoordinatedReplicatedApply {
            result: ReplicatedApplyResult {
                applied: false,
                local_seq: None,
            },
            echo_rejected: true,
            search_sync: SearchSyncStatus::Synced,
        });
    }

    let result = storage.apply_replicated_delta(local_node_id, delta, ack)?;
    let search_sync = match (search, result.local_seq, &delta.search_op) {
        (Some(engine), Some(local_seq), Some(op)) if result.applied => {
            ingest_or_defer(engine, local_seq, 0, storage.epoch(), op.clone())
        }
        _ => SearchSyncStatus::Synced,
    };
    Ok(CoordinatedReplicatedApply {
        result,
        echo_rejected: false,
        search_sync,
    })
}

pub(crate) fn is_local_route_origin(route_origin: &str, local_node_id: &str) -> bool {
    let trimmed = route_origin.trim();
    if trimmed == local_node_id {
        return true;
    }
    if trimmed.len() >= 7 && trimmed[..7].eq_ignore_ascii_case("router:") {
        return trimmed[7..].trim() == local_node_id;
    }
    false
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
        idempotent_replay: false,
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
        idempotent_replay: false,
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
        idempotent_replay: false,
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
