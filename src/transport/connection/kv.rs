use std::sync::atomic::Ordering;
use std::sync::Arc;

use crate::protocol::{Request, Response};
use crate::transport::connection::state::{ConnectionState, SnapshotReservationGuard};
use crate::transport::helpers::run_blocking_query;
use crate::transport::write_coord::{
    coordinated_commit_indexed_tx, coordinated_put_indexed, coordinated_remove_indexed,
    CoordinatedPutArgs, CoordinatedRemoveArgs,
};
use crate::types::MembusError;

pub(super) async fn dispatch_kv_rw(conn: &Arc<ConnectionState>, req: Request) -> Response {
    match req {
        Request::Get { .. } | Request::GetMany { .. } => dispatch_kv_read(conn, req).await,
        Request::Put { .. }
        | Request::PutIndexed { .. }
        | Request::Remove { .. }
        | Request::RemoveIndexed { .. } => dispatch_kv_write(conn, req).await,
        _ => Response::Error(MembusError::Protocol(
            "unexpected request in kv_rw dispatch".to_owned(),
        )),
    }
}

async fn dispatch_kv_read(conn: &Arc<ConnectionState>, req: Request) -> Response {
    let storage = Arc::clone(&conn.ctx.storage);
    match req {
        Request::Get {
            namespace,
            key,
            snapshot_id,
        } => {
            let snap = match conn.resolve_snapshot(snapshot_id) {
                Ok(s) => s,
                Err(e) => return Response::Error(e),
            };
            let result = tokio::task::spawn_blocking(move || match snap {
                Some(s) => storage.get_in_snapshot(&s, &namespace, &key),
                None => storage.get(&namespace, &key),
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("get task failed: {e}"))));
            match result {
                Ok(rec) => Response::Get(rec),
                Err(e) => Response::Error(e),
            }
        }
        Request::GetMany {
            namespace,
            keys,
            snapshot_id,
        } => {
            let snap = match conn.resolve_snapshot(snapshot_id) {
                Ok(s) => s,
                Err(e) => return Response::Error(e),
            };
            let result = tokio::task::spawn_blocking(move || match snap {
                Some(s) => storage.get_many_in_snapshot(&s, &namespace, &keys),
                None => storage.get_many(&namespace, &keys),
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("get_many task failed: {e}"))));
            match result {
                Ok(recs) => Response::GetMany(recs),
                Err(e) => Response::Error(e),
            }
        }
        _ => Response::Error(MembusError::Protocol(
            "unexpected request in kv_read dispatch".to_owned(),
        )),
    }
}

async fn dispatch_kv_write(conn: &Arc<ConnectionState>, req: Request) -> Response {
    let storage = Arc::clone(&conn.ctx.storage);
    match req {
        Request::Put {
            namespace,
            key,
            value,
            ack,
            expected,
        } => {
            let ctx = conn.ctx.clone();
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                storage.put(&namespace, &key, &value, ack, expected)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("put task failed: {e}"))));
            match result {
                Ok(commit) => Response::Commit(commit),
                Err(e) => Response::Error(e),
            }
        }
        Request::PutIndexed {
            namespace,
            key,
            value,
            ack,
            expected,
            indexes,
            search_op,
        } => {
            let ctx = conn.ctx.clone();
            let search = conn.ctx.search.clone();
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                coordinated_put_indexed(
                    &storage,
                    search.as_ref(),
                    CoordinatedPutArgs {
                        namespace: &namespace,
                        key: &key,
                        value: &value,
                        ack,
                        expected,
                        indexes: indexes.as_ref(),
                        search_op,
                        idempotency: None,
                    },
                )
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("put_indexed failed: {e}"))));
            match result {
                Ok(coord) => Response::Commit(coord.commit),
                Err(e) => Response::Error(e),
            }
        }
        Request::Remove {
            namespace,
            key,
            ack,
            expected,
        } => {
            let ctx = conn.ctx.clone();
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                storage.remove(&namespace, &key, ack, expected)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("remove task failed: {e}"))));
            match result {
                Ok(commit) => Response::Commit(commit),
                Err(e) => Response::Error(e),
            }
        }
        Request::RemoveIndexed {
            namespace,
            key,
            ack,
            expected,
            search_op,
        } => {
            let ctx = conn.ctx.clone();
            let search = conn.ctx.search.clone();
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                coordinated_remove_indexed(
                    &storage,
                    search.as_ref(),
                    CoordinatedRemoveArgs {
                        namespace: &namespace,
                        key: &key,
                        ack,
                        expected,
                        search_op,
                        idempotency: None,
                    },
                )
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("remove_indexed failed: {e}"))));
            match result {
                Ok(coord) => Response::Commit(coord.commit),
                Err(e) => Response::Error(e),
            }
        }
        _ => Response::Error(MembusError::Protocol(
            "unexpected request in kv_rw dispatch".to_owned(),
        )),
    }
}

pub(super) async fn dispatch_tx(conn: &Arc<ConnectionState>, req: Request) -> Response {
    let storage = Arc::clone(&conn.ctx.storage);
    match req {
        Request::CommitTx {
            ack,
            guards,
            mutations,
        } => {
            let ctx = conn.ctx.clone();
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                storage.commit_tx(&guards, &mutations, ack)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("commit_tx task failed: {e}"))));
            match result {
                Ok(commit) => Response::Commit(commit),
                Err(e) => Response::Error(e),
            }
        }
        Request::CommitIndexedTx {
            ack,
            guards,
            mutations,
        } => {
            let ctx = conn.ctx.clone();
            let search = conn.ctx.search.clone();
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                coordinated_commit_indexed_tx(&storage, search.as_ref(), &guards, mutations, ack)
            })
            .await
            .unwrap_or_else(|e| {
                Err(MembusError::Storage(format!(
                    "commit_indexed_tx failed: {e}"
                )))
            });
            match result {
                Ok(coord) => Response::Commit(coord.commit),
                Err(e) => Response::Error(e),
            }
        }
        _ => Response::Error(MembusError::Protocol(
            "unexpected request in tx dispatch".to_owned(),
        )),
    }
}

pub(super) async fn dispatch_snapshot_scan(conn: &Arc<ConnectionState>, req: Request) -> Response {
    let storage = Arc::clone(&conn.ctx.storage);
    match req {
        Request::OpenSnapshot => {
            let reservation = {
                let mut table = conn.snapshots.lock();
                table.reap_expired(conn.ctx.config.snapshot_ttl);
                if table.entries.len().saturating_add(table.reserved)
                    >= conn.ctx.config.max_snapshots_per_conn
                {
                    return Response::Error(MembusError::Busy {
                        retry_after_ms: 10,
                        reason: "per-connection snapshot limit reached".to_owned(),
                    });
                }
                table.reserved += 1;
                SnapshotReservationGuard {
                    table: Arc::clone(&conn.snapshots),
                    committed: false,
                }
            };

            let result = tokio::task::spawn_blocking(move || storage.open_snapshot())
                .await
                .unwrap_or_else(|e| {
                    Err(MembusError::Storage(format!(
                        "open_snapshot task failed: {e}"
                    )))
                });
            match result {
                Ok(snapshot) => {
                    let snapshot_seq = snapshot.snapshot_seq();
                    let snapshot_id = conn.next_snapshot_id.fetch_add(1, Ordering::AcqRel);
                    reservation.commit(snapshot_id, Arc::new(snapshot));
                    Response::SnapshotOpened {
                        snapshot_id,
                        snapshot_seq,
                    }
                }
                Err(e) => Response::Error(e),
            }
        }
        Request::CloseSnapshot { snapshot_id } => {
            if conn.snapshots.lock().entries.remove(&snapshot_id).is_some() {
                Response::Unit
            } else {
                Response::Error(MembusError::NotFound(format!(
                    "unknown snapshot_id {snapshot_id}"
                )))
            }
        }
        Request::ScanPrefix {
            namespace,
            prefix,
            options,
            snapshot_id,
        } => {
            let pinned_snap = match conn.resolve_snapshot(snapshot_id) {
                Ok(s) => s,
                Err(e) => return Response::Error(e),
            };
            let result = run_blocking_query(
                conn.ctx.config.query_timeout,
                "scan_prefix",
                move || {
                    let snap = match pinned_snap {
                        Some(s) => s,
                        None => Arc::new(storage.open_snapshot()?),
                    };
                    storage.scan_prefix(&snap, &namespace, &prefix, &options)
                },
            )
            .await;
            match result {
                Ok(page) => Response::Scan(page),
                Err(e) => Response::Error(e),
            }
        }
        Request::ScanRange {
            namespace,
            start_key,
            end_key,
            options,
            snapshot_id,
        } => {
            let pinned_snap = match conn.resolve_snapshot(snapshot_id) {
                Ok(s) => s,
                Err(e) => return Response::Error(e),
            };
            let result = run_blocking_query(
                conn.ctx.config.query_timeout,
                "scan_range",
                move || {
                    let snap = match pinned_snap {
                        Some(s) => s,
                        None => Arc::new(storage.open_snapshot()?),
                    };
                    storage.scan_range(&snap, &namespace, &start_key, &end_key, &options)
                },
            )
            .await;
            match result {
                Ok(page) => Response::Scan(page),
                Err(e) => Response::Error(e),
            }
        }
        _ => Response::Error(MembusError::Protocol(
            "unexpected request in snapshot_scan dispatch".to_owned(),
        )),
    }
}

pub(super) async fn dispatch_blob(conn: &Arc<ConnectionState>, req: Request) -> Response {
    let storage = Arc::clone(&conn.ctx.storage);
    match req {
        Request::PutBlob { digest, data, ack } => {
            let ctx = conn.ctx.clone();
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                storage.put_blob(&digest, &data, ack)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("put_blob failed: {e}"))));
            match result {
                Ok(()) => Response::Unit,
                Err(e) => Response::Error(e),
            }
        }
        Request::GetBlob { digest } => {
            let result = tokio::task::spawn_blocking(move || storage.get_blob(&digest))
                .await
                .unwrap_or_else(|e| Err(MembusError::Storage(format!("get_blob failed: {e}"))));
            match result {
                Ok(data) => Response::Blob(data),
                Err(e) => Response::Error(e),
            }
        }
        Request::PutCasBlob {
            namespace,
            data,
            ack,
        } => {
            let ctx = conn.ctx.clone();
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                storage.put_cas_blob(&namespace, &data, ack)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("put_cas_blob failed: {e}"))));
            match result {
                Ok(digest) => Response::CasBlobDigest(digest),
                Err(e) => Response::Error(e),
            }
        }
        Request::GetCasBlob { namespace, sha256 } => {
            let result =
                tokio::task::spawn_blocking(move || storage.get_cas_blob(&namespace, &sha256))
                    .await
                    .unwrap_or_else(|e| {
                        Err(MembusError::Storage(format!("get_cas_blob failed: {e}")))
                    });
            match result {
                Ok(data) => Response::Blob(data),
                Err(e) => Response::Error(e),
            }
        }
        Request::HasCasBlob { namespace, sha256 } => {
            let result =
                tokio::task::spawn_blocking(move || storage.has_cas_blob(&namespace, &sha256))
                    .await
                    .unwrap_or_else(|e| {
                        Err(MembusError::Storage(format!("has_cas_blob failed: {e}")))
                    });
            match result {
                Ok(exists) => Response::Bool(exists),
                Err(e) => Response::Error(e),
            }
        }
        _ => Response::Error(MembusError::Protocol(
            "unexpected request in blob dispatch".to_owned(),
        )),
    }
}
