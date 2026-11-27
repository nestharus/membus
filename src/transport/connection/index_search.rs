use std::sync::Arc;

use crate::protocol::{Request, Response};
use crate::transport::connection::state::ConnectionState;
use crate::transport::helpers::run_blocking_query;
use crate::transport::write_coord::{
    coordinated_delete_search_document, coordinated_index_document,
    coordinated_revoke_search_prefix,
};
use crate::types::MembusError;

pub(super) async fn dispatch_index_query(conn: &Arc<ConnectionState>, req: Request) -> Response {
    let storage = Arc::clone(&conn.ctx.storage);
    match req {
        Request::QueryOrderedIndex {
            namespace,
            index_id,
            options,
            snapshot_id,
        } => {
            let pinned_snap = match conn.resolve_snapshot(snapshot_id) {
                Ok(s) => s,
                Err(e) => return Response::Error(e),
            };
            let result = run_blocking_query(
                conn.ctx.config.query_timeout,
                "query_ordered_index",
                move || {
                    let snap = match pinned_snap {
                        Some(s) => s,
                        None => Arc::new(storage.open_snapshot()?),
                    };
                    storage.query_ordered_index(&snap, &namespace, &index_id, &options)
                },
            )
            .await;
            match result {
                Ok(page) => Response::OrderedIndex(page),
                Err(e) => Response::Error(e),
            }
        }
        Request::QueryNeighbors {
            namespace,
            index_id,
            node_id,
            options,
            snapshot_id,
        } => {
            let pinned_snap = match conn.resolve_snapshot(snapshot_id) {
                Ok(s) => s,
                Err(e) => return Response::Error(e),
            };
            let result = run_blocking_query(
                conn.ctx.config.query_timeout,
                "query_neighbors",
                move || {
                    let snap = match pinned_snap {
                        Some(s) => s,
                        None => Arc::new(storage.open_snapshot()?),
                    };
                    storage.query_neighbors(&snap, &namespace, &index_id, &node_id, &options)
                },
            )
            .await;
            match result {
                Ok(page) => Response::Neighbors(page),
                Err(e) => Response::Error(e),
            }
        }
        Request::BfsTraverse {
            namespace,
            index_id,
            seed_node,
            options,
            snapshot_id,
        } => {
            let pinned_snap = match conn.resolve_snapshot(snapshot_id) {
                Ok(s) => s,
                Err(e) => return Response::Error(e),
            };
            let result = run_blocking_query(
                conn.ctx.config.query_timeout,
                "bfs_traverse",
                move || {
                    let snap = match pinned_snap {
                        Some(s) => s,
                        None => Arc::new(storage.open_snapshot()?),
                    };
                    storage.bfs_traverse(&snap, &namespace, &index_id, &seed_node, &options)
                },
            )
            .await;
            match result {
                Ok(res) => Response::Bfs(res),
                Err(e) => Response::Error(e),
            }
        }
        _ => Response::Error(MembusError::Protocol(
            "unexpected request in index_query dispatch".to_owned(),
        )),
    }
}

pub(super) async fn dispatch_search(conn: &Arc<ConnectionState>, req: Request) -> Response {
    match req {
        Request::IndexDocument { doc, ack } => {
            let ctx = conn.ctx.clone();
            let Some(search) = conn.ctx.search.clone() else {
                return Response::Error(MembusError::InvalidArgument(
                    "search engine is not enabled on this server".to_owned(),
                ));
            };
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                coordinated_index_document(&search, doc, ack)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("index_document failed: {e}"))));
            match result {
                Ok(coord) => Response::Commit(coord.commit),
                Err(e) => Response::Error(e),
            }
        }
        Request::DeleteSearchDocument {
            namespace,
            doc_id,
            ack,
        } => {
            let ctx = conn.ctx.clone();
            let Some(search) = conn.ctx.search.clone() else {
                return Response::Error(MembusError::InvalidArgument(
                    "search engine is not enabled on this server".to_owned(),
                ));
            };
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                coordinated_delete_search_document(&search, &namespace, &doc_id, ack)
            })
            .await
            .unwrap_or_else(|e| {
                Err(MembusError::Storage(format!(
                    "delete_search_document failed: {e}"
                )))
            });
            match result {
                Ok(coord) => Response::Commit(coord.commit),
                Err(e) => Response::Error(e),
            }
        }
        Request::RevokeSearchPrefix {
            namespace,
            prefix,
            ack,
        } => {
            let ctx = conn.ctx.clone();
            let Some(search) = conn.ctx.search.clone() else {
                return Response::Error(MembusError::InvalidArgument(
                    "search engine is not enabled on this server".to_owned(),
                ));
            };
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                coordinated_revoke_search_prefix(&search, &namespace, &prefix, ack)
            })
            .await
            .unwrap_or_else(|e| {
                Err(MembusError::Storage(format!(
                    "revoke_search_prefix failed: {e}"
                )))
            });
            match result {
                Ok(coord) => Response::Commit(coord.commit),
                Err(e) => Response::Error(e),
            }
        }
        Request::Search(query) => {
            let Some(search) = conn.ctx.search.clone() else {
                return Response::Error(MembusError::InvalidArgument(
                    "search engine is not enabled on this server".to_owned(),
                ));
            };
            let result = run_blocking_query(
                conn.ctx.config.query_timeout,
                "search",
                move || search.search(&query),
            )
            .await;
            match result {
                Ok(page) => Response::Search(page),
                Err(e) => Response::Error(e),
            }
        }
        _ => Response::Error(MembusError::Protocol(
            "unexpected request in search dispatch".to_owned(),
        )),
    }
}
