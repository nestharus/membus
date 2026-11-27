mod admin;
mod blobs;
mod catalog;
mod index_search;
mod kv;
mod leases;
mod replication;
mod signals;
mod state;
mod ws;
mod ws_stream;

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::DefaultBodyLimit;
use axum::middleware;
use axum::routing::{delete, get, post, put};
use axum::Router;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch, Semaphore};
use tokio_rustls::server::TlsStream;

use crate::transport::tls::{build_http_tls_acceptor, MAX_CONCURRENT_TLS_HANDSHAKES};
use crate::transport::{Endpoint, ServerContext, ServerHandle, TlsServerConfig};
use crate::types::MembusError;

use admin::{
    handle_admin_role_transition, handle_checkpoint, handle_health, handle_namespace_digest,
};
use blobs::{handle_blob_get, handle_blob_head, handle_blob_put};
use catalog::{
    handle_catalog_get, handle_catalog_get_version, handle_catalog_list, handle_catalog_register,
    handle_migration_abort, handle_migration_advance, handle_migration_begin,
    handle_migration_commit, handle_migration_contract, handle_migration_reverse_shadow,
    handle_migration_rollback,
};
use index_search::{
    handle_graph_bfs, handle_graph_neighbors, handle_ordered_query, handle_search_delete_doc,
    handle_search_index_doc, handle_search_query, handle_search_revoke_prefix,
};
use kv::{
    handle_kv_delete, handle_kv_get, handle_kv_put, handle_scan_prefix, handle_scan_range,
    handle_tx,
};
use leases::{
    handle_lease_acquire, handle_lease_inspect, handle_lease_release, handle_lease_renew,
    handle_lease_validate,
};
use replication::{handle_replication_apply, handle_updates_scan};
use signals::handle_signals_sse;
use state::{enforce_http_concurrency_limit, wait_shutdown, wait_shutdown_ref};
use ws::handle_ws_upgrade;

pub(crate) use state::HttpState;

/// Axum [`axum::serve::Listener`] adapter terminating TLS/mTLS off the accept loop
/// with bounded concurrent handshakes and per-handshake timeout.
struct TlsHttpListener {
    local_addr: SocketAddr,
    accepted_rx: mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
    accept_loop: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for TlsHttpListener {
    fn drop(&mut self) {
        if let Some(handle) = self.accept_loop.take() {
            handle.abort();
        }
    }
}

impl axum::serve::Listener for TlsHttpListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.accepted_rx.recv().await {
            Some(conn) => conn,
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(self.local_addr)
    }
}

/// Bind an Axum HTTP/2 + HTTP/1.1 REST, SSE, and WebSocket listener sharing `ServerContext`.
pub async fn spawn_http_listener(
    addr: SocketAddr,
    ctx: ServerContext,
) -> Result<ServerHandle, MembusError> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let active_connections = Arc::clone(&ctx.active_connections);

    let state = HttpState {
        ctx,
        shutdown_rx: shutdown_rx.clone(),
    };
    let app = build_router(state);

    let accept_task = tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, app)
            .with_graceful_shutdown(wait_shutdown(shutdown_rx))
            .await
        {
            eprintln!("membus HTTP listener error: {err}");
        }
    });

    Ok(ServerHandle::new(
        Endpoint::Http(local_addr),
        shutdown_tx,
        accept_task,
        active_connections,
    ))
}

/// Bind an Axum HTTPS (TLS/mTLS with HTTP/2 + HTTP/1.1 ALPN) REST, SSE, and WebSocket listener.
pub async fn spawn_http_tls_listener(
    addr: SocketAddr,
    tls_cfg: &TlsServerConfig,
    ctx: ServerContext,
) -> Result<ServerHandle, MembusError> {
    let acceptor = build_http_tls_acceptor(tls_cfg)?;
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let active_connections = Arc::clone(&ctx.active_connections);

    let (accepted_tx, accepted_rx) = mpsc::channel::<(TlsStream<TcpStream>, SocketAddr)>(256);
    let handshake_sem = Arc::new(Semaphore::new(MAX_CONCURRENT_TLS_HANDSHAKES));
    let mut loop_shutdown = shutdown_rx.clone();

    let accept_loop = tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = wait_shutdown_ref(&mut loop_shutdown) => break,
                accepted = listener.accept() => {
                    match accepted {
                        Ok((tcp_stream, peer)) => {
                            let Ok(permit) = Arc::clone(&handshake_sem).try_acquire_owned() else {
                                drop(tcp_stream);
                                continue;
                            };
                            let _ = tcp_stream.set_nodelay(true);
                            let acceptor_ref = acceptor.clone();
                            let tx = accepted_tx.clone();
                            let mut hs_shutdown = loop_shutdown.clone();
                            tokio::spawn(async move {
                                let hs_res = {
                                    let _hs_permit = permit;
                                    tokio::select! {
                                        biased;
                                        _ = wait_shutdown_ref(&mut hs_shutdown) => None,
                                        res = tokio::time::timeout(
                                            Duration::from_secs(10),
                                            acceptor_ref.accept(tcp_stream),
                                        ) => res.ok().and_then(Result::ok),
                                    }
                                };
                                if let Some(tls_stream) = hs_res {
                                    tokio::select! {
                                        biased;
                                        _ = wait_shutdown_ref(&mut hs_shutdown) => {}
                                        _ = tx.send((tls_stream, peer)) => {}
                                    }
                                }
                            });
                        }
                        Err(_) => {
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                    }
                }
            }
        }
    });

    let tls_listener = TlsHttpListener {
        local_addr,
        accepted_rx,
        accept_loop: Some(accept_loop),
    };

    let state = HttpState {
        ctx,
        shutdown_rx: shutdown_rx.clone(),
    };
    let app = build_router(state);

    let accept_task = tokio::spawn(async move {
        if let Err(err) = axum::serve(tls_listener, app)
            .with_graceful_shutdown(wait_shutdown(shutdown_rx))
            .await
        {
            eprintln!("membus HTTPS listener error: {err}");
        }
    });

    Ok(ServerHandle::new(
        Endpoint::HttpTls(local_addr),
        shutdown_tx,
        accept_task,
        active_connections,
    ))
}

pub(crate) fn build_router(state: HttpState) -> Router {
    let max_body = state.ctx.config.max_frame_body;
    // Keep `/v2/health` outside the data-plane concurrency gate so liveness/readiness probes
    // and live saturation metrics remain observable even while data-plane traffic is saturated.
    let health_router = Router::new()
        .route("/v2/health", get(handle_health))
        .with_state(state.clone());

    let data_router = Router::new()
        .route("/v2/checkpoint", post(handle_checkpoint))
        .route(
            "/v2/kv/{ns}/{*key}",
            get(handle_kv_get)
                .put(handle_kv_put)
                .delete(handle_kv_delete),
        )
        .route("/v2/tx", post(handle_tx))
        .route("/v2/scan/prefix", post(handle_scan_prefix))
        .route("/v2/scan/range", post(handle_scan_range))
        .route("/v2/index/ordered/query", post(handle_ordered_query))
        .route("/v2/index/graph/neighbors", post(handle_graph_neighbors))
        .route("/v2/index/graph/bfs", post(handle_graph_bfs))
        .route("/v2/blobs/{ns}", put(handle_blob_put))
        .route(
            "/v2/blobs/{ns}/{sha256}",
            get(handle_blob_get).head(handle_blob_head),
        )
        .route("/v2/leases/acquire", post(handle_lease_acquire))
        .route("/v2/leases/renew", post(handle_lease_renew))
        .route("/v2/leases/release", post(handle_lease_release))
        .route("/v2/leases/validate", post(handle_lease_validate))
        .route("/v2/leases/{ns}/{*resource}", get(handle_lease_inspect))
        .route(
            "/v2/catalog/collections",
            get(handle_catalog_list).post(handle_catalog_register),
        )
        .route("/v2/catalog/collections/{ns}", get(handle_catalog_get))
        .route(
            "/v2/catalog/collections/{ns}/versions/{ver}",
            get(handle_catalog_get_version),
        )
        .route(
            "/v2/catalog/collections/{ns}/migrations/begin",
            post(handle_migration_begin),
        )
        .route(
            "/v2/catalog/collections/{ns}/migrations/advance",
            post(handle_migration_advance),
        )
        .route(
            "/v2/catalog/collections/{ns}/migrations/reverse-shadow",
            post(handle_migration_reverse_shadow),
        )
        .route(
            "/v2/catalog/collections/{ns}/migrations/rollback",
            post(handle_migration_rollback),
        )
        .route(
            "/v2/catalog/collections/{ns}/migrations/contract",
            post(handle_migration_contract),
        )
        .route(
            "/v2/catalog/collections/{ns}/migrations/commit",
            post(handle_migration_commit),
        )
        .route(
            "/v2/catalog/collections/{ns}/migrations/abort",
            post(handle_migration_abort),
        )
        .route("/v2/namespaces/{ns}/digest", post(handle_namespace_digest))
        .route("/v2/admin/role", post(handle_admin_role_transition))
        .route("/v2/replication/apply", post(handle_replication_apply))
        .route("/v2/updates", get(handle_updates_scan))
        .route("/v2/search/documents", post(handle_search_index_doc))
        .route(
            "/v2/search/documents/{ns}/{*doc_id}",
            delete(handle_search_delete_doc),
        )
        .route("/v2/search/revoke-prefix", post(handle_search_revoke_prefix))
        .route("/v2/search/query", post(handle_search_query))
        .route("/v2/signals/{ns}/stream", get(handle_signals_sse))
        .route("/v2/ws", get(handle_ws_upgrade))
        .layer(middleware::from_fn_with_state(
            state.clone(),
            enforce_http_concurrency_limit,
        ))
        .layer(DefaultBodyLimit::max(max_body))
        .with_state(state);

    health_router.merge(data_router)
}
