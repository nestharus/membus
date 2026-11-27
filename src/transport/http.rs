use std::collections::HashMap;
use std::convert::Infallible;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, Path, Query, Request as AxumRequest, State};
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response as AxumResponse};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch, Semaphore};
use tokio_rustls::server::TlsStream;
use tokio_stream::wrappers::ReceiverStream;

use crate::index::{BfsOptions, NeighborQueryOptions, OrderedKeyFilter, OrderedQueryOptions};
use crate::signals::{EachChangeSubscription, LiveCoalescedSubscription};
use crate::storage::{compute_namespace_parity_digest_with_timeout, encode_sha256_hex};
use crate::transport::connection::{
    authorize_index_mutations, authorize_search_op, resolve_checkpoint_path, run_blocking_query,
    ConnectionGuard,
};
use crate::transport::tls::{build_http_tls_acceptor, MAX_CONCURRENT_TLS_HANDSHAKES};
use crate::transport::write_coord::{
    coordinated_apply_replicated_delta, coordinated_commit_indexed_tx_idempotent,
    coordinated_delete_search_document, coordinated_index_document,
    coordinated_put_indexed, coordinated_put_indexed_idempotent, coordinated_remove_indexed,
    coordinated_remove_indexed_idempotent, coordinated_revoke_search_prefix, CoordinatedCommit,
    CoordinatedReplicatedApply,
};
use crate::transport::{Endpoint, ServerContext, ServerHandle, TlsServerConfig};
use crate::types::{
    Ack, AuthMode, CollectionDescriptor, CommitResult, ExpectedVersion, GraphDirection,
    HealthStatus, IdempotencyContext, IndexMutations, IndexedMutation, MembusError,
    NamespaceParityDigest, ParityDigestMode, Permission, Principal, ReplicatedApplyResult,
    ReplicatedDelta, ScanOptions, SearchDocument, SearchLogOp, SearchQuery, SearchSyncStatus,
    SignalEvent, SlotRole, SlotRoleState, VersionGuard, MAX_IDEMPOTENCY_KEY_BYTES,
};

const HEADER_SEARCH_SYNC: HeaderName = HeaderName::from_static("x-membus-search-sync");
const HEADER_ROLE_EPOCH: HeaderName = HeaderName::from_static("x-membus-role-epoch");
const HEADER_IDEMPOTENCY_KEY: HeaderName = HeaderName::from_static("x-membus-idempotency-key");
const HEADER_IDEMPOTENT_REPLAY: HeaderName = HeaderName::from_static("x-membus-idempotent-replay");
const HEADER_ROUTE_ORIGIN: HeaderName = HeaderName::from_static("x-membus-route-origin");
const HEADER_ECHO_LOOP: HeaderName = HeaderName::from_static("x-membus-echo-loop");

#[derive(Clone)]
pub(crate) struct HttpState {
    ctx: ServerContext,
    shutdown_rx: watch::Receiver<bool>,
}

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

async fn enforce_http_concurrency_limit(
    State(state): State<HttpState>,
    req: AxumRequest,
    next: Next,
) -> AxumResponse {
    let Ok(_permit) = Arc::clone(&state.ctx.http_in_flight_sem).try_acquire_owned() else {
        return HttpError(MembusError::Busy {
            retry_after_ms: 10,
            reason: format!(
                "global HTTP in-flight request limit reached ({})",
                state.ctx.config.max_http_in_flight.max(1)
            ),
        })
        .into_response();
    };
    next.run(req).await
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

#[derive(Serialize)]
struct HttpCommitResp {
    #[serde(flatten)]
    commit: CommitResult,
    search_sync: SearchSyncStatus,
}

fn coordinated_commit_response(res: CoordinatedCommit) -> AxumResponse {
    let header_val = HeaderValue::from_static(res.search_sync.as_str());
    let replayed = res.idempotent_replay;
    let body = HttpCommitResp {
        commit: res.commit,
        search_sync: res.search_sync,
    };
    let mut resp = (StatusCode::OK, Json(body)).into_response();
    resp.headers_mut().insert(HEADER_SEARCH_SYNC, header_val);
    if replayed {
        resp.headers_mut()
            .insert(HEADER_IDEMPOTENT_REPLAY, HeaderValue::from_static("true"));
    }
    resp
}

#[derive(Serialize)]
struct HttpReplicationApplyResp {
    #[serde(flatten)]
    result: ReplicatedApplyResult,
    echo_rejected: bool,
    search_sync: SearchSyncStatus,
}

fn coordinated_replicated_apply_response(res: CoordinatedReplicatedApply) -> AxumResponse {
    let header_val = HeaderValue::from_static(res.search_sync.as_str());
    let echo_rejected = res.echo_rejected;
    let body = HttpReplicationApplyResp {
        result: res.result,
        echo_rejected,
        search_sync: res.search_sync,
    };
    let mut resp = (StatusCode::OK, Json(body)).into_response();
    resp.headers_mut().insert(HEADER_SEARCH_SYNC, header_val);
    if echo_rejected {
        resp.headers_mut()
            .insert(HEADER_ECHO_LOOP, HeaderValue::from_static("rejected"));
    }
    resp
}

#[derive(Serialize)]
struct ErrorBody {
    error: &'static str,
    message: String,
}

struct HttpError(MembusError);

impl From<MembusError> for HttpError {
    fn from(value: MembusError) -> Self {
        Self(value)
    }
}

impl IntoResponse for HttpError {
    fn into_response(self) -> AxumResponse {
        let (status, code) = match &self.0 {
            MembusError::Unauthorized(_) => (StatusCode::UNAUTHORIZED, "unauthorized"),
            MembusError::NotFound(_) => (StatusCode::NOT_FOUND, "not_found"),
            MembusError::InvalidArgument(_) => (StatusCode::BAD_REQUEST, "invalid_argument"),
            MembusError::Protocol(_) => (StatusCode::BAD_REQUEST, "protocol"),
            MembusError::Conflict { .. } => (StatusCode::CONFLICT, "conflict"),
            MembusError::TxConflict(_) => (StatusCode::CONFLICT, "tx_conflict"),
            MembusError::LeaseHeld { .. } => (StatusCode::CONFLICT, "lease_held"),
            MembusError::RoleFenced { role, .. } => {
                let st = if *role == SlotRole::Draining {
                    StatusCode::SERVICE_UNAVAILABLE
                } else {
                    StatusCode::CONFLICT
                };
                (st, "role_fenced")
            }
            MembusError::IdempotencyConflict(_) => (StatusCode::CONFLICT, "idempotency_conflict"),
            MembusError::Busy { .. } => (StatusCode::SERVICE_UNAVAILABLE, "busy"),
            MembusError::Gap { .. } => (StatusCode::GONE, "gap"),
            MembusError::FrameTooLarge { .. } => {
                (StatusCode::PAYLOAD_TOO_LARGE, "frame_too_large")
            }
            MembusError::Corruption(_) => (StatusCode::INTERNAL_SERVER_ERROR, "corruption"),
            MembusError::Storage(_) => (StatusCode::INTERNAL_SERVER_ERROR, "storage"),
            MembusError::Io(_) => (StatusCode::INTERNAL_SERVER_ERROR, "io"),
            MembusError::Closed => (StatusCode::SERVICE_UNAVAILABLE, "closed"),
        };
        (
            status,
            Json(ErrorBody {
                error: code,
                message: self.0.to_string(),
            }),
        )
            .into_response()
    }
}

fn extract_single_header<'a>(
    headers: &'a HeaderMap,
    name: &HeaderName,
    display: &str,
) -> Result<Option<&'a str>, MembusError> {
    let mut iter = headers.get_all(name).iter();
    let Some(first) = iter.next() else {
        return Ok(None);
    };
    if iter.next().is_some() {
        return Err(MembusError::InvalidArgument(format!(
            "duplicate {display} header is not permitted"
        )));
    }
    let s = first.to_str().map_err(|_| {
        MembusError::InvalidArgument(format!("{display} header must be valid UTF-8"))
    })?;
    Ok(Some(s))
}

fn parse_role_epoch_header(headers: &HeaderMap) -> Result<Option<u64>, MembusError> {
    let Some(s) = extract_single_header(headers, &HEADER_ROLE_EPOCH, "X-Membus-Role-Epoch")?
    else {
        return Ok(None);
    };
    let parsed = s.trim().parse::<u64>().map_err(|_| {
        MembusError::InvalidArgument(format!(
            "invalid X-Membus-Role-Epoch header '{s}' (expected unsigned 64-bit integer)"
        ))
    })?;
    Ok(Some(parsed))
}

fn parse_idempotency_key_header(headers: &HeaderMap) -> Result<Option<String>, MembusError> {
    let Some(s) =
        extract_single_header(headers, &HEADER_IDEMPOTENCY_KEY, "X-Membus-Idempotency-Key")?
    else {
        return Ok(None);
    };
    if s.is_empty() || s.len() > MAX_IDEMPOTENCY_KEY_BYTES {
        return Err(MembusError::InvalidArgument(format!(
            "X-Membus-Idempotency-Key length must be 1..={MAX_IDEMPOTENCY_KEY_BYTES} bytes"
        )));
    }
    if s.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return Err(MembusError::InvalidArgument(
            "X-Membus-Idempotency-Key must not contain ASCII control characters".to_owned(),
        ));
    }
    Ok(Some(s.to_owned()))
}

fn parse_route_origin_header(headers: &HeaderMap) -> Result<Option<String>, MembusError> {
    let Some(s) = extract_single_header(headers, &HEADER_ROUTE_ORIGIN, "X-Membus-Route-Origin")?
    else {
        return Ok(None);
    };
    validate_route_origin_value(s, "X-Membus-Route-Origin header")
}

fn validate_route_origin_value(raw: &str, label: &str) -> Result<Option<String>, MembusError> {
    let trimmed = raw.trim();
    if trimmed.is_empty() || trimmed.len() > 128 || trimmed.bytes().any(|b| b < 0x20 || b == 0x7f) {
        return Err(MembusError::InvalidArgument(format!("invalid {label}")));
    }
    Ok(Some(trimmed.to_owned()))
}

fn tx_namespace_scope(guards: &[VersionGuard], mutations: &[IndexedMutation]) -> String {
    let mut namespaces: Vec<&str> = Vec::with_capacity(guards.len() + mutations.len());
    for g in guards {
        namespaces.push(g.namespace.as_str());
    }
    for m in mutations {
        namespaces.push(m.mutation.namespace.as_str());
    }
    namespaces.sort_unstable();
    namespaces.dedup();
    let mut h = Sha256::new();
    h.update(b"membus:idem:tx_scope:v1\0");
    h.update((namespaces.len() as u32).to_be_bytes());
    for ns in namespaces {
        h.update((ns.len() as u32).to_be_bytes());
        h.update(ns.as_bytes());
    }
    let digest: [u8; 32] = h.finalize().into();
    encode_sha256_hex(&digest)
}

fn fingerprint_kv_put(
    ns: &str,
    key: &[u8],
    value: &[u8],
    expected: ExpectedVersion,
    indexes: Option<&IndexMutations>,
    search_op: Option<&SearchLogOp>,
) -> Result<[u8; 32], MembusError> {
    let mut h = Sha256::new();
    h.update(b"membus:idem:kv_put:v1\0");
    h.update((ns.len() as u32).to_be_bytes());
    h.update(ns.as_bytes());
    h.update((key.len() as u32).to_be_bytes());
    h.update(key);
    h.update((value.len() as u32).to_be_bytes());
    h.update(value);
    let aux = serde_json::to_vec(&(expected, indexes, search_op))
        .map_err(|err| MembusError::Storage(format!("fingerprint encode failed: {err}")))?;
    h.update((aux.len() as u32).to_be_bytes());
    h.update(&aux);
    Ok(h.finalize().into())
}

fn fingerprint_kv_delete(
    ns: &str,
    key: &[u8],
    expected: ExpectedVersion,
    search_op: Option<&SearchLogOp>,
) -> Result<[u8; 32], MembusError> {
    let mut h = Sha256::new();
    h.update(b"membus:idem:kv_delete:v1\0");
    h.update((ns.len() as u32).to_be_bytes());
    h.update(ns.as_bytes());
    h.update((key.len() as u32).to_be_bytes());
    h.update(key);
    let aux = serde_json::to_vec(&(expected, search_op))
        .map_err(|err| MembusError::Storage(format!("fingerprint encode failed: {err}")))?;
    h.update((aux.len() as u32).to_be_bytes());
    h.update(&aux);
    Ok(h.finalize().into())
}

fn fingerprint_tx(
    guards: &[VersionGuard],
    mutations: &[IndexedMutation],
) -> Result<[u8; 32], MembusError> {
    let mut h = Sha256::new();
    h.update(b"membus:idem:tx:v1\0");
    let body = serde_json::to_vec(&(guards, mutations))
        .map_err(|err| MembusError::Storage(format!("fingerprint encode failed: {err}")))?;
    h.update((body.len() as u32).to_be_bytes());
    h.update(&body);
    Ok(h.finalize().into())
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<&str> {
    let raw = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = raw.split_once(' ')?;
    if scheme.eq_ignore_ascii_case("bearer") {
        Some(token.trim())
    } else {
        None
    }
}

fn authenticate_http(
    state: &HttpState,
    headers: &HeaderMap,
) -> Result<Option<Principal>, HttpError> {
    let token = extract_bearer_token(headers);
    Ok(state.ctx.auth.authenticate_token(token)?)
}

fn decode_bytes_field(
    utf8: Option<String>,
    b64: Option<String>,
    field: &str,
) -> Result<Vec<u8>, MembusError> {
    match (utf8, b64) {
        (Some(s), None) => Ok(s.into_bytes()),
        (None, Some(encoded)) => B64.decode(encoded).map_err(|err| {
            MembusError::InvalidArgument(format!("invalid base64 for '{field}': {err}"))
        }),
        (Some(_), Some(_)) => Err(MembusError::InvalidArgument(format!(
            "specify only one of '{field}_utf8' or '{field}_b64'"
        ))),
        (None, None) => Ok(Vec::new()),
    }
}

fn decode_opt_bytes_field(
    utf8: Option<String>,
    b64: Option<String>,
    field: &str,
) -> Result<Option<Vec<u8>>, MembusError> {
    match (utf8, b64) {
        (None, None) => Ok(None),
        (u, b) => Ok(Some(decode_bytes_field(u, b, field)?)),
    }
}

fn parse_sha256_hex(raw: &str) -> Result<[u8; 32], MembusError> {
    if raw.len() != 64 {
        return Err(MembusError::InvalidArgument(
            "sha256 hex digest must be exactly 64 hex characters".to_owned(),
        ));
    }
    let mut out = [0u8; 32];
    let bytes = raw.as_bytes();
    for i in 0..32 {
        let hi = hex_nibble(bytes[i * 2])?;
        let lo = hex_nibble(bytes[i * 2 + 1])?;
        out[i] = (hi << 4) | lo;
    }
    Ok(out)
}

fn hex_nibble(b: u8) -> Result<u8, MembusError> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(MembusError::InvalidArgument(
            "invalid hex character in sha256 digest".to_owned(),
        )),
    }
}

fn default_ack() -> Ack {
    Ack::Fast
}

fn default_expected() -> ExpectedVersion {
    ExpectedVersion::Any
}

fn parse_ack_param(raw: Option<&str>) -> Result<Ack, MembusError> {
    match raw {
        None => Ok(Ack::Fast),
        Some(s) if s.eq_ignore_ascii_case("fast") => Ok(Ack::Fast),
        Some(s) if s.eq_ignore_ascii_case("flush") => Ok(Ack::Flush),
        Some(other) => Err(MembusError::InvalidArgument(format!(
            "invalid ack mode '{other}' (expected 'fast' or 'flush')"
        ))),
    }
}

async fn handle_health(
    State(state): State<HttpState>,
    headers: HeaderMap,
) -> Result<Json<HealthStatus>, HttpError> {
    if !state.ctx.config.public_health_probe {
        let principal = authenticate_http(&state, &headers)?;
        state.ctx.auth.require_authenticated(principal.as_ref())?;
    }
    Ok(Json(state.ctx.health_status()))
}

#[derive(Deserialize)]
struct CheckpointReq {
    dest_dir: String,
}

#[derive(Serialize)]
struct CheckpointResp {
    seq: u64,
}

async fn handle_checkpoint(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<CheckpointReq>,
) -> Result<Json<CheckpointResp>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.authorize_admin(principal.as_ref(), None)?;
    let safe_dest = resolve_checkpoint_path(
        state.ctx.config.checkpoint_root.as_deref(),
        &req.dest_dir,
    )?;
    let storage = Arc::clone(&state.ctx.storage);
    let seq = tokio::task::spawn_blocking(move || storage.create_checkpoint(&safe_dest))
        .await
        .map_err(|e| MembusError::Storage(format!("checkpoint task failed: {e}")))??;
    Ok(Json(CheckpointResp { seq }))
}

async fn handle_kv_get(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((ns, key)): Path<(String, String)>,
) -> Result<AxumResponse, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state
        .ctx
        .auth
        .authorize(principal.as_ref(), &ns, key.as_bytes(), Permission::Read)?;
    let storage = Arc::clone(&state.ctx.storage);
    let rec = tokio::task::spawn_blocking(move || storage.get(&ns, key.as_bytes()))
        .await
        .map_err(|e| MembusError::Storage(format!("kv get failed: {e}")))??;
    match rec {
        Some(record) => Ok(Json(record).into_response()),
        None => Err(HttpError(MembusError::NotFound("key not found".to_owned()))),
    }
}

#[derive(Deserialize)]
struct HttpKvPutReq {
    value_utf8: Option<String>,
    value_b64: Option<String>,
    #[serde(default = "default_ack")]
    ack: Ack,
    #[serde(default = "default_expected")]
    expected: ExpectedVersion,
    indexes: Option<IndexMutations>,
    search_op: Option<SearchLogOp>,
}

async fn handle_kv_put(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((ns, key)): Path<(String, String)>,
    Json(req): Json<HttpKvPutReq>,
) -> Result<AxumResponse, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state
        .ctx
        .auth
        .authorize(principal.as_ref(), &ns, key.as_bytes(), Permission::Write)?;
    if let Some(idx) = &req.indexes {
        authorize_index_mutations(&state.ctx.auth, principal.as_ref(), &ns, idx)?;
    }
    if let Some(op) = &req.search_op {
        authorize_search_op(&state.ctx.auth, principal.as_ref(), op)?;
    }
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_client_write_allowed(role_epoch)?;
    let idem_key = parse_idempotency_key_header(&headers)?;
    let value = decode_bytes_field(req.value_utf8, req.value_b64, "value")?;
    let principal_id = if state.ctx.auth.mode() == AuthMode::Enforced {
        principal.as_ref().map(|p| p.id.clone())
    } else {
        None
    };
    let idempotency = match idem_key {
        Some(k) => Some(IdempotencyContext {
            scope_namespace: ns.clone(),
            principal_id,
            is_tx: false,
            key: k,
            request_fingerprint_sha256: fingerprint_kv_put(
                &ns,
                key.as_bytes(),
                &value,
                req.expected,
                req.indexes.as_ref(),
                req.search_op.as_ref(),
            )?,
        }),
        None => None,
    };
    let ctx = state.ctx.clone();
    let storage = Arc::clone(&state.ctx.storage);
    let search = state.ctx.search.clone();
    let coord = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_client_write_fence(role_epoch)?;
        coordinated_put_indexed_idempotent(
            &storage,
            search.as_ref(),
            &ns,
            key.as_bytes(),
            &value,
            req.ack,
            req.expected,
            req.indexes.as_ref(),
            req.search_op,
            idempotency.as_ref(),
        )
    })
    .await
    .map_err(|e| MembusError::Storage(format!("kv put failed: {e}")))??;
    Ok(coordinated_commit_response(coord))
}

#[derive(Deserialize)]
struct HttpKvDeleteReq {
    #[serde(default = "default_ack")]
    ack: Ack,
    #[serde(default = "default_expected")]
    expected: ExpectedVersion,
    search_op: Option<SearchLogOp>,
}

impl Default for HttpKvDeleteReq {
    fn default() -> Self {
        Self {
            ack: default_ack(),
            expected: default_expected(),
            search_op: None,
        }
    }
}

async fn handle_kv_delete(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((ns, key)): Path<(String, String)>,
    body: Bytes,
) -> Result<AxumResponse, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state
        .ctx
        .auth
        .authorize(principal.as_ref(), &ns, key.as_bytes(), Permission::Write)?;
    let req: HttpKvDeleteReq = if body.is_empty() {
        HttpKvDeleteReq::default()
    } else {
        serde_json::from_slice(&body).map_err(|err| {
            MembusError::InvalidArgument(format!("invalid delete request JSON: {err}"))
        })?
    };
    if let Some(op) = &req.search_op {
        authorize_search_op(&state.ctx.auth, principal.as_ref(), op)?;
    }
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_client_write_allowed(role_epoch)?;
    let idem_key = parse_idempotency_key_header(&headers)?;
    let principal_id = if state.ctx.auth.mode() == AuthMode::Enforced {
        principal.as_ref().map(|p| p.id.clone())
    } else {
        None
    };
    let idempotency = match idem_key {
        Some(k) => Some(IdempotencyContext {
            scope_namespace: ns.clone(),
            principal_id,
            is_tx: false,
            key: k,
            request_fingerprint_sha256: fingerprint_kv_delete(
                &ns,
                key.as_bytes(),
                req.expected,
                req.search_op.as_ref(),
            )?,
        }),
        None => None,
    };
    let ctx = state.ctx.clone();
    let storage = Arc::clone(&state.ctx.storage);
    let search = state.ctx.search.clone();
    let coord = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_client_write_fence(role_epoch)?;
        coordinated_remove_indexed_idempotent(
            &storage,
            search.as_ref(),
            &ns,
            key.as_bytes(),
            req.ack,
            req.expected,
            req.search_op,
            idempotency.as_ref(),
        )
    })
    .await
    .map_err(|e| MembusError::Storage(format!("kv delete failed: {e}")))??;
    Ok(coordinated_commit_response(coord))
}

#[derive(Deserialize)]
struct HttpTxReq {
    #[serde(default = "default_ack")]
    ack: Ack,
    #[serde(default)]
    guards: Vec<VersionGuard>,
    mutations: Vec<IndexedMutation>,
}

async fn handle_tx(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<HttpTxReq>,
) -> Result<AxumResponse, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.require_authenticated(principal.as_ref())?;
    for g in &req.guards {
        state
            .ctx
            .auth
            .authorize(principal.as_ref(), &g.namespace, &g.key, Permission::Read)?;
    }
    for m in &req.mutations {
        state.ctx.auth.authorize(
            principal.as_ref(),
            &m.mutation.namespace,
            &m.mutation.key,
            Permission::Write,
        )?;
        if let Some(idx) = &m.indexes {
            authorize_index_mutations(
                &state.ctx.auth,
                principal.as_ref(),
                &m.mutation.namespace,
                idx,
            )?;
        }
        if let Some(op) = &m.search_op {
            authorize_search_op(&state.ctx.auth, principal.as_ref(), op)?;
        }
    }
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_client_write_allowed(role_epoch)?;
    let idem_key = parse_idempotency_key_header(&headers)?;
    let principal_id = if state.ctx.auth.mode() == AuthMode::Enforced {
        principal.as_ref().map(|p| p.id.clone())
    } else {
        None
    };
    let idempotency = match idem_key {
        Some(k) => Some(IdempotencyContext {
            scope_namespace: tx_namespace_scope(&req.guards, &req.mutations),
            principal_id,
            is_tx: true,
            key: k,
            request_fingerprint_sha256: fingerprint_tx(&req.guards, &req.mutations)?,
        }),
        None => None,
    };
    let ctx = state.ctx.clone();
    let storage = Arc::clone(&state.ctx.storage);
    let search = state.ctx.search.clone();
    let coord = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_client_write_fence(role_epoch)?;
        coordinated_commit_indexed_tx_idempotent(
            &storage,
            search.as_ref(),
            &req.guards,
            req.mutations,
            req.ack,
            idempotency.as_ref(),
        )
    })
    .await
    .map_err(|e| MembusError::Storage(format!("tx task failed: {e}")))??;
    Ok(coordinated_commit_response(coord))
}

#[derive(Deserialize)]
struct HttpScanPrefixReq {
    namespace: String,
    prefix_utf8: Option<String>,
    prefix_b64: Option<String>,
    after_key_b64: Option<String>,
    limit: Option<usize>,
    max_bytes: Option<usize>,
}

async fn handle_scan_prefix(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<HttpScanPrefixReq>,
) -> Result<Json<crate::types::ScanPage>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    let prefix = decode_bytes_field(req.prefix_utf8, req.prefix_b64, "prefix")?;
    state.ctx.auth.authorize(
        principal.as_ref(),
        &req.namespace,
        &prefix,
        Permission::Read,
    )?;
    let defaults = ScanOptions::default();
    let options = ScanOptions {
        after_key: decode_opt_bytes_field(None, req.after_key_b64, "after_key")?,
        limit: req.limit.unwrap_or(defaults.limit),
        max_bytes: req.max_bytes.unwrap_or(defaults.max_bytes),
    };
    let storage = Arc::clone(&state.ctx.storage);
    let page = run_blocking_query(
        state.ctx.config.query_timeout,
        "scan_prefix",
        move || {
            let snap = storage.open_snapshot()?;
            storage.scan_prefix(&snap, &req.namespace, &prefix, &options)
        },
    )
    .await?;
    Ok(Json(page))
}

#[derive(Deserialize)]
struct HttpScanRangeReq {
    namespace: String,
    start_key_utf8: Option<String>,
    start_key_b64: Option<String>,
    end_key_utf8: Option<String>,
    end_key_b64: Option<String>,
    after_key_b64: Option<String>,
    limit: Option<usize>,
    max_bytes: Option<usize>,
}

async fn handle_scan_range(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<HttpScanRangeReq>,
) -> Result<Json<crate::types::ScanPage>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    let start_key = decode_bytes_field(req.start_key_utf8, req.start_key_b64, "start_key")?;
    let end_key = decode_bytes_field(req.end_key_utf8, req.end_key_b64, "end_key")?;
    state.ctx.auth.authorize_range(
        principal.as_ref(),
        &req.namespace,
        &start_key,
        &end_key,
        Permission::Read,
    )?;
    let defaults = ScanOptions::default();
    let options = ScanOptions {
        after_key: decode_opt_bytes_field(None, req.after_key_b64, "after_key")?,
        limit: req.limit.unwrap_or(defaults.limit),
        max_bytes: req.max_bytes.unwrap_or(defaults.max_bytes),
    };
    let storage = Arc::clone(&state.ctx.storage);
    let page = run_blocking_query(
        state.ctx.config.query_timeout,
        "scan_range",
        move || {
            let snap = storage.open_snapshot()?;
            storage.scan_range(&snap, &req.namespace, &start_key, &end_key, &options)
        },
    )
    .await?;
    Ok(Json(page))
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum HttpOrderedFilter {
    All,
    Prefix {
        prefix_utf8: Option<String>,
        prefix_b64: Option<String>,
    },
    Range {
        start_utf8: Option<String>,
        start_b64: Option<String>,
        end_utf8: Option<String>,
        end_b64: Option<String>,
    },
}

impl HttpOrderedFilter {
    fn into_filter(self) -> Result<OrderedKeyFilter, MembusError> {
        match self {
            Self::All => Ok(OrderedKeyFilter::All),
            Self::Prefix {
                prefix_utf8,
                prefix_b64,
            } => Ok(OrderedKeyFilter::Prefix(decode_bytes_field(
                prefix_utf8,
                prefix_b64,
                "prefix",
            )?)),
            Self::Range {
                start_utf8,
                start_b64,
                end_utf8,
                end_b64,
            } => Ok(OrderedKeyFilter::Range {
                start: decode_bytes_field(start_utf8, start_b64, "start")?,
                end: decode_bytes_field(end_utf8, end_b64, "end")?,
            }),
        }
    }
}

#[derive(Deserialize)]
struct HttpOrderedQueryReq {
    namespace: String,
    index_id: String,
    filter: Option<HttpOrderedFilter>,
    after_cursor_b64: Option<String>,
    limit: Option<usize>,
    max_bytes: Option<usize>,
}

async fn handle_ordered_query(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<HttpOrderedQueryReq>,
) -> Result<Json<crate::types::OrderedIndexPage>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.authorize(
        principal.as_ref(),
        &req.namespace,
        b"",
        Permission::Read,
    )?;
    let defaults = OrderedQueryOptions::default();
    let options = OrderedQueryOptions {
        filter: match req.filter {
            Some(f) => f.into_filter()?,
            None => OrderedKeyFilter::All,
        },
        after_cursor: decode_opt_bytes_field(None, req.after_cursor_b64, "after_cursor")?,
        limit: req.limit.unwrap_or(defaults.limit),
        max_bytes: req.max_bytes.unwrap_or(defaults.max_bytes),
    };
    let storage = Arc::clone(&state.ctx.storage);
    let page = run_blocking_query(
        state.ctx.config.query_timeout,
        "ordered_index_query",
        move || {
            let snap = storage.open_snapshot()?;
            storage.query_ordered_index(&snap, &req.namespace, &req.index_id, &options)
        },
    )
    .await?;
    Ok(Json(page))
}

#[derive(Deserialize)]
struct HttpNeighborsReq {
    namespace: String,
    index_id: String,
    node_id_utf8: Option<String>,
    node_id_b64: Option<String>,
    #[serde(default = "default_graph_dir")]
    direction: GraphDirection,
    rel_filter_utf8: Option<String>,
    rel_filter_b64: Option<String>,
    after_cursor_b64: Option<String>,
    limit: Option<usize>,
}

fn default_graph_dir() -> GraphDirection {
    GraphDirection::Out
}

async fn handle_graph_neighbors(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<HttpNeighborsReq>,
) -> Result<Json<crate::types::NeighborPage>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.authorize(
        principal.as_ref(),
        &req.namespace,
        b"",
        Permission::Read,
    )?;
    let node_id = decode_bytes_field(req.node_id_utf8, req.node_id_b64, "node_id")?;
    let options = NeighborQueryOptions {
        direction: req.direction,
        rel_filter: decode_opt_bytes_field(req.rel_filter_utf8, req.rel_filter_b64, "rel_filter")?,
        after_cursor: decode_opt_bytes_field(None, req.after_cursor_b64, "after_cursor")?,
        limit: req.limit.unwrap_or(NeighborQueryOptions::default().limit),
    };
    let storage = Arc::clone(&state.ctx.storage);
    let page = run_blocking_query(
        state.ctx.config.query_timeout,
        "graph_neighbors",
        move || {
            let snap = storage.open_snapshot()?;
            storage.query_neighbors(&snap, &req.namespace, &req.index_id, &node_id, &options)
        },
    )
    .await?;
    Ok(Json(page))
}

#[derive(Deserialize)]
struct HttpBfsReq {
    namespace: String,
    index_id: String,
    seed_node_utf8: Option<String>,
    seed_node_b64: Option<String>,
    #[serde(default = "default_graph_dir")]
    direction: GraphDirection,
    rel_filter_utf8: Option<String>,
    rel_filter_b64: Option<String>,
    max_hops: Option<u8>,
    max_nodes: Option<usize>,
}

async fn handle_graph_bfs(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<HttpBfsReq>,
) -> Result<Json<crate::types::BfsResult>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.authorize(
        principal.as_ref(),
        &req.namespace,
        b"",
        Permission::Read,
    )?;
    let seed_node = decode_bytes_field(req.seed_node_utf8, req.seed_node_b64, "seed_node")?;
    let defaults = BfsOptions::default();
    let options = BfsOptions {
        direction: req.direction,
        rel_filter: decode_opt_bytes_field(req.rel_filter_utf8, req.rel_filter_b64, "rel_filter")?,
        max_hops: req.max_hops.unwrap_or(defaults.max_hops),
        max_nodes: req.max_nodes.unwrap_or(defaults.max_nodes),
    };
    let storage = Arc::clone(&state.ctx.storage);
    let res = run_blocking_query(
        state.ctx.config.query_timeout,
        "graph_bfs",
        move || {
            let snap = storage.open_snapshot()?;
            storage.bfs_traverse(&snap, &req.namespace, &req.index_id, &seed_node, &options)
        },
    )
    .await?;
    Ok(Json(res))
}

#[derive(Deserialize)]
struct BlobPutQuery {
    ack: Option<String>,
}

#[derive(Serialize)]
struct BlobPutResp {
    sha256_hex: String,
}

async fn handle_blob_put(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Query(q): Query<BlobPutQuery>,
    body: Bytes,
) -> Result<Json<BlobPutResp>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state
        .ctx
        .auth
        .authorize(principal.as_ref(), &ns, b"", Permission::Write)?;
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_client_write_allowed(role_epoch)?;
    let ack = parse_ack_param(q.ack.as_deref())?;
    let ctx = state.ctx.clone();
    let storage = Arc::clone(&state.ctx.storage);
    let data = body.to_vec();
    let digest = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_client_write_fence(role_epoch)?;
        storage.put_cas_blob(&ns, &data, ack)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("put_cas_blob failed: {e}")))??;
    Ok(Json(BlobPutResp {
        sha256_hex: encode_sha256_hex(&digest),
    }))
}

async fn handle_blob_get(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((ns, sha256_hex)): Path<(String, String)>,
) -> Result<AxumResponse, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state
        .ctx
        .auth
        .authorize(principal.as_ref(), &ns, b"", Permission::Read)?;
    let digest = parse_sha256_hex(&sha256_hex)?;
    let storage = Arc::clone(&state.ctx.storage);
    let data = tokio::task::spawn_blocking(move || storage.get_cas_blob(&ns, &digest))
        .await
        .map_err(|e| MembusError::Storage(format!("get_cas_blob failed: {e}")))??;
    match data {
        Some(bytes) => Ok((
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/octet-stream")],
            bytes,
        )
            .into_response()),
        None => Err(HttpError(MembusError::NotFound("blob not found".to_owned()))),
    }
}

async fn handle_blob_head(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((ns, sha256_hex)): Path<(String, String)>,
) -> Result<StatusCode, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state
        .ctx
        .auth
        .authorize(principal.as_ref(), &ns, b"", Permission::Read)?;
    let digest = parse_sha256_hex(&sha256_hex)?;
    let storage = Arc::clone(&state.ctx.storage);
    let exists = tokio::task::spawn_blocking(move || storage.has_cas_blob(&ns, &digest))
        .await
        .map_err(|e| MembusError::Storage(format!("has_cas_blob failed: {e}")))??;
    if exists {
        Ok(StatusCode::OK)
    } else {
        Err(HttpError(MembusError::NotFound("blob not found".to_owned())))
    }
}

#[derive(Deserialize)]
struct LeaseAcquireReq {
    namespace: String,
    resource: String,
    holder: String,
    ttl_ms: u64,
}

async fn handle_lease_acquire(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<LeaseAcquireReq>,
) -> Result<Json<crate::types::LeaseGrant>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.authorize(
        principal.as_ref(),
        &req.namespace,
        req.resource.as_bytes(),
        Permission::Write,
    )?;
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_client_write_allowed(role_epoch)?;
    let ctx = state.ctx.clone();
    let leases = Arc::clone(&state.ctx.leases);
    let grant = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_client_write_fence(role_epoch)?;
        leases.acquire_lease(&req.namespace, &req.resource, &req.holder, req.ttl_ms)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("acquire_lease failed: {e}")))??;
    Ok(Json(grant))
}

#[derive(Deserialize)]
struct LeaseRenewReq {
    namespace: String,
    resource: String,
    holder: String,
    generation: u64,
    token: String,
    ttl_ms: u64,
}

async fn handle_lease_renew(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<LeaseRenewReq>,
) -> Result<Json<crate::types::LeaseGrant>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.authorize(
        principal.as_ref(),
        &req.namespace,
        req.resource.as_bytes(),
        Permission::Write,
    )?;
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_client_write_allowed(role_epoch)?;
    let ctx = state.ctx.clone();
    let leases = Arc::clone(&state.ctx.leases);
    let grant = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_client_write_fence(role_epoch)?;
        leases.renew_lease(
            &req.namespace,
            &req.resource,
            &req.holder,
            req.generation,
            &req.token,
            req.ttl_ms,
        )
    })
    .await
    .map_err(|e| MembusError::Storage(format!("renew_lease failed: {e}")))??;
    Ok(Json(grant))
}

#[derive(Deserialize)]
struct LeaseTokenReq {
    namespace: String,
    resource: String,
    holder: String,
    generation: u64,
    token: String,
}

#[derive(Serialize)]
struct BoolResp {
    ok: bool,
}

async fn handle_lease_release(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<LeaseTokenReq>,
) -> Result<Json<BoolResp>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.authorize(
        principal.as_ref(),
        &req.namespace,
        req.resource.as_bytes(),
        Permission::Write,
    )?;
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_client_write_allowed(role_epoch)?;
    let ctx = state.ctx.clone();
    let leases = Arc::clone(&state.ctx.leases);
    tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_client_write_fence(role_epoch)?;
        leases.release_lease(
            &req.namespace,
            &req.resource,
            &req.holder,
            req.generation,
            &req.token,
        )
    })
    .await
    .map_err(|e| MembusError::Storage(format!("release_lease failed: {e}")))??;
    Ok(Json(BoolResp { ok: true }))
}

async fn handle_lease_validate(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<LeaseTokenReq>,
) -> Result<Json<BoolResp>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.authorize(
        principal.as_ref(),
        &req.namespace,
        req.resource.as_bytes(),
        Permission::Read,
    )?;
    let leases = Arc::clone(&state.ctx.leases);
    let valid = tokio::task::spawn_blocking(move || {
        leases.validate_lease(
            &req.namespace,
            &req.resource,
            &req.holder,
            req.generation,
            &req.token,
        )
    })
    .await
    .map_err(|e| MembusError::Storage(format!("validate_lease failed: {e}")))??;
    Ok(Json(BoolResp { ok: valid }))
}

async fn handle_lease_inspect(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((ns, resource)): Path<(String, String)>,
) -> Result<AxumResponse, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.authorize(
        principal.as_ref(),
        &ns,
        resource.as_bytes(),
        Permission::Read,
    )?;
    let leases = Arc::clone(&state.ctx.leases);
    let lease = tokio::task::spawn_blocking(move || leases.inspect_lease(&ns, &resource))
        .await
        .map_err(|e| MembusError::Storage(format!("inspect_lease failed: {e}")))??;
    match lease {
        Some(grant) => Ok(Json(grant).into_response()),
        None => Err(HttpError(MembusError::NotFound(
            "active lease not found".to_owned(),
        ))),
    }
}

#[derive(Deserialize)]
struct RegisterCollectionReq {
    expected_version: Option<u32>,
    descriptor: CollectionDescriptor,
}

async fn handle_catalog_register(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<RegisterCollectionReq>,
) -> Result<Json<CollectionDescriptor>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state
        .ctx
        .auth
        .authorize_admin(principal.as_ref(), Some(&req.descriptor.namespace))?;
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_catalog_write_allowed(role_epoch)?;
    let ctx = state.ctx.clone();
    let catalog = Arc::clone(&state.ctx.catalog);
    let desc = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_catalog_write_fence(role_epoch)?;
        catalog.register_collection(req.expected_version, req.descriptor)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("register_collection failed: {e}")))??;
    Ok(Json(desc))
}

async fn handle_catalog_list(
    State(state): State<HttpState>,
    headers: HeaderMap,
) -> Result<Json<Vec<CollectionDescriptor>>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.require_authenticated(principal.as_ref())?;
    let catalog = Arc::clone(&state.ctx.catalog);
    let mut list = tokio::task::spawn_blocking(move || catalog.list_collections())
        .await
        .map_err(|e| MembusError::Storage(format!("list_collections failed: {e}")))??;
    if state.ctx.auth.mode() == AuthMode::Enforced {
        list.retain(|desc| {
            state
                .ctx
                .auth
                .authorize(principal.as_ref(), &desc.namespace, b"", Permission::Read)
                .is_ok()
        });
    }
    Ok(Json(list))
}

async fn handle_catalog_get(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
) -> Result<AxumResponse, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state
        .ctx
        .auth
        .authorize(principal.as_ref(), &ns, b"", Permission::Read)?;
    let catalog = Arc::clone(&state.ctx.catalog);
    let desc = tokio::task::spawn_blocking(move || catalog.get_collection(&ns))
        .await
        .map_err(|e| MembusError::Storage(format!("get_collection failed: {e}")))??;
    match desc {
        Some(c) => Ok(Json(c).into_response()),
        None => Err(HttpError(MembusError::NotFound(
            "collection not found".to_owned(),
        ))),
    }
}

async fn handle_catalog_get_version(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((ns, ver)): Path<(String, u32)>,
) -> Result<AxumResponse, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state
        .ctx
        .auth
        .authorize(principal.as_ref(), &ns, b"", Permission::Read)?;
    let catalog = Arc::clone(&state.ctx.catalog);
    let desc = tokio::task::spawn_blocking(move || catalog.get_collection_version(&ns, ver))
        .await
        .map_err(|e| MembusError::Storage(format!("get_collection_version failed: {e}")))??;
    match desc {
        Some(c) => Ok(Json(c).into_response()),
        None => Err(HttpError(MembusError::NotFound(
            "collection version not found".to_owned(),
        ))),
    }
}

#[derive(Deserialize)]
struct BeginMigrationReq {
    expected_version: u32,
    target_version: u32,
    target_sha256: [u8; 32],
    target_schema_bytes: Vec<u8>,
}

async fn handle_migration_begin(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Json(req): Json<BeginMigrationReq>,
) -> Result<Json<CollectionDescriptor>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state
        .ctx
        .auth
        .authorize_admin(principal.as_ref(), Some(&ns))?;
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_catalog_write_allowed(role_epoch)?;
    let ctx = state.ctx.clone();
    let catalog = Arc::clone(&state.ctx.catalog);
    let desc = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_catalog_write_fence(role_epoch)?;
        catalog.begin_migration(
            &ns,
            req.expected_version,
            req.target_version,
            req.target_sha256,
            req.target_schema_bytes,
        )
    })
    .await
    .map_err(|e| MembusError::Storage(format!("begin_migration failed: {e}")))??;
    Ok(Json(desc))
}

#[derive(Deserialize)]
struct CommitMigrationReq {
    target_version: u32,
    verified_through: u64,
}

async fn handle_migration_commit(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Json(req): Json<CommitMigrationReq>,
) -> Result<Json<CollectionDescriptor>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state
        .ctx
        .auth
        .authorize_admin(principal.as_ref(), Some(&ns))?;
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_catalog_write_allowed(role_epoch)?;
    let ctx = state.ctx.clone();
    let catalog = Arc::clone(&state.ctx.catalog);
    let desc = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_catalog_write_fence(role_epoch)?;
        catalog.commit_migration(&ns, req.target_version, req.verified_through)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("commit_migration failed: {e}")))??;
    Ok(Json(desc))
}

#[derive(Deserialize)]
struct AbortMigrationReq {
    target_version: u32,
}

async fn handle_migration_abort(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Json(req): Json<AbortMigrationReq>,
) -> Result<Json<CollectionDescriptor>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state
        .ctx
        .auth
        .authorize_admin(principal.as_ref(), Some(&ns))?;
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_catalog_write_allowed(role_epoch)?;
    let ctx = state.ctx.clone();
    let catalog = Arc::clone(&state.ctx.catalog);
    let desc = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_catalog_write_fence(role_epoch)?;
        catalog.abort_migration(&ns, req.target_version)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("abort_migration failed: {e}")))??;
    Ok(Json(desc))
}

#[derive(Deserialize)]
struct AdvanceMigrationReq {
    target_version: u32,
    verified_through: u64,
}

async fn handle_migration_advance(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Json(req): Json<AdvanceMigrationReq>,
) -> Result<Json<CollectionDescriptor>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state
        .ctx
        .auth
        .authorize_admin(principal.as_ref(), Some(&ns))?;
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_catalog_write_allowed(role_epoch)?;
    let ctx = state.ctx.clone();
    let catalog = Arc::clone(&state.ctx.catalog);
    let desc = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_catalog_write_fence(role_epoch)?;
        catalog.advance_migration_verification(&ns, req.target_version, req.verified_through)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("advance_migration_verification failed: {e}")))??;
    Ok(Json(desc))
}

#[derive(Deserialize)]
struct ReverseShadowReq {
    target_version: u32,
    verified_through: u64,
}

async fn handle_migration_reverse_shadow(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Json(req): Json<ReverseShadowReq>,
) -> Result<Json<CollectionDescriptor>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state
        .ctx
        .auth
        .authorize_admin(principal.as_ref(), Some(&ns))?;
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_catalog_write_allowed(role_epoch)?;
    let ctx = state.ctx.clone();
    let catalog = Arc::clone(&state.ctx.catalog);
    let desc = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_catalog_write_fence(role_epoch)?;
        catalog.enter_reverse_shadow(&ns, req.target_version, req.verified_through)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("enter_reverse_shadow failed: {e}")))??;
    Ok(Json(desc))
}

#[derive(Deserialize)]
struct RollbackMigrationReq {
    rollback_version: u32,
}

async fn handle_migration_rollback(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Json(req): Json<RollbackMigrationReq>,
) -> Result<Json<CollectionDescriptor>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state
        .ctx
        .auth
        .authorize_admin(principal.as_ref(), Some(&ns))?;
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_catalog_write_allowed(role_epoch)?;
    let ctx = state.ctx.clone();
    let catalog = Arc::clone(&state.ctx.catalog);
    let desc = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_catalog_write_fence(role_epoch)?;
        catalog.rollback_migration(&ns, req.rollback_version)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("rollback_migration failed: {e}")))??;
    Ok(Json(desc))
}

#[derive(Deserialize)]
struct ContractMigrationReq {
    expected_version: u32,
}

async fn handle_migration_contract(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Json(req): Json<ContractMigrationReq>,
) -> Result<Json<CollectionDescriptor>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state
        .ctx
        .auth
        .authorize_admin(principal.as_ref(), Some(&ns))?;
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_catalog_write_allowed(role_epoch)?;
    let ctx = state.ctx.clone();
    let catalog = Arc::clone(&state.ctx.catalog);
    let desc = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_catalog_write_fence(role_epoch)?;
        catalog.contract_migration(&ns, req.expected_version)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("contract_migration failed: {e}")))??;
    Ok(Json(desc))
}

#[derive(Default, Deserialize)]
struct HttpNamespaceDigestReq {
    #[serde(default)]
    mode: ParityDigestMode,
    prefix_utf8: Option<String>,
    prefix_b64: Option<String>,
}

async fn handle_namespace_digest(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    body: Bytes,
) -> Result<Json<NamespaceParityDigest>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    let req: HttpNamespaceDigestReq = if body.is_empty() {
        HttpNamespaceDigestReq::default()
    } else {
        serde_json::from_slice(&body).map_err(|err| {
            MembusError::InvalidArgument(format!("invalid digest request JSON: {err}"))
        })?
    };
    let key_prefix = decode_bytes_field(req.prefix_utf8, req.prefix_b64, "prefix")?;
    state
        .ctx
        .auth
        .authorize(principal.as_ref(), &ns, &key_prefix, Permission::Read)?;
    let storage = Arc::clone(&state.ctx.storage);
    let catalog = Arc::clone(&state.ctx.catalog);
    let node_id = state.ctx.node_id().to_owned();
    let mode = req.mode;
    let timeout = state.ctx.config.query_timeout;
    let digest = run_blocking_query(timeout, "namespace_digest", move || {
        compute_namespace_parity_digest_with_timeout(
            storage.as_ref(),
            catalog.as_ref(),
            &node_id,
            &ns,
            &key_prefix,
            mode,
            timeout,
        )
    })
    .await?;
    Ok(Json(digest))
}

#[derive(Deserialize)]
struct AdminRoleTransitionReq {
    expected_role: SlotRole,
    expected_role_epoch: u64,
    new_role: SlotRole,
}

async fn handle_admin_role_transition(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<AdminRoleTransitionReq>,
) -> Result<Json<SlotRoleState>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.authorize_admin(principal.as_ref(), None)?;
    let ctx = state.ctx.clone();
    let next = tokio::task::spawn_blocking(move || {
        ctx.transition_slot_role(req.expected_role, req.expected_role_epoch, req.new_role)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("transition_slot_role failed: {e}")))??;
    Ok(Json(next))
}

#[derive(Deserialize)]
struct HttpReplicationApplyReq {
    delta: ReplicatedDelta,
    #[serde(default = "default_ack")]
    ack: Ack,
    #[serde(default)]
    route_origin: Option<String>,
}

async fn handle_replication_apply(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<HttpReplicationApplyReq>,
) -> Result<AxumResponse, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.authorize(
        principal.as_ref(),
        &req.delta.namespace,
        &req.delta.key,
        Permission::Write,
    )?;
    if let Some(idx) = &req.delta.indexes {
        authorize_index_mutations(
            &state.ctx.auth,
            principal.as_ref(),
            &req.delta.namespace,
            idx,
        )?;
    }
    if let Some(op) = &req.delta.search_op {
        authorize_search_op(&state.ctx.auth, principal.as_ref(), op)?;
    }
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_replicated_apply_allowed(role_epoch)?;
    let header_origin = parse_route_origin_header(&headers)?;
    let body_origin = match req.route_origin.as_deref() {
        Some(raw) => validate_route_origin_value(raw, "route_origin field")?,
        None => None,
    };
    let route_origin = match (header_origin, body_origin) {
        (Some(h), Some(b)) if h != b => {
            return Err(HttpError(MembusError::InvalidArgument(format!(
                "conflicting X-Membus-Route-Origin header '{h}' and body route_origin '{b}'"
            ))));
        }
        (Some(h), _) => Some(h),
        (None, Some(b)) => Some(b),
        (None, None) => None,
    };
    let ctx = state.ctx.clone();
    let storage = Arc::clone(&state.ctx.storage);
    let search = state.ctx.search.clone();
    let local_node_id = state.ctx.node_id().to_owned();
    let coord = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_replicated_apply_fence(role_epoch)?;
        coordinated_apply_replicated_delta(
            &storage,
            search.as_ref(),
            &local_node_id,
            route_origin.as_deref(),
            &req.delta,
            req.ack,
        )
    })
    .await
    .map_err(|e| MembusError::Storage(format!("replication_apply failed: {e}")))??;
    Ok(coordinated_replicated_apply_response(coord))
}

#[derive(Deserialize)]
struct IndexDocReq {
    doc: SearchDocument,
    #[serde(default = "default_ack")]
    ack: Ack,
}

async fn handle_search_index_doc(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<IndexDocReq>,
) -> Result<AxumResponse, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.authorize(
        principal.as_ref(),
        &req.doc.namespace,
        req.doc.doc_id.as_bytes(),
        Permission::Write,
    )?;
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_client_write_allowed(role_epoch)?;
    let ctx = state.ctx.clone();
    let search = state.ctx.search.clone().ok_or_else(|| {
        MembusError::InvalidArgument("search engine is not enabled on this server".to_owned())
    })?;
    let coord = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_client_write_fence(role_epoch)?;
        coordinated_index_document(&search, req.doc, req.ack)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("index_document failed: {e}")))??;
    Ok(coordinated_commit_response(coord))
}

#[derive(Deserialize)]
struct SearchDeleteQuery {
    ack: Option<String>,
}

async fn handle_search_delete_doc(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path((ns, doc_id)): Path<(String, String)>,
    Query(q): Query<SearchDeleteQuery>,
) -> Result<AxumResponse, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.authorize(
        principal.as_ref(),
        &ns,
        doc_id.as_bytes(),
        Permission::Write,
    )?;
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_client_write_allowed(role_epoch)?;
    let ack = parse_ack_param(q.ack.as_deref())?;
    let ctx = state.ctx.clone();
    let search = state.ctx.search.clone().ok_or_else(|| {
        MembusError::InvalidArgument("search engine is not enabled on this server".to_owned())
    })?;
    let coord = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_client_write_fence(role_epoch)?;
        coordinated_delete_search_document(&search, &ns, &doc_id, ack)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("delete_search_document failed: {e}")))??;
    Ok(coordinated_commit_response(coord))
}

#[derive(Deserialize)]
struct RevokePrefixReq {
    namespace: String,
    prefix: String,
    #[serde(default = "default_ack")]
    ack: Ack,
}

async fn handle_search_revoke_prefix(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<RevokePrefixReq>,
) -> Result<AxumResponse, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state
        .ctx
        .auth
        .authorize_admin(principal.as_ref(), Some(&req.namespace))?;
    let role_epoch = parse_role_epoch_header(&headers)?;
    state.ctx.check_client_write_allowed(role_epoch)?;
    let ctx = state.ctx.clone();
    let search = state.ctx.search.clone().ok_or_else(|| {
        MembusError::InvalidArgument("search engine is not enabled on this server".to_owned())
    })?;
    let coord = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_client_write_fence(role_epoch)?;
        coordinated_revoke_search_prefix(&search, &req.namespace, &req.prefix, req.ack)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("revoke_search_prefix failed: {e}")))??;
    Ok(coordinated_commit_response(coord))
}

async fn handle_search_query(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(query): Json<SearchQuery>,
) -> Result<Json<crate::types::SearchPage>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    let ns = query.namespace.as_deref().unwrap_or("*");
    state
        .ctx
        .auth
        .authorize(principal.as_ref(), ns, b"", Permission::Read)?;
    let search = state.ctx.search.clone().ok_or_else(|| {
        MembusError::InvalidArgument("search engine is not enabled on this server".to_owned())
    })?;
    let page = run_blocking_query(
        state.ctx.config.query_timeout,
        "search",
        move || search.search(&query),
    )
    .await?;
    Ok(Json(page))
}

enum StreamSub {
    EachChange(EachChangeSubscription),
    Live(LiveCoalescedSubscription),
}

impl StreamSub {
    async fn next_event(&mut self) -> Result<Option<SignalEvent>, MembusError> {
        match self {
            Self::EachChange(s) => s.next_event().await,
            Self::Live(s) => s.next_event().await,
        }
    }
}

#[derive(Deserialize)]
struct SseQuery {
    mode: Option<String>,
    prefix: Option<String>,
    prefix_b64: Option<String>,
    epoch: Option<u64>,
    since_seq: Option<u64>,
    max_pending: Option<usize>,
}

async fn handle_signals_sse(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Query(q): Query<SseQuery>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    let key_prefix = decode_bytes_field(q.prefix, q.prefix_b64, "prefix")?;
    state.ctx.auth.authorize(
        principal.as_ref(),
        &ns,
        &key_prefix,
        Permission::Read,
    )?;

    let mode = q.mode.as_deref().unwrap_or("each");
    let (mut sub, epoch, start_seq) = if mode.eq_ignore_ascii_case("live") {
        let s = state
            .ctx
            .broker
            .subscribe_live(&ns, key_prefix, q.max_pending)?;
        let ep = s.epoch();
        let seq = state.ctx.storage.latest_seq();
        (StreamSub::Live(s), ep, seq)
    } else if mode.eq_ignore_ascii_case("each") {
        let ep = q.epoch.unwrap_or(0);
        let since = q.since_seq.unwrap_or_else(|| state.ctx.storage.latest_seq());
        let s = state
            .ctx
            .broker
            .subscribe_each_change(&ns, key_prefix, ep, since)?;
        let ep_out = s.epoch();
        let seq_out = s.cursor_seq();
        (StreamSub::EachChange(s), ep_out, seq_out)
    } else {
        return Err(HttpError(MembusError::InvalidArgument(format!(
            "invalid SSE mode '{mode}' (expected 'each' or 'live')"
        ))));
    };

    let stream_guard = state.ctx.try_acquire_stream_guard()?;
    let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(256);
    let mut shutdown_rx = state.shutdown_rx.clone();
    let active_connections = Arc::clone(&state.ctx.active_connections);

    tokio::spawn(async move {
        let _stream_guard = stream_guard;
        let _conn_guard = ConnectionGuard::acquire(&active_connections);
        let init_json = serde_json::json!({
            "epoch": epoch,
            "start_seq": start_seq,
        })
        .to_string();
        if !send_sse_or_shutdown(
            &tx,
            Ok(Event::default().event("subscribed").data(init_json)),
            &mut shutdown_rx,
        )
        .await
        {
            return;
        }

        loop {
            tokio::select! {
                biased;
                _ = wait_shutdown_ref(&mut shutdown_rx) => break,
                _ = tx.closed() => break,
                next = sub.next_event() => match next {
                    Ok(Some(ev)) => {
                        let Ok(data) = serde_json::to_string(&ev) else {
                            continue;
                        };
                        let sse_ev = Event::default()
                            .event("signal")
                            .id(ev.seq.to_string())
                            .data(data);
                        if !send_sse_or_shutdown(&tx, Ok(sse_ev), &mut shutdown_rx).await {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        let payload = serde_json::json!({ "error": err.to_string() }).to_string();
                        let _ = send_sse_or_shutdown(
                            &tx,
                            Ok(Event::default().event("error").data(payload)),
                            &mut shutdown_rx,
                        )
                        .await;
                        break;
                    }
                }
            }
        }
    });

    Ok(Sse::new(ReceiverStream::new(rx))
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

async fn send_sse_or_shutdown(
    tx: &mpsc::Sender<Result<Event, Infallible>>,
    item: Result<Event, Infallible>,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> bool {
    if *shutdown_rx.borrow() {
        return false;
    }
    tokio::select! {
        biased;
        _ = wait_shutdown_ref(shutdown_rx) => false,
        res = tx.send(item) => res.is_ok(),
    }
}

struct AuthPrincipal(Option<Principal>);

impl axum::extract::FromRequestParts<HttpState> for AuthPrincipal {
    type Rejection = HttpError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &HttpState,
    ) -> Result<Self, Self::Rejection> {
        let principal = authenticate_http(state, &parts.headers)?;
        Ok(Self(principal))
    }
}

async fn handle_ws_upgrade(
    AuthPrincipal(initial_principal): AuthPrincipal,
    State(state): State<HttpState>,
    ws: WebSocketUpgrade,
) -> Result<AxumResponse, HttpError> {
    let max_msg = state.ctx.config.max_frame_body;
    Ok(ws
        .max_message_size(max_msg)
        .max_frame_size(max_msg)
        .on_upgrade(move |socket| handle_ws_socket(socket, state, initial_principal)))
}

struct WsStreamTable {
    closed: bool,
    auth_epoch: u64,
    authenticated: Option<Principal>,
    streams: HashMap<u64, oneshot::Sender<()>>,
}

struct WsConnState {
    ctx: ServerContext,
    active_streams: Mutex<WsStreamTable>,
    /// Priority control channel for RPC replies, pongs, `subscribed`, and `stream_end` frames.
    control_tx: mpsc::Sender<Message>,
    /// Bounded data channel for push `signal` frames (non-blocking `try_send`).
    data_tx: mpsc::Sender<Message>,
}

impl WsConnState {
    fn snapshot_auth(&self) -> (Option<Principal>, u64) {
        let table = self.active_streams.lock();
        (table.authenticated.clone(), table.auth_epoch)
    }

    fn close_all_streams(&self) {
        let mut table = self.active_streams.lock();
        table.closed = true;
        for (_id, tx) in table.streams.drain() {
            let _ = tx.send(());
        }
    }
}

#[derive(Deserialize)]
struct WsEnvelope {
    id: u64,
    #[serde(flatten)]
    op: WsOp,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum WsOp {
    Ping,
    Health,
    Authenticate {
        token: String,
    },
    Get {
        namespace: String,
        key_utf8: Option<String>,
        key_b64: Option<String>,
    },
    Put {
        namespace: String,
        key_utf8: Option<String>,
        key_b64: Option<String>,
        value_utf8: Option<String>,
        value_b64: Option<String>,
        #[serde(default = "default_ack")]
        ack: Ack,
        #[serde(default = "default_expected")]
        expected: ExpectedVersion,
        indexes: Option<IndexMutations>,
        search_op: Option<SearchLogOp>,
    },
    Remove {
        namespace: String,
        key_utf8: Option<String>,
        key_b64: Option<String>,
        #[serde(default = "default_ack")]
        ack: Ack,
        #[serde(default = "default_expected")]
        expected: ExpectedVersion,
        search_op: Option<SearchLogOp>,
    },
    Search {
        query: SearchQuery,
    },
    SubscribeEach {
        namespace: String,
        prefix_utf8: Option<String>,
        prefix_b64: Option<String>,
        epoch: Option<u64>,
        since_seq: Option<u64>,
    },
    SubscribeLive {
        namespace: String,
        prefix_utf8: Option<String>,
        prefix_b64: Option<String>,
        max_pending: Option<usize>,
    },
    Cancel {
        stream_id: u64,
    },
}

fn ws_ok_msg(id: u64, kind: &str, data: serde_json::Value) -> Message {
    Message::Text(
        serde_json::json!({
            "id": id,
            "ok": true,
            "kind": kind,
            "data": data,
        })
        .to_string()
        .into(),
    )
}

fn ws_err_msg(id: u64, err: &MembusError) -> Message {
    Message::Text(
        serde_json::json!({
            "id": id,
            "ok": false,
            "error": err.to_string(),
        })
        .to_string()
        .into(),
    )
}

async fn send_ws_or_shutdown(
    tx: &mpsc::Sender<Message>,
    msg: Message,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> bool {
    if *shutdown_rx.borrow() {
        return false;
    }
    tokio::select! {
        biased;
        _ = wait_shutdown_ref(shutdown_rx) => false,
        res = tx.send(msg) => res.is_ok(),
    }
}

fn apply_ws_authenticate_inline(conn: &WsConnState, id: u64, token: &str) -> Message {
    let auth_res = conn.ctx.auth.authenticate_token(Some(token));
    let mut table = conn.active_streams.lock();
    match auth_res {
        Ok(Some(p)) => {
            let pid = p.id.clone();
            let changed = table.authenticated.as_ref() != Some(&p);
            table.authenticated = Some(p);
            if changed {
                table.auth_epoch = table.auth_epoch.saturating_add(1);
                for (_id, tx) in table.streams.drain() {
                    let _ = tx.send(());
                }
            }
            ws_ok_msg(id, "authenticated", serde_json::json!({ "principal_id": pid }))
        }
        Ok(None) => ws_ok_msg(
            id,
            "authenticated",
            serde_json::json!({ "principal_id": "anonymous" }),
        ),
        Err(err) => {
            table.authenticated = None;
            table.auth_epoch = table.auth_epoch.saturating_add(1);
            for (_id, tx) in table.streams.drain() {
                let _ = tx.send(());
            }
            ws_err_msg(id, &err)
        }
    }
}

async fn handle_ws_socket(
    socket: WebSocket,
    state: HttpState,
    initial_principal: Option<Principal>,
) {
    let _conn_guard = ConnectionGuard::acquire(&state.ctx.active_connections);
    let (mut ws_tx, mut ws_rx) = socket.split();
    let control_cap = state
        .ctx
        .config
        .max_subscriptions_per_conn
        .saturating_mul(2)
        .saturating_add(128)
        .max(256);
    let (control_tx, mut control_rx) = mpsc::channel::<Message>(control_cap);
    let (data_tx, mut data_rx) =
        mpsc::channel::<Message>(state.ctx.config.outbound_queue_capacity);

    let mut writer_shutdown = state.shutdown_rx.clone();
    let writer_task = tokio::spawn(async move {
        loop {
            let msg_opt = tokio::select! {
                biased;
                _ = wait_shutdown_ref(&mut writer_shutdown) => None,
                ctrl = control_rx.recv() => ctrl,
                data = data_rx.recv() => data,
            };
            let Some(m) = msg_opt else {
                break;
            };
            let send_ok = tokio::select! {
                biased;
                _ = wait_shutdown_ref(&mut writer_shutdown) => false,
                res = tokio::time::timeout(Duration::from_secs(5), ws_tx.send(m)) => {
                    matches!(res, Ok(Ok(())))
                }
            };
            if !send_ok {
                break;
            }
        }
        if !*writer_shutdown.borrow() {
            let _ = tokio::time::timeout(Duration::from_millis(500), ws_tx.close()).await;
        }
    });

    let credit_window = Arc::new(Semaphore::new(
        state.ctx.config.max_in_flight_per_conn.max(1),
    ));
    let conn = Arc::new(WsConnState {
        ctx: state.ctx.clone(),
        active_streams: Mutex::new(WsStreamTable {
            closed: false,
            auth_epoch: 0,
            authenticated: initial_principal,
            streams: HashMap::new(),
        }),
        control_tx,
        data_tx,
    });

    let mut shutdown_rx = state.shutdown_rx.clone();
    loop {
        let next_msg = tokio::select! {
            biased;
            _ = wait_shutdown_ref(&mut shutdown_rx) => break,
            item = ws_rx.next() => item,
        };

        let Some(Ok(msg)) = next_msg else {
            break;
        };

        match msg {
            Message::Text(text) => {
                let env: WsEnvelope = match serde_json::from_str(&text) {
                    Ok(e) => e,
                    Err(err) => {
                        let reply = ws_err_msg(
                            0,
                            &MembusError::Protocol(format!("invalid ws JSON: {err}")),
                        );
                        if !send_ws_or_shutdown(&conn.control_tx, reply, &mut shutdown_rx).await {
                            break;
                        }
                        continue;
                    }
                };
                // Handle Authenticate & Cancel synchronously inline BEFORE subsequent messages.
                match env.op {
                    WsOp::Authenticate { token } => {
                        let reply = apply_ws_authenticate_inline(&conn, env.id, &token);
                        if !send_ws_or_shutdown(&conn.control_tx, reply, &mut shutdown_rx).await {
                            break;
                        }
                        continue;
                    }
                    WsOp::Cancel { stream_id } => {
                        if let Some(tx) = conn.active_streams.lock().streams.remove(&stream_id) {
                            let _ = tx.send(());
                        }
                        let ack = ws_ok_msg(
                            env.id,
                            "cancelled",
                            serde_json::json!({ "stream_id": stream_id }),
                        );
                        if !send_ws_or_shutdown(&conn.control_tx, ack, &mut shutdown_rx).await {
                            break;
                        }
                        continue;
                    }
                    _ => {}
                }

                let permit = match Arc::clone(&credit_window).try_acquire_owned() {
                    Ok(p) => p,
                    Err(_) => {
                        let busy = ws_err_msg(
                            env.id,
                            &MembusError::Busy {
                                retry_after_ms: 5,
                                reason: "ws per-connection credit window exhausted".to_owned(),
                            },
                        );
                        if !send_ws_or_shutdown(&conn.control_tx, busy, &mut shutdown_rx).await {
                            break;
                        }
                        continue;
                    }
                };

                let conn_ref = Arc::clone(&conn);
                let task_shutdown = shutdown_rx.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    handle_ws_envelope(conn_ref, env, task_shutdown).await;
                });
            }
            Message::Ping(payload) => {
                let _ = conn.control_tx.try_send(Message::Pong(payload));
            }
            Message::Close(_) => break,
            Message::Pong(_) | Message::Binary(_) => {}
        }
    }

    conn.close_all_streams();
    drop(conn);
    let _ = writer_task.await;
}

async fn handle_ws_envelope(
    conn: Arc<WsConnState>,
    env: WsEnvelope,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let id = env.id;
    let (principal, captured_auth_epoch) = conn.snapshot_auth();
    match env.op {
        WsOp::Ping => {
            let _ = send_ws_or_shutdown(
                &conn.control_tx,
                ws_ok_msg(id, "pong", serde_json::json!({})),
                &mut shutdown_rx,
            )
            .await;
        }
        WsOp::Authenticate { token } => {
            let reply = apply_ws_authenticate_inline(&conn, id, &token);
            let _ = send_ws_or_shutdown(&conn.control_tx, reply, &mut shutdown_rx).await;
        }
        WsOp::Health => {
            if let Err(err) = conn.ctx.auth.require_authenticated(principal.as_ref()) {
                let _ = send_ws_or_shutdown(&conn.control_tx, ws_err_msg(id, &err), &mut shutdown_rx)
                    .await;
                return;
            }
            let status = conn.ctx.health_status();
            let _ = send_ws_or_shutdown(
                &conn.control_tx,
                ws_ok_msg(
                    id,
                    "health",
                    serde_json::to_value(status).unwrap_or_default(),
                ),
                &mut shutdown_rx,
            )
            .await;
        }
        WsOp::Get {
            namespace,
            key_utf8,
            key_b64,
        } => {
            let key = match decode_bytes_field(key_utf8, key_b64, "key") {
                Ok(k) => k,
                Err(err) => {
                    let _ = send_ws_or_shutdown(
                        &conn.control_tx,
                        ws_err_msg(id, &err),
                        &mut shutdown_rx,
                    )
                    .await;
                    return;
                }
            };
            if let Err(err) =
                conn.ctx
                    .auth
                    .authorize(principal.as_ref(), &namespace, &key, Permission::Read)
            {
                let _ = send_ws_or_shutdown(&conn.control_tx, ws_err_msg(id, &err), &mut shutdown_rx)
                    .await;
                return;
            }
            let storage = Arc::clone(&conn.ctx.storage);
            let res = tokio::task::spawn_blocking(move || storage.get(&namespace, &key))
                .await
                .unwrap_or_else(|e| Err(MembusError::Storage(format!("ws get failed: {e}"))));
            match res {
                Ok(rec) => {
                    let _ = send_ws_or_shutdown(
                        &conn.control_tx,
                        ws_ok_msg(id, "get", serde_json::to_value(rec).unwrap_or_default()),
                        &mut shutdown_rx,
                    )
                    .await;
                }
                Err(err) => {
                    let _ = send_ws_or_shutdown(
                        &conn.control_tx,
                        ws_err_msg(id, &err),
                        &mut shutdown_rx,
                    )
                    .await;
                }
            }
        }
        WsOp::Put {
            namespace,
            key_utf8,
            key_b64,
            value_utf8,
            value_b64,
            ack,
            expected,
            indexes,
            search_op,
        } => {
            let key = match decode_bytes_field(key_utf8, key_b64, "key") {
                Ok(k) => k,
                Err(err) => {
                    let _ = send_ws_or_shutdown(
                        &conn.control_tx,
                        ws_err_msg(id, &err),
                        &mut shutdown_rx,
                    )
                    .await;
                    return;
                }
            };
            let value = match decode_bytes_field(value_utf8, value_b64, "value") {
                Ok(v) => v,
                Err(err) => {
                    let _ = send_ws_or_shutdown(
                        &conn.control_tx,
                        ws_err_msg(id, &err),
                        &mut shutdown_rx,
                    )
                    .await;
                    return;
                }
            };
            if let Err(err) =
                conn.ctx
                    .auth
                    .authorize(principal.as_ref(), &namespace, &key, Permission::Write)
            {
                let _ = send_ws_or_shutdown(&conn.control_tx, ws_err_msg(id, &err), &mut shutdown_rx)
                    .await;
                return;
            }
            if let Some(idx) = &indexes {
                if let Err(err) =
                    authorize_index_mutations(&conn.ctx.auth, principal.as_ref(), &namespace, idx)
                {
                    let _ = send_ws_or_shutdown(
                        &conn.control_tx,
                        ws_err_msg(id, &err),
                        &mut shutdown_rx,
                    )
                    .await;
                    return;
                }
            }
            if let Some(op) = &search_op {
                if let Err(err) = authorize_search_op(&conn.ctx.auth, principal.as_ref(), op) {
                    let _ = send_ws_or_shutdown(
                        &conn.control_tx,
                        ws_err_msg(id, &err),
                        &mut shutdown_rx,
                    )
                    .await;
                    return;
                }
            }
            let ctx = conn.ctx.clone();
            let storage = Arc::clone(&conn.ctx.storage);
            let search = conn.ctx.search.clone();
            let res = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                coordinated_put_indexed(
                    &storage,
                    search.as_ref(),
                    &namespace,
                    &key,
                    &value,
                    ack,
                    expected,
                    indexes.as_ref(),
                    search_op,
                )
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("ws put failed: {e}"))));
            match res {
                Ok(coord) => {
                    let payload = HttpCommitResp {
                        commit: coord.commit,
                        search_sync: coord.search_sync,
                    };
                    let _ = send_ws_or_shutdown(
                        &conn.control_tx,
                        ws_ok_msg(
                            id,
                            "commit",
                            serde_json::to_value(payload).unwrap_or_default(),
                        ),
                        &mut shutdown_rx,
                    )
                    .await;
                }
                Err(err) => {
                    let _ = send_ws_or_shutdown(
                        &conn.control_tx,
                        ws_err_msg(id, &err),
                        &mut shutdown_rx,
                    )
                    .await;
                }
            }
        }
        WsOp::Remove {
            namespace,
            key_utf8,
            key_b64,
            ack,
            expected,
            search_op,
        } => {
            let key = match decode_bytes_field(key_utf8, key_b64, "key") {
                Ok(k) => k,
                Err(err) => {
                    let _ = send_ws_or_shutdown(
                        &conn.control_tx,
                        ws_err_msg(id, &err),
                        &mut shutdown_rx,
                    )
                    .await;
                    return;
                }
            };
            if let Err(err) =
                conn.ctx
                    .auth
                    .authorize(principal.as_ref(), &namespace, &key, Permission::Write)
            {
                let _ = send_ws_or_shutdown(&conn.control_tx, ws_err_msg(id, &err), &mut shutdown_rx)
                    .await;
                return;
            }
            if let Some(op) = &search_op {
                if let Err(err) = authorize_search_op(&conn.ctx.auth, principal.as_ref(), op) {
                    let _ = send_ws_or_shutdown(
                        &conn.control_tx,
                        ws_err_msg(id, &err),
                        &mut shutdown_rx,
                    )
                    .await;
                    return;
                }
            }
            let ctx = conn.ctx.clone();
            let storage = Arc::clone(&conn.ctx.storage);
            let search = conn.ctx.search.clone();
            let res = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                coordinated_remove_indexed(
                    &storage,
                    search.as_ref(),
                    &namespace,
                    &key,
                    ack,
                    expected,
                    search_op,
                )
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("ws remove failed: {e}"))));
            match res {
                Ok(coord) => {
                    let payload = HttpCommitResp {
                        commit: coord.commit,
                        search_sync: coord.search_sync,
                    };
                    let _ = send_ws_or_shutdown(
                        &conn.control_tx,
                        ws_ok_msg(
                            id,
                            "commit",
                            serde_json::to_value(payload).unwrap_or_default(),
                        ),
                        &mut shutdown_rx,
                    )
                    .await;
                }
                Err(err) => {
                    let _ = send_ws_or_shutdown(
                        &conn.control_tx,
                        ws_err_msg(id, &err),
                        &mut shutdown_rx,
                    )
                    .await;
                }
            }
        }
        WsOp::Search { query } => {
            let ns = query.namespace.as_deref().unwrap_or("*");
            if let Err(err) = conn
                .ctx
                .auth
                .authorize(principal.as_ref(), ns, b"", Permission::Read)
            {
                let _ = send_ws_or_shutdown(&conn.control_tx, ws_err_msg(id, &err), &mut shutdown_rx)
                    .await;
                return;
            }
            let Some(search) = conn.ctx.search.clone() else {
                let _ = send_ws_or_shutdown(
                    &conn.control_tx,
                    ws_err_msg(
                        id,
                        &MembusError::InvalidArgument(
                            "search engine is not enabled on this server".to_owned(),
                        ),
                    ),
                    &mut shutdown_rx,
                )
                .await;
                return;
            };
            let res = run_blocking_query(
                conn.ctx.config.query_timeout,
                "ws_search",
                move || search.search(&query),
            )
            .await;
            match res {
                Ok(page) => {
                    let _ = send_ws_or_shutdown(
                        &conn.control_tx,
                        ws_ok_msg(
                            id,
                            "search",
                            serde_json::to_value(page).unwrap_or_default(),
                        ),
                        &mut shutdown_rx,
                    )
                    .await;
                }
                Err(err) => {
                    let _ = send_ws_or_shutdown(
                        &conn.control_tx,
                        ws_err_msg(id, &err),
                        &mut shutdown_rx,
                    )
                    .await;
                }
            }
        }
        WsOp::SubscribeEach {
            namespace,
            prefix_utf8,
            prefix_b64,
            epoch,
            since_seq,
        } => {
            let prefix = match decode_bytes_field(prefix_utf8, prefix_b64, "prefix") {
                Ok(p) => p,
                Err(err) => {
                    let _ = send_ws_or_shutdown(
                        &conn.control_tx,
                        ws_err_msg(id, &err),
                        &mut shutdown_rx,
                    )
                    .await;
                    return;
                }
            };
            if let Err(err) =
                conn.ctx
                    .auth
                    .authorize(principal.as_ref(), &namespace, &prefix, Permission::Read)
            {
                let _ = send_ws_or_shutdown(&conn.control_tx, ws_err_msg(id, &err), &mut shutdown_rx)
                    .await;
                return;
            }
            let ep = epoch.unwrap_or(0);
            let since = since_seq.unwrap_or_else(|| conn.ctx.storage.latest_seq());
            match conn
                .ctx
                .broker
                .subscribe_each_change(&namespace, prefix, ep, since)
            {
                Ok(sub) => {
                    let ep_out = sub.epoch();
                    let seq_out = sub.cursor_seq();
                    launch_ws_subscription(
                        conn,
                        id,
                        captured_auth_epoch,
                        ep_out,
                        seq_out,
                        StreamSub::EachChange(sub),
                        shutdown_rx,
                    )
                    .await;
                }
                Err(err) => {
                    let _ = send_ws_or_shutdown(
                        &conn.control_tx,
                        ws_err_msg(id, &err),
                        &mut shutdown_rx,
                    )
                    .await;
                }
            }
        }
        WsOp::SubscribeLive {
            namespace,
            prefix_utf8,
            prefix_b64,
            max_pending,
        } => {
            let prefix = match decode_bytes_field(prefix_utf8, prefix_b64, "prefix") {
                Ok(p) => p,
                Err(err) => {
                    let _ = send_ws_or_shutdown(
                        &conn.control_tx,
                        ws_err_msg(id, &err),
                        &mut shutdown_rx,
                    )
                    .await;
                    return;
                }
            };
            if let Err(err) =
                conn.ctx
                    .auth
                    .authorize(principal.as_ref(), &namespace, &prefix, Permission::Read)
            {
                let _ = send_ws_or_shutdown(&conn.control_tx, ws_err_msg(id, &err), &mut shutdown_rx)
                    .await;
                return;
            }
            match conn.ctx.broker.subscribe_live(&namespace, prefix, max_pending) {
                Ok(sub) => {
                    let ep_out = sub.epoch();
                    let seq_out = conn.ctx.storage.latest_seq();
                    launch_ws_subscription(
                        conn,
                        id,
                        captured_auth_epoch,
                        ep_out,
                        seq_out,
                        StreamSub::Live(sub),
                        shutdown_rx,
                    )
                    .await;
                }
                Err(err) => {
                    let _ = send_ws_or_shutdown(
                        &conn.control_tx,
                        ws_err_msg(id, &err),
                        &mut shutdown_rx,
                    )
                    .await;
                }
            }
        }
        WsOp::Cancel { .. } => {}
    }
}

async fn launch_ws_subscription(
    conn: Arc<WsConnState>,
    stream_id: u64,
    captured_auth_epoch: u64,
    epoch: u64,
    start_seq: u64,
    mut sub: StreamSub,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let stream_guard = match conn.ctx.try_acquire_stream_guard() {
        Ok(guard) => guard,
        Err(err) => {
            let _ =
                send_ws_or_shutdown(&conn.control_tx, ws_err_msg(stream_id, &err), &mut shutdown_rx)
                    .await;
            return;
        }
    };
    let (cancel_tx, mut cancel_rx) = oneshot::channel::<()>();
    let register_result = {
        let mut table = conn.active_streams.lock();
        if table.closed {
            return;
        }
        if table.auth_epoch != captured_auth_epoch {
            Err(MembusError::Unauthorized(
                "ws connection authentication changed while opening subscription".to_owned(),
            ))
        } else if table.streams.contains_key(&stream_id) {
            Err(MembusError::Protocol(format!(
                "duplicate ws stream_id {stream_id}"
            )))
        } else if table.streams.len() >= conn.ctx.config.max_subscriptions_per_conn {
            Err(MembusError::Busy {
                retry_after_ms: 10,
                reason: "ws per-connection subscription limit reached".to_owned(),
            })
        } else {
            table.streams.insert(stream_id, cancel_tx);
            Ok(())
        }
    };
    if let Err(err) = register_result {
        let _ = send_ws_or_shutdown(&conn.control_tx, ws_err_msg(stream_id, &err), &mut shutdown_rx)
            .await;
        return;
    }

    let ack = ws_ok_msg(
        stream_id,
        "subscribed",
        serde_json::json!({ "epoch": epoch, "start_seq": start_seq }),
    );
    if !send_ws_or_shutdown(&conn.control_tx, ack, &mut shutdown_rx).await {
        conn.active_streams.lock().streams.remove(&stream_id);
        return;
    }

    tokio::spawn(async move {
        let _stream_guard = stream_guard;
        let end_err: Option<String> = loop {
            tokio::select! {
                biased;
                _ = &mut cancel_rx => break None,
                _ = wait_shutdown_ref(&mut shutdown_rx) => break None,
                next = sub.next_event() => match next {
                    Ok(Some(ev)) => {
                        let msg = Message::Text(
                            serde_json::json!({
                                "id": stream_id,
                                "kind": "signal",
                                "data": ev,
                            })
                            .to_string()
                            .into(),
                        );
                        // Non-blocking try_send ensures a slow subscriber cannot freeze the socket.
                        match conn.data_tx.try_send(msg) {
                            Ok(()) => {}
                            Err(TrySendError::Full(_)) => {
                                break Some("ws outbound queue overflowed".to_owned());
                            }
                            Err(TrySendError::Closed(_)) => break None,
                        }
                    }
                    Ok(None) => break None,
                    Err(err) => break Some(err.to_string()),
                }
            }
        };
        conn.active_streams.lock().streams.remove(&stream_id);
        let end = Message::Text(
            serde_json::json!({
                "id": stream_id,
                "kind": "stream_end",
                "error": end_err,
            })
            .to_string()
            .into(),
        );
        // Send `stream_end` on the priority control channel so overflowed data queues never drop it.
        let _ = send_ws_or_shutdown(&conn.control_tx, end, &mut shutdown_rx).await;
    });
}

async fn wait_shutdown_ref(rx: &mut watch::Receiver<bool>) {
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}

async fn wait_shutdown(mut rx: watch::Receiver<bool>) {
    wait_shutdown_ref(&mut rx).await;
}
