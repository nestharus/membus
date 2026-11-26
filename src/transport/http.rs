use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response as AxumResponse};
use axum::routing::{delete, get, post, put};
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use futures_util::{SinkExt, StreamExt};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch, Semaphore};
use tokio_stream::wrappers::ReceiverStream;

use crate::index::{BfsOptions, NeighborQueryOptions, OrderedKeyFilter, OrderedQueryOptions};
use crate::signals::{EachChangeSubscription, LiveCoalescedSubscription};
use crate::transport::connection::{
    authorize_index_mutations, authorize_search_op, resolve_checkpoint_path, ConnectionGuard,
};
use crate::transport::{Endpoint, ServerContext, ServerHandle};
use crate::types::{
    Ack, AuthMode, CollectionDescriptor, ExpectedVersion, GraphDirection, HealthStatus,
    IndexMutations, IndexedMutation, MembusError, Permission, Principal, ScanOptions,
    SearchDocument, SearchLogOp, SearchQuery, SignalEvent, VersionGuard,
};

#[derive(Clone)]
pub(crate) struct HttpState {
    ctx: ServerContext,
    shutdown_rx: watch::Receiver<bool>,
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
        let _ = axum::serve(listener, app)
            .with_graceful_shutdown(wait_shutdown(shutdown_rx))
            .await;
    });

    Ok(ServerHandle::new(
        Endpoint::Http(local_addr),
        shutdown_tx,
        accept_task,
        active_connections,
    ))
}

pub(crate) fn build_router(state: HttpState) -> Router {
    let max_body = state.ctx.config.max_frame_body;
    Router::new()
        .route("/v2/health", get(handle_health))
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
            "/v2/catalog/collections/{ns}/migrations/commit",
            post(handle_migration_commit),
        )
        .route(
            "/v2/catalog/collections/{ns}/migrations/abort",
            post(handle_migration_abort),
        )
        .route("/v2/search/documents", post(handle_search_index_doc))
        .route(
            "/v2/search/documents/{ns}/{*doc_id}",
            delete(handle_search_delete_doc),
        )
        .route("/v2/search/revoke-prefix", post(handle_search_revoke_prefix))
        .route("/v2/search/query", post(handle_search_query))
        .route("/v2/signals/{ns}/stream", get(handle_signals_sse))
        .route("/v2/ws", get(handle_ws_upgrade))
        .layer(DefaultBodyLimit::max(max_body))
        .with_state(state)
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

fn encode_sha256_hex(digest: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for &b in digest {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
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
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.require_authenticated(principal.as_ref())?;
    Ok(Json(HealthStatus {
        epoch: state.ctx.storage.epoch(),
        latest_seq: state.ctx.storage.latest_seq(),
        log_horizon_seq: state.ctx.storage.log_horizon_seq(),
        in_flight_ops: state.ctx.storage.in_flight_ops(),
        in_flight_bytes: state.ctx.storage.in_flight_bytes(),
        active_connections: state.ctx.active_connections(),
        active_subscriptions: state.ctx.broker.active_subscriptions(),
    }))
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
) -> Result<Json<crate::types::CommitResult>, HttpError> {
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
    let value = decode_bytes_field(req.value_utf8, req.value_b64, "value")?;
    let storage = Arc::clone(&state.ctx.storage);
    let search = state.ctx.search.clone();
    let commit = tokio::task::spawn_blocking(move || {
        let commit = storage.put_indexed(
            &ns,
            key.as_bytes(),
            &value,
            req.ack,
            req.expected,
            req.indexes.as_ref(),
            req.search_op.as_ref(),
        )?;
        if let (Some(engine), Some(op)) = (search, req.search_op) {
            engine.ingest_committed_op(commit.seq, 0, commit.epoch, op)?;
        }
        Ok::<_, MembusError>(commit)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("kv put failed: {e}")))??;
    Ok(Json(commit))
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
) -> Result<Json<crate::types::CommitResult>, HttpError> {
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
    let storage = Arc::clone(&state.ctx.storage);
    let search = state.ctx.search.clone();
    let commit = tokio::task::spawn_blocking(move || {
        let commit = storage.remove_indexed(
            &ns,
            key.as_bytes(),
            req.ack,
            req.expected,
            req.search_op.as_ref(),
        )?;
        if let (Some(engine), Some(op)) = (search, req.search_op) {
            engine.ingest_committed_op(commit.seq, 0, commit.epoch, op)?;
        }
        Ok::<_, MembusError>(commit)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("kv delete failed: {e}")))??;
    Ok(Json(commit))
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
) -> Result<Json<crate::types::CommitResult>, HttpError> {
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
    let storage = Arc::clone(&state.ctx.storage);
    let search = state.ctx.search.clone();
    let commit = tokio::task::spawn_blocking(move || {
        let commit = storage.commit_indexed_tx(&req.guards, &req.mutations, req.ack)?;
        if let Some(engine) = search {
            let mut op_idx = 0u32;
            for m in req.mutations {
                if let Some(op) = m.search_op {
                    engine.ingest_committed_op(commit.seq, op_idx, commit.epoch, op)?;
                    op_idx = op_idx.saturating_add(1);
                }
            }
        }
        Ok::<_, MembusError>(commit)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("tx task failed: {e}")))??;
    Ok(Json(commit))
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
    let page = tokio::task::spawn_blocking(move || {
        let snap = storage.open_snapshot()?;
        storage.scan_prefix(&snap, &req.namespace, &prefix, &options)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("scan_prefix failed: {e}")))??;
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
    let page = tokio::task::spawn_blocking(move || {
        let snap = storage.open_snapshot()?;
        storage.scan_range(&snap, &req.namespace, &start_key, &end_key, &options)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("scan_range failed: {e}")))??;
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
    let page = tokio::task::spawn_blocking(move || {
        let snap = storage.open_snapshot()?;
        storage.query_ordered_index(&snap, &req.namespace, &req.index_id, &options)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("ordered index query failed: {e}")))??;
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
    let page = tokio::task::spawn_blocking(move || {
        let snap = storage.open_snapshot()?;
        storage.query_neighbors(&snap, &req.namespace, &req.index_id, &node_id, &options)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("graph neighbors failed: {e}")))??;
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
    let res = tokio::task::spawn_blocking(move || {
        let snap = storage.open_snapshot()?;
        storage.bfs_traverse(&snap, &req.namespace, &req.index_id, &seed_node, &options)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("graph bfs failed: {e}")))??;
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
    let ack = parse_ack_param(q.ack.as_deref())?;
    let storage = Arc::clone(&state.ctx.storage);
    let data = body.to_vec();
    let digest = tokio::task::spawn_blocking(move || storage.put_cas_blob(&ns, &data, ack))
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
    let leases = Arc::clone(&state.ctx.leases);
    let grant = tokio::task::spawn_blocking(move || {
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
    let leases = Arc::clone(&state.ctx.leases);
    let grant = tokio::task::spawn_blocking(move || {
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
    let leases = Arc::clone(&state.ctx.leases);
    tokio::task::spawn_blocking(move || {
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
    let catalog = Arc::clone(&state.ctx.catalog);
    let desc = tokio::task::spawn_blocking(move || {
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
    let catalog = Arc::clone(&state.ctx.catalog);
    let desc = tokio::task::spawn_blocking(move || {
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
    let catalog = Arc::clone(&state.ctx.catalog);
    let desc = tokio::task::spawn_blocking(move || {
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
    let catalog = Arc::clone(&state.ctx.catalog);
    let desc = tokio::task::spawn_blocking(move || catalog.abort_migration(&ns, req.target_version))
        .await
        .map_err(|e| MembusError::Storage(format!("abort_migration failed: {e}")))??;
    Ok(Json(desc))
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
) -> Result<Json<crate::types::CommitResult>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.authorize(
        principal.as_ref(),
        &req.doc.namespace,
        req.doc.doc_id.as_bytes(),
        Permission::Write,
    )?;
    let search = state.ctx.search.clone().ok_or_else(|| {
        MembusError::InvalidArgument("search engine is not enabled on this server".to_owned())
    })?;
    let commit = tokio::task::spawn_blocking(move || search.index_document(req.doc, req.ack))
        .await
        .map_err(|e| MembusError::Storage(format!("index_document failed: {e}")))??;
    Ok(Json(commit))
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
) -> Result<Json<crate::types::CommitResult>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.authorize(
        principal.as_ref(),
        &ns,
        doc_id.as_bytes(),
        Permission::Write,
    )?;
    let ack = parse_ack_param(q.ack.as_deref())?;
    let search = state.ctx.search.clone().ok_or_else(|| {
        MembusError::InvalidArgument("search engine is not enabled on this server".to_owned())
    })?;
    let commit = tokio::task::spawn_blocking(move || search.delete_document(&ns, &doc_id, ack))
        .await
        .map_err(|e| MembusError::Storage(format!("delete_search_document failed: {e}")))??;
    Ok(Json(commit))
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
) -> Result<Json<crate::types::CommitResult>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state
        .ctx
        .auth
        .authorize_admin(principal.as_ref(), Some(&req.namespace))?;
    let search = state.ctx.search.clone().ok_or_else(|| {
        MembusError::InvalidArgument("search engine is not enabled on this server".to_owned())
    })?;
    let commit = tokio::task::spawn_blocking(move || {
        search.revoke_search_prefix(&req.namespace, &req.prefix, req.ack)
    })
    .await
    .map_err(|e| MembusError::Storage(format!("revoke_search_prefix failed: {e}")))??;
    Ok(Json(commit))
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
    let page = tokio::task::spawn_blocking(move || search.search(&query))
        .await
        .map_err(|e| MembusError::Storage(format!("search failed: {e}")))??;
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

    let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(256);
    let mut shutdown_rx = state.shutdown_rx.clone();
    let active_connections = Arc::clone(&state.ctx.active_connections);

    tokio::spawn(async move {
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
            let status = HealthStatus {
                epoch: conn.ctx.storage.epoch(),
                latest_seq: conn.ctx.storage.latest_seq(),
                log_horizon_seq: conn.ctx.storage.log_horizon_seq(),
                in_flight_ops: conn.ctx.storage.in_flight_ops(),
                in_flight_bytes: conn.ctx.storage.in_flight_bytes(),
                active_connections: conn.ctx.active_connections(),
                active_subscriptions: conn.ctx.broker.active_subscriptions(),
            };
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
            let storage = Arc::clone(&conn.ctx.storage);
            let search = conn.ctx.search.clone();
            let res = tokio::task::spawn_blocking(move || {
                let commit = storage.put_indexed(
                    &namespace,
                    &key,
                    &value,
                    ack,
                    expected,
                    indexes.as_ref(),
                    search_op.as_ref(),
                )?;
                if let (Some(engine), Some(op)) = (search, search_op) {
                    engine.ingest_committed_op(commit.seq, 0, commit.epoch, op)?;
                }
                Ok::<_, MembusError>(commit)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("ws put failed: {e}"))));
            match res {
                Ok(commit) => {
                    let _ = send_ws_or_shutdown(
                        &conn.control_tx,
                        ws_ok_msg(
                            id,
                            "commit",
                            serde_json::to_value(commit).unwrap_or_default(),
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
            let storage = Arc::clone(&conn.ctx.storage);
            let search = conn.ctx.search.clone();
            let res = tokio::task::spawn_blocking(move || {
                let commit =
                    storage.remove_indexed(&namespace, &key, ack, expected, search_op.as_ref())?;
                if let (Some(engine), Some(op)) = (search, search_op) {
                    engine.ingest_committed_op(commit.seq, 0, commit.epoch, op)?;
                }
                Ok::<_, MembusError>(commit)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("ws remove failed: {e}"))));
            match res {
                Ok(commit) => {
                    let _ = send_ws_or_shutdown(
                        &conn.control_tx,
                        ws_ok_msg(
                            id,
                            "commit",
                            serde_json::to_value(commit).unwrap_or_default(),
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
            let res = tokio::task::spawn_blocking(move || search.search(&query))
                .await
                .unwrap_or_else(|e| Err(MembusError::Storage(format!("ws search failed: {e}"))));
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
