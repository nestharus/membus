//! Data-plane Axum reverse proxy implementing Primary-first multi-route write-teeing,
//! deterministic idempotency key attachment, role-epoch forwarding, control-plane path
//! blocking, and strict echo-loop rejection.

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::extract::{Request, State};
use axum::http::header::{HeaderName, HeaderValue};
use axum::http::{HeaderMap, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::{Json, Router};
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::router::config::SlotId;
use crate::router::shadow_queue::{
    mutation_value_sha256, RouterRuntime, ShadowEnqueueStatus, ShadowItem,
};
use crate::router::upstream::{is_hop_by_hop, strip_hop_by_hop_headers};
use crate::types::{
    now_unix_ms, IndexMutations, IndexedMutation, MutationOp, ReplicatedDelta, ReplicationStamp,
    SearchLogOp,
};

const HEADER_ROUTE_PRIMARY: HeaderName = HeaderName::from_static("x-route-primary");
const HEADER_ROUTE_SHADOW_SYNC: HeaderName = HeaderName::from_static("x-route-shadow-sync");
const HEADER_IDEMPOTENCY_KEY: HeaderName = HeaderName::from_static("x-membus-idempotency-key");
const HEADER_IDEMPOTENT_REPLAY: HeaderName = HeaderName::from_static("x-membus-idempotent-replay");
const HEADER_COMMIT_TS_MS: HeaderName = HeaderName::from_static("x-membus-commit-ts-ms");
const HEADER_ROLE_EPOCH: HeaderName = HeaderName::from_static("x-membus-role-epoch");
const HEADER_ROUTE_ORIGIN: HeaderName = HeaderName::from_static("x-membus-route-origin");
const HEADER_ECHO_LOOP: HeaderName = HeaderName::from_static("x-membus-echo-loop");

#[derive(Clone)]
struct ProxyState {
    runtime: Arc<RouterRuntime>,
    max_body_bytes: usize,
}

#[derive(Serialize)]
struct ProxyErrorBody {
    error: &'static str,
    message: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RouteKind {
    Read,
    KvPut,
    KvDelete,
    Tx,
    BlobWrite,
    OtherWrite,
}

/// Build the data-plane Axum router bound to `--data-listen`.
pub fn build_data_router(runtime: Arc<RouterRuntime>, max_body_bytes: usize) -> Router {
    let state = ProxyState {
        runtime,
        max_body_bytes: max_body_bytes.max(1024),
    };
    Router::new()
        .fallback(handle_data_request)
        .with_state(state)
}

async fn handle_data_request(State(state): State<ProxyState>, req: Request) -> Response {
    let Ok(_in_flight_permit) = Arc::clone(state.runtime.data_concurrency_sem()).try_acquire_owned()
    else {
        return proxy_error(
            StatusCode::SERVICE_UNAVAILABLE,
            "busy",
            "data-plane concurrency limit reached".to_owned(),
        );
    };

    let method = req.method().clone();
    let uri = req.uri().clone();
    let path = uri.path().to_owned();
    let path_and_query = uri
        .path_and_query()
        .map(|pq| pq.as_str().to_owned())
        .unwrap_or_else(|| path.clone());

    let normalized_path = match validate_and_normalize_path(&path) {
        Ok(p) => p,
        Err(msg) => {
            return proxy_error(StatusCode::BAD_REQUEST, "invalid_argument", msg);
        }
    };

    if is_blocked_control_plane_path(&normalized_path) {
        return proxy_error(
            StatusCode::FORBIDDEN,
            "forbidden_control_plane_path",
            format!("path '{path}' is restricted to internal control-plane operations"),
        );
    }

    if is_unsupported_streaming_path(&normalized_path) {
        return proxy_error(
            StatusCode::NOT_IMPLEMENTED,
            "streaming_not_supported",
            format!("streaming endpoint '{path}' is not supported over the buffered A/B proxy"),
        );
    }

    // Strip standard and RFC 9110 `Connection`-nominated hop-by-hop headers BEFORE security
    // checks and proxy header injection.
    let mut headers = strip_hop_by_hop_headers(req.headers());

    let route_kind = classify_route(&method, &normalized_path);

    // Strict echo-loop validation across ALL `X-Membus-Route-Origin` header occurrences.
    if let Some(err_resp) =
        validate_and_strip_route_origin(&mut headers, route_kind != RouteKind::Read)
    {
        return err_resp;
    }

    let body = match to_bytes(req.into_body(), state.max_body_bytes).await {
        Ok(b) => b,
        Err(err) => {
            return proxy_error(
                StatusCode::PAYLOAD_TOO_LARGE,
                "frame_too_large",
                format!("request body exceeds router limit: {err}"),
            );
        }
    };

    let topo = state.runtime.topology_snapshot();
    let primary_slot = topo.primary_slot;

    if route_kind == RouteKind::Read {
        return forward_read(&state.runtime, primary_slot, method, &path_and_query, headers, body)
            .await;
    }

    // Attach deterministic per-boot idempotency key if caller omitted one.
    if !headers.contains_key(&HEADER_IDEMPOTENCY_KEY)
        && matches!(
            route_kind,
            RouteKind::KvPut | RouteKind::KvDelete | RouteKind::Tx | RouteKind::BlobWrite
        )
    {
        let req_id = state.runtime.next_request_id();
        let idem_key = synthesize_idempotency_key(
            state.runtime.boot_nonce(),
            primary_slot,
            topo.primary_role_epoch,
            req_id,
            &method,
            &path_and_query,
            &body,
        );
        if let Ok(val) = HeaderValue::from_str(&idem_key) {
            headers.insert(HEADER_IDEMPOTENCY_KEY, val);
        }
    }

    // Attach current Primary role epoch if caller omitted one and epoch is known.
    if !headers.contains_key(&HEADER_ROLE_EPOCH) && topo.primary_role_epoch > 0 {
        if let Ok(val) = HeaderValue::from_str(&topo.primary_role_epoch.to_string()) {
            headers.insert(HEADER_ROLE_EPOCH, val);
        }
    }

    let pre_commit_ts_ms = now_unix_ms();

    let primary_client = state.runtime.client_for(primary_slot);
    let upstream_resp = match primary_client
        .forward_client_raw(
            method,
            &path_and_query,
            headers,
            body.clone(),
            state.runtime.primary_timeout(),
        )
        .await
    {
        Ok(resp) => resp,
        Err(err) => {
            return proxy_error(
                StatusCode::BAD_GATEWAY,
                "primary_upstream_error",
                format!("primary {primary_slot} request failed: {err}"),
            );
        }
    };

    // Only mirror to Shadow upon authoritative Primary 2xx commit (and NEVER on idempotent replay).
    let shadow_sync = if upstream_resp.status.is_success() {
        handle_primary_write_shadow(
            &state.runtime,
            &PrimaryWriteContext {
                primary_slot,
                route_kind,
                path: &path,
                req_body: &body,
                resp_headers: &upstream_resp.headers,
                resp_body: &upstream_resp.body,
                fallback_ts_ms: pre_commit_ts_ms,
            },
        )
    } else {
        None
    };

    build_client_response(
        upstream_resp.status,
        upstream_resp.headers,
        upstream_resp.body,
        primary_slot,
        shadow_sync,
    )
}

async fn forward_read(
    runtime: &RouterRuntime,
    primary_slot: SlotId,
    method: Method,
    path_and_query: &str,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let client = runtime.client_for(primary_slot);
    match client
        .forward_client_raw(method, path_and_query, headers, body, runtime.primary_timeout())
        .await
    {
        Ok(resp) => build_client_response(resp.status, resp.headers, resp.body, primary_slot, None),
        Err(err) => proxy_error(
            StatusCode::BAD_GATEWAY,
            "primary_upstream_error",
            format!("primary {primary_slot} read failed: {err}"),
        ),
    }
}

fn validate_and_strip_route_origin(headers: &mut HeaderMap, is_write: bool) -> Option<Response> {
    let mut values = headers.get_all(&HEADER_ROUTE_ORIGIN).iter();
    if let Some(first) = values.next() {
        if values.next().is_some() {
            return Some(proxy_error(
                StatusCode::BAD_REQUEST,
                "invalid_argument",
                "duplicate X-Membus-Route-Origin header is not permitted".to_owned(),
            ));
        }
        let Ok(origin_str) = first.to_str() else {
            return Some(proxy_error(
                StatusCode::BAD_REQUEST,
                "invalid_argument",
                "X-Membus-Route-Origin header must be valid UTF-8".to_owned(),
            ));
        };
        if is_write && is_self_route_origin(origin_str) {
            let mut resp = proxy_error(
                StatusCode::CONFLICT,
                "echo_loop_rejected",
                format!(
                    "inbound write rejected: X-Membus-Route-Origin '{origin_str}' matches router echo loop"
                ),
            );
            resp.headers_mut()
                .insert(HEADER_ECHO_LOOP, HeaderValue::from_static("rejected"));
            return Some(resp);
        }
    }
    // Always strip untrusted client-supplied X-Membus-Route-Origin before forwarding.
    headers.remove(&HEADER_ROUTE_ORIGIN);
    None
}

struct PrimaryWriteContext<'a> {
    primary_slot: SlotId,
    route_kind: RouteKind,
    path: &'a str,
    req_body: &'a [u8],
    resp_headers: &'a HeaderMap,
    resp_body: &'a [u8],
    fallback_ts_ms: u64,
}

fn handle_primary_write_shadow(
    runtime: &RouterRuntime,
    ctx: &PrimaryWriteContext<'_>,
) -> Option<ShadowEnqueueStatus> {
    if !runtime.topology_snapshot().shadow_enabled {
        return Some(ShadowEnqueueStatus::Disabled);
    }

    // Never enqueue shadow replication on idempotent replays (prevents LWW resurrection).
    if is_idempotent_replay(ctx.resp_headers) {
        return Some(if runtime.is_parity_dirty() {
            ShadowEnqueueStatus::Deferred
        } else {
            ShadowEnqueueStatus::Queued
        });
    }

    let origin_seq = parse_commit_seq(ctx.resp_body);
    let commit_ts_ms = parse_commit_ts_ms(ctx.resp_headers).unwrap_or(ctx.fallback_ts_ms);

    let deltas = match extract_deltas(
        ctx.primary_slot,
        ctx.route_kind,
        ctx.path,
        ctx.req_body,
        origin_seq,
        commit_ts_ms,
    ) {
        Ok(d) if !d.is_empty() => d,
        _ => {
            // Any 2xx mutating request we cannot mirror (e.g., BlobWrite, catalog/lease/search
            // OtherWrite, or parse mismatch) MUST invalidate parity rather than drifting silently.
            runtime.mark_parity_dirty();
            return Some(ShadowEnqueueStatus::Deferred);
        }
    };

    for delta in &deltas {
        runtime.record_touched_namespace(&delta.namespace);
    }

    let approx_bytes = ctx
        .req_body
        .len()
        .max(deltas.iter().map(estimate_delta_bytes).sum());
    let item = ShadowItem {
        origin_slot: ctx.primary_slot,
        origin_seq,
        approx_bytes,
        deltas,
    };
    Some(runtime.enqueue_shadow(item))
}

/// Reject ambiguous/traversal paths (`//`, raw `\`, encoded `%2f`/`%5c`, dot-segments `.`/`..`,
/// malformed percent escapes, or decoded ASCII control bytes) and return the percent-decoded,
/// ASCII-lowercased path used strictly for security blocking and route classification.
fn validate_and_normalize_path(raw_path: &str) -> Result<String, String> {
    if !raw_path.starts_with('/') {
        return Err(format!("request path '{raw_path}' must start with '/'"));
    }
    if raw_path.contains("//") {
        return Err(format!(
            "request path '{raw_path}' contains prohibited empty segment ('//')"
        ));
    }
    if raw_path.contains('\\') {
        return Err(format!(
            "request path '{raw_path}' contains prohibited backslash"
        ));
    }

    let mut normalized_segments = Vec::new();
    for seg in raw_path[1..].split('/') {
        if seg.is_empty() {
            // Only permitted as a single trailing slash (e.g., `/v2/admin/`).
            continue;
        }
        let decoded_bytes = percent_decode(seg)
            .map_err(|()| format!("malformed percent-encoding in request path '{raw_path}'"))?;
        if decoded_bytes
            .iter()
            .any(|&b| b == b'/' || b == b'\\' || b < 0x20 || b == 0x7f)
        {
            return Err(format!(
                "request path '{raw_path}' contains prohibited encoded slash or control byte"
            ));
        }
        let decoded_str = String::from_utf8(decoded_bytes)
            .map_err(|_| format!("request path '{raw_path}' contains non-UTF-8 segment"))?;
        if decoded_str == "." || decoded_str == ".." {
            return Err(format!(
                "request path '{raw_path}' contains prohibited dot-segment '{decoded_str}'"
            ));
        }
        normalized_segments.push(decoded_str.to_ascii_lowercase());
    }

    if normalized_segments.is_empty() {
        Ok("/".to_owned())
    } else {
        Ok(format!("/{}", normalized_segments.join("/")))
    }
}

fn is_blocked_control_plane_path(path: &str) -> bool {
    let trimmed = path.trim_end_matches('/');
    trimmed == "/v2/admin"
        || trimmed.starts_with("/v2/admin/")
        || trimmed == "/v2/checkpoint"
        || trimmed == "/v2/updates"
        || trimmed == "/v2/replication/apply"
}

fn is_unsupported_streaming_path(path: &str) -> bool {
    let trimmed = path.trim_end_matches('/');
    trimmed == "/v2/ws"
        || (trimmed.starts_with("/v2/signals/") && trimmed.ends_with("/stream"))
}

fn classify_route(method: &Method, path: &str) -> RouteKind {
    if matches!(*method, Method::GET | Method::HEAD | Method::OPTIONS) {
        return RouteKind::Read;
    }
    if *method == Method::POST && is_read_only_post(path) {
        return RouteKind::Read;
    }
    if *method == Method::PUT && path.starts_with("/v2/kv/") {
        return RouteKind::KvPut;
    }
    if *method == Method::DELETE && path.starts_with("/v2/kv/") {
        return RouteKind::KvDelete;
    }
    if *method == Method::POST && path == "/v2/tx" {
        return RouteKind::Tx;
    }
    if matches!(*method, Method::PUT | Method::POST) && path.starts_with("/v2/blobs") {
        return RouteKind::BlobWrite;
    }
    RouteKind::OtherWrite
}

fn is_read_only_post(path: &str) -> bool {
    matches!(
        path,
        "/v2/scan/prefix"
            | "/v2/scan/range"
            | "/v2/index/ordered/query"
            | "/v2/index/graph/neighbors"
            | "/v2/index/graph/bfs"
            | "/v2/search/query"
            | "/v2/leases/validate"
    ) || (path.starts_with("/v2/namespaces/") && path.ends_with("/digest"))
}

fn is_self_route_origin(origin: &str) -> bool {
    let trimmed = origin.trim();
    trimmed.len() >= 7 && trimmed[..7].eq_ignore_ascii_case("router:")
}

fn is_idempotent_replay(headers: &HeaderMap) -> bool {
    headers
        .get(&HEADER_IDEMPOTENT_REPLAY)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|s| s.trim().eq_ignore_ascii_case("true"))
}

fn parse_commit_ts_ms(headers: &HeaderMap) -> Option<u64> {
    headers
        .get(&HEADER_COMMIT_TS_MS)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
}

fn synthesize_idempotency_key(
    boot_nonce: &[u8; 16],
    primary_slot: SlotId,
    role_epoch: u64,
    req_id: u64,
    method: &Method,
    path_and_query: &str,
    body: &[u8],
) -> String {
    let mut h = Sha256::new();
    h.update(b"membus-ab-router:idem:v2\0");
    h.update(boot_nonce);
    h.update(primary_slot.as_str().as_bytes());
    h.update(role_epoch.to_be_bytes());
    h.update(req_id.to_be_bytes());
    h.update(method.as_str().as_bytes());
    h.update(b"\0");
    h.update(path_and_query.as_bytes());
    h.update(b"\0");
    h.update(body);
    let digest = h.finalize();
    let mut hex = String::with_capacity(40);
    hex.push_str("router-");
    for b in &digest[..16] {
        use std::fmt::Write as _;
        let _ = write!(hex, "{b:02x}");
    }
    hex
}

fn parse_commit_seq(resp_body: &[u8]) -> u64 {
    #[derive(Deserialize)]
    struct SeqOnly {
        seq: Option<u64>,
    }
    serde_json::from_slice::<SeqOnly>(resp_body)
        .ok()
        .and_then(|s| s.seq)
        .unwrap_or(0)
}

#[derive(Deserialize)]
struct KvPutShadowBody {
    value_utf8: Option<String>,
    value_b64: Option<String>,
    indexes: Option<IndexMutations>,
    search_op: Option<SearchLogOp>,
}

#[derive(Default, Deserialize)]
struct KvDeleteShadowBody {
    search_op: Option<SearchLogOp>,
}

#[derive(Deserialize)]
struct TxShadowBody {
    mutations: Vec<IndexedMutation>,
}

fn extract_deltas(
    primary_slot: SlotId,
    route_kind: RouteKind,
    path: &str,
    req_body: &[u8],
    origin_seq: u64,
    ts_ms: u64,
) -> Result<Vec<ReplicatedDelta>, ()> {
    let origin_node_id = primary_slot.as_str().to_owned();
    match route_kind {
        RouteKind::KvPut => {
            let (namespace, key) = parse_kv_path(path)?;
            let parsed: KvPutShadowBody = serde_json::from_slice(req_body).map_err(|_| ())?;
            let value = match (parsed.value_utf8, parsed.value_b64) {
                (Some(s), None) => s.into_bytes(),
                (None, Some(b64)) => B64.decode(b64).map_err(|_| ())?,
                (None, None) => Vec::new(),
                (Some(_), Some(_)) => return Err(()),
            };
            let op = MutationOp::Put { value };
            let stamp = ReplicationStamp {
                timestamp_ms: ts_ms,
                origin_node_id,
                origin_seq,
                value_sha256: mutation_value_sha256(&op),
            };
            Ok(vec![ReplicatedDelta {
                stamp,
                namespace,
                key,
                op,
                indexes: parsed.indexes,
                search_op: parsed.search_op,
            }])
        }
        RouteKind::KvDelete => {
            let (namespace, key) = parse_kv_path(path)?;
            let parsed: KvDeleteShadowBody = if req_body.is_empty() {
                KvDeleteShadowBody::default()
            } else {
                serde_json::from_slice(req_body).map_err(|_| ())?
            };
            let op = MutationOp::Remove;
            let stamp = ReplicationStamp {
                timestamp_ms: ts_ms,
                origin_node_id,
                origin_seq,
                value_sha256: [0u8; 32],
            };
            Ok(vec![ReplicatedDelta {
                stamp,
                namespace,
                key,
                op,
                indexes: None,
                search_op: parsed.search_op,
            }])
        }
        RouteKind::Tx => {
            let parsed: TxShadowBody = serde_json::from_slice(req_body).map_err(|_| ())?;
            let mut out = Vec::with_capacity(parsed.mutations.len());
            for item in parsed.mutations {
                let stamp = ReplicationStamp {
                    timestamp_ms: ts_ms,
                    origin_node_id: origin_node_id.clone(),
                    origin_seq,
                    value_sha256: mutation_value_sha256(&item.mutation.op),
                };
                out.push(ReplicatedDelta {
                    stamp,
                    namespace: item.mutation.namespace,
                    key: item.mutation.key,
                    op: item.mutation.op,
                    indexes: item.indexes,
                    search_op: item.search_op,
                });
            }
            Ok(out)
        }
        RouteKind::BlobWrite | RouteKind::OtherWrite | RouteKind::Read => Err(()),
    }
}

fn parse_kv_path(path: &str) -> Result<(String, Vec<u8>), ()> {
    let rest = path.strip_prefix("/v2/kv/").ok_or(())?;
    let (raw_ns, raw_key) = rest.split_once('/').ok_or(())?;
    let ns_bytes = percent_decode(raw_ns)?;
    let namespace = String::from_utf8(ns_bytes).map_err(|_| ())?;
    let key = percent_decode(raw_key)?;
    Ok((namespace, key))
}

fn percent_decode(raw: &str) -> Result<Vec<u8>, ()> {
    let bytes = raw.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut idx = 0;
    while idx < bytes.len() {
        if bytes[idx] == b'%' {
            if idx + 2 >= bytes.len() {
                return Err(());
            }
            let hi = decode_hex_nibble(bytes[idx + 1])?;
            let lo = decode_hex_nibble(bytes[idx + 2])?;
            out.push((hi << 4) | lo);
            idx += 3;
        } else {
            out.push(bytes[idx]);
            idx += 1;
        }
    }
    Ok(out)
}

fn decode_hex_nibble(b: u8) -> Result<u8, ()> {
    match b {
        b'0'..=b'9' => Ok(b - b'0'),
        b'a'..=b'f' => Ok(b - b'a' + 10),
        b'A'..=b'F' => Ok(b - b'A' + 10),
        _ => Err(()),
    }
}

fn estimate_delta_bytes(delta: &ReplicatedDelta) -> usize {
    let val_len = match &delta.op {
        MutationOp::Put { value } => value.len(),
        MutationOp::Remove => 0,
    };
    delta.namespace.len() + delta.key.len() + val_len + 128
}

fn build_client_response(
    status: StatusCode,
    upstream_headers: HeaderMap,
    body: Bytes,
    primary_slot: SlotId,
    shadow_sync: Option<ShadowEnqueueStatus>,
) -> Response {
    let mut resp = Response::builder()
        .status(status)
        .body(Body::from(body))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());

    for (name, value) in &upstream_headers {
        if !is_hop_by_hop(name) {
            resp.headers_mut().append(name.clone(), value.clone());
        }
    }
    resp.headers_mut().insert(
        HEADER_ROUTE_PRIMARY,
        HeaderValue::from_static(primary_slot.as_str()),
    );
    if let Some(sync_status) = shadow_sync {
        resp.headers_mut().insert(
            HEADER_ROUTE_SHADOW_SYNC,
            HeaderValue::from_static(sync_status.as_str()),
        );
    }
    resp
}

fn proxy_error(status: StatusCode, code: &'static str, message: String) -> Response {
    (
        status,
        Json(ProxyErrorBody {
            error: code,
            message,
        }),
    )
        .into_response()
}
