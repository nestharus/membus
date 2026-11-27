use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::response::{IntoResponse, Response as AxumResponse};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::transport::helpers::{
    authorize_index_mutations, authorize_search_op, run_blocking_query,
};
use crate::transport::http::state::{
    authenticate_http, default_ack, extract_single_header, parse_role_epoch_header, HttpError,
    HttpState, HEADER_ECHO_LOOP, HEADER_ROUTE_ORIGIN, HEADER_SEARCH_SYNC,
};
use crate::transport::write_coord::{
    coordinated_apply_replicated_delta, CoordinatedReplicatedApply,
};
use crate::types::{
    Ack, DeltaLogPage, MembusError, Permission, ReplicatedApplyResult, ReplicatedDelta,
    SearchSyncStatus,
};

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

#[derive(Deserialize)]
pub(super) struct HttpReplicationApplyReq {
    delta: ReplicatedDelta,
    #[serde(default = "default_ack")]
    ack: Ack,
    #[serde(default)]
    route_origin: Option<String>,
}

pub(super) async fn handle_replication_apply(
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
pub(super) struct UpdatesQuery {
    since_seq: Option<u64>,
    limit: Option<usize>,
}

pub(super) async fn handle_updates_scan(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Query(q): Query<UpdatesQuery>,
) -> Result<Json<DeltaLogPage>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    state.ctx.auth.authorize_admin(principal.as_ref(), None)?;
    if q.limit == Some(0) {
        return Err(HttpError(MembusError::InvalidArgument(
            "updates query limit must be greater than 0".to_owned(),
        )));
    }
    let since_seq = q.since_seq.unwrap_or(0);
    let limit = q.limit.unwrap_or(256).min(1024);
    let byte_budget = (state.ctx.config.max_frame_body / 4).max(4096);
    let storage = Arc::clone(&state.ctx.storage);
    let page = run_blocking_query(state.ctx.config.query_timeout, "updates_scan", move || {
        let raw = storage.scan_deltas_after(since_seq, limit)?;
        if raw.entries.is_empty() {
            return Ok(raw);
        }
        let mut kept = Vec::with_capacity(raw.entries.len());
        let mut used_bytes = 0usize;
        let mut truncated = false;
        for entry in raw.entries {
            let entry_bytes = serde_json::to_vec(&entry).map(|v| v.len()).unwrap_or(0);
            if !kept.is_empty() && used_bytes.saturating_add(entry_bytes) > byte_budget {
                truncated = true;
                break;
            }
            used_bytes = used_bytes.saturating_add(entry_bytes);
            kept.push(entry);
        }
        let scanned_through_seq = if truncated {
            kept.last().map(|e| e.seq).unwrap_or(since_seq)
        } else {
            raw.scanned_through_seq
        };
        Ok(DeltaLogPage {
            entries: kept,
            scanned_through_seq,
        })
    })
    .await?;
    Ok(Json(page))
}
