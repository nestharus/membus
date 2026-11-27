use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::storage::compute_namespace_parity_digest_with_timeout;
use crate::transport::helpers::{resolve_checkpoint_path, run_blocking_query};
use crate::transport::http::state::{
    authenticate_http, decode_bytes_field, HttpError, HttpState,
};
use crate::types::{
    HealthStatus, MembusError, NamespaceParityDigest, ParityDigestMode, Permission, SlotRole,
    SlotRoleState,
};

pub(super) async fn handle_health(
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
pub(super) struct CheckpointReq {
    dest_dir: String,
}

#[derive(Serialize)]
pub(super) struct CheckpointResp {
    seq: u64,
}

pub(super) async fn handle_checkpoint(
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

#[derive(Default, Deserialize)]
struct HttpNamespaceDigestReq {
    #[serde(default)]
    mode: ParityDigestMode,
    prefix_utf8: Option<String>,
    prefix_b64: Option<String>,
}

pub(super) async fn handle_namespace_digest(
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
pub(super) struct AdminRoleTransitionReq {
    expected_role: SlotRole,
    expected_role_epoch: u64,
    new_role: SlotRole,
}

pub(super) async fn handle_admin_role_transition(
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
