use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response as AxumResponse};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::transport::http::state::{
    authenticate_http, parse_role_epoch_header, HttpError, HttpState,
};
use crate::types::{LeaseGrant, MembusError, Permission};

#[derive(Deserialize)]
pub(super) struct LeaseAcquireReq {
    namespace: String,
    resource: String,
    holder: String,
    ttl_ms: u64,
}

pub(super) async fn handle_lease_acquire(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<LeaseAcquireReq>,
) -> Result<Json<LeaseGrant>, HttpError> {
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
pub(super) struct LeaseRenewReq {
    namespace: String,
    resource: String,
    holder: String,
    generation: u64,
    token: String,
    ttl_ms: u64,
}

pub(super) async fn handle_lease_renew(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<LeaseRenewReq>,
) -> Result<Json<LeaseGrant>, HttpError> {
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
pub(super) struct LeaseTokenReq {
    namespace: String,
    resource: String,
    holder: String,
    generation: u64,
    token: String,
}

#[derive(Serialize)]
pub(super) struct BoolResp {
    ok: bool,
}

pub(super) async fn handle_lease_release(
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

pub(super) async fn handle_lease_validate(
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

pub(super) async fn handle_lease_inspect(
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
