use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response as AxumResponse};
use axum::Json;
use serde::Deserialize;

use crate::catalog::CollectionCatalog;
use crate::transport::http::state::{
    authenticate_http, parse_role_epoch_header, HttpError, HttpState,
};
use crate::types::{AuthMode, CollectionDescriptor, MembusError, Permission};

async fn run_catalog_admin_write<F>(
    state: &HttpState,
    headers: &HeaderMap,
    namespace: &str,
    op_name: &'static str,
    f: F,
) -> Result<Json<CollectionDescriptor>, HttpError>
where
    F: FnOnce(&CollectionCatalog) -> Result<CollectionDescriptor, MembusError> + Send + 'static,
{
    let principal = authenticate_http(state, headers)?;
    state
        .ctx
        .auth
        .authorize_admin(principal.as_ref(), Some(namespace))?;
    let role_epoch = parse_role_epoch_header(headers)?;
    state.ctx.check_catalog_write_allowed(role_epoch)?;
    let ctx = state.ctx.clone();
    let catalog = Arc::clone(&state.ctx.catalog);
    let desc = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_catalog_write_fence(role_epoch)?;
        f(catalog.as_ref())
    })
    .await
    .map_err(|e| MembusError::Storage(format!("{op_name} failed: {e}")))??;
    Ok(Json(desc))
}

#[derive(Deserialize)]
pub(super) struct RegisterCollectionReq {
    expected_version: Option<u32>,
    descriptor: CollectionDescriptor,
}

pub(super) async fn handle_catalog_register(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<RegisterCollectionReq>,
) -> Result<Json<CollectionDescriptor>, HttpError> {
    let ns = req.descriptor.namespace.clone();
    run_catalog_admin_write(&state, &headers, &ns, "register_collection", move |catalog| {
        catalog.register_collection(req.expected_version, req.descriptor)
    })
    .await
}

pub(super) async fn handle_catalog_list(
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

pub(super) async fn handle_catalog_get(
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

pub(super) async fn handle_catalog_get_version(
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
pub(super) struct BeginMigrationReq {
    expected_version: u32,
    target_version: u32,
    target_sha256: [u8; 32],
    target_schema_bytes: Vec<u8>,
}

pub(super) async fn handle_migration_begin(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Json(req): Json<BeginMigrationReq>,
) -> Result<Json<CollectionDescriptor>, HttpError> {
    let ns_owned = ns.clone();
    run_catalog_admin_write(&state, &headers, &ns, "begin_migration", move |catalog| {
        catalog.begin_migration(
            &ns_owned,
            req.expected_version,
            req.target_version,
            req.target_sha256,
            req.target_schema_bytes,
        )
    })
    .await
}

#[derive(Deserialize)]
pub(super) struct CommitMigrationReq {
    target_version: u32,
    verified_through: u64,
}

pub(super) async fn handle_migration_commit(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Json(req): Json<CommitMigrationReq>,
) -> Result<Json<CollectionDescriptor>, HttpError> {
    let ns_owned = ns.clone();
    run_catalog_admin_write(&state, &headers, &ns, "commit_migration", move |catalog| {
        catalog.commit_migration(&ns_owned, req.target_version, req.verified_through)
    })
    .await
}

#[derive(Deserialize)]
pub(super) struct AbortMigrationReq {
    target_version: u32,
}

pub(super) async fn handle_migration_abort(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Json(req): Json<AbortMigrationReq>,
) -> Result<Json<CollectionDescriptor>, HttpError> {
    let ns_owned = ns.clone();
    run_catalog_admin_write(&state, &headers, &ns, "abort_migration", move |catalog| {
        catalog.abort_migration(&ns_owned, req.target_version)
    })
    .await
}

#[derive(Deserialize)]
pub(super) struct AdvanceMigrationReq {
    target_version: u32,
    verified_through: u64,
}

pub(super) async fn handle_migration_advance(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Json(req): Json<AdvanceMigrationReq>,
) -> Result<Json<CollectionDescriptor>, HttpError> {
    let ns_owned = ns.clone();
    run_catalog_admin_write(
        &state,
        &headers,
        &ns,
        "advance_migration_verification",
        move |catalog| {
            catalog.advance_migration_verification(
                &ns_owned,
                req.target_version,
                req.verified_through,
            )
        },
    )
    .await
}

#[derive(Deserialize)]
pub(super) struct ReverseShadowReq {
    target_version: u32,
    verified_through: u64,
}

pub(super) async fn handle_migration_reverse_shadow(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Json(req): Json<ReverseShadowReq>,
) -> Result<Json<CollectionDescriptor>, HttpError> {
    let ns_owned = ns.clone();
    run_catalog_admin_write(
        &state,
        &headers,
        &ns,
        "enter_reverse_shadow",
        move |catalog| {
            catalog.enter_reverse_shadow(&ns_owned, req.target_version, req.verified_through)
        },
    )
    .await
}

#[derive(Deserialize)]
pub(super) struct RollbackMigrationReq {
    rollback_version: u32,
}

pub(super) async fn handle_migration_rollback(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Json(req): Json<RollbackMigrationReq>,
) -> Result<Json<CollectionDescriptor>, HttpError> {
    let ns_owned = ns.clone();
    run_catalog_admin_write(&state, &headers, &ns, "rollback_migration", move |catalog| {
        catalog.rollback_migration(&ns_owned, req.rollback_version)
    })
    .await
}

#[derive(Deserialize)]
pub(super) struct ContractMigrationReq {
    expected_version: u32,
}

pub(super) async fn handle_migration_contract(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Json(req): Json<ContractMigrationReq>,
) -> Result<Json<CollectionDescriptor>, HttpError> {
    let ns_owned = ns.clone();
    run_catalog_admin_write(&state, &headers, &ns, "contract_migration", move |catalog| {
        catalog.contract_migration(&ns_owned, req.expected_version)
    })
    .await
}
