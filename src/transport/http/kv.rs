use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::response::{IntoResponse, Response as AxumResponse};
use axum::Json;
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::storage::contract::StorageEngine;
use crate::types::encode_sha256_hex;
use crate::transport::helpers::{
    authorize_index_mutations, authorize_search_op, run_blocking_query,
};
use crate::transport::http::state::{
    authenticate_http, coordinated_commit_response_with_ts, decode_bytes_field,
    decode_opt_bytes_field, default_ack, default_expected, parse_idempotency_key_header,
    parse_role_epoch_header, HttpError, HttpState,
};
use crate::transport::write_coord::{
    coordinated_commit_indexed_tx_idempotent, coordinated_put_indexed, coordinated_remove_indexed,
    CoordinatedPutArgs, CoordinatedRemoveArgs,
};
use crate::types::{
    Ack, AuthMode, ExpectedVersion, IdempotencyContext, IndexMutations, IndexedMutation,
    MembusError, Permission, ScanOptions, ScanPage, SearchLogOp, VersionGuard,
};

fn lookup_commit_ts_ms(storage: &dyn StorageEngine, seq: u64) -> Option<u64> {
    if seq == 0 {
        return None;
    }
    let page = storage.scan_deltas_after(seq.saturating_sub(1), 1).ok()?;
    page.entries
        .first()
        .filter(|e| e.seq == seq)
        .map(|e| e.ts_ms)
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

pub(super) async fn handle_kv_get(
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
pub(super) struct HttpKvPutReq {
    value_utf8: Option<String>,
    value_b64: Option<String>,
    #[serde(default = "default_ack")]
    ack: Ack,
    #[serde(default = "default_expected")]
    expected: ExpectedVersion,
    indexes: Option<IndexMutations>,
    search_op: Option<SearchLogOp>,
}

pub(super) async fn handle_kv_put(
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
    let (coord, commit_ts_ms) = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_client_write_fence(role_epoch)?;
        let coord = coordinated_put_indexed(
            &storage,
            search.as_ref(),
            CoordinatedPutArgs {
                namespace: &ns,
                key: key.as_bytes(),
                value: &value,
                ack: req.ack,
                expected: req.expected,
                indexes: req.indexes.as_ref(),
                search_op: req.search_op,
                idempotency: idempotency.as_ref(),
            },
        )?;
        let ts_ms = lookup_commit_ts_ms(storage.as_ref(), coord.commit.seq);
        Ok::<_, MembusError>((coord, ts_ms))
    })
    .await
    .map_err(|e| MembusError::Storage(format!("kv put failed: {e}")))??;
    Ok(coordinated_commit_response_with_ts(coord, commit_ts_ms))
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

pub(super) async fn handle_kv_delete(
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
    let (coord, commit_ts_ms) = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_client_write_fence(role_epoch)?;
        let coord = coordinated_remove_indexed(
            &storage,
            search.as_ref(),
            CoordinatedRemoveArgs {
                namespace: &ns,
                key: key.as_bytes(),
                ack: req.ack,
                expected: req.expected,
                search_op: req.search_op,
                idempotency: idempotency.as_ref(),
            },
        )?;
        let ts_ms = lookup_commit_ts_ms(storage.as_ref(), coord.commit.seq);
        Ok::<_, MembusError>((coord, ts_ms))
    })
    .await
    .map_err(|e| MembusError::Storage(format!("kv delete failed: {e}")))??;
    Ok(coordinated_commit_response_with_ts(coord, commit_ts_ms))
}

#[derive(Deserialize)]
pub(super) struct HttpTxReq {
    #[serde(default = "default_ack")]
    ack: Ack,
    #[serde(default)]
    guards: Vec<VersionGuard>,
    mutations: Vec<IndexedMutation>,
}

pub(super) async fn handle_tx(
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
    let (coord, commit_ts_ms) = tokio::task::spawn_blocking(move || {
        let _fence = ctx.acquire_client_write_fence(role_epoch)?;
        let coord = coordinated_commit_indexed_tx_idempotent(
            &storage,
            search.as_ref(),
            &req.guards,
            req.mutations,
            req.ack,
            idempotency.as_ref(),
        )?;
        let ts_ms = lookup_commit_ts_ms(storage.as_ref(), coord.commit.seq);
        Ok::<_, MembusError>((coord, ts_ms))
    })
    .await
    .map_err(|e| MembusError::Storage(format!("tx task failed: {e}")))??;
    Ok(coordinated_commit_response_with_ts(coord, commit_ts_ms))
}

#[derive(Deserialize)]
pub(super) struct HttpScanPrefixReq {
    namespace: String,
    prefix_utf8: Option<String>,
    prefix_b64: Option<String>,
    after_key_b64: Option<String>,
    limit: Option<usize>,
    max_bytes: Option<usize>,
}

pub(super) async fn handle_scan_prefix(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<HttpScanPrefixReq>,
) -> Result<Json<ScanPage>, HttpError> {
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
pub(super) struct HttpScanRangeReq {
    namespace: String,
    start_key_utf8: Option<String>,
    start_key_b64: Option<String>,
    end_key_utf8: Option<String>,
    end_key_b64: Option<String>,
    after_key_b64: Option<String>,
    limit: Option<usize>,
    max_bytes: Option<usize>,
}

pub(super) async fn handle_scan_range(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Json(req): Json<HttpScanRangeReq>,
) -> Result<Json<ScanPage>, HttpError> {
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
