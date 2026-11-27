use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response as AxumResponse};
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::storage::encode_sha256_hex;
use crate::transport::http::state::{
    authenticate_http, parse_ack_param, parse_role_epoch_header, HttpError, HttpState,
};
use crate::types::{MembusError, Permission};

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

#[derive(Deserialize)]
pub(super) struct BlobPutQuery {
    ack: Option<String>,
}

#[derive(Serialize)]
pub(super) struct BlobPutResp {
    sha256_hex: String,
}

pub(super) async fn handle_blob_put(
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

pub(super) async fn handle_blob_get(
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

pub(super) async fn handle_blob_head(
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
