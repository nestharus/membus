use std::sync::Arc;

use axum::extract::{FromRequestParts, Request as AxumRequest, State};
use axum::http::request::Parts;
use axum::http::{header, HeaderMap, HeaderName, HeaderValue, StatusCode};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response as AxumResponse};
use axum::Json;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use serde::Serialize;
use tokio::sync::watch;

use crate::signals::{EachChangeSubscription, LiveCoalescedSubscription};
use crate::transport::ServerContext;
use crate::types::{
    Ack, CommitResult, CoordinatedCommit, ExpectedVersion, MembusError, Principal,
    SearchSyncStatus, SignalEvent, SlotRole, MAX_IDEMPOTENCY_KEY_BYTES,
};

pub(super) const HEADER_SEARCH_SYNC: HeaderName = HeaderName::from_static("x-membus-search-sync");
pub(super) const HEADER_ROLE_EPOCH: HeaderName = HeaderName::from_static("x-membus-role-epoch");
pub(super) const HEADER_IDEMPOTENCY_KEY: HeaderName =
    HeaderName::from_static("x-membus-idempotency-key");
pub(super) const HEADER_IDEMPOTENT_REPLAY: HeaderName =
    HeaderName::from_static("x-membus-idempotent-replay");
pub(super) const HEADER_COMMIT_TS_MS: HeaderName = HeaderName::from_static("x-membus-commit-ts-ms");
pub(super) const HEADER_ROUTE_ORIGIN: HeaderName = HeaderName::from_static("x-membus-route-origin");
pub(super) const HEADER_ECHO_LOOP: HeaderName = HeaderName::from_static("x-membus-echo-loop");

#[derive(Clone)]
pub(crate) struct HttpState {
    pub(crate) ctx: ServerContext,
    pub(crate) shutdown_rx: watch::Receiver<bool>,
}

pub(super) async fn enforce_http_concurrency_limit(
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

#[derive(Serialize)]
pub(super) struct HttpCommitResp {
    #[serde(flatten)]
    pub(super) commit: CommitResult,
    pub(super) search_sync: SearchSyncStatus,
}

pub(super) fn coordinated_commit_response(res: CoordinatedCommit) -> AxumResponse {
    coordinated_commit_response_with_ts(res, None)
}

pub(super) fn coordinated_commit_response_with_ts(
    res: CoordinatedCommit,
    commit_ts_ms: Option<u64>,
) -> AxumResponse {
    let header_val = HeaderValue::from_static(res.search_sync.as_str());
    let replayed = res.idempotent_replay;
    let body = HttpCommitResp {
        commit: res.commit,
        search_sync: res.search_sync,
    };
    let mut resp = (StatusCode::OK, Json(body)).into_response();
    resp.headers_mut().insert(HEADER_SEARCH_SYNC, header_val);
    if let Some(ts_ms) = commit_ts_ms {
        if let Ok(val) = HeaderValue::from_str(&ts_ms.to_string()) {
            resp.headers_mut().insert(HEADER_COMMIT_TS_MS, val);
        }
    }
    if replayed {
        resp.headers_mut()
            .insert(HEADER_IDEMPOTENT_REPLAY, HeaderValue::from_static("true"));
    }
    resp
}

#[derive(Serialize)]
struct ErrorBody {
    error: &'static str,
    message: String,
}

pub(super) struct HttpError(pub(super) MembusError);

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

pub(super) fn extract_single_header<'a>(
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

pub(super) fn parse_role_epoch_header(headers: &HeaderMap) -> Result<Option<u64>, MembusError> {
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

pub(super) fn parse_idempotency_key_header(
    headers: &HeaderMap,
) -> Result<Option<String>, MembusError> {
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

fn extract_bearer_token(headers: &HeaderMap) -> Option<&str> {
    let raw = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = raw.split_once(' ')?;
    if scheme.eq_ignore_ascii_case("bearer") {
        Some(token.trim())
    } else {
        None
    }
}

pub(super) fn authenticate_http(
    state: &HttpState,
    headers: &HeaderMap,
) -> Result<Option<Principal>, HttpError> {
    let token = extract_bearer_token(headers);
    Ok(state.ctx.auth.authenticate_token(token)?)
}

pub(super) struct AuthPrincipal(pub(super) Option<Principal>);

impl FromRequestParts<HttpState> for AuthPrincipal {
    type Rejection = HttpError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &HttpState,
    ) -> Result<Self, Self::Rejection> {
        let principal = authenticate_http(state, &parts.headers)?;
        Ok(Self(principal))
    }
}

pub(super) fn decode_bytes_field(
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

pub(super) fn decode_opt_bytes_field(
    utf8: Option<String>,
    b64: Option<String>,
    field: &str,
) -> Result<Option<Vec<u8>>, MembusError> {
    match (utf8, b64) {
        (None, None) => Ok(None),
        (u, b) => Ok(Some(decode_bytes_field(u, b, field)?)),
    }
}

pub(super) fn default_ack() -> Ack {
    Ack::Fast
}

pub(super) fn default_expected() -> ExpectedVersion {
    ExpectedVersion::Any
}

pub(super) fn parse_ack_param(raw: Option<&str>) -> Result<Ack, MembusError> {
    match raw {
        None => Ok(Ack::Fast),
        Some(s) if s.eq_ignore_ascii_case("fast") => Ok(Ack::Fast),
        Some(s) if s.eq_ignore_ascii_case("flush") => Ok(Ack::Flush),
        Some(other) => Err(MembusError::InvalidArgument(format!(
            "invalid ack mode '{other}' (expected 'fast' or 'flush')"
        ))),
    }
}

pub(super) enum StreamSub {
    EachChange(EachChangeSubscription),
    Live(LiveCoalescedSubscription),
}

impl StreamSub {
    pub(super) async fn next_event(&mut self) -> Result<Option<SignalEvent>, MembusError> {
        match self {
            Self::EachChange(s) => s.next_event().await,
            Self::Live(s) => s.next_event().await,
        }
    }
}

pub(super) async fn wait_shutdown_ref(rx: &mut watch::Receiver<bool>) {
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}

pub(super) async fn wait_shutdown(mut rx: watch::Receiver<bool>) {
    wait_shutdown_ref(&mut rx).await;
}
