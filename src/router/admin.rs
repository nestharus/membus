//! Separate authenticated admin control-plane Axum router (`:8081`) implementing live status,
//! dual-slot parity verification, `CF_LOG` catch-up, fail-closed 5-stage cutover, and rollback.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::router::config::SlotId;
use crate::router::shadow_queue::RouterRuntime;
use crate::router::upstream::UpstreamClient;
use crate::types::{MembusError, ParityDigestMode, SlotRole};

const MAX_ADMIN_BODY_BYTES: usize = 64 * 1024;

#[derive(Clone)]
struct AdminState {
    runtime: Arc<RouterRuntime>,
}

#[derive(Serialize)]
struct AdminErrorBody {
    error: &'static str,
    message: String,
}

struct AdminError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl IntoResponse for AdminError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(AdminErrorBody {
                error: self.code,
                message: self.message,
            }),
        )
            .into_response()
    }
}

#[derive(Serialize)]
struct AdminHealthResp {
    ok: bool,
    primary_slot: SlotId,
    shadow_slot: SlotId,
    parity_dirty: bool,
}

#[derive(Default, Deserialize)]
struct ParityReq {
    #[serde(default)]
    namespaces: Vec<String>,
    namespace: Option<String>,
    #[serde(default)]
    mode: ParityDigestMode,
    prefix: Option<String>,
}

#[derive(Default, Deserialize)]
struct CatchupReq {
    #[serde(default)]
    namespaces: Vec<String>,
    namespace: Option<String>,
    #[serde(default)]
    mode: ParityDigestMode,
    from_seq: Option<u64>,
}

#[derive(Default, Deserialize)]
struct CutoverReq {
    #[serde(default)]
    namespaces: Vec<String>,
    namespace: Option<String>,
    #[serde(default)]
    mode: ParityDigestMode,
}

#[derive(Serialize)]
struct CutoverResp {
    cutover: bool,
    previous_primary: SlotId,
    new_primary: SlotId,
    new_shadow: SlotId,
    reverse_shadow_enabled: bool,
    primary_role_epoch: u64,
    shadow_role_epoch: u64,
    parity_verified: bool,
}

#[derive(Default, Deserialize)]
struct RollbackReq {
    #[serde(default)]
    namespaces: Vec<String>,
    namespace: Option<String>,
    #[serde(default)]
    mode: ParityDigestMode,
}

#[derive(Serialize)]
struct RollbackResp {
    rolled_back: bool,
    previous_primary: SlotId,
    new_primary: SlotId,
    new_shadow: SlotId,
    reverse_shadow_enabled: bool,
    primary_role_epoch: u64,
    shadow_role_epoch: u64,
}

/// Build the admin control-plane Axum router bound to `--admin-listen`.
pub fn build_admin_router(runtime: Arc<RouterRuntime>) -> Router {
    let state = AdminState { runtime };
    Router::new()
        .route("/admin/health", get(handle_admin_health))
        .route("/admin/status", get(handle_admin_status))
        .route("/admin/parity", post(handle_admin_parity))
        .route("/admin/catchup", post(handle_admin_catchup))
        .route("/admin/cutover", post(handle_admin_cutover))
        .route("/admin/rollback", post(handle_admin_rollback))
        .layer(DefaultBodyLimit::max(MAX_ADMIN_BODY_BYTES))
        .with_state(state)
}

async fn handle_admin_health(State(state): State<AdminState>) -> Json<AdminHealthResp> {
    let snap = state.runtime.metrics_snapshot();
    Json(AdminHealthResp {
        ok: true,
        primary_slot: snap.primary_slot,
        shadow_slot: snap.shadow_slot,
        parity_dirty: snap.parity_dirty,
    })
}

async fn handle_admin_status(
    State(state): State<AdminState>,
    headers: HeaderMap,
) -> Result<Response, AdminError> {
    authorize_admin(&state.runtime, &headers)?;
    Ok((StatusCode::OK, Json(state.runtime.metrics_snapshot())).into_response())
}

async fn handle_admin_parity(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AdminError> {
    authorize_admin(&state.runtime, &headers)?;
    let req: ParityReq = parse_optional_json(&body)?;
    let namespaces = collect_namespaces(req.namespaces, req.namespace);
    if namespaces.is_empty() {
        return Err(admin_error(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "provide 'namespaces' (or 'namespace') to verify parity".to_owned(),
        ));
    }
    let report = state
        .runtime
        .verify_parity(&namespaces, req.mode, req.prefix.as_deref())
        .await
        .map_err(map_membus_error)?;
    Ok((StatusCode::OK, Json(report)).into_response())
}

async fn handle_admin_catchup(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AdminError> {
    authorize_admin(&state.runtime, &headers)?;
    let req: CatchupReq = parse_optional_json(&body)?;
    let namespaces = collect_namespaces(req.namespaces, req.namespace);
    let report = state
        .runtime
        .run_catchup(&namespaces, req.mode, req.from_seq)
        .await
        .map_err(map_membus_error)?;
    Ok((StatusCode::OK, Json(report)).into_response())
}

async fn handle_admin_cutover(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AdminError> {
    authorize_admin(&state.runtime, &headers)?;
    let req: CutoverReq = parse_optional_json(&body)?;
    let _guard = state.runtime.cutover_lock().lock().await;

    if state.runtime.is_parity_dirty() {
        return Err(admin_error(
            StatusCode::CONFLICT,
            "parity_dirty",
            "cutover blocked: parity_dirty is true; run POST /admin/catchup and verify parity first"
                .to_owned(),
        ));
    }

    let depth = state.runtime.shadow_queue_depth();
    if depth > 0 {
        return Err(admin_error(
            StatusCode::CONFLICT,
            "shadow_queue_not_empty",
            format!("cutover blocked: shadow_queue_depth is {depth} (must be 0)"),
        ));
    }

    let explicit = collect_namespaces(req.namespaces, req.namespace);
    let namespaces = state
        .runtime
        .discover_namespaces(&explicit, &BTreeSet::new())
        .await;
    if namespaces.is_empty() {
        return Err(admin_error(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "cutover requires at least one declared or discovered namespace for parity verification"
                .to_owned(),
        ));
    }

    let pre_parity = state
        .runtime
        .verify_parity(&namespaces, req.mode, None)
        .await
        .map_err(map_membus_error)?;
    if !pre_parity.matched {
        state.runtime.mark_parity_dirty();
        return Err(admin_error(
            StatusCode::CONFLICT,
            "parity_mismatch",
            "cutover blocked: Slot A and Slot B parity digests do not match".to_owned(),
        ));
    }

    let swap = execute_fenced_slot_swap(&state.runtime, &namespaces, req.mode).await?;

    Ok((
        StatusCode::OK,
        Json(CutoverResp {
            cutover: true,
            previous_primary: swap.previous_primary,
            new_primary: swap.new_primary,
            new_shadow: swap.previous_primary,
            reverse_shadow_enabled: true,
            primary_role_epoch: swap.new_primary_epoch,
            shadow_role_epoch: swap.new_shadow_epoch,
            parity_verified: swap.parity_verified,
        }),
    )
        .into_response())
}

async fn handle_admin_rollback(
    State(state): State<AdminState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Response, AdminError> {
    authorize_admin(&state.runtime, &headers)?;
    let req: RollbackReq = parse_optional_json(&body)?;
    let _guard = state.runtime.cutover_lock().lock().await;

    if state.runtime.is_parity_dirty() {
        return Err(admin_error(
            StatusCode::CONFLICT,
            "parity_dirty",
            "rollback blocked: reverse-shadow parity_dirty is true; run POST /admin/catchup first"
                .to_owned(),
        ));
    }

    let drain_budget = Duration::from_millis(state.runtime.shadow_timeout().as_millis() as u64);
    if !state.runtime.wait_shadow_queue_drain(drain_budget).await {
        return Err(admin_error(
            StatusCode::CONFLICT,
            "shadow_queue_not_empty",
            "rollback blocked: reverse-shadow queue did not drain within timeout".to_owned(),
        ));
    }

    let explicit = collect_namespaces(req.namespaces, req.namespace);
    let namespaces = state
        .runtime
        .discover_namespaces(&explicit, &BTreeSet::new())
        .await;
    if namespaces.is_empty() {
        return Err(admin_error(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "rollback requires at least one declared or discovered namespace for parity verification"
                .to_owned(),
        ));
    }

    let swap = execute_fenced_slot_swap(&state.runtime, &namespaces, req.mode).await?;

    Ok((
        StatusCode::OK,
        Json(RollbackResp {
            rolled_back: true,
            previous_primary: swap.previous_primary,
            new_primary: swap.new_primary,
            new_shadow: swap.previous_primary,
            reverse_shadow_enabled: true,
            primary_role_epoch: swap.new_primary_epoch,
            shadow_role_epoch: swap.new_shadow_epoch,
        }),
    )
        .into_response())
}

struct FencedSwapOutcome {
    previous_primary: SlotId,
    new_primary: SlotId,
    new_primary_epoch: u64,
    new_shadow_epoch: u64,
    parity_verified: bool,
}

/// Shared fail-closed fenced role swap used by both cutover and rollback:
/// 1. Verify outgoing Primary is `Active` and Candidate is `Shadow`.
/// 2. CAS-fence outgoing Primary FIRST (`Active -> Shadow`).
/// 3. Drain tail deltas via `CF_LOG` catch-up, assert `cursor >= fenced_primary.latest_seq`,
///    and verify post-fence parity digests across all discovered/declared namespaces.
/// 4. CAS-promote Candidate (`Shadow -> Active`). On uncertain failure, probe Candidate
///    health FIRST to avoid dual-Active split-brain; compensate only if confirmed `Shadow`
///    and immediately update cached `role_epoch` so post-compensation writes never brick!
async fn execute_fenced_slot_swap(
    runtime: &RouterRuntime,
    namespaces: &[String],
    mode: ParityDigestMode,
) -> Result<FencedSwapOutcome, AdminError> {
    let timeout = runtime.primary_timeout();
    let old_primary = runtime.primary_slot();
    let candidate = old_primary.other();
    let primary_client = runtime.client_for(old_primary);
    let candidate_client = runtime.client_for(candidate);

    let (primary_health, candidate_health) =
        verify_swap_preconditions(&primary_client, &candidate_client, old_primary, candidate, timeout)
            .await?;

    let fenced_old = fence_outgoing_primary(
        runtime,
        &primary_client,
        old_primary,
        primary_health.role_epoch,
        candidate_health.role_epoch,
        timeout,
    )
    .await?;

    let post_fence_ctx = PostFenceContext {
        runtime,
        primary_client: &primary_client,
        candidate_client: &candidate_client,
        old_primary,
        fenced_old_epoch: fenced_old.role_epoch,
        candidate_epoch: candidate_health.role_epoch,
        timeout,
    };

    drain_and_verify_post_fence_parity(&post_fence_ctx, namespaces, mode).await?;

    let promoted = promote_candidate_slot(&post_fence_ctx, candidate).await?;

    let new_primary_seq = candidate_client
        .health(timeout)
        .await
        .map(|h| h.latest_seq)
        .unwrap_or(candidate_health.latest_seq);

    runtime.commit_topology_swap(
        candidate,
        promoted.role_epoch,
        fenced_old.role_epoch,
        new_primary_seq,
        true,
    );

    Ok(FencedSwapOutcome {
        previous_primary: old_primary,
        new_primary: candidate,
        new_primary_epoch: promoted.role_epoch,
        new_shadow_epoch: fenced_old.role_epoch,
        parity_verified: true,
    })
}

async fn verify_swap_preconditions(
    primary_client: &UpstreamClient,
    candidate_client: &UpstreamClient,
    old_primary: SlotId,
    candidate: SlotId,
    timeout: Duration,
) -> Result<(crate::types::HealthStatus, crate::types::HealthStatus), AdminError> {
    let primary_health = primary_client
        .health(timeout)
        .await
        .map_err(map_membus_error)?;
    let candidate_health = candidate_client
        .health(timeout)
        .await
        .map_err(map_membus_error)?;
    if primary_health.slot_role != SlotRole::Active {
        return Err(admin_error(
            StatusCode::CONFLICT,
            "unexpected_slot_role",
            format!(
                "outgoing primary {old_primary} is in role '{}' (expected 'active')",
                primary_health.slot_role.as_str()
            ),
        ));
    }
    if candidate_health.slot_role != SlotRole::Shadow {
        return Err(admin_error(
            StatusCode::CONFLICT,
            "unexpected_slot_role",
            format!(
                "candidate {candidate} is in role '{}' (expected 'shadow')",
                candidate_health.slot_role.as_str()
            ),
        ));
    }
    Ok((primary_health, candidate_health))
}

async fn fence_outgoing_primary(
    runtime: &RouterRuntime,
    primary_client: &UpstreamClient,
    old_primary: SlotId,
    primary_epoch: u64,
    candidate_epoch: u64,
    timeout: Duration,
) -> Result<crate::types::SlotRoleState, AdminError> {
    match primary_client
        .transition_role(SlotRole::Active, primary_epoch, SlotRole::Shadow, timeout)
        .await
    {
        Ok(st) => Ok(st),
        Err(fence_err) => match primary_client.health(timeout).await {
            Ok(h) if h.slot_role == SlotRole::Shadow => {
                runtime.update_role_epochs(h.role_epoch, candidate_epoch);
                Ok(crate::types::SlotRoleState {
                    role: SlotRole::Shadow,
                    role_epoch: h.role_epoch,
                })
            }
            Ok(h) if h.slot_role == SlotRole::Active => {
                runtime.update_role_epochs(h.role_epoch, candidate_epoch);
                Err(map_membus_error(fence_err))
            }
            Ok(other) => {
                runtime.mark_parity_dirty();
                runtime.update_role_epochs(other.role_epoch, candidate_epoch);
                Err(admin_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "fence_uncertain_degraded",
                    format!(
                        "outgoing primary {old_primary} fence failed ({fence_err}) and reported unexpected role '{}'",
                        other.slot_role.as_str()
                    ),
                ))
            }
            Err(probe_err) => {
                runtime.mark_parity_dirty();
                Err(admin_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "fence_uncertain_degraded",
                    format!(
                        "outgoing primary {old_primary} fence failed ({fence_err}) and health probe failed ({probe_err})"
                    ),
                ))
            }
        },
    }
}

struct PostFenceContext<'a> {
    runtime: &'a RouterRuntime,
    primary_client: &'a UpstreamClient,
    candidate_client: &'a UpstreamClient,
    old_primary: SlotId,
    fenced_old_epoch: u64,
    candidate_epoch: u64,
    timeout: Duration,
}

async fn drain_and_verify_post_fence_parity(
    ctx: &PostFenceContext<'_>,
    namespaces: &[String],
    mode: ParityDigestMode,
) -> Result<(), AdminError> {
    let start_seq = ctx.runtime.topology_snapshot().catchup_start_seq();
    let (_, _, cursor, touched_tail, saw_standalone_search) = match ctx
        .runtime
        .replay_updates_until_caught_up(
            ctx.primary_client,
            ctx.candidate_client,
            ctx.old_primary,
            ctx.candidate_epoch,
            start_seq,
            ctx.timeout,
        )
        .await
    {
        Ok(res) => res,
        Err(err) => {
            ctx.runtime.mark_parity_dirty();
            return Err(compensate_restore_primary(ctx, map_membus_error(err)).await);
        }
    };

    if saw_standalone_search {
        ctx.runtime.mark_parity_dirty();
        let err = admin_error(
            StatusCode::CONFLICT,
            "unreplicated_search_ops",
            "swap aborted after fencing: standalone search_ops require manual sync".to_owned(),
        );
        return Err(compensate_restore_primary(ctx, err).await);
    }

    let fenced_health = match ctx.primary_client.health(ctx.timeout).await {
        Ok(h) => h,
        Err(err) => {
            ctx.runtime.mark_parity_dirty();
            return Err(compensate_restore_primary(ctx, map_membus_error(err)).await);
        }
    };
    if cursor < fenced_health.latest_seq {
        ctx.runtime.mark_parity_dirty();
        let err = admin_error(
            StatusCode::CONFLICT,
            "catchup_incomplete",
            format!(
                "swap aborted after fencing: tail catchup reached seq {cursor} < fenced primary latest_seq {}",
                fenced_health.latest_seq
            ),
        );
        return Err(compensate_restore_primary(ctx, err).await);
    }

    let all_namespaces = ctx.runtime.discover_namespaces(namespaces, &touched_tail).await;
    match ctx.runtime.verify_parity(&all_namespaces, mode, None).await {
        Ok(report) if report.matched => Ok(()),
        Ok(_) => {
            ctx.runtime.mark_parity_dirty();
            let err = admin_error(
                StatusCode::CONFLICT,
                "parity_mismatch",
                "swap aborted after fencing: post-fence parity digest mismatch; restored outgoing primary"
                    .to_owned(),
            );
            Err(compensate_restore_primary(ctx, err).await)
        }
        Err(err) => {
            ctx.runtime.mark_parity_dirty();
            Err(compensate_restore_primary(ctx, map_membus_error(err)).await)
        }
    }
}

async fn promote_candidate_slot(
    ctx: &PostFenceContext<'_>,
    candidate: SlotId,
) -> Result<crate::types::SlotRoleState, AdminError> {
    match ctx
        .candidate_client
        .transition_role(SlotRole::Shadow, ctx.candidate_epoch, SlotRole::Active, ctx.timeout)
        .await
    {
        Ok(st) => Ok(st),
        Err(promo_err) => match ctx.candidate_client.health(ctx.timeout).await {
            Ok(h) if h.slot_role == SlotRole::Active => Ok(crate::types::SlotRoleState {
                role: SlotRole::Active,
                role_epoch: h.role_epoch,
            }),
            Ok(h) if h.slot_role == SlotRole::Shadow => {
                let comp_ctx = PostFenceContext {
                    runtime: ctx.runtime,
                    primary_client: ctx.primary_client,
                    candidate_client: ctx.candidate_client,
                    old_primary: ctx.old_primary,
                    fenced_old_epoch: ctx.fenced_old_epoch,
                    candidate_epoch: h.role_epoch,
                    timeout: ctx.timeout,
                };
                Err(compensate_restore_primary(&comp_ctx, map_membus_error(promo_err)).await)
            }
            Ok(other) => {
                ctx.runtime.mark_parity_dirty();
                ctx.runtime
                    .update_role_epochs(ctx.fenced_old_epoch, other.role_epoch);
                Err(admin_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "ambiguous_promotion_degraded",
                    format!(
                        "candidate {candidate} promotion failed ({promo_err}) and reported unexpected role '{}'; refusing to re-promote {} to prevent split-brain",
                        other.slot_role.as_str(),
                        ctx.old_primary
                    ),
                ))
            }
            Err(probe_err) => {
                ctx.runtime.mark_parity_dirty();
                ctx.runtime
                    .update_role_epochs(ctx.fenced_old_epoch, ctx.candidate_epoch);
                Err(admin_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "ambiguous_promotion_degraded",
                    format!(
                        "candidate {candidate} promotion failed ({promo_err}) and health probe failed ({probe_err}); refusing to re-promote {} to prevent dual-Active split-brain",
                        ctx.old_primary
                    ),
                ))
            }
        },
    }
}

/// Compensate by restoring the fenced Primary (`Shadow -> Active`) and immediately updating
/// cached role epochs so subsequent client writes succeed without stale `X-Membus-Role-Epoch`.
/// If compensation itself fails, returns `503 double_fenced_degraded` explicitly.
async fn compensate_restore_primary(
    ctx: &PostFenceContext<'_>,
    original_err: AdminError,
) -> AdminError {
    match ctx
        .primary_client
        .transition_role(SlotRole::Shadow, ctx.fenced_old_epoch, SlotRole::Active, ctx.timeout)
        .await
    {
        Ok(restored) => {
            ctx.runtime
                .update_role_epochs(restored.role_epoch, ctx.candidate_epoch);
            original_err
        }
        Err(comp_err) => match ctx.primary_client.health(ctx.timeout).await {
            Ok(h) if h.slot_role == SlotRole::Active => {
                ctx.runtime
                    .update_role_epochs(h.role_epoch, ctx.candidate_epoch);
                original_err
            }
            Ok(h) => {
                ctx.runtime.mark_parity_dirty();
                ctx.runtime
                    .update_role_epochs(h.role_epoch, ctx.candidate_epoch);
                admin_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "double_fenced_degraded",
                    format!(
                        "swap failed ({}) AND primary restoration failed ({comp_err}): primary is '{}', candidate is 'shadow'; manual operator recovery required",
                        original_err.message,
                        h.slot_role.as_str()
                    ),
                )
            }
            Err(probe_err) => {
                ctx.runtime.mark_parity_dirty();
                ctx.runtime
                    .update_role_epochs(ctx.fenced_old_epoch, ctx.candidate_epoch);
                admin_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "double_fenced_degraded",
                    format!(
                        "swap failed ({}) AND primary restoration failed ({comp_err}; health probe: {probe_err}): BOTH slots may be fenced in Shadow; manual operator recovery required",
                        original_err.message
                    ),
                )
            }
        },
    }
}

fn authorize_admin(runtime: &RouterRuntime, headers: &HeaderMap) -> Result<(), AdminError> {
    let Some(expected) = runtime.admin_token() else {
        return Ok(());
    };
    let supplied = extract_bearer_token(headers).ok_or_else(|| {
        admin_error(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "missing admin bearer token".to_owned(),
        )
    })?;
    let supplied_hash: [u8; 32] = Sha256::digest(supplied.as_bytes()).into();
    let expected_hash: [u8; 32] = Sha256::digest(expected.as_bytes()).into();
    if bool::from(supplied_hash.ct_eq(&expected_hash)) {
        Ok(())
    } else {
        Err(admin_error(
            StatusCode::UNAUTHORIZED,
            "unauthorized",
            "invalid admin bearer token".to_owned(),
        ))
    }
}

fn extract_bearer_token(headers: &HeaderMap) -> Option<&str> {
    let raw = headers.get(header::AUTHORIZATION)?.to_str().ok()?;
    let (scheme, token) = raw.split_once(' ')?;
    if scheme.eq_ignore_ascii_case("bearer") {
        let trimmed = token.trim();
        if !trimmed.is_empty() {
            return Some(trimmed);
        }
    }
    None
}

fn parse_optional_json<T: Default + for<'de> Deserialize<'de>>(
    body: &[u8],
) -> Result<T, AdminError> {
    if body.is_empty() {
        return Ok(T::default());
    }
    serde_json::from_slice(body).map_err(|err| {
        admin_error(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            format!("invalid JSON request body: {err}"),
        )
    })
}

fn collect_namespaces(mut list: Vec<String>, single: Option<String>) -> Vec<String> {
    if let Some(ns) = single {
        if !ns.trim().is_empty() && !list.contains(&ns) {
            list.push(ns);
        }
    }
    list.retain(|s| !s.trim().is_empty());
    list
}

fn map_membus_error(err: MembusError) -> AdminError {
    match err {
        MembusError::InvalidArgument(msg) => {
            admin_error(StatusCode::BAD_REQUEST, "invalid_argument", msg)
        }
        MembusError::Unauthorized(msg) => {
            admin_error(StatusCode::UNAUTHORIZED, "unauthorized", msg)
        }
        MembusError::Conflict { .. } | MembusError::RoleFenced { .. } => {
            admin_error(StatusCode::CONFLICT, "conflict", err.to_string())
        }
        other => admin_error(StatusCode::BAD_GATEWAY, "upstream_error", other.to_string()),
    }
}

fn admin_error(status: StatusCode, code: &'static str, message: String) -> AdminError {
    AdminError {
        status,
        code,
        message,
    }
}
