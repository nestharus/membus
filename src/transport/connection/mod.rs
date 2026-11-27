mod auth;
mod index_search;
mod kv;
mod leases_catalog;
mod signals_admin;
mod state;

use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, watch};

use crate::protocol::message::OP_AUTHENTICATE;
use crate::protocol::{
    decode_request, read_frame, run_batched_frame_writer, Frame, FrameKind, Request, Response,
};
use crate::transport::helpers::ConnectionGuard;
use crate::transport::ServerContext;
use crate::types::{MembusError, Principal};

use auth::authorize_request;
use signals_admin::{
    start_each_change_stream, start_live_stream, EachChangeParams, LiveStreamParams,
    SubscriptionStreamCtx,
};
use state::{apply_authenticate_inline, build_response_frame, send_or_shutdown, ConnectionState};

/// Serve one multiplexed client connection until EOF, protocol error, or server shutdown.
pub async fn serve_connection<S>(
    stream: S,
    ctx: ServerContext,
    mut shutdown_rx: watch::Receiver<bool>,
) where
    S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
    let _conn_guard = ConnectionGuard::acquire(&ctx.active_connections);
    let (mut reader, mut writer) = tokio::io::split(stream);
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Frame>(ctx.config.outbound_queue_capacity);
    let max_frame_body = ctx.config.max_frame_body;

    let writer_task = tokio::spawn(async move {
        run_batched_frame_writer(&mut writer, &mut outbound_rx, max_frame_body).await;
    });

    let conn = Arc::new(ConnectionState::new(ctx, outbound_tx));
    let mut read_buf = BytesMut::with_capacity(64 * 1024);
    let snapshot_ttl = conn.ctx.config.snapshot_ttl;
    let reap_period = if snapshot_ttl.is_zero() {
        Duration::from_secs(3600)
    } else {
        (snapshot_ttl / 2).clamp(Duration::from_millis(25), Duration::from_secs(30))
    };
    let mut reap_interval = tokio::time::interval(reap_period);
    reap_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        let frame_opt = tokio::select! {
            biased;
            _ = shutdown_rx.changed() => {
                if *shutdown_rx.borrow() {
                    break;
                }
                continue;
            }
            _ = reap_interval.tick() => {
                conn.snapshots.lock().reap_expired(snapshot_ttl);
                continue;
            }
            res = read_frame(&mut reader, &mut read_buf, max_frame_body) => {
                match res {
                    Ok(frame_opt) => frame_opt,
                    Err(_) => break,
                }
            }
        };

        let Some(frame) = frame_opt else {
            break;
        };

        if !handle_inbound_frame(&conn, frame, max_frame_body, &mut shutdown_rx).await {
            break;
        }
    }

    conn.close_and_cancel_all_streams();
    drop(conn);
    let _ = writer_task.await;
}

async fn handle_inbound_frame(
    conn: &Arc<ConnectionState>,
    frame: Frame,
    max_frame_body: usize,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> bool {
    match frame.kind {
        FrameKind::Ping => {
            let pong = Frame::new(FrameKind::Pong, frame.stream_id, frame.payload);
            send_or_shutdown(&conn.outbound_tx, pong, shutdown_rx).await
        }
        FrameKind::Cancel => {
            let cancel = conn.active_streams.lock().streams.remove(&frame.stream_id);
            if let Some(tx) = cancel {
                let _ = tx.send(());
            }
            true
        }
        FrameKind::Request => {
            // Process `Authenticate` synchronously inline on the reader loop BEFORE
            // dispatching any subsequent pipelined frames so `[Authenticate, Get]`
            // never races and principal switches immediately revoke active streams.
            if frame.payload.first() == Some(&OP_AUTHENTICATE) {
                let response = match decode_request(&frame.payload) {
                    Ok(Request::Authenticate { token }) => apply_authenticate_inline(conn, &token),
                    Ok(_) => Response::Error(MembusError::Protocol(
                        "malformed authenticate frame".to_owned(),
                    )),
                    Err(err) => Response::Error(err),
                };
                let reply = build_response_frame(frame.stream_id, &response, max_frame_body);
                return send_or_shutdown(&conn.outbound_tx, reply, shutdown_rx).await;
            }

            let permit = match Arc::clone(&conn.credit_window).try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    let busy = Response::Error(MembusError::Busy {
                        retry_after_ms: 5,
                        reason: "per-connection credit window exhausted".to_owned(),
                    });
                    let reply = build_response_frame(frame.stream_id, &busy, max_frame_body);
                    return send_or_shutdown(&conn.outbound_tx, reply, shutdown_rx).await;
                }
            };

            let conn_ref = Arc::clone(conn);
            let task_shutdown = shutdown_rx.clone();
            tokio::spawn(async move {
                let _permit = permit;
                handle_request_frame(conn_ref, frame.stream_id, frame.payload, task_shutdown).await;
            });
            true
        }
        _ => {
            let err = Response::Error(MembusError::Protocol(format!(
                "unexpected client frame kind {:?}",
                frame.kind
            )));
            let reply = build_response_frame(frame.stream_id, &err, max_frame_body);
            let _ = send_or_shutdown(&conn.outbound_tx, reply, shutdown_rx).await;
            false
        }
    }
}

async fn handle_request_frame(
    conn: Arc<ConnectionState>,
    stream_id: u64,
    payload: Bytes,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let max_frame_body = conn.ctx.config.max_frame_body;
    let request = match decode_request(&payload) {
        Ok(req) => req,
        Err(err) => {
            let reply = build_response_frame(stream_id, &Response::Error(err), max_frame_body);
            let _ = send_or_shutdown(&conn.outbound_tx, reply, &mut shutdown_rx).await;
            return;
        }
    };

    let (principal, captured_auth_epoch) = conn.snapshot_auth();
    if let Err(err) = authorize_request(&conn.ctx.auth, principal.as_ref(), &request) {
        let reply = build_response_frame(stream_id, &Response::Error(err), max_frame_body);
        let _ = send_or_shutdown(&conn.outbound_tx, reply, &mut shutdown_rx).await;
        return;
    }

    match request {
        Request::SubscribeEachChange {
            namespace,
            key_prefix,
            epoch,
            since_seq,
        } => {
            start_each_change_stream(
                SubscriptionStreamCtx {
                    conn,
                    stream_id,
                    captured_auth_epoch,
                    shutdown_rx,
                },
                EachChangeParams {
                    namespace,
                    key_prefix,
                    epoch,
                    since_seq,
                },
            )
            .await;
            return;
        }
        Request::SubscribeLive {
            namespace,
            key_prefix,
            max_pending,
        } => {
            start_live_stream(
                SubscriptionStreamCtx {
                    conn,
                    stream_id,
                    captured_auth_epoch,
                    shutdown_rx,
                },
                LiveStreamParams {
                    namespace,
                    key_prefix,
                    max_pending,
                },
            )
            .await;
            return;
        }
        _ => {}
    }

    let response = dispatch_unary(&conn, principal.as_ref(), request).await;
    let reply = build_response_frame(stream_id, &response, max_frame_body);
    let _ = send_or_shutdown(&conn.outbound_tx, reply, &mut shutdown_rx).await;
}

async fn dispatch_unary(
    conn: &Arc<ConnectionState>,
    principal: Option<&Principal>,
    req: Request,
) -> Response {
    match req {
        Request::Authenticate { token } => apply_authenticate_inline(conn, &token),
        Request::Get { .. }
        | Request::GetMany { .. }
        | Request::Put { .. }
        | Request::PutIndexed { .. }
        | Request::Remove { .. }
        | Request::RemoveIndexed { .. } => kv::dispatch_kv_rw(conn, req).await,
        Request::CommitTx { .. } | Request::CommitIndexedTx { .. } => {
            kv::dispatch_tx(conn, req).await
        }
        Request::OpenSnapshot
        | Request::CloseSnapshot { .. }
        | Request::ScanPrefix { .. }
        | Request::ScanRange { .. } => kv::dispatch_snapshot_scan(conn, req).await,
        Request::PutBlob { .. }
        | Request::GetBlob { .. }
        | Request::PutCasBlob { .. }
        | Request::GetCasBlob { .. }
        | Request::HasCasBlob { .. } => kv::dispatch_blob(conn, req).await,
        Request::QueryOrderedIndex { .. }
        | Request::QueryNeighbors { .. }
        | Request::BfsTraverse { .. } => index_search::dispatch_index_query(conn, req).await,
        Request::IndexDocument { .. }
        | Request::DeleteSearchDocument { .. }
        | Request::RevokeSearchPrefix { .. }
        | Request::Search(_) => index_search::dispatch_search(conn, req).await,
        Request::AcquireLease { .. }
        | Request::RenewLease { .. }
        | Request::ReleaseLease { .. }
        | Request::InspectLease { .. }
        | Request::ValidateLease { .. } => leases_catalog::dispatch_lease(conn, req).await,
        Request::RegisterCollection { .. }
        | Request::GetCollection { .. }
        | Request::ListCollections
        | Request::GetCollectionVersion { .. }
        | Request::BeginMigration { .. }
        | Request::CommitMigration { .. }
        | Request::AbortMigration { .. }
        | Request::AdvanceMigrationVerification { .. }
        | Request::EnterReverseShadow { .. }
        | Request::RollbackMigration { .. }
        | Request::ContractMigration { .. } => {
            leases_catalog::dispatch_catalog(conn, principal, req).await
        }
        Request::Checkpoint { .. }
        | Request::Health
        | Request::HealthV2
        | Request::NamespaceDigest { .. }
        | Request::TransitionSlotRole { .. }
        | Request::ApplyReplicatedDelta { .. } => signals_admin::dispatch_admin(conn, req).await,
        Request::SubscribeEachChange { .. } | Request::SubscribeLive { .. } => {
            Response::Error(MembusError::Protocol(
                "subscription request misrouted to unary dispatch".to_owned(),
            ))
        }
    }
}
