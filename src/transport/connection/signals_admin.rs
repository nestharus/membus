use std::sync::Arc;

use tokio::sync::{oneshot, watch};

use crate::protocol::{
    encode_signal_event, encode_stream_end, Frame, FrameKind, Request, Response,
};
use crate::signals::{EachChangeSubscription, LiveCoalescedSubscription};
use crate::storage::compute_namespace_parity_digest_with_timeout;
use crate::transport::connection::state::{
    build_response_frame, send_or_shutdown, ConnectionState,
};
use crate::transport::helpers::{resolve_checkpoint_path, run_blocking_query};
use crate::transport::write_coord::coordinated_apply_replicated_delta;
use crate::types::{MembusError, SignalEvent};

pub(super) async fn dispatch_admin(conn: &Arc<ConnectionState>, req: Request) -> Response {
    let storage = Arc::clone(&conn.ctx.storage);
    match req {
        Request::Checkpoint { dest_dir } => {
            let safe_dest = match resolve_checkpoint_path(
                conn.ctx.config.checkpoint_root.as_deref(),
                &dest_dir,
            ) {
                Ok(path) => path,
                Err(e) => return Response::Error(e),
            };
            let result = tokio::task::spawn_blocking(move || storage.create_checkpoint(&safe_dest))
                .await
                .unwrap_or_else(|e| Err(MembusError::Storage(format!("checkpoint failed: {e}"))));
            match result {
                Ok(seq) => Response::Checkpoint { seq },
                Err(e) => Response::Error(e),
            }
        }
        Request::Health => Response::Health(conn.ctx.health_status()),
        Request::HealthV2 => Response::HealthV2(conn.ctx.health_status()),
        Request::NamespaceDigest {
            namespace,
            key_prefix,
            mode,
        } => {
            let catalog = Arc::clone(&conn.ctx.catalog);
            let node_id = conn.ctx.node_id().to_owned();
            let timeout = conn.ctx.config.query_timeout;
            let result = run_blocking_query(timeout, "namespace_digest", move || {
                compute_namespace_parity_digest_with_timeout(
                    storage.as_ref(),
                    catalog.as_ref(),
                    &node_id,
                    &namespace,
                    &key_prefix,
                    mode,
                    timeout,
                )
            })
            .await;
            match result {
                Ok(digest) => Response::NamespaceDigest(digest),
                Err(e) => Response::Error(e),
            }
        }
        Request::TransitionSlotRole {
            expected_role,
            expected_role_epoch,
            new_role,
        } => {
            let ctx = conn.ctx.clone();
            let result = tokio::task::spawn_blocking(move || {
                ctx.transition_slot_role(expected_role, expected_role_epoch, new_role)
            })
            .await
            .unwrap_or_else(|e| {
                Err(MembusError::Storage(format!(
                    "transition_slot_role failed: {e}"
                )))
            });
            match result {
                Ok(state) => Response::SlotRole(state),
                Err(e) => Response::Error(e),
            }
        }
        Request::ApplyReplicatedDelta {
            delta,
            ack,
            route_origin,
        } => {
            let ctx = conn.ctx.clone();
            let search = conn.ctx.search.clone();
            let local_node_id = conn.ctx.node_id().to_owned();
            let result = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_replicated_apply_fence(None)?;
                coordinated_apply_replicated_delta(
                    &storage,
                    search.as_ref(),
                    &local_node_id,
                    route_origin.as_deref(),
                    &delta,
                    ack,
                )
            })
            .await
            .unwrap_or_else(|e| {
                Err(MembusError::Storage(format!(
                    "apply_replicated_delta failed: {e}"
                )))
            });
            match result {
                Ok(coord) => Response::ReplicatedApply(coord.result),
                Err(e) => Response::Error(e),
            }
        }
        _ => Response::Error(MembusError::Protocol(
            "unexpected request in admin dispatch".to_owned(),
        )),
    }
}

pub(super) struct SubscriptionStreamCtx {
    pub(super) conn: Arc<ConnectionState>,
    pub(super) stream_id: u64,
    pub(super) captured_auth_epoch: u64,
    pub(super) shutdown_rx: watch::Receiver<bool>,
}

pub(super) struct EachChangeParams {
    pub(super) namespace: String,
    pub(super) key_prefix: Vec<u8>,
    pub(super) epoch: u64,
    pub(super) since_seq: u64,
}

pub(super) struct LiveStreamParams {
    pub(super) namespace: String,
    pub(super) key_prefix: Vec<u8>,
    pub(super) max_pending: Option<u32>,
}

enum ActiveSubscription {
    EachChange(EachChangeSubscription),
    Live(LiveCoalescedSubscription),
}

impl ActiveSubscription {
    async fn next_event(&mut self) -> Result<Option<SignalEvent>, MembusError> {
        match self {
            Self::EachChange(s) => s.next_event().await,
            Self::Live(s) => s.next_event().await,
        }
    }
}

pub(super) async fn start_each_change_stream(
    mut stream_ctx: SubscriptionStreamCtx,
    params: EachChangeParams,
) {
    match stream_ctx.conn.ctx.broker.subscribe_each_change(
        &params.namespace,
        params.key_prefix,
        params.epoch,
        params.since_seq,
    ) {
        Ok(sub) => {
            let epoch = sub.epoch();
            let start_seq = sub.cursor_seq();
            launch_subscription_stream(
                stream_ctx,
                epoch,
                start_seq,
                ActiveSubscription::EachChange(sub),
            )
            .await;
        }
        Err(err) => {
            let reply = build_response_frame(
                stream_ctx.stream_id,
                &Response::Error(err),
                stream_ctx.conn.ctx.config.max_frame_body,
            );
            let _ = send_or_shutdown(
                &stream_ctx.conn.outbound_tx,
                reply,
                &mut stream_ctx.shutdown_rx,
            )
            .await;
        }
    }
}

pub(super) async fn start_live_stream(
    mut stream_ctx: SubscriptionStreamCtx,
    params: LiveStreamParams,
) {
    match stream_ctx.conn.ctx.broker.subscribe_live(
        &params.namespace,
        params.key_prefix,
        params.max_pending.map(|v| v as usize),
    ) {
        Ok(sub) => {
            let epoch = sub.epoch();
            let start_seq = stream_ctx.conn.ctx.storage.latest_seq();
            launch_subscription_stream(
                stream_ctx,
                epoch,
                start_seq,
                ActiveSubscription::Live(sub),
            )
            .await;
        }
        Err(err) => {
            let reply = build_response_frame(
                stream_ctx.stream_id,
                &Response::Error(err),
                stream_ctx.conn.ctx.config.max_frame_body,
            );
            let _ = send_or_shutdown(
                &stream_ctx.conn.outbound_tx,
                reply,
                &mut stream_ctx.shutdown_rx,
            )
            .await;
        }
    }
}

async fn launch_subscription_stream(
    mut stream_ctx: SubscriptionStreamCtx,
    epoch: u64,
    start_seq: u64,
    mut sub: ActiveSubscription,
) {
    let conn = stream_ctx.conn;
    let stream_id = stream_ctx.stream_id;
    let captured_auth_epoch = stream_ctx.captured_auth_epoch;
    let max_frame_body = conn.ctx.config.max_frame_body;
    let stream_guard = match conn.ctx.try_acquire_stream_guard() {
        Ok(guard) => guard,
        Err(err) => {
            let reply = build_response_frame(stream_id, &Response::Error(err), max_frame_body);
            let _ = send_or_shutdown(&conn.outbound_tx, reply, &mut stream_ctx.shutdown_rx).await;
            return;
        }
    };
    let (cancel_tx, mut cancel_rx) = oneshot::channel::<()>();
    let register_result = {
        let mut table = conn.active_streams.lock();
        if table.closed {
            return;
        }
        if table.auth_epoch != captured_auth_epoch {
            Err(MembusError::Unauthorized(
                "connection authentication changed while opening subscription".to_owned(),
            ))
        } else if table.streams.contains_key(&stream_id) {
            Err(MembusError::Protocol(format!(
                "duplicate active subscription stream_id {stream_id}"
            )))
        } else if table.streams.len() >= conn.ctx.config.max_subscriptions_per_conn {
            Err(MembusError::Busy {
                retry_after_ms: 10,
                reason: "per-connection subscription limit reached".to_owned(),
            })
        } else {
            table.streams.insert(stream_id, cancel_tx);
            Ok(())
        }
    };
    if let Err(err) = register_result {
        let reply = build_response_frame(stream_id, &Response::Error(err), max_frame_body);
        let _ = send_or_shutdown(&conn.outbound_tx, reply, &mut stream_ctx.shutdown_rx).await;
        return;
    }

    let ack = Response::Subscribed { epoch, start_seq };
    let ack_frame = build_response_frame(stream_id, &ack, max_frame_body);
    if !send_or_shutdown(&conn.outbound_tx, ack_frame, &mut stream_ctx.shutdown_rx).await {
        conn.active_streams.lock().streams.remove(&stream_id);
        return;
    }

    let outbound_tx = conn.outbound_tx.clone();
    let active_streams = Arc::clone(&conn.active_streams);
    let mut shutdown_rx = stream_ctx.shutdown_rx;
    tokio::spawn(async move {
        let _stream_guard = stream_guard;
        let end_err = loop {
            tokio::select! {
                biased;
                _ = &mut cancel_rx => break None,
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break None;
                    }
                }
                next = sub.next_event() => match next {
                    Ok(Some(event)) => {
                        let encoded = encode_signal_event(&event);
                        if encoded.len() > max_frame_body {
                            break Some(MembusError::FrameTooLarge {
                                len: encoded.len(),
                                max_len: max_frame_body,
                            });
                        }
                        let frame = Frame::new(FrameKind::StreamEvent, stream_id, encoded);
                        tokio::select! {
                            biased;
                            _ = &mut cancel_rx => break None,
                            _ = shutdown_rx.changed() => {
                                if *shutdown_rx.borrow() {
                                    break None;
                                }
                            }
                            send_res = outbound_tx.send(frame) => {
                                if send_res.is_err() {
                                    break None;
                                }
                            }
                        }
                    }
                    Ok(None) => break None,
                    Err(err) => break Some(err),
                }
            }
        };
        active_streams.lock().streams.remove(&stream_id);
        let end_frame = Frame::new(
            FrameKind::StreamEnd,
            stream_id,
            encode_stream_end(end_err.as_ref()),
        );
        let _ = outbound_tx.try_send(end_frame);
    });
}
