use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::Response as AxumResponse;
use futures_util::StreamExt;
use parking_lot::Mutex;
use tokio::sync::{mpsc, watch, Semaphore};

use crate::transport::helpers::{
    authorize_index_mutations, authorize_search_op, run_blocking_query, ConnectionGuard,
};
use crate::transport::http::state::{
    decode_bytes_field, wait_shutdown_ref, AuthPrincipal, HttpCommitResp, HttpError, HttpState,
    StreamSub,
};
use crate::transport::http::ws_stream::{
    apply_ws_authenticate_inline, launch_ws_subscription, run_ws_writer, send_ws_or_shutdown,
    ws_err_msg, ws_ok_msg, WsConnState, WsEnvelope, WsOp, WsStreamTable, WsSubscriptionMeta,
};
use crate::transport::write_coord::{
    coordinated_put_indexed, coordinated_remove_indexed, CoordinatedPutArgs, CoordinatedRemoveArgs,
};
use crate::types::{MembusError, Permission, Principal, SearchQuery};

pub(super) async fn handle_ws_upgrade(
    AuthPrincipal(initial_principal): AuthPrincipal,
    State(state): State<HttpState>,
    ws: WebSocketUpgrade,
) -> Result<AxumResponse, HttpError> {
    let max_msg = state.ctx.config.max_frame_body;
    Ok(ws
        .max_message_size(max_msg)
        .max_frame_size(max_msg)
        .on_upgrade(move |socket| handle_ws_socket(socket, state, initial_principal)))
}

async fn handle_ws_socket(
    socket: WebSocket,
    state: HttpState,
    initial_principal: Option<Principal>,
) {
    let _conn_guard = ConnectionGuard::acquire(&state.ctx.active_connections);
    let (ws_tx, mut ws_rx) = socket.split();
    let control_cap = state
        .ctx
        .config
        .max_subscriptions_per_conn
        .saturating_mul(2)
        .saturating_add(128)
        .max(256);
    let (control_tx, control_rx) = mpsc::channel::<Message>(control_cap);
    let (data_tx, data_rx) = mpsc::channel::<Message>(state.ctx.config.outbound_queue_capacity);

    let writer_task = tokio::spawn(run_ws_writer(
        ws_tx,
        control_rx,
        data_rx,
        state.shutdown_rx.clone(),
    ));

    let credit_window = Arc::new(Semaphore::new(
        state.ctx.config.max_in_flight_per_conn.max(1),
    ));
    let conn = Arc::new(WsConnState {
        ctx: state.ctx.clone(),
        active_streams: Mutex::new(WsStreamTable {
            closed: false,
            auth_epoch: 0,
            authenticated: initial_principal,
            streams: HashMap::new(),
        }),
        control_tx,
        data_tx,
    });

    let mut shutdown_rx = state.shutdown_rx.clone();
    loop {
        let next_msg = tokio::select! {
            biased;
            _ = wait_shutdown_ref(&mut shutdown_rx) => break,
            item = ws_rx.next() => item,
        };

        let Some(Ok(msg)) = next_msg else {
            break;
        };

        match msg {
            Message::Text(text) => {
                if !handle_ws_text_frame(&conn, &credit_window, &text, &mut shutdown_rx).await {
                    break;
                }
            }
            Message::Ping(payload) => {
                let _ = conn.control_tx.try_send(Message::Pong(payload));
            }
            Message::Close(_) => break,
            Message::Pong(_) | Message::Binary(_) => {}
        }
    }

    conn.close_all_streams();
    drop(conn);
    let _ = writer_task.await;
}

async fn handle_ws_text_frame(
    conn: &Arc<WsConnState>,
    credit_window: &Arc<Semaphore>,
    text: &str,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> bool {
    let env: WsEnvelope = match serde_json::from_str(text) {
        Ok(e) => e,
        Err(err) => {
            let reply = ws_err_msg(0, &MembusError::Protocol(format!("invalid ws JSON: {err}")));
            return send_ws_or_shutdown(&conn.control_tx, reply, shutdown_rx).await;
        }
    };
    // Handle Authenticate & Cancel synchronously inline BEFORE subsequent messages.
    match env.op {
        WsOp::Authenticate { token } => {
            let reply = apply_ws_authenticate_inline(conn, env.id, &token);
            return send_ws_or_shutdown(&conn.control_tx, reply, shutdown_rx).await;
        }
        WsOp::Cancel { stream_id } => {
            if let Some(tx) = conn.active_streams.lock().streams.remove(&stream_id) {
                let _ = tx.send(());
            }
            let ack = ws_ok_msg(
                env.id,
                "cancelled",
                serde_json::json!({ "stream_id": stream_id }),
            );
            return send_ws_or_shutdown(&conn.control_tx, ack, shutdown_rx).await;
        }
        _ => {}
    }

    let permit = match Arc::clone(credit_window).try_acquire_owned() {
        Ok(p) => p,
        Err(_) => {
            let busy = ws_err_msg(
                env.id,
                &MembusError::Busy {
                    retry_after_ms: 5,
                    reason: "ws per-connection credit window exhausted".to_owned(),
                },
            );
            return send_ws_or_shutdown(&conn.control_tx, busy, shutdown_rx).await;
        }
    };

    let conn_ref = Arc::clone(conn);
    let task_shutdown = shutdown_rx.clone();
    tokio::spawn(async move {
        let _permit = permit;
        handle_ws_envelope(conn_ref, env, task_shutdown).await;
    });
    true
}

async fn send_ws_op_result(
    conn: &WsConnState,
    id: u64,
    result: Result<(&'static str, serde_json::Value), MembusError>,
    shutdown_rx: &mut watch::Receiver<bool>,
) {
    let msg = match result {
        Ok((kind, data)) => ws_ok_msg(id, kind, data),
        Err(err) => ws_err_msg(id, &err),
    };
    let _ = send_ws_or_shutdown(&conn.control_tx, msg, shutdown_rx).await;
}

async fn handle_ws_envelope(
    conn: Arc<WsConnState>,
    env: WsEnvelope,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let id = env.id;
    let (principal, captured_auth_epoch) = conn.snapshot_auth();
    match env.op {
        WsOp::Ping => {
            let _ = send_ws_or_shutdown(
                &conn.control_tx,
                ws_ok_msg(id, "pong", serde_json::json!({})),
                &mut shutdown_rx,
            )
            .await;
        }
        WsOp::Authenticate { token } => {
            let reply = apply_ws_authenticate_inline(&conn, id, &token);
            let _ = send_ws_or_shutdown(&conn.control_tx, reply, &mut shutdown_rx).await;
        }
        WsOp::Health => {
            let res = handle_ws_health(&conn, principal.as_ref());
            send_ws_op_result(&conn, id, res, &mut shutdown_rx).await;
        }
        WsOp::Get { .. } | WsOp::Put { .. } | WsOp::Remove { .. } => {
            let res = handle_ws_kv(&conn, principal.as_ref(), env.op).await;
            send_ws_op_result(&conn, id, res, &mut shutdown_rx).await;
        }
        WsOp::Search { query } => {
            let res = handle_ws_search(&conn, principal.as_ref(), query).await;
            send_ws_op_result(&conn, id, res, &mut shutdown_rx).await;
        }
        WsOp::SubscribeEach { .. } | WsOp::SubscribeLive { .. } => {
            handle_ws_subscribe(
                conn,
                id,
                principal.as_ref(),
                captured_auth_epoch,
                env.op,
                shutdown_rx,
            )
            .await;
        }
        WsOp::Cancel { .. } => {}
    }
}

fn handle_ws_health(
    conn: &WsConnState,
    principal: Option<&Principal>,
) -> Result<(&'static str, serde_json::Value), MembusError> {
    conn.ctx.auth.require_authenticated(principal)?;
    let status = conn.ctx.health_status();
    Ok(("health", serde_json::to_value(status).unwrap_or_default()))
}

async fn handle_ws_kv(
    conn: &WsConnState,
    principal: Option<&Principal>,
    op: WsOp,
) -> Result<(&'static str, serde_json::Value), MembusError> {
    match op {
        WsOp::Get {
            namespace,
            key_utf8,
            key_b64,
        } => {
            let key = decode_bytes_field(key_utf8, key_b64, "key")?;
            conn.ctx
                .auth
                .authorize(principal, &namespace, &key, Permission::Read)?;
            let storage = Arc::clone(&conn.ctx.storage);
            let rec = tokio::task::spawn_blocking(move || storage.get(&namespace, &key))
                .await
                .unwrap_or_else(|e| Err(MembusError::Storage(format!("ws get failed: {e}"))))?;
            Ok(("get", serde_json::to_value(rec).unwrap_or_default()))
        }
        WsOp::Put {
            namespace,
            key_utf8,
            key_b64,
            value_utf8,
            value_b64,
            ack,
            expected,
            indexes,
            search_op,
        } => {
            let key = decode_bytes_field(key_utf8, key_b64, "key")?;
            let value = decode_bytes_field(value_utf8, value_b64, "value")?;
            conn.ctx
                .auth
                .authorize(principal, &namespace, &key, Permission::Write)?;
            if let Some(idx) = &indexes {
                authorize_index_mutations(&conn.ctx.auth, principal, &namespace, idx)?;
            }
            if let Some(op) = &search_op {
                authorize_search_op(&conn.ctx.auth, principal, op)?;
            }
            let ctx = conn.ctx.clone();
            let storage = Arc::clone(&conn.ctx.storage);
            let search = conn.ctx.search.clone();
            let coord = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                coordinated_put_indexed(
                    &storage,
                    search.as_ref(),
                    CoordinatedPutArgs {
                        namespace: &namespace,
                        key: &key,
                        value: &value,
                        ack,
                        expected,
                        indexes: indexes.as_ref(),
                        search_op,
                        idempotency: None,
                    },
                )
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("ws put failed: {e}"))))?;
            let payload = HttpCommitResp {
                commit: coord.commit,
                search_sync: coord.search_sync,
            };
            Ok(("commit", serde_json::to_value(payload).unwrap_or_default()))
        }
        WsOp::Remove {
            namespace,
            key_utf8,
            key_b64,
            ack,
            expected,
            search_op,
        } => {
            let key = decode_bytes_field(key_utf8, key_b64, "key")?;
            conn.ctx
                .auth
                .authorize(principal, &namespace, &key, Permission::Write)?;
            if let Some(op) = &search_op {
                authorize_search_op(&conn.ctx.auth, principal, op)?;
            }
            let ctx = conn.ctx.clone();
            let storage = Arc::clone(&conn.ctx.storage);
            let search = conn.ctx.search.clone();
            let coord = tokio::task::spawn_blocking(move || {
                let _fence = ctx.acquire_client_write_fence(None)?;
                coordinated_remove_indexed(
                    &storage,
                    search.as_ref(),
                    CoordinatedRemoveArgs {
                        namespace: &namespace,
                        key: &key,
                        ack,
                        expected,
                        search_op,
                        idempotency: None,
                    },
                )
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("ws remove failed: {e}"))))?;
            let payload = HttpCommitResp {
                commit: coord.commit,
                search_sync: coord.search_sync,
            };
            Ok(("commit", serde_json::to_value(payload).unwrap_or_default()))
        }
        _ => Err(MembusError::Protocol(
            "unexpected op in ws kv handler".to_owned(),
        )),
    }
}

async fn handle_ws_search(
    conn: &WsConnState,
    principal: Option<&Principal>,
    query: SearchQuery,
) -> Result<(&'static str, serde_json::Value), MembusError> {
    let ns = query.namespace.as_deref().unwrap_or("*");
    conn.ctx.auth.authorize(principal, ns, b"", Permission::Read)?;
    let search = conn.ctx.search.clone().ok_or_else(|| {
        MembusError::InvalidArgument("search engine is not enabled on this server".to_owned())
    })?;
    let page = run_blocking_query(conn.ctx.config.query_timeout, "ws_search", move || {
        search.search(&query)
    })
    .await?;
    Ok(("search", serde_json::to_value(page).unwrap_or_default()))
}

async fn handle_ws_subscribe(
    conn: Arc<WsConnState>,
    id: u64,
    principal: Option<&Principal>,
    captured_auth_epoch: u64,
    op: WsOp,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let open_res: Result<(u64, u64, StreamSub), MembusError> = match op {
        WsOp::SubscribeEach {
            namespace,
            prefix_utf8,
            prefix_b64,
            epoch,
            since_seq,
        } => (|| {
            let prefix = decode_bytes_field(prefix_utf8, prefix_b64, "prefix")?;
            conn.ctx
                .auth
                .authorize(principal, &namespace, &prefix, Permission::Read)?;
            let ep = epoch.unwrap_or(0);
            let since = since_seq.unwrap_or_else(|| conn.ctx.storage.latest_seq());
            let sub = conn
                .ctx
                .broker
                .subscribe_each_change(&namespace, prefix, ep, since)?;
            let ep_out = sub.epoch();
            let seq_out = sub.cursor_seq();
            Ok((ep_out, seq_out, StreamSub::EachChange(sub)))
        })(),
        WsOp::SubscribeLive {
            namespace,
            prefix_utf8,
            prefix_b64,
            max_pending,
        } => (|| {
            let prefix = decode_bytes_field(prefix_utf8, prefix_b64, "prefix")?;
            conn.ctx
                .auth
                .authorize(principal, &namespace, &prefix, Permission::Read)?;
            let sub = conn.ctx.broker.subscribe_live(&namespace, prefix, max_pending)?;
            let ep_out = sub.epoch();
            let seq_out = conn.ctx.storage.latest_seq();
            Ok((ep_out, seq_out, StreamSub::Live(sub)))
        })(),
        _ => Err(MembusError::Protocol(
            "unexpected op in ws subscribe handler".to_owned(),
        )),
    };

    match open_res {
        Ok((epoch, start_seq, sub)) => {
            launch_ws_subscription(
                conn,
                WsSubscriptionMeta {
                    stream_id: id,
                    captured_auth_epoch,
                    epoch,
                    start_seq,
                },
                sub,
                shutdown_rx,
            )
            .await;
        }
        Err(err) => {
            let _ =
                send_ws_or_shutdown(&conn.control_tx, ws_err_msg(id, &err), &mut shutdown_rx).await;
        }
    }
}
