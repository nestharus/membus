use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket};
use futures_util::stream::SplitSink;
use futures_util::SinkExt;
use parking_lot::Mutex;
use serde::Deserialize;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch};

use crate::transport::http::state::{
    default_ack, default_expected, wait_shutdown_ref, StreamSub,
};
use crate::transport::ServerContext;
use crate::types::{
    Ack, ExpectedVersion, IndexMutations, MembusError, Principal, SearchLogOp, SearchQuery,
};

#[derive(Deserialize)]
pub(super) struct WsEnvelope {
    pub(super) id: u64,
    #[serde(flatten)]
    pub(super) op: WsOp,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub(super) enum WsOp {
    Ping,
    Health,
    Authenticate {
        token: String,
    },
    Get {
        namespace: String,
        key_utf8: Option<String>,
        key_b64: Option<String>,
    },
    Put {
        namespace: String,
        key_utf8: Option<String>,
        key_b64: Option<String>,
        value_utf8: Option<String>,
        value_b64: Option<String>,
        #[serde(default = "default_ack")]
        ack: Ack,
        #[serde(default = "default_expected")]
        expected: ExpectedVersion,
        indexes: Option<IndexMutations>,
        search_op: Option<SearchLogOp>,
    },
    Remove {
        namespace: String,
        key_utf8: Option<String>,
        key_b64: Option<String>,
        #[serde(default = "default_ack")]
        ack: Ack,
        #[serde(default = "default_expected")]
        expected: ExpectedVersion,
        search_op: Option<SearchLogOp>,
    },
    Search {
        query: SearchQuery,
    },
    SubscribeEach {
        namespace: String,
        prefix_utf8: Option<String>,
        prefix_b64: Option<String>,
        epoch: Option<u64>,
        since_seq: Option<u64>,
    },
    SubscribeLive {
        namespace: String,
        prefix_utf8: Option<String>,
        prefix_b64: Option<String>,
        max_pending: Option<usize>,
    },
    Cancel {
        stream_id: u64,
    },
}

pub(super) async fn run_ws_writer(
    mut ws_tx: SplitSink<WebSocket, Message>,
    mut control_rx: mpsc::Receiver<Message>,
    mut data_rx: mpsc::Receiver<Message>,
    mut writer_shutdown: watch::Receiver<bool>,
) {
    loop {
        let msg_opt = tokio::select! {
            biased;
            _ = wait_shutdown_ref(&mut writer_shutdown) => None,
            ctrl = control_rx.recv() => ctrl,
            data = data_rx.recv() => data,
        };
        let Some(m) = msg_opt else {
            break;
        };
        let send_ok = tokio::select! {
            biased;
            _ = wait_shutdown_ref(&mut writer_shutdown) => false,
            res = tokio::time::timeout(Duration::from_secs(5), ws_tx.send(m)) => {
                matches!(res, Ok(Ok(())))
            }
        };
        if !send_ok {
            break;
        }
    }
    if !*writer_shutdown.borrow() {
        let _ = tokio::time::timeout(Duration::from_millis(500), ws_tx.close()).await;
    }
}

pub(super) struct WsStreamTable {
    pub(super) closed: bool,
    pub(super) auth_epoch: u64,
    pub(super) authenticated: Option<Principal>,
    pub(super) streams: HashMap<u64, oneshot::Sender<()>>,
}

pub(super) struct WsConnState {
    pub(super) ctx: ServerContext,
    pub(super) active_streams: Mutex<WsStreamTable>,
    /// Priority control channel for RPC replies, pongs, `subscribed`, and `stream_end` frames.
    pub(super) control_tx: mpsc::Sender<Message>,
    /// Bounded data channel for push `signal` frames (non-blocking `try_send`).
    pub(super) data_tx: mpsc::Sender<Message>,
}

impl WsConnState {
    pub(super) fn snapshot_auth(&self) -> (Option<Principal>, u64) {
        let table = self.active_streams.lock();
        (table.authenticated.clone(), table.auth_epoch)
    }

    pub(super) fn close_all_streams(&self) {
        let mut table = self.active_streams.lock();
        table.closed = true;
        for (_id, tx) in table.streams.drain() {
            let _ = tx.send(());
        }
    }
}

pub(super) struct WsSubscriptionMeta {
    pub(super) stream_id: u64,
    pub(super) captured_auth_epoch: u64,
    pub(super) epoch: u64,
    pub(super) start_seq: u64,
}

pub(super) fn ws_ok_msg(id: u64, kind: &str, data: serde_json::Value) -> Message {
    Message::Text(
        serde_json::json!({
            "id": id,
            "ok": true,
            "kind": kind,
            "data": data,
        })
        .to_string()
        .into(),
    )
}

pub(super) fn ws_err_msg(id: u64, err: &MembusError) -> Message {
    Message::Text(
        serde_json::json!({
            "id": id,
            "ok": false,
            "error": err.to_string(),
        })
        .to_string()
        .into(),
    )
}

pub(super) async fn send_ws_or_shutdown(
    tx: &mpsc::Sender<Message>,
    msg: Message,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> bool {
    if *shutdown_rx.borrow() {
        return false;
    }
    tokio::select! {
        biased;
        _ = wait_shutdown_ref(shutdown_rx) => false,
        res = tx.send(msg) => res.is_ok(),
    }
}

pub(super) fn apply_ws_authenticate_inline(conn: &WsConnState, id: u64, token: &str) -> Message {
    let auth_res = conn.ctx.auth.authenticate_token(Some(token));
    let mut table = conn.active_streams.lock();
    match auth_res {
        Ok(Some(p)) => {
            let pid = p.id.clone();
            let changed = table.authenticated.as_ref() != Some(&p);
            table.authenticated = Some(p);
            if changed {
                table.auth_epoch = table.auth_epoch.saturating_add(1);
                for (_id, tx) in table.streams.drain() {
                    let _ = tx.send(());
                }
            }
            ws_ok_msg(id, "authenticated", serde_json::json!({ "principal_id": pid }))
        }
        Ok(None) => ws_ok_msg(
            id,
            "authenticated",
            serde_json::json!({ "principal_id": "anonymous" }),
        ),
        Err(err) => {
            table.authenticated = None;
            table.auth_epoch = table.auth_epoch.saturating_add(1);
            for (_id, tx) in table.streams.drain() {
                let _ = tx.send(());
            }
            ws_err_msg(id, &err)
        }
    }
}

pub(super) async fn launch_ws_subscription(
    conn: Arc<WsConnState>,
    meta: WsSubscriptionMeta,
    mut sub: StreamSub,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let stream_id = meta.stream_id;
    let stream_guard = match conn.ctx.try_acquire_stream_guard() {
        Ok(guard) => guard,
        Err(err) => {
            let _ =
                send_ws_or_shutdown(&conn.control_tx, ws_err_msg(stream_id, &err), &mut shutdown_rx)
                    .await;
            return;
        }
    };
    let (cancel_tx, mut cancel_rx) = oneshot::channel::<()>();
    let register_result = {
        let mut table = conn.active_streams.lock();
        if table.closed {
            return;
        }
        if table.auth_epoch != meta.captured_auth_epoch {
            Err(MembusError::Unauthorized(
                "ws connection authentication changed while opening subscription".to_owned(),
            ))
        } else if table.streams.contains_key(&stream_id) {
            Err(MembusError::Protocol(format!(
                "duplicate ws stream_id {stream_id}"
            )))
        } else if table.streams.len() >= conn.ctx.config.max_subscriptions_per_conn {
            Err(MembusError::Busy {
                retry_after_ms: 10,
                reason: "ws per-connection subscription limit reached".to_owned(),
            })
        } else {
            table.streams.insert(stream_id, cancel_tx);
            Ok(())
        }
    };
    if let Err(err) = register_result {
        let _ = send_ws_or_shutdown(&conn.control_tx, ws_err_msg(stream_id, &err), &mut shutdown_rx)
            .await;
        return;
    }

    let ack = ws_ok_msg(
        stream_id,
        "subscribed",
        serde_json::json!({ "epoch": meta.epoch, "start_seq": meta.start_seq }),
    );
    if !send_ws_or_shutdown(&conn.control_tx, ack, &mut shutdown_rx).await {
        conn.active_streams.lock().streams.remove(&stream_id);
        return;
    }

    tokio::spawn(async move {
        let _stream_guard = stream_guard;
        let end_err: Option<String> = loop {
            tokio::select! {
                biased;
                _ = &mut cancel_rx => break None,
                _ = wait_shutdown_ref(&mut shutdown_rx) => break None,
                next = sub.next_event() => match next {
                    Ok(Some(ev)) => {
                        let msg = Message::Text(
                            serde_json::json!({
                                "id": stream_id,
                                "kind": "signal",
                                "data": ev,
                            })
                            .to_string()
                            .into(),
                        );
                        // Non-blocking try_send ensures a slow subscriber cannot freeze the socket.
                        match conn.data_tx.try_send(msg) {
                            Ok(()) => {}
                            Err(TrySendError::Full(_)) => {
                                break Some("ws outbound queue overflowed".to_owned());
                            }
                            Err(TrySendError::Closed(_)) => break None,
                        }
                    }
                    Ok(None) => break None,
                    Err(err) => break Some(err.to_string()),
                }
            }
        };
        conn.active_streams.lock().streams.remove(&stream_id);
        let end = Message::Text(
            serde_json::json!({
                "id": stream_id,
                "kind": "stream_end",
                "error": end_err,
            })
            .to_string()
            .into(),
        );
        // Send `stream_end` on the priority control channel so overflowed data queues never drop it.
        let _ = send_ws_or_shutdown(&conn.control_tx, end, &mut shutdown_rx).await;
    });
}
