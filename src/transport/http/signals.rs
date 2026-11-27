use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event, KeepAlive, Sse};
use serde::Deserialize;
use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;

use crate::transport::helpers::ConnectionGuard;
use crate::transport::http::state::{
    authenticate_http, decode_bytes_field, wait_shutdown_ref, HttpError, HttpState, StreamSub,
};
use crate::types::{MembusError, Permission};

#[derive(Deserialize)]
pub(super) struct SseQuery {
    mode: Option<String>,
    prefix: Option<String>,
    prefix_b64: Option<String>,
    epoch: Option<u64>,
    since_seq: Option<u64>,
    max_pending: Option<usize>,
}

pub(super) async fn handle_signals_sse(
    State(state): State<HttpState>,
    headers: HeaderMap,
    Path(ns): Path<String>,
    Query(q): Query<SseQuery>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<Event, Infallible>>>, HttpError> {
    let principal = authenticate_http(&state, &headers)?;
    let key_prefix = decode_bytes_field(q.prefix, q.prefix_b64, "prefix")?;
    state.ctx.auth.authorize(
        principal.as_ref(),
        &ns,
        &key_prefix,
        Permission::Read,
    )?;

    let mode = q.mode.as_deref().unwrap_or("each");
    let (mut sub, epoch, start_seq) = if mode.eq_ignore_ascii_case("live") {
        let s = state
            .ctx
            .broker
            .subscribe_live(&ns, key_prefix, q.max_pending)?;
        let ep = s.epoch();
        let seq = state.ctx.storage.latest_seq();
        (StreamSub::Live(s), ep, seq)
    } else if mode.eq_ignore_ascii_case("each") {
        let ep = q.epoch.unwrap_or(0);
        let since = q.since_seq.unwrap_or_else(|| state.ctx.storage.latest_seq());
        let s = state
            .ctx
            .broker
            .subscribe_each_change(&ns, key_prefix, ep, since)?;
        let ep_out = s.epoch();
        let seq_out = s.cursor_seq();
        (StreamSub::EachChange(s), ep_out, seq_out)
    } else {
        return Err(HttpError(MembusError::InvalidArgument(format!(
            "invalid SSE mode '{mode}' (expected 'each' or 'live')"
        ))));
    };

    let stream_guard = state.ctx.try_acquire_stream_guard()?;
    let (tx, rx) = mpsc::channel::<Result<Event, Infallible>>(256);
    let mut shutdown_rx = state.shutdown_rx.clone();
    let active_connections = Arc::clone(&state.ctx.active_connections);

    tokio::spawn(async move {
        let _stream_guard = stream_guard;
        let _conn_guard = ConnectionGuard::acquire(&active_connections);
        let init_json = serde_json::json!({
            "epoch": epoch,
            "start_seq": start_seq,
        })
        .to_string();
        if !send_sse_or_shutdown(
            &tx,
            Ok(Event::default().event("subscribed").data(init_json)),
            &mut shutdown_rx,
        )
        .await
        {
            return;
        }

        loop {
            tokio::select! {
                biased;
                _ = wait_shutdown_ref(&mut shutdown_rx) => break,
                _ = tx.closed() => break,
                next = sub.next_event() => match next {
                    Ok(Some(ev)) => {
                        let Ok(data) = serde_json::to_string(&ev) else {
                            continue;
                        };
                        let sse_ev = Event::default()
                            .event("signal")
                            .id(ev.seq.to_string())
                            .data(data);
                        if !send_sse_or_shutdown(&tx, Ok(sse_ev), &mut shutdown_rx).await {
                            break;
                        }
                    }
                    Ok(None) => break,
                    Err(err) => {
                        let payload = serde_json::json!({ "error": err.to_string() }).to_string();
                        let _ = send_sse_or_shutdown(
                            &tx,
                            Ok(Event::default().event("error").data(payload)),
                            &mut shutdown_rx,
                        )
                        .await;
                        break;
                    }
                }
            }
        }
    });

    Ok(Sse::new(ReceiverStream::new(rx))
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(15))))
}

async fn send_sse_or_shutdown(
    tx: &mpsc::Sender<Result<Event, Infallible>>,
    item: Result<Event, Infallible>,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> bool {
    if *shutdown_rx.borrow() {
        return false;
    }
    tokio::select! {
        biased;
        _ = wait_shutdown_ref(shutdown_rx) => false,
        res = tx.send(item) => res.is_ok(),
    }
}
