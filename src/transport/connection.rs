use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::{mpsc, oneshot, watch, Semaphore};

use crate::protocol::{
    decode_request, encode_response, encode_signal_event, encode_stream_end, read_frame,
    run_batched_frame_writer, Frame, FrameFlags, FrameKind, Request, Response,
};
use crate::signals::{EachChangeSubscription, LiveCoalescedSubscription};
use crate::storage::OwnedSnapshot;
use crate::transport::ServerContext;
use crate::types::{HealthStatus, MembusError, SignalEvent};

struct ConnectionGuard {
    counter: Arc<std::sync::atomic::AtomicUsize>,
}

impl ConnectionGuard {
    fn acquire(counter: &Arc<std::sync::atomic::AtomicUsize>) -> Self {
        counter.fetch_add(1, Ordering::AcqRel);
        Self {
            counter: Arc::clone(counter),
        }
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

struct StreamTable {
    closed: bool,
    streams: HashMap<u64, oneshot::Sender<()>>,
}

struct SnapshotEntry {
    snapshot: Arc<OwnedSnapshot>,
    last_used: Instant,
}

struct SnapshotTable {
    reserved: usize,
    entries: HashMap<u64, SnapshotEntry>,
}

impl SnapshotTable {
    fn reap_expired(&mut self, ttl: Duration) {
        if ttl.is_zero() {
            return;
        }
        let now = Instant::now();
        self.entries
            .retain(|_id, entry| now.saturating_duration_since(entry.last_used) < ttl);
    }
}

struct SnapshotReservationGuard {
    table: Arc<Mutex<SnapshotTable>>,
    committed: bool,
}

impl SnapshotReservationGuard {
    fn commit(mut self, id: u64, snapshot: Arc<OwnedSnapshot>) {
        let mut table = self.table.lock();
        table.reserved = table.reserved.saturating_sub(1);
        table.entries.insert(
            id,
            SnapshotEntry {
                snapshot,
                last_used: Instant::now(),
            },
        );
        self.committed = true;
    }
}

impl Drop for SnapshotReservationGuard {
    fn drop(&mut self) {
        if !self.committed {
            let mut table = self.table.lock();
            table.reserved = table.reserved.saturating_sub(1);
        }
    }
}

struct ConnectionState {
    ctx: ServerContext,
    credit_window: Arc<Semaphore>,
    snapshots: Arc<Mutex<SnapshotTable>>,
    next_snapshot_id: AtomicU64,
    active_streams: Arc<Mutex<StreamTable>>,
    outbound_tx: mpsc::Sender<Frame>,
}

impl ConnectionState {
    fn close_and_cancel_all_streams(&self) {
        let mut table = self.active_streams.lock();
        table.closed = true;
        for (_id, cancel_tx) in table.streams.drain() {
            let _ = cancel_tx.send(());
        }
    }
}

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

    let conn = Arc::new(ConnectionState {
        credit_window: Arc::new(Semaphore::new(ctx.config.max_in_flight_per_conn.max(1))),
        snapshots: Arc::new(Mutex::new(SnapshotTable {
            reserved: 0,
            entries: HashMap::new(),
        })),
        next_snapshot_id: AtomicU64::new(1),
        active_streams: Arc::new(Mutex::new(StreamTable {
            closed: false,
            streams: HashMap::new(),
        })),
        outbound_tx,
        ctx,
    });

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

        match frame.kind {
            FrameKind::Ping => {
                let pong = Frame::new(FrameKind::Pong, frame.stream_id, frame.payload);
                if !send_or_shutdown(&conn.outbound_tx, pong, &mut shutdown_rx).await {
                    break;
                }
            }
            FrameKind::Cancel => {
                let cancel = conn.active_streams.lock().streams.remove(&frame.stream_id);
                if let Some(tx) = cancel {
                    let _ = tx.send(());
                }
            }
            FrameKind::Request => {
                let permit = match Arc::clone(&conn.credit_window).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        let busy = Response::Error(MembusError::Busy {
                            retry_after_ms: 5,
                            reason: "per-connection credit window exhausted".to_owned(),
                        });
                        let reply = build_response_frame(frame.stream_id, &busy, max_frame_body);
                        if !send_or_shutdown(&conn.outbound_tx, reply, &mut shutdown_rx).await {
                            break;
                        }
                        continue;
                    }
                };

                let conn_ref = Arc::clone(&conn);
                let task_shutdown = shutdown_rx.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    handle_request_frame(conn_ref, frame.stream_id, frame.payload, task_shutdown)
                        .await;
                });
            }
            _ => {
                let err = Response::Error(MembusError::Protocol(format!(
                    "unexpected client frame kind {:?}",
                    frame.kind
                )));
                let reply = build_response_frame(frame.stream_id, &err, max_frame_body);
                let _ = send_or_shutdown(&conn.outbound_tx, reply, &mut shutdown_rx).await;
                break;
            }
        }
    }

    conn.close_and_cancel_all_streams();
    drop(conn);
    let _ = writer_task.await;
}

async fn send_or_shutdown(
    tx: &mpsc::Sender<Frame>,
    frame: Frame,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> bool {
    if *shutdown_rx.borrow() {
        return false;
    }
    tokio::select! {
        biased;
        _ = shutdown_rx.changed() => false,
        res = tx.send(frame) => res.is_ok(),
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

    match request {
        Request::SubscribeEachChange {
            namespace,
            key_prefix,
            epoch,
            since_seq,
        } => {
            start_each_change_stream(
                conn,
                stream_id,
                namespace,
                key_prefix,
                epoch,
                since_seq,
                shutdown_rx,
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
                conn,
                stream_id,
                namespace,
                key_prefix,
                max_pending,
                shutdown_rx,
            )
            .await;
            return;
        }
        _ => {}
    }

    let response = dispatch_unary(&conn, request).await;
    let reply = build_response_frame(stream_id, &response, max_frame_body);
    let _ = send_or_shutdown(&conn.outbound_tx, reply, &mut shutdown_rx).await;
}

async fn dispatch_unary(conn: &Arc<ConnectionState>, req: Request) -> Response {
    let storage = Arc::clone(&conn.ctx.storage);
    match req {
        Request::Get {
            namespace,
            key,
            snapshot_id,
        } => {
            let snap = match resolve_snapshot(conn, snapshot_id) {
                Ok(s) => s,
                Err(e) => return Response::Error(e),
            };
            let result = tokio::task::spawn_blocking(move || match snap {
                Some(s) => storage.get_in_snapshot(&s, &namespace, &key),
                None => storage.get(&namespace, &key),
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("get task failed: {e}"))));
            match result {
                Ok(rec) => Response::Get(rec),
                Err(e) => Response::Error(e),
            }
        }
        Request::GetMany {
            namespace,
            keys,
            snapshot_id,
        } => {
            let snap = match resolve_snapshot(conn, snapshot_id) {
                Ok(s) => s,
                Err(e) => return Response::Error(e),
            };
            let result = tokio::task::spawn_blocking(move || match snap {
                Some(s) => storage.get_many_in_snapshot(&s, &namespace, &keys),
                None => storage.get_many(&namespace, &keys),
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("get_many task failed: {e}"))));
            match result {
                Ok(recs) => Response::GetMany(recs),
                Err(e) => Response::Error(e),
            }
        }
        Request::Put {
            namespace,
            key,
            value,
            ack,
            expected,
        } => {
            let result = tokio::task::spawn_blocking(move || {
                storage.put(&namespace, &key, &value, ack, expected)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("put task failed: {e}"))));
            match result {
                Ok(commit) => Response::Commit(commit),
                Err(e) => Response::Error(e),
            }
        }
        Request::Remove {
            namespace,
            key,
            ack,
            expected,
        } => {
            let result = tokio::task::spawn_blocking(move || {
                storage.remove(&namespace, &key, ack, expected)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("remove task failed: {e}"))));
            match result {
                Ok(commit) => Response::Commit(commit),
                Err(e) => Response::Error(e),
            }
        }
        Request::CommitTx {
            ack,
            guards,
            mutations,
        } => {
            let result =
                tokio::task::spawn_blocking(move || storage.commit_tx(&guards, &mutations, ack))
                    .await
                    .unwrap_or_else(|e| {
                        Err(MembusError::Storage(format!("commit_tx task failed: {e}")))
                    });
            match result {
                Ok(commit) => Response::Commit(commit),
                Err(e) => Response::Error(e),
            }
        }
        Request::OpenSnapshot => {
            let reservation = {
                let mut table = conn.snapshots.lock();
                table.reap_expired(conn.ctx.config.snapshot_ttl);
                if table.entries.len().saturating_add(table.reserved)
                    >= conn.ctx.config.max_snapshots_per_conn
                {
                    return Response::Error(MembusError::Busy {
                        retry_after_ms: 10,
                        reason: "per-connection snapshot limit reached".to_owned(),
                    });
                }
                table.reserved += 1;
                SnapshotReservationGuard {
                    table: Arc::clone(&conn.snapshots),
                    committed: false,
                }
            };

            let result = tokio::task::spawn_blocking(move || storage.open_snapshot())
                .await
                .unwrap_or_else(|e| {
                    Err(MembusError::Storage(format!(
                        "open_snapshot task failed: {e}"
                    )))
                });
            match result {
                Ok(snapshot) => {
                    let snapshot_seq = snapshot.snapshot_seq();
                    let snapshot_id = conn.next_snapshot_id.fetch_add(1, Ordering::AcqRel);
                    reservation.commit(snapshot_id, Arc::new(snapshot));
                    Response::SnapshotOpened {
                        snapshot_id,
                        snapshot_seq,
                    }
                }
                Err(e) => Response::Error(e),
            }
        }
        Request::CloseSnapshot { snapshot_id } => {
            if conn.snapshots.lock().entries.remove(&snapshot_id).is_some() {
                Response::Unit
            } else {
                Response::Error(MembusError::NotFound(format!(
                    "unknown snapshot_id {snapshot_id}"
                )))
            }
        }
        Request::ScanPrefix {
            namespace,
            prefix,
            options,
            snapshot_id,
        } => {
            let pinned_snap = match resolve_snapshot(conn, snapshot_id) {
                Ok(s) => s,
                Err(e) => return Response::Error(e),
            };
            let result = tokio::task::spawn_blocking(move || {
                let snap = match pinned_snap {
                    Some(s) => s,
                    None => Arc::new(storage.open_snapshot()?),
                };
                storage.scan_prefix(&snap, &namespace, &prefix, &options)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("scan_prefix failed: {e}"))));
            match result {
                Ok(page) => Response::Scan(page),
                Err(e) => Response::Error(e),
            }
        }
        Request::ScanRange {
            namespace,
            start_key,
            end_key,
            options,
            snapshot_id,
        } => {
            let pinned_snap = match resolve_snapshot(conn, snapshot_id) {
                Ok(s) => s,
                Err(e) => return Response::Error(e),
            };
            let result = tokio::task::spawn_blocking(move || {
                let snap = match pinned_snap {
                    Some(s) => s,
                    None => Arc::new(storage.open_snapshot()?),
                };
                storage.scan_range(&snap, &namespace, &start_key, &end_key, &options)
            })
            .await
            .unwrap_or_else(|e| Err(MembusError::Storage(format!("scan_range failed: {e}"))));
            match result {
                Ok(page) => Response::Scan(page),
                Err(e) => Response::Error(e),
            }
        }
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
        Request::Health => Response::Health(HealthStatus {
            epoch: storage.epoch(),
            latest_seq: storage.latest_seq(),
            log_horizon_seq: storage.log_horizon_seq(),
            in_flight_ops: storage.in_flight_ops(),
            in_flight_bytes: storage.in_flight_bytes(),
            active_connections: conn.ctx.active_connections(),
            active_subscriptions: conn.ctx.broker.active_subscriptions(),
        }),
        Request::PutBlob { digest, data, ack } => {
            let result = tokio::task::spawn_blocking(move || storage.put_blob(&digest, &data, ack))
                .await
                .unwrap_or_else(|e| Err(MembusError::Storage(format!("put_blob failed: {e}"))));
            match result {
                Ok(()) => Response::Unit,
                Err(e) => Response::Error(e),
            }
        }
        Request::GetBlob { digest } => {
            let result = tokio::task::spawn_blocking(move || storage.get_blob(&digest))
                .await
                .unwrap_or_else(|e| Err(MembusError::Storage(format!("get_blob failed: {e}"))));
            match result {
                Ok(data) => Response::Blob(data),
                Err(e) => Response::Error(e),
            }
        }
        Request::SubscribeEachChange { .. } | Request::SubscribeLive { .. } => {
            Response::Error(MembusError::Protocol(
                "subscription request misrouted to unary dispatch".to_owned(),
            ))
        }
    }
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

async fn start_each_change_stream(
    conn: Arc<ConnectionState>,
    stream_id: u64,
    namespace: String,
    key_prefix: Vec<u8>,
    epoch: u64,
    since_seq: u64,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    match conn
        .ctx
        .broker
        .subscribe_each_change(&namespace, key_prefix, epoch, since_seq)
    {
        Ok(sub) => {
            let epoch = sub.epoch();
            let start_seq = sub.cursor_seq();
            launch_subscription_stream(
                conn,
                stream_id,
                epoch,
                start_seq,
                ActiveSubscription::EachChange(sub),
                shutdown_rx,
            )
            .await;
        }
        Err(err) => {
            let reply = build_response_frame(
                stream_id,
                &Response::Error(err),
                conn.ctx.config.max_frame_body,
            );
            let _ = send_or_shutdown(&conn.outbound_tx, reply, &mut shutdown_rx).await;
        }
    }
}

async fn start_live_stream(
    conn: Arc<ConnectionState>,
    stream_id: u64,
    namespace: String,
    key_prefix: Vec<u8>,
    max_pending: Option<u32>,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    match conn.ctx.broker.subscribe_live(
        &namespace,
        key_prefix,
        max_pending.map(|v| v as usize),
    ) {
        Ok(sub) => {
            let epoch = sub.epoch();
            let start_seq = conn.ctx.storage.latest_seq();
            launch_subscription_stream(
                conn,
                stream_id,
                epoch,
                start_seq,
                ActiveSubscription::Live(sub),
                shutdown_rx,
            )
            .await;
        }
        Err(err) => {
            let reply = build_response_frame(
                stream_id,
                &Response::Error(err),
                conn.ctx.config.max_frame_body,
            );
            let _ = send_or_shutdown(&conn.outbound_tx, reply, &mut shutdown_rx).await;
        }
    }
}

async fn launch_subscription_stream(
    conn: Arc<ConnectionState>,
    stream_id: u64,
    epoch: u64,
    start_seq: u64,
    mut sub: ActiveSubscription,
    mut shutdown_rx: watch::Receiver<bool>,
) {
    let max_frame_body = conn.ctx.config.max_frame_body;
    let (cancel_tx, mut cancel_rx) = oneshot::channel::<()>();
    let register_result = {
        let mut table = conn.active_streams.lock();
        if table.closed {
            return;
        }
        if table.streams.contains_key(&stream_id) {
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
        let _ = send_or_shutdown(&conn.outbound_tx, reply, &mut shutdown_rx).await;
        return;
    }

    let ack = Response::Subscribed { epoch, start_seq };
    let ack_frame = build_response_frame(stream_id, &ack, max_frame_body);
    if !send_or_shutdown(&conn.outbound_tx, ack_frame, &mut shutdown_rx).await {
        conn.active_streams.lock().streams.remove(&stream_id);
        return;
    }

    let outbound_tx = conn.outbound_tx.clone();
    let active_streams = Arc::clone(&conn.active_streams);
    tokio::spawn(async move {
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

fn resolve_snapshot(
    conn: &ConnectionState,
    snapshot_id: Option<u64>,
) -> Result<Option<Arc<OwnedSnapshot>>, MembusError> {
    match snapshot_id {
        None => Ok(None),
        Some(id) => {
            let mut table = conn.snapshots.lock();
            table.reap_expired(conn.ctx.config.snapshot_ttl);
            let entry = table
                .entries
                .get_mut(&id)
                .ok_or_else(|| MembusError::NotFound(format!("unknown or expired snapshot_id {id}")))?;
            entry.last_used = Instant::now();
            Ok(Some(Arc::clone(&entry.snapshot)))
        }
    }
}

/// Restrict remote checkpoint destinations strictly inside a configured `checkpoint_root`.
pub(crate) fn resolve_checkpoint_path(
    root: Option<&Path>,
    raw_dest: &str,
) -> Result<PathBuf, MembusError> {
    let root = root.ok_or_else(|| {
        MembusError::InvalidArgument(
            "checkpoint RPC is disabled unless server checkpoint_root is configured".to_owned(),
        )
    })?;
    if raw_dest.is_empty() {
        return Err(MembusError::InvalidArgument(
            "checkpoint destination must not be empty".to_owned(),
        ));
    }
    let rel = Path::new(raw_dest);
    if rel.is_absolute() {
        return Err(MembusError::InvalidArgument(
            "checkpoint destination must be a relative path under checkpoint_root".to_owned(),
        ));
    }

    let mut clean = PathBuf::new();
    for component in rel.components() {
        match component {
            Component::Normal(part) => clean.push(part),
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => {
                return Err(MembusError::InvalidArgument(format!(
                    "checkpoint destination '{raw_dest}' escapes checkpoint_root"
                )));
            }
        }
    }
    if clean.as_os_str().is_empty() {
        return Err(MembusError::InvalidArgument(
            "checkpoint destination must name a subdirectory under checkpoint_root".to_owned(),
        ));
    }
    Ok(root.join(clean))
}

fn build_response_frame(stream_id: u64, resp: &Response, max_frame_body: usize) -> Frame {
    let encoded = encode_response(resp);
    if encoded.len() > max_frame_body {
        let too_large = Response::Error(MembusError::FrameTooLarge {
            len: encoded.len(),
            max_len: max_frame_body,
        });
        return Frame::with_flags(
            FrameKind::Response,
            FrameFlags::ERROR,
            stream_id,
            encode_response(&too_large),
        );
    }
    let flags = if matches!(resp, Response::Error(_)) {
        FrameFlags::ERROR
    } else {
        FrameFlags::NONE
    };
    Frame::with_flags(FrameKind::Response, flags, stream_id, encoded)
}
