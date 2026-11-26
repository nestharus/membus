use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch};

use crate::protocol::{
    decode_response, decode_signal_event, decode_stream_end, encode_request, read_frame,
    run_batched_frame_writer, Frame, FrameKind, Request, Response, DEFAULT_MAX_FRAME_BODY,
};
use crate::transport::{connect_endpoint, Endpoint};
use crate::types::{
    Ack, CommitResult, ExpectedVersion, HealthStatus, MembusError, ScanOptions, ScanPage,
    SignalEvent, StoredRecord, VersionGuard, WriteMutation,
};

const CLIENT_OUTBOUND_QUEUE: usize = 8192;
const SUBSCRIPTION_CHANNEL_CAPACITY: usize = 1024;

struct StreamDispatchEntry {
    tx: mpsc::Sender<Result<SignalEvent, MembusError>>,
    terminal_error: Arc<Mutex<Option<MembusError>>>,
}

struct ClientState {
    closed: bool,
    pending_rpcs: HashMap<u64, oneshot::Sender<Result<Response, MembusError>>>,
    pending_pings: HashMap<u64, oneshot::Sender<Result<(), MembusError>>>,
    active_streams: HashMap<u64, StreamDispatchEntry>,
}

impl ClientState {
    fn new() -> Self {
        Self {
            closed: false,
            pending_rpcs: HashMap::new(),
            pending_pings: HashMap::new(),
            active_streams: HashMap::new(),
        }
    }

    fn fail_all(&mut self) {
        self.closed = true;
        for (_id, tx) in self.pending_rpcs.drain() {
            let _ = tx.send(Err(MembusError::Closed));
        }
        for (_id, tx) in self.pending_pings.drain() {
            let _ = tx.send(Err(MembusError::Closed));
        }
        for (_id, entry) in self.active_streams.drain() {
            let mut slot = entry.terminal_error.lock();
            if slot.is_none() {
                *slot = Some(MembusError::Closed);
            }
        }
    }
}

struct ClientInner {
    outbound_tx: mpsc::Sender<Frame>,
    next_stream_id: AtomicU64,
    max_frame_body: usize,
    subscription_capacity: usize,
    state: Arc<Mutex<ClientState>>,
}

/// Pipelined, multiplexed async `membus/2` client.
///
/// Cheaply cloneable: hundreds or thousands of concurrent async tasks can share one
/// underlying TCP or Windows Named Pipe connection with full RPC pipelining and live
/// subscription push streams multiplexed by `stream_id`.
#[derive(Clone)]
pub struct Client {
    inner: Arc<ClientInner>,
}

impl Client {
    pub async fn connect(endpoint: &Endpoint) -> Result<Self, MembusError> {
        let stream = connect_endpoint(endpoint).await?;
        Ok(Self::from_stream(stream, DEFAULT_MAX_FRAME_BODY))
    }

    pub async fn connect_tcp(addr: SocketAddr) -> Result<Self, MembusError> {
        Self::connect(&Endpoint::Tcp(addr)).await
    }

    pub async fn connect_named_pipe(pipe_name: &str) -> Result<Self, MembusError> {
        Self::connect(&Endpoint::NamedPipe(pipe_name.to_owned())).await
    }

    pub fn from_stream<S>(stream: S, max_frame_body: usize) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        Self::from_stream_with_capacity(stream, max_frame_body, SUBSCRIPTION_CHANNEL_CAPACITY)
    }

    pub fn from_stream_with_capacity<S>(
        stream: S,
        max_frame_body: usize,
        subscription_capacity: usize,
    ) -> Self
    where
        S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
    {
        let (mut reader, mut writer) = tokio::io::split(stream);
        let (outbound_tx, mut outbound_rx) = mpsc::channel::<Frame>(CLIENT_OUTBOUND_QUEUE);

        let state = Arc::new(Mutex::new(ClientState::new()));
        let (shutdown_tx, mut writer_shutdown_rx) = watch::channel(false);
        let mut reader_shutdown_rx = shutdown_tx.subscribe();

        let writer_state = Arc::clone(&state);
        let writer_shutdown_tx = shutdown_tx.clone();
        tokio::spawn(async move {
            tokio::select! {
                biased;
                _ = wait_shutdown(&mut writer_shutdown_rx) => {}
                _ = run_batched_frame_writer(&mut writer, &mut outbound_rx, max_frame_body) => {}
            }
            writer_state.lock().fail_all();
            let _ = writer_shutdown_tx.send(true);
        });

        let reader_state = Arc::clone(&state);
        let reader_shutdown_tx = shutdown_tx;
        // Downgrade so the reader task never keeps `outbound_rx` alive once all
        // `Client` and `ClientSubscription` handles have been dropped.
        let reader_outbound = outbound_tx.downgrade();
        tokio::spawn(async move {
            let mut read_buf = BytesMut::with_capacity(64 * 1024);
            loop {
                let frame_opt = tokio::select! {
                    biased;
                    _ = wait_shutdown(&mut reader_shutdown_rx) => None,
                    res = read_frame(&mut reader, &mut read_buf, max_frame_body) => {
                        res.ok().flatten()
                    }
                };
                let Some(frame) = frame_opt else {
                    break;
                };

                match frame.kind {
                    FrameKind::Response => {
                        let decoded = decode_response(&frame.payload);
                        let rpc_tx = reader_state.lock().pending_rpcs.remove(&frame.stream_id);
                        if let Some(tx) = rpc_tx {
                            let _ = tx.send(decoded);
                        }
                    }
                    FrameKind::Pong => {
                        let ping_tx = reader_state.lock().pending_pings.remove(&frame.stream_id);
                        if let Some(tx) = ping_tx {
                            let _ = tx.send(Ok(()));
                        }
                    }
                    FrameKind::StreamEvent => {
                        let decoded = decode_signal_event(&frame.payload);
                        // Non-blocking try_send ensures a slow subscription can NEVER
                        // stall unrelated RPCs or other subscriptions on the shared connection.
                        let lagged_stream = {
                            let mut guard = reader_state.lock();
                            if let Some(entry) = guard.active_streams.get(&frame.stream_id) {
                                match entry.tx.try_send(decoded) {
                                    Ok(()) => false,
                                    Err(TrySendError::Full(_)) => {
                                        if let Some(removed) =
                                            guard.active_streams.remove(&frame.stream_id)
                                        {
                                            *removed.terminal_error.lock() =
                                                Some(MembusError::Busy {
                                                    retry_after_ms: 0,
                                                    reason: format!(
                                                        "client subscription stream {} buffer overflowed",
                                                        frame.stream_id
                                                    ),
                                                });
                                        }
                                        true
                                    }
                                    Err(TrySendError::Closed(_)) => {
                                        guard.active_streams.remove(&frame.stream_id);
                                        true
                                    }
                                }
                            } else {
                                false
                            }
                        };
                        if lagged_stream {
                            if let Some(tx) = reader_outbound.upgrade() {
                                let _ = tx.try_send(Frame::new(
                                    FrameKind::Cancel,
                                    frame.stream_id,
                                    Bytes::new(),
                                ));
                            }
                        }
                    }
                    FrameKind::StreamEnd => {
                        let end_err = decode_stream_end(&frame.payload);
                        let removed = reader_state.lock().active_streams.remove(&frame.stream_id);
                        if let Some(entry) = removed {
                            let err_opt = match end_err {
                                Ok(Some(err)) => Some(err),
                                Err(proto_err) => Some(proto_err),
                                Ok(None) => None,
                            };
                            if let Some(err) = err_opt {
                                if entry.tx.try_send(Err(err.clone())).is_err() {
                                    *entry.terminal_error.lock() = Some(err);
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
            reader_state.lock().fail_all();
            let _ = reader_shutdown_tx.send(true);
        });

        Self {
            inner: Arc::new(ClientInner {
                outbound_tx,
                next_stream_id: AtomicU64::new(1),
                max_frame_body,
                subscription_capacity: subscription_capacity.max(1),
                state,
            }),
        }
    }

    pub fn is_closed(&self) -> bool {
        self.inner.state.lock().closed
    }

    pub fn pending_rpc_count(&self) -> usize {
        self.inner.state.lock().pending_rpcs.len()
    }

    pub async fn ping(&self) -> Result<(), MembusError> {
        let stream_id = self.alloc_stream_id();
        let (tx, rx) = oneshot::channel();
        {
            let mut state = self.inner.state.lock();
            if state.closed {
                return Err(MembusError::Closed);
            }
            state.pending_pings.insert(stream_id, tx);
        }
        let mut guard = PendingPingGuard {
            state: Arc::clone(&self.inner.state),
            stream_id,
            active: true,
        };

        let frame = Frame::new(FrameKind::Ping, stream_id, Bytes::new());
        if self.inner.outbound_tx.send(frame).await.is_err() {
            return Err(MembusError::Closed);
        }
        let res = rx.await.map_err(|_| MembusError::Closed)?;
        guard.active = false;
        res
    }

    pub async fn get(
        &self,
        namespace: &str,
        key: &[u8],
    ) -> Result<Option<StoredRecord>, MembusError> {
        self.get_with_snapshot(namespace, key, None).await
    }

    pub async fn get_many(
        &self,
        namespace: &str,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<StoredRecord>>, MembusError> {
        self.get_many_with_snapshot(namespace, keys, None).await
    }

    pub async fn put(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        self.cas(namespace, key, value, ack, ExpectedVersion::Any)
            .await
    }

    pub async fn cas(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
    ) -> Result<CommitResult, MembusError> {
        let resp = self
            .call(Request::Put {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
                value: value.to_vec(),
                ack,
                expected,
            })
            .await?;
        match resp {
            Response::Commit(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for put/cas: {other:?}"
            ))),
        }
    }

    pub async fn remove(
        &self,
        namespace: &str,
        key: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
    ) -> Result<CommitResult, MembusError> {
        let resp = self
            .call(Request::Remove {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
                ack,
                expected,
            })
            .await?;
        match resp {
            Response::Commit(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for remove: {other:?}"
            ))),
        }
    }

    pub async fn commit_tx(
        &self,
        guards: Vec<VersionGuard>,
        mutations: Vec<WriteMutation>,
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        let resp = self
            .call(Request::CommitTx {
                ack,
                guards,
                mutations,
            })
            .await?;
        match resp {
            Response::Commit(c) => Ok(c),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for commit_tx: {other:?}"
            ))),
        }
    }

    pub async fn open_snapshot(&self) -> Result<ClientSnapshot, MembusError> {
        let resp = self.call(Request::OpenSnapshot).await?;
        match resp {
            Response::SnapshotOpened {
                snapshot_id,
                snapshot_seq,
            } => Ok(ClientSnapshot {
                client: self.clone(),
                snapshot_id,
                snapshot_seq,
                closed: false,
            }),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for open_snapshot: {other:?}"
            ))),
        }
    }

    pub async fn scan_prefix(
        &self,
        namespace: &str,
        prefix: &[u8],
        options: ScanOptions,
        snapshot_id: Option<u64>,
    ) -> Result<ScanPage, MembusError> {
        let resp = self
            .call(Request::ScanPrefix {
                namespace: namespace.to_owned(),
                prefix: prefix.to_vec(),
                options,
                snapshot_id,
            })
            .await?;
        match resp {
            Response::Scan(page) => Ok(page),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for scan_prefix: {other:?}"
            ))),
        }
    }

    pub async fn scan_range(
        &self,
        namespace: &str,
        start_key: &[u8],
        end_key: &[u8],
        options: ScanOptions,
        snapshot_id: Option<u64>,
    ) -> Result<ScanPage, MembusError> {
        let resp = self
            .call(Request::ScanRange {
                namespace: namespace.to_owned(),
                start_key: start_key.to_vec(),
                end_key: end_key.to_vec(),
                options,
                snapshot_id,
            })
            .await?;
        match resp {
            Response::Scan(page) => Ok(page),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for scan_range: {other:?}"
            ))),
        }
    }

    pub async fn put_blob(&self, digest: &[u8], data: &[u8], ack: Ack) -> Result<(), MembusError> {
        let resp = self
            .call(Request::PutBlob {
                digest: digest.to_vec(),
                data: data.to_vec(),
                ack,
            })
            .await?;
        match resp {
            Response::Unit => Ok(()),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for put_blob: {other:?}"
            ))),
        }
    }

    pub async fn get_blob(&self, digest: &[u8]) -> Result<Option<Vec<u8>>, MembusError> {
        let resp = self
            .call(Request::GetBlob {
                digest: digest.to_vec(),
            })
            .await?;
        match resp {
            Response::Blob(b) => Ok(b),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for get_blob: {other:?}"
            ))),
        }
    }

    pub async fn subscribe_each_change(
        &self,
        namespace: &str,
        key_prefix: &[u8],
        epoch: u64,
        since_seq: u64,
    ) -> Result<ClientSubscription, MembusError> {
        self.open_subscription(Request::SubscribeEachChange {
            namespace: namespace.to_owned(),
            key_prefix: key_prefix.to_vec(),
            epoch,
            since_seq,
        })
        .await
    }

    pub async fn subscribe_live(
        &self,
        namespace: &str,
        key_prefix: &[u8],
        max_pending: Option<u32>,
    ) -> Result<ClientSubscription, MembusError> {
        self.open_subscription(Request::SubscribeLive {
            namespace: namespace.to_owned(),
            key_prefix: key_prefix.to_vec(),
            max_pending,
        })
        .await
    }

    pub async fn checkpoint(&self, dest_dir: &str) -> Result<u64, MembusError> {
        let resp = self
            .call(Request::Checkpoint {
                dest_dir: dest_dir.to_owned(),
            })
            .await?;
        match resp {
            Response::Checkpoint { seq } => Ok(seq),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for checkpoint: {other:?}"
            ))),
        }
    }

    pub async fn health(&self) -> Result<HealthStatus, MembusError> {
        let resp = self.call(Request::Health).await?;
        match resp {
            Response::Health(status) => Ok(status),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for health: {other:?}"
            ))),
        }
    }

    async fn get_with_snapshot(
        &self,
        namespace: &str,
        key: &[u8],
        snapshot_id: Option<u64>,
    ) -> Result<Option<StoredRecord>, MembusError> {
        let resp = self
            .call(Request::Get {
                namespace: namespace.to_owned(),
                key: key.to_vec(),
                snapshot_id,
            })
            .await?;
        match resp {
            Response::Get(rec) => Ok(rec),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for get: {other:?}"
            ))),
        }
    }

    async fn get_many_with_snapshot(
        &self,
        namespace: &str,
        keys: Vec<Vec<u8>>,
        snapshot_id: Option<u64>,
    ) -> Result<Vec<Option<StoredRecord>>, MembusError> {
        let resp = self
            .call(Request::GetMany {
                namespace: namespace.to_owned(),
                keys,
                snapshot_id,
            })
            .await?;
        match resp {
            Response::GetMany(recs) => Ok(recs),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for get_many: {other:?}"
            ))),
        }
    }

    async fn close_snapshot_id(&self, snapshot_id: u64) -> Result<(), MembusError> {
        let resp = self.call(Request::CloseSnapshot { snapshot_id }).await?;
        match resp {
            Response::Unit => Ok(()),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for close_snapshot: {other:?}"
            ))),
        }
    }

    async fn open_subscription(&self, req: Request) -> Result<ClientSubscription, MembusError> {
        let encoded = encode_request(&req)?;
        self.check_frame_size(&encoded)?;

        let stream_id = self.alloc_stream_id();
        let (rpc_tx, rpc_rx) = oneshot::channel();
        let (ev_tx, ev_rx) = mpsc::channel(self.inner.subscription_capacity);
        let terminal_error = Arc::new(Mutex::new(None));

        {
            let mut state = self.inner.state.lock();
            if state.closed {
                return Err(MembusError::Closed);
            }
            // Register stream before sending request so immediate push frames arriving
            // right after `Response::Subscribed` are never missed.
            state.active_streams.insert(
                stream_id,
                StreamDispatchEntry {
                    tx: ev_tx,
                    terminal_error: Arc::clone(&terminal_error),
                },
            );
            state.pending_rpcs.insert(stream_id, rpc_tx);
        }

        let mut handshake_guard = PendingSubGuard {
            state: Arc::clone(&self.inner.state),
            outbound_tx: self.inner.outbound_tx.clone(),
            stream_id,
            active: true,
        };

        let frame = Frame::new(FrameKind::Request, stream_id, encoded);
        if self.inner.outbound_tx.send(frame).await.is_err() {
            return Err(MembusError::Closed);
        }

        let resp = match rpc_rx.await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => return Err(e),
            Err(_) => return Err(MembusError::Closed),
        };

        match resp {
            Response::Subscribed { epoch, start_seq } => {
                handshake_guard.active = false;
                Ok(ClientSubscription {
                    stream_id,
                    epoch,
                    start_seq,
                    rx: ev_rx,
                    terminal_error,
                    outbound_tx: self.inner.outbound_tx.clone(),
                    state: Arc::clone(&self.inner.state),
                    cancelled: false,
                })
            }
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for subscribe: {other:?}"
            ))),
        }
    }

    async fn call(&self, req: Request) -> Result<Response, MembusError> {
        let encoded = encode_request(&req)?;
        self.check_frame_size(&encoded)?;

        let stream_id = self.alloc_stream_id();
        let (tx, rx) = oneshot::channel();
        {
            let mut state = self.inner.state.lock();
            if state.closed {
                return Err(MembusError::Closed);
            }
            state.pending_rpcs.insert(stream_id, tx);
        }
        let mut guard = PendingRpcGuard {
            state: Arc::clone(&self.inner.state),
            stream_id,
            active: true,
        };

        let frame = Frame::new(FrameKind::Request, stream_id, encoded);
        if self.inner.outbound_tx.send(frame).await.is_err() {
            return Err(MembusError::Closed);
        }

        let res = rx.await.map_err(|_| MembusError::Closed)?;
        guard.active = false;
        res
    }

    fn check_frame_size(&self, payload: &Bytes) -> Result<(), MembusError> {
        if payload.len() > self.inner.max_frame_body {
            return Err(MembusError::FrameTooLarge {
                len: payload.len(),
                max_len: self.inner.max_frame_body,
            });
        }
        Ok(())
    }

    fn alloc_stream_id(&self) -> u64 {
        self.inner.next_stream_id.fetch_add(1, Ordering::Relaxed)
    }
}

struct PendingRpcGuard {
    state: Arc<Mutex<ClientState>>,
    stream_id: u64,
    active: bool,
}

impl Drop for PendingRpcGuard {
    fn drop(&mut self) {
        if self.active {
            self.state.lock().pending_rpcs.remove(&self.stream_id);
        }
    }
}

struct PendingPingGuard {
    state: Arc<Mutex<ClientState>>,
    stream_id: u64,
    active: bool,
}

impl Drop for PendingPingGuard {
    fn drop(&mut self) {
        if self.active {
            self.state.lock().pending_pings.remove(&self.stream_id);
        }
    }
}

struct PendingSubGuard {
    state: Arc<Mutex<ClientState>>,
    outbound_tx: mpsc::Sender<Frame>,
    stream_id: u64,
    active: bool,
}

impl Drop for PendingSubGuard {
    fn drop(&mut self) {
        if self.active {
            {
                let mut state = self.state.lock();
                state.pending_rpcs.remove(&self.stream_id);
                state.active_streams.remove(&self.stream_id);
            }
            enqueue_cleanup_frame(
                &self.outbound_tx,
                Frame::new(FrameKind::Cancel, self.stream_id, Bytes::new()),
            );
        }
    }
}

/// Server-pinned MVCC snapshot handle bound to a [`Client`] connection.
pub struct ClientSnapshot {
    client: Client,
    snapshot_id: u64,
    snapshot_seq: u64,
    closed: bool,
}

impl ClientSnapshot {
    pub fn snapshot_id(&self) -> u64 {
        self.snapshot_id
    }

    pub fn snapshot_seq(&self) -> u64 {
        self.snapshot_seq
    }

    pub async fn get(
        &self,
        namespace: &str,
        key: &[u8],
    ) -> Result<Option<StoredRecord>, MembusError> {
        self.client
            .get_with_snapshot(namespace, key, Some(self.snapshot_id))
            .await
    }

    pub async fn get_many(
        &self,
        namespace: &str,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<StoredRecord>>, MembusError> {
        self.client
            .get_many_with_snapshot(namespace, keys, Some(self.snapshot_id))
            .await
    }

    pub async fn scan_prefix(
        &self,
        namespace: &str,
        prefix: &[u8],
        options: ScanOptions,
    ) -> Result<ScanPage, MembusError> {
        self.client
            .scan_prefix(namespace, prefix, options, Some(self.snapshot_id))
            .await
    }

    pub async fn scan_range(
        &self,
        namespace: &str,
        start_key: &[u8],
        end_key: &[u8],
        options: ScanOptions,
    ) -> Result<ScanPage, MembusError> {
        self.client
            .scan_range(namespace, start_key, end_key, options, Some(self.snapshot_id))
            .await
    }

    pub async fn close(mut self) -> Result<(), MembusError> {
        self.closed = true;
        self.client.close_snapshot_id(self.snapshot_id).await
    }
}

impl Drop for ClientSnapshot {
    fn drop(&mut self) {
        if !self.closed && !self.client.is_closed() {
            let stream_id = self.client.alloc_stream_id();
            if let Ok(payload) = encode_request(&Request::CloseSnapshot {
                snapshot_id: self.snapshot_id,
            }) {
                let frame = Frame::new(FrameKind::Request, stream_id, payload);
                enqueue_cleanup_frame(&self.client.inner.outbound_tx, frame);
            }
        }
    }
}

/// Multiplexed client handle to an active server push subscription stream.
pub struct ClientSubscription {
    stream_id: u64,
    epoch: u64,
    start_seq: u64,
    rx: mpsc::Receiver<Result<SignalEvent, MembusError>>,
    terminal_error: Arc<Mutex<Option<MembusError>>>,
    outbound_tx: mpsc::Sender<Frame>,
    state: Arc<Mutex<ClientState>>,
    cancelled: bool,
}

impl ClientSubscription {
    pub fn stream_id(&self) -> u64 {
        self.stream_id
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn start_seq(&self) -> u64 {
        self.start_seq
    }

    pub async fn next_event(&mut self) -> Result<Option<SignalEvent>, MembusError> {
        if let Some(err) = self.terminal_error.lock().take() {
            return Err(err);
        }
        match self.rx.recv().await {
            Some(Ok(ev)) => Ok(Some(ev)),
            Some(Err(err)) => Err(err),
            None => {
                if let Some(err) = self.terminal_error.lock().take() {
                    Err(err)
                } else {
                    Ok(None)
                }
            }
        }
    }

    pub async fn cancel(mut self) -> Result<(), MembusError> {
        self.cancelled = true;
        self.state.lock().active_streams.remove(&self.stream_id);
        let frame = Frame::new(FrameKind::Cancel, self.stream_id, Bytes::new());
        let _ = self.outbound_tx.send(frame).await;
        Ok(())
    }
}

impl Drop for ClientSubscription {
    fn drop(&mut self) {
        if !self.cancelled {
            self.state.lock().active_streams.remove(&self.stream_id);
            let frame = Frame::new(FrameKind::Cancel, self.stream_id, Bytes::new());
            enqueue_cleanup_frame(&self.outbound_tx, frame);
        }
    }
}

async fn wait_shutdown(rx: &mut watch::Receiver<bool>) {
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}

fn enqueue_cleanup_frame(tx: &mpsc::Sender<Frame>, frame: Frame) {
    if let Err(TrySendError::Full(unsent)) = tx.try_send(frame) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let tx_clone = tx.clone();
            handle.spawn(async move {
                let _ = tx_clone.send(unsent).await;
            });
        }
    }
}
