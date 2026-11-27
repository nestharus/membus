use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use parking_lot::Mutex;
use tokio::io::{AsyncRead, AsyncWrite, ReadHalf, WriteHalf};
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::{mpsc, oneshot, watch};

use crate::protocol::{
    decode_response, decode_signal_event, decode_stream_end, encode_request, read_frame,
    run_batched_frame_writer, Frame, FrameKind, Request, Response, DEFAULT_MAX_FRAME_BODY,
};
use crate::transport::tls::connect_tcp_tls;
use crate::transport::{connect_endpoint, Endpoint, TlsClientConfig};
use crate::types::{MembusError, SignalEvent};

const CLIENT_OUTBOUND_QUEUE: usize = 8192;
const SUBSCRIPTION_CHANNEL_CAPACITY: usize = 1024;

pub(super) struct StreamDispatchEntry {
    pub(super) tx: mpsc::Sender<Result<SignalEvent, MembusError>>,
    pub(super) terminal_error: Arc<Mutex<Option<MembusError>>>,
}

pub(super) struct ClientState {
    pub(super) closed: bool,
    pub(super) pending_rpcs: HashMap<u64, oneshot::Sender<Result<Response, MembusError>>>,
    pub(super) pending_pings: HashMap<u64, oneshot::Sender<Result<(), MembusError>>>,
    pub(super) active_streams: HashMap<u64, StreamDispatchEntry>,
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

pub(super) struct ClientInner {
    pub(super) outbound_tx: mpsc::Sender<Frame>,
    pub(super) next_stream_id: AtomicU64,
    pub(super) max_frame_body: usize,
    pub(super) subscription_capacity: usize,
    pub(super) state: Arc<Mutex<ClientState>>,
}

/// Pipelined, multiplexed async `membus/2` client.
///
/// Cheaply cloneable: hundreds or thousands of concurrent async tasks can share one
/// underlying TCP, TLS/mTLS TCP, or Windows Named Pipe connection with full RPC pipelining
/// and live subscription push streams multiplexed by `stream_id`.
#[derive(Clone)]
pub struct Client {
    pub(super) inner: Arc<ClientInner>,
}

impl Client {
    pub async fn connect(endpoint: &Endpoint) -> Result<Self, MembusError> {
        let stream = connect_endpoint(endpoint).await?;
        Ok(Self::from_stream(stream, DEFAULT_MAX_FRAME_BODY))
    }

    pub async fn connect_tcp(addr: SocketAddr) -> Result<Self, MembusError> {
        Self::connect(&Endpoint::Tcp(addr)).await
    }

    pub async fn connect_tcp_tls(
        addr: SocketAddr,
        tls_cfg: &TlsClientConfig,
    ) -> Result<Self, MembusError> {
        let tls_stream = connect_tcp_tls(addr, tls_cfg).await?;
        Ok(Self::from_stream(tls_stream, DEFAULT_MAX_FRAME_BODY))
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
        let (reader, writer) = tokio::io::split(stream);
        let (outbound_tx, outbound_rx) = mpsc::channel::<Frame>(CLIENT_OUTBOUND_QUEUE);

        let state = Arc::new(Mutex::new(ClientState::new()));
        let (shutdown_tx, writer_shutdown_rx) = watch::channel(false);
        let reader_shutdown_rx = shutdown_tx.subscribe();

        spawn_writer_task(
            writer,
            outbound_rx,
            max_frame_body,
            Arc::clone(&state),
            shutdown_tx.clone(),
            writer_shutdown_rx,
        );
        spawn_reader_task(
            reader,
            max_frame_body,
            Arc::clone(&state),
            shutdown_tx,
            reader_shutdown_rx,
            outbound_tx.downgrade(),
        );

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

    pub async fn authenticate(&self, token: &str) -> Result<String, MembusError> {
        let resp = self
            .call(Request::Authenticate {
                token: token.to_owned(),
            })
            .await?;
        match resp {
            Response::Authenticated { principal_id } => Ok(principal_id),
            Response::Error(e) => Err(e),
            other => Err(MembusError::Protocol(format!(
                "unexpected response for authenticate: {other:?}"
            ))),
        }
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

    pub(super) async fn call(&self, req: Request) -> Result<Response, MembusError> {
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

    pub(super) fn check_frame_size(&self, payload: &Bytes) -> Result<(), MembusError> {
        if payload.len() > self.inner.max_frame_body {
            return Err(MembusError::FrameTooLarge {
                len: payload.len(),
                max_len: self.inner.max_frame_body,
            });
        }
        Ok(())
    }

    pub(super) fn alloc_stream_id(&self) -> u64 {
        self.inner.next_stream_id.fetch_add(1, Ordering::Relaxed)
    }
}

fn spawn_writer_task<S>(
    mut writer: WriteHalf<S>,
    mut outbound_rx: mpsc::Receiver<Frame>,
    max_frame_body: usize,
    writer_state: Arc<Mutex<ClientState>>,
    writer_shutdown_tx: watch::Sender<bool>,
    mut writer_shutdown_rx: watch::Receiver<bool>,
) where
    S: AsyncWrite + Send + Unpin + 'static,
{
    tokio::spawn(async move {
        tokio::select! {
            biased;
            _ = wait_shutdown(&mut writer_shutdown_rx) => {}
            _ = run_batched_frame_writer(&mut writer, &mut outbound_rx, max_frame_body) => {}
        }
        writer_state.lock().fail_all();
        let _ = writer_shutdown_tx.send(true);
    });
}

fn spawn_reader_task<S>(
    mut reader: ReadHalf<S>,
    max_frame_body: usize,
    reader_state: Arc<Mutex<ClientState>>,
    reader_shutdown_tx: watch::Sender<bool>,
    mut reader_shutdown_rx: watch::Receiver<bool>,
    reader_outbound: mpsc::WeakSender<Frame>,
) where
    S: AsyncRead + Send + Unpin + 'static,
{
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
            dispatch_inbound_frame(frame, &reader_state, &reader_outbound);
        }
        reader_state.lock().fail_all();
        let _ = reader_shutdown_tx.send(true);
    });
}

fn dispatch_inbound_frame(
    frame: Frame,
    reader_state: &Arc<Mutex<ClientState>>,
    reader_outbound: &mpsc::WeakSender<Frame>,
) {
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
            dispatch_stream_event(frame, reader_state, reader_outbound);
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

fn dispatch_stream_event(
    frame: Frame,
    reader_state: &Arc<Mutex<ClientState>>,
    reader_outbound: &mpsc::WeakSender<Frame>,
) {
    let decoded = decode_signal_event(&frame.payload);
    let lagged_stream = {
        let mut guard = reader_state.lock();
        if let Some(entry) = guard.active_streams.get(&frame.stream_id) {
            match entry.tx.try_send(decoded) {
                Ok(()) => false,
                Err(TrySendError::Full(_)) => {
                    if let Some(removed) = guard.active_streams.remove(&frame.stream_id) {
                        *removed.terminal_error.lock() = Some(MembusError::Busy {
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
            let _ = tx.try_send(Frame::new(FrameKind::Cancel, frame.stream_id, Bytes::new()));
        }
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

async fn wait_shutdown(rx: &mut watch::Receiver<bool>) {
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}

pub(super) fn enqueue_cleanup_frame(tx: &mpsc::Sender<Frame>, frame: Frame) {
    if let Err(TrySendError::Full(unsent)) = tx.try_send(frame) {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let tx_clone = tx.clone();
            handle.spawn(async move {
                let _ = tx_clone.send(unsent).await;
            });
        }
    }
}
