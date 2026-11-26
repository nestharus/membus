pub mod connection;
pub mod named_pipe;
pub mod tcp;

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::protocol::DEFAULT_MAX_FRAME_BODY;
use crate::signals::SignalBroker;
use crate::storage::StorageEngine;
use crate::types::MembusError;

/// Concrete transport endpoint for serving or connecting.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Endpoint {
    Tcp(SocketAddr),
    NamedPipe(String),
}

/// Configuration for the multiplexed async server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerConfig {
    pub max_frame_body: usize,
    pub max_in_flight_per_conn: usize,
    pub max_snapshots_per_conn: usize,
    pub snapshot_ttl: Duration,
    pub max_subscriptions_per_conn: usize,
    pub outbound_queue_capacity: usize,
    /// Optional root directory restricting `Request::Checkpoint` destinations.
    /// When `None`, remote checkpoint RPCs are refused.
    pub checkpoint_root: Option<PathBuf>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            max_frame_body: DEFAULT_MAX_FRAME_BODY,
            max_in_flight_per_conn: 2048,
            max_snapshots_per_conn: 64,
            snapshot_ttl: Duration::from_secs(300),
            max_subscriptions_per_conn: 256,
            outbound_queue_capacity: 4096,
            checkpoint_root: None,
        }
    }
}

/// Shared runtime state owned by a running [`MembusServer`].
#[derive(Clone)]
pub struct ServerContext {
    pub(crate) storage: Arc<dyn StorageEngine>,
    pub(crate) broker: Arc<SignalBroker>,
    pub(crate) config: ServerConfig,
    pub(crate) active_connections: Arc<AtomicUsize>,
}

impl ServerContext {
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        broker: Arc<SignalBroker>,
        config: ServerConfig,
    ) -> Self {
        Self {
            storage,
            broker,
            config,
            active_connections: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Acquire)
    }
}

/// Handle to a running TCP or Named Pipe server listener.
pub struct ServerHandle {
    endpoint: Endpoint,
    shutdown_tx: watch::Sender<bool>,
    accept_task: Option<JoinHandle<()>>,
    active_connections: Arc<AtomicUsize>,
}

impl ServerHandle {
    pub(crate) fn new(
        endpoint: Endpoint,
        shutdown_tx: watch::Sender<bool>,
        accept_task: JoinHandle<()>,
        active_connections: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            endpoint,
            shutdown_tx,
            accept_task: Some(accept_task),
            active_connections,
        }
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Acquire)
    }

    /// Trigger graceful shutdown and wait for the listener accept loop to exit.
    pub async fn shutdown(mut self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(task) = self.accept_task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        let _ = self.shutdown_tx.send(true);
        if let Some(task) = self.accept_task.take() {
            task.abort();
        }
    }
}

/// Entrypoint for binding multiplexed Membus v2 listeners.
pub struct MembusServer {
    ctx: ServerContext,
}

impl MembusServer {
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        broker: Arc<SignalBroker>,
        config: ServerConfig,
    ) -> Self {
        Self {
            ctx: ServerContext::new(storage, broker, config),
        }
    }

    pub fn context(&self) -> &ServerContext {
        &self.ctx
    }

    pub async fn bind_tcp(&self, addr: SocketAddr) -> Result<ServerHandle, MembusError> {
        tcp::spawn_tcp_listener(addr, self.ctx.clone()).await
    }

    pub async fn bind_named_pipe(&self, pipe_name: &str) -> Result<ServerHandle, MembusError> {
        named_pipe::spawn_named_pipe_listener(pipe_name.to_owned(), self.ctx.clone()).await
    }
}

/// Platform-unified bidirectional async stream dialed by [`connect_endpoint`].
pub enum TransportStream {
    Tcp(TcpStream),
    #[cfg(windows)]
    NamedPipe(tokio::net::windows::named_pipe::NamedPipeClient),
}

impl AsyncRead for TransportStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(windows)]
            Self::NamedPipe(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for TransportStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(windows)]
            Self::NamedPipe(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_flush(cx),
            #[cfg(windows)]
            Self::NamedPipe(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(windows)]
            Self::NamedPipe(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Connect to either a TCP or Windows Named Pipe [`Endpoint`].
pub async fn connect_endpoint(endpoint: &Endpoint) -> Result<TransportStream, MembusError> {
    match endpoint {
        Endpoint::Tcp(addr) => Ok(TransportStream::Tcp(tcp::connect_tcp(*addr).await?)),
        Endpoint::NamedPipe(pipe_name) => named_pipe::connect_named_pipe_stream(pipe_name).await,
    }
}
