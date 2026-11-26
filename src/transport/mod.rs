pub mod connection;
pub mod http;
pub mod named_pipe;
pub mod tcp;
pub mod tls;
pub mod write_coord;

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
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;

use crate::auth::{AuthConfig, LeaseManager};
use crate::catalog::CollectionCatalog;
use crate::protocol::DEFAULT_MAX_FRAME_BODY;
use crate::search::SearchEngine;
use crate::signals::SignalBroker;
use crate::storage::StorageEngine;
use crate::types::{AuthMode, HealthStatus, MembusError};

pub use tls::{TlsClientConfig, TlsServerConfig};
pub use write_coord::CoordinatedCommit;

/// Concrete transport endpoint for serving or connecting.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Endpoint {
    Tcp(SocketAddr),
    TcpTls(SocketAddr),
    Http(SocketAddr),
    HttpTls(SocketAddr),
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
    /// Global maximum concurrent in-flight HTTP/REST data-plane requests.
    pub max_http_in_flight: usize,
    /// Global maximum concurrent active SSE, WebSocket, and binary subscription streams.
    pub max_active_streams: usize,
    /// Optional timeout applied to blocking scan, index, graph, and search queries.
    pub query_timeout: Option<Duration>,
    /// Break-glass override permitting non-loopback listener binds without both Auth and TLS.
    pub allow_insecure_non_loopback: bool,
    /// Allow unauthenticated `GET /v2/health` liveness/readiness probes even when `AuthMode::Enforced`.
    pub public_health_probe: bool,
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
            max_http_in_flight: 1024,
            max_active_streams: 1024,
            query_timeout: Some(Duration::from_secs(30)),
            allow_insecure_non_loopback: false,
            public_health_probe: false,
        }
    }
}

/// RAII guard holding one global active stream slot (SSE, WS, or binary subscription).
pub(crate) struct StreamGuard {
    counter: Arc<AtomicUsize>,
    _permit: OwnedSemaphorePermit,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Shared runtime state owned by a running [`MembusServer`] across binary, HTTP, WS, and SSE transports.
#[derive(Clone)]
pub struct ServerContext {
    pub(crate) storage: Arc<dyn StorageEngine>,
    pub(crate) broker: Arc<SignalBroker>,
    pub(crate) auth: Arc<AuthConfig>,
    pub(crate) leases: Arc<LeaseManager>,
    pub(crate) catalog: Arc<CollectionCatalog>,
    pub(crate) search: Option<Arc<SearchEngine>>,
    pub(crate) config: ServerConfig,
    pub(crate) active_connections: Arc<AtomicUsize>,
    pub(crate) active_streams: Arc<AtomicUsize>,
    pub(crate) stream_sem: Arc<Semaphore>,
    pub(crate) http_in_flight_sem: Arc<Semaphore>,
}

impl ServerContext {
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        broker: Arc<SignalBroker>,
        config: ServerConfig,
    ) -> Self {
        let leases = Arc::new(LeaseManager::new(Arc::clone(&storage)));
        let catalog = Arc::new(CollectionCatalog::new(Arc::clone(&storage)));
        let stream_sem = Arc::new(Semaphore::new(config.max_active_streams.max(1)));
        let http_in_flight_sem = Arc::new(Semaphore::new(config.max_http_in_flight.max(1)));
        Self {
            storage,
            broker,
            auth: Arc::new(AuthConfig::disabled()),
            leases,
            catalog,
            search: None,
            config,
            active_connections: Arc::new(AtomicUsize::new(0)),
            active_streams: Arc::new(AtomicUsize::new(0)),
            stream_sem,
            http_in_flight_sem,
        }
    }

    pub fn with_auth(mut self, auth: AuthConfig) -> Self {
        self.auth = Arc::new(auth);
        self
    }

    pub fn with_search(mut self, search: Arc<SearchEngine>) -> Self {
        self.search = Some(search);
        self
    }

    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::Acquire)
    }

    pub fn active_streams(&self) -> usize {
        self.active_streams.load(Ordering::Acquire)
    }

    pub(crate) fn try_acquire_stream_guard(&self) -> Result<StreamGuard, MembusError> {
        let permit = Arc::clone(&self.stream_sem)
            .try_acquire_owned()
            .map_err(|_| MembusError::Busy {
                retry_after_ms: 10,
                reason: format!(
                    "global active stream limit reached ({})",
                    self.config.max_active_streams.max(1)
                ),
            })?;
        self.active_streams.fetch_add(1, Ordering::AcqRel);
        Ok(StreamGuard {
            counter: Arc::clone(&self.active_streams),
            _permit: permit,
        })
    }

    pub fn health_status(&self) -> HealthStatus {
        let (search_ingest_deferred_total, search_dirty) = self
            .search
            .as_ref()
            .map_or((0, false), |s| (s.deferred_ingest_total(), s.is_dirty()));
        HealthStatus {
            epoch: self.storage.epoch(),
            latest_seq: self.storage.latest_seq(),
            log_horizon_seq: self.storage.log_horizon_seq(),
            in_flight_ops: self.storage.in_flight_ops(),
            in_flight_bytes: self.storage.in_flight_bytes(),
            active_connections: self.active_connections(),
            active_subscriptions: self.broker.active_subscriptions(),
            active_snapshots: self.storage.active_snapshots(),
            active_streams: self.active_streams(),
            search_ingest_deferred_total,
            search_dirty,
        }
    }
}

/// Fail-closed bind guard rejecting non-loopback TCP/HTTP listeners unless BOTH auth and TLS
/// are configured or the explicit `allow_insecure_non_loopback` break-glass override is set.
pub fn validate_listener_bind_security(
    listener_kind: &str,
    addr: SocketAddr,
    auth_enabled: bool,
    tls_enabled: bool,
    allow_insecure_non_loopback: bool,
) -> Result<(), MembusError> {
    if addr.ip().is_loopback() {
        return Ok(());
    }
    if auth_enabled && tls_enabled {
        return Ok(());
    }
    if allow_insecure_non_loopback {
        eprintln!(
            "WARNING: binding {listener_kind} listener on non-loopback address {addr} with allow_insecure_non_loopback override (auth_enabled={auth_enabled}, tls_enabled={tls_enabled})"
        );
        return Ok(());
    }
    let mut missing = Vec::new();
    if !auth_enabled {
        missing.push("auth");
    }
    if !tls_enabled {
        missing.push("TLS");
    }
    Err(MembusError::InvalidArgument(format!(
        "refusing to bind {listener_kind} listener on non-loopback address {addr} without {} enabled; configure both auth and TLS or pass --allow-insecure-non-loopback / allow_insecure_non_loopback=true to override",
        missing.join(" and ")
    )))
}

/// Handle to a running TCP, TLS/mTLS TCP, HTTP/2+WS+SSE, HTTPS/mTLS, or Named Pipe listener.
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

    /// Trigger graceful shutdown and wait up to `timeout` for the listener accept loop to exit,
    /// returning an explicit error on join panic or drain timeout.
    pub async fn shutdown_with_timeout(mut self, timeout: Duration) -> Result<(), MembusError> {
        let _ = self.shutdown_tx.send(true);
        if let Some(mut task) = self.accept_task.take() {
            match tokio::time::timeout(timeout, &mut task).await {
                Ok(Ok(())) => Ok(()),
                Ok(Err(join_err)) => Err(MembusError::Storage(format!(
                    "listener {:?} accept task failed during shutdown: {join_err}",
                    self.endpoint
                ))),
                Err(_) => {
                    task.abort();
                    Err(MembusError::Io(format!(
                        "listener {:?} graceful shutdown timed out after {:?}",
                        self.endpoint, timeout
                    )))
                }
            }
        } else {
            Ok(())
        }
    }

    /// Trigger graceful shutdown and wait for the listener accept loop to exit.
    pub async fn shutdown(self) {
        if let Err(err) = self.shutdown_with_timeout(Duration::from_secs(30)).await {
            eprintln!("membus listener shutdown error: {err}");
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

/// Entrypoint for binding multiplexed Membus v2 listeners (TCP, TLS/mTLS, Named Pipe, Axum HTTP/2+WS+SSE).
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

    pub fn from_context(ctx: ServerContext) -> Self {
        Self { ctx }
    }

    pub fn with_auth(mut self, auth: AuthConfig) -> Self {
        self.ctx = self.ctx.with_auth(auth);
        self
    }

    pub fn with_search(mut self, search: Arc<SearchEngine>) -> Self {
        self.ctx = self.ctx.with_search(search);
        self
    }

    pub fn context(&self) -> &ServerContext {
        &self.ctx
    }

    pub async fn bind_tcp(&self, addr: SocketAddr) -> Result<ServerHandle, MembusError> {
        validate_listener_bind_security(
            "binary TCP",
            addr,
            self.ctx.auth.mode() == AuthMode::Enforced,
            false,
            self.ctx.config.allow_insecure_non_loopback,
        )?;
        tcp::spawn_tcp_listener(addr, self.ctx.clone()).await
    }

    pub async fn bind_tcp_tls(
        &self,
        addr: SocketAddr,
        tls_cfg: &TlsServerConfig,
    ) -> Result<ServerHandle, MembusError> {
        validate_listener_bind_security(
            "binary TCP/TLS",
            addr,
            self.ctx.auth.mode() == AuthMode::Enforced,
            true,
            self.ctx.config.allow_insecure_non_loopback,
        )?;
        tls::spawn_tls_tcp_listener(addr, tls_cfg, self.ctx.clone()).await
    }

    pub async fn bind_http(&self, addr: SocketAddr) -> Result<ServerHandle, MembusError> {
        validate_listener_bind_security(
            "HTTP",
            addr,
            self.ctx.auth.mode() == AuthMode::Enforced,
            false,
            self.ctx.config.allow_insecure_non_loopback,
        )?;
        http::spawn_http_listener(addr, self.ctx.clone()).await
    }

    pub async fn bind_http_tls(
        &self,
        addr: SocketAddr,
        tls_cfg: &TlsServerConfig,
    ) -> Result<ServerHandle, MembusError> {
        validate_listener_bind_security(
            "HTTPS",
            addr,
            self.ctx.auth.mode() == AuthMode::Enforced,
            true,
            self.ctx.config.allow_insecure_non_loopback,
        )?;
        http::spawn_http_tls_listener(addr, tls_cfg, self.ctx.clone()).await
    }

    pub async fn bind_named_pipe(&self, pipe_name: &str) -> Result<ServerHandle, MembusError> {
        named_pipe::spawn_named_pipe_listener(pipe_name.to_owned(), self.ctx.clone()).await
    }
}

/// Platform-unified bidirectional async stream dialed by [`connect_endpoint`].
pub enum TransportStream {
    Tcp(TcpStream),
    TcpTls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
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
            Self::TcpTls(s) => Pin::new(s).poll_read(cx, buf),
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
            Self::TcpTls(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(windows)]
            Self::NamedPipe(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_flush(cx),
            Self::TcpTls(s) => Pin::new(s).poll_flush(cx),
            #[cfg(windows)]
            Self::NamedPipe(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            Self::TcpTls(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(windows)]
            Self::NamedPipe(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Connect to a plaintext TCP or Windows Named Pipe [`Endpoint`].
pub async fn connect_endpoint(endpoint: &Endpoint) -> Result<TransportStream, MembusError> {
    match endpoint {
        Endpoint::Tcp(addr) => Ok(TransportStream::Tcp(tcp::connect_tcp(*addr).await?)),
        Endpoint::NamedPipe(pipe_name) => named_pipe::connect_named_pipe_stream(pipe_name).await,
        Endpoint::TcpTls(_) => Err(MembusError::InvalidArgument(
            "use Client::connect_tcp_tls with TlsClientConfig for TcpTls endpoints".to_owned(),
        )),
        Endpoint::Http(_) | Endpoint::HttpTls(_) => Err(MembusError::InvalidArgument(
            "Http/HttpTls endpoints use HTTP/WS/SSE rather than binary membus/2 framing".to_owned(),
        )),
    }
}
