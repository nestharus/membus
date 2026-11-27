pub mod connection;
pub(crate) mod helpers;
pub mod http;
pub mod named_pipe;
pub mod tcp;
pub mod tls;
pub mod write_coord;

use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use parking_lot::{RwLock, RwLockReadGuard};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::{watch, OwnedSemaphorePermit, Semaphore};
use tokio::task::JoinHandle;

use crate::auth::{AuthConfig, LeaseManager};
use crate::catalog::CollectionCatalog;
use crate::protocol::DEFAULT_MAX_FRAME_BODY;
use crate::search::SearchEngine;
use crate::signals::SignalBroker;
use crate::storage::{MetaCasGuard, StorageEngine};
use crate::types::{AuthMode, ExpectedVersion, HealthStatus, MembusError, SlotRole, SlotRoleState};

pub use tls::{TlsClientConfig, TlsServerConfig};
pub use write_coord::CoordinatedCommit;

/// `CF_META` key persisting the resident's [`SlotRoleState`] (`role` + monotonic `role_epoch`).
pub const META_KEY_SLOT_ROLE: &[u8] = b"sys:slot_role";

const ROLE_EPOCH_MASK: u64 = (1u64 << 60) - 1;

fn pack_slot_role(state: SlotRoleState) -> u64 {
    ((u64::from(state.role.as_u8())) << 60) | (state.role_epoch & ROLE_EPOCH_MASK)
}

fn unpack_slot_role(packed: u64) -> SlotRoleState {
    let role_bits = (packed >> 60) as u8;
    let role = SlotRole::from_u8(role_bits).unwrap_or(SlotRole::Active);
    let role_epoch = packed & ROLE_EPOCH_MASK;
    SlotRoleState { role, role_epoch }
}

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
    /// Default runtime slot role initialized on fresh storage (`Active` by default).
    /// Existing storage preserves its persisted `CF_META` (`sys:slot_role`) state across restarts.
    pub initial_slot_role: SlotRole,
    /// Optional explicit node/slot identifier used for parity digests and replication echo-loop guards.
    pub node_id: Option<String>,
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
            initial_slot_role: SlotRole::Active,
            node_id: None,
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
    pub(crate) node_id: Arc<String>,
    pub(crate) slot_role_packed: Arc<AtomicU64>,
    /// Linearizes slot-role transitions (`write`) against all storage/catalog mutations (`read`)
    /// so no write queued in `spawn_blocking` can commit after a fence transition returns.
    pub(crate) role_fence_gate: Arc<RwLock<()>>,
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
        let node_id = config
            .node_id
            .clone()
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| format!("node-{:016x}", storage.epoch()));
        let initial_state = load_or_init_slot_role(storage.as_ref(), config.initial_slot_role);
        Self {
            storage,
            broker,
            auth: Arc::new(AuthConfig::disabled()),
            leases,
            catalog,
            search: None,
            config,
            node_id: Arc::new(node_id),
            slot_role_packed: Arc::new(AtomicU64::new(pack_slot_role(initial_state))),
            role_fence_gate: Arc::new(RwLock::new(())),
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

    pub fn with_node_id(mut self, node_id: impl Into<String>) -> Self {
        let id = node_id.into();
        if !id.is_empty() {
            self.node_id = Arc::new(id);
        }
        self
    }

    pub fn node_id(&self) -> &str {
        self.node_id.as_str()
    }

    pub fn slot_role_state(&self) -> SlotRoleState {
        unpack_slot_role(self.slot_role_packed.load(Ordering::Acquire))
    }

    /// CAS-transition the resident's [`SlotRole`], monotonically incrementing `role_epoch`
    /// and persisting atomically to `CF_META` (`sys:slot_role`).
    ///
    /// Acquires the exclusive `role_fence_gate` write guard so all in-flight mutations complete
    /// before the transition commits and returns, preventing post-fence writes.
    pub fn transition_slot_role(
        &self,
        expected_role: SlotRole,
        expected_role_epoch: u64,
        new_role: SlotRole,
    ) -> Result<SlotRoleState, MembusError> {
        let _write_gate = self.role_fence_gate.write();
        let prev_raw = self.storage.meta_get(META_KEY_SLOT_ROLE)?;
        let current = match &prev_raw {
            Some(bytes) => {
                let state = serde_json::from_slice::<SlotRoleState>(bytes).map_err(|err| {
                    MembusError::Corruption(format!("corrupt sys:slot_role metadata: {err}"))
                })?;
                if state.role_epoch == 0 || state.role_epoch > ROLE_EPOCH_MASK {
                    return Err(MembusError::Corruption(format!(
                        "corrupt sys:slot_role role_epoch {}",
                        state.role_epoch
                    )));
                }
                state
            }
            None => self.slot_role_state(),
        };
        if current.role != expected_role || current.role_epoch != expected_role_epoch {
            // Return `Conflict` (always HTTP 409) rather than `RoleFenced` (which maps `Draining` to 503).
            return Err(MembusError::Conflict {
                namespace: "_admin:slot_role".to_owned(),
                key: format!(
                    "expected={}:{},actual={}:{}",
                    expected_role.as_str(),
                    expected_role_epoch,
                    current.role.as_str(),
                    current.role_epoch
                )
                .into_bytes(),
                expected: ExpectedVersion::Version(expected_role_epoch),
                actual: Some(current.role_epoch),
            });
        }
        let next_epoch = current
            .role_epoch
            .checked_add(1)
            .ok_or_else(|| MembusError::Storage("role_epoch overflow".to_owned()))?;
        if next_epoch > ROLE_EPOCH_MASK {
            return Err(MembusError::Storage(
                "role_epoch exceeds maximum".to_owned(),
            ));
        }
        let next = SlotRoleState {
            role: new_role,
            role_epoch: next_epoch,
        };
        let encoded = serde_json::to_vec(&next)
            .map_err(|err| MembusError::Storage(format!("encode sys:slot_role failed: {err}")))?;
        self.storage.meta_cas_batch(
            &[MetaCasGuard {
                key: META_KEY_SLOT_ROLE.to_vec(),
                expected: prev_raw,
            }],
            &[(META_KEY_SLOT_ROLE.to_vec(), encoded)],
            &[],
            true,
        )?;
        self.slot_role_packed
            .store(pack_slot_role(next), Ordering::Release);
        Ok(next)
    }

    /// Enforce slot-role write fencing for normal client data-plane mutations.
    pub fn check_client_write_allowed(
        &self,
        supplied_role_epoch: Option<u64>,
    ) -> Result<SlotRoleState, MembusError> {
        let state = self.slot_role_state();
        if state.role != SlotRole::Active {
            return Err(MembusError::RoleFenced {
                role: state.role,
                role_epoch: state.role_epoch,
                reason: format!(
                    "client writes are rejected while slot role is '{}'",
                    state.role.as_str()
                ),
            });
        }
        if let Some(req_epoch) = supplied_role_epoch {
            if req_epoch < state.role_epoch {
                return Err(MembusError::RoleFenced {
                    role: state.role,
                    role_epoch: state.role_epoch,
                    reason: format!(
                        "stale X-Membus-Role-Epoch {req_epoch} < current role_epoch {}",
                        state.role_epoch
                    ),
                });
            }
        }
        Ok(state)
    }

    /// Acquire the linearizable read fence guard and verify client write eligibility inside
    /// `spawn_blocking` across the entire storage commit.
    pub(crate) fn acquire_client_write_fence(
        &self,
        supplied_role_epoch: Option<u64>,
    ) -> Result<RwLockReadGuard<'_, ()>, MembusError> {
        let guard = self.role_fence_gate.read();
        self.check_client_write_allowed(supplied_role_epoch)?;
        Ok(guard)
    }

    /// Enforce slot-role fencing for authenticated replicated delta application.
    pub fn check_replicated_apply_allowed(
        &self,
        supplied_role_epoch: Option<u64>,
    ) -> Result<SlotRoleState, MembusError> {
        let state = self.slot_role_state();
        if !matches!(state.role, SlotRole::Active | SlotRole::Shadow) {
            return Err(MembusError::RoleFenced {
                role: state.role,
                role_epoch: state.role_epoch,
                reason: format!(
                    "replicated apply is rejected while slot role is '{}'",
                    state.role.as_str()
                ),
            });
        }
        if let Some(req_epoch) = supplied_role_epoch {
            if req_epoch < state.role_epoch {
                return Err(MembusError::RoleFenced {
                    role: state.role,
                    role_epoch: state.role_epoch,
                    reason: format!(
                        "stale X-Membus-Role-Epoch {req_epoch} < current role_epoch {}",
                        state.role_epoch
                    ),
                });
            }
        }
        Ok(state)
    }

    /// Acquire the linearizable read fence guard and verify replicated-apply eligibility.
    pub(crate) fn acquire_replicated_apply_fence(
        &self,
        supplied_role_epoch: Option<u64>,
    ) -> Result<RwLockReadGuard<'_, ()>, MembusError> {
        let guard = self.role_fence_gate.read();
        self.check_replicated_apply_allowed(supplied_role_epoch)?;
        Ok(guard)
    }

    /// Enforce slot-role fencing for catalog registration and migration metadata writes.
    /// Permits `Active` and `Shadow`; rejects `ReadOnly` (409), `Draining` (503), and stale epochs.
    pub fn check_catalog_write_allowed(
        &self,
        supplied_role_epoch: Option<u64>,
    ) -> Result<SlotRoleState, MembusError> {
        let state = self.slot_role_state();
        if !matches!(state.role, SlotRole::Active | SlotRole::Shadow) {
            return Err(MembusError::RoleFenced {
                role: state.role,
                role_epoch: state.role_epoch,
                reason: format!(
                    "catalog writes are rejected while slot role is '{}'",
                    state.role.as_str()
                ),
            });
        }
        if let Some(req_epoch) = supplied_role_epoch {
            if req_epoch < state.role_epoch {
                return Err(MembusError::RoleFenced {
                    role: state.role,
                    role_epoch: state.role_epoch,
                    reason: format!(
                        "stale X-Membus-Role-Epoch {req_epoch} < current role_epoch {}",
                        state.role_epoch
                    ),
                });
            }
        }
        Ok(state)
    }

    /// Acquire the linearizable read fence guard and verify catalog write eligibility.
    pub(crate) fn acquire_catalog_write_fence(
        &self,
        supplied_role_epoch: Option<u64>,
    ) -> Result<RwLockReadGuard<'_, ()>, MembusError> {
        let guard = self.role_fence_gate.read();
        self.check_catalog_write_allowed(supplied_role_epoch)?;
        Ok(guard)
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
        let role_state = self.slot_role_state();
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
            slot_role: role_state.role,
            role_epoch: role_state.role_epoch,
        }
    }
}

fn decode_persisted_slot_role(raw: &[u8]) -> Result<SlotRoleState, String> {
    let state = serde_json::from_slice::<SlotRoleState>(raw).map_err(|err| err.to_string())?;
    if state.role_epoch == 0 || state.role_epoch > ROLE_EPOCH_MASK {
        return Err(format!(
            "persisted role_epoch {} out of valid range 1..={ROLE_EPOCH_MASK}",
            state.role_epoch
        ));
    }
    Ok(state)
}

fn load_or_init_slot_role(storage: &dyn StorageEngine, initial_role: SlotRole) -> SlotRoleState {
    let fail_closed = SlotRoleState {
        role: SlotRole::ReadOnly,
        role_epoch: 0,
    };
    match storage.meta_get(META_KEY_SLOT_ROLE) {
        Ok(Some(raw)) => match decode_persisted_slot_role(&raw) {
            Ok(state) => state,
            Err(err) => {
                eprintln!(
                    "ERROR: corrupt persisted sys:slot_role metadata ({err}); failing closed into read_only"
                );
                fail_closed
            }
        },
        Err(err) => {
            eprintln!(
                "ERROR: failed to read sys:slot_role metadata ({err}); failing closed into read_only"
            );
            fail_closed
        }
        Ok(None) => {
            let initial = SlotRoleState {
                role: initial_role,
                role_epoch: 1,
            };
            let Ok(encoded) = serde_json::to_vec(&initial) else {
                eprintln!("ERROR: failed to serialize initial sys:slot_role; failing closed into read_only");
                return fail_closed;
            };
            match storage.meta_cas_batch(
                &[MetaCasGuard {
                    key: META_KEY_SLOT_ROLE.to_vec(),
                    expected: None,
                }],
                &[(META_KEY_SLOT_ROLE.to_vec(), encoded)],
                &[],
                true,
            ) {
                Ok(()) => initial,
                Err(cas_err) => match storage.meta_get(META_KEY_SLOT_ROLE) {
                    Ok(Some(raw)) => match decode_persisted_slot_role(&raw) {
                        Ok(state) => state,
                        Err(err) => {
                            eprintln!(
                                "ERROR: corrupt sys:slot_role after concurrent init ({err}); failing closed into read_only"
                            );
                            fail_closed
                        }
                    },
                    Ok(None) => {
                        eprintln!(
                            "ERROR: failed to persist initial sys:slot_role ({cas_err}); failing closed into read_only"
                        );
                        fail_closed
                    }
                    Err(err) => {
                        eprintln!(
                            "ERROR: failed to re-read sys:slot_role ({err}); failing closed into read_only"
                        );
                        fail_closed
                    }
                },
            }
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

    pub fn with_node_id(mut self, node_id: impl Into<String>) -> Self {
        self.ctx = self.ctx.with_node_id(node_id);
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
