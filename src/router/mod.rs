//! Reusable primary-isolated multi-route A/B write-tee gateway for Membus v2.
//!
//! Strictly layered zero-SCC module graph:
//! `config` -> `upstream` -> `shadow_queue` -> (`proxy`, `admin`) -> `mod` (composition root).

pub mod admin;
pub mod config;
pub mod proxy;
pub mod shadow_queue;
pub mod upstream;

use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::types::MembusError;

pub use config::{
    load_secret_file, parse_router_args, router_usage_text, validate_router_bind_security,
    validate_router_config, RouterCliCommand, RouterConfig, ShadowQueueConfig, SlotEndpointConfig,
    SlotId, DEFAULT_MAX_IN_FLIGHT, DEFAULT_ROUTER_DRAIN_TIMEOUT,
};
pub use shadow_queue::{
    CatchupReport, NamespaceParityResult, ParityCheckReport, RouterMetricsSnapshot, RouterRuntime,
    ShadowEnqueueStatus, VerifiedParityRecord,
};
pub use upstream::{UpstreamClient, UpstreamResponse};

/// Running handle to a live [`AbRouterServer`] exposing separate data and admin listeners.
pub struct RouterHandle {
    data_addr: SocketAddr,
    admin_addr: SocketAddr,
    runtime: Arc<RouterRuntime>,
    listener_shutdown_tx: watch::Sender<bool>,
    worker_shutdown_tx: watch::Sender<bool>,
    data_task: Option<JoinHandle<()>>,
    admin_task: Option<JoinHandle<()>>,
    worker_task: Option<JoinHandle<()>>,
}

impl RouterHandle {
    pub fn data_addr(&self) -> SocketAddr {
        self.data_addr
    }

    pub fn admin_addr(&self) -> SocketAddr {
        self.admin_addr
    }

    pub fn runtime(&self) -> &Arc<RouterRuntime> {
        &self.runtime
    }

    /// Trigger graceful shutdown in strict drain order:
    /// 1. Stop accepting new HTTP traffic and drain `data_task` + `admin_task`.
    /// 2. Flush remaining queued shadow deltas within the remaining drain budget.
    /// 3. Stop and join `worker_task`.
    pub async fn shutdown_with_timeout(mut self, timeout: Duration) -> Result<(), MembusError> {
        let deadline = Instant::now() + timeout;
        let mut errors = Vec::new();

        // Phase 1: Drain data and admin HTTP listeners first.
        let _ = self.listener_shutdown_tx.send(true);
        for (label, slot) in [
            ("data listener", &mut self.data_task),
            ("admin listener", &mut self.admin_task),
        ] {
            if let Some(mut handle) = slot.take() {
                let budget = deadline
                    .saturating_duration_since(Instant::now())
                    .max(Duration::from_millis(50));
                match tokio::time::timeout(budget, &mut handle).await {
                    Ok(Ok(())) => {}
                    Ok(Err(err)) => {
                        errors.push(format!("{label} panicked during shutdown: {err}"));
                    }
                    Err(_) => {
                        handle.abort();
                        errors.push(format!("{label} drain timed out after {budget:?}"));
                    }
                }
            }
        }

        // Phase 2: Allow the shadow worker to flush queued items within remaining budget.
        let flush_budget = deadline
            .saturating_duration_since(Instant::now())
            .min(Duration::from_secs(5));
        if !flush_budget.is_zero() {
            let _ = self.runtime.wait_shadow_queue_drain(flush_budget).await;
        }

        // Phase 3: Signal and join the background shadow worker.
        let _ = self.worker_shutdown_tx.send(true);
        if let Some(mut handle) = self.worker_task.take() {
            let budget = deadline
                .saturating_duration_since(Instant::now())
                .max(Duration::from_millis(50));
            match tokio::time::timeout(budget, &mut handle).await {
                Ok(Ok(())) => {}
                Ok(Err(err)) => {
                    errors.push(format!("shadow worker panicked during shutdown: {err}"));
                }
                Err(_) => {
                    handle.abort();
                    errors.push(format!("shadow worker drain timed out after {budget:?}"));
                }
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(MembusError::Io(format!(
                "ab-router graceful shutdown failed: {}",
                errors.join("; ")
            )))
        }
    }

    /// Trigger graceful shutdown using the default bounded drain timeout.
    pub async fn shutdown(self) {
        if let Err(err) = self.shutdown_with_timeout(DEFAULT_ROUTER_DRAIN_TIMEOUT).await {
            eprintln!("membus-ab-router shutdown error: {err}");
        }
    }
}

impl Drop for RouterHandle {
    fn drop(&mut self) {
        let _ = self.listener_shutdown_tx.send(true);
        let _ = self.worker_shutdown_tx.send(true);
        for task in [&mut self.data_task, &mut self.admin_task, &mut self.worker_task] {
            if let Some(h) = task.take() {
                h.abort();
            }
        }
    }
}

/// Composition root binding separate data (`:8080`) and admin (`:8081`) listeners
/// with an asynchronous bounded shadow write-tee worker.
pub struct AbRouterServer;

impl AbRouterServer {
    pub async fn start(cfg: RouterConfig) -> Result<RouterHandle, MembusError> {
        validate_router_config(&cfg)?;

        let slot_a = Arc::new(UpstreamClient::new(
            SlotId::SlotA,
            &cfg.slot_a,
            cfg.max_body_bytes,
        )?);
        let slot_b = Arc::new(UpstreamClient::new(
            SlotId::SlotB,
            &cfg.slot_b,
            cfg.max_body_bytes,
        )?);

        let admin_token = match (&cfg.admin_inline_token, &cfg.admin_token_file) {
            (Some(tok), _) if !tok.trim().is_empty() => Some(tok.trim().to_owned()),
            (_, Some(path)) => Some(load_secret_file(path)?),
            _ => None,
        };

        let runtime = Arc::new(RouterRuntime::new(slot_a, slot_b, &cfg, admin_token)?);
        runtime.reconcile_startup_slots(cfg.primary_auto).await?;

        let data_listener = TcpListener::bind(cfg.data_listen).await?;
        let data_addr = data_listener.local_addr()?;
        let admin_listener = TcpListener::bind(cfg.admin_listen).await?;
        let admin_addr = admin_listener.local_addr()?;

        let (listener_shutdown_tx, listener_shutdown_rx) = watch::channel(false);
        let (worker_shutdown_tx, worker_shutdown_rx) = watch::channel(false);

        let worker_task =
            shadow_queue::spawn_shadow_worker(Arc::clone(&runtime), worker_shutdown_rx);

        let data_app = proxy::build_data_router(Arc::clone(&runtime), cfg.max_body_bytes);
        let mut data_shutdown = listener_shutdown_rx.clone();
        let data_task = tokio::spawn(async move {
            if let Err(err) = axum::serve(data_listener, data_app)
                .with_graceful_shutdown(async move {
                    shadow_queue::wait_shutdown_ref(&mut data_shutdown).await;
                })
                .await
            {
                eprintln!("membus-ab-router data listener error: {err}");
            }
        });

        let admin_app = admin::build_admin_router(Arc::clone(&runtime));
        let mut admin_shutdown = listener_shutdown_rx;
        let admin_task = tokio::spawn(async move {
            if let Err(err) = axum::serve(admin_listener, admin_app)
                .with_graceful_shutdown(async move {
                    shadow_queue::wait_shutdown_ref(&mut admin_shutdown).await;
                })
                .await
            {
                eprintln!("membus-ab-router admin listener error: {err}");
            }
        });

        Ok(RouterHandle {
            data_addr,
            admin_addr,
            runtime,
            listener_shutdown_tx,
            worker_shutdown_tx,
            data_task: Some(data_task),
            admin_task: Some(admin_task),
            worker_task: Some(worker_task),
        })
    }
}

/// Execute a parsed CLI command for `membus-ab-router`.
pub async fn run_router_cli(cmd: RouterCliCommand) -> Result<(), MembusError> {
    match cmd {
        RouterCliCommand::Help(text) => {
            println!("{text}");
            Ok(())
        }
        RouterCliCommand::Run(cfg) => {
            let drain_timeout = cfg.shutdown_drain_timeout;
            let handle = AbRouterServer::start(*cfg).await?;
            println!(
                "listening data={} admin={}",
                handle.data_addr(),
                handle.admin_addr()
            );
            let _ = std::io::stdout().flush();

            let signal_name = wait_shutdown_signal().await?;
            eprintln!("membus-ab-router received {signal_name}; draining listeners and shadow worker...");
            handle.shutdown_with_timeout(drain_timeout).await
        }
    }
}

async fn wait_shutdown_signal() -> Result<&'static str, MembusError> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm = signal(SignalKind::terminate()).map_err(|err| {
            MembusError::Io(format!("failed to register SIGTERM handler: {err}"))
        })?;
        tokio::select! {
            res = tokio::signal::ctrl_c() => {
                res?;
                Ok("SIGINT")
            }
            _ = sigterm.recv() => Ok("SIGTERM"),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        Ok("SIGINT")
    }
}
