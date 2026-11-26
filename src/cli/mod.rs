pub mod diag;

use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::auth::AuthConfig;
use crate::cli::diag::{parse_diag_args, run_diag, DiagCommand};
use crate::client::Client;
use crate::mesh::{validate_mesh_listen_endpoints, MeshConfig, MeshNode};
use crate::search::{SearchConfig, SearchEngine};
use crate::signals::{SignalBroker, SignalConfig, SignalRing};
use crate::storage::{RocksDbStorage, SignalPublisher, StorageConfig, StorageEngine};
use crate::transport::{
    validate_listener_bind_security, Endpoint, MembusServer, ServerConfig, ServerHandle,
    TlsClientConfig, TlsServerConfig,
};
use crate::types::{AuthMode, MembusError};

/// Default bounded drain timeout during graceful resident shutdown (leaves headroom
/// beneath standard 30s container/Kubernetes `terminationGracePeriodSeconds`).
pub const DEFAULT_SHUTDOWN_DRAIN_TIMEOUT: Duration = Duration::from_secs(25);

/// Client connection options shared by `membus health` and `membus checkpoint`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientConnectArgs {
    pub endpoint: Endpoint,
    pub token: Option<String>,
    pub tls: Option<TlsClientConfig>,
}

/// Parsed CLI command for `membus`.
#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Serve(Box<ServeArgs>),
    Health(Box<ClientConnectArgs>),
    Checkpoint {
        connect: Box<ClientConnectArgs>,
        dest_dir: PathBuf,
    },
    Diag(DiagCommand),
    Help(&'static str),
}

#[derive(Debug, PartialEq, Eq)]
pub struct ServeArgs {
    pub data_dir: PathBuf,
    pub tcp: Option<SocketAddr>,
    pub pipe: Option<String>,
    pub http_listen: Option<SocketAddr>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub tls_client_ca: Option<PathBuf>,
    pub http_tls_cert: Option<PathBuf>,
    pub http_tls_key: Option<PathBuf>,
    pub http_tls_client_ca: Option<PathBuf>,
    pub auth_config: Option<PathBuf>,
    pub checkpoint_root: Option<PathBuf>,
    pub max_in_flight_ops: Option<usize>,
    pub max_in_flight_bytes: Option<usize>,
    pub max_in_flight_per_conn: Option<usize>,
    pub max_http_in_flight: Option<usize>,
    pub max_active_streams: Option<usize>,
    pub max_active_snapshots: Option<usize>,
    pub query_timeout_ms: Option<u64>,
    pub allow_insecure_non_loopback: bool,
    pub public_health_probe: bool,
    pub mesh_node_id: Option<String>,
    pub mesh_cluster: Option<String>,
    pub mesh_listen: Vec<String>,
    pub mesh_connect: Vec<String>,
}

/// Parse command-line arguments (excluding argv[0]) into a typed [`Command`].
pub fn parse_args<I, S>(args: I) -> Result<Command, MembusError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let raw: Vec<String> = args.into_iter().map(|s| s.as_ref().to_owned()).collect();
    if raw.is_empty() {
        return Err(MembusError::InvalidArgument(usage_text().to_owned()));
    }
    match raw[0].as_str() {
        "-h" | "--help" | "help" => Ok(Command::Help(usage_text())),
        "serve" => parse_serve(&raw[1..]),
        "health" => parse_health(&raw[1..]),
        "checkpoint" => parse_checkpoint(&raw[1..]),
        "diag" => {
            if raw.get(1).is_some_and(|a| a == "help")
                || raw[1..].iter().any(|a| matches!(a.as_str(), "-h" | "--help"))
            {
                return Ok(Command::Help(diag_usage_text()));
            }
            Ok(Command::Diag(parse_diag_args(&raw[1..])?))
        }
        other => Err(MembusError::InvalidArgument(format!(
            "unknown subcommand '{other}'\n\n{}",
            usage_text()
        ))),
    }
}

/// Execute a parsed CLI command on the Tokio runtime.
pub async fn execute(cmd: Command) -> Result<(), MembusError> {
    match cmd {
        Command::Serve(args) => run_serve(*args).await,
        Command::Health(connect) => run_health(*connect).await,
        Command::Checkpoint { connect, dest_dir } => run_checkpoint(*connect, dest_dir).await,
        Command::Diag(diag_cmd) => tokio::task::spawn_blocking(move || run_diag(diag_cmd))
            .await
            .map_err(|err| MembusError::Storage(format!("diag task failed: {err}")))?,
        Command::Help(text) => {
            println!("{text}");
            Ok(())
        }
    }
}

async fn connect_cli_client(connect: &ClientConnectArgs) -> Result<Client, MembusError> {
    let client = match (&connect.endpoint, &connect.tls) {
        (Endpoint::Tcp(addr), Some(tls_cfg)) | (Endpoint::TcpTls(addr), Some(tls_cfg)) => {
            Client::connect_tcp_tls(*addr, tls_cfg).await?
        }
        (ep, None) => Client::connect(ep).await?,
        (_, Some(_)) => {
            return Err(MembusError::InvalidArgument(
                "--tls-ca/--tls-cert/--tls-key can only be used with --tcp <ADDR>".to_owned(),
            ));
        }
    };
    if let Some(token) = &connect.token {
        client.authenticate(token).await?;
    }
    Ok(client)
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

/// Orchestrate bounded, observable graceful shutdown across mesh, transport listeners,
/// secondary Tantivy search flush, and authoritative RocksDB WAL flush.
///
/// Uses a single shared `drain_timeout` budget while reserving a guaranteed durability floor
/// (up to 5s) so network/listener draining can never starve `search.flush_sync()` or
/// `storage.flush_wal(true)` before orchestrator `SIGKILL`. Every step is attempted even if
/// an earlier step fails; all errors are logged explicitly and returned as a combined `Err(...)`
/// so the resident process exits with a non-zero code whenever any drain or durability step fails.
pub async fn shutdown_resident(
    mesh_node: Option<MeshNode>,
    handles: Vec<ServerHandle>,
    search: Option<Arc<SearchEngine>>,
    storage: Arc<dyn StorageEngine>,
    drain_timeout: Duration,
) -> Result<(), MembusError> {
    let mut errors: Vec<String> = Vec::new();
    let total_deadline = Instant::now() + drain_timeout;
    let durability_floor = drain_timeout.min(Duration::from_secs(5));
    let net_deadline = total_deadline
        .checked_sub(durability_floor)
        .unwrap_or_else(Instant::now);

    if let Some(node) = mesh_node {
        let budget = net_deadline.saturating_duration_since(Instant::now());
        match tokio::time::timeout(budget, node.shutdown()).await {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                let msg = format!("mesh shutdown failed: {err}");
                eprintln!("membus shutdown error: {msg}");
                errors.push(msg);
            }
            Err(_) => {
                let msg = format!("mesh shutdown timed out after {budget:?}");
                eprintln!("membus shutdown error: {msg}");
                errors.push(msg);
            }
        }
    }

    for handle in handles {
        let budget = net_deadline.saturating_duration_since(Instant::now());
        if let Err(err) = handle.shutdown_with_timeout(budget).await {
            let msg = format!("listener shutdown failed: {err}");
            eprintln!("membus shutdown error: {msg}");
            errors.push(msg);
        }
    }

    let min_step_floor = (durability_floor / 2).max(Duration::from_millis(250));

    if let Some(search_engine) = search {
        let remaining = total_deadline.saturating_duration_since(Instant::now());
        let budget = remaining
            .saturating_sub(min_step_floor)
            .max(min_step_floor);
        let flush_res = tokio::time::timeout(
            budget,
            tokio::task::spawn_blocking(move || search_engine.flush_sync()),
        )
        .await;
        match flush_res {
            Ok(Ok(Ok(_seq))) => {}
            Ok(Ok(Err(err))) => {
                let msg = format!("search flush failed: {err}");
                eprintln!("membus shutdown error: {msg}");
                errors.push(msg);
            }
            Ok(Err(join_err)) => {
                let msg = format!("search flush task panicked: {join_err}");
                eprintln!("membus shutdown error: {msg}");
                errors.push(msg);
            }
            Err(_) => {
                let msg = format!("search flush timed out after {budget:?}");
                eprintln!("membus shutdown error: {msg}");
                errors.push(msg);
            }
        }
    }

    let wal_budget = total_deadline
        .saturating_duration_since(Instant::now())
        .max(min_step_floor);
    let wal_res = tokio::time::timeout(
        wal_budget,
        tokio::task::spawn_blocking(move || storage.flush_wal(true)),
    )
    .await;
    match wal_res {
        Ok(Ok(Ok(()))) => {}
        Ok(Ok(Err(err))) => {
            let msg = format!("WAL flush failed: {err}");
            eprintln!("membus shutdown error: {msg}");
            errors.push(msg);
        }
        Ok(Err(join_err)) => {
            let msg = format!("WAL flush task panicked: {join_err}");
            eprintln!("membus shutdown error: {msg}");
            errors.push(msg);
        }
        Err(_) => {
            let msg = format!("WAL flush timed out after {wal_budget:?}");
            eprintln!("membus shutdown error: {msg}");
            errors.push(msg);
        }
    }

    if errors.is_empty() {
        Ok(())
    } else {
        Err(MembusError::Storage(format!(
            "resident shutdown failed with {} error(s): {}",
            errors.len(),
            errors.join("; ")
        )))
    }
}

async fn run_serve(args: ServeArgs) -> Result<(), MembusError> {
    let mut storage_cfg = StorageConfig::default();
    if let Some(ops) = args.max_in_flight_ops {
        storage_cfg.admission.max_in_flight_ops = ops;
    }
    if let Some(bytes) = args.max_in_flight_bytes {
        storage_cfg.admission.max_in_flight_bytes = bytes;
    }
    if let Some(snaps) = args.max_active_snapshots {
        storage_cfg.max_active_snapshots = snaps;
    }
    if let Some(timeout_ms) = args.query_timeout_ms {
        storage_cfg.query_timeout = if timeout_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(timeout_ms))
        };
    }

    let mut server_cfg = ServerConfig::default();
    if let Some(per_conn) = args.max_in_flight_per_conn {
        server_cfg.max_in_flight_per_conn = per_conn;
    }
    if let Some(max_http) = args.max_http_in_flight {
        server_cfg.max_http_in_flight = max_http;
    }
    if let Some(max_streams) = args.max_active_streams {
        server_cfg.max_active_streams = max_streams;
    }
    if let Some(timeout_ms) = args.query_timeout_ms {
        server_cfg.query_timeout = if timeout_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(timeout_ms))
        };
    }
    if let Some(ref root) = args.checkpoint_root {
        std::fs::create_dir_all(root)?;
    }
    server_cfg.checkpoint_root = args.checkpoint_root;
    server_cfg.allow_insecure_non_loopback = args.allow_insecure_non_loopback;
    server_cfg.public_health_probe = args.public_health_probe;

    let auth_cfg = if let Some(path) = &args.auth_config {
        let raw = std::fs::read(path)?;
        serde_json::from_slice::<AuthConfig>(&raw).map_err(|err| {
            MembusError::InvalidArgument(format!(
                "invalid auth config JSON '{}': {err}",
                path.display()
            ))
        })?
    } else {
        AuthConfig::disabled()
    };

    let tcp_tls_cfg = match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => Some(TlsServerConfig {
            cert_pem_path: cert.clone(),
            key_pem_path: key.clone(),
            client_ca_pem_path: args.tls_client_ca.clone(),
        }),
        (None, None) => None,
        _ => {
            return Err(MembusError::InvalidArgument(
                "--tls-cert and --tls-key must both be provided together".to_owned(),
            ));
        }
    };

    let http_tls_cfg = match (&args.http_tls_cert, &args.http_tls_key) {
        (Some(cert), Some(key)) => Some(TlsServerConfig {
            cert_pem_path: cert.clone(),
            key_pem_path: key.clone(),
            client_ca_pem_path: args.http_tls_client_ca.clone(),
        }),
        (None, None) => None,
        _ => {
            return Err(MembusError::InvalidArgument(
                "--http-tls-cert and --http-tls-key must both be provided together".to_owned(),
            ));
        }
    };

    // Fail-closed pre-flight security check before opening/locking storage.
    let auth_enabled = auth_cfg.mode() == AuthMode::Enforced;
    if let Some(tcp_addr) = args.tcp {
        validate_listener_bind_security(
            if tcp_tls_cfg.is_some() {
                "binary TCP/TLS"
            } else {
                "binary TCP"
            },
            tcp_addr,
            auth_enabled,
            tcp_tls_cfg.is_some(),
            args.allow_insecure_non_loopback,
        )?;
    }
    if let Some(http_addr) = args.http_listen {
        validate_listener_bind_security(
            if http_tls_cfg.is_some() {
                "HTTPS"
            } else {
                "HTTP"
            },
            http_addr,
            auth_enabled,
            http_tls_cfg.is_some(),
            args.allow_insecure_non_loopback,
        )?;
    }
    if args.mesh_node_id.is_some() {
        validate_mesh_listen_endpoints(&args.mesh_listen, args.allow_insecure_non_loopback)?;
    }

    let signal_cfg = SignalConfig::default();
    let ring = Arc::new(SignalRing::new(signal_cfg));
    let storage = Arc::new(RocksDbStorage::open(
        &args.data_dir,
        storage_cfg,
        Arc::clone(&ring) as Arc<dyn SignalPublisher>,
    )?);
    let broker = Arc::new(SignalBroker::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        ring,
        signal_cfg,
    ));
    let search = Arc::new(SearchEngine::open(
        &args.data_dir,
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        SearchConfig::default(),
    )?);

    let server = MembusServer::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        broker,
        server_cfg,
    )
    .with_auth(auth_cfg)
    .with_search(Arc::clone(&search));

    let mut handles: Vec<ServerHandle> = Vec::new();
    if let Some(tcp_addr) = args.tcp {
        let handle = if let Some(tls) = &tcp_tls_cfg {
            let h = server.bind_tcp_tls(tcp_addr, tls).await?;
            if let Endpoint::TcpTls(bound) = h.endpoint() {
                println!("listening tcp_tls={bound}");
            }
            h
        } else {
            let h = server.bind_tcp(tcp_addr).await?;
            if let Endpoint::Tcp(bound) = h.endpoint() {
                println!("listening tcp={bound}");
            }
            h
        };
        handles.push(handle);
    }
    if let Some(pipe_name) = args.pipe {
        let handle = server.bind_named_pipe(&pipe_name).await?;
        if let Endpoint::NamedPipe(name) = handle.endpoint() {
            println!("listening pipe={name}");
        }
        handles.push(handle);
    }
    if let Some(http_addr) = args.http_listen {
        let handle = if let Some(http_tls) = &http_tls_cfg {
            let h = server.bind_http_tls(http_addr, http_tls).await?;
            if let Endpoint::HttpTls(bound) = h.endpoint() {
                println!("listening https={bound}");
            }
            h
        } else {
            let h = server.bind_http(http_addr).await?;
            if let Endpoint::Http(bound) = h.endpoint() {
                println!("listening http={bound}");
            }
            h
        };
        handles.push(handle);
    }

    let mesh_node = if let Some(node_id) = args.mesh_node_id {
        let mut mesh_cfg = MeshConfig {
            node_id,
            listen_endpoints: args.mesh_listen,
            connect_endpoints: args.mesh_connect,
            allow_insecure_non_loopback: args.allow_insecure_non_loopback,
            ..Default::default()
        };
        if let Some(cluster) = args.mesh_cluster {
            mesh_cfg.cluster_prefix = cluster;
        }
        let node = MeshNode::start(
            mesh_cfg,
            Arc::clone(&storage) as Arc<dyn StorageEngine>,
            Some(Arc::clone(&search)),
        )
        .await?;
        println!("mesh node_id={}", node.node_id());
        Some(node)
    } else {
        None
    };
    let _ = std::io::stdout().flush();

    let signal_name = wait_shutdown_signal().await?;
    eprintln!("membus received {signal_name}; draining listeners and flushing state...");

    shutdown_resident(
        mesh_node,
        handles,
        Some(search),
        storage as Arc<dyn StorageEngine>,
        DEFAULT_SHUTDOWN_DRAIN_TIMEOUT,
    )
    .await
}

async fn run_health(connect: ClientConnectArgs) -> Result<(), MembusError> {
    let client = connect_cli_client(&connect).await?;
    let status = match client.health_v2().await {
        Ok(st) => st,
        Err(MembusError::Protocol(_)) => client.health().await?,
        Err(err) => return Err(err),
    };
    println!(
        "ok epoch={} latest_seq={} log_horizon_seq={} in_flight_ops={} in_flight_bytes={} active_connections={} active_subscriptions={} active_snapshots={} active_streams={} search_ingest_deferred_total={} search_dirty={}",
        status.epoch,
        status.latest_seq,
        status.log_horizon_seq,
        status.in_flight_ops,
        status.in_flight_bytes,
        status.active_connections,
        status.active_subscriptions,
        status.active_snapshots,
        status.active_streams,
        status.search_ingest_deferred_total,
        status.search_dirty,
    );
    Ok(())
}

async fn run_checkpoint(connect: ClientConnectArgs, dest_dir: PathBuf) -> Result<(), MembusError> {
    let client = connect_cli_client(&connect).await?;
    let dest_str = dest_dir.to_string_lossy().into_owned();
    let seq = client.checkpoint(&dest_str).await?;
    println!("checkpoint_seq={seq} dest={dest_str}");
    Ok(())
}

fn parse_serve(args: &[String]) -> Result<Command, MembusError> {
    if args.iter().any(|a| matches!(a.as_str(), "-h" | "--help")) {
        return Ok(Command::Help(serve_usage_text()));
    }
    let mut data_dir: Option<PathBuf> = None;
    let mut tcp: Option<SocketAddr> = None;
    let mut pipe: Option<String> = None;
    let mut http_listen: Option<SocketAddr> = None;
    let mut tls_cert: Option<PathBuf> = None;
    let mut tls_key: Option<PathBuf> = None;
    let mut tls_client_ca: Option<PathBuf> = None;
    let mut http_tls_cert: Option<PathBuf> = None;
    let mut http_tls_key: Option<PathBuf> = None;
    let mut http_tls_client_ca: Option<PathBuf> = None;
    let mut auth_config: Option<PathBuf> = None;
    let mut checkpoint_root: Option<PathBuf> = None;
    let mut max_in_flight_ops: Option<usize> = None;
    let mut max_in_flight_bytes: Option<usize> = None;
    let mut max_in_flight_per_conn: Option<usize> = None;
    let mut max_http_in_flight: Option<usize> = None;
    let mut max_active_streams: Option<usize> = None;
    let mut max_active_snapshots: Option<usize> = None;
    let mut query_timeout_ms: Option<u64> = None;
    let mut allow_insecure_non_loopback = false;
    let mut public_health_probe = false;
    let mut mesh_node_id: Option<String> = None;
    let mut mesh_cluster: Option<String> = None;
    let mut mesh_listen: Vec<String> = Vec::new();
    let mut mesh_connect: Vec<String> = Vec::new();

    let mut idx = 0;
    while idx < args.len() {
        match args[idx].as_str() {
            "--data-dir" => {
                idx += 1;
                data_dir = Some(PathBuf::from(require_arg(args, idx, "--data-dir")?));
            }
            "--tcp" => {
                idx += 1;
                let val = require_arg(args, idx, "--tcp")?;
                tcp = Some(val.parse::<SocketAddr>().map_err(|err| {
                    MembusError::InvalidArgument(format!("invalid --tcp address '{val}': {err}"))
                })?);
            }
            "--pipe" => {
                idx += 1;
                pipe = Some(require_arg(args, idx, "--pipe")?.to_owned());
            }
            "--http-listen" => {
                idx += 1;
                let val = require_arg(args, idx, "--http-listen")?;
                http_listen = Some(val.parse::<SocketAddr>().map_err(|err| {
                    MembusError::InvalidArgument(format!(
                        "invalid --http-listen address '{val}': {err}"
                    ))
                })?);
            }
            "--tls-cert" => {
                idx += 1;
                tls_cert = Some(PathBuf::from(require_arg(args, idx, "--tls-cert")?));
            }
            "--tls-key" => {
                idx += 1;
                tls_key = Some(PathBuf::from(require_arg(args, idx, "--tls-key")?));
            }
            "--tls-client-ca" => {
                idx += 1;
                tls_client_ca = Some(PathBuf::from(require_arg(args, idx, "--tls-client-ca")?));
            }
            "--http-tls-cert" => {
                idx += 1;
                http_tls_cert = Some(PathBuf::from(require_arg(args, idx, "--http-tls-cert")?));
            }
            "--http-tls-key" => {
                idx += 1;
                http_tls_key = Some(PathBuf::from(require_arg(args, idx, "--http-tls-key")?));
            }
            "--http-tls-client-ca" => {
                idx += 1;
                http_tls_client_ca =
                    Some(PathBuf::from(require_arg(args, idx, "--http-tls-client-ca")?));
            }
            "--auth-config" => {
                idx += 1;
                auth_config = Some(PathBuf::from(require_arg(args, idx, "--auth-config")?));
            }
            "--checkpoint-root" => {
                idx += 1;
                checkpoint_root = Some(PathBuf::from(require_arg(args, idx, "--checkpoint-root")?));
            }
            "--max-in-flight-ops" => {
                idx += 1;
                max_in_flight_ops = Some(parse_usize(
                    require_arg(args, idx, "--max-in-flight-ops")?,
                    "--max-in-flight-ops",
                )?);
            }
            "--max-in-flight-bytes" => {
                idx += 1;
                max_in_flight_bytes = Some(parse_usize(
                    require_arg(args, idx, "--max-in-flight-bytes")?,
                    "--max-in-flight-bytes",
                )?);
            }
            "--max-in-flight-per-conn" => {
                idx += 1;
                max_in_flight_per_conn = Some(parse_usize(
                    require_arg(args, idx, "--max-in-flight-per-conn")?,
                    "--max-in-flight-per-conn",
                )?);
            }
            "--max-http-in-flight" => {
                idx += 1;
                max_http_in_flight = Some(parse_usize(
                    require_arg(args, idx, "--max-http-in-flight")?,
                    "--max-http-in-flight",
                )?);
            }
            "--max-active-streams" => {
                idx += 1;
                max_active_streams = Some(parse_usize(
                    require_arg(args, idx, "--max-active-streams")?,
                    "--max-active-streams",
                )?);
            }
            "--max-active-snapshots" => {
                idx += 1;
                max_active_snapshots = Some(parse_usize(
                    require_arg(args, idx, "--max-active-snapshots")?,
                    "--max-active-snapshots",
                )?);
            }
            "--query-timeout-ms" => {
                idx += 1;
                query_timeout_ms = Some(parse_u64(
                    require_arg(args, idx, "--query-timeout-ms")?,
                    "--query-timeout-ms",
                )?);
            }
            "--allow-insecure-non-loopback" => {
                allow_insecure_non_loopback = true;
            }
            "--public-health-probe" => {
                public_health_probe = true;
            }
            "--mesh-node-id" => {
                idx += 1;
                mesh_node_id = Some(require_arg(args, idx, "--mesh-node-id")?.to_owned());
            }
            "--mesh-cluster" => {
                idx += 1;
                mesh_cluster = Some(require_arg(args, idx, "--mesh-cluster")?.to_owned());
            }
            "--mesh-listen" => {
                idx += 1;
                mesh_listen.push(require_arg(args, idx, "--mesh-listen")?.to_owned());
            }
            "--mesh-connect" => {
                idx += 1;
                mesh_connect.push(require_arg(args, idx, "--mesh-connect")?.to_owned());
            }
            other => {
                return Err(MembusError::InvalidArgument(format!(
                    "unexpected serve option '{other}'"
                )));
            }
        }
        idx += 1;
    }

    let data_dir = data_dir.ok_or_else(|| {
        MembusError::InvalidArgument("serve requires --data-dir <PATH>".to_owned())
    })?;
    if tcp.is_none() && pipe.is_none() && http_listen.is_none() {
        return Err(MembusError::InvalidArgument(
            "serve requires at least one of --tcp <ADDR>, --pipe <NAME>, or --http-listen <ADDR>"
                .to_owned(),
        ));
    }
    if tls_cert.is_some() != tls_key.is_some() {
        return Err(MembusError::InvalidArgument(
            "--tls-cert and --tls-key must both be specified together".to_owned(),
        ));
    }
    if tls_client_ca.is_some() && tls_cert.is_none() {
        return Err(MembusError::InvalidArgument(
            "--tls-client-ca requires --tls-cert and --tls-key".to_owned(),
        ));
    }
    if tls_cert.is_some() && tcp.is_none() {
        return Err(MembusError::InvalidArgument(
            "--tls-cert/--tls-key requires --tcp <ADDR>".to_owned(),
        ));
    }
    if http_tls_cert.is_some() != http_tls_key.is_some() {
        return Err(MembusError::InvalidArgument(
            "--http-tls-cert and --http-tls-key must both be specified together".to_owned(),
        ));
    }
    if http_tls_client_ca.is_some() && http_tls_cert.is_none() {
        return Err(MembusError::InvalidArgument(
            "--http-tls-client-ca requires --http-tls-cert and --http-tls-key".to_owned(),
        ));
    }
    if http_tls_cert.is_some() && http_listen.is_none() {
        return Err(MembusError::InvalidArgument(
            "--http-tls-cert/--http-tls-key requires --http-listen <ADDR>".to_owned(),
        ));
    }

    Ok(Command::Serve(Box::new(ServeArgs {
        data_dir,
        tcp,
        pipe,
        http_listen,
        tls_cert,
        tls_key,
        tls_client_ca,
        http_tls_cert,
        http_tls_key,
        http_tls_client_ca,
        auth_config,
        checkpoint_root,
        max_in_flight_ops,
        max_in_flight_bytes,
        max_in_flight_per_conn,
        max_http_in_flight,
        max_active_streams,
        max_active_snapshots,
        query_timeout_ms,
        allow_insecure_non_loopback,
        public_health_probe,
        mesh_node_id,
        mesh_cluster,
        mesh_listen,
        mesh_connect,
    })))
}

#[derive(Default)]
struct ClientFlagsBuilder {
    endpoint: Option<Endpoint>,
    token: Option<String>,
    tls_ca: Option<PathBuf>,
    tls_server_name: Option<String>,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
}

impl ClientFlagsBuilder {
    fn try_parse(&mut self, args: &[String], idx: &mut usize) -> Result<bool, MembusError> {
        match args[*idx].as_str() {
            "--tcp" => {
                *idx += 1;
                let val = require_arg(args, *idx, "--tcp")?;
                let addr = val.parse::<SocketAddr>().map_err(|err| {
                    MembusError::InvalidArgument(format!("invalid --tcp address '{val}': {err}"))
                })?;
                self.set_endpoint(Endpoint::Tcp(addr))?;
                Ok(true)
            }
            "--pipe" => {
                *idx += 1;
                self.set_endpoint(Endpoint::NamedPipe(
                    require_arg(args, *idx, "--pipe")?.to_owned(),
                ))?;
                Ok(true)
            }
            "--token" => {
                *idx += 1;
                self.token = Some(require_arg(args, *idx, "--token")?.to_owned());
                Ok(true)
            }
            "--tls-ca" => {
                *idx += 1;
                self.tls_ca = Some(PathBuf::from(require_arg(args, *idx, "--tls-ca")?));
                Ok(true)
            }
            "--tls-server-name" => {
                *idx += 1;
                self.tls_server_name =
                    Some(require_arg(args, *idx, "--tls-server-name")?.to_owned());
                Ok(true)
            }
            "--tls-cert" => {
                *idx += 1;
                self.tls_cert = Some(PathBuf::from(require_arg(args, *idx, "--tls-cert")?));
                Ok(true)
            }
            "--tls-key" => {
                *idx += 1;
                self.tls_key = Some(PathBuf::from(require_arg(args, *idx, "--tls-key")?));
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    fn set_endpoint(&mut self, ep: Endpoint) -> Result<(), MembusError> {
        if self.endpoint.is_some() {
            return Err(MembusError::InvalidArgument(
                "specify only one of --tcp or --pipe".to_owned(),
            ));
        }
        self.endpoint = Some(ep);
        Ok(())
    }

    fn finish(self, cmd: &str) -> Result<ClientConnectArgs, MembusError> {
        let mut endpoint = self.endpoint.ok_or_else(|| {
            MembusError::InvalidArgument(format!("{cmd} requires --tcp <ADDR> or --pipe <NAME>"))
        })?;
        if self.tls_cert.is_some() != self.tls_key.is_some() {
            return Err(MembusError::InvalidArgument(
                "--tls-cert and --tls-key must both be specified together".to_owned(),
            ));
        }
        if (self.tls_cert.is_some() || self.tls_server_name.is_some()) && self.tls_ca.is_none() {
            return Err(MembusError::InvalidArgument(
                "--tls-cert/--tls-key/--tls-server-name requires --tls-ca <PEM>".to_owned(),
            ));
        }
        let tls = if let Some(ca_pem_path) = self.tls_ca {
            let addr = match endpoint {
                Endpoint::Tcp(a) => a,
                _ => {
                    return Err(MembusError::InvalidArgument(
                        "--tls-ca requires --tcp <ADDR>".to_owned(),
                    ));
                }
            };
            endpoint = Endpoint::TcpTls(addr);
            let server_name = self
                .tls_server_name
                .unwrap_or_else(|| "localhost".to_owned());
            Some(TlsClientConfig {
                ca_pem_path,
                server_name,
                client_cert_pem_path: self.tls_cert,
                client_key_pem_path: self.tls_key,
            })
        } else {
            None
        };
        Ok(ClientConnectArgs {
            endpoint,
            token: self.token,
            tls,
        })
    }
}

fn parse_health(args: &[String]) -> Result<Command, MembusError> {
    if args.iter().any(|a| matches!(a.as_str(), "-h" | "--help")) {
        return Ok(Command::Help(health_usage_text()));
    }
    let mut builder = ClientFlagsBuilder::default();
    let mut idx = 0;
    while idx < args.len() {
        if builder.try_parse(args, &mut idx)? {
            idx += 1;
            continue;
        }
        return Err(MembusError::InvalidArgument(format!(
            "unexpected health option '{}'",
            args[idx]
        )));
    }
    Ok(Command::Health(Box::new(builder.finish("health")?)))
}

fn parse_checkpoint(args: &[String]) -> Result<Command, MembusError> {
    if args.iter().any(|a| matches!(a.as_str(), "-h" | "--help")) {
        return Ok(Command::Help(checkpoint_usage_text()));
    }
    let mut builder = ClientFlagsBuilder::default();
    let mut dest_dir: Option<PathBuf> = None;
    let mut idx = 0;
    while idx < args.len() {
        if builder.try_parse(args, &mut idx)? {
            idx += 1;
            continue;
        }
        match args[idx].as_str() {
            "--dest" => {
                idx += 1;
                dest_dir = Some(PathBuf::from(require_arg(args, idx, "--dest")?));
            }
            other => {
                return Err(MembusError::InvalidArgument(format!(
                    "unexpected checkpoint option '{other}'"
                )));
            }
        }
        idx += 1;
    }

    let connect = builder.finish("checkpoint")?;
    let dest_dir = dest_dir.ok_or_else(|| {
        MembusError::InvalidArgument("checkpoint requires --dest <DIR>".to_owned())
    })?;
    Ok(Command::Checkpoint {
        connect: Box::new(connect),
        dest_dir,
    })
}

fn require_arg<'a>(args: &'a [String], idx: usize, flag: &str) -> Result<&'a str, MembusError> {
    args.get(idx).map(String::as_str).ok_or_else(|| {
        MembusError::InvalidArgument(format!("missing value for option '{flag}'"))
    })
}

fn parse_usize(val: &str, flag: &str) -> Result<usize, MembusError> {
    val.parse::<usize>().map_err(|err| {
        MembusError::InvalidArgument(format!("invalid integer for {flag} ('{val}'): {err}"))
    })
}

fn parse_u64(val: &str, flag: &str) -> Result<u64, MembusError> {
    val.parse::<u64>().map_err(|err| {
        MembusError::InvalidArgument(format!("invalid integer for {flag} ('{val}'): {err}"))
    })
}

fn usage_text() -> &'static str {
    "Usage:\n  membus serve --data-dir <PATH> [--http-listen <ADDR> [--http-tls-cert <PEM> --http-tls-key <PEM> [--http-tls-client-ca <PEM>]]] [--tcp <ADDR> [--tls-cert <PEM> --tls-key <PEM> [--tls-client-ca <PEM>]]] [--pipe <PIPE_NAME>] [--auth-config <JSON>] [--allow-insecure-non-loopback] [--public-health-probe] [--checkpoint-root <DIR>] [--mesh-node-id <ID> [--mesh-cluster <PREFIX>] [--mesh-listen <ENDPOINT>] [--mesh-connect <ENDPOINT>]] [--max-in-flight-ops <N>] [--max-in-flight-bytes <N>] [--max-in-flight-per-conn <N>] [--max-http-in-flight <N>] [--max-active-streams <N>] [--max-active-snapshots <N>] [--query-timeout-ms <N>]\n  membus health (--tcp <ADDR> | --pipe <PIPE_NAME>) [--token <TOKEN>] [--tls-ca <PEM> [--tls-server-name <NAME>] [--tls-cert <PEM> --tls-key <PEM>]]\n  membus checkpoint (--tcp <ADDR> | --pipe <PIPE_NAME>) --dest <RELATIVE_DIR> [--token <TOKEN>] [--tls-ca <PEM> [--tls-server-name <NAME>] [--tls-cert <PEM> --tls-key <PEM>]]\n  membus diag inspect --data-dir <PATH>\n  membus diag verify --data-dir <PATH>\n  membus diag restore --backup-dir <DIR> --target-dir <DIR> [--delta <FILE>...]\n  membus --help | -h | help"
}

fn serve_usage_text() -> &'static str {
    "Usage:\n  membus serve --data-dir <PATH> [--http-listen <ADDR> [--http-tls-cert <PEM> --http-tls-key <PEM> [--http-tls-client-ca <PEM>]]] [--tcp <ADDR> [--tls-cert <PEM> --tls-key <PEM> [--tls-client-ca <PEM>]]] [--pipe <PIPE_NAME>] [--auth-config <JSON>] [--allow-insecure-non-loopback] [--public-health-probe] [--checkpoint-root <DIR>] [--mesh-node-id <ID> [--mesh-cluster <PREFIX>] [--mesh-listen <ENDPOINT>] [--mesh-connect <ENDPOINT>]] [--max-in-flight-ops <N>] [--max-in-flight-bytes <N>] [--max-in-flight-per-conn <N>] [--max-http-in-flight <N>] [--max-active-streams <N>] [--max-active-snapshots <N>] [--query-timeout-ms <N>]"
}

fn health_usage_text() -> &'static str {
    "Usage:\n  membus health (--tcp <ADDR> | --pipe <PIPE_NAME>) [--token <TOKEN>] [--tls-ca <PEM> [--tls-server-name <NAME>] [--tls-cert <PEM> --tls-key <PEM>]]"
}

fn checkpoint_usage_text() -> &'static str {
    "Usage:\n  membus checkpoint (--tcp <ADDR> | --pipe <PIPE_NAME>) --dest <RELATIVE_DIR> [--token <TOKEN>] [--tls-ca <PEM> [--tls-server-name <NAME>] [--tls-cert <PEM> --tls-key <PEM>]]"
}

fn diag_usage_text() -> &'static str {
    "Usage:\n  membus diag inspect --data-dir <PATH>\n  membus diag verify --data-dir <PATH>\n  membus diag restore --backup-dir <DIR> --target-dir <DIR> [--delta <FILE>...]"
}
