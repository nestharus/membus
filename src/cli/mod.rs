pub mod diag;

use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use crate::auth::AuthConfig;
use crate::cli::diag::{parse_diag_args, run_diag, DiagCommand};
use crate::client::Client;
use crate::mesh::{MeshConfig, MeshNode};
use crate::search::{SearchConfig, SearchEngine};
use crate::signals::{SignalBroker, SignalConfig, SignalRing};
use crate::storage::{RocksDbStorage, SignalPublisher, StorageConfig, StorageEngine};
use crate::transport::{
    Endpoint, MembusServer, ServerConfig, ServerHandle, TlsClientConfig, TlsServerConfig,
};
use crate::types::MembusError;

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
    pub auth_config: Option<PathBuf>,
    pub checkpoint_root: Option<PathBuf>,
    pub max_in_flight_ops: Option<usize>,
    pub max_in_flight_bytes: Option<usize>,
    pub max_in_flight_per_conn: Option<usize>,
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
        "serve" => parse_serve(&raw[1..]),
        "health" => parse_health(&raw[1..]),
        "checkpoint" => parse_checkpoint(&raw[1..]),
        "diag" => Ok(Command::Diag(parse_diag_args(&raw[1..])?)),
        "-h" | "--help" | "help" => Err(MembusError::InvalidArgument(usage_text().to_owned())),
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

async fn run_serve(args: ServeArgs) -> Result<(), MembusError> {
    let mut storage_cfg = StorageConfig::default();
    if let Some(ops) = args.max_in_flight_ops {
        storage_cfg.admission.max_in_flight_ops = ops;
    }
    if let Some(bytes) = args.max_in_flight_bytes {
        storage_cfg.admission.max_in_flight_bytes = bytes;
    }

    let mut server_cfg = ServerConfig::default();
    if let Some(per_conn) = args.max_in_flight_per_conn {
        server_cfg.max_in_flight_per_conn = per_conn;
    }
    server_cfg.checkpoint_root = args.checkpoint_root;

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

    let tls_cfg = match (&args.tls_cert, &args.tls_key) {
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
        let handle = if let Some(tls) = &tls_cfg {
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
        let handle = server.bind_http(http_addr).await?;
        if let Endpoint::Http(bound) = handle.endpoint() {
            println!("listening http={bound}");
        }
        handles.push(handle);
    }

    let mesh_node = if let Some(node_id) = args.mesh_node_id {
        let mut mesh_cfg = MeshConfig {
            node_id,
            listen_endpoints: args.mesh_listen,
            connect_endpoints: args.mesh_connect,
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

    tokio::signal::ctrl_c().await?;

    if let Some(node) = mesh_node {
        let _ = node.shutdown().await;
    }
    for handle in handles {
        handle.shutdown().await;
    }
    let _ = search.flush_sync();
    storage.flush_wal(true)?;
    Ok(())
}

async fn run_health(connect: ClientConnectArgs) -> Result<(), MembusError> {
    let client = connect_cli_client(&connect).await?;
    let status = client.health().await?;
    println!(
        "ok epoch={} latest_seq={} log_horizon_seq={} in_flight_ops={} in_flight_bytes={} active_connections={} active_subscriptions={}",
        status.epoch,
        status.latest_seq,
        status.log_horizon_seq,
        status.in_flight_ops,
        status.in_flight_bytes,
        status.active_connections,
        status.active_subscriptions
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
    let mut data_dir: Option<PathBuf> = None;
    let mut tcp: Option<SocketAddr> = None;
    let mut pipe: Option<String> = None;
    let mut http_listen: Option<SocketAddr> = None;
    let mut tls_cert: Option<PathBuf> = None;
    let mut tls_key: Option<PathBuf> = None;
    let mut tls_client_ca: Option<PathBuf> = None;
    let mut auth_config: Option<PathBuf> = None;
    let mut checkpoint_root: Option<PathBuf> = None;
    let mut max_in_flight_ops: Option<usize> = None;
    let mut max_in_flight_bytes: Option<usize> = None;
    let mut max_in_flight_per_conn: Option<usize> = None;
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

    Ok(Command::Serve(Box::new(ServeArgs {
        data_dir,
        tcp,
        pipe,
        http_listen,
        tls_cert,
        tls_key,
        tls_client_ca,
        auth_config,
        checkpoint_root,
        max_in_flight_ops,
        max_in_flight_bytes,
        max_in_flight_per_conn,
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

fn usage_text() -> &'static str {
    "Usage:\n  membus serve --data-dir <PATH> [--http-listen <ADDR>] [--tcp <ADDR>] [--pipe <PIPE_NAME>] [--tls-cert <PEM> --tls-key <PEM> [--tls-client-ca <PEM>]] [--auth-config <JSON>] [--checkpoint-root <DIR>] [--mesh-node-id <ID> [--mesh-cluster <PREFIX>] [--mesh-listen <ENDPOINT>] [--mesh-connect <ENDPOINT>]] [--max-in-flight-ops <N>] [--max-in-flight-bytes <N>] [--max-in-flight-per-conn <N>]\n  membus health (--tcp <ADDR> | --pipe <PIPE_NAME>) [--token <TOKEN>] [--tls-ca <PEM> [--tls-server-name <NAME>] [--tls-cert <PEM> --tls-key <PEM>]]\n  membus checkpoint (--tcp <ADDR> | --pipe <PIPE_NAME>) --dest <RELATIVE_DIR> [--token <TOKEN>] [--tls-ca <PEM> [--tls-server-name <NAME>] [--tls-cert <PEM> --tls-key <PEM>]]\n  membus diag inspect --data-dir <PATH>\n  membus diag verify --data-dir <PATH>\n  membus diag restore --backup-dir <DIR> --target-dir <DIR> [--delta <FILE>...]"
}
