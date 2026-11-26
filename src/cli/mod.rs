use std::io::Write;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use crate::client::Client;
use crate::signals::{SignalBroker, SignalConfig, SignalRing};
use crate::storage::{RocksDbStorage, SignalPublisher, StorageConfig, StorageEngine};
use crate::transport::{Endpoint, MembusServer, ServerConfig, ServerHandle};
use crate::types::MembusError;

/// Parsed CLI command for `membus`.
#[derive(Debug, PartialEq, Eq)]
pub enum Command {
    Serve(ServeArgs),
    Health(Endpoint),
    Checkpoint { endpoint: Endpoint, dest_dir: PathBuf },
}

#[derive(Debug, PartialEq, Eq)]
pub struct ServeArgs {
    pub data_dir: PathBuf,
    pub tcp: Option<SocketAddr>,
    pub pipe: Option<String>,
    pub checkpoint_root: Option<PathBuf>,
    pub max_in_flight_ops: Option<usize>,
    pub max_in_flight_bytes: Option<usize>,
    pub max_in_flight_per_conn: Option<usize>,
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
        Command::Serve(args) => run_serve(args).await,
        Command::Health(endpoint) => run_health(endpoint).await,
        Command::Checkpoint { endpoint, dest_dir } => run_checkpoint(endpoint, dest_dir).await,
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

    let mut server_cfg = ServerConfig::default();
    if let Some(per_conn) = args.max_in_flight_per_conn {
        server_cfg.max_in_flight_per_conn = per_conn;
    }
    server_cfg.checkpoint_root = args.checkpoint_root;

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

    let server = MembusServer::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        broker,
        server_cfg,
    );

    let mut handles: Vec<ServerHandle> = Vec::new();
    if let Some(tcp_addr) = args.tcp {
        let handle = server.bind_tcp(tcp_addr).await?;
        if let Endpoint::Tcp(bound) = handle.endpoint() {
            println!("listening tcp={bound}");
        }
        handles.push(handle);
    }
    if let Some(pipe_name) = args.pipe {
        let handle = server.bind_named_pipe(&pipe_name).await?;
        if let Endpoint::NamedPipe(name) = handle.endpoint() {
            println!("listening pipe={name}");
        }
        handles.push(handle);
    }
    let _ = std::io::stdout().flush();

    tokio::signal::ctrl_c().await?;

    for handle in handles {
        handle.shutdown().await;
    }
    storage.flush_wal(true)?;
    Ok(())
}

async fn run_health(endpoint: Endpoint) -> Result<(), MembusError> {
    let client = Client::connect(&endpoint).await?;
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

async fn run_checkpoint(endpoint: Endpoint, dest_dir: PathBuf) -> Result<(), MembusError> {
    let client = Client::connect(&endpoint).await?;
    let dest_str = dest_dir.to_string_lossy().into_owned();
    let seq = client.checkpoint(&dest_str).await?;
    println!("checkpoint_seq={seq} dest={dest_str}");
    Ok(())
}

fn parse_serve(args: &[String]) -> Result<Command, MembusError> {
    let mut data_dir: Option<PathBuf> = None;
    let mut tcp: Option<SocketAddr> = None;
    let mut pipe: Option<String> = None;
    let mut checkpoint_root: Option<PathBuf> = None;
    let mut max_in_flight_ops: Option<usize> = None;
    let mut max_in_flight_bytes: Option<usize> = None;
    let mut max_in_flight_per_conn: Option<usize> = None;

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
    if tcp.is_none() && pipe.is_none() {
        return Err(MembusError::InvalidArgument(
            "serve requires at least one of --tcp <ADDR> or --pipe <NAME>".to_owned(),
        ));
    }

    Ok(Command::Serve(ServeArgs {
        data_dir,
        tcp,
        pipe,
        checkpoint_root,
        max_in_flight_ops,
        max_in_flight_bytes,
        max_in_flight_per_conn,
    }))
}

fn parse_health(args: &[String]) -> Result<Command, MembusError> {
    let endpoint = parse_single_endpoint(args, "health")?;
    Ok(Command::Health(endpoint))
}

fn parse_checkpoint(args: &[String]) -> Result<Command, MembusError> {
    let mut endpoint: Option<Endpoint> = None;
    let mut dest_dir: Option<PathBuf> = None;
    let mut idx = 0;
    while idx < args.len() {
        if try_parse_endpoint_flag(args, &mut idx, &mut endpoint)? {
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

    let endpoint = endpoint.ok_or_else(|| {
        MembusError::InvalidArgument("checkpoint requires --tcp <ADDR> or --pipe <NAME>".to_owned())
    })?;
    let dest_dir = dest_dir.ok_or_else(|| {
        MembusError::InvalidArgument("checkpoint requires --dest <DIR>".to_owned())
    })?;
    Ok(Command::Checkpoint { endpoint, dest_dir })
}

fn parse_single_endpoint(args: &[String], cmd: &str) -> Result<Endpoint, MembusError> {
    let mut endpoint: Option<Endpoint> = None;
    let mut idx = 0;
    while idx < args.len() {
        if try_parse_endpoint_flag(args, &mut idx, &mut endpoint)? {
            idx += 1;
            continue;
        }
        return Err(MembusError::InvalidArgument(format!(
            "unexpected {cmd} option '{}'",
            args[idx]
        )));
    }
    endpoint.ok_or_else(|| {
        MembusError::InvalidArgument(format!("{cmd} requires --tcp <ADDR> or --pipe <NAME>"))
    })
}

fn try_parse_endpoint_flag(
    args: &[String],
    idx: &mut usize,
    endpoint: &mut Option<Endpoint>,
) -> Result<bool, MembusError> {
    let parsed = match args[*idx].as_str() {
        "--tcp" => {
            *idx += 1;
            let val = require_arg(args, *idx, "--tcp")?;
            let addr = val.parse::<SocketAddr>().map_err(|err| {
                MembusError::InvalidArgument(format!("invalid --tcp address '{val}': {err}"))
            })?;
            Endpoint::Tcp(addr)
        }
        "--pipe" => {
            *idx += 1;
            Endpoint::NamedPipe(require_arg(args, *idx, "--pipe")?.to_owned())
        }
        _ => return Ok(false),
    };
    if endpoint.is_some() {
        return Err(MembusError::InvalidArgument(
            "specify only one of --tcp or --pipe".to_owned(),
        ));
    }
    *endpoint = Some(parsed);
    Ok(true)
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
    "Usage:\n  membus serve --data-dir <PATH> [--tcp <ADDR>] [--pipe <PIPE_NAME>] [--checkpoint-root <DIR>] [--max-in-flight-ops <N>] [--max-in-flight-bytes <N>] [--max-in-flight-per-conn <N>]\n  membus health (--tcp <ADDR> | --pipe <PIPE_NAME>)\n  membus checkpoint (--tcp <ADDR> | --pipe <PIPE_NAME>) --dest <RELATIVE_DIR>"
}
