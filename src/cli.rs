//! Pure admission for the binary's existing command forms.
use membus::{Config, MAX_ID};
use std::{io, net::SocketAddr, path::PathBuf};

const USAGE: &str = concat!(
    "usage: membus STATE_DIR tcp|pipe ADDRESS",
    " [--idle-ms N] [--heartbeat-ms N] [--state-limit-bytes N]",
    " [--flush-ms N] [--retry-ms N] [--quota N] [--index-cap N]",
    " [--capacity N] [--cache-bytes N] [--queue-bytes N] [--dedup-cap N]",
    " [--delay-ms N] [--allow-mem] [--snapshot-bytes N] [--snapshot-max N]",
    " [--snapshot-ttl-ms N] [--maintenance-reserve-bytes N] [--chunk-bytes N]",
    " [--rotation-ms N] [--retire-ms N] [--signal-history N] [--signal-bytes N]",
    " [--cache-policy NS,PREFIX,CLASS,LABEL,INITIAL_MS,ACTIVE_MS,REFRESH,NEVER]",
    "; membus health PIPE --state-dir PATH",
    "; membus policy PIPE get NS|set NS PREFIX forever|latest|expire:MS|clear NS PREFIX",
    "; membus expiry resume STATE_DIR; membus retirement-status PIPE",
    "; membus catalogue-state STOPPED_STATE_DIR ABSOLUTE_CHUNK_PATH",
);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Admin {
    Retirement,
    Catalogue,
    Health,
    Policy,
    Expiry,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Transport {
    Tcp,
    #[cfg(windows)]
    Pipe,
}

pub(super) struct Server {
    pub(super) path: PathBuf,
    pub(super) transport: Transport,
    pub(super) address: String,
    pub(super) config: Config,
    pub(super) idle_ms: u32,
    pub(super) heartbeat_ms: u64,
}

pub(super) enum Command<'a> {
    Admin(Admin, &'a [String]),
    Server(Server),
}

// The production adapter and resource-spy tests use this same boundary. No
// effect adapter is invoked until the complete server command has been admitted.
pub(super) fn dispatch<'a, T>(
    args: &'a [String],
    execute: impl FnOnce(Command<'a>) -> io::Result<T>,
) -> io::Result<T> {
    execute(parse(args)?)
}

fn parse(args: &[String]) -> io::Result<Command<'_>> {
    let admin = match args.get(1).map(String::as_str) {
        Some("retirement-status") => Some(Admin::Retirement),
        Some("catalogue-state") => Some(Admin::Catalogue),
        Some("health") => Some(Admin::Health),
        Some("policy") => Some(Admin::Policy),
        Some("expiry") => Some(Admin::Expiry),
        _ => None,
    };
    if let Some(admin) = admin {
        // Administrative arity/platform checks stay with their existing owners.
        return Ok(Command::Admin(admin, &args[2..]));
    }
    if args.len() < 4 {
        return Err(io::Error::other(USAGE));
    }
    let mut config = Config::default();
    let mut idle_ms = 60000u32;
    let mut heartbeat_ms = 30000u64;
    let mut reserve = None;
    let mut i = 4;
    while i < args.len() {
        if args[i] == "--cache-policy" {
            let text = args
                .get(i + 1)
                .ok_or_else(|| io::Error::other("missing cache policy"))?;
            config.cache_rules.push(text.parse()?);
            i += 2;
            continue;
        }
        if args[i] == "--allow-mem" {
            config.allow_mem = true;
            i += 1;
            continue;
        }
        let n = args
            .get(i + 1)
            .ok_or_else(|| io::Error::other("missing value"))?
            .parse::<u64>()
            .map_err(io::Error::other)?;
        match args[i].as_str() {
            "--signal-history" => {
                config.signal_history = usize::try_from(n).map_err(io::Error::other)?
            }
            "--signal-bytes" => {
                config.signal_bytes = usize::try_from(n).map_err(io::Error::other)?
            }
            "--state-limit-bytes" => {
                if n == 0 {
                    return Err(io::Error::other("state-limit-bytes must be positive"));
                }
                config.state_limit_bytes = n;
            }
            "--idle-ms" => {
                idle_ms = u32::try_from(n)
                    .ok()
                    .filter(|n| *n > 0 && *n < u32::MAX)
                    .ok_or_else(|| io::Error::other("idle-ms must be 1..4294967294"))?
            }
            "--heartbeat-ms" => {
                if n != 0 && n < 100 {
                    return Err(io::Error::other("heartbeat-ms must be zero or >=100"));
                }
                heartbeat_ms = n;
            }
            "--flush-ms" => config.flush_ms = n,
            "--retry-ms" => config.retry_ms = n,
            "--quota" => config.quota = n as usize,
            "--index-cap" => config.index_cap = n as usize,
            "--capacity" => config.capacity = n as usize,
            "--cache-bytes" => config.cache_bytes = usize::try_from(n).map_err(io::Error::other)?,
            "--queue-bytes" => config.queue_bytes = n as usize,
            "--dedup-cap" => config.dedup_cap = n as usize,
            "--delay-ms" => config.write_delay_ms = n,
            "--snapshot-bytes" => {
                config.snapshot_bytes = usize::try_from(n).map_err(io::Error::other)?
            }
            "--snapshot-max" => {
                config.snapshot_max = usize::try_from(n).map_err(io::Error::other)?
            }
            "--snapshot-ttl-ms" => config.snapshot_ttl_ms = n,
            "--maintenance-reserve-bytes" => reserve = Some(n),
            "--chunk-bytes" => {
                if n == 0 {
                    return Err(io::Error::other("chunk-bytes must be positive"));
                }
                config.chunk_bytes = n;
            }
            "--rotation-ms" => config.rotation_ms = n,
            "--retire-ms" => config.retire_ms = n,
            _ => return Err(io::Error::other("unknown option")),
        }
        i += 2
    }

    // Keep the executable's reserve default and final-value relationship. In
    // particular, a scalar repeat does not change eager per-occurrence checks.
    config.maintenance_reserve_bytes =
        reserve.unwrap_or_else(|| (1u64 << 30).min(config.state_limit_bytes / 8));
    if config.maintenance_reserve_bytes >= config.state_limit_bytes {
        return Err(io::Error::other(
            "maintenance-reserve-bytes must be below state-limit-bytes",
        ));
    }
    // These are the existing Store guards, repeated at the binary's admission
    // boundary so invalid commands cannot even inspect Store marker paths.
    // The public library retains its independent checks for embedded callers.
    if config.cache_rules.len() > 64
        || config.cache_rules.iter().any(|r| {
            r.label.is_empty()
                || r.label.len() > MAX_ID
                || r.label.contains(['=', '\n', '\r'])
                || r.prefix.len() > MAX_ID
                || r.namespace.as_ref().is_some_and(|ns| ns.len() > MAX_ID)
        })
    {
        return Err(io::Error::other(
            "invalid cache rules or more than 64 rules",
        ));
    }
    if config.snapshot_ttl_ms == 0 {
        return Err(io::Error::other("snapshot lifetime must be positive"));
    }
    let transport = match args[2].as_str() {
        "tcp" => {
            validate_tcp(&args[3])?;
            Transport::Tcp
        }
        #[cfg(windows)]
        "pipe" => Transport::Pipe,
        _ => return Err(io::Error::other("unsupported transport")),
    };
    Ok(Command::Server(Server {
        path: PathBuf::from(&args[1]),
        transport,
        address: args[3].clone(),
        config,
        idle_ms,
        heartbeat_ms,
    }))
}

fn validate_tcp(address: &str) -> io::Result<()> {
    if let Ok(literal) = address.parse::<SocketAddr>() {
        if !literal.ip().is_loopback() {
            return Err(io::Error::other("TCP must bind loopback"));
        }
    } else {
        // Match std's default string lookup's *syntax* checks without calling
        // ToSocketAddrs or resolving a name. Leave the original address intact:
        // names, IPv6 scopes, resolution and availability remain native work.
        // SGX supplies a different string lookup; keep its delegated grammar.
        #[cfg(not(all(target_vendor = "fortanix", target_env = "sgx")))]
        {
            let (_, port) = address.rsplit_once(':').ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "invalid socket address")
            })?;
            port.parse::<u16>()
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "invalid port value"))?;
        }
    }
    // The existing transport still checks the actual bound address. This is
    // neither a resolution result nor authority to bind or serve.
    Ok(())
}

#[cfg(test)]
mod tests;
