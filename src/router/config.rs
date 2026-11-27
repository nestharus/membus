//! Configuration, CLI parsing, and fail-closed bind security validation for `membus-ab-router`.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::protocol::DEFAULT_MAX_FRAME_BODY;
use crate::types::MembusError;

/// Default bounded drain timeout during graceful router shutdown.
pub const DEFAULT_ROUTER_DRAIN_TIMEOUT: Duration = Duration::from_secs(25);

/// Default maximum queued operations in the bounded asynchronous shadow queue.
pub const DEFAULT_SHADOW_MAX_OPS: usize = 4096;

/// Default maximum queued payload bytes in the bounded asynchronous shadow queue (64 MiB).
pub const DEFAULT_SHADOW_MAX_BYTES: usize = 64 * 1024 * 1024;

/// Default per-operation shadow apply timeout in milliseconds.
pub const DEFAULT_SHADOW_TIMEOUT_MS: u64 = 2000;

/// Default periodic background catch-up retry interval in milliseconds.
pub const DEFAULT_SHADOW_CATCHUP_INTERVAL_MS: u64 = 5000;

/// Default maximum exponential backoff for shadow delivery errors in milliseconds.
pub const DEFAULT_SHADOW_MAX_BACKOFF_MS: u64 = 5000;

/// Default synchronous timeout for primary slot forwarding in milliseconds.
pub const DEFAULT_PRIMARY_TIMEOUT_MS: u64 = 15_000;

/// Default maximum concurrent in-flight data-plane requests handled by the router.
pub const DEFAULT_MAX_IN_FLIGHT: usize = 1024;

/// Explicit dual-slot identity managed by `membus-ab-router`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SlotId {
    #[default]
    #[serde(rename = "slot-a")]
    SlotA,
    #[serde(rename = "slot-b")]
    SlotB,
}

impl SlotId {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::SlotA => "slot-a",
            Self::SlotB => "slot-b",
        }
    }

    pub fn other(self) -> Self {
        match self {
            Self::SlotA => Self::SlotB,
            Self::SlotB => Self::SlotA,
        }
    }

    pub fn parse(raw: &str) -> Result<Self, MembusError> {
        let trimmed = raw.trim();
        if trimmed.eq_ignore_ascii_case("slot-a") || trimmed.eq_ignore_ascii_case("a") {
            Ok(Self::SlotA)
        } else if trimmed.eq_ignore_ascii_case("slot-b") || trimmed.eq_ignore_ascii_case("b") {
            Ok(Self::SlotB)
        } else {
            Err(MembusError::InvalidArgument(format!(
                "invalid slot '{raw}' (expected 'slot-a', 'slot-b', or 'auto')"
            )))
        }
    }

    pub fn route_origin(self) -> String {
        format!("router:{}", self.as_str())
    }
}

impl fmt::Display for SlotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Connection and credential settings for a single upstream Membus slot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SlotEndpointConfig {
    pub url: String,
    pub token_file: Option<PathBuf>,
    pub inline_token: Option<String>,
    pub tls_ca_file: Option<PathBuf>,
    pub tls_cert_file: Option<PathBuf>,
    pub tls_key_file: Option<PathBuf>,
    pub tls_server_name: Option<String>,
}

impl SlotEndpointConfig {
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            token_file: None,
            inline_token: None,
            tls_ca_file: None,
            tls_cert_file: None,
            tls_key_file: None,
            tls_server_name: None,
        }
    }

    pub fn has_auth_configured(&self) -> bool {
        self.token_file.is_some()
            || self
                .inline_token
                .as_deref()
                .is_some_and(|t| !t.trim().is_empty())
    }
}

/// Capacity, timeout, and retry bounds for the asynchronous shadow write-tee queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ShadowQueueConfig {
    pub max_ops: usize,
    pub max_bytes: usize,
    pub timeout_ms: u64,
    pub catchup_interval_ms: u64,
    pub max_backoff_ms: u64,
}

impl Default for ShadowQueueConfig {
    fn default() -> Self {
        Self {
            max_ops: DEFAULT_SHADOW_MAX_OPS,
            max_bytes: DEFAULT_SHADOW_MAX_BYTES,
            timeout_ms: DEFAULT_SHADOW_TIMEOUT_MS,
            catchup_interval_ms: DEFAULT_SHADOW_CATCHUP_INTERVAL_MS,
            max_backoff_ms: DEFAULT_SHADOW_MAX_BACKOFF_MS,
        }
    }
}

/// Complete runtime configuration for [`crate::router::AbRouterServer`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RouterConfig {
    pub data_listen: SocketAddr,
    pub admin_listen: SocketAddr,
    pub slot_a: SlotEndpointConfig,
    pub slot_b: SlotEndpointConfig,
    pub initial_primary: SlotId,
    /// When `true`, startup MUST discover the single `Active` slot from `/v2/health`
    /// on both slots (failing closed if 0 or 2 slots report `Active`).
    pub primary_auto: bool,
    pub shadow_enabled: bool,
    pub admin_token_file: Option<PathBuf>,
    pub admin_inline_token: Option<String>,
    pub shadow: ShadowQueueConfig,
    pub primary_timeout_ms: u64,
    pub max_body_bytes: usize,
    pub max_in_flight: usize,
    pub allow_insecure_non_loopback: bool,
    pub shutdown_drain_timeout: Duration,
}

impl Default for RouterConfig {
    fn default() -> Self {
        Self {
            data_listen: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8080),
            admin_listen: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 8081),
            slot_a: SlotEndpointConfig::new("http://127.0.0.1:18080"),
            slot_b: SlotEndpointConfig::new("http://127.0.0.1:18081"),
            initial_primary: SlotId::SlotA,
            primary_auto: false,
            shadow_enabled: true,
            admin_token_file: None,
            admin_inline_token: None,
            shadow: ShadowQueueConfig::default(),
            primary_timeout_ms: DEFAULT_PRIMARY_TIMEOUT_MS,
            max_body_bytes: DEFAULT_MAX_FRAME_BODY,
            max_in_flight: DEFAULT_MAX_IN_FLIGHT,
            allow_insecure_non_loopback: false,
            shutdown_drain_timeout: DEFAULT_ROUTER_DRAIN_TIMEOUT,
        }
    }
}

impl RouterConfig {
    pub fn has_admin_auth_configured(&self) -> bool {
        self.admin_token_file.is_some()
            || self
                .admin_inline_token
                .as_deref()
                .is_some_and(|t| !t.trim().is_empty())
    }
}

/// Parsed CLI command for `membus-ab-router`.
#[derive(Debug, PartialEq, Eq)]
pub enum RouterCliCommand {
    Run(Box<RouterConfig>),
    Help(&'static str),
}

/// Usage help text printed to stdout with exit code 0 on `--help`, `-h`, or `help`.
pub fn router_usage_text() -> &'static str {
    "\
membus-ab-router — Primary-isolated multi-route A/B write-tee gateway for Membus v2

USAGE:
    membus-ab-router [OPTIONS]
    membus-ab-router --help | -h | help

OPTIONS:
    --data-listen <ADDR>                Data-plane listen address (default: 127.0.0.1:8080)
    --admin-listen <ADDR>               Admin control-plane listen address (default: 127.0.0.1:8081)
    --slot-a-url <URL>                  Upstream Slot A base URL (http:// or https://)
    --slot-b-url <URL>                  Upstream Slot B base URL (http:// or https://)
    --primary <auto|slot-a|slot-b>      Initial active primary slot or 'auto' discovery (default: slot-a)
    --shadow-enabled[=<true|false>]     Enable asynchronous shadow write-teeing (default: true)
    --shadow-disabled                   Disable asynchronous shadow write-teeing
    --slot-a-token-file <PATH>          Bearer token file for Slot A control-plane/shadow calls
    --slot-b-token-file <PATH>          Bearer token file for Slot B control-plane/shadow calls
    --admin-token-file <PATH>           Bearer token file protecting admin endpoints
    --slot-a-tls-ca <PATH>              CA bundle PEM for HTTPS Slot A verification
    --slot-b-tls-ca <PATH>              CA bundle PEM for HTTPS Slot B verification
    --slot-a-tls-cert <PATH>            Optional mTLS client certificate PEM for Slot A
    --slot-a-tls-key <PATH>             Optional mTLS client private key PEM for Slot A
    --slot-b-tls-cert <PATH>            Optional mTLS client certificate PEM for Slot B
    --slot-b-tls-key <PATH>             Optional mTLS client private key PEM for Slot B
    --shadow-max-ops <N>                Maximum queued shadow operations (default: 4096)
    --shadow-max-bytes <N>              Maximum queued shadow payload bytes (default: 67108864)
    --shadow-timeout-ms <MS>            Per-request shadow apply timeout in ms (default: 2000)
    --shadow-catchup-interval-ms <MS>   Background catch-up retry interval in ms (default: 5000)
    --max-in-flight <N>                 Maximum concurrent data-plane requests (default: 1024)
    --allow-insecure-non-loopback       Explicit override allowing non-loopback binds without auth
    -h, --help                          Print this help message and exit 0"
}

/// Parse CLI arguments (excluding `argv[0]`) with optional environment variable fallbacks.
pub fn parse_router_args<I, S>(args: I) -> Result<RouterCliCommand, MembusError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let raw: Vec<String> = args.into_iter().map(|s| s.as_ref().to_owned()).collect();
    if raw
        .first()
        .is_some_and(|a| matches!(a.as_str(), "-h" | "--help" | "help"))
        || raw.iter().any(|a| matches!(a.as_str(), "-h" | "--help"))
    {
        return Ok(RouterCliCommand::Help(router_usage_text()));
    }

    let mut cfg = RouterConfig::default();
    let mut slot_a_set = false;
    let mut slot_b_set = false;
    apply_env_defaults(&mut cfg, &mut slot_a_set, &mut slot_b_set)?;

    let mut idx = 0;
    while idx < raw.len() {
        let arg = raw[idx].as_str();
        if let Some(val) = arg.strip_prefix("--shadow-enabled=") {
            cfg.shadow_enabled = parse_bool_flag(val, "--shadow-enabled")?;
            idx += 1;
            continue;
        }
        match arg {
            "--data-listen" => {
                idx += 1;
                cfg.data_listen =
                    parse_addr(require_val(&raw, idx, "--data-listen")?, "--data-listen")?;
            }
            "--admin-listen" => {
                idx += 1;
                cfg.admin_listen =
                    parse_addr(require_val(&raw, idx, "--admin-listen")?, "--admin-listen")?;
            }
            "--slot-a-url" => {
                idx += 1;
                cfg.slot_a.url = require_val(&raw, idx, "--slot-a-url")?.to_owned();
                slot_a_set = true;
            }
            "--slot-b-url" => {
                idx += 1;
                cfg.slot_b.url = require_val(&raw, idx, "--slot-b-url")?.to_owned();
                slot_b_set = true;
            }
            "--primary" => {
                idx += 1;
                parse_primary_setting(require_val(&raw, idx, "--primary")?, &mut cfg)?;
            }
            "--shadow-enabled" => {
                if let Some(next) = raw.get(idx + 1) {
                    if !next.starts_with("--") {
                        cfg.shadow_enabled = parse_bool_flag(next, "--shadow-enabled")?;
                        idx += 1;
                    } else {
                        cfg.shadow_enabled = true;
                    }
                } else {
                    cfg.shadow_enabled = true;
                }
            }
            "--shadow-disabled" => {
                cfg.shadow_enabled = false;
            }
            "--slot-a-token-file" => {
                idx += 1;
                cfg.slot_a.token_file = opt_path(require_val(&raw, idx, "--slot-a-token-file")?);
            }
            "--slot-b-token-file" => {
                idx += 1;
                cfg.slot_b.token_file = opt_path(require_val(&raw, idx, "--slot-b-token-file")?);
            }
            "--admin-token-file" => {
                idx += 1;
                cfg.admin_token_file = opt_path(require_val(&raw, idx, "--admin-token-file")?);
            }
            "--slot-a-tls-ca" => {
                idx += 1;
                cfg.slot_a.tls_ca_file = opt_path(require_val(&raw, idx, "--slot-a-tls-ca")?);
            }
            "--slot-b-tls-ca" => {
                idx += 1;
                cfg.slot_b.tls_ca_file = opt_path(require_val(&raw, idx, "--slot-b-tls-ca")?);
            }
            "--slot-a-tls-cert" => {
                idx += 1;
                cfg.slot_a.tls_cert_file = opt_path(require_val(&raw, idx, "--slot-a-tls-cert")?);
            }
            "--slot-a-tls-key" => {
                idx += 1;
                cfg.slot_a.tls_key_file = opt_path(require_val(&raw, idx, "--slot-a-tls-key")?);
            }
            "--slot-b-tls-cert" => {
                idx += 1;
                cfg.slot_b.tls_cert_file = opt_path(require_val(&raw, idx, "--slot-b-tls-cert")?);
            }
            "--slot-b-tls-key" => {
                idx += 1;
                cfg.slot_b.tls_key_file = opt_path(require_val(&raw, idx, "--slot-b-tls-key")?);
            }
            "--shadow-max-ops" => {
                idx += 1;
                cfg.shadow.max_ops = parse_positive_usize(
                    require_val(&raw, idx, "--shadow-max-ops")?,
                    "--shadow-max-ops",
                )?;
            }
            "--shadow-max-bytes" => {
                idx += 1;
                cfg.shadow.max_bytes = parse_positive_usize(
                    require_val(&raw, idx, "--shadow-max-bytes")?,
                    "--shadow-max-bytes",
                )?;
            }
            "--shadow-timeout-ms" => {
                idx += 1;
                cfg.shadow.timeout_ms = parse_positive_u64(
                    require_val(&raw, idx, "--shadow-timeout-ms")?,
                    "--shadow-timeout-ms",
                )?;
            }
            "--shadow-catchup-interval-ms" => {
                idx += 1;
                cfg.shadow.catchup_interval_ms = parse_positive_u64(
                    require_val(&raw, idx, "--shadow-catchup-interval-ms")?,
                    "--shadow-catchup-interval-ms",
                )?;
            }
            "--max-in-flight" => {
                idx += 1;
                cfg.max_in_flight = parse_positive_usize(
                    require_val(&raw, idx, "--max-in-flight")?,
                    "--max-in-flight",
                )?;
            }
            "--allow-insecure-non-loopback" => {
                cfg.allow_insecure_non_loopback = true;
            }
            other => {
                return Err(MembusError::InvalidArgument(format!(
                    "unknown option '{other}'\n\n{}",
                    router_usage_text()
                )));
            }
        }
        idx += 1;
    }

    if !slot_a_set {
        return Err(MembusError::InvalidArgument(
            "missing required --slot-a-url <URL> (or AB_SLOT_A_URL)".to_owned(),
        ));
    }
    if !slot_b_set {
        return Err(MembusError::InvalidArgument(
            "missing required --slot-b-url <URL> (or AB_SLOT_B_URL)".to_owned(),
        ));
    }

    validate_router_config(&cfg)?;
    Ok(RouterCliCommand::Run(Box::new(cfg)))
}

/// Validate structural configuration invariants and fail-closed bind security rules.
pub fn validate_router_config(cfg: &RouterConfig) -> Result<(), MembusError> {
    if cfg.data_listen.port() != 0 && cfg.data_listen == cfg.admin_listen {
        return Err(MembusError::InvalidArgument(format!(
            "--data-listen and --admin-listen must use distinct addresses (both set to {})",
            cfg.data_listen
        )));
    }
    if cfg.shadow.max_ops == 0 {
        return Err(MembusError::InvalidArgument(
            "shadow max_ops must be greater than 0".to_owned(),
        ));
    }
    if cfg.shadow.max_bytes == 0 {
        return Err(MembusError::InvalidArgument(
            "shadow max_bytes must be greater than 0".to_owned(),
        ));
    }
    if cfg.shadow.timeout_ms == 0 {
        return Err(MembusError::InvalidArgument(
            "shadow timeout_ms must be greater than 0".to_owned(),
        ));
    }
    if cfg.shadow.catchup_interval_ms == 0 {
        return Err(MembusError::InvalidArgument(
            "shadow catchup_interval_ms must be greater than 0".to_owned(),
        ));
    }
    if cfg.shadow.max_backoff_ms == 0 {
        return Err(MembusError::InvalidArgument(
            "shadow max_backoff_ms must be greater than 0".to_owned(),
        ));
    }
    if cfg.primary_timeout_ms == 0 {
        return Err(MembusError::InvalidArgument(
            "primary_timeout_ms must be greater than 0".to_owned(),
        ));
    }
    if cfg.max_in_flight == 0 {
        return Err(MembusError::InvalidArgument(
            "max_in_flight must be greater than 0".to_owned(),
        ));
    }
    validate_base_url_syntax("slot-a", &cfg.slot_a.url)?;
    validate_base_url_syntax("slot-b", &cfg.slot_b.url)?;
    if normalize_url_for_equality(&cfg.slot_a.url) == normalize_url_for_equality(&cfg.slot_b.url) {
        return Err(MembusError::InvalidArgument(format!(
            "--slot-a-url and --slot-b-url must target distinct endpoints (both set to '{}')",
            cfg.slot_a.url
        )));
    }
    validate_tls_pair("slot-a", &cfg.slot_a)?;
    validate_tls_pair("slot-b", &cfg.slot_b)?;
    validate_router_bind_security(cfg)
}

/// Fail-closed bind guard rejecting non-loopback data or admin listeners without authentication
/// unless `--allow-insecure-non-loopback` is explicitly set.
pub fn validate_router_bind_security(cfg: &RouterConfig) -> Result<(), MembusError> {
    let admin_auth = cfg.has_admin_auth_configured();
    let slots_auth = cfg.slot_a.has_auth_configured() && cfg.slot_b.has_auth_configured();

    if !cfg.data_listen.ip().is_loopback() && !slots_auth && !cfg.allow_insecure_non_loopback {
        return Err(MembusError::InvalidArgument(format!(
            "refusing to bind data listener on non-loopback address {} without upstream slot auth tokens configured; provide --slot-a-token-file and --slot-b-token-file or pass --allow-insecure-non-loopback",
            cfg.data_listen
        )));
    }

    if !cfg.admin_listen.ip().is_loopback() && !admin_auth && !cfg.allow_insecure_non_loopback {
        return Err(MembusError::InvalidArgument(format!(
            "refusing to bind admin listener on non-loopback address {} without --admin-token-file configured; provide --admin-token-file or pass --allow-insecure-non-loopback",
            cfg.admin_listen
        )));
    }

    Ok(())
}

/// Read and trim a bearer token secret from disk, failing closed on missing or empty files.
pub fn load_secret_file(path: &Path) -> Result<String, MembusError> {
    let raw = std::fs::read_to_string(path).map_err(|err| {
        MembusError::InvalidArgument(format!(
            "failed to read secret file '{}': {err}",
            path.display()
        ))
    })?;
    let trimmed = raw.trim().to_owned();
    if trimmed.is_empty() {
        return Err(MembusError::InvalidArgument(format!(
            "secret file '{}' must not be empty",
            path.display()
        )));
    }
    Ok(trimmed)
}

fn parse_primary_setting(raw: &str, cfg: &mut RouterConfig) -> Result<(), MembusError> {
    if raw.trim().eq_ignore_ascii_case("auto") {
        cfg.primary_auto = true;
        return Ok(());
    }
    cfg.initial_primary = SlotId::parse(raw)?;
    cfg.primary_auto = false;
    Ok(())
}

fn validate_base_url_syntax(label: &str, url: &str) -> Result<(), MembusError> {
    let trimmed = url.trim();
    if !(trimmed.starts_with("http://") || trimmed.starts_with("https://")) {
        return Err(MembusError::InvalidArgument(format!(
            "--{label}-url '{url}' must start with http:// or https://"
        )));
    }
    if trimmed.contains('?') || trimmed.contains('#') {
        return Err(MembusError::InvalidArgument(format!(
            "--{label}-url '{url}' must not contain query ('?') or fragment ('#') components"
        )));
    }
    Ok(())
}

fn normalize_url_for_equality(url: &str) -> String {
    url.trim().trim_end_matches('/').to_ascii_lowercase()
}

fn validate_tls_pair(label: &str, ep: &SlotEndpointConfig) -> Result<(), MembusError> {
    if ep.tls_cert_file.is_some() != ep.tls_key_file.is_some() {
        return Err(MembusError::InvalidArgument(format!(
            "--{label}-tls-cert and --{label}-tls-key must both be provided together"
        )));
    }
    Ok(())
}

fn apply_env_defaults(
    cfg: &mut RouterConfig,
    slot_a_set: &mut bool,
    slot_b_set: &mut bool,
) -> Result<(), MembusError> {
    if let Ok(v) = std::env::var("AB_DATA_LISTEN") {
        if !v.trim().is_empty() {
            cfg.data_listen = parse_addr(v.trim(), "AB_DATA_LISTEN")?;
        }
    }
    if let Ok(v) = std::env::var("AB_ADMIN_LISTEN") {
        if !v.trim().is_empty() {
            cfg.admin_listen = parse_addr(v.trim(), "AB_ADMIN_LISTEN")?;
        }
    }
    if let Ok(v) = std::env::var("AB_SLOT_A_URL") {
        if !v.trim().is_empty() {
            cfg.slot_a.url = v.trim().to_owned();
            *slot_a_set = true;
        }
    }
    if let Ok(v) = std::env::var("AB_SLOT_B_URL") {
        if !v.trim().is_empty() {
            cfg.slot_b.url = v.trim().to_owned();
            *slot_b_set = true;
        }
    }
    if let Ok(v) = std::env::var("AB_PRIMARY_SLOT") {
        if !v.trim().is_empty() {
            parse_primary_setting(v.trim(), cfg)?;
        }
    }
    if let Ok(v) = std::env::var("AB_SHADOW_ENABLED") {
        if !v.trim().is_empty() {
            cfg.shadow_enabled = parse_bool_flag(v.trim(), "AB_SHADOW_ENABLED")?;
        }
    }
    if let Ok(v) = std::env::var("AB_SLOT_A_TOKEN_FILE") {
        cfg.slot_a.token_file = opt_path(&v);
    }
    if let Ok(v) = std::env::var("AB_SLOT_B_TOKEN_FILE") {
        cfg.slot_b.token_file = opt_path(&v);
    }
    if let Ok(v) = std::env::var("AB_ADMIN_TOKEN_FILE") {
        cfg.admin_token_file = opt_path(&v);
    }
    if let Ok(v) = std::env::var("AB_SHADOW_MAX_QUEUE_OPS") {
        if !v.trim().is_empty() {
            cfg.shadow.max_ops = parse_positive_usize(v.trim(), "AB_SHADOW_MAX_QUEUE_OPS")?;
        }
    }
    if let Ok(v) = std::env::var("AB_SHADOW_MAX_QUEUE_BYTES") {
        if !v.trim().is_empty() {
            cfg.shadow.max_bytes = parse_positive_usize(v.trim(), "AB_SHADOW_MAX_QUEUE_BYTES")?;
        }
    }
    if let Ok(v) = std::env::var("AB_SHADOW_TIMEOUT_MS") {
        if !v.trim().is_empty() {
            cfg.shadow.timeout_ms = parse_positive_u64(v.trim(), "AB_SHADOW_TIMEOUT_MS")?;
        }
    }
    if let Ok(v) = std::env::var("AB_ALLOW_INSECURE_NON_LOOPBACK") {
        if !v.trim().is_empty() {
            cfg.allow_insecure_non_loopback =
                parse_bool_flag(v.trim(), "AB_ALLOW_INSECURE_NON_LOOPBACK")?;
        }
    }
    Ok(())
}

fn opt_path(raw: &str) -> Option<PathBuf> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(PathBuf::from(trimmed))
    }
}

fn require_val<'a>(args: &'a [String], idx: usize, flag: &str) -> Result<&'a str, MembusError> {
    args.get(idx)
        .map(String::as_str)
        .ok_or_else(|| MembusError::InvalidArgument(format!("missing value for {flag}")))
}

fn parse_addr(raw: &str, flag: &str) -> Result<SocketAddr, MembusError> {
    raw.parse::<SocketAddr>().map_err(|err| {
        MembusError::InvalidArgument(format!("invalid {flag} socket address '{raw}': {err}"))
    })
}

fn parse_positive_usize(raw: &str, flag: &str) -> Result<usize, MembusError> {
    let parsed = raw.parse::<usize>().map_err(|err| {
        MembusError::InvalidArgument(format!("invalid {flag} value '{raw}': {err}"))
    })?;
    if parsed == 0 {
        return Err(MembusError::InvalidArgument(format!(
            "{flag} must be greater than 0"
        )));
    }
    Ok(parsed)
}

fn parse_positive_u64(raw: &str, flag: &str) -> Result<u64, MembusError> {
    let parsed = raw.parse::<u64>().map_err(|err| {
        MembusError::InvalidArgument(format!("invalid {flag} value '{raw}': {err}"))
    })?;
    if parsed == 0 {
        return Err(MembusError::InvalidArgument(format!(
            "{flag} must be greater than 0"
        )));
    }
    Ok(parsed)
}

fn parse_bool_flag(raw: &str, flag: &str) -> Result<bool, MembusError> {
    if raw.eq_ignore_ascii_case("true") || raw == "1" || raw.eq_ignore_ascii_case("yes") {
        Ok(true)
    } else if raw.eq_ignore_ascii_case("false") || raw == "0" || raw.eq_ignore_ascii_case("no") {
        Ok(false)
    } else {
        Err(MembusError::InvalidArgument(format!(
            "invalid boolean value '{raw}' for {flag} (expected true or false)"
        )))
    }
}
