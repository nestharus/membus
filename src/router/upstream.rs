//! Reusable HTTP/1.1 + optional TLS/mTLS upstream slot client built on existing
//! `hyper`, `hyper-util`, `http-body-util`, `tokio`, and `tokio-rustls` crates.
//!
//! Strictly separates client data-plane forwarding (`forward_client_raw`, which NEVER injects
//! the router's control-plane bearer token) from authenticated internal control-plane calls
//! (`send_json`).

use std::collections::HashSet;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use axum::http::header::{self, HeaderMap, HeaderName, HeaderValue};
use axum::http::{Method, StatusCode};
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::router::config::{load_secret_file, SlotEndpointConfig, SlotId};
use crate::transport::tls::{build_tls_connector, TlsClientConfig};
use crate::types::{
    Ack, CollectionDescriptor, DeltaLogPage, HealthStatus, MembusError, NamespaceParityDigest,
    ParityDigestMode, ReplicatedDelta, SlotRole, SlotRoleState,
};

const HOP_BY_HOP_HEADERS: &[&str] = &[
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "host",
    "content-length",
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Scheme {
    Http,
    Https,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ParsedUpstreamUrl {
    scheme: Scheme,
    host: String,
    port: u16,
    authority: String,
    base_path: String,
}

impl ParsedUpstreamUrl {
    fn parse(raw: &str) -> Result<Self, MembusError> {
        let trimmed = raw.trim();
        if trimmed.contains('?') || trimmed.contains('#') {
            return Err(MembusError::InvalidArgument(format!(
                "upstream URL '{raw}' must not contain query ('?') or fragment ('#') components"
            )));
        }
        let (scheme, rest) = if let Some(r) = trimmed.strip_prefix("http://") {
            (Scheme::Http, r)
        } else if let Some(r) = trimmed.strip_prefix("https://") {
            (Scheme::Https, r)
        } else {
            return Err(MembusError::InvalidArgument(format!(
                "upstream URL '{raw}' must start with http:// or https://"
            )));
        };

        let (authority_raw, path_raw) = match rest.find('/') {
            Some(pos) => (&rest[..pos], &rest[pos..]),
            None => (rest, ""),
        };
        if authority_raw.is_empty() {
            return Err(MembusError::InvalidArgument(format!(
                "upstream URL '{raw}' is missing host"
            )));
        }
        if authority_raw.contains('@') {
            return Err(MembusError::InvalidArgument(format!(
                "upstream URL '{raw}' must not embed userinfo credentials"
            )));
        }

        let default_port = match scheme {
            Scheme::Http => 80,
            Scheme::Https => 443,
        };
        let (host, port) = parse_host_port(authority_raw, default_port, raw)?;
        let authority = if host.contains(':') {
            format!("[{host}]:{port}")
        } else {
            format!("{host}:{port}")
        };
        let base_path = path_raw.trim_end_matches('/').to_owned();

        Ok(Self {
            scheme,
            host,
            port,
            authority,
            base_path,
        })
    }

    fn connect_addr(&self) -> String {
        if self.host.contains(':') {
            format!("[{}]:{}", self.host, self.port)
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }

    fn resolve_path(&self, path_and_query: &str) -> String {
        if self.base_path.is_empty() {
            if path_and_query.starts_with('/') {
                path_and_query.to_owned()
            } else {
                format!("/{path_and_query}")
            }
        } else if path_and_query.starts_with('/') {
            format!("{}{}", self.base_path, path_and_query)
        } else {
            format!("{}/{}", self.base_path, path_and_query)
        }
    }
}

fn parse_host_port(
    authority: &str,
    default_port: u16,
    raw_url: &str,
) -> Result<(String, u16), MembusError> {
    if let Some(after_open) = authority.strip_prefix('[') {
        let Some(close_idx) = after_open.find(']') else {
            return Err(MembusError::InvalidArgument(format!(
                "unterminated IPv6 literal in upstream URL '{raw_url}'"
            )));
        };
        let host = &after_open[..close_idx];
        let remainder = &after_open[close_idx + 1..];
        let port = if remainder.is_empty() {
            default_port
        } else if let Some(port_str) = remainder.strip_prefix(':') {
            port_str.parse::<u16>().map_err(|err| {
                MembusError::InvalidArgument(format!(
                    "invalid port in upstream URL '{raw_url}': {err}"
                ))
            })?
        } else {
            return Err(MembusError::InvalidArgument(format!(
                "invalid authority in upstream URL '{raw_url}'"
            )));
        };
        return Ok((host.to_owned(), port));
    }

    match authority.rsplit_once(':') {
        Some((host, port_str)) if !host.is_empty() => {
            let port = port_str.parse::<u16>().map_err(|err| {
                MembusError::InvalidArgument(format!(
                    "invalid port in upstream URL '{raw_url}': {err}"
                ))
            })?;
            Ok((host.to_owned(), port))
        }
        _ => Ok((authority.to_owned(), default_port)),
    }
}

enum UpstreamStream {
    Plain(TcpStream),
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for UpstreamStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            Self::Tls(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for UpstreamStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            Self::Tls(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            Self::Tls(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            Self::Tls(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Raw buffered response returned from an upstream Membus slot.
#[derive(Clone, Debug)]
pub struct UpstreamResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

/// HTTP/1.1 + optional TLS/mTLS client targeting one Membus slot.
pub struct UpstreamClient {
    slot: SlotId,
    url: ParsedUpstreamUrl,
    control_bearer_token: Option<String>,
    tls: Option<(TlsConnector, rustls_pki_types::ServerName<'static>)>,
    max_body_bytes: usize,
}

impl UpstreamClient {
    pub fn new(
        slot: SlotId,
        cfg: &SlotEndpointConfig,
        max_body_bytes: usize,
    ) -> Result<Self, MembusError> {
        let url = ParsedUpstreamUrl::parse(&cfg.url)?;
        let control_bearer_token = match (&cfg.inline_token, &cfg.token_file) {
            (Some(inline), _) if !inline.trim().is_empty() => Some(inline.trim().to_owned()),
            (_, Some(path)) => Some(load_secret_file(path)?),
            _ => None,
        };

        let tls = match url.scheme {
            Scheme::Http => None,
            Scheme::Https => {
                let ca_path = cfg.tls_ca_file.clone().ok_or_else(|| {
                    MembusError::InvalidArgument(format!(
                        "HTTPS upstream for {} ('{}') requires --{}-tls-ca <PATH>",
                        slot,
                        cfg.url,
                        slot.as_str()
                    ))
                })?;
                let server_name = cfg
                    .tls_server_name
                    .clone()
                    .unwrap_or_else(|| url.host.clone());
                let tls_cfg = TlsClientConfig {
                    ca_pem_path: ca_path,
                    server_name,
                    client_cert_pem_path: cfg.tls_cert_file.clone(),
                    client_key_pem_path: cfg.tls_key_file.clone(),
                };
                Some(build_tls_connector(&tls_cfg)?)
            }
        };

        Ok(Self {
            slot,
            url,
            control_bearer_token,
            tls,
            max_body_bytes: max_body_bytes.max(1024),
        })
    }

    pub fn slot(&self) -> SlotId {
        self.slot
    }

    pub fn authority(&self) -> &str {
        &self.url.authority
    }

    /// Forward an untrusted client data-plane HTTP request to this slot within `timeout`.
    ///
    /// Security invariant: forwards ONLY the caller's own `Authorization` header (if present)
    /// and **NEVER** injects the router's privileged `control_bearer_token`.
    pub async fn forward_client_raw(
        &self,
        method: Method,
        path_and_query: &str,
        inbound_headers: HeaderMap,
        body: Bytes,
        timeout: Duration,
    ) -> Result<UpstreamResponse, MembusError> {
        let clean_headers = strip_hop_by_hop_headers(&inbound_headers);
        self.send_http_with_timeout(method, path_and_query, clean_headers, body, false, timeout)
            .await
    }

    /// Send an authenticated internal control-plane JSON request using `control_bearer_token`.
    pub async fn send_json<T: Serialize>(
        &self,
        method: Method,
        path_and_query: &str,
        extra_headers: &[(&str, &str)],
        payload: Option<&T>,
        timeout: Duration,
    ) -> Result<UpstreamResponse, MembusError> {
        let mut headers = HeaderMap::new();
        for (k, v) in extra_headers {
            let name = HeaderName::from_bytes(k.as_bytes()).map_err(|err| {
                MembusError::InvalidArgument(format!("invalid header name '{k}': {err}"))
            })?;
            let val = HeaderValue::from_str(v).map_err(|err| {
                MembusError::InvalidArgument(format!("invalid header value for '{k}': {err}"))
            })?;
            headers.insert(name, val);
        }
        let body = match payload {
            Some(p) => {
                headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
                Bytes::from(
                    serde_json::to_vec(p)
                        .map_err(|err| MembusError::Protocol(format!("JSON encode failed: {err}")))?,
                )
            }
            None => Bytes::new(),
        };
        self.send_http_with_timeout(method, path_and_query, headers, body, true, timeout)
            .await
    }

    async fn send_http_with_timeout(
        &self,
        method: Method,
        path_and_query: &str,
        headers: HeaderMap,
        body: Bytes,
        attach_control_auth: bool,
        timeout: Duration,
    ) -> Result<UpstreamResponse, MembusError> {
        tokio::time::timeout(
            timeout,
            self.send_http_inner(method, path_and_query, headers, body, attach_control_auth),
        )
        .await
        .map_err(|_| {
            MembusError::Io(format!(
                "upstream {} request timed out after {}ms",
                self.slot,
                timeout.as_millis()
            ))
        })?
    }

    async fn send_http_inner(
        &self,
        method: Method,
        path_and_query: &str,
        headers: HeaderMap,
        body: Bytes,
        attach_control_auth: bool,
    ) -> Result<UpstreamResponse, MembusError> {
        let tcp = TcpStream::connect(self.url.connect_addr())
            .await
            .map_err(|err| {
                MembusError::Io(format!(
                    "failed to connect to {} ({}): {err}",
                    self.slot, self.url.authority
                ))
            })?;
        let _ = tcp.set_nodelay(true);

        let stream = match &self.tls {
            Some((connector, server_name)) => {
                let tls_stream = connector
                    .connect(server_name.clone(), tcp)
                    .await
                    .map_err(|err| {
                        MembusError::Io(format!(
                            "TLS handshake to {} ({}) failed: {err}",
                            self.slot, self.url.authority
                        ))
                    })?;
                UpstreamStream::Tls(Box::new(tls_stream))
            }
            None => UpstreamStream::Plain(tcp),
        };

        let io = TokioIo::new(stream);
        let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
            .await
            .map_err(|err| {
                MembusError::Io(format!("HTTP/1.1 handshake to {} failed: {err}", self.slot))
            })?;
        tokio::spawn(async move {
            let _ = conn.await;
        });

        let target_uri = self.url.resolve_path(path_and_query);
        let mut builder = hyper::Request::builder()
            .method(method)
            .uri(&target_uri)
            .header(header::HOST, &self.url.authority);

        for (name, value) in &headers {
            if is_hop_by_hop(name) {
                continue;
            }
            if attach_control_auth && *name == header::AUTHORIZATION {
                continue;
            }
            builder = builder.header(name, value);
        }
        if attach_control_auth {
            if let Some(tok) = &self.control_bearer_token {
                builder = builder.header(header::AUTHORIZATION, format!("Bearer {tok}"));
            }
        }

        let req = builder
            .body(Full::new(body))
            .map_err(|err| MembusError::Protocol(format!("failed to build upstream request: {err}")))?;

        let resp = sender.send_request(req).await.map_err(|err| {
            MembusError::Io(format!("upstream {} request failed: {err}", self.slot))
        })?;
        let status = resp.status();
        let resp_headers = strip_hop_by_hop_headers(resp.headers());
        let body = read_bounded_body(resp.into_body(), self.max_body_bytes).await?;

        Ok(UpstreamResponse {
            status,
            headers: resp_headers,
            body,
        })
    }

    /// Query `GET /v2/health` on this slot.
    pub async fn health(&self, timeout: Duration) -> Result<HealthStatus, MembusError> {
        let resp = self
            .send_json::<serde_json::Value>(Method::GET, "/v2/health", &[], None, timeout)
            .await?;
        decode_json_response(self.slot, "GET /v2/health", resp)
    }

    /// Query `GET /v2/catalog/collections` on this slot.
    pub async fn list_collections(
        &self,
        timeout: Duration,
    ) -> Result<Vec<CollectionDescriptor>, MembusError> {
        let resp = self
            .send_json::<serde_json::Value>(
                Method::GET,
                "/v2/catalog/collections",
                &[],
                None,
                timeout,
            )
            .await?;
        decode_json_response(self.slot, "GET /v2/catalog/collections", resp)
    }

    /// Query `POST /v2/namespaces/{ns}/digest` on this slot.
    pub async fn namespace_digest(
        &self,
        namespace: &str,
        mode: ParityDigestMode,
        prefix: Option<&str>,
        timeout: Duration,
    ) -> Result<NamespaceParityDigest, MembusError> {
        #[derive(Serialize)]
        struct DigestBody<'a> {
            mode: ParityDigestMode,
            #[serde(skip_serializing_if = "Option::is_none")]
            prefix_utf8: Option<&'a str>,
        }
        let encoded_ns = percent_encode_segment(namespace);
        let path = format!("/v2/namespaces/{encoded_ns}/digest");
        let resp = self
            .send_json(
                Method::POST,
                &path,
                &[],
                Some(&DigestBody {
                    mode,
                    prefix_utf8: prefix,
                }),
                timeout,
            )
            .await?;
        decode_json_response(self.slot, &path, resp)
    }

    /// CAS-transition slot role via `POST /v2/admin/role`.
    pub async fn transition_role(
        &self,
        expected_role: SlotRole,
        expected_role_epoch: u64,
        new_role: SlotRole,
        timeout: Duration,
    ) -> Result<SlotRoleState, MembusError> {
        #[derive(Serialize)]
        struct RoleReq {
            expected_role: SlotRole,
            expected_role_epoch: u64,
            new_role: SlotRole,
        }
        let resp = self
            .send_json(
                Method::POST,
                "/v2/admin/role",
                &[],
                Some(&RoleReq {
                    expected_role,
                    expected_role_epoch,
                    new_role,
                }),
                timeout,
            )
            .await?;
        decode_json_response(self.slot, "POST /v2/admin/role", resp)
    }

    /// Fetch committed `CF_LOG` deltas after `since_seq` via `GET /v2/updates`.
    pub async fn fetch_updates(
        &self,
        since_seq: u64,
        limit: usize,
        timeout: Duration,
    ) -> Result<DeltaLogPage, MembusError> {
        let path = format!("/v2/updates?since_seq={since_seq}&limit={limit}");
        let resp = self
            .send_json::<serde_json::Value>(Method::GET, &path, &[], None, timeout)
            .await?;
        decode_json_response(self.slot, &path, resp)
    }

    /// Apply a single replicated delta via `POST /v2/replication/apply`.
    pub async fn apply_replicated_delta(
        &self,
        delta: &ReplicatedDelta,
        route_origin: &str,
        role_epoch: Option<u64>,
        timeout: Duration,
    ) -> Result<UpstreamResponse, MembusError> {
        #[derive(Serialize)]
        struct ApplyReq<'a> {
            delta: &'a ReplicatedDelta,
            ack: Ack,
            route_origin: &'a str,
        }
        let mut extra: Vec<(&str, String)> =
            vec![("X-Membus-Route-Origin", route_origin.to_owned())];
        if let Some(ep) = role_epoch {
            extra.push(("X-Membus-Role-Epoch", ep.to_string()));
        }
        let header_refs: Vec<(&str, &str)> =
            extra.iter().map(|(k, v)| (*k, v.as_str())).collect();
        self.send_json(
            Method::POST,
            "/v2/replication/apply",
            &header_refs,
            Some(&ApplyReq {
                delta,
                ack: Ack::Fast,
                route_origin,
            }),
            timeout,
        )
        .await
    }
}

fn decode_json_response<T: DeserializeOwned>(
    slot: SlotId,
    op: &str,
    resp: UpstreamResponse,
) -> Result<T, MembusError> {
    if !resp.status.is_success() {
        let msg = String::from_utf8_lossy(&resp.body);
        return Err(MembusError::Io(format!(
            "upstream {slot} {op} returned HTTP {}: {}",
            resp.status.as_u16(),
            msg.trim()
        )));
    }
    serde_json::from_slice(&resp.body).map_err(|err| {
        MembusError::Protocol(format!(
            "failed to decode JSON from {slot} {op}: {err}"
        ))
    })
}

async fn read_bounded_body(
    mut body: hyper::body::Incoming,
    max_bytes: usize,
) -> Result<Bytes, MembusError> {
    let mut buf = Vec::new();
    while let Some(frame_res) = body.frame().await {
        let frame = frame_res.map_err(|err| {
            MembusError::Io(format!("failed to read upstream response body: {err}"))
        })?;
        if let Some(data) = frame.data_ref() {
            if buf.len().saturating_add(data.len()) > max_bytes {
                return Err(MembusError::FrameTooLarge {
                    len: buf.len().saturating_add(data.len()),
                    max_len: max_bytes,
                });
            }
            buf.extend_from_slice(data);
        }
    }
    Ok(Bytes::from(buf))
}

pub(crate) fn is_hop_by_hop(name: &HeaderName) -> bool {
    let lower = name.as_str();
    HOP_BY_HOP_HEADERS.contains(&lower)
}

/// Strip standard hop-by-hop headers AND any headers nominated by RFC 9110 `Connection`.
pub(crate) fn strip_hop_by_hop_headers(headers: &HeaderMap) -> HeaderMap {
    let mut nominated: HashSet<String> = HashSet::new();
    for val in headers.get_all(header::CONNECTION) {
        if let Ok(s) = val.to_str() {
            for token in s.split(',') {
                let trimmed = token.trim().to_ascii_lowercase();
                if !trimmed.is_empty() {
                    nominated.insert(trimmed);
                }
            }
        }
    }
    let mut out = HeaderMap::new();
    for (name, value) in headers {
        if is_hop_by_hop(name) || nominated.contains(name.as_str()) {
            continue;
        }
        out.append(name.clone(), value.clone());
    }
    out
}

fn percent_encode_segment(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for b in raw.bytes() {
        let unreserved = b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~');
        if unreserved {
            out.push(char::from(b));
        } else {
            use std::fmt::Write as _;
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}
