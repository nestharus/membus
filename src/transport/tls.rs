use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use rustls::server::WebPkiClientVerifier;
use rustls::{ClientConfig, RootCertStore, ServerConfig as RustlsServerConfig};
use rustls_pki_types::pem::PemObject;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{watch, Semaphore};
use tokio::task::JoinSet;
use tokio_rustls::{TlsAcceptor, TlsConnector};

pub(crate) const MAX_CONCURRENT_TLS_HANDSHAKES: usize = 256;

use crate::transport::connection::serve_connection;
use crate::transport::{Endpoint, ServerContext, ServerHandle};
use crate::types::MembusError;

/// Server-side TLS / optional mTLS configuration using explicit PEM paths.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsServerConfig {
    pub cert_pem_path: PathBuf,
    pub key_pem_path: PathBuf,
    /// When `Some`, requires valid client certificates signed by this CA bundle (mTLS).
    pub client_ca_pem_path: Option<PathBuf>,
}

/// Client-side TLS / optional mTLS configuration using explicit PEM paths.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsClientConfig {
    pub ca_pem_path: PathBuf,
    pub server_name: String,
    pub client_cert_pem_path: Option<PathBuf>,
    pub client_key_pem_path: Option<PathBuf>,
}

fn build_rustls_server_config(
    cfg: &TlsServerConfig,
    alpn_protocols: &[&[u8]],
) -> Result<RustlsServerConfig, MembusError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let certs = load_certs(&cfg.cert_pem_path)?;
    let key = load_private_key(&cfg.key_pem_path)?;

    let builder = RustlsServerConfig::builder_with_provider(Arc::clone(&provider))
        .with_safe_default_protocol_versions()
        .map_err(|err| MembusError::InvalidArgument(format!("tls protocol config error: {err}")))?;

    let mut server_cfg = if let Some(ca_path) = &cfg.client_ca_pem_path {
        let roots = load_root_store(ca_path)?;
        let verifier = WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider)
            .build()
            .map_err(|err| {
                MembusError::InvalidArgument(format!("mtls client verifier error: {err}"))
            })?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certs, key)
            .map_err(|err| MembusError::InvalidArgument(format!("mtls server cert error: {err}")))?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|err| MembusError::InvalidArgument(format!("tls server cert error: {err}")))?
    };

    if !alpn_protocols.is_empty() {
        server_cfg.alpn_protocols = alpn_protocols.iter().map(|p| p.to_vec()).collect();
    }
    Ok(server_cfg)
}

/// Build a `tokio_rustls::TlsAcceptor` backed by the `ring` crypto provider with zero global state.
pub fn build_tls_acceptor(cfg: &TlsServerConfig) -> Result<TlsAcceptor, MembusError> {
    let server_cfg = build_rustls_server_config(cfg, &[])?;
    Ok(TlsAcceptor::from(Arc::new(server_cfg)))
}

/// Build a `tokio_rustls::TlsAcceptor` advertising HTTP/2 (`h2`) and HTTP/1.1 (`http/1.1`) ALPN.
pub fn build_http_tls_acceptor(cfg: &TlsServerConfig) -> Result<TlsAcceptor, MembusError> {
    let server_cfg = build_rustls_server_config(cfg, &[b"h2", b"http/1.1"])?;
    Ok(TlsAcceptor::from(Arc::new(server_cfg)))
}

/// Build a `tokio_rustls::TlsConnector` backed by the `ring` crypto provider.
pub fn build_tls_connector(cfg: &TlsClientConfig) -> Result<(TlsConnector, ServerName<'static>), MembusError> {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let roots = load_root_store(&cfg.ca_pem_path)?;
    let server_name = ServerName::try_from(cfg.server_name.clone()).map_err(|err| {
        MembusError::InvalidArgument(format!("invalid tls server_name '{}': {err}", cfg.server_name))
    })?;

    let builder = ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .map_err(|err| MembusError::InvalidArgument(format!("tls client protocol error: {err}")))?
        .with_root_certificates(roots);

    let client_cfg = match (&cfg.client_cert_pem_path, &cfg.client_key_pem_path) {
        (Some(cert_path), Some(key_path)) => {
            let certs = load_certs(cert_path)?;
            let key = load_private_key(key_path)?;
            builder.with_client_auth_cert(certs, key).map_err(|err| {
                MembusError::InvalidArgument(format!("mtls client cert error: {err}"))
            })?
        }
        (None, None) => builder.with_no_client_auth(),
        _ => {
            return Err(MembusError::InvalidArgument(
                "client_cert_pem_path and client_key_pem_path must both be set or both omitted"
                    .to_owned(),
            ));
        }
    };

    Ok((TlsConnector::from(Arc::new(client_cfg)), server_name))
}

/// Bind a TLS/mTLS TCP listener serving multiplexed `membus/2` binary connections.
pub async fn spawn_tls_tcp_listener(
    addr: SocketAddr,
    tls_cfg: &TlsServerConfig,
    ctx: ServerContext,
) -> Result<ServerHandle, MembusError> {
    let acceptor = build_tls_acceptor(tls_cfg)?;
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    let active_connections = Arc::clone(&ctx.active_connections);
    let handshake_sem = Arc::new(Semaphore::new(MAX_CONCURRENT_TLS_HANDSHAKES));

    let accept_task = tokio::spawn(async move {
        let mut conn_tasks: JoinSet<()> = JoinSet::new();
        loop {
            while conn_tasks.try_join_next().is_some() {}
            tokio::select! {
                biased;
                _ = shutdown_rx.changed() => {
                    if *shutdown_rx.borrow() {
                        break;
                    }
                }
                accepted = listener.accept() => {
                    match accepted {
                        Ok((tcp_stream, _peer)) => {
                            let Ok(permit) = Arc::clone(&handshake_sem).try_acquire_owned() else {
                                drop(tcp_stream);
                                continue;
                            };
                            let _ = tcp_stream.set_nodelay(true);
                            let acceptor_ref = acceptor.clone();
                            let conn_ctx = ctx.clone();
                            let mut conn_shutdown = shutdown_rx.clone();
                            // Run TLS handshake off accept loop, bounded & cancelable on shutdown.
                            conn_tasks.spawn(async move {
                                let handshake_res = {
                                    let _hs_permit = permit;
                                    tokio::select! {
                                        biased;
                                        _ = wait_shutdown(&mut conn_shutdown) => None,
                                        res = tokio::time::timeout(
                                            Duration::from_secs(10),
                                            acceptor_ref.accept(tcp_stream),
                                        ) => res.ok().and_then(Result::ok),
                                    }
                                };
                                if let Some(tls_stream) = handshake_res {
                                    serve_connection(tls_stream, conn_ctx, conn_shutdown).await;
                                }
                            });
                        }
                        Err(_) => {
                            tokio::time::sleep(Duration::from_millis(2)).await;
                        }
                    }
                }
            }
        }
        // Drain active TLS handshakes & binary connections cleanly before shutdown completes.
        while conn_tasks.join_next().await.is_some() {}
    });

    Ok(ServerHandle::new(
        Endpoint::TcpTls(local_addr),
        shutdown_tx,
        accept_task,
        active_connections,
    ))
}

/// Dial a TLS/mTLS TCP endpoint for the binary `membus/2` client.
pub async fn connect_tcp_tls(
    addr: SocketAddr,
    tls_cfg: &TlsClientConfig,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>, MembusError> {
    let (connector, server_name) = build_tls_connector(tls_cfg)?;
    let tcp = crate::transport::tcp::connect_tcp(addr).await?;
    connector
        .connect(server_name, tcp)
        .await
        .map_err(|err| MembusError::Io(format!("tls handshake failed: {err}")))
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, MembusError> {
    let iter = CertificateDer::pem_file_iter(path).map_err(|err| {
        MembusError::InvalidArgument(format!("failed to read PEM cert '{}': {err}", path.display()))
    })?;
    let mut certs = Vec::new();
    for item in iter {
        certs.push(item.map_err(|err| {
            MembusError::InvalidArgument(format!("invalid PEM cert in '{}': {err}", path.display()))
        })?);
    }
    if certs.is_empty() {
        return Err(MembusError::InvalidArgument(format!(
            "no PEM certificates found in '{}'",
            path.display()
        )));
    }
    Ok(certs)
}

fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>, MembusError> {
    PrivateKeyDer::from_pem_file(path).map_err(|err| {
        MembusError::InvalidArgument(format!("failed to read PEM key '{}': {err}", path.display()))
    })
}

fn load_root_store(ca_path: &Path) -> Result<RootCertStore, MembusError> {
    let certs = load_certs(ca_path)?;
    let mut store = RootCertStore::empty();
    let (added, _ignored) = store.add_parsable_certificates(certs);
    if added == 0 {
        return Err(MembusError::InvalidArgument(format!(
            "no valid CA certificates loaded from '{}'",
            ca_path.display()
        )));
    }
    Ok(store)
}

async fn wait_shutdown(rx: &mut watch::Receiver<bool>) {
    while !*rx.borrow() {
        if rx.changed().await.is_err() {
            return;
        }
    }
}
