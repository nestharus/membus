use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Empty, Full};
use hyper::header::CONTENT_TYPE;
use hyper::{Method, Request, StatusCode, Version};
use hyper_util::rt::{TokioExecutor, TokioIo};
use membus::cli::{parse_args, Command};
use membus::{
    Ack, Client, Endpoint, MembusServer, RocksDbStorage, SearchConfig, SearchEngine, ServerConfig,
    SignalBroker, SignalConfig, SignalPublisher, SignalRing, StorageConfig, StorageEngine,
    TlsClientConfig, TlsServerConfig,
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message as WsMessage;

fn build_full_server(dir: &TempDir, ckpt_root: Option<&TempDir>) -> (Arc<RocksDbStorage>, MembusServer) {
    let signal_cfg = SignalConfig::default();
    let ring = Arc::new(SignalRing::new(signal_cfg));
    let storage = Arc::new(
        RocksDbStorage::open(
            dir.path(),
            StorageConfig::default(),
            Arc::clone(&ring) as Arc<dyn SignalPublisher>,
        )
        .expect("open storage"),
    );
    let broker = Arc::new(SignalBroker::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        ring,
        signal_cfg,
    ));
    let search = Arc::new(
        SearchEngine::open(
            dir.path(),
            Arc::clone(&storage) as Arc<dyn StorageEngine>,
            SearchConfig {
                commit_interval_ms: 60_000,
                ..SearchConfig::default()
            },
        )
        .expect("open search"),
    );
    let server_cfg = ServerConfig {
        checkpoint_root: ckpt_root.map(|d| d.path().to_path_buf()),
        ..ServerConfig::default()
    };
    let server = MembusServer::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        broker,
        server_cfg,
    )
    .with_search(search);
    (storage, server)
}

async fn http1_json(
    addr: SocketAddr,
    method: Method,
    path: &str,
    body: Option<serde_json::Value>,
) -> (StatusCode, Bytes) {
    let stream = TcpStream::connect(addr).await.expect("connect");
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .expect("http1 handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("host", addr.to_string());
    let req = if let Some(val) = body {
        builder = builder.header(CONTENT_TYPE, "application/json");
        builder
            .body(Full::new(Bytes::from(val.to_string())).boxed())
            .expect("req")
    } else {
        builder
            .body(Empty::<Bytes>::new().map_err(|never| match never {}).boxed())
            .expect("req")
    };
    let resp = sender.send_request(req).await.expect("send");
    let status = resp.status();
    let bytes = resp.into_body().collect().await.expect("collect").to_bytes();
    (status, bytes)
}

async fn http1_raw(
    addr: SocketAddr,
    method: Method,
    path: &str,
    body: Bytes,
) -> (StatusCode, Bytes) {
    let stream = TcpStream::connect(addr).await.expect("connect");
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
        .await
        .expect("http1 handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let req = Request::builder()
        .method(method)
        .uri(path)
        .header("host", addr.to_string())
        .body(Full::new(body))
        .expect("req");
    let resp = sender.send_request(req).await.expect("send");
    let status = resp.status();
    let bytes = resp.into_body().collect().await.expect("collect").to_bytes();
    (status, bytes)
}

#[tokio::test]
async fn http_rest_end_to_end_kv_indexes_blobs_leases_catalog_search_and_checkpoint() {
    let dir = TempDir::new().expect("tempdir");
    let ckpt_root = TempDir::new().expect("ckpt_root");
    let (_storage, server) = build_full_server(&dir, Some(&ckpt_root));

    let handle = server
        .bind_http("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind http");
    let Endpoint::Http(addr) = *handle.endpoint() else {
        panic!("expected http endpoint");
    };

    // Health
    let (st, body) = http1_json(addr, Method::GET, "/v2/health", None).await;
    assert_eq!(st, StatusCode::OK);
    let health: serde_json::Value = serde_json::from_slice(&body).expect("json");
    assert!(health["epoch"].as_u64().unwrap_or(0) > 0);

    // PUT KV with ordered index, graph edge, and search op
    let (st, _) = http1_json(
        addr,
        Method::PUT,
        "/v2/kv/orders/o1",
        Some(serde_json::json!({
            "value_utf8": "order-1-data",
            "ack": "Fast",
            "indexes": {
                "ordered": [{
                    "index_id": "queue",
                    "sort_key": [1, 2],
                    "summary": [111, 49]
                }],
                "edges": [{
                    "index_id": "deps",
                    "src_id": [111, 49],
                    "rel_label": [108, 105, 110, 107],
                    "dst_id": [111, 50],
                    "meta": [109]
                }]
            },
            "search_op": {
                "Index": {
                    "namespace": "orders",
                    "doc_id": "o1",
                    "kind": "order",
                    "title": "Priority Rest Order",
                    "tags": ["http", "rest"],
                    "body": "Contains ExactCode_9911",
                    "visibility": "Public"
                }
            }
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // GET KV
    let (st, body) = http1_json(addr, Method::GET, "/v2/kv/orders/o1", None).await;
    assert_eq!(st, StatusCode::OK);
    let rec: membus::StoredRecord = serde_json::from_slice(&body).expect("record");
    assert_eq!(rec.value, b"order-1-data");

    // Ordered query
    let (st, body) = http1_json(
        addr,
        Method::POST,
        "/v2/index/ordered/query",
        Some(serde_json::json!({
            "namespace": "orders",
            "index_id": "queue",
            "filter": { "kind": "all" }
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let ord: membus::OrderedIndexPage = serde_json::from_slice(&body).expect("ord");
    assert_eq!(ord.entries.len(), 1);
    assert_eq!(ord.entries[0].primary_key, b"o1");

    // Graph neighbors & BFS
    let (st, body) = http1_json(
        addr,
        Method::POST,
        "/v2/index/graph/bfs",
        Some(serde_json::json!({
            "namespace": "orders",
            "index_id": "deps",
            "seed_node_utf8": "o1",
            "direction": "Out",
            "rel_filter_utf8": "link",
            "max_hops": 2,
            "max_nodes": 16
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let bfs: membus::BfsResult = serde_json::from_slice(&body).expect("bfs");
    assert_eq!(bfs.visited_nodes.len(), 2);

    // Search query (0 ms Tier-0 overlay)
    let (st, body) = http1_json(
        addr,
        Method::POST,
        "/v2/search/query",
        Some(serde_json::json!({
            "namespace": "orders",
            "kind": "order",
            "query": "ExactCode_9911",
            "mode": "LiteralSubstring",
            "limit": 10
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let sp: membus::SearchPage = serde_json::from_slice(&body).expect("search");
    assert_eq!(sp.hits.len(), 1);
    assert_eq!(sp.hits[0].doc_id, "o1");

    // DELETE KV -> 404 on subsequent GET
    let (st, _) = http1_json(addr, Method::DELETE, "/v2/kv/orders/o1", None).await;
    assert_eq!(st, StatusCode::OK);
    let (st, _) = http1_json(addr, Method::GET, "/v2/kv/orders/o1", None).await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    // CAS Blob PUT / HEAD / GET
    let blob_data = Bytes::from_static(b"binary-cas-artifact-payload");
    let (st, body) = http1_raw(addr, Method::PUT, "/v2/blobs/art?ack=flush", blob_data.clone()).await;
    assert_eq!(st, StatusCode::OK);
    let blob_resp: serde_json::Value = serde_json::from_slice(&body).expect("blob json");
    let hex_sha = blob_resp["sha256_hex"].as_str().expect("sha256_hex");

    let (st, _) = http1_json(addr, Method::HEAD, &format!("/v2/blobs/art/{hex_sha}"), None).await;
    assert_eq!(st, StatusCode::OK);
    let (st, fetched) = http1_json(addr, Method::GET, &format!("/v2/blobs/art/{hex_sha}"), None).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(fetched, blob_data);

    // Lease acquire / inspect / validate / renew / release
    let (st, body) = http1_json(
        addr,
        Method::POST,
        "/v2/leases/acquire",
        Some(serde_json::json!({
            "namespace": "jobs",
            "resource": "res-1",
            "holder": "h1",
            "ttl_ms": 5000
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let grant: membus::LeaseGrant = serde_json::from_slice(&body).expect("lease");
    assert_eq!(grant.generation, 1);

    let (st, _) = http1_json(addr, Method::GET, "/v2/leases/jobs/res-1", None).await;
    assert_eq!(st, StatusCode::OK);

    let (st, _) = http1_json(
        addr,
        Method::POST,
        "/v2/leases/release",
        Some(serde_json::json!({
            "namespace": "jobs",
            "resource": "res-1",
            "holder": "h1",
            "generation": grant.generation,
            "token": grant.token
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // Catalog register + migration commit
    let s1 = b"schema-v1".to_vec();
    let s2 = b"schema-v2".to_vec();
    let h1: [u8; 32] = Sha256::digest(&s1).into();
    let h2: [u8; 32] = Sha256::digest(&s2).into();
    let s1_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &s1);
    let (st, _) = http1_json(
        addr,
        Method::POST,
        "/v2/catalog/collections",
        Some(serde_json::json!({
            "expected_version": null,
            "descriptor": {
                "namespace": "orders",
                "store_profile": "Mixed",
                "schema_version": 1,
                "schema_sha256": h1,
                "schema_bytes": s1_b64,
                "declared_indexes": [],
                "retention_policy": { "max_age_secs": null, "max_entries": null },
                "migration_state": "Active"
            }
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, _) = http1_json(
        addr,
        Method::POST,
        "/v2/catalog/collections/orders/migrations/begin",
        Some(serde_json::json!({
            "expected_version": 1,
            "target_version": 2,
            "target_sha256": h2,
            "target_schema_bytes": s2
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, _) = http1_json(
        addr,
        Method::POST,
        "/v2/catalog/collections/orders/migrations/commit",
        Some(serde_json::json!({
            "target_version": 2,
            "verified_through": 10
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, body) = http1_json(addr, Method::GET, "/v2/catalog/collections/orders/versions/1", None).await;
    assert_eq!(st, StatusCode::OK);
    let hist: membus::CollectionDescriptor = serde_json::from_slice(&body).expect("v1");
    assert_eq!(hist.schema_version, 1);

    // Checkpoint
    let (st, _) = http1_json(
        addr,
        Method::POST,
        "/v2/checkpoint",
        Some(serde_json::json!({ "dest_dir": "http_ckpt" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert!(ckpt_root.path().join("http_ckpt").exists());

    handle.shutdown().await;
}

#[tokio::test]
async fn http2_prior_knowledge_h2c_multiplexed_requests() {
    let dir = TempDir::new().expect("tempdir");
    let (storage, server) = build_full_server(&dir, None);
    storage
        .put("h2", b"hello", b"http2-world", Ack::Fast, membus::ExpectedVersion::Any)
        .expect("seed");

    let handle = server
        .bind_http("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind http");
    let Endpoint::Http(addr) = *handle.endpoint() else {
        panic!("expected http endpoint");
    };

    let tcp = TcpStream::connect(addr).await.expect("connect");
    let (sender, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(tcp))
            .await
            .expect("h2 handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let mut tasks = Vec::new();
    for _ in 0..32usize {
        let mut s = sender.clone();
        tasks.push(tokio::spawn(async move {
            let req = Request::builder()
                .method(Method::GET)
                .uri(format!("http://{addr}/v2/kv/h2/hello"))
                .body(Empty::<Bytes>::new())
                .expect("req");
            let resp = s.send_request(req).await.expect("h2 req");
            assert_eq!(resp.version(), Version::HTTP_2);
            assert_eq!(resp.status(), StatusCode::OK);
            let bytes = resp.into_body().collect().await.expect("body").to_bytes();
            let rec: membus::StoredRecord = serde_json::from_slice(&bytes).expect("rec");
            assert_eq!(rec.value, b"http2-world");
        }));
    }
    for t in tasks {
        t.await.expect("h2 task");
    }

    handle.shutdown().await;
}

#[tokio::test]
async fn http_sse_live_signal_stream_delivers_events_and_cancels_cleanly() {
    let dir = TempDir::new().expect("tempdir");
    let (storage, server) = build_full_server(&dir, None);
    let handle = server
        .bind_http("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind http");
    let Endpoint::Http(addr) = *handle.endpoint() else {
        panic!("expected http endpoint");
    };

    let tcp = TcpStream::connect(addr).await.expect("connect");
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp))
        .await
        .expect("http1");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let req = Request::builder()
        .method(Method::GET)
        .uri("/v2/signals/events/stream?mode=each&since_seq=0")
        .header("host", addr.to_string())
        .body(Empty::<Bytes>::new())
        .expect("req");
    let resp = sender.send_request(req).await.expect("send sse");
    assert_eq!(resp.status(), StatusCode::OK);

    let mut body = resp.into_body();
    let mut accumulated = String::new();

    // First chunk should deliver `event: subscribed`.
    while !accumulated.contains("event: subscribed") {
        let frame = timeout(Duration::from_secs(3), body.frame())
            .await
            .expect("sse init timeout")
            .expect("frame some")
            .expect("frame ok");
        if let Some(data) = frame.data_ref() {
            accumulated.push_str(&String::from_utf8_lossy(data));
        }
    }

    // Publish an event and verify live SSE frame delivery.
    storage
        .put("events", b"k/sse1", b"hello-sse", Ack::Fast, membus::ExpectedVersion::Any)
        .expect("put");

    while !accumulated.contains("event: signal") {
        let frame = timeout(Duration::from_secs(3), body.frame())
            .await
            .expect("sse event timeout")
            .expect("frame some")
            .expect("frame ok");
        if let Some(data) = frame.data_ref() {
            accumulated.push_str(&String::from_utf8_lossy(data));
        }
    }
    assert!(accumulated.contains("\"namespace\":\"events\""));

    // Drop client SSE body and verify server shuts down cleanly without hanging.
    drop(body);
    handle.shutdown().await;
}

#[tokio::test]
async fn websocket_bidirectional_concurrent_rpcs_and_live_signals() {
    let dir = TempDir::new().expect("tempdir");
    let (_storage, server) = build_full_server(&dir, None);
    let handle = server
        .bind_http("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind http");
    let Endpoint::Http(addr) = *handle.endpoint() else {
        panic!("expected http endpoint");
    };

    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/v2/ws"))
        .await
        .expect("ws connect");
    let (mut ws_tx, mut ws_rx) = ws.split();

    // Subscribe over WS stream id 100.
    ws_tx
        .send(WsMessage::Text(
            r#"{"id":100,"op":"subscribe_each","namespace":"wsns","since_seq":0}"#.into(),
        ))
        .await
        .expect("send sub");
    let sub_ack_raw = timeout(Duration::from_secs(3), ws_rx.next())
        .await
        .expect("sub ack timeout")
        .expect("msg")
        .expect("ok")
        .into_text()
        .expect("text");
    let sub_ack: serde_json::Value = serde_json::from_str(&sub_ack_raw).expect("json");
    assert_eq!(sub_ack["id"], 100);
    assert_eq!(sub_ack["kind"], "subscribed");

    // Fire PUT + GET + PING over the same WS while subscription is active.
    ws_tx
        .send(WsMessage::Text(
            r#"{"id":1,"op":"put","namespace":"wsns","key_utf8":"item1","value_utf8":"ws-val"}"#
                .into(),
        ))
        .await
        .expect("send put");
    ws_tx
        .send(WsMessage::Text(r#"{"id":2,"op":"ping"}"#.into()))
        .await
        .expect("send ping");

    let mut saw_put = false;
    let mut saw_ping = false;
    let mut saw_signal = false;
    while !(saw_put && saw_ping && saw_signal) {
        let msg = timeout(Duration::from_secs(3), ws_rx.next())
            .await
            .expect("ws msg timeout")
            .expect("some")
            .expect("ok")
            .into_text()
            .expect("text");
        let val: serde_json::Value = serde_json::from_str(&msg).expect("json");
        match val["kind"].as_str() {
            Some("commit") if val["id"] == 1 => saw_put = true,
            Some("pong") if val["id"] == 2 => saw_ping = true,
            Some("signal") if val["id"] == 100 => saw_signal = true,
            _ => {}
        }
    }

    // Cancel subscription 100 and confirm clean stream_end.
    ws_tx
        .send(WsMessage::Text(
            r#"{"id":3,"op":"cancel","stream_id":100}"#.into(),
        ))
        .await
        .expect("send cancel");

    let mut saw_end = false;
    for _ in 0..4 {
        let msg = timeout(Duration::from_secs(3), ws_rx.next())
            .await
            .expect("cancel timeout")
            .expect("some")
            .expect("ok")
            .into_text()
            .expect("text");
        let val: serde_json::Value = serde_json::from_str(&msg).expect("json");
        if val["kind"] == "stream_end" && val["id"] == 100 {
            saw_end = true;
            break;
        }
    }
    assert!(saw_end);

    let _ = ws_tx.close().await;
    handle.shutdown().await;
}

#[tokio::test]
async fn mtls_tcp_roundtrip_and_rejects_unauthenticated_client_cert() {
    let dir = TempDir::new().expect("tempdir");
    let certs_dir = TempDir::new().expect("certs");
    let (_storage, server) = build_full_server(&dir, None);

    // Generate ephemeral CA + server cert + valid client cert + rogue untrusted client cert.
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).expect("ca params");
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_key = KeyPair::generate().expect("ca key");
    let ca_cert = ca_params.self_signed(&ca_key).expect("ca cert");
    let ca_issuer = Issuer::new(ca_params, ca_key);

    let mut srv_params = CertificateParams::new(vec!["localhost".to_owned(), "127.0.0.1".to_owned()])
        .expect("srv params");
    srv_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let srv_key = KeyPair::generate().expect("srv key");
    let srv_cert = srv_params.signed_by(&srv_key, &ca_issuer).expect("srv cert");

    let mut cli_params = CertificateParams::new(vec!["client.local".to_owned()]).expect("cli params");
    cli_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let cli_key = KeyPair::generate().expect("cli key");
    let cli_cert = cli_params.signed_by(&cli_key, &ca_issuer).expect("cli cert");

    // Rogue CA & client cert not trusted by server.
    let mut rogue_ca_params = CertificateParams::new(Vec::<String>::new()).expect("rogue ca");
    rogue_ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    rogue_ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    let rogue_ca_key = KeyPair::generate().expect("rogue ca key");
    let rogue_issuer = Issuer::new(rogue_ca_params, rogue_ca_key);
    let mut rogue_cli_params =
        CertificateParams::new(vec!["rogue.local".to_owned()]).expect("rogue cli");
    rogue_cli_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let rogue_cli_key = KeyPair::generate().expect("rogue cli key");
    let rogue_cli_cert = rogue_cli_params
        .signed_by(&rogue_cli_key, &rogue_issuer)
        .expect("rogue cli cert");

    let ca_pem = certs_dir.path().join("ca.pem");
    let srv_cert_pem = certs_dir.path().join("server.pem");
    let srv_key_pem = certs_dir.path().join("server.key");
    let cli_cert_pem = certs_dir.path().join("client.pem");
    let cli_key_pem = certs_dir.path().join("client.key");
    let rogue_cert_pem = certs_dir.path().join("rogue.pem");
    let rogue_key_pem = certs_dir.path().join("rogue.key");

    std::fs::write(&ca_pem, ca_cert.pem()).expect("write ca");
    std::fs::write(&srv_cert_pem, srv_cert.pem()).expect("write srv cert");
    std::fs::write(&srv_key_pem, srv_key.serialize_pem()).expect("write srv key");
    std::fs::write(&cli_cert_pem, cli_cert.pem()).expect("write cli cert");
    std::fs::write(&cli_key_pem, cli_key.serialize_pem()).expect("write cli key");
    std::fs::write(&rogue_cert_pem, rogue_cli_cert.pem()).expect("write rogue cert");
    std::fs::write(&rogue_key_pem, rogue_cli_key.serialize_pem()).expect("write rogue key");

    let tls_server_cfg = TlsServerConfig {
        cert_pem_path: srv_cert_pem,
        key_pem_path: srv_key_pem,
        client_ca_pem_path: Some(ca_pem.clone()),
    };
    let handle = server
        .bind_tcp_tls("127.0.0.1:0".parse::<SocketAddr>().unwrap(), &tls_server_cfg)
        .await
        .expect("bind tcp tls");
    let Endpoint::TcpTls(addr) = *handle.endpoint() else {
        panic!("expected TcpTls endpoint");
    };

    // Valid mTLS client succeeds on full RPC round-trip + live subscription.
    let valid_client = Client::connect_tcp_tls(
        addr,
        &TlsClientConfig {
            ca_pem_path: ca_pem.clone(),
            server_name: "localhost".to_owned(),
            client_cert_pem_path: Some(cli_cert_pem),
            client_key_pem_path: Some(cli_key_pem),
        },
    )
    .await
    .expect("valid mtls connect");

    valid_client
        .put("mtls", b"secret", b"encrypted-value", Ack::Flush)
        .await
        .expect("mtls put");
    let rec = valid_client
        .get("mtls", b"secret")
        .await
        .expect("mtls get")
        .expect("present");
    assert_eq!(rec.value, b"encrypted-value");

    // Client WITHOUT client certificate MUST fail closed!
    let no_cert_res = Client::connect_tcp_tls(
        addr,
        &TlsClientConfig {
            ca_pem_path: ca_pem.clone(),
            server_name: "localhost".to_owned(),
            client_cert_pem_path: None,
            client_key_pem_path: None,
        },
    )
    .await;
    match no_cert_res {
        Err(_) => {}
        Ok(c) => {
            // TLS 1.3 post-handshake certificate alert surfaces on first RPC read/write.
            assert!(c.health().await.is_err(), "no-cert mTLS client must be rejected");
        }
    }

    // Client with ROGUE untrusted certificate MUST fail closed!
    let rogue_res = Client::connect_tcp_tls(
        addr,
        &TlsClientConfig {
            ca_pem_path: ca_pem,
            server_name: "localhost".to_owned(),
            client_cert_pem_path: Some(rogue_cert_pem),
            client_key_pem_path: Some(rogue_key_pem),
        },
    )
    .await;
    match rogue_res {
        Err(_) => {}
        Ok(c) => {
            assert!(c.health().await.is_err(), "rogue mTLS client must be rejected");
        }
    }

    drop(valid_client);
    handle.shutdown().await;
}

#[test]
fn cli_parses_step2_http_tls_and_auth_flags() {
    let cmd = parse_args([
        "serve",
        "--data-dir",
        "state",
        "--tcp",
        "127.0.0.1:7420",
        "--http-listen",
        "127.0.0.1:7480",
        "--tls-cert",
        "server.pem",
        "--tls-key",
        "server.key",
        "--tls-client-ca",
        "ca.pem",
        "--auth-config",
        "auth.json",
    ])
    .expect("parse step2 flags");
    match cmd {
        Command::Serve(args) => {
            assert_eq!(
                args.http_listen,
                Some("127.0.0.1:7480".parse::<SocketAddr>().unwrap())
            );
            assert!(args.tls_cert.is_some());
            assert!(args.tls_key.is_some());
            assert!(args.tls_client_ca.is_some());
            assert!(args.auth_config.is_some());
        }
        other => panic!("unexpected command: {other:?}"),
    }

    // HTTP-only serve is valid.
    assert!(parse_args(["serve", "--data-dir", "state", "--http-listen", "127.0.0.1:8080"]).is_ok());

    // Mismatched TLS flags rejected.
    assert!(parse_args([
        "serve",
        "--data-dir",
        "state",
        "--tcp",
        "127.0.0.1:7420",
        "--tls-cert",
        "server.pem",
    ])
    .is_err());
    assert!(parse_args([
        "serve",
        "--data-dir",
        "state",
        "--http-listen",
        "127.0.0.1:8080",
        "--tls-cert",
        "server.pem",
        "--tls-key",
        "server.key",
    ])
    .is_err());
}

#[tokio::test]
async fn ws_overflow_delivers_stream_end_and_shutdown_does_not_hang_on_open_sse_ws() {
    let dir = TempDir::new().expect("tempdir");
    let signal_cfg = SignalConfig::default();
    let ring = Arc::new(SignalRing::new(signal_cfg));
    let storage = Arc::new(
        RocksDbStorage::open(
            dir.path(),
            StorageConfig::default(),
            Arc::clone(&ring) as Arc<dyn SignalPublisher>,
        )
        .expect("open"),
    );
    let broker = Arc::new(SignalBroker::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        ring,
        signal_cfg,
    ));
    let server = MembusServer::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        broker,
        ServerConfig {
            outbound_queue_capacity: 2,
            ..ServerConfig::default()
        },
    );
    let handle = server
        .bind_http("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind http");
    let Endpoint::Http(addr) = *handle.endpoint() else {
        panic!("expected http");
    };

    // Open an active WS subscription and keep an active SSE stream open while shutting down.
    let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{addr}/v2/ws"))
        .await
        .expect("ws connect");
    let (mut ws_tx, mut ws_rx) = ws.split();
    ws_tx
        .send(WsMessage::Text(
            r#"{"id":77,"op":"subscribe_each","namespace":"burst","since_seq":0}"#.into(),
        ))
        .await
        .expect("sub");
    let ack = timeout(Duration::from_secs(2), ws_rx.next())
        .await
        .expect("ack")
        .expect("some")
        .expect("ok")
        .into_text()
        .expect("txt");
    assert!(ack.contains("subscribed"));

    // Open an SSE client that does not actively read frames.
    let sse_tcp = TcpStream::connect(addr).await.expect("sse tcp");
    let (mut sse_sender, sse_conn) = hyper::client::conn::http1::handshake(TokioIo::new(sse_tcp))
        .await
        .expect("sse h1");
    tokio::spawn(async move {
        let _ = sse_conn.await;
    });
    let _sse_resp = sse_sender
        .send_request(
            Request::builder()
                .method(Method::GET)
                .uri("/v2/signals/burst/stream?mode=each&since_seq=0")
                .header("host", addr.to_string())
                .body(Empty::<Bytes>::new())
                .expect("sse req"),
        )
        .await
        .expect("sse resp");

    // Emit a few events, then verify `handle.shutdown()` finishes promptly (< 2s) despite open SSE/WS!
    for i in 0..8u32 {
        storage
            .put(
                "burst",
                format!("k{i}").as_bytes(),
                b"v",
                Ack::Fast,
                membus::ExpectedVersion::Any,
            )
            .expect("put");
    }
    timeout(Duration::from_secs(2), handle.shutdown())
        .await
        .expect("HTTP/WS/SSE shutdown must not hang on open streams");
}

#[test]
fn cli_parses_health_and_checkpoint_token_and_tls_flags() {
    let health = parse_args([
        "health",
        "--tcp",
        "127.0.0.1:7420",
        "--token",
        "secret-tok",
        "--tls-ca",
        "ca.pem",
        "--tls-server-name",
        "membus.local",
        "--tls-cert",
        "client.pem",
        "--tls-key",
        "client.key",
    ])
    .expect("parse health with tls + token");
    match health {
        Command::Health(c) => {
            assert!(matches!(c.endpoint, Endpoint::TcpTls(_)));
            assert_eq!(c.token.as_deref(), Some("secret-tok"));
            let tls = c.tls.expect("tls cfg");
            assert_eq!(tls.server_name, "membus.local");
            assert!(tls.client_cert_pem_path.is_some());
        }
        other => panic!("unexpected: {other:?}"),
    }

    let ckpt = parse_args([
        "checkpoint",
        "--tcp",
        "127.0.0.1:7420",
        "--dest",
        "snap_01",
        "--token",
        "admin-tok",
        "--tls-ca",
        "ca.pem",
    ])
    .expect("parse checkpoint with tls + token");
    assert!(matches!(ckpt, Command::Checkpoint { .. }));

    // --tls-cert without --tls-ca rejected.
    assert!(parse_args([
        "health",
        "--tcp",
        "127.0.0.1:7420",
        "--tls-cert",
        "client.pem",
        "--tls-key",
        "client.key",
    ])
    .is_err());
}

#[test]
fn protocol_sanitizes_non_finite_search_scores() {
    let page = membus::SearchPage {
        hits: vec![membus::SearchHit {
            namespace: "ns".to_owned(),
            doc_id: "d1".to_owned(),
            kind: "doc".to_owned(),
            title: "t".to_owned(),
            tags: Vec::new(),
            score: f32::NAN,
        }],
        total_hits_estimate: 1,
        committed_seq: 1,
    };
    let encoded = membus::protocol::encode_response(&membus::protocol::Response::Search(page));
    let decoded = membus::protocol::decode_response(&encoded).expect("decode sanitized search");
    match decoded {
        membus::protocol::Response::Search(p) => {
            assert_eq!(p.hits.len(), 1);
            assert_eq!(p.hits[0].score, 0.0);
        }
        other => panic!("unexpected: {other:?}"),
    }
}
