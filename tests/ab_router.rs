use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::process::Command as StdCommand;
use std::sync::Arc;
use std::time::Duration;

use axum::body::to_bytes;
use axum::extract::Request as AxumRequest;
use axum::response::{IntoResponse, Response as AxumResponse};
use axum::Router as AxumRouter;
use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full};
use hyper::header::{AUTHORIZATION, CONTENT_TYPE};
use hyper::{HeaderMap, Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use membus::router::{
    parse_router_args, validate_router_bind_security, validate_router_config, AbRouterServer,
    RouterCliCommand, RouterConfig, ShadowQueueConfig, SlotEndpointConfig, SlotId,
};
use membus::{
    AclGrant, AuthConfig, Endpoint, MembusServer, Permission, Principal, RocksDbStorage,
    ServerConfig, ServerHandle, SignalBroker, SignalConfig, SignalPublisher, SignalRing, SlotRole,
    StorageConfig, StorageEngine, TokenBinding,
};
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};

struct EphemeralSlot {
    _dir: TempDir,
    storage: Arc<RocksDbStorage>,
    http_addr: SocketAddr,
    http_handle: ServerHandle,
}

impl EphemeralSlot {
    async fn start(node_id: &str, initial_role: SlotRole, auth: AuthConfig) -> Self {
        let dir = TempDir::new().expect("tempdir");
        let signal_cfg = SignalConfig::default();
        let ring = Arc::new(SignalRing::new(signal_cfg));
        let storage = Arc::new(
            RocksDbStorage::open(
                dir.path(),
                StorageConfig {
                    bg_sync_interval: None,
                    ..StorageConfig::default()
                },
                Arc::clone(&ring) as Arc<dyn SignalPublisher>,
            )
            .expect("open rocksdb"),
        );
        let broker = Arc::new(SignalBroker::new(
            Arc::clone(&storage) as Arc<dyn StorageEngine>,
            ring,
            signal_cfg,
        ));
        let cfg = ServerConfig {
            initial_slot_role: initial_role,
            node_id: Some(node_id.to_owned()),
            ..ServerConfig::default()
        };
        let server = MembusServer::new(Arc::clone(&storage) as Arc<dyn StorageEngine>, broker, cfg)
            .with_auth(auth);

        let http_handle = server
            .bind_http("127.0.0.1:0".parse::<SocketAddr>().unwrap())
            .await
            .expect("bind http");
        let Endpoint::Http(http_addr) = *http_handle.endpoint() else {
            panic!("expected http endpoint");
        };

        Self {
            _dir: dir,
            storage,
            http_addr,
            http_handle,
        }
    }

    async fn shutdown(self) {
        self.http_handle.shutdown().await;
    }
}

fn test_slot_auth() -> AuthConfig {
    AuthConfig::enforced(vec![
        TokenBinding {
            token: "tok-client".to_owned(),
            principal: Principal {
                id: "client".to_owned(),
                grants: vec![AclGrant {
                    namespace_pattern: "*".to_owned(),
                    key_prefix: Vec::new(),
                    permissions: vec![Permission::Read, Permission::Write],
                }],
            },
        },
        TokenBinding {
            token: "tok-router-admin".to_owned(),
            principal: Principal {
                id: "ab-router-control".to_owned(),
                grants: vec![AclGrant {
                    namespace_pattern: "*".to_owned(),
                    key_prefix: Vec::new(),
                    permissions: vec![Permission::Admin],
                }],
            },
        },
    ])
}

async fn http_json(
    addr: SocketAddr,
    method: Method,
    path: &str,
    bearer: Option<&str>,
    extra_headers: &[(&str, &str)],
    body: Option<serde_json::Value>,
) -> (StatusCode, HeaderMap, serde_json::Value) {
    let stream = TcpStream::connect(addr).await.expect("connect");
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .expect("http1 handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });

    let mut builder = Request::builder()
        .method(method)
        .uri(path)
        .header("host", addr.to_string());
    if let Some(tok) = bearer {
        builder = builder.header(AUTHORIZATION, format!("Bearer {tok}"));
    }
    for (k, v) in extra_headers {
        builder = builder.header(*k, *v);
    }
    let req = if let Some(json) = body {
        builder
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(json.to_string())).boxed())
            .expect("req")
    } else {
        builder
            .body(
                Empty::<Bytes>::new()
                    .map_err(|never| match never {})
                    .boxed(),
            )
            .expect("req")
    };

    let resp = sender.send_request(req).await.expect("send");
    let status = resp.status();
    let headers = resp.headers().clone();
    let raw = resp.into_body().collect().await.expect("body").to_bytes();
    let parsed = if raw.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&raw).unwrap_or_else(|_| {
            serde_json::Value::String(String::from_utf8_lossy(&raw).into_owned())
        })
    };
    (status, headers, parsed)
}

async fn wait_for_status(
    admin_addr: SocketAddr,
    admin_tok: &str,
    predicate: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    for _ in 0..80 {
        let (st, _, status) = http_json(
            admin_addr,
            Method::GET,
            "/admin/status",
            Some(admin_tok),
            &[],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        if predicate(&status) {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    let (_, _, last) = http_json(
        admin_addr,
        Method::GET,
        "/admin/status",
        Some(admin_tok),
        &[],
        None,
    )
    .await;
    panic!("timed out waiting for router status predicate; last status: {last}");
}

#[test]
fn cli_help_config_validation_and_fail_closed_ipv4_ipv6_bind_guard() {
    for flag in ["--help", "-h", "help"] {
        let parsed = parse_router_args([flag]).expect("help should parse");
        assert!(matches!(parsed, RouterCliCommand::Help(_)));
    }

    // Verify compiled binary exits 0 and writes usage to stdout on --help.
    let bin = env!("CARGO_BIN_EXE_membus-ab-router");
    let out = StdCommand::new(bin)
        .arg("--help")
        .output()
        .expect("run membus-ab-router --help");
    assert!(out.status.success(), "expected exit 0 on --help");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.contains("--data-listen"));
    assert!(stdout.contains("--admin-listen"));
    assert!(stdout.contains("--allow-insecure-non-loopback"));

    // Fail-closed non-loopback IPv4 & IPv6 listeners without auth.
    for non_loopback in [
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        IpAddr::V6(Ipv6Addr::UNSPECIFIED),
    ] {
        let cfg_data = RouterConfig {
            data_listen: SocketAddr::new(non_loopback, 8080),
            ..RouterConfig::default()
        };
        assert!(validate_router_bind_security(&cfg_data).is_err());

        let mut cfg_admin = RouterConfig {
            admin_listen: SocketAddr::new(non_loopback, 8081),
            ..RouterConfig::default()
        };
        assert!(validate_router_bind_security(&cfg_admin).is_err());

        cfg_admin.allow_insecure_non_loopback = true;
        assert!(validate_router_bind_security(&cfg_admin).is_ok());
    }

    // Succeeds on IPv6 [::] when auth tokens are configured on both surfaces.
    let mut cfg_authed = RouterConfig {
        data_listen: SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 8080),
        admin_listen: SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), 8081),
        admin_inline_token: Some("tok-adm".to_owned()),
        ..RouterConfig::default()
    };
    cfg_authed.slot_a.inline_token = Some("tok-a".to_owned());
    cfg_authed.slot_b.inline_token = Some("tok-b".to_owned());
    assert!(validate_router_bind_security(&cfg_authed).is_ok());

    // Config validation rejects zero timeouts, identical slot URLs, and query/fragment base URLs.
    let bad_timeout = RouterConfig {
        primary_timeout_ms: 0,
        ..RouterConfig::default()
    };
    assert!(validate_router_config(&bad_timeout).is_err());

    let bad_catchup = RouterConfig {
        shadow: ShadowQueueConfig {
            catchup_interval_ms: 0,
            ..ShadowQueueConfig::default()
        },
        ..RouterConfig::default()
    };
    assert!(validate_router_config(&bad_catchup).is_err());

    let identical_slots = RouterConfig {
        slot_a: SlotEndpointConfig::new("http://127.0.0.1:18080/"),
        slot_b: SlotEndpointConfig::new("http://127.0.0.1:18080"),
        ..RouterConfig::default()
    };
    assert!(validate_router_config(&identical_slots).is_err());

    let query_url = RouterConfig {
        slot_a: SlotEndpointConfig::new("http://127.0.0.1:18080?foo=1"),
        ..RouterConfig::default()
    };
    assert!(validate_router_config(&query_url).is_err());
}

#[tokio::test]
async fn ab_router_auth_isolation_control_plane_blocking_updates_acl_and_limits() {
    let slot_a = EphemeralSlot::start("slot-a", SlotRole::Active, test_slot_auth()).await;
    let slot_b = EphemeralSlot::start("slot-b", SlotRole::Shadow, test_slot_auth()).await;

    let mut cfg = RouterConfig {
        data_listen: "127.0.0.1:0".parse().unwrap(),
        admin_listen: "127.0.0.1:0".parse().unwrap(),
        slot_a: SlotEndpointConfig::new(format!("http://{}", slot_a.http_addr)),
        slot_b: SlotEndpointConfig::new(format!("http://{}", slot_b.http_addr)),
        initial_primary: SlotId::SlotA,
        shadow_enabled: true,
        admin_inline_token: Some("adm-sec".to_owned()),
        max_body_bytes: 2048,
        ..RouterConfig::default()
    };
    cfg.slot_a.inline_token = Some("tok-router-admin".to_owned());
    cfg.slot_b.inline_token = Some("tok-router-admin".to_owned());

    let router = AbRouterServer::start(cfg).await.expect("start router");

    // 1. Unauthenticated data-plane request MUST return 401 (router NEVER injects admin token!).
    let (st, _, _) = http_json(
        router.data_addr(),
        Method::PUT,
        "/v2/kv/orders/unauth",
        None,
        &[],
        Some(serde_json::json!({ "value_utf8": "should-not-commit" })),
    )
    .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);
    assert!(slot_a.storage.get("orders", b"unauth").unwrap().is_none());

    // 2. Control-plane paths MUST be blocked with 403 Forbidden on the :8080 data surface,
    //    and traversal/obfuscated variants MUST be blocked with 400 or 403!
    for (method, path) in [
        (Method::POST, "/v2/admin/role"),
        (Method::POST, "/v2/checkpoint"),
        (Method::GET, "/v2/updates"),
        (Method::POST, "/v2/replication/apply"),
        (Method::POST, "/V2/admin/role"),
        (Method::POST, "/v2/%61dmin/role"),
    ] {
        let (st, _, body) = http_json(
            router.data_addr(),
            method,
            path,
            Some("tok-client"),
            &[],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::FORBIDDEN, "expected 403 on {path}: {body}");
        assert_eq!(body["error"], "forbidden_control_plane_path");
    }

    for bad_path in ["/v2/./admin/role", "//v2/updates", "/v2/kv/orders/a%2fb"] {
        let (st, _, body) = http_json(
            router.data_addr(),
            Method::GET,
            bad_path,
            Some("tok-client"),
            &[],
            None,
        )
        .await;
        assert_eq!(
            st,
            StatusCode::BAD_REQUEST,
            "expected 400 on {bad_path}: {body}"
        );
        assert_eq!(body["error"], "invalid_argument");
    }

    // 3. Direct GET /v2/updates ACL + limit=0 validation on Slot A.
    let (st, _, _) = http_json(
        slot_a.http_addr,
        Method::GET,
        "/v2/updates?since_seq=0&limit=10",
        Some("tok-client"),
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    let (st, _, bad_limit) = http_json(
        slot_a.http_addr,
        Method::GET,
        "/v2/updates?since_seq=0&limit=0",
        Some("tok-router-admin"),
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(bad_limit["error"], "invalid_argument");

    // 4. Streaming paths (/v2/ws, /v2/signals/.../stream) fail fast with 501 Not Implemented.
    for stream_path in ["/v2/ws", "/v2/signals/orders/stream"] {
        let (st, _, body) = http_json(
            router.data_addr(),
            Method::GET,
            stream_path,
            Some("tok-client"),
            &[],
            None,
        )
        .await;
        assert_eq!(
            st,
            StatusCode::NOT_IMPLEMENTED,
            "path={stream_path}: {body}"
        );
    }

    // 5. Uppercase X-Membus-Route-Origin triggers 409 echo_loop_rejected; duplicate triggers 400.
    let (st, hdrs, echo_upper) = http_json(
        router.data_addr(),
        Method::PUT,
        "/v2/kv/orders/upper-echo",
        Some("tok-client"),
        &[("X-Membus-Route-Origin", "ROUTER:slot-a")],
        Some(serde_json::json!({ "value_utf8": "blocked" })),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(echo_upper["error"], "echo_loop_rejected");
    assert_eq!(hdrs.get("x-membus-echo-loop").unwrap(), "rejected");

    let (st, _, dup_err) = http_json(
        router.data_addr(),
        Method::PUT,
        "/v2/kv/orders/dup-origin",
        Some("tok-client"),
        &[
            ("X-Membus-Route-Origin", "client-a"),
            ("X-Membus-Route-Origin", "client-b"),
        ],
        Some(serde_json::json!({ "value_utf8": "blocked" })),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    assert_eq!(dup_err["error"], "invalid_argument");

    // 6. Oversized body (> 2048 bytes) rejected with 413 Payload Too Large.
    let oversized = "x".repeat(4096);
    let (st, _, body_err) = http_json(
        router.data_addr(),
        Method::PUT,
        "/v2/kv/orders/huge",
        Some("tok-client"),
        &[],
        Some(serde_json::json!({ "value_utf8": oversized })),
    )
    .await;
    assert_eq!(st, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(body_err["error"], "frame_too_large");

    router.shutdown().await;
    slot_a.shutdown().await;
    slot_b.shutdown().await;
}

#[tokio::test]
async fn ab_router_bounded_queue_shedding_returns_dropped_and_never_fails_primary() {
    let slot_a = EphemeralSlot::start("slot-a", SlotRole::Active, test_slot_auth()).await;

    // Slow mock shadow endpoint holding /v2/replication/apply for 350ms so max_ops=1 saturates.
    let slow_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind slow");
    let slow_addr = slow_listener.local_addr().expect("addr");
    let slow_app = AxumRouter::new().fallback(|_req: AxumRequest| async {
        tokio::time::sleep(Duration::from_millis(350)).await;
        StatusCode::SERVICE_UNAVAILABLE
    });
    let slow_task = tokio::spawn(async move {
        let _ = axum::serve(slow_listener, slow_app).await;
    });

    let mut cfg = RouterConfig {
        data_listen: "127.0.0.1:0".parse().unwrap(),
        admin_listen: "127.0.0.1:0".parse().unwrap(),
        slot_a: SlotEndpointConfig::new(format!("http://{}", slot_a.http_addr)),
        slot_b: SlotEndpointConfig::new(format!("http://{slow_addr}")),
        initial_primary: SlotId::SlotA,
        shadow_enabled: true,
        admin_inline_token: Some("adm-shed".to_owned()),
        shadow: ShadowQueueConfig {
            max_ops: 1,
            max_bytes: 1024 * 1024,
            timeout_ms: 400,
            catchup_interval_ms: 60_000,
            max_backoff_ms: 500,
        },
        ..RouterConfig::default()
    };
    cfg.slot_a.inline_token = Some("tok-router-admin".to_owned());
    cfg.slot_b.inline_token = Some("tok-router-admin".to_owned());

    let router = AbRouterServer::start(cfg).await.expect("start ab-router");

    let mut saw_dropped = false;
    for i in 0..6 {
        let (st, hdrs, body) = http_json(
            router.data_addr(),
            Method::PUT,
            &format!("/v2/kv/orders/burst-{i}"),
            Some("tok-client"),
            &[],
            Some(serde_json::json!({ "value_utf8": format!("val-{i}"), "ack": "Flush" })),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "primary write {i} failed: {body}");
        assert_eq!(hdrs.get("x-route-primary").unwrap(), "slot-a");
        if hdrs
            .get("x-route-shadow-sync")
            .and_then(|v| v.to_str().ok())
            == Some("dropped")
        {
            saw_dropped = true;
        }
    }
    assert!(
        saw_dropped,
        "expected bounded queue (max_ops=1) to emit X-Route-Shadow-Sync: dropped under burst"
    );

    let status = wait_for_status(router.admin_addr(), "adm-shed", |s| {
        s["parity_dirty"] == true && s["shadow_dropped_total"].as_u64().unwrap_or(0) >= 1
    })
    .await;
    assert_eq!(status["parity_dirty"], true);

    let (st, _, cutover_err) = http_json(
        router.admin_addr(),
        Method::POST,
        "/admin/cutover",
        Some("adm-shed"),
        &[],
        Some(serde_json::json!({ "namespaces": ["orders"] })),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(cutover_err["error"], "parity_dirty");

    router.shutdown().await;
    slow_task.abort();
    slot_a.shutdown().await;
}

#[tokio::test]
async fn ab_router_idempotent_replay_does_not_resurrect_older_value_on_shadow() {
    let slot_a = EphemeralSlot::start("slot-a", SlotRole::Active, test_slot_auth()).await;
    let slot_b = EphemeralSlot::start("slot-b", SlotRole::Shadow, test_slot_auth()).await;

    let mut cfg = RouterConfig {
        data_listen: "127.0.0.1:0".parse().unwrap(),
        admin_listen: "127.0.0.1:0".parse().unwrap(),
        slot_a: SlotEndpointConfig::new(format!("http://{}", slot_a.http_addr)),
        slot_b: SlotEndpointConfig::new(format!("http://{}", slot_b.http_addr)),
        initial_primary: SlotId::SlotA,
        shadow_enabled: true,
        admin_inline_token: Some("adm-idem".to_owned()),
        ..RouterConfig::default()
    };
    cfg.slot_a.inline_token = Some("tok-router-admin".to_owned());
    cfg.slot_b.inline_token = Some("tok-router-admin".to_owned());

    let router = AbRouterServer::start(cfg).await.expect("start router");

    // 1. Write v1 with idem-k1.
    let (st, _, _) = http_json(
        router.data_addr(),
        Method::PUT,
        "/v2/kv/orders/item",
        Some("tok-client"),
        &[("X-Membus-Idempotency-Key", "idem-k1")],
        Some(serde_json::json!({ "value_utf8": "v1-old", "ack": "Flush" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    wait_for_status(router.admin_addr(), "adm-idem", |s| {
        s["shadow_applied_total"].as_u64().unwrap_or(0) >= 1 && s["shadow_queue_depth"] == 0
    })
    .await;

    // 2. Write newer v2 with idem-k2.
    let (st, _, _) = http_json(
        router.data_addr(),
        Method::PUT,
        "/v2/kv/orders/item",
        Some("tok-client"),
        &[("X-Membus-Idempotency-Key", "idem-k2")],
        Some(serde_json::json!({ "value_utf8": "v2-newer", "ack": "Flush" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let applied_before_replay = wait_for_status(router.admin_addr(), "adm-idem", |s| {
        s["shadow_applied_total"].as_u64().unwrap_or(0) >= 2 && s["shadow_queue_depth"] == 0
    })
    .await["shadow_applied_total"]
        .as_u64()
        .unwrap();

    // 3. Replay older idem-k1 (v1-old). Primary replays idempotently; router MUST NOT enqueue to Shadow!
    let (st, hdrs, _) = http_json(
        router.data_addr(),
        Method::PUT,
        "/v2/kv/orders/item",
        Some("tok-client"),
        &[("X-Membus-Idempotency-Key", "idem-k1")],
        Some(serde_json::json!({ "value_utf8": "v1-old", "ack": "Flush" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(hdrs.get("x-membus-idempotent-replay").unwrap(), "true");

    tokio::time::sleep(Duration::from_millis(80)).await;
    let status_after = wait_for_status(router.admin_addr(), "adm-idem", |s| {
        s["shadow_queue_depth"] == 0
    })
    .await;
    assert_eq!(
        status_after["shadow_applied_total"].as_u64().unwrap(),
        applied_before_replay,
        "idempotent replay must not trigger extra shadow apply"
    );

    let rec_b = slot_b
        .storage
        .get("orders", b"item")
        .unwrap()
        .expect("record on slot_b");
    assert_eq!(rec_b.value, b"v2-newer");

    router.shutdown().await;
    slot_a.shutdown().await;
    slot_b.shutdown().await;
}

#[tokio::test]
async fn ab_router_stage4_promotion_failure_compensates_and_updates_primary_epoch() {
    let slot_a = EphemeralSlot::start("slot-a", SlotRole::Active, test_slot_auth()).await;
    let slot_b = EphemeralSlot::start("slot-b", SlotRole::Shadow, test_slot_auth()).await;

    // Fault-injecting proxy in front of Slot B: rejects ONLY `POST /v2/admin/role` with 500.
    let fault_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind fault");
    let fault_addr = fault_listener.local_addr().expect("addr");
    let real_b_addr = slot_b.http_addr;
    let fault_app = AxumRouter::new().fallback(move |req: AxumRequest| async move {
        if req.uri().path() == "/v2/admin/role" {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                "injected stage-4 promotion failure",
            )
                .into_response();
        }
        let method = req.method().clone();
        let pq = req
            .uri()
            .path_and_query()
            .map(|p| p.as_str().to_owned())
            .unwrap_or_else(|| "/".to_owned());
        let headers = req.headers().clone();
        let body = to_bytes(req.into_body(), 1024 * 1024)
            .await
            .unwrap_or_default();

        let stream = TcpStream::connect(real_b_addr).await.unwrap();
        let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = conn.await;
        });
        let mut b = Request::builder()
            .method(method)
            .uri(pq)
            .header("host", real_b_addr.to_string());
        for (k, v) in &headers {
            if k != "host" && k != "content-length" {
                b = b.header(k, v);
            }
        }
        let resp = sender
            .send_request(b.body(Full::new(body)).unwrap())
            .await
            .unwrap();
        let st = resp.status();
        let resp_bytes = resp.into_body().collect().await.unwrap().to_bytes();
        AxumResponse::builder()
            .status(st)
            .header(CONTENT_TYPE, "application/json")
            .body(axum::body::Body::from(resp_bytes))
            .unwrap()
    });
    let fault_task = tokio::spawn(async move {
        let _ = axum::serve(fault_listener, fault_app).await;
    });

    let mut cfg = RouterConfig {
        data_listen: "127.0.0.1:0".parse().unwrap(),
        admin_listen: "127.0.0.1:0".parse().unwrap(),
        slot_a: SlotEndpointConfig::new(format!("http://{}", slot_a.http_addr)),
        slot_b: SlotEndpointConfig::new(format!("http://{fault_addr}")),
        initial_primary: SlotId::SlotA,
        shadow_enabled: true,
        admin_inline_token: Some("adm-comp".to_owned()),
        ..RouterConfig::default()
    };
    cfg.slot_a.inline_token = Some("tok-router-admin".to_owned());
    cfg.slot_b.inline_token = Some("tok-router-admin".to_owned());

    let router = AbRouterServer::start(cfg).await.expect("start router");

    // Seed and mirror one key cleanly before cutover attempt.
    let (st, _, _) = http_json(
        router.data_addr(),
        Method::PUT,
        "/v2/kv/orders/pre-comp",
        Some("tok-client"),
        &[],
        Some(serde_json::json!({ "value_utf8": "ok", "ack": "Flush" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    wait_for_status(router.admin_addr(), "adm-comp", |s| {
        s["shadow_applied_total"].as_u64().unwrap_or(0) >= 1 && s["shadow_queue_depth"] == 0
    })
    .await;

    // Attempt cutover: Stage 4b fails on Slot B -> router probes Slot B (confirmed Shadow) ->
    // restores Slot A (Active, epoch 3) AND updates cached primary_role_epoch = 3!
    let (st, _, cut_err) = http_json(
        router.admin_addr(),
        Method::POST,
        "/admin/cutover",
        Some("adm-comp"),
        &[],
        Some(serde_json::json!({ "namespaces": ["orders"] })),
    )
    .await;
    assert_eq!(
        st,
        StatusCode::BAD_GATEWAY,
        "expected 502 on injected error: {cut_err}"
    );

    // Verify Slot A was restored to Active at epoch 3 and subsequent writes succeed (not bricked!).
    let (_, _, health_a) = http_json(
        slot_a.http_addr,
        Method::GET,
        "/v2/health",
        Some("tok-router-admin"),
        &[],
        None,
    )
    .await;
    assert_eq!(health_a["slot_role"], "active");
    assert_eq!(health_a["role_epoch"], 3);

    let (st, _, post_write) = http_json(
        router.data_addr(),
        Method::PUT,
        "/v2/kv/orders/post-comp",
        Some("tok-client"),
        &[],
        Some(serde_json::json!({ "value_utf8": "still-writable", "ack": "Flush" })),
    )
    .await;
    assert_eq!(
        st,
        StatusCode::OK,
        "post-compensation write MUST NOT brick with stale role_epoch: {post_write}"
    );

    router.shutdown().await;
    fault_task.abort();
    slot_a.shutdown().await;
    slot_b.shutdown().await;
}

#[tokio::test]
async fn ab_router_full_e2e_multi_ns_shadow_503_catchup_parity_cutover_auto_restart_and_rollback() {
    // Slot A Active, Slot B initially Draining (returns 503 on /v2/replication/apply).
    let slot_a = EphemeralSlot::start("slot-a", SlotRole::Active, test_slot_auth()).await;
    let slot_b = EphemeralSlot::start("slot-b", SlotRole::Draining, test_slot_auth()).await;

    let mut cfg = RouterConfig {
        data_listen: "127.0.0.1:0".parse().unwrap(),
        admin_listen: "127.0.0.1:0".parse().unwrap(),
        slot_a: SlotEndpointConfig::new(format!("http://{}", slot_a.http_addr)),
        slot_b: SlotEndpointConfig::new(format!("http://{}", slot_b.http_addr)),
        initial_primary: SlotId::SlotA,
        shadow_enabled: true,
        admin_inline_token: Some("adm-token-e2e".to_owned()),
        shadow: ShadowQueueConfig {
            max_ops: 64,
            max_bytes: 4 * 1024 * 1024,
            timeout_ms: 1000,
            catchup_interval_ms: 60_000,
            max_backoff_ms: 500,
        },
        ..RouterConfig::default()
    };
    cfg.slot_a.inline_token = Some("tok-router-admin".to_owned());
    cfg.slot_b.inline_token = Some("tok-router-admin".to_owned());

    let router = AbRouterServer::start(cfg.clone())
        .await
        .expect("start ab-router");

    // Admin health probe works without auth; /admin/status enforces admin bearer token.
    let (st, _, health) = http_json(
        router.admin_addr(),
        Method::GET,
        "/admin/health",
        None,
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(health["ok"], true);
    assert_eq!(health["primary_slot"], "slot-a");

    let (st, _, _) = http_json(
        router.admin_addr(),
        Method::GET,
        "/admin/status",
        None,
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    // (a) Multi-namespace writes (orders + invoices) return 200 OK while Slot B returns 503!
    let (st, hdrs, _) = http_json(
        router.data_addr(),
        Method::PUT,
        "/v2/kv/orders/ord-1",
        Some("tok-client"),
        &[],
        Some(serde_json::json!({ "value_utf8": "order-one", "ack": "Flush" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(hdrs.get("x-route-primary").unwrap(), "slot-a");
    assert!(hdrs.get("x-membus-commit-ts-ms").is_some());

    let (st, _, _) = http_json(
        router.data_addr(),
        Method::PUT,
        "/v2/kv/invoices/inv-1",
        Some("tok-client"),
        &[],
        Some(serde_json::json!({ "value_utf8": "invoice-one", "ack": "Flush" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // Multi-key TX + delete in orders.
    let (st, _, _) = http_json(
        router.data_addr(),
        Method::POST,
        "/v2/tx",
        Some("tok-client"),
        &[],
        Some(serde_json::json!({
            "ack": "Flush",
            "guards": [],
            "mutations": [
                {
                    "mutation": {
                        "namespace": "orders",
                        "key": b"ord-2".to_vec(),
                        "op": { "Put": { "value": b"order-two".to_vec() } }
                    },
                    "indexes": null,
                    "search_op": null
                },
                {
                    "mutation": {
                        "namespace": "orders",
                        "key": b"ord-3".to_vec(),
                        "op": { "Put": { "value": b"order-three".to_vec() } }
                    },
                    "indexes": null,
                    "search_op": null
                }
            ]
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, _, _) = http_json(
        router.data_addr(),
        Method::DELETE,
        "/v2/kv/orders/ord-3",
        Some("tok-client"),
        &[],
        Some(serde_json::json!({ "ack": "Flush" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let dirty_status = wait_for_status(router.admin_addr(), "adm-token-e2e", |s| {
        s["parity_dirty"] == true && s["shadow_errors_total"].as_u64().unwrap_or(0) >= 1
    })
    .await;
    assert_eq!(dirty_status["parity_dirty"], true);

    let (st, _, cutover_blocked) = http_json(
        router.admin_addr(),
        Method::POST,
        "/admin/cutover",
        Some("adm-token-e2e"),
        &[],
        Some(serde_json::json!({ "namespaces": ["orders", "invoices"] })),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(cutover_blocked["error"], "parity_dirty");

    // (b) Echo-loop inbound write tagged with router origin MUST be rejected!
    let seq_a_before_echo = slot_a.storage.latest_seq();
    let (st, hdrs, echo_err) = http_json(
        router.data_addr(),
        Method::PUT,
        "/v2/kv/orders/echo-key",
        Some("tok-client"),
        &[("X-Membus-Route-Origin", "router:slot-a")],
        Some(serde_json::json!({ "value_utf8": "looped-write" })),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(echo_err["error"], "echo_loop_rejected");
    assert_eq!(hdrs.get("x-membus-echo-loop").unwrap(), "rejected");
    assert_eq!(slot_a.storage.latest_seq(), seq_a_before_echo);

    // (c) Transition Slot B Draining -> Shadow -> Multi-NS Catchup -> Parity -> Cutover!
    let (st, _, _) = http_json(
        slot_b.http_addr,
        Method::POST,
        "/v2/admin/role",
        Some("tok-router-admin"),
        &[],
        Some(serde_json::json!({
            "expected_role": "draining",
            "expected_role_epoch": 1,
            "new_role": "shadow"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, _, catchup_resp) = http_json(
        router.admin_addr(),
        Method::POST,
        "/admin/catchup",
        Some("adm-token-e2e"),
        &[],
        Some(serde_json::json!({ "namespaces": ["orders", "invoices"] })),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "catchup failed: {catchup_resp}");
    assert_eq!(catchup_resp["caught_up"], true);
    assert_eq!(catchup_resp["parity_verified"], true);
    assert_eq!(catchup_resp["parity_dirty"], false);

    let (st, _, parity_resp) = http_json(
        router.admin_addr(),
        Method::POST,
        "/admin/parity",
        Some("adm-token-e2e"),
        &[],
        Some(serde_json::json!({ "namespaces": ["orders", "invoices"], "mode": "full" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(parity_resp["match"], true);
    assert_eq!(parity_resp["namespaces"].as_array().unwrap().len(), 2);

    let (st, _, cutover_resp) = http_json(
        router.admin_addr(),
        Method::POST,
        "/admin/cutover",
        Some("adm-token-e2e"),
        &[],
        Some(serde_json::json!({ "namespaces": ["orders", "invoices"], "mode": "full" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "cutover failed: {cutover_resp}");
    assert_eq!(cutover_resp["new_primary"], "slot-b");
    assert_eq!(cutover_resp["new_shadow"], "slot-a");

    // Verify startup `--primary auto` discovers `slot-b` as Active (no reversion to slot-a on restart!).
    router.shutdown().await;
    let mut auto_cfg = cfg;
    auto_cfg.primary_auto = true;
    auto_cfg.initial_primary = SlotId::SlotA; // intentionally opposite of live Active slot-b
    let router = AbRouterServer::start(auto_cfg)
        .await
        .expect("restart router with primary_auto");
    assert_eq!(router.runtime().primary_slot(), SlotId::SlotB);

    // Clear any initial seq-difference dirty flag via catchup before reverse-shadow write + rollback.
    if router.runtime().is_parity_dirty() {
        let (st, _, cu) = http_json(
            router.admin_addr(),
            Method::POST,
            "/admin/catchup",
            Some("adm-token-e2e"),
            &[],
            Some(serde_json::json!({ "namespaces": ["orders", "invoices"] })),
        )
        .await;
        assert_eq!(st, StatusCode::OK, "post-restart catchup failed: {cu}");
    }

    // Write NEW data on Slot B -> reverse-shadows back to Slot A.
    let (st, hdrs, _) = http_json(
        router.data_addr(),
        Method::PUT,
        "/v2/kv/orders/ord-4-reverse-shadow",
        Some("tok-client"),
        &[],
        Some(serde_json::json!({ "value_utf8": "written-on-slot-b", "ack": "Flush" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(hdrs.get("x-route-primary").unwrap(), "slot-b");
    assert_eq!(hdrs.get("x-route-shadow-sync").unwrap(), "queued");

    wait_for_status(router.admin_addr(), "adm-token-e2e", |s| {
        s["shadow_queue_depth"] == 0 && s["parity_dirty"] == false
    })
    .await;

    let mirrored_on_a = slot_a
        .storage
        .get("orders", b"ord-4-reverse-shadow")
        .unwrap()
        .expect("reverse-shadow write must exist on slot-a");
    assert_eq!(mirrored_on_a.value, b"written-on-slot-b");

    // Execute POST /admin/rollback -> fences B, promotes A with ZERO lost writes!
    let (st, _, rb_resp) = http_json(
        router.admin_addr(),
        Method::POST,
        "/admin/rollback",
        Some("adm-token-e2e"),
        &[],
        Some(serde_json::json!({ "namespaces": ["orders", "invoices"] })),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "rollback failed: {rb_resp}");
    assert_eq!(rb_resp["new_primary"], "slot-a");

    for (ns, key, expected_val) in [
        ("orders", "ord-1", "order-one"),
        ("orders", "ord-2", "order-two"),
        ("orders", "ord-4-reverse-shadow", "written-on-slot-b"),
        ("invoices", "inv-1", "invoice-one"),
    ] {
        let (st, hdrs, rec) = http_json(
            router.data_addr(),
            Method::GET,
            &format!("/v2/kv/{ns}/{key}"),
            Some("tok-client"),
            &[],
            None,
        )
        .await;
        assert_eq!(st, StatusCode::OK, "missing {ns}/{key} after rollback");
        assert_eq!(hdrs.get("x-route-primary").unwrap(), "slot-a");
        let got_bytes: Vec<u8> = rec["value"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap() as u8)
            .collect();
        assert_eq!(got_bytes, expected_val.as_bytes());
    }

    let (st, _, _) = http_json(
        router.data_addr(),
        Method::GET,
        "/v2/kv/orders/ord-3",
        Some("tok-client"),
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::NOT_FOUND);

    router.shutdown().await;
    slot_a.shutdown().await;
    slot_b.shutdown().await;
}

#[tokio::test]
async fn ab_router_live_concurrent_writes_survive_cutover_and_rollback_with_zero_loss() {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

    let slot_a = EphemeralSlot::start("slot-a", SlotRole::Active, test_slot_auth()).await;
    let slot_b = EphemeralSlot::start("slot-b", SlotRole::Shadow, test_slot_auth()).await;

    let mut cfg = RouterConfig {
        data_listen: "127.0.0.1:0".parse().unwrap(),
        admin_listen: "127.0.0.1:0".parse().unwrap(),
        slot_a: SlotEndpointConfig::new(format!("http://{}", slot_a.http_addr)),
        slot_b: SlotEndpointConfig::new(format!("http://{}", slot_b.http_addr)),
        initial_primary: SlotId::SlotA,
        shadow_enabled: true,
        admin_inline_token: Some("adm-live".to_owned()),
        ..RouterConfig::default()
    };
    cfg.slot_a.inline_token = Some("tok-router-admin".to_owned());
    cfg.slot_b.inline_token = Some("tok-router-admin".to_owned());

    let router = AbRouterServer::start(cfg).await.expect("start router");
    let data_addr = router.data_addr();
    let stop = Arc::new(AtomicBool::new(false));
    let seq = Arc::new(AtomicU64::new(0));
    let acked = Arc::new(tokio::sync::Mutex::new(Vec::<u64>::new()));

    let mut writers = Vec::new();
    for _ in 0..4 {
        let stop = Arc::clone(&stop);
        let seq = Arc::clone(&seq);
        let acked = Arc::clone(&acked);
        writers.push(tokio::spawn(async move {
            let mut sender: Option<hyper::client::conn::http1::SendRequest<Full<Bytes>>> = None;
            while !stop.load(Ordering::Relaxed) {
                let id = seq.fetch_add(1, Ordering::Relaxed) + 1;
                let idem = format!("live-idem-{id}");
                let body_bytes =
                    Bytes::from(serde_json::json!({ "value_utf8": format!("v-{id}") }).to_string());
                let path = format!("/v2/kv/orders/live-{id}");
                for _ in 0..80 {
                    if sender.as_ref().is_none_or(|s| s.is_closed()) {
                        if let Ok(stream) = TcpStream::connect(data_addr).await {
                            if let Ok((s, conn)) =
                                hyper::client::conn::http1::handshake(TokioIo::new(stream)).await
                            {
                                tokio::spawn(async move {
                                    let _ = conn.await;
                                });
                                sender = Some(s);
                            }
                        }
                    }
                    let req = Request::builder()
                        .method(Method::PUT)
                        .uri(&path)
                        .header("host", data_addr.to_string())
                        .header(AUTHORIZATION, "Bearer tok-client")
                        .header(CONTENT_TYPE, "application/json")
                        .header("X-Membus-Idempotency-Key", idem.as_str())
                        .body(Full::new(body_bytes.clone()))
                        .expect("req");
                    let ok = match sender.as_mut() {
                        Some(s) => match s.send_request(req).await {
                            Ok(resp) => {
                                let st = resp.status();
                                let _ = resp.into_body().collect().await;
                                st == StatusCode::OK
                            }
                            Err(_) => {
                                sender = None;
                                false
                            }
                        },
                        None => false,
                    };
                    if ok {
                        acked.lock().await.push(id);
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }));
    }

    tokio::time::sleep(Duration::from_millis(250)).await;

    let (st, _, cut_body) = http_json(
        router.admin_addr(),
        Method::POST,
        "/admin/cutover",
        Some("adm-live"),
        &[],
        Some(serde_json::json!({ "namespaces": ["orders"] })),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "live cutover failed: {cut_body}");
    assert_eq!(cut_body["new_primary"], "slot-b");

    tokio::time::sleep(Duration::from_millis(250)).await;

    let (st, _, rb_body) = http_json(
        router.admin_addr(),
        Method::POST,
        "/admin/rollback",
        Some("adm-live"),
        &[],
        Some(serde_json::json!({ "namespaces": ["orders"] })),
    )
    .await;
    assert_eq!(st, StatusCode::OK, "live rollback failed: {rb_body}");
    assert_eq!(rb_body["new_primary"], "slot-a");

    tokio::time::sleep(Duration::from_millis(150)).await;
    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.await.expect("writer join");
    }

    wait_for_status(router.admin_addr(), "adm-live", |s| {
        s["shadow_queue_depth"] == 0 && s["parity_dirty"] == false
    })
    .await;

    let (st, _, parity) = http_json(
        router.admin_addr(),
        Method::POST,
        "/admin/parity",
        Some("adm-live"),
        &[],
        Some(serde_json::json!({ "namespaces": ["orders"], "mode": "full" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(parity["match"], true, "parity mismatch: {parity}");

    let all_acked = acked.lock().await;
    assert!(
        all_acked.len() >= 20,
        "expected >= 20 live acked writes, got {}",
        all_acked.len()
    );
    for id in all_acked.iter() {
        let key = format!("live-{id}");
        let expected = format!("v-{id}");
        let rec_a = slot_a
            .storage
            .get("orders", key.as_bytes())
            .unwrap()
            .expect("key on A");
        let rec_b = slot_b
            .storage
            .get("orders", key.as_bytes())
            .unwrap()
            .expect("key on B");
        assert_eq!(rec_a.value, expected.as_bytes());
        assert_eq!(rec_b.value, expected.as_bytes());
    }

    router.shutdown().await;
    slot_a.shutdown().await;
    slot_b.shutdown().await;
}
