use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full};
use hyper::header::{AUTHORIZATION, CONTENT_TYPE};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use membus::{
    Ack, AclGrant, AuthConfig, Client, CollectionDescriptor, Endpoint, ExpectedVersion, IndexKind,
    IndexSpec, MembusError, MembusServer, MigrationState, Permission, Principal, RetentionPolicy,
    RocksDbStorage, SearchConfig, SearchDocument, SearchEngine, SearchLogOp, SearchMode,
    SearchQuery, SearchVisibility, ServerConfig, SignalBroker, SignalConfig, SignalPublisher,
    SignalRing, StorageConfig, StorageEngine, StoreProfile, TokenBinding,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::net::TcpStream;

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

async fn http1_call(
    addr: SocketAddr,
    method: Method,
    path: &str,
    bearer: Option<&str>,
    body: Option<String>,
) -> (StatusCode, String) {
    let stream = TcpStream::connect(addr).await.expect("connect http");
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
    let req = if let Some(json) = body {
        builder
            .header(CONTENT_TYPE, "application/json")
            .body(Full::new(Bytes::from(json)).boxed())
            .expect("req")
    } else {
        builder
            .body(Empty::<Bytes>::new().map_err(|never| match never {}).boxed())
            .expect("req")
    };

    let resp = sender.send_request(req).await.expect("send http1");
    let status = resp.status();
    let collected = resp.into_body().collect().await.expect("body").to_bytes();
    (status, String::from_utf8_lossy(&collected).into_owned())
}

#[tokio::test]
async fn acl_enforced_across_binary_socket_and_http() {
    let dir = TempDir::new().expect("tempdir");
    let ckpt_root = TempDir::new().expect("ckpt_root");

    let auth = AuthConfig::enforced(vec![
        TokenBinding {
            token: "tok-reader".to_owned(),
            principal: Principal {
                id: "reader".to_owned(),
                grants: vec![AclGrant {
                    namespace_pattern: "app:*".to_owned(),
                    key_prefix: b"pub/".to_vec(),
                    permissions: vec![Permission::Read],
                }],
            },
        },
        TokenBinding {
            token: "tok-writer".to_owned(),
            principal: Principal {
                id: "writer".to_owned(),
                grants: vec![AclGrant {
                    namespace_pattern: "app:orders".to_owned(),
                    key_prefix: Vec::new(),
                    permissions: vec![Permission::Read, Permission::Write],
                }],
            },
        },
        TokenBinding {
            token: "tok-admin".to_owned(),
            principal: Principal {
                id: "admin".to_owned(),
                grants: vec![AclGrant {
                    namespace_pattern: "*".to_owned(),
                    key_prefix: Vec::new(),
                    permissions: vec![Permission::Admin],
                }],
            },
        },
    ]);

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
    let search = Arc::new(
        SearchEngine::open(
            dir.path(),
            Arc::clone(&storage) as Arc<dyn StorageEngine>,
            SearchConfig::default(),
        )
        .expect("search"),
    );

    let server_cfg = ServerConfig {
        checkpoint_root: Some(ckpt_root.path().to_path_buf()),
        ..ServerConfig::default()
    };
    let server = MembusServer::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        broker,
        server_cfg,
    )
    .with_auth(auth)
    .with_search(search);

    let tcp_handle = server
        .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind tcp");
    let http_handle = server
        .bind_http("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind http");
    let Endpoint::Http(http_addr) = *http_handle.endpoint() else {
        panic!("expected http endpoint");
    };

    // --- Binary socket enforcement ---
    let unauth = Client::connect(tcp_handle.endpoint()).await.expect("connect");
    assert!(matches!(
        unauth.health().await,
        Err(MembusError::Unauthorized(_))
    ));
    assert!(matches!(
        unauth.get("app:orders", b"pub/1").await,
        Err(MembusError::Unauthorized(_))
    ));
    assert!(matches!(
        unauth.authenticate("wrong-token").await,
        Err(MembusError::Unauthorized(_))
    ));

    // Authenticate writer and populate data.
    let writer = Client::connect(tcp_handle.endpoint()).await.expect("writer");
    assert_eq!(writer.authenticate("tok-writer").await.expect("auth"), "writer");
    writer
        .put("app:orders", b"pub/1", b"visible", Ack::Fast)
        .await
        .expect("writer put pub");
    writer
        .put("app:orders", b"priv/1", b"secret", Ack::Fast)
        .await
        .expect("writer put priv");

    // Writer CANNOT write to another namespace or smuggle a cross-namespace search_op.
    assert!(matches!(
        writer.put("app:billing", b"k", b"v", Ack::Fast).await,
        Err(MembusError::Unauthorized(_))
    ));
    assert!(matches!(
        writer
            .put_indexed(
                "app:orders",
                b"pub/2",
                b"v",
                Ack::Fast,
                ExpectedVersion::Any,
                None,
                Some(SearchLogOp::Index(SearchDocument {
                    namespace: "app:billing".to_owned(),
                    doc_id: "smuggled".to_owned(),
                    kind: "doc".to_owned(),
                    title: "smuggled".to_owned(),
                    tags: Vec::new(),
                    body: "smuggled".to_owned(),
                    visibility: SearchVisibility::Public,
                })),
            )
            .await,
        Err(MembusError::Unauthorized(_))
    ));
    // Writer CANNOT call admin checkpoint or revoke_search_prefix.
    assert!(matches!(
        writer.checkpoint("snap1").await,
        Err(MembusError::Unauthorized(_))
    ));
    assert!(matches!(
        writer.revoke_search_prefix("app:orders", "pub/", Ack::Fast).await,
        Err(MembusError::Unauthorized(_))
    ));

    // Reader with prefix `pub/` on `app:*` can read `pub/1`, denied `priv/1`, denied writes.
    let reader = Client::connect(tcp_handle.endpoint()).await.expect("reader");
    assert_eq!(reader.authenticate("tok-reader").await.expect("auth"), "reader");
    assert_eq!(
        reader
            .get("app:orders", b"pub/1")
            .await
            .expect("read pub")
            .expect("some")
            .value,
        b"visible"
    );
    assert!(matches!(
        reader.get("app:orders", b"priv/1").await,
        Err(MembusError::Unauthorized(_))
    ));
    assert!(matches!(
        reader.put("app:orders", b"pub/1", b"hack", Ack::Fast).await,
        Err(MembusError::Unauthorized(_))
    ));
    assert!(matches!(
        reader.subscribe_each_change("app:orders", b"priv/", 0, 0).await,
        Err(MembusError::Unauthorized(_))
    ));

    // Admin can checkpoint and search globally.
    let admin = Client::connect(tcp_handle.endpoint()).await.expect("admin");
    assert_eq!(admin.authenticate("tok-admin").await.expect("auth"), "admin");
    let seq = admin.checkpoint("admin_snap").await.expect("admin ckpt");
    assert!(seq > 0);
    let _ = admin
        .search(SearchQuery {
            namespace: None,
            kind: None,
            query: "nothing".to_owned(),
            mode: SearchMode::Bm25,
            limit: 10,
        })
        .await
        .expect("admin global search");

    // --- HTTP enforcement (identical ACL policy) ---
    let (st, _) = http1_call(http_addr, Method::GET, "/v2/health", None, None).await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    let (st, _) = http1_call(http_addr, Method::GET, "/v2/kv/app:orders/pub/1", Some("bad"), None).await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    let (st, _) = http1_call(
        http_addr,
        Method::GET,
        "/v2/kv/app:orders/pub/1",
        Some("tok-reader"),
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, _) = http1_call(
        http_addr,
        Method::GET,
        "/v2/kv/app:orders/priv/1",
        Some("tok-reader"),
        None,
    )
    .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    let (st, _) = http1_call(
        http_addr,
        Method::PUT,
        "/v2/kv/app:orders/pub/3",
        Some("tok-reader"),
        Some(r#"{"value_utf8":"nope"}"#.to_owned()),
    )
    .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    let (st, _) = http1_call(
        http_addr,
        Method::POST,
        "/v2/checkpoint",
        Some("tok-writer"),
        Some(r#"{"dest_dir":"bad"}"#.to_owned()),
    )
    .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    let (st, _) = http1_call(
        http_addr,
        Method::POST,
        "/v2/checkpoint",
        Some("tok-admin"),
        Some(r#"{"dest_dir":"http_snap"}"#.to_owned()),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // Unauthenticated WS upgrade fails closed with 401.
    let (st, _) = http1_call(http_addr, Method::GET, "/v2/ws", None, None).await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    drop((unauth, writer, reader, admin));
    tcp_handle.shutdown().await;
    http_handle.shutdown().await;
}

#[tokio::test]
async fn fenced_leases_acquire_conflict_renew_release_and_expiry_reclaim() {
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
        ServerConfig::default(),
    );
    let handle = server
        .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind");
    let client = Client::connect(handle.endpoint()).await.expect("connect");

    // 1. Acquire lease -> generation 1.
    let g1 = client
        .acquire_lease("jobs", "shard-0", "worker-a", 5_000)
        .await
        .expect("acquire g1");
    assert_eq!(g1.generation, 1);
    assert_eq!(g1.holder, "worker-a");
    assert!(!g1.token.is_empty());

    assert!(client
        .validate_lease("jobs", "shard-0", "worker-a", g1.generation, &g1.token)
        .await
        .expect("validate"));
    assert!(!client
        .validate_lease("jobs", "shard-0", "worker-a", g1.generation, "wrong-token")
        .await
        .expect("validate bad token"));
    assert_eq!(
        client
            .inspect_lease("jobs", "shard-0")
            .await
            .expect("inspect")
            .expect("active")
            .generation,
        1
    );

    // 2. Foreign acquire rejected with explicit LeaseHeld.
    let held = client
        .acquire_lease("jobs", "shard-0", "worker-b", 5_000)
        .await;
    assert!(matches!(
        held,
        Err(MembusError::LeaseHeld {
            ref holder,
            generation: 1,
            ..
        }) if holder == "worker-a"
    ));

    // 3. Renew preserves generation and extends deadline.
    let g1_renewed = client
        .renew_lease(
            "jobs",
            "shard-0",
            "worker-a",
            g1.generation,
            &g1.token,
            10_000,
        )
        .await
        .expect("renew");
    assert_eq!(g1_renewed.generation, 1);
    assert!(g1_renewed.expires_at_ms >= g1.expires_at_ms);

    // 4. Release preserves generation history so next acquire bumps to 2.
    client
        .release_lease("jobs", "shard-0", "worker-a", g1.generation, &g1.token)
        .await
        .expect("release");
    assert!(client
        .inspect_lease("jobs", "shard-0")
        .await
        .expect("inspect after release")
        .is_none());
    assert!(!client
        .validate_lease("jobs", "shard-0", "worker-a", g1.generation, &g1.token)
        .await
        .expect("validate after release"));

    // 5. Acquire short lease (generation 2), wait for expiry, reclaim by foreign holder (generation 3).
    let g2 = client
        .acquire_lease("jobs", "shard-0", "worker-b", 80)
        .await
        .expect("acquire g2");
    assert_eq!(g2.generation, 2);

    tokio::time::sleep(Duration::from_millis(140)).await;
    assert!(!client
        .validate_lease("jobs", "shard-0", "worker-b", g2.generation, &g2.token)
        .await
        .expect("expired validate"));

    let g3 = client
        .acquire_lease("jobs", "shard-0", "worker-c", 5_000)
        .await
        .expect("reclaim g3");
    assert_eq!(g3.generation, 3);
    assert_eq!(g3.holder, "worker-c");

    drop(client);
    handle.shutdown().await;
}

#[tokio::test]
async fn collection_catalog_versioning_history_and_migration_state_machine() {
    let dir = TempDir::new().expect("tempdir");

    let schema_v1 = br#"{"type":"object","version":1}"#.to_vec();
    let schema_v2 = br#"{"type":"object","version":2,"fields":["id","ts"]}"#.to_vec();
    let schema_v3 = br#"{"type":"object","version":3,"fields":["id","ts","tags"]}"#.to_vec();

    let desc_v1 = CollectionDescriptor {
        namespace: "ledger:events".to_owned(),
        store_profile: StoreProfile::Log,
        schema_version: 1,
        schema_sha256: sha256(&schema_v1),
        schema_bytes: schema_v1.clone(),
        declared_indexes: vec![IndexSpec {
            index_id: "by_ts".to_owned(),
            kind: IndexKind::Ordered,
            description: "ordered by timestamp".to_owned(),
        }],
        retention_policy: RetentionPolicy {
            max_age_secs: Some(86_400),
            max_entries: Some(1_000_000),
        },
        migration_state: MigrationState::Active,
    };

    {
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
            ServerConfig::default(),
        );
        let handle = server
            .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
            .await
            .expect("bind");
        let client = Client::connect(handle.endpoint()).await.expect("connect");

        // Mismatched SHA-256 rejected.
        let mut bad_sha = desc_v1.clone();
        bad_sha.schema_sha256[0] ^= 0xFF;
        assert!(matches!(
            client.register_collection(None, bad_sha).await,
            Err(MembusError::InvalidArgument(_))
        ));

        // Register v1.
        let created = client
            .register_collection(None, desc_v1.clone())
            .await
            .expect("register v1");
        assert_eq!(created.schema_version, 1);

        // Duplicate None fails CAS conflict.
        assert!(matches!(
            client.register_collection(None, desc_v1.clone()).await,
            Err(MembusError::Conflict { .. })
        ));

        // Direct CAS upgrade v1 -> v2.
        let mut desc_v2 = desc_v1.clone();
        desc_v2.schema_version = 2;
        desc_v2.schema_sha256 = sha256(&schema_v2);
        desc_v2.schema_bytes = schema_v2.clone();
        let upgraded = client
            .register_collection(Some(1), desc_v2.clone())
            .await
            .expect("register v2");
        assert_eq!(upgraded.schema_version, 2);

        // Begin migration v2 -> v3, then abort back to Active v2.
        let migrating = client
            .begin_migration("ledger:events", 2, 3, sha256(&schema_v3), schema_v3.clone())
            .await
            .expect("begin_migration");
        assert!(matches!(
            migrating.migration_state,
            MigrationState::Migrating {
                target_version: 3,
                ..
            }
        ));

        let aborted = client
            .abort_migration("ledger:events", 3)
            .await
            .expect("abort_migration");
        assert_eq!(aborted.migration_state, MigrationState::Active);
        assert_eq!(aborted.schema_version, 2);

        // Begin migration v2 -> v3 again and commit!
        client
            .begin_migration("ledger:events", 2, 3, sha256(&schema_v3), schema_v3.clone())
            .await
            .expect("begin_migration 2");
        let committed = client
            .commit_migration("ledger:events", 3, 500)
            .await
            .expect("commit_migration");
        assert_eq!(committed.migration_state, MigrationState::Active);
        assert_eq!(committed.schema_version, 3);
        assert_eq!(committed.schema_bytes, schema_v3);

        // List & immutable version history checks.
        let all = client.list_collections().await.expect("list");
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].schema_version, 3);

        let h1 = client
            .get_collection_version("ledger:events", 1)
            .await
            .expect("v1")
            .expect("present");
        let h2 = client
            .get_collection_version("ledger:events", 2)
            .await
            .expect("v2")
            .expect("present");
        let h3 = client
            .get_collection_version("ledger:events", 3)
            .await
            .expect("v3")
            .expect("present");
        assert_eq!(h1.schema_bytes, schema_v1);
        assert_eq!(h2.schema_bytes, schema_v2);
        assert_eq!(h3.schema_bytes, schema_v3);

        drop(client);
        handle.shutdown().await;
    }

    // Reopen DB from disk and verify catalog & immutable history survived cleanly.
    let storage = Arc::new(
        RocksDbStorage::open(
            dir.path(),
            StorageConfig::default(),
            Arc::new(membus::NoopPublisher),
        )
        .expect("reopen"),
    );
    let catalog = membus::CollectionCatalog::new(storage);
    let current = catalog
        .get_collection("ledger:events")
        .expect("get")
        .expect("persisted");
    assert_eq!(current.schema_version, 3);
    assert_eq!(
        catalog
            .get_collection_version("ledger:events", 1)
            .expect("v1")
            .expect("persisted v1")
            .schema_bytes,
        schema_v1
    );
}

#[tokio::test]
async fn inspect_lease_omits_secret_token_and_acquire_blocks_holder_echo_hijack() {
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
        ServerConfig::default(),
    );
    let tcp_handle = server
        .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind tcp");
    let http_handle = server
        .bind_http("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind http");
    let Endpoint::Http(http_addr) = *http_handle.endpoint() else {
        panic!("expected http");
    };
    let client = Client::connect(tcp_handle.endpoint()).await.expect("connect");

    let grant = client
        .acquire_lease("jobs", "lock-1", "worker-a", 10_000)
        .await
        .expect("acquire");
    // CSPRNG 32-byte hex token (64 hex chars).
    assert_eq!(grant.token.len(), 64);

    // Public inspection via HTTP must NEVER leak secret `token`.
    let (st, body) = http1_call(http_addr, Method::GET, "/v2/leases/jobs/lock-1", None, None).await;
    assert_eq!(st, StatusCode::OK);
    let val: serde_json::Value = serde_json::from_str(&body).expect("json");
    assert_eq!(val["holder"], "worker-a");
    assert_eq!(val["generation"], 1);
    assert!(val.get("token").is_none(), "inspect_lease leaked secret token!");

    // Re-acquiring with the SAME holder string while active MUST be rejected (anti-hijack).
    let hijack = client
        .acquire_lease("jobs", "lock-1", "worker-a", 10_000)
        .await;
    assert!(matches!(
        hijack,
        Err(MembusError::LeaseHeld {
            ref holder,
            generation: 1,
            ..
        }) if holder == "worker-a"
    ));

    // Only renewing with valid `(generation, token)` succeeds.
    let renewed = client
        .renew_lease(
            "jobs",
            "lock-1",
            "worker-a",
            grant.generation,
            &grant.token,
            10_000,
        )
        .await
        .expect("renew");
    assert_eq!(renewed.token, grant.token);

    drop(client);
    tcp_handle.shutdown().await;
    http_handle.shutdown().await;
}

#[tokio::test]
async fn prefix_scoped_writer_cannot_escape_prefix_via_graph_edges_or_pipelined_auth_race() {
    let dir = TempDir::new().expect("tempdir");
    let auth = AuthConfig::enforced(vec![
        TokenBinding {
            token: "tok-prefix".to_owned(),
            principal: Principal {
                id: "prefix-writer".to_owned(),
                grants: vec![AclGrant {
                    namespace_pattern: "app:g".to_owned(),
                    key_prefix: b"pub/".to_vec(),
                    permissions: vec![Permission::Read, Permission::Write],
                }],
            },
        },
        TokenBinding {
            token: "tok-other".to_owned(),
            principal: Principal {
                id: "other-reader".to_owned(),
                grants: vec![AclGrant {
                    namespace_pattern: "other".to_owned(),
                    key_prefix: Vec::new(),
                    permissions: vec![Permission::Read],
                }],
            },
        },
    ]);

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
        ServerConfig::default(),
    )
    .with_auth(auth);
    let tcp_handle = server
        .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind tcp");
    let http_handle = server
        .bind_http("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind http");
    let Endpoint::Http(http_addr) = *http_handle.endpoint() else {
        panic!("expected http");
    };

    // B6: Pipelined [Authenticate, Put] sent concurrently on a fresh connection must NOT race!
    let client = Client::connect(tcp_handle.endpoint()).await.expect("connect");
    let auth_fut = client.authenticate("tok-prefix");
    let put_fut = client.put_indexed(
        "app:g",
        b"pub/n1",
        b"ok",
        Ack::Fast,
        ExpectedVersion::Any,
        Some(membus::IndexMutations {
            ordered: Vec::new(),
            edges: vec![membus::GraphEdgeEntry {
                index_id: "rel".to_owned(),
                src_id: b"pub/n1".to_vec(),
                rel_label: b"to".to_vec(),
                dst_id: b"pub/n2".to_vec(),
                meta: b"m".to_vec(),
            }],
        }),
        None,
    );
    let (auth_res, put_res) = tokio::join!(auth_fut, put_fut);
    assert_eq!(auth_res.expect("pipelined auth"), "prefix-writer");
    put_res.expect("pipelined put after auth");

    // A4: Prefix-scoped writer CANNOT inject graph edge targeting `priv/` over binary socket or HTTP.
    let escape_res = client
        .put_indexed(
            "app:g",
            b"pub/n1",
            b"bad",
            Ack::Fast,
            ExpectedVersion::Any,
            Some(membus::IndexMutations {
                ordered: Vec::new(),
                edges: vec![membus::GraphEdgeEntry {
                    index_id: "rel".to_owned(),
                    src_id: b"pub/n1".to_vec(),
                    rel_label: b"to".to_vec(),
                    dst_id: b"priv/secret".to_vec(),
                    meta: b"m".to_vec(),
                }],
            }),
            None,
        )
        .await;
    assert!(matches!(escape_res, Err(MembusError::Unauthorized(_))));

    let (st, _) = http1_call(
        http_addr,
        Method::PUT,
        "/v2/kv/app:g/pub/n1",
        Some("tok-prefix"),
        Some(
            serde_json::json!({
                "value_utf8": "bad",
                "indexes": {
                    "ordered": [],
                    "edges": [{
                        "index_id": "rel",
                        "src_id": b"pub/n1".to_vec(),
                        "rel_label": b"to".to_vec(),
                        "dst_id": b"priv/secret".to_vec(),
                        "meta": b"m".to_vec()
                    }]
                }
            })
            .to_string(),
        ),
    )
    .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    // B6: Switching principal on an active connection immediately revokes existing push subscriptions!
    let mut sub = client
        .subscribe_each_change("app:g", b"pub/", 0, 0)
        .await
        .expect("subscribe");
    assert_eq!(
        client.authenticate("tok-other").await.expect("switch principal"),
        "other-reader"
    );
    // Drain any already-buffered event and confirm stream ends promptly without delivering new events.
    let mut ended = false;
    for _ in 0..8 {
        match tokio::time::timeout(Duration::from_secs(2), sub.next_event()).await {
            Ok(Ok(None)) => {
                ended = true;
                break;
            }
            Ok(Ok(Some(_))) => continue,
            Ok(Err(_)) => {
                ended = true;
                break;
            }
            Err(_) => break,
        }
    }
    assert!(ended, "active subscription must be revoked on principal switch");

    drop(client);
    tcp_handle.shutdown().await;
    http_handle.shutdown().await;
}

#[test]
fn catalog_migration_verified_through_monotonicity_base64_and_meta_limits() {
    let dir = TempDir::new().expect("tempdir");
    let cfg = StorageConfig {
        max_value_bytes: 1024,
        ..StorageConfig::default()
    };
    let storage = Arc::new(
        RocksDbStorage::open(dir.path(), cfg, Arc::new(membus::NoopPublisher)).expect("open"),
    );
    let catalog = membus::CollectionCatalog::new(Arc::clone(&storage) as Arc<dyn StorageEngine>);

    // A8: meta_cas_batch enforces max_value_bytes.
    let too_big = storage.meta_cas_batch(&[], &[(b"k".to_vec(), vec![0u8; 2048])], &[], false);
    assert!(matches!(too_big, Err(MembusError::InvalidArgument(_))));

    let s1 = b"schema-v1".to_vec();
    let s2 = b"schema-v2".to_vec();
    let desc = CollectionDescriptor {
        namespace: "ns_cat".to_owned(),
        store_profile: StoreProfile::Kv,
        schema_version: 1,
        schema_sha256: sha256(&s1),
        schema_bytes: s1.clone(),
        declared_indexes: Vec::new(),
        retention_policy: RetentionPolicy::default(),
        migration_state: MigrationState::Active,
    };
    // Verify schema_bytes serializes compactly as a base64 string in JSON.
    let json_val = serde_json::to_value(&desc).expect("to_value");
    assert!(json_val["schema_bytes"].is_string());

    catalog.register_collection(None, desc).expect("reg v1");
    catalog
        .begin_migration("ns_cat", 1, 2, sha256(&s2), s2)
        .expect("begin");

    // Advance verification monotonically; reject regression both in advance and commit.
    catalog
        .advance_migration_verification("ns_cat", 2, 50)
        .expect("advance 50");
    assert!(matches!(
        catalog.advance_migration_verification("ns_cat", 2, 25),
        Err(MembusError::InvalidArgument(_))
    ));
    assert!(matches!(
        catalog.commit_migration("ns_cat", 2, 40),
        Err(MembusError::InvalidArgument(_))
    ));
    catalog
        .commit_migration("ns_cat", 2, 100)
        .expect("commit 100");
    assert_eq!(
        catalog.last_verified_through("ns_cat").expect("wm"),
        Some(100)
    );
}

#[tokio::test]
async fn same_id_narrower_grant_reauth_revokes_streams_and_pending_migration_uses_base64() {
    let dir = TempDir::new().expect("tempdir");
    // Two distinct tokens sharing the EXACT SAME principal.id ("svc") with different grants.
    let auth = AuthConfig::enforced(vec![
        TokenBinding {
            token: "tok-svc-broad".to_owned(),
            principal: Principal {
                id: "svc".to_owned(),
                grants: vec![AclGrant {
                    namespace_pattern: "app:*".to_owned(),
                    key_prefix: Vec::new(),
                    permissions: vec![Permission::Read, Permission::Write],
                }],
            },
        },
        TokenBinding {
            token: "tok-svc-narrow".to_owned(),
            principal: Principal {
                id: "svc".to_owned(),
                grants: vec![AclGrant {
                    namespace_pattern: "other:*".to_owned(),
                    key_prefix: Vec::new(),
                    permissions: vec![Permission::Read],
                }],
            },
        },
    ]);

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
        ServerConfig::default(),
    )
    .with_auth(auth);
    let tcp_handle = server
        .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind tcp");

    // Authenticate with broad token, open subscription on `app:events`, then re-auth with
    // same-id narrower token: full Principal equality must detect the downgrade and revoke!
    let client = Client::connect(tcp_handle.endpoint()).await.expect("connect");
    assert_eq!(
        client.authenticate("tok-svc-broad").await.expect("broad auth"),
        "svc"
    );
    let mut sub = client
        .subscribe_each_change("app:events", b"", 0, 0)
        .await
        .expect("subscribe");
    assert_eq!(
        client
            .authenticate("tok-svc-narrow")
            .await
            .expect("narrow auth"),
        "svc"
    );

    let end = tokio::time::timeout(Duration::from_secs(2), sub.next_event())
        .await
        .expect("revoked stream must close promptly");
    assert!(
        matches!(end, Ok(None) | Err(_)),
        "expected stream termination on same-id narrower-grant re-auth, got {end:?}"
    );

    // Verify pipelined [Subscribe, Authenticate(narrow)] never leaks an authorized stream!
    let client2 = Client::connect(tcp_handle.endpoint()).await.expect("connect 2");
    client2.authenticate("tok-svc-broad").await.expect("broad 2");
    let (sub_res, reauth_res) = tokio::join!(
        client2.subscribe_each_change("app:events", b"", 0, 0),
        client2.authenticate("tok-svc-narrow")
    );
    assert_eq!(reauth_res.expect("reauth 2"), "svc");
    if let Ok(mut s) = sub_res {
        // If subscribe won before re-auth, re-auth MUST have cancelled it immediately!
        let next = tokio::time::timeout(Duration::from_secs(2), s.next_event())
            .await
            .expect("pipelined stream must close promptly");
        assert!(matches!(next, Ok(None) | Err(_)));
    }

    // Verify PendingMigrationTarget in CF_META stores `target_schema_bytes` as compact base64 string.
    let catalog = membus::CollectionCatalog::new(Arc::clone(&storage) as Arc<dyn StorageEngine>);
    let s1 = b"schema-v1-raw".to_vec();
    let s2 = b"schema-v2-pending-raw".to_vec();
    catalog
        .register_collection(
            None,
            CollectionDescriptor {
                namespace: "mig_b64".to_owned(),
                store_profile: StoreProfile::Kv,
                schema_version: 1,
                schema_sha256: sha256(&s1),
                schema_bytes: s1,
                declared_indexes: Vec::new(),
                retention_policy: RetentionPolicy::default(),
                migration_state: MigrationState::Active,
            },
        )
        .expect("reg");
    catalog
        .begin_migration("mig_b64", 1, 2, sha256(&s2), s2)
        .expect("begin");
    let raw_mig = storage
        .meta_get(b"cat:mig:mig_b64")
        .expect("meta_get")
        .expect("pending migration key");
    let mig_json: serde_json::Value = serde_json::from_slice(&raw_mig).expect("json");
    assert!(
        mig_json["target_schema_bytes"].is_string(),
        "PendingMigrationTarget.target_schema_bytes must serialize as compact base64 string"
    );

    drop((client, client2));
    tcp_handle.shutdown().await;
}
