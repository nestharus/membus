use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::header::CONTENT_TYPE;
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use membus::{
    Ack, Endpoint, ExpectedVersion, IndexedMutation, LeaseGuard, LeaseManager, MembusError,
    MembusServer, MutationOp, NoopPublisher, RocksDbStorage, ServerConfig, SignalBroker,
    SignalConfig, SignalPublisher, SignalRing, StorageConfig, StorageEngine, VersionGuard,
    WriteMutation,
};
use tokio::net::TcpStream;

fn open_test_storage() -> (tempfile::TempDir, Arc<RocksDbStorage>, Arc<LeaseManager>) {
    let dir = tempfile::tempdir().expect("tempdir");
    let storage = Arc::new(
        RocksDbStorage::open(dir.path(), StorageConfig::default(), Arc::new(NoopPublisher))
            .expect("open storage"),
    );
    let leases = Arc::new(LeaseManager::new(Arc::clone(&storage) as Arc<dyn StorageEngine>));
    (dir, storage, leases)
}

fn put_mutation(ns: &str, key: &[u8], val: &[u8]) -> IndexedMutation {
    IndexedMutation {
        mutation: WriteMutation {
            namespace: ns.to_owned(),
            key: key.to_vec(),
            op: MutationOp::Put {
                value: val.to_vec(),
            },
        },
        indexes: None,
        search_op: None,
    }
}

#[test]
fn lease_guarded_tx_commits_when_active_and_rejects_released_expired_and_mismatched() {
    let (_dir, storage, leases) = open_test_storage();
    let grant = leases
        .acquire_lease("slots", "agent/alpha", "controller-1", 60_000)
        .expect("acquire lease");
    let guard = LeaseGuard::from_grant(&grant);

    // 1. Active lease commits atomically with Ack::Flush.
    let (commit, replayed) = storage
        .commit_indexed_tx_idempotent_with_leases(
            &[VersionGuard {
                namespace: "slots".to_owned(),
                key: b"slot/alpha".to_vec(),
                expected: ExpectedVersion::Absent,
            }],
            std::slice::from_ref(&guard),
            &[put_mutation("slots", b"slot/alpha", b"waking:g1")],
            Ack::Flush,
            None,
        )
        .expect("active lease must commit");
    assert!(!replayed);
    assert_eq!(commit.versions.len(), 1);
    assert_eq!(
        storage
            .get("slots", b"slot/alpha")
            .unwrap()
            .unwrap()
            .value,
        b"waking:g1"
    );

    // 2. Wrong secret token fails with LeaseHeld and NEVER leaks the token or mutates KV.
    let mut bad_token_guard = guard.clone();
    bad_token_guard.token = "forged-secret-token-xyz".to_owned();
    let err = storage
        .commit_indexed_tx_idempotent_with_leases(
            &[],
            &[bad_token_guard],
            &[put_mutation("slots", b"slot/alpha", b"tampered")],
            Ack::Flush,
            None,
        )
        .expect_err("wrong token must fail");
    assert!(matches!(err, MembusError::LeaseHeld { .. }));
    assert!(!err.to_string().contains(&grant.token));
    assert!(!err.to_string().contains("forged-secret-token-xyz"));
    assert_eq!(
        storage
            .get("slots", b"slot/alpha")
            .unwrap()
            .unwrap()
            .value,
        b"waking:g1"
    );

    // 3. Released lease with NO successor fails with NotFound and mutates nothing.
    leases
        .release_lease(
            &grant.namespace,
            &grant.resource,
            &grant.holder,
            grant.generation,
            &grant.token,
        )
        .expect("release lease");
    let err = storage
        .commit_indexed_tx_idempotent_with_leases(
            &[],
            std::slice::from_ref(&guard),
            &[put_mutation("slots", b"slot/alpha", b"stale-after-release")],
            Ack::Flush,
            None,
        )
        .expect_err("released lease without successor must fail");
    assert!(matches!(err, MembusError::NotFound(_)));
    assert_eq!(
        storage
            .get("slots", b"slot/alpha")
            .unwrap()
            .unwrap()
            .value,
        b"waking:g1"
    );

    // 4. Reacquire with SAME holder name gets new generation & token; old guard fails with LeaseHeld.
    let grant2 = leases
        .acquire_lease("slots", "agent/alpha", "controller-1", 60_000)
        .expect("reacquire");
    assert_eq!(grant2.generation, 2);
    let err = storage
        .commit_indexed_tx_idempotent_with_leases(
            &[],
            std::slice::from_ref(&guard),
            &[put_mutation("slots", b"slot/alpha", b"stale-gen1")],
            Ack::Flush,
            None,
        )
        .expect_err("stale generation must fail");
    assert!(matches!(
        err,
        MembusError::LeaseHeld { generation: 2, .. }
    ));

    // 5. Expired lease with NO successor fails with NotFound and mutates nothing.
    let short_grant = leases
        .acquire_lease("slots", "agent/beta", "controller-1", 40)
        .expect("acquire short lease");
    let short_guard = LeaseGuard::from_grant(&short_grant);
    std::thread::sleep(Duration::from_millis(80));
    let err = storage
        .commit_indexed_tx_idempotent_with_leases(
            &[],
            &[short_guard],
            &[put_mutation("slots", b"slot/beta", b"expired-write")],
            Ack::Flush,
            None,
        )
        .expect_err("expired lease without successor must fail");
    assert!(matches!(err, MembusError::NotFound(_)));
    assert!(storage.get("slots", b"slot/beta").unwrap().is_none());

    // 6. Duplicate lease_guards on the same resource neither deadlock nor allow mismatch bypass.
    let g2 = LeaseGuard::from_grant(&grant2);
    storage
        .commit_indexed_tx_idempotent_with_leases(
            &[],
            &[g2.clone(), g2.clone()],
            &[put_mutation("slots", b"slot/alpha", b"dup-ok")],
            Ack::Flush,
            None,
        )
        .expect("duplicate identical lease guards commit without deadlock");
    let mut g2_bad = g2.clone();
    g2_bad.token = "wrong-second-guard".to_owned();
    let dup_err = storage
        .commit_indexed_tx_idempotent_with_leases(
            &[],
            &[g2.clone(), g2_bad],
            &[put_mutation("slots", b"slot/alpha", b"dup-bad")],
            Ack::Flush,
            None,
        )
        .expect_err("mismatched duplicate guard must abort");
    assert!(matches!(dup_err, MembusError::LeaseHeld { .. }));
    assert_eq!(storage.get("slots", b"slot/alpha").unwrap().unwrap().value, b"dup-ok");

    // 7. Invalid LeaseGuard fields (generation == 0, empty token, empty resource) fail with InvalidArgument.
    let mut zero_gen = g2.clone();
    zero_gen.generation = 0;
    assert!(matches!(
        storage.commit_indexed_tx_idempotent_with_leases(
            &[],
            &[zero_gen],
            &[put_mutation("slots", b"slot/alpha", b"bad")],
            Ack::Flush,
            None,
        ),
        Err(MembusError::InvalidArgument(_))
    ));
    let mut empty_tok = g2;
    empty_tok.token.clear();
    assert!(matches!(
        storage.commit_indexed_tx_idempotent_with_leases(
            &[],
            &[empty_tok],
            &[put_mutation("slots", b"slot/alpha", b"bad")],
            Ack::Flush,
            None,
        ),
        Err(MembusError::InvalidArgument(_))
    ));
}

#[test]
fn multi_lease_tx_locks_canonically_and_rejects_if_any_lease_invalid() {
    let (_dir, storage, leases) = open_test_storage();
    let g_a = leases
        .acquire_lease("slots", "res/a", "worker-1", 60_000)
        .expect("lease a");
    let g_b = leases
        .acquire_lease("slots", "res/b", "worker-1", 60_000)
        .expect("lease b");

    // Supply guards in reverse order (b, a) to verify canonical stripe lock ordering.
    let (res, _) = storage
        .commit_indexed_tx_idempotent_with_leases(
            &[],
            &[LeaseGuard::from_grant(&g_b), LeaseGuard::from_grant(&g_a)],
            &[
                put_mutation("slots", b"k/a", b"ok-a"),
                put_mutation("slots", b"k/b", b"ok-b"),
            ],
            Ack::Flush,
            None,
        )
        .expect("both active leases commit");
    assert_eq!(res.versions.len(), 2);

    // Release only res/b; multi-lease tx must abort completely without touching k/a.
    leases
        .release_lease(&g_b.namespace, &g_b.resource, &g_b.holder, g_b.generation, &g_b.token)
        .expect("release b");
    let err = storage
        .commit_indexed_tx_idempotent_with_leases(
            &[],
            &[LeaseGuard::from_grant(&g_a), LeaseGuard::from_grant(&g_b)],
            &[put_mutation("slots", b"k/a", b"partial-bad")],
            Ack::Flush,
            None,
        )
        .expect_err("one released lease must abort entire tx");
    assert!(matches!(err, MembusError::NotFound(_)));
    assert_eq!(storage.get("slots", b"k/a").unwrap().unwrap().value, b"ok-a");
}

#[test]
fn concurrent_release_vs_lease_guarded_commit_is_linearizable() {
    let (_dir, storage, leases) = open_test_storage();
    for round in 0..25 {
        let resource = format!("race/slot-{round}");
        let kv_key = format!("kv/slot-{round}").into_bytes();
        let grant = leases
            .acquire_lease("slots", &resource, "holder-1", 60_000)
            .expect("acquire");
        let guard = LeaseGuard::from_grant(&grant);

        let barrier = Arc::new(std::sync::Barrier::new(2));
        let st_c = Arc::clone(&storage);
        let b_c = Arc::clone(&barrier);
        let key_c = kv_key.clone();
        let guard_c = guard.clone();

        let writer = std::thread::spawn(move || {
            b_c.wait();
            st_c.commit_indexed_tx_idempotent_with_leases(
                &[],
                &[guard_c],
                &[put_mutation("slots", &key_c, b"committed")],
                Ack::Fast,
                None,
            )
        });

        barrier.wait();
        leases
            .release_lease(
                &grant.namespace,
                &grant.resource,
                &grant.holder,
                grant.generation,
                &grant.token,
            )
            .expect("release");

        let write_outcome = writer.join().expect("join writer");
        // Regardless of which thread won the stripe lock, a subsequent write with the released
        // guard MUST always fail — never committing after release.
        let post_release = storage.commit_indexed_tx_idempotent_with_leases(
            &[],
            &[guard],
            &[put_mutation("slots", &kv_key, b"after-release-illegal")],
            Ack::Fast,
            None,
        );
        assert!(matches!(post_release, Err(MembusError::NotFound(_))));
        let observed = storage.get("slots", &kv_key).unwrap();
        match write_outcome {
            Ok(_) => assert_eq!(observed.unwrap().value, b"committed"),
            Err(MembusError::NotFound(_)) => assert!(observed.is_none()),
            Err(other) => panic!("unexpected error in race round {round}: {other:?}"),
        }
    }
}

async fn send_http_json(
    addr: SocketAddr,
    method: Method,
    path: &str,
    idem_key: Option<&str>,
    body: serde_json::Value,
) -> (StatusCode, Option<String>, Bytes) {
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
        .header("host", addr.to_string())
        .header(CONTENT_TYPE, "application/json");
    if let Some(k) = idem_key {
        builder = builder.header("x-membus-idempotency-key", k);
    }
    let req = builder
        .body(Full::new(Bytes::from(body.to_string())))
        .expect("build req");
    let resp = sender.send_request(req).await.expect("send req");
    let status = resp.status();
    let replayed = resp
        .headers()
        .get("x-membus-idempotent-replay")
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let bytes = resp.into_body().collect().await.expect("collect").to_bytes();
    (status, replayed, bytes)
}

#[tokio::test]
async fn http_v2_tx_lease_guards_idempotency_and_rejection() {
    let dir = tempfile::tempdir().expect("tempdir");
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
    let server = MembusServer::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        broker,
        ServerConfig::default(),
    );
    let bound = server
        .bind_http("127.0.0.1:0".parse().expect("addr"))
        .await
        .expect("bind http");
    let Endpoint::Http(addr) = *bound.endpoint() else {
        panic!("expected http endpoint");
    };

    // 1. Acquire lease over HTTP.
    let (status, _, body) = send_http_json(
        addr,
        Method::POST,
        "/v2/leases/acquire",
        None,
        serde_json::json!({
            "namespace": "slots",
            "resource": "agent/http-alpha",
            "holder": "orch-1",
            "ttl_ms": 60_000
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let grant: serde_json::Value = serde_json::from_slice(&body).expect("parse grant");
    let lease_guard = serde_json::json!({
        "namespace": grant["namespace"],
        "resource": grant["resource"],
        "holder": grant["holder"],
        "generation": grant["generation"],
        "token": grant["token"],
    });

    let tx_body = serde_json::json!({
        "ack": "Flush",
        "guards": [],
        "lease_guards": [lease_guard],
        "mutations": [{
            "mutation": {
                "namespace": "slots",
                "key": b"slot/http-alpha".to_vec(),
                "op": { "Put": { "value": b"awake".to_vec() } }
            }
        }]
    });

    // 2. First guarded /v2/tx commits cleanly with idempotency key.
    let (status, replayed, _) = send_http_json(
        addr,
        Method::POST,
        "/v2/tx",
        Some("idem-lease-tx-1"),
        tx_body.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_ne!(replayed.as_deref(), Some("true"));
    assert_eq!(
        storage.get("slots", b"slot/http-alpha").unwrap().unwrap().value,
        b"awake"
    );

    // 3. Exact replay with same idempotency key replays 200 without re-executing.
    let (status, replayed, _) = send_http_json(
        addr,
        Method::POST,
        "/v2/tx",
        Some("idem-lease-tx-1"),
        tx_body,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(replayed.as_deref(), Some("true"));

    // 4. Same idempotency key with altered lease_guard generation fails with 409 Conflict.
    let mut bad_idem_body = serde_json::json!({
        "ack": "Flush",
        "guards": [],
        "lease_guards": [{
            "namespace": grant["namespace"],
            "resource": grant["resource"],
            "holder": grant["holder"],
            "generation": 999,
            "token": grant["token"],
        }],
        "mutations": [{
            "mutation": {
                "namespace": "slots",
                "key": b"slot/http-alpha".to_vec(),
                "op": { "Put": { "value": b"awake".to_vec() } }
            }
        }]
    });
    let (status, _, _) = send_http_json(
        addr,
        Method::POST,
        "/v2/tx",
        Some("idem-lease-tx-1"),
        bad_idem_body.clone(),
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);

    // 5. Forged token returns 409 Conflict and NEVER leaks either secret token.
    bad_idem_body["lease_guards"][0]["generation"] = grant["generation"].clone();
    bad_idem_body["lease_guards"][0]["token"] = serde_json::json!("forged-http-secret");
    let (status, _, err_body) = send_http_json(
        addr,
        Method::POST,
        "/v2/tx",
        None,
        bad_idem_body,
    )
    .await;
    assert_eq!(status, StatusCode::CONFLICT);
    let err_str = String::from_utf8_lossy(&err_body);
    assert!(!err_str.contains(grant["token"].as_str().unwrap()));
    assert!(!err_str.contains("forged-http-secret"));

    // 6. Release lease, then guarded /v2/tx returns 404 Not Found and does not mutate KV.
    let (status, _, _) = send_http_json(
        addr,
        Method::POST,
        "/v2/leases/release",
        None,
        serde_json::json!({
            "namespace": grant["namespace"],
            "resource": grant["resource"],
            "holder": grant["holder"],
            "generation": grant["generation"],
            "token": grant["token"],
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let (status, _, _) = send_http_json(
        addr,
        Method::POST,
        "/v2/tx",
        None,
        serde_json::json!({
            "ack": "Flush",
            "guards": [],
            "lease_guards": [{
                "namespace": grant["namespace"],
                "resource": grant["resource"],
                "holder": grant["holder"],
                "generation": grant["generation"],
                "token": grant["token"],
            }],
            "mutations": [{
                "mutation": {
                    "namespace": "slots",
                    "key": b"slot/http-alpha".to_vec(),
                    "op": { "Put": { "value": b"post-release-bad".to_vec() } }
                }
            }]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(
        storage.get("slots", b"slot/http-alpha").unwrap().unwrap().value,
        b"awake"
    );

    bound.shutdown().await;
}

#[tokio::test]
async fn binary_client_commit_indexed_tx_with_leases_enforces_fencing_and_auth() {
    let dir = tempfile::tempdir().expect("tempdir");
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
    let server = MembusServer::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        broker,
        ServerConfig::default(),
    );
    let bound = server
        .bind_tcp("127.0.0.1:0".parse().expect("addr"))
        .await
        .expect("bind tcp");
    let client = membus::Client::connect(bound.endpoint())
        .await
        .expect("connect");

    let grant = client
        .acquire_lease("slots", "req/bin-1", "worker-1", 60_000)
        .await
        .expect("acquire");
    let guard = LeaseGuard::from_grant(&grant);

    // 1. Valid lease guard commits cleanly over binary protocol.
    let commit = client
        .commit_indexed_tx_with_leases(
            vec![],
            vec![guard.clone()],
            vec![put_mutation("slots", b"req/bin-1/ack", b"ok-1")],
            Ack::Flush,
        )
        .await
        .expect("valid binary lease-guarded tx");
    assert_eq!(commit.versions.len(), 1);
    assert_eq!(
        client
            .get("slots", b"req/bin-1/ack")
            .await
            .unwrap()
            .unwrap()
            .value,
        b"ok-1"
    );

    // 2. Wrong token aborts atomically.
    let mut bad_guard = guard.clone();
    bad_guard.token = "0".repeat(64);
    let err = client
        .commit_indexed_tx_with_leases(
            vec![],
            vec![bad_guard],
            vec![put_mutation("slots", b"req/bin-1/ack", b"bad-token")],
            Ack::Flush,
        )
        .await
        .expect_err("wrong token must fail");
    assert!(matches!(err, MembusError::LeaseHeld { .. }));

    // 3. Released lease aborts atomically and leaves KV untouched.
    client
        .release_lease(
            &grant.namespace,
            &grant.resource,
            &grant.holder,
            grant.generation,
            &grant.token,
        )
        .await
        .expect("release");
    let err = client
        .commit_indexed_tx_with_leases(
            vec![],
            vec![guard],
            vec![put_mutation("slots", b"req/bin-1/ack", b"after-release")],
            Ack::Flush,
        )
        .await
        .expect_err("released lease must fail");
    assert!(matches!(err, MembusError::NotFound(_)));
    assert_eq!(
        client
            .get("slots", b"req/bin-1/ack")
            .await
            .unwrap()
            .unwrap()
            .value,
        b"ok-1"
    );

    bound.shutdown().await;
}
