use std::net::SocketAddr;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine as _;
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Empty, Full};
use hyper::header::{AUTHORIZATION, CONTENT_TYPE};
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use membus::cli::{parse_args, Command};
use membus::{
    Ack, AclGrant, AuthConfig, Client, CollectionDescriptor, CommitResult, CompatibilityMode,
    Endpoint, ExpectedVersion, FileColdStore, HealthStatus, IdempotencyContext, MembusError,
    MembusServer, MigrationState, MutationOp, NamespaceParityDigest, NoopPublisher,
    ParityDigestMode, Permission, Principal, ReplicatedDelta, ReplicationStamp, RetentionPolicy,
    RocksDbStorage, ServerConfig, ServerHandle, SignalBroker, SignalConfig, SignalPublisher,
    SignalRing, SlotRole, SlotRoleState, StorageConfig, StorageEngine, StoreProfile,
    TieringManager, TieringPolicy, TokenBinding,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message as WsMsg;

fn sha256(bytes: &[u8]) -> [u8; 32] {
    Sha256::digest(bytes).into()
}

struct SlotNode {
    _dir: TempDir,
    storage: Arc<RocksDbStorage>,
    http_addr: SocketAddr,
    tcp_endpoint: Endpoint,
    http_handle: ServerHandle,
    tcp_handle: ServerHandle,
}

impl SlotNode {
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
            .expect("open storage"),
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
        let server = MembusServer::new(
            Arc::clone(&storage) as Arc<dyn StorageEngine>,
            broker,
            cfg,
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
            panic!("expected http endpoint");
        };
        let tcp_endpoint = tcp_handle.endpoint().clone();

        Self {
            _dir: dir,
            storage,
            http_addr,
            tcp_endpoint,
            http_handle,
            tcp_handle,
        }
    }

    async fn shutdown(self) {
        self.http_handle.shutdown().await;
        self.tcp_handle.shutdown().await;
    }
}

fn ab_auth_config() -> AuthConfig {
    AuthConfig::enforced(vec![
        TokenBinding {
            token: "tok-reader".to_owned(),
            principal: Principal {
                id: "reader".to_owned(),
                grants: vec![AclGrant {
                    namespace_pattern: "*".to_owned(),
                    key_prefix: Vec::new(),
                    permissions: vec![Permission::Read],
                }],
            },
        },
        TokenBinding {
            token: "tok-writer".to_owned(),
            principal: Principal {
                id: "client-writer".to_owned(),
                grants: vec![AclGrant {
                    namespace_pattern: "*".to_owned(),
                    key_prefix: Vec::new(),
                    permissions: vec![Permission::Read, Permission::Write],
                }],
            },
        },
        TokenBinding {
            token: "tok-replicator".to_owned(),
            principal: Principal {
                id: "ab-router-shadow".to_owned(),
                grants: vec![AclGrant {
                    namespace_pattern: "*".to_owned(),
                    key_prefix: Vec::new(),
                    permissions: vec![Permission::Read, Permission::Write],
                }],
            },
        },
        TokenBinding {
            token: "tok-admin".to_owned(),
            principal: Principal {
                id: "operator-admin".to_owned(),
                grants: vec![AclGrant {
                    namespace_pattern: "*".to_owned(),
                    key_prefix: Vec::new(),
                    permissions: vec![Permission::Admin],
                }],
            },
        },
    ])
}

async fn http_call(
    addr: SocketAddr,
    method: Method,
    path: &str,
    bearer: Option<&str>,
    extra_headers: &[(&str, &str)],
    body: Option<serde_json::Value>,
) -> (StatusCode, hyper::HeaderMap, serde_json::Value) {
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
            .body(Empty::<Bytes>::new().map_err(|never| match never {}).boxed())
            .expect("req")
    };

    let resp = sender.send_request(req).await.expect("send http1");
    let status = resp.status();
    let headers = resp.headers().clone();
    let raw = resp.into_body().collect().await.expect("body").to_bytes();
    let parsed = if raw.is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::from_slice(&raw)
            .unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(&raw).into_owned()))
    };
    (status, headers, parsed)
}

async fn http_raw_put(
    addr: SocketAddr,
    path: &str,
    bearer: Option<&str>,
    body: &[u8],
) -> (StatusCode, serde_json::Value) {
    let stream = TcpStream::connect(addr).await.expect("connect http");
    let io = TokioIo::new(stream);
    let (mut sender, conn) = hyper::client::conn::http1::handshake(io)
        .await
        .expect("http1 handshake");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let mut builder = Request::builder()
        .method(Method::PUT)
        .uri(path)
        .header("host", addr.to_string())
        .header(CONTENT_TYPE, "application/octet-stream");
    if let Some(tok) = bearer {
        builder = builder.header(AUTHORIZATION, format!("Bearer {tok}"));
    }
    let req = builder
        .body(Full::new(Bytes::copy_from_slice(body)).boxed())
        .expect("req");
    let resp = sender.send_request(req).await.expect("send");
    let status = resp.status();
    let raw = resp.into_body().collect().await.expect("body").to_bytes();
    let parsed = serde_json::from_slice(&raw).unwrap_or(serde_json::Value::Null);
    (status, parsed)
}

async fn mirror_delta(
    target_addr: SocketAddr,
    origin_node: &str,
    origin_seq: u64,
    ts_ms: u64,
    ns: &str,
    key: &[u8],
    op: MutationOp,
) -> (StatusCode, hyper::HeaderMap, serde_json::Value) {
    let value_sha256 = match &op {
        MutationOp::Put { value } => sha256(value),
        MutationOp::Remove => [0u8; 32],
    };
    let delta = ReplicatedDelta {
        stamp: ReplicationStamp {
            timestamp_ms: ts_ms,
            origin_node_id: origin_node.to_owned(),
            origin_seq,
            value_sha256,
        },
        namespace: ns.to_owned(),
        key: key.to_vec(),
        op,
        indexes: None,
        search_op: None,
    };
    let route_origin = format!("router:{origin_node}");
    http_call(
        target_addr,
        Method::POST,
        "/v2/replication/apply",
        Some("tok-replicator"),
        &[("X-Membus-Route-Origin", &route_origin)],
        Some(serde_json::json!({
            "delta": delta,
            "ack": "Flush"
        })),
    )
    .await
}

#[tokio::test]
async fn five_stage_live_ab_cutover_reverse_shadow_rollback_and_contract() {
    let slot_a = SlotNode::start("slot-a", SlotRole::Active, ab_auth_config()).await;
    let slot_b = SlotNode::start("slot-b", SlotRole::Shadow, ab_auth_config()).await;

    // Verify initial slot roles & epochs on /v2/health.
    let (st, _, health_a) = http_call(
        slot_a.http_addr,
        Method::GET,
        "/v2/health",
        Some("tok-reader"),
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(health_a["slot_role"], "active");
    assert_eq!(health_a["role_epoch"], 1);

    let (st, _, health_b) = http_call(
        slot_b.http_addr,
        Method::GET,
        "/v2/health",
        Some("tok-reader"),
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(health_b["slot_role"], "shadow");
    assert_eq!(health_b["role_epoch"], 1);

    // =========================================================================
    // STAGE 1: Register v1, Expand Catalog (v1 -> v2, ExpandDual), Idempotent Writes on Slot A
    // =========================================================================
    let schema_v1 = br#"{"version":1,"fields":["id","amount"]}"#.to_vec();
    let schema_v2 = br#"{"version":2,"fields":["id","amount","currency"]}"#.to_vec();
    let sha_v1 = sha256(&schema_v1);
    let sha_v2 = sha256(&schema_v2);

    let reg_payload = serde_json::json!({
        "expected_version": null,
        "descriptor": {
            "namespace": "orders",
            "store_profile": "Kv",
            "schema_version": 1,
            "schema_sha256": sha_v1,
            "schema_bytes": B64.encode(&schema_v1),
            "declared_indexes": [],
            "retention_policy": { "max_age_secs": null, "max_entries": null },
            "migration_state": "Active"
        }
    });

    for addr in [slot_a.http_addr, slot_b.http_addr] {
        let (st, _, desc) = http_call(
            addr,
            Method::POST,
            "/v2/catalog/collections",
            Some("tok-admin"),
            &[],
            Some(reg_payload.clone()),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(desc["schema_version"], 1);
        assert_eq!(desc["compatibility_mode"], "Strict");
        assert_eq!(desc["compatible_versions"], serde_json::json!([1]));

        let (st, _, expanded) = http_call(
            addr,
            Method::POST,
            "/v2/catalog/collections/orders/migrations/begin",
            Some("tok-admin"),
            &[],
            Some(serde_json::json!({
                "expected_version": 1,
                "target_version": 2,
                "target_sha256": sha_v2,
                "target_schema_bytes": schema_v2
            })),
        )
        .await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(expanded["compatibility_mode"], "ExpandDual");
        assert_eq!(expanded["compatible_versions"], serde_json::json!([1, 2]));
    }

    // Write to Slot A with X-Membus-Idempotency-Key (PUT with Absent CAS guard).
    let (st, hdrs, c1) = http_call(
        slot_a.http_addr,
        Method::PUT,
        "/v2/kv/orders/ord-1",
        Some("tok-writer"),
        &[
            ("X-Membus-Idempotency-Key", "idem-ord-1-create"),
            ("X-Membus-Role-Epoch", "1"),
        ],
        Some(serde_json::json!({
            "value_utf8": "{\"id\":\"ord-1\",\"amount\":100}",
            "ack": "Flush",
            "expected": "Absent"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert!(hdrs.get("x-membus-idempotent-replay").is_none());
    let seq_ord1 = c1["seq"].as_u64().expect("seq");

    // Replaying exact same idempotency key + payload returns identical CommitResult without double-applying.
    let (st, hdrs, c1_replay) = http_call(
        slot_a.http_addr,
        Method::PUT,
        "/v2/kv/orders/ord-1",
        Some("tok-writer"),
        &[("X-Membus-Idempotency-Key", "idem-ord-1-create")],
        Some(serde_json::json!({
            "value_utf8": "{\"id\":\"ord-1\",\"amount\":100}",
            "ack": "Flush",
            "expected": "Absent"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(hdrs.get("x-membus-idempotent-replay").unwrap(), "true");
    assert_eq!(c1_replay["seq"], seq_ord1);
    assert_eq!(c1_replay["epoch"], c1["epoch"]);
    assert_eq!(c1_replay["versions"], c1["versions"]);
    assert_eq!(slot_a.storage.latest_seq(), seq_ord1);

    // Reusing the SAME idempotency key with a DIFFERENT payload MUST fail closed with 409 Conflict!
    let (st, _, conflict_body) = http_call(
        slot_a.http_addr,
        Method::PUT,
        "/v2/kv/orders/ord-1",
        Some("tok-writer"),
        &[("X-Membus-Idempotency-Key", "idem-ord-1-create")],
        Some(serde_json::json!({
            "value_utf8": "{\"id\":\"ord-1\",\"amount\":999}",
            "ack": "Flush",
            "expected": "Absent"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(conflict_body["error"], "idempotency_conflict");

    // Idempotent TX on Slot A (create ord-2 and ord-3).
    let tx_body = serde_json::json!({
        "ack": "Flush",
        "guards": [
            { "namespace": "orders", "key": b"ord-2".to_vec(), "expected": "Absent" }
        ],
        "mutations": [
            {
                "mutation": {
                    "namespace": "orders",
                    "key": b"ord-2".to_vec(),
                    "op": { "Put": { "value": br#"{"id":"ord-2","amount":200}"#.to_vec() } }
                },
                "indexes": null,
                "search_op": null
            },
            {
                "mutation": {
                    "namespace": "orders",
                    "key": b"ord-3".to_vec(),
                    "op": { "Put": { "value": br#"{"id":"ord-3","amount":300}"#.to_vec() } }
                },
                "indexes": null,
                "search_op": null
            }
        ]
    });
    let (st, _, tx1) = http_call(
        slot_a.http_addr,
        Method::POST,
        "/v2/tx",
        Some("tok-writer"),
        &[("X-Membus-Idempotency-Key", "idem-tx-batch-1")],
        Some(tx_body.clone()),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let tx1_seq = tx1["seq"].as_u64().unwrap();

    // Replay TX idempotently.
    let (st, hdrs, tx1_replay) = http_call(
        slot_a.http_addr,
        Method::POST,
        "/v2/tx",
        Some("tok-writer"),
        &[("X-Membus-Idempotency-Key", "idem-tx-batch-1")],
        Some(tx_body),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(hdrs.get("x-membus-idempotent-replay").unwrap(), "true");
    assert_eq!(tx1_replay["seq"], tx1_seq);
    assert_eq!(slot_a.storage.latest_seq(), tx1_seq);

    // Idempotent DELETE on Slot A (delete ord-3), verify replay + mismatch 409.
    let (st, _, del1) = http_call(
        slot_a.http_addr,
        Method::DELETE,
        "/v2/kv/orders/ord-3",
        Some("tok-writer"),
        &[("X-Membus-Idempotency-Key", "idem-del-ord-3")],
        Some(serde_json::json!({ "ack": "Flush", "expected": { "Version": tx1_seq } })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let del1_seq = del1["seq"].as_u64().unwrap();
    let (st, hdrs, del1_replay) = http_call(
        slot_a.http_addr,
        Method::DELETE,
        "/v2/kv/orders/ord-3",
        Some("tok-writer"),
        &[("X-Membus-Idempotency-Key", "idem-del-ord-3")],
        Some(serde_json::json!({ "ack": "Flush", "expected": { "Version": tx1_seq } })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(hdrs.get("x-membus-idempotent-replay").unwrap(), "true");
    assert_eq!(del1_replay["seq"], del1_seq);

    // Advance migration verification on A and B.
    let (st, _, adv) = http_call(
        slot_b.http_addr,
        Method::POST,
        "/v2/catalog/collections/orders/migrations/advance",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({ "target_version": 2, "verified_through": del1_seq })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(adv["migration_state"]["Migrating"]["verified_through"], del1_seq);

    // =========================================================================
    // STAGE 2: Mirror A -> B via /v2/replication/apply, Echo-Loop & Shadow Fencing Checks
    // =========================================================================

    // Direct client writes to Shadow Slot B MUST be rejected with 409 role_fenced!
    let (st, _, err_put) = http_call(
        slot_b.http_addr,
        Method::PUT,
        "/v2/kv/orders/illegal",
        Some("tok-writer"),
        &[],
        Some(serde_json::json!({ "value_utf8": "nope" })),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(err_put["error"], "role_fenced");

    let (st, _, err_del) = http_call(
        slot_b.http_addr,
        Method::DELETE,
        "/v2/kv/orders/illegal",
        Some("tok-writer"),
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(err_del["error"], "role_fenced");

    let (st, err_blob) = http_raw_put(
        slot_b.http_addr,
        "/v2/blobs/orders",
        Some("tok-writer"),
        b"blob-data",
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(err_blob["error"], "role_fenced");

    let (st, _, err_lease) = http_call(
        slot_b.http_addr,
        Method::POST,
        "/v2/leases/acquire",
        Some("tok-writer"),
        &[],
        Some(serde_json::json!({
            "namespace": "orders",
            "resource": "r1",
            "holder": "w1",
            "ttl_ms": 5000
        })),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(err_lease["error"], "role_fenced");

    // Echo-loop check: sending a delta with origin_node_id == "slot-b" to Slot B MUST be rejected/no-oped!
    let seq_b_before = slot_b.storage.latest_seq();
    let (st, hdrs, echo_resp) = mirror_delta(
        slot_b.http_addr,
        "slot-b",
        99,
        1_700_000_000_001,
        "orders",
        b"echo-key",
        MutationOp::Put {
            value: b"should-not-apply".to_vec(),
        },
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(echo_resp["applied"], false);
    assert!(echo_resp["local_seq"].is_null());
    assert_eq!(echo_resp["echo_rejected"], true);
    assert_eq!(hdrs.get("x-membus-echo-loop").unwrap(), "rejected");
    assert_eq!(slot_b.storage.latest_seq(), seq_b_before);
    assert!(slot_b.storage.get("orders", b"echo-key").unwrap().is_none());

    // Mirror Slot A's committed CF_LOG deltas to Shadow Slot B via /v2/replication/apply.
    let deltas_page = slot_a.storage.scan_deltas_after(0, 100).expect("scan deltas");
    for entry in &deltas_page.entries {
        for m in &entry.mutations {
            let (st, _, res) = mirror_delta(
                slot_b.http_addr,
                "slot-a",
                entry.seq,
                entry.ts_ms,
                &m.namespace,
                &m.key,
                m.op.clone(),
            )
            .await;
            assert_eq!(st, StatusCode::OK);
            assert_eq!(res["applied"], true);
            assert_eq!(res["echo_rejected"], false);
        }
    }

    // Reads on Shadow Slot B succeed.
    let (st, _, rec_b) = http_call(
        slot_b.http_addr,
        Method::GET,
        "/v2/kv/orders/ord-1",
        Some("tok-reader"),
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        rec_b["value"].as_array().unwrap().len(),
        br#"{"id":"ord-1","amount":100}"#.len()
    );

    // =========================================================================
    // STAGE 3: Parity Digest Gate (`full` AND `keys_only`)
    // =========================================================================
    let (st_a, _, dig_a_full) = http_call(
        slot_a.http_addr,
        Method::POST,
        "/v2/namespaces/orders/digest",
        Some("tok-reader"),
        &[],
        Some(serde_json::json!({ "mode": "full" })),
    )
    .await;
    let (st_b, _, dig_b_full) = http_call(
        slot_b.http_addr,
        Method::POST,
        "/v2/namespaces/orders/digest",
        Some("tok-reader"),
        &[],
        Some(serde_json::json!({ "mode": "full" })),
    )
    .await;
    assert_eq!(st_a, StatusCode::OK);
    assert_eq!(st_b, StatusCode::OK);
    let dig_a: NamespaceParityDigest = serde_json::from_value(dig_a_full).expect("digest A");
    let dig_b: NamespaceParityDigest = serde_json::from_value(dig_b_full).expect("digest B");
    assert_eq!(dig_a.node_id, "slot-a");
    assert_eq!(dig_b.node_id, "slot-b");
    assert_eq!(dig_a.entry_count, 2);
    assert_eq!(dig_b.entry_count, 2);
    assert_eq!(dig_a.xor_digest_hex, dig_b.xor_digest_hex);
    assert_eq!(dig_a.catalog_schema_version, Some(1));
    assert_eq!(dig_b.catalog_schema_version, Some(1));

    let (st_a, _, dig_a_keys) = http_call(
        slot_a.http_addr,
        Method::POST,
        "/v2/namespaces/orders/digest",
        Some("tok-reader"),
        &[],
        Some(serde_json::json!({ "mode": "keys_only" })),
    )
    .await;
    let (st_b, _, dig_b_keys) = http_call(
        slot_b.http_addr,
        Method::POST,
        "/v2/namespaces/orders/digest",
        Some("tok-reader"),
        &[],
        Some(serde_json::json!({ "mode": "keys_only" })),
    )
    .await;
    assert_eq!(st_a, StatusCode::OK);
    assert_eq!(st_b, StatusCode::OK);
    assert_eq!(dig_a_keys["entry_count"], 2);
    assert_eq!(dig_a_keys["xor_digest_hex"], dig_b_keys["xor_digest_hex"]);
    assert_ne!(dig_a_keys["xor_digest_hex"], dig_a.xor_digest_hex);

    // =========================================================================
    // STAGE 4: Fence A -> Promote B -> ReverseShadow -> Reverse-Mirror B->A -> Instant Rollback!
    // =========================================================================

    // Non-admin cannot transition slot role.
    let (st, _, _) = http_call(
        slot_a.http_addr,
        Method::POST,
        "/v2/admin/role",
        Some("tok-writer"),
        &[],
        Some(serde_json::json!({
            "expected_role": "active",
            "expected_role_epoch": 1,
            "new_role": "shadow"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::UNAUTHORIZED);

    // Fence Slot A: Active (epoch 1) -> Shadow (epoch 2).
    let (st, _, role_a2) = http_call(
        slot_a.http_addr,
        Method::POST,
        "/v2/admin/role",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({
            "expected_role": "active",
            "expected_role_epoch": 1,
            "new_role": "shadow"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(role_a2["role"], "shadow");
    assert_eq!(role_a2["role_epoch"], 2);

    // Stale CAS transition on Slot A fails closed with 409 conflict.
    let (st, _, stale_cas) = http_call(
        slot_a.http_addr,
        Method::POST,
        "/v2/admin/role",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({
            "expected_role": "active",
            "expected_role_epoch": 1,
            "new_role": "read_only"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(stale_cas["error"], "conflict");

    // Client writes to fenced Slot A now fail with 409 role_fenced.
    let (st, _, _) = http_call(
        slot_a.http_addr,
        Method::PUT,
        "/v2/kv/orders/fenced-a",
        Some("tok-writer"),
        &[],
        Some(serde_json::json!({ "value_utf8": "blocked" })),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);

    // Promote Slot B: Shadow (epoch 1) -> Active (epoch 2).
    let (st, _, role_b2) = http_call(
        slot_b.http_addr,
        Method::POST,
        "/v2/admin/role",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({
            "expected_role": "shadow",
            "expected_role_epoch": 1,
            "new_role": "active"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(role_b2["role"], "active");
    assert_eq!(role_b2["role_epoch"], 2);

    // Stale X-Membus-Role-Epoch: 1 on Active Slot B (epoch 2) MUST be rejected with 409 role_fenced!
    let (st, _, stale_hdr) = http_call(
        slot_b.http_addr,
        Method::PUT,
        "/v2/kv/orders/ord-4",
        Some("tok-writer"),
        &[("X-Membus-Role-Epoch", "1")],
        Some(serde_json::json!({ "value_utf8": "stale-writer" })),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(stale_hdr["error"], "role_fenced");

    // Enter ReverseShadow on Slot B catalog (v1 -> v2 with rollback window to v1).
    let (st, _, rev_b) = http_call(
        slot_b.http_addr,
        Method::POST,
        "/v2/catalog/collections/orders/migrations/reverse-shadow",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({
            "target_version": 2,
            "verified_through": dig_b.latest_seq
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(rev_b["schema_version"], 2);
    assert_eq!(rev_b["compatibility_mode"], "ReverseShadow");
    assert_eq!(rev_b["compatible_versions"], serde_json::json!([1, 2]));
    assert_eq!(rev_b["migration_state"]["ReverseShadow"]["rollback_version"], 1);

    // Write new v2 record to Active Slot B with valid X-Membus-Role-Epoch: 2.
    let v2_val = r#"{"id":"ord-4","amount":450,"currency":"USD"}"#;
    let (st, _, cb_new) = http_call(
        slot_b.http_addr,
        Method::PUT,
        "/v2/kv/orders/ord-4",
        Some("tok-writer"),
        &[
            ("X-Membus-Role-Epoch", "2"),
            ("X-Membus-Idempotency-Key", "idem-ord-4-b"),
        ],
        Some(serde_json::json!({ "value_utf8": v2_val, "ack": "Flush" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let b_new_seq = cb_new["seq"].as_u64().unwrap();

    // Reverse-mirror Slot B's new write back to Shadow Slot A!
    let rec_b4 = slot_b.storage.get("orders", b"ord-4").unwrap().unwrap();
    let (st, _, rev_apply) = mirror_delta(
        slot_a.http_addr,
        "slot-b",
        b_new_seq,
        rec_b4.meta.ts_ms,
        "orders",
        b"ord-4",
        MutationOp::Put {
            value: rec_b4.value.clone(),
        },
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(rev_apply["applied"], true);

    // Verify full parity digest matches between B and A (now 3 records each!).
    let (_, _, da4) = http_call(
        slot_a.http_addr,
        Method::POST,
        "/v2/namespaces/orders/digest",
        Some("tok-reader"),
        &[],
        Some(serde_json::json!({ "mode": "full" })),
    )
    .await;
    let (_, _, db4) = http_call(
        slot_b.http_addr,
        Method::POST,
        "/v2/namespaces/orders/digest",
        Some("tok-reader"),
        &[],
        Some(serde_json::json!({ "mode": "full" })),
    )
    .await;
    assert_eq!(da4["entry_count"], 3);
    assert_eq!(db4["entry_count"], 3);
    assert_eq!(da4["xor_digest_hex"], db4["xor_digest_hex"]);
    assert_eq!(db4["catalog_schema_version"], 2);

    // --- INSTANT ZERO-LOSS ROLLBACK TEST ---
    // 1. Fence Slot B Active (epoch 2) -> Shadow (epoch 3).
    let (st, _, rb_b_role) = http_call(
        slot_b.http_addr,
        Method::POST,
        "/v2/admin/role",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({
            "expected_role": "active",
            "expected_role_epoch": 2,
            "new_role": "shadow"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(rb_b_role["role"], "shadow");
    assert_eq!(rb_b_role["role_epoch"], 3);

    // 2. Roll back Slot B catalog v2 -> v1 cleanly without tripping forward-only version guard!
    let (st, _, rb_desc) = http_call(
        slot_b.http_addr,
        Method::POST,
        "/v2/catalog/collections/orders/migrations/rollback",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({ "rollback_version": 1 })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(rb_desc["schema_version"], 1);
    assert_eq!(rb_desc["compatibility_mode"], "Strict");
    assert_eq!(rb_desc["compatible_versions"], serde_json::json!([1]));
    assert_eq!(rb_desc["migration_state"], "Active");

    // 3. Flip Slot A back Shadow (epoch 2) -> Active (epoch 3) and prove ZERO data loss!
    let (st, _, role_a3) = http_call(
        slot_a.http_addr,
        Method::POST,
        "/v2/admin/role",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({
            "expected_role": "shadow",
            "expected_role_epoch": 2,
            "new_role": "active"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(role_a3["role"], "active");
    assert_eq!(role_a3["role_epoch"], 3);

    let ord4_on_a = slot_a
        .storage
        .get("orders", b"ord-4")
        .expect("get ord-4 on A")
        .expect("ord-4 MUST be preserved on A with zero data loss");
    assert_eq!(ord4_on_a.value, v2_val.as_bytes());

    // =========================================================================
    // STAGE 5: Re-Promote B -> Contract v2 -> Drain A (ReadOnly -> Draining)
    // =========================================================================
    // Re-expand and enter ReverseShadow on Slot B (reusing archived immutable v2 safely!).
    let (st, _, _) = http_call(
        slot_b.http_addr,
        Method::POST,
        "/v2/catalog/collections/orders/migrations/begin",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({
            "expected_version": 1,
            "target_version": 2,
            "target_sha256": sha_v2,
            "target_schema_bytes": schema_v2
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, _, _) = http_call(
        slot_b.http_addr,
        Method::POST,
        "/v2/catalog/collections/orders/migrations/reverse-shadow",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({
            "target_version": 2,
            "verified_through": b_new_seq
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // Fence A Active (3) -> Shadow (4), Promote B Shadow (3) -> Active (4).
    let (st, _, _) = http_call(
        slot_a.http_addr,
        Method::POST,
        "/v2/admin/role",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({
            "expected_role": "active",
            "expected_role_epoch": 3,
            "new_role": "shadow"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, _, _) = http_call(
        slot_b.http_addr,
        Method::POST,
        "/v2/admin/role",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({
            "expected_role": "shadow",
            "expected_role_epoch": 3,
            "new_role": "active"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // Contract Slot B catalog to Strict v2 Active!
    let (st, _, contracted) = http_call(
        slot_b.http_addr,
        Method::POST,
        "/v2/catalog/collections/orders/migrations/contract",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({ "expected_version": 2 })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(contracted["schema_version"], 2);
    assert_eq!(contracted["compatibility_mode"], "Strict");
    assert_eq!(contracted["compatible_versions"], serde_json::json!([2]));
    assert_eq!(contracted["migration_state"], "Active");

    // Transition Slot A: Shadow (4) -> ReadOnly (5).
    let (st, _, ro_a) = http_call(
        slot_a.http_addr,
        Method::POST,
        "/v2/admin/role",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({
            "expected_role": "shadow",
            "expected_role_epoch": 4,
            "new_role": "read_only"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(ro_a["role"], "read_only");
    assert_eq!(ro_a["role_epoch"], 5);

    // In ReadOnly, BOTH client writes AND replicated apply fail with 409 role_fenced.
    let (st, _, _) = http_call(
        slot_a.http_addr,
        Method::PUT,
        "/v2/kv/orders/ro-nope",
        Some("tok-writer"),
        &[],
        Some(serde_json::json!({ "value_utf8": "x" })),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);
    let (st, _, ro_rep) = mirror_delta(
        slot_a.http_addr,
        "slot-b",
        999,
        slot_b.storage.hlc_now_ms() + 100,
        "orders",
        b"ro-rep",
        MutationOp::Put {
            value: b"nope".to_vec(),
        },
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(ro_rep["error"], "role_fenced");

    // Reads, parity digests, and CF_LOG catch-up scans still succeed in ReadOnly.
    let (st, _, _) = http_call(
        slot_a.http_addr,
        Method::GET,
        "/v2/kv/orders/ord-1",
        Some("tok-reader"),
        &[],
        None,
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let (st, _, _) = http_call(
        slot_a.http_addr,
        Method::POST,
        "/v2/namespaces/orders/digest",
        Some("tok-reader"),
        &[],
        Some(serde_json::json!({ "mode": "full" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert!(!slot_a.storage.scan_deltas_after(0, 100).unwrap().entries.is_empty());

    // Transition Slot A: ReadOnly (5) -> Draining (6).
    let (st, _, drain_a) = http_call(
        slot_a.http_addr,
        Method::POST,
        "/v2/admin/role",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({
            "expected_role": "read_only",
            "expected_role_epoch": 5,
            "new_role": "draining"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(drain_a["role"], "draining");
    assert_eq!(drain_a["role_epoch"], 6);

    // In Draining, writes return 503 Service Unavailable (`role_fenced`) while reads & digests still serve!
    let (st, _, drain_err) = http_call(
        slot_a.http_addr,
        Method::PUT,
        "/v2/kv/orders/drain-nope",
        Some("tok-writer"),
        &[],
        Some(serde_json::json!({ "value_utf8": "x" })),
    )
    .await;
    assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(drain_err["error"], "role_fenced");

    let (st, _, _) = http_call(
        slot_a.http_addr,
        Method::POST,
        "/v2/namespaces/orders/digest",
        Some("tok-reader"),
        &[],
        Some(serde_json::json!({ "mode": "keys_only" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    slot_a.shutdown().await;
    slot_b.shutdown().await;
}

#[tokio::test]
async fn binary_rpc_and_ws_ab_primitives_and_role_fencing() {
    let slot = SlotNode::start("bin-slot", SlotRole::Active, ab_auth_config()).await;

    let admin = Client::connect(&slot.tcp_endpoint).await.expect("connect admin");
    admin.authenticate("tok-admin").await.expect("auth admin");
    let writer = Client::connect(&slot.tcp_endpoint).await.expect("connect writer");
    writer.authenticate("tok-writer").await.expect("auth writer");

    // HealthV2 reports slot_role + role_epoch while legacy Health preserves 7-field compatibility.
    let h2 = admin.health_v2().await.expect("health_v2");
    assert_eq!(h2.slot_role, SlotRole::Active);
    assert_eq!(h2.role_epoch, 1);
    let h1 = admin.health().await.expect("legacy health");
    assert_eq!(h1.epoch, h2.epoch);

    // Register collection v1 and exercise all binary migration RPCs.
    let s1 = b"v1-schema".to_vec();
    let s2 = b"v2-schema".to_vec();
    admin
        .register_collection(
            None,
            CollectionDescriptor {
                namespace: "items".to_owned(),
                store_profile: StoreProfile::Kv,
                schema_version: 1,
                schema_sha256: sha256(&s1),
                schema_bytes: s1.clone(),
                declared_indexes: Vec::new(),
                retention_policy: RetentionPolicy::default(),
                migration_state: MigrationState::Active,
                compatible_versions: Vec::new(),
                compatibility_mode: CompatibilityMode::Strict,
            },
        )
        .await
        .expect("reg");

    let beg = admin
        .begin_migration("items", 1, 2, sha256(&s2), s2.clone())
        .await
        .expect("begin");
    assert_eq!(beg.compatibility_mode, CompatibilityMode::ExpandDual);
    assert_eq!(beg.compatible_versions, vec![1, 2]);

    let adv = admin
        .advance_migration_verification("items", 2, 10)
        .await
        .expect("advance");
    assert_eq!(
        adv.migration_state,
        MigrationState::Migrating {
            target_version: 2,
            target_sha256: sha256(&s2),
            verified_through: 10,
        }
    );

    let rev = admin
        .enter_reverse_shadow("items", 2, 12)
        .await
        .expect("reverse_shadow");
    assert_eq!(rev.schema_version, 2);
    assert_eq!(rev.compatibility_mode, CompatibilityMode::ReverseShadow);

    let rb = admin.rollback_migration("items", 1).await.expect("rollback");
    assert_eq!(rb.schema_version, 1);
    assert_eq!(rb.compatibility_mode, CompatibilityMode::Strict);

    // Re-expand -> reverse_shadow -> contract over binary RPC.
    admin
        .begin_migration("items", 1, 2, sha256(&s2), s2)
        .await
        .expect("begin 2");
    admin
        .enter_reverse_shadow("items", 2, 15)
        .await
        .expect("rev 2");
    let contracted = admin
        .contract_migration("items", 2)
        .await
        .expect("contract");
    assert_eq!(contracted.schema_version, 2);
    assert_eq!(contracted.compatibility_mode, CompatibilityMode::Strict);
    assert_eq!(contracted.compatible_versions, vec![2]);

    // Write a record, then transition to Shadow via binary RPC.
    writer
        .put("items", b"k1", b"v1", Ack::Flush)
        .await
        .expect("put active");

    let st2 = admin
        .transition_slot_role(SlotRole::Active, 1, SlotRole::Shadow)
        .await
        .expect("transition shadow");
    assert_eq!(
        st2,
        SlotRoleState {
            role: SlotRole::Shadow,
            role_epoch: 2
        }
    );

    // Binary client write rejected with RoleFenced in Shadow.
    let err = writer.put("items", b"k2", b"v2", Ack::Fast).await;
    assert!(matches!(
        err,
        Err(MembusError::RoleFenced {
            role: SlotRole::Shadow,
            role_epoch: 2,
            ..
        })
    ));

    // Binary ApplyReplicatedDelta succeeds in Shadow (and rejects local echo loop).
    let rep_val = b"replicated-v2".to_vec();
    let applied = writer
        .apply_replicated_delta(
            ReplicatedDelta {
                stamp: ReplicationStamp {
                    timestamp_ms: slot.storage.hlc_now_ms() + 50,
                    origin_node_id: "peer-slot".to_owned(),
                    origin_seq: 1,
                    value_sha256: sha256(&rep_val),
                },
                namespace: "items".to_owned(),
                key: b"k2".to_vec(),
                op: MutationOp::Put {
                    value: rep_val.clone(),
                },
                indexes: None,
                search_op: None,
            },
            Ack::Flush,
            Some("router:peer-slot".to_owned()),
        )
        .await
        .expect("apply_replicated_delta");
    assert!(applied.applied);
    assert!(applied.local_seq.is_some());

    let echo = writer
        .apply_replicated_delta(
            ReplicatedDelta {
                stamp: ReplicationStamp {
                    timestamp_ms: slot.storage.hlc_now_ms() + 60,
                    origin_node_id: "bin-slot".to_owned(),
                    origin_seq: 2,
                    value_sha256: sha256(b"echo"),
                },
                namespace: "items".to_owned(),
                key: b"k3".to_vec(),
                op: MutationOp::Put {
                    value: b"echo".to_vec(),
                },
                indexes: None,
                search_op: None,
            },
            Ack::Fast,
            None,
        )
        .await
        .expect("echo no-op");
    assert!(!echo.applied);
    assert!(echo.local_seq.is_none());

    // Binary NamespaceDigest works in Shadow.
    let dig = writer
        .namespace_digest("items", b"", ParityDigestMode::Full)
        .await
        .expect("digest");
    assert_eq!(dig.node_id, "bin-slot");
    assert_eq!(dig.entry_count, 2);
    assert_eq!(dig.catalog_schema_version, Some(2));

    // WebSocket Put MUST also be rejected when slot is Shadow!
    let ws_url = format!("ws://{}/v2/ws", slot.http_addr);
    let mut ws_req = tokio_tungstenite::tungstenite::client::IntoClientRequest::into_client_request(ws_url)
        .expect("ws req");
    ws_req
        .headers_mut()
        .insert(AUTHORIZATION, "Bearer tok-writer".parse().unwrap());
    let (mut ws, _) = tokio_tungstenite::connect_async(ws_req)
        .await
        .expect("connect ws");
    ws.send(WsMsg::Text(
        serde_json::json!({
            "id": 1,
            "op": "put",
            "namespace": "items",
            "key_utf8": "ws-k",
            "value_utf8": "ws-v"
        })
        .to_string()
        .into(),
    ))
    .await
    .expect("send ws put");
    let reply = ws.next().await.expect("ws reply").expect("ws msg");
    let reply_json: serde_json::Value =
        serde_json::from_str(reply.to_text().unwrap()).expect("json");
    assert_eq!(reply_json["ok"], false);
    assert!(
        reply_json["error"]
            .as_str()
            .unwrap_or("")
            .contains("slot role fenced"),
        "expected role fenced ws error, got {reply_json}"
    );
    let _ = ws.close(None).await;

    drop((admin, writer));
    slot.shutdown().await;
}

#[tokio::test]
async fn slot_role_persistence_idempotency_pruning_cold_ref_digest_and_legacy_serde() {
    // 1. Legacy JSON deserialization compatibility (#[serde(default)]).
    let legacy_sha = sha256(b"legacy");
    let legacy_desc_json = serde_json::json!({
        "namespace": "legacy",
        "store_profile": "Kv",
        "schema_version": 1,
        "schema_sha256": legacy_sha,
        "schema_bytes": B64.encode(b"legacy"),
        "declared_indexes": [],
        "retention_policy": { "max_age_secs": null, "max_entries": null },
        "migration_state": "Active"
    });
    let parsed_desc: CollectionDescriptor =
        serde_json::from_value(legacy_desc_json).expect("legacy CollectionDescriptor");
    assert_eq!(parsed_desc.compatibility_mode, CompatibilityMode::Strict);
    assert!(parsed_desc.compatible_versions.is_empty());

    let legacy_health_json = serde_json::json!({
        "epoch": 7,
        "latest_seq": 10,
        "log_horizon_seq": 0,
        "in_flight_ops": 0,
        "in_flight_bytes": 0,
        "active_connections": 1,
        "active_subscriptions": 0
    });
    let parsed_health: HealthStatus =
        serde_json::from_value(legacy_health_json).expect("legacy HealthStatus");
    assert_eq!(parsed_health.slot_role, SlotRole::Active);
    assert_eq!(parsed_health.role_epoch, 0);

    // 2. CLI --slot-role and --node-id parsing.
    let cmd = parse_args([
        "serve",
        "--data-dir",
        "/var/lib/membus",
        "--http-listen",
        "127.0.0.1:8080",
        "--slot-role",
        "shadow",
        "--node-id",
        "slot-b",
    ])
    .expect("parse serve args");
    match cmd {
        Command::Serve(args) => {
            assert_eq!(args.slot_role, SlotRole::Shadow);
            assert_eq!(args.node_id.as_deref(), Some("slot-b"));
        }
        other => panic!("unexpected command: {other:?}"),
    }

    // 3. Slot role persistence across DB restart + bounded idempotency pruning.
    let dir = TempDir::new().expect("tempdir");
    {
        let storage = Arc::new(
            RocksDbStorage::open(
                dir.path(),
                StorageConfig {
                    bg_sync_interval: None,
                    max_idempotency_entries: 3,
                    ..StorageConfig::default()
                },
                Arc::new(NoopPublisher),
            )
            .expect("open"),
        );
        let broker = Arc::new(SignalBroker::new(
            Arc::clone(&storage) as Arc<dyn StorageEngine>,
            Arc::new(SignalRing::new(SignalConfig::default())),
            SignalConfig::default(),
        ));
        let server = MembusServer::new(
            Arc::clone(&storage) as Arc<dyn StorageEngine>,
            broker,
            ServerConfig::default(),
        );
        let transitioned = server
            .context()
            .transition_slot_role(SlotRole::Active, 1, SlotRole::Shadow)
            .expect("transition");
        assert_eq!(transitioned.role, SlotRole::Shadow);
        assert_eq!(transitioned.role_epoch, 2);

        // Insert 6 idempotent keys with max_idempotency_entries = 3 -> oldest 3 pruned!
        for i in 0..6u32 {
            let key = format!("k-{i}");
            let idem = IdempotencyContext {
                scope_namespace: "prune_ns".to_owned(),
                key: format!("idem-{i}"),
                request_fingerprint_sha256: sha256(key.as_bytes()),
                principal_id: None,
                is_tx: false,
            };
            let (commit, replayed) = storage
                .put_indexed_idempotent(
                    "prune_ns",
                    key.as_bytes(),
                    b"val",
                    Ack::Flush,
                    ExpectedVersion::Any,
                    None,
                    None,
                    Some(&idem),
                )
                .expect("idem put");
            assert!(!replayed);
            assert!(commit.seq > 0);
        }
        let retained_ord = storage.meta_scan_prefix(b"idem:ord:").expect("scan ord");
        let retained_k = storage.meta_scan_prefix(b"idem:k:").expect("scan k");
        assert!(
            retained_ord.len() <= 3 && retained_k.len() <= 3,
            "expected bounded <= 3 idempotency records, got ord={} k={}",
            retained_ord.len(),
            retained_k.len()
        );
        storage.flush_wal(true).expect("flush");
    }

    // Reopen DB: persisted Shadow role (epoch 2) MUST survive even when ServerConfig defaults to Active!
    {
        let storage = Arc::new(
            RocksDbStorage::open(
                dir.path(),
                StorageConfig {
                    bg_sync_interval: None,
                    max_idempotency_entries: 3,
                    ..StorageConfig::default()
                },
                Arc::new(NoopPublisher),
            )
            .expect("reopen"),
        );
        let broker = Arc::new(SignalBroker::new(
            Arc::clone(&storage) as Arc<dyn StorageEngine>,
            Arc::new(SignalRing::new(SignalConfig::default())),
            SignalConfig::default(),
        ));
        let server = MembusServer::new(
            Arc::clone(&storage) as Arc<dyn StorageEngine>,
            broker,
            ServerConfig::default(),
        );
        let persisted = server.context().slot_role_state();
        assert_eq!(persisted.role, SlotRole::Shadow);
        assert_eq!(persisted.role_epoch, 2);

        // Most recent idempotency key (`idem-5`) survived restart and still replays!
        let idem5 = IdempotencyContext {
            scope_namespace: "prune_ns".to_owned(),
            key: "idem-5".to_owned(),
            request_fingerprint_sha256: sha256(b"k-5"),
            principal_id: None,
            is_tx: false,
        };
        let (CommitResult { seq, .. }, replayed) = storage
            .put_indexed_idempotent(
                "prune_ns",
                b"k-5",
                b"val",
                Ack::Fast,
                ExpectedVersion::Any,
                None,
                None,
                Some(&idem5),
            )
            .expect("replay after restart");
        assert!(replayed);
        assert!(seq > 0);
    }

    // 4. ColdRef parity digest resolution matches hot replica byte-for-byte!
    let hot_dir = TempDir::new().expect("hot");
    let cold_dir = TempDir::new().expect("cold");
    let tier_dir = TempDir::new().expect("tier");
    let hot = Arc::new(
        RocksDbStorage::open(
            hot_dir.path(),
            StorageConfig {
                bg_sync_interval: None,
                ..StorageConfig::default()
            },
            Arc::new(NoopPublisher),
        )
        .expect("hot"),
    );
    let cold = Arc::new(
        RocksDbStorage::open(
            cold_dir.path(),
            StorageConfig {
                bg_sync_interval: None,
                ..StorageConfig::default()
            },
            Arc::new(NoopPublisher),
        )
        .expect("cold"),
    );
    let payload = vec![b'Z'; 4096];
    hot.put("archive", b"big", &payload, Ack::Flush, ExpectedVersion::Any)
        .expect("hot put");
    cold.put("archive", b"big", &payload, Ack::Flush, ExpectedVersion::Any)
        .expect("cold put");

    let cold_store = Arc::new(FileColdStore::open(tier_dir.path()).expect("cold store"));
    let tiering = TieringManager::new(
        Arc::clone(&cold) as Arc<dyn StorageEngine>,
        cold_store,
    );
    let policy = TieringPolicy {
        namespace: "archive".to_owned(),
        key_prefix: Vec::new(),
        min_age_ms: 0,
        min_value_bytes: 128,
        max_entries_per_run: 64,
    };
    let archived = tiering
        .archive_cold_entries(&policy, u64::MAX, Ack::Flush)
        .expect("archive");
    assert_eq!(archived.archived, 1);

    let hot_cat = membus::CollectionCatalog::new(Arc::clone(&hot) as Arc<dyn StorageEngine>);
    let cold_cat = membus::CollectionCatalog::new(Arc::clone(&cold) as Arc<dyn StorageEngine>);
    let d_hot = membus::compute_namespace_parity_digest(
        hot.as_ref(),
        &hot_cat,
        "hot-slot",
        "archive",
        b"",
        ParityDigestMode::Full,
    )
    .expect("hot digest");
    let d_cold = membus::compute_namespace_parity_digest(
        cold.as_ref(),
        &cold_cat,
        "cold-slot",
        "archive",
        b"",
        ParityDigestMode::Full,
    )
    .expect("cold digest");
    assert_eq!(d_hot.entry_count, 1);
    assert_eq!(d_cold.entry_count, 1);
    assert_eq!(d_hot.xor_digest_hex, d_cold.xor_digest_hex);
}

#[tokio::test]
async fn audit_linearizable_role_fencing_blocks_post_fence_commits() {
    let slot = SlotNode::start("race-slot", SlotRole::Active, ab_auth_config()).await;
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mut writers = Vec::new();

    // Launch concurrent HTTP and binary writer tasks hammering the Active slot.
    for worker in 0..4usize {
        let addr = slot.http_addr;
        let stop_flag = Arc::clone(&stop);
        writers.push(tokio::spawn(async move {
            let mut i = 0u64;
            while !stop_flag.load(std::sync::atomic::Ordering::Acquire) {
                let path = format!("/v2/kv/race/http-{worker}-{i}");
                let (st, _, _) = http_call(
                    addr,
                    Method::PUT,
                    &path,
                    Some("tok-writer"),
                    &[],
                    Some(serde_json::json!({ "value_utf8": "v", "ack": "Fast" })),
                )
                .await;
                assert!(
                    st == StatusCode::OK || st == StatusCode::CONFLICT,
                    "unexpected status during fence race: {st}"
                );
                i += 1;
            }
        }));
    }

    for worker in 0..2usize {
        let ep = slot.tcp_endpoint.clone();
        let stop_flag = Arc::clone(&stop);
        writers.push(tokio::spawn(async move {
            let client = Client::connect(&ep).await.expect("connect");
            client.authenticate("tok-writer").await.expect("auth");
            let mut i = 0u64;
            while !stop_flag.load(std::sync::atomic::Ordering::Acquire) {
                let key = format!("bin-{worker}-{i}");
                let res = client
                    .put("race", key.as_bytes(), b"v", Ack::Fast)
                    .await;
                assert!(
                    matches!(res, Ok(_) | Err(MembusError::RoleFenced { .. })),
                    "unexpected binary result during fence race: {res:?}"
                );
                i += 1;
            }
        }));
    }

    // Let writers establish active concurrency, then CAS-fence Active -> Shadow.
    tokio::time::sleep(std::time::Duration::from_millis(40)).await;
    let (st, _, role_after) = http_call(
        slot.http_addr,
        Method::POST,
        "/v2/admin/role",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({
            "expected_role": "active",
            "expected_role_epoch": 1,
            "new_role": "shadow"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(role_after["role"], "shadow");
    assert_eq!(role_after["role_epoch"], 2);

    // Linearizability invariant: once transition_slot_role returns Shadow, NO in-flight
    // client write may commit afterward, so `latest_seq` MUST remain frozen!
    let seq_at_fence = slot.storage.latest_seq();
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    stop.store(true, std::sync::atomic::Ordering::Release);
    for w in writers {
        w.await.expect("writer join");
    }
    assert_eq!(
        slot.storage.latest_seq(),
        seq_at_fence,
        "a client write committed after Active -> Shadow role fence returned"
    );

    slot.shutdown().await;
}

#[tokio::test]
async fn audit_catalog_write_fencing_archived_digest_and_legacy_normalization() {
    let slot = SlotNode::start("cat-slot", SlotRole::Active, ab_auth_config()).await;
    let admin = Client::connect(&slot.tcp_endpoint).await.expect("connect admin");
    admin.authenticate("tok-admin").await.expect("auth admin");

    let s1 = b"schema-v1".to_vec();
    let s2 = b"schema-v2".to_vec();
    let s2_conflicting = b"schema-v2-different".to_vec();

    // Register v1 in Active (epoch 1).
    let (st, _, _) = http_call(
        slot.http_addr,
        Method::POST,
        "/v2/catalog/collections",
        Some("tok-admin"),
        &[("X-Membus-Role-Epoch", "1")],
        Some(serde_json::json!({
            "expected_version": null,
            "descriptor": {
                "namespace": "orders",
                "store_profile": "Kv",
                "schema_version": 1,
                "schema_sha256": sha256(&s1).to_vec(),
                "schema_bytes": B64.encode(&s1),
                "declared_indexes": [],
                "retention_policy": RetentionPolicy::default(),
                "migration_state": "Active"
            }
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // Transition Active (1) -> Shadow (2): catalog migrations MUST still be allowed in Shadow,
    // while stale X-Membus-Role-Epoch (1 < 2) MUST be rejected with 409 role_fenced!
    let (st, _, _) = http_call(
        slot.http_addr,
        Method::POST,
        "/v2/admin/role",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({
            "expected_role": "active",
            "expected_role_epoch": 1,
            "new_role": "shadow"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, _, stale_cat) = http_call(
        slot.http_addr,
        Method::POST,
        "/v2/catalog/collections/orders/migrations/begin",
        Some("tok-admin"),
        &[("X-Membus-Role-Epoch", "1")],
        Some(serde_json::json!({
            "expected_version": 1,
            "target_version": 2,
            "target_sha256": sha256(&s2).to_vec(),
            "target_schema_bytes": s2.clone()
        })),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(stale_cat["error"], "role_fenced");

    // With valid epoch 2 in Shadow, begin -> reverse-shadow -> rollback succeeds.
    let (st, _, _) = http_call(
        slot.http_addr,
        Method::POST,
        "/v2/catalog/collections/orders/migrations/begin",
        Some("tok-admin"),
        &[("X-Membus-Role-Epoch", "2")],
        Some(serde_json::json!({
            "expected_version": 1,
            "target_version": 2,
            "target_sha256": sha256(&s2).to_vec(),
            "target_schema_bytes": s2.clone()
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, _, _) = http_call(
        slot.http_addr,
        Method::POST,
        "/v2/catalog/collections/orders/migrations/reverse-shadow",
        Some("tok-admin"),
        &[("X-Membus-Role-Epoch", "2")],
        Some(serde_json::json!({ "target_version": 2, "verified_through": 0 })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, _, _) = http_call(
        slot.http_addr,
        Method::POST,
        "/v2/catalog/collections/orders/migrations/rollback",
        Some("tok-admin"),
        &[("X-Membus-Role-Epoch", "2")],
        Some(serde_json::json!({ "rollback_version": 1 })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // Audit #7: Re-beginning migration to archived version 2 with a DIFFERENT schema digest
    // MUST fail immediately at begin_migration with Conflict, while identical digest succeeds!
    let (st, _, collision_err) = http_call(
        slot.http_addr,
        Method::POST,
        "/v2/catalog/collections/orders/migrations/begin",
        Some("tok-admin"),
        &[("X-Membus-Role-Epoch", "2")],
        Some(serde_json::json!({
            "expected_version": 1,
            "target_version": 2,
            "target_sha256": sha256(&s2_conflicting).to_vec(),
            "target_schema_bytes": s2_conflicting.clone()
        })),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(collision_err["error"], "conflict");

    let (st, _, _) = http_call(
        slot.http_addr,
        Method::POST,
        "/v2/catalog/collections/orders/migrations/begin",
        Some("tok-admin"),
        &[("X-Membus-Role-Epoch", "2")],
        Some(serde_json::json!({
            "expected_version": 1,
            "target_version": 2,
            "target_sha256": sha256(&s2).to_vec(),
            "target_schema_bytes": s2.clone()
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    // Transition Shadow (2) -> ReadOnly (3): ALL catalog writes (HTTP & binary) MUST be fenced (409)!
    let (st, _, _) = http_call(
        slot.http_addr,
        Method::POST,
        "/v2/admin/role",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({
            "expected_role": "shadow",
            "expected_role_epoch": 2,
            "new_role": "read_only"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, _, ro_err) = http_call(
        slot.http_addr,
        Method::POST,
        "/v2/catalog/collections/orders/migrations/advance",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({ "target_version": 2, "verified_through": 1 })),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(ro_err["error"], "role_fenced");

    let bin_ro_err = admin
        .commit_migration("orders", 2, 0)
        .await
        .expect_err("binary catalog write must fail in ReadOnly");
    assert!(matches!(
        bin_ro_err,
        MembusError::RoleFenced {
            role: SlotRole::ReadOnly,
            role_epoch: 3,
            ..
        }
    ));

    // Transition ReadOnly (3) -> Draining (4): catalog writes MUST return 503 role_fenced,
    // while a CAS mismatch on `/v2/admin/role` MUST return 409 conflict (NOT 503)!
    let (st, _, _) = http_call(
        slot.http_addr,
        Method::POST,
        "/v2/admin/role",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({
            "expected_role": "read_only",
            "expected_role_epoch": 3,
            "new_role": "draining"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);

    let (st, _, drain_cat_err) = http_call(
        slot.http_addr,
        Method::POST,
        "/v2/catalog/collections/orders/migrations/abort",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({ "target_version": 2 })),
    )
    .await;
    assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(drain_cat_err["error"], "role_fenced");

    let bin_drain_err = admin
        .abort_migration("orders", 2)
        .await
        .expect_err("binary catalog write must fail in Draining");
    assert!(matches!(
        bin_drain_err,
        MembusError::RoleFenced {
            role: SlotRole::Draining,
            role_epoch: 4,
            ..
        }
    ));

    // Audit #9: CAS mismatch on /v2/admin/role while Draining returns 409 conflict (never 503).
    let (st, _, cas_err) = http_call(
        slot.http_addr,
        Method::POST,
        "/v2/admin/role",
        Some("tok-admin"),
        &[],
        Some(serde_json::json!({
            "expected_role": "read_only",
            "expected_role_epoch": 3,
            "new_role": "shadow"
        })),
    )
    .await;
    assert_eq!(st, StatusCode::CONFLICT);
    assert_eq!(cas_err["error"], "conflict");

    // Audit #9: Legacy Strict descriptor persisted with empty `compatible_versions: []`
    // normalizes to `[schema_version]` when read through `CollectionCatalog::get_collection`.
    let legacy_desc_json = serde_json::json!({
        "namespace": "legacy_orders",
        "store_profile": "Kv",
        "schema_version": 3,
        "schema_sha256": sha256(b"v3").to_vec(),
        "schema_bytes": B64.encode(b"v3"),
        "declared_indexes": [],
        "retention_policy": RetentionPolicy::default(),
        "migration_state": "Active"
    });
    slot.storage
        .meta_cas_batch(
            &[],
            &[(
                b"cat:cur:legacy_orders".to_vec(),
                serde_json::to_vec(&legacy_desc_json).unwrap(),
            )],
            &[],
            true,
        )
        .expect("seed legacy descriptor");
    let cat = membus::CollectionCatalog::new(Arc::clone(&slot.storage) as Arc<dyn StorageEngine>);
    let loaded = cat
        .get_collection("legacy_orders")
        .expect("get legacy")
        .expect("present");
    assert_eq!(loaded.compatibility_mode, CompatibilityMode::Strict);
    assert_eq!(loaded.compatible_versions, vec![3]);

    slot.shutdown().await;
}

#[tokio::test]
async fn audit_fail_closed_slot_role_metadata_corruption() {
    for (label, bad_bytes) in [
        ("invalid_json", b"{not-valid-json".to_vec()),
        (
            "epoch_zero",
            serde_json::to_vec(&SlotRoleState {
                role: SlotRole::Active,
                role_epoch: 0,
            })
            .unwrap(),
        ),
        (
            "epoch_overflow",
            serde_json::to_vec(&SlotRoleState {
                role: SlotRole::Active,
                role_epoch: u64::MAX,
            })
            .unwrap(),
        ),
    ] {
        let dir = TempDir::new().expect("tempdir");
        let storage = Arc::new(
            RocksDbStorage::open(
                dir.path(),
                StorageConfig {
                    bg_sync_interval: None,
                    ..StorageConfig::default()
                },
                Arc::new(NoopPublisher),
            )
            .expect("open"),
        );
        storage
            .meta_cas_batch(
                &[],
                &[(membus::META_KEY_SLOT_ROLE.to_vec(), bad_bytes)],
                &[],
                true,
            )
            .expect("write corrupt sys:slot_role");

        let broker = Arc::new(SignalBroker::new(
            Arc::clone(&storage) as Arc<dyn StorageEngine>,
            Arc::new(SignalRing::new(SignalConfig::default())),
            SignalConfig::default(),
        ));
        let server = MembusServer::new(
            Arc::clone(&storage) as Arc<dyn StorageEngine>,
            broker,
            ServerConfig {
                initial_slot_role: SlotRole::Active,
                ..ServerConfig::default()
            },
        );
        let state = server.context().slot_role_state();
        assert_eq!(
            state.role,
            SlotRole::ReadOnly,
            "corrupt sys:slot_role ({label}) MUST fail closed to ReadOnly"
        );
        assert_eq!(state.role_epoch, 0);
        let err = server
            .context()
            .check_client_write_allowed(None)
            .expect_err("corrupt slot must fence client writes");
        assert!(matches!(
            err,
            MembusError::RoleFenced {
                role: SlotRole::ReadOnly,
                ..
            }
        ));
    }
}

#[tokio::test]
async fn audit_multitenant_idempotency_scoping_and_flush_replay() {
    let slot = SlotNode::start("idem-slot", SlotRole::Active, ab_auth_config()).await;

    // 1. Two distinct authenticated principals (`client-writer` via tok-writer and
    //    `ab-router-shadow` via tok-replicator) using the SAME X-Membus-Idempotency-Key
    //    on the same namespace MUST NOT collide or leak each other's seq!
    let (st, hdrs, r1) = http_call(
        slot.http_addr,
        Method::PUT,
        "/v2/kv/orders/shared-k",
        Some("tok-writer"),
        &[("X-Membus-Idempotency-Key", "shared-idem-key")],
        Some(serde_json::json!({ "value_utf8": "principal-1", "ack": "Fast" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert!(hdrs.get("x-membus-idempotent-replay").is_none());
    let seq1 = r1["seq"].as_u64().unwrap();

    let (st, hdrs, r2) = http_call(
        slot.http_addr,
        Method::PUT,
        "/v2/kv/orders/shared-k",
        Some("tok-replicator"),
        &[("X-Membus-Idempotency-Key", "shared-idem-key")],
        Some(serde_json::json!({ "value_utf8": "principal-2", "ack": "Fast" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert!(hdrs.get("x-membus-idempotent-replay").is_none());
    let seq2 = r2["seq"].as_u64().unwrap();
    assert_ne!(seq1, seq2, "distinct principals must not share idempotency records");

    // 2. Audit #9: Replaying an earlier `Ack::Fast` idempotent write with `Ack::Flush`
    //    flushes WAL durably and returns the original committed seq with replay header.
    let (st, hdrs, r1_flush_replay) = http_call(
        slot.http_addr,
        Method::PUT,
        "/v2/kv/orders/shared-k",
        Some("tok-writer"),
        &[("X-Membus-Idempotency-Key", "shared-idem-key")],
        Some(serde_json::json!({ "value_utf8": "principal-1", "ack": "Flush" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(hdrs.get("x-membus-idempotent-replay").unwrap(), "true");
    assert_eq!(r1_flush_replay["seq"].as_u64().unwrap(), seq1);

    // 3. Domain separation: a single-key write to namespace `_tx` and a multi-key `/v2/tx`
    //    using the SAME X-Membus-Idempotency-Key and principal MUST NOT collide!
    let (st, _, kv_tx_res) = http_call(
        slot.http_addr,
        Method::PUT,
        "/v2/kv/_tx/k1",
        Some("tok-writer"),
        &[("X-Membus-Idempotency-Key", "domain-sep-key")],
        Some(serde_json::json!({ "value_utf8": "kv-in-underscore-tx", "ack": "Flush" })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    let kv_seq = kv_tx_res["seq"].as_u64().unwrap();

    let (st, hdrs, tx_res) = http_call(
        slot.http_addr,
        Method::POST,
        "/v2/tx",
        Some("tok-writer"),
        &[("X-Membus-Idempotency-Key", "domain-sep-key")],
        Some(serde_json::json!({
            "ack": "Flush",
            "guards": [],
            "mutations": [
                {
                    "mutation": {
                        "namespace": "_tx",
                        "key": b"k2".to_vec(),
                        "op": { "Put": { "value": b"from-tx".to_vec() } }
                    },
                    "indexes": null,
                    "search_op": null
                }
            ]
        })),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert!(hdrs.get("x-membus-idempotent-replay").is_none());
    let tx_seq = tx_res["seq"].as_u64().unwrap();
    assert_ne!(tx_seq, kv_seq, "tx domain must not collide with namespace _tx");

    slot.shutdown().await;
}

#[tokio::test]
async fn audit_echo_loop_duplicate_headers_and_parity_bounds() {
    let active_slot = SlotNode::start("slot-a", SlotRole::Active, ab_auth_config()).await;
    let slot = SlotNode::start("slot-b", SlotRole::Shadow, ab_auth_config()).await;

    // 1. Duplicate X-Membus-Role-Epoch, X-Membus-Idempotency-Key, and X-Membus-Route-Origin
    //    headers MUST be rejected with 400 Bad Request!
    let (st, _, _) = http_call(
        active_slot.http_addr,
        Method::PUT,
        "/v2/kv/orders/dup-epoch",
        Some("tok-writer"),
        &[("X-Membus-Role-Epoch", "1"), ("X-Membus-Role-Epoch", "2")],
        Some(serde_json::json!({ "value_utf8": "x" })),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    let (st, _, _) = http_call(
        active_slot.http_addr,
        Method::PUT,
        "/v2/kv/orders/dup-idem",
        Some("tok-writer"),
        &[
            ("X-Membus-Idempotency-Key", "k1"),
            ("X-Membus-Idempotency-Key", "k2"),
        ],
        Some(serde_json::json!({ "value_utf8": "x" })),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);
    active_slot.shutdown().await;

    let sample_delta = serde_json::json!({
        "delta": {
            "namespace": "orders",
            "key": b"k1".to_vec(),
            "op": { "Put": { "value": b"v1".to_vec() } },
            "stamp": {
                "timestamp_ms": 1_700_000_000_001u64,
                "origin_node_id": "slot-a",
                "origin_seq": 1,
                "value_sha256": sha256(b"v1")
            },
            "indexes": null,
            "search_op": null
        },
        "ack": "Flush"
    });

    let (st, _, _) = http_call(
        slot.http_addr,
        Method::POST,
        "/v2/replication/apply",
        Some("tok-replicator"),
        &[
            ("X-Membus-Route-Origin", "slot-a"),
            ("X-Membus-Route-Origin", "slot-c"),
        ],
        Some(sample_delta.clone()),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    // 2. Conflicting X-Membus-Route-Origin header vs JSON body `route_origin` -> 400 Bad Request!
    let mut conflicting_body = sample_delta.clone();
    conflicting_body["route_origin"] = serde_json::json!("slot-c");
    let (st, _, _) = http_call(
        slot.http_addr,
        Method::POST,
        "/v2/replication/apply",
        Some("tok-replicator"),
        &[("X-Membus-Route-Origin", "slot-a")],
        Some(conflicting_body),
    )
    .await;
    assert_eq!(st, StatusCode::BAD_REQUEST);

    // 3. Trimmed & case-insensitive `RoUtEr:slot-b` route_origin triggers echo-loop rejection!
    let (st, hdrs, echo_res) = http_call(
        slot.http_addr,
        Method::POST,
        "/v2/replication/apply",
        Some("tok-replicator"),
        &[("X-Membus-Route-Origin", "  RoUtEr: slot-b  ")],
        Some(sample_delta),
    )
    .await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(echo_res["applied"], false);
    assert_eq!(echo_res["echo_rejected"], true);
    assert_eq!(hdrs.get("x-membus-echo-loop").unwrap(), "rejected");

    // 4. Invalid origin_node_id (whitespace / control chars / empty) rejected with 400!
    for bad_node in ["", " slot-a", "slot-a\n", "slot\ta"] {
        let bad_delta = serde_json::json!({
            "delta": {
                "namespace": "orders",
                "key": b"k-bad".to_vec(),
                "op": { "Put": { "value": b"v".to_vec() } },
                "stamp": {
                    "timestamp_ms": 1_700_000_000_001u64,
                    "origin_node_id": bad_node,
                    "origin_seq": 1,
                    "value_sha256": sha256(b"v")
                },
                "indexes": null,
                "search_op": null
            },
            "ack": "Flush"
        });
        let (st, _, _) = http_call(
            slot.http_addr,
            Method::POST,
            "/v2/replication/apply",
            Some("tok-replicator"),
            &[],
            Some(bad_delta),
        )
        .await;
        assert_eq!(
            st,
            StatusCode::BAD_REQUEST,
            "origin_node_id {bad_node:?} must be rejected with 400"
        );
    }

    // 5. Audit #6: Parity digest enforces MAX_KEY_BYTES on key_prefix and overall execution deadline!
    let cat = membus::CollectionCatalog::new(Arc::clone(&slot.storage) as Arc<dyn StorageEngine>);
    let oversized_prefix = vec![b'a'; membus::types::MAX_KEY_BYTES + 1];
    let prefix_err = membus::compute_namespace_parity_digest(
        slot.storage.as_ref(),
        &cat,
        "slot-b",
        "orders",
        &oversized_prefix,
        ParityDigestMode::Full,
    )
    .expect_err("oversized key_prefix must be rejected");
    assert!(matches!(prefix_err, MembusError::InvalidArgument(_)));

    let expired_deadline = std::time::Instant::now()
        .checked_sub(std::time::Duration::from_millis(1))
        .unwrap_or_else(std::time::Instant::now);
    let timeout_err = membus::compute_namespace_xor_digest_with_mode(
        slot.storage.as_ref(),
        "slot-b",
        "orders",
        b"",
        ParityDigestMode::Full,
        Some(expired_deadline),
    )
    .expect_err("expired parity deadline must return Busy");
    assert!(matches!(timeout_err, MembusError::Busy { .. }));

    slot.shutdown().await;
}
