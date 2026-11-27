use std::net::SocketAddr;
use std::path::Path;
use std::process::Command as ProcessCommand;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use bytes::Bytes;
use http_body_util::{BodyExt, Empty, Full};
use hyper::header::CONTENT_TYPE;
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::TokioIo;
use membus::cli::{parse_args, shutdown_resident, Command};
use membus::index::{BfsOptions, NeighborQueryOptions, OrderedQueryOptions};
use membus::mesh::{validate_mesh_listen_endpoints, MeshConfig, MeshNode};
use membus::transport::tls::build_tls_connector;
use membus::{
    validate_listener_bind_security, Ack, AclGrant, AuthConfig, BfsResult, Client, CommitResult,
    DeltaLogPage, Endpoint, ExpectedVersion, IndexMutations, IndexedMutation, LogScanPage,
    LogWatermark, MembusError, MembusServer, MetaCasGuard, NeighborPage, NoopPublisher,
    OrderedIndexPage, OwnedSnapshot, Permission, Principal, ReplicatedApplyResult, ReplicatedDelta,
    ReplicationStamp, RocksDbStorage, ScanOptions, ScanPage, SearchConfig, SearchDocument,
    SearchEngine, SearchLogOp, SearchLogPage, SearchMode, SearchQuery, SearchVisibility,
    ServerConfig, SignalBroker, SignalConfig, SignalPublisher, SignalRing, StorageConfig,
    StorageEngine, StoredRecord, TlsClientConfig, TlsServerConfig, TokenBinding, VersionGuard,
    WalBatch, WriteMutation,
};
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use tempfile::TempDir;
use tokio::net::TcpStream;

fn open_test_stack(
    dir: &TempDir,
    storage_cfg: StorageConfig,
    server_cfg: ServerConfig,
) -> (Arc<RocksDbStorage>, Arc<SearchEngine>, MembusServer) {
    let signal_cfg = SignalConfig::default();
    let ring = Arc::new(SignalRing::new(signal_cfg));
    let storage = Arc::new(
        RocksDbStorage::open(
            dir.path(),
            storage_cfg,
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
    let server = MembusServer::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        broker,
        server_cfg,
    )
    .with_search(Arc::clone(&search));
    (storage, search, server)
}

async fn http1_call_with_headers(
    addr: SocketAddr,
    method: Method,
    path: &str,
    body: Option<serde_json::Value>,
) -> (StatusCode, hyper::HeaderMap, Bytes) {
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
    let headers = resp.headers().clone();
    let bytes = resp.into_body().collect().await.expect("collect").to_bytes();
    (status, headers, bytes)
}

// ============================================================================
// 1. Post-Commit Secondary Search Ingestion Decoupling (HTTP + Binary RPC)
// ============================================================================

#[tokio::test]
async fn http_post_commit_search_ingest_failure_returns_ok_deferred_and_recovers_from_log() {
    let dir = TempDir::new().expect("tempdir");
    let (_storage, search, server) =
        open_test_stack(&dir, StorageConfig::default(), ServerConfig::default());

    let handle = server
        .bind_http("127.0.0.1:0".parse().unwrap())
        .await
        .expect("bind http");
    let Endpoint::Http(addr) = *handle.endpoint() else {
        panic!("expected Http endpoint");
    };

    // Inject deterministic post-commit secondary search ingestion failure.
    search.inject_ingest_failure(1);

    let (st, headers, body) = http1_call_with_headers(
        addr,
        Method::PUT,
        "/v2/kv/orders/o1",
        Some(serde_json::json!({
            "value_utf8": "authoritative-order-payload",
            "ack": "Flush",
            "search_op": {
                "Index": {
                    "namespace": "orders",
                    "doc_id": "o1",
                    "kind": "order",
                    "title": "Deferred Recovery Order",
                    "tags": ["hardening"],
                    "body": "UniqueToken_Deferred_8821",
                    "visibility": "Public"
                }
            }
        })),
    )
    .await;

    // (a) Client receives authoritative commit success (200 OK) with explicit `deferred` marker.
    assert_eq!(st, StatusCode::OK);
    assert_eq!(
        headers
            .get("x-membus-search-sync")
            .and_then(|v| v.to_str().ok()),
        Some("deferred")
    );
    let commit_json: serde_json::Value = serde_json::from_slice(&body).expect("json");
    let committed_seq = commit_json["seq"].as_u64().expect("seq");
    assert!(committed_seq >= 1);
    assert_eq!(commit_json["search_sync"].as_str(), Some("deferred"));

    // (b) Storage holds the committed record authoritatively.
    let (st, _, rec_body) =
        http1_call_with_headers(addr, Method::GET, "/v2/kv/orders/o1", None).await;
    assert_eq!(st, StatusCode::OK);
    let rec: StoredRecord = serde_json::from_slice(&rec_body).expect("record");
    assert_eq!(rec.value, b"authoritative-order-payload");
    assert_eq!(rec.meta.version, committed_seq);

    // Health exposes deferred counter and dirty flag.
    let (st, _, health_body) =
        http1_call_with_headers(addr, Method::GET, "/v2/health", None).await;
    assert_eq!(st, StatusCode::OK);
    let health: serde_json::Value = serde_json::from_slice(&health_body).expect("health");
    assert_eq!(health["search_ingest_deferred_total"].as_u64(), Some(1));
    assert_eq!(health["search_dirty"].as_bool(), Some(true));

    // Before CF_LOG catch-up, search does not yet have the deferred doc in overlay.
    let before = search
        .search(&SearchQuery {
            query: "UniqueToken_Deferred_8821".to_owned(),
            mode: SearchMode::LiteralSubstring,
            namespace: Some("orders".to_owned()),
            kind: None,
            limit: 10,
        })
        .expect("search before catchup");
    assert!(before.hits.is_empty());

    // (c) Catch-up replay from CF_LOG restores searchability and clears dirty flag.
    let flushed_seq = search.catch_up_from_log().expect("catch_up_from_log");
    assert!(flushed_seq >= committed_seq);

    let after = search
        .search(&SearchQuery {
            query: "UniqueToken_Deferred_8821".to_owned(),
            mode: SearchMode::LiteralSubstring,
            namespace: Some("orders".to_owned()),
            kind: None,
            limit: 10,
        })
        .expect("search after catchup");
    assert_eq!(after.hits.len(), 1);
    assert_eq!(after.hits[0].doc_id, "o1");

    let (st, _, health_body) =
        http1_call_with_headers(addr, Method::GET, "/v2/health", None).await;
    assert_eq!(st, StatusCode::OK);
    let health: serde_json::Value = serde_json::from_slice(&health_body).expect("health");
    assert_eq!(health["search_ingest_deferred_total"].as_u64(), Some(1));
    assert_eq!(health["search_dirty"].as_bool(), Some(false));

    handle.shutdown().await;
}

#[tokio::test]
async fn binary_rpc_post_commit_search_ingest_failure_returns_commit_and_recovers_from_log() {
    let dir = TempDir::new().expect("tempdir");
    let (_storage, search, server) =
        open_test_stack(&dir, StorageConfig::default(), ServerConfig::default());

    let handle = server
        .bind_tcp("127.0.0.1:0".parse().unwrap())
        .await
        .expect("bind tcp");
    let Endpoint::Tcp(addr) = *handle.endpoint() else {
        panic!("expected Tcp endpoint");
    };
    let client = Client::connect_tcp(addr).await.expect("connect");

    search.inject_ingest_failure(1);
    let doc = SearchDocument {
        namespace: "bin_ns".to_owned(),
        doc_id: "b1".to_owned(),
        kind: "doc".to_owned(),
        title: "Binary Deferred Title".to_owned(),
        tags: vec!["rpc".to_owned()],
        body: "BinaryToken_4490".to_owned(),
        visibility: SearchVisibility::Public,
    };

    // (a) Binary RPC returns authoritative CommitResult (NOT an error!).
    let commit = client
        .put_indexed(
            "bin_ns",
            b"b1",
            b"binary-committed-val",
            Ack::Flush,
            ExpectedVersion::Absent,
            None,
            Some(SearchLogOp::Index(doc)),
        )
        .await
        .expect("put_indexed must succeed despite secondary search failure");
    assert!(commit.seq >= 1);

    // (b) Storage holds committed record; health reflects deferred count + dirty flag.
    let rec = client
        .get("bin_ns", b"b1")
        .await
        .expect("get")
        .expect("present");
    assert_eq!(rec.value, b"binary-committed-val");
    assert_eq!(rec.meta.version, commit.seq);

    let health_legacy = client.health().await.expect("legacy health");
    assert_eq!(health_legacy.epoch, commit.epoch);
    let health = client.health_v2().await.expect("health_v2");
    assert_eq!(health.search_ingest_deferred_total, 1);
    assert!(health.search_dirty);

    // (c) CF_LOG catch-up restores searchability and clears dirty flag.
    search.catch_up_from_log().expect("catch up");
    let page = client
        .search(SearchQuery {
            query: "BinaryToken_4490".to_owned(),
            mode: SearchMode::LiteralSubstring,
            namespace: Some("bin_ns".to_owned()),
            kind: None,
            limit: 10,
        })
        .await
        .expect("search");
    assert_eq!(page.hits.len(), 1);
    assert_eq!(page.hits[0].doc_id, "b1");

    let health_after = client.health_v2().await.expect("health_v2 after");
    assert!(!health_after.search_dirty);
    assert_eq!(health_after.search_ingest_deferred_total, 1);

    handle.shutdown().await;
}

#[test]
fn search_dirty_never_clears_prematurely_before_dirty_through_seq_is_committed() {
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
        .expect("open storage"),
    );
    let search = SearchEngine::open(
        dir.path(),
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        SearchConfig {
            commit_interval_ms: 20,
            ..SearchConfig::default()
        },
    )
    .expect("open search");

    let c1 = storage
        .put("dirty", b"k1", b"v1", Ack::Flush, ExpectedVersion::Any)
        .expect("put 1");
    search.flush_sync().expect("initial flush");
    assert!(!search.is_dirty());

    // Simulate a higher sequence being deferred while storage has only settled up to c1.seq.
    let future_seq = c1.seq + 3;
    search.record_deferred_ingest(
        future_seq,
        &MembusError::Storage("simulated concurrent deferred seq".to_owned()),
    );
    assert!(search.is_dirty());

    // Wait past commit_interval so the background worker wakes while settled_seq <= committed_seq.
    thread::sleep(Duration::from_millis(80));
    assert!(
        search.is_dirty(),
        "background commit worker must not prematurely clear search_dirty when dirty_through_seq > committed_seq"
    );

    // Advance storage through `future_seq` and flush; only now should search_dirty clear.
    for i in 0..3u32 {
        storage
            .put(
                "dirty",
                format!("k_extra_{i}").as_bytes(),
                b"v",
                Ack::Flush,
                ExpectedVersion::Any,
            )
            .expect("advance seq");
    }
    let flushed = search.catch_up_from_log().expect("catch up through future_seq");
    assert!(flushed >= future_seq);
    assert!(
        !search.is_dirty(),
        "search_dirty should clear once committed_seq covers dirty_through_seq"
    );
}

#[test]
fn binary_resp_health_preserves_legacy_7_u64_wire_format_and_health_v2_roundtrips_gauges() {
    use membus::protocol::{decode_response, encode_response, Response};
    use membus::HealthStatus;

    let full = HealthStatus {
        epoch: 42,
        latest_seq: 100,
        log_horizon_seq: 10,
        in_flight_ops: 3,
        in_flight_bytes: 4096,
        active_connections: 7,
        active_subscriptions: 5,
        active_snapshots: 9,
        active_streams: 4,
        search_ingest_deferred_total: 2,
        search_dirty: true,
        slot_role: membus::SlotRole::Shadow,
        role_epoch: 7,
    };

    // Legacy RESP_HEALTH MUST stay strictly 1 tag byte + 7 * 8 bytes = 57 bytes.
    let legacy_bytes = encode_response(&Response::Health(full.clone()));
    assert_eq!(
        legacy_bytes.len(),
        1 + 7 * 8,
        "legacy RESP_HEALTH must remain 57 bytes for 195d00d5 wire compatibility"
    );
    let decoded_legacy = decode_response(&legacy_bytes).expect("decode legacy health");
    assert_eq!(
        decoded_legacy,
        Response::Health(HealthStatus {
            epoch: 42,
            latest_seq: 100,
            log_horizon_seq: 10,
            in_flight_ops: 3,
            in_flight_bytes: 4096,
            active_connections: 7,
            active_subscriptions: 5,
            active_snapshots: 0,
            active_streams: 0,
            search_ingest_deferred_total: 0,
            search_dirty: false,
            slot_role: membus::SlotRole::Active,
            role_epoch: 0,
        })
    );

    // RESP_HEALTH_V2 preserves all extended gauges.
    let v2_bytes = encode_response(&Response::HealthV2(full.clone()));
    let decoded_v2 = decode_response(&v2_bytes).expect("decode health_v2");
    assert_eq!(decoded_v2, Response::HealthV2(full));
}

// ============================================================================
// 2. Resident Lifecycle: Clean Shutdown & Observable Error Propagation
// ============================================================================

struct SpyStorage {
    inner: Arc<RocksDbStorage>,
    wal_flush_called: Arc<AtomicBool>,
    fail_wal_flush: bool,
    get_barrier: Option<Arc<Barrier>>,
}

impl StorageEngine for SpyStorage {
    fn data_dir(&self) -> Option<&Path> {
        self.inner.data_dir()
    }
    fn epoch(&self) -> u64 {
        self.inner.epoch()
    }
    fn latest_seq(&self) -> u64 {
        self.inner.latest_seq()
    }
    fn log_horizon_seq(&self) -> u64 {
        self.inner.log_horizon_seq()
    }
    fn rocksdb_latest_sequence(&self) -> u64 {
        self.inner.rocksdb_latest_sequence()
    }
    fn in_flight_ops(&self) -> usize {
        self.inner.in_flight_ops()
    }
    fn in_flight_bytes(&self) -> usize {
        self.inner.in_flight_bytes()
    }
    fn active_snapshots(&self) -> usize {
        self.inner.active_snapshots()
    }
    fn hlc_now_ms(&self) -> u64 {
        self.inner.hlc_now_ms()
    }
    fn get(&self, namespace: &str, key: &[u8]) -> Result<Option<StoredRecord>, MembusError> {
        if let Some(barrier) = &self.get_barrier {
            if key == b"hold" {
                barrier.wait();
                barrier.wait();
            }
        }
        self.inner.get(namespace, key)
    }
    fn get_many(
        &self,
        namespace: &str,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<StoredRecord>>, MembusError> {
        self.inner.get_many(namespace, keys)
    }
    fn put(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
    ) -> Result<CommitResult, MembusError> {
        self.inner.put(namespace, key, value, ack, expected)
    }
    fn put_with_flags(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        flags: u8,
        preserve_indexes: bool,
        ack: Ack,
        expected: ExpectedVersion,
    ) -> Result<CommitResult, MembusError> {
        self.inner.put_with_flags(
            namespace,
            key,
            value,
            flags,
            preserve_indexes,
            ack,
            expected,
        )
    }
    fn tier_swap_record(
        &self,
        namespace: &str,
        key: &[u8],
        new_value: &[u8],
        set_cold_flag: bool,
        logical_value_sha256: [u8; 32],
        ack: Ack,
        expected: ExpectedVersion,
    ) -> Result<CommitResult, MembusError> {
        self.inner.tier_swap_record(
            namespace,
            key,
            new_value,
            set_cold_flag,
            logical_value_sha256,
            ack,
            expected,
        )
    }
    fn apply_replicated_delta(
        &self,
        local_node_id: &str,
        delta: &ReplicatedDelta,
        ack: Ack,
    ) -> Result<ReplicatedApplyResult, MembusError> {
        self.inner.apply_replicated_delta(local_node_id, delta, ack)
    }
    fn append_replicated_search_ops(
        &self,
        origin_stamp: &ReplicationStamp,
        ops: &[SearchLogOp],
        ack: Ack,
    ) -> Result<ReplicatedApplyResult, MembusError> {
        self.inner
            .append_replicated_search_ops(origin_stamp, ops, ack)
    }
    fn put_indexed(
        &self,
        namespace: &str,
        key: &[u8],
        value: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
        indexes: Option<&IndexMutations>,
        search_op: Option<&SearchLogOp>,
    ) -> Result<CommitResult, MembusError> {
        self.inner
            .put_indexed(namespace, key, value, ack, expected, indexes, search_op)
    }
    fn remove(
        &self,
        namespace: &str,
        key: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
    ) -> Result<CommitResult, MembusError> {
        self.inner.remove(namespace, key, ack, expected)
    }
    fn remove_indexed(
        &self,
        namespace: &str,
        key: &[u8],
        ack: Ack,
        expected: ExpectedVersion,
        search_op: Option<&SearchLogOp>,
    ) -> Result<CommitResult, MembusError> {
        self.inner
            .remove_indexed(namespace, key, ack, expected, search_op)
    }
    fn commit_tx(
        &self,
        guards: &[VersionGuard],
        mutations: &[WriteMutation],
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        self.inner.commit_tx(guards, mutations, ack)
    }
    fn commit_indexed_tx(
        &self,
        guards: &[VersionGuard],
        mutations: &[IndexedMutation],
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        self.inner.commit_indexed_tx(guards, mutations, ack)
    }
    fn open_snapshot(&self) -> Result<OwnedSnapshot, MembusError> {
        self.inner.open_snapshot()
    }
    fn get_in_snapshot(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        key: &[u8],
    ) -> Result<Option<StoredRecord>, MembusError> {
        self.inner.get_in_snapshot(snapshot, namespace, key)
    }
    fn get_many_in_snapshot(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        keys: &[Vec<u8>],
    ) -> Result<Vec<Option<StoredRecord>>, MembusError> {
        self.inner.get_many_in_snapshot(snapshot, namespace, keys)
    }
    fn scan_prefix(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        prefix: &[u8],
        options: &ScanOptions,
    ) -> Result<ScanPage, MembusError> {
        self.inner.scan_prefix(snapshot, namespace, prefix, options)
    }
    fn scan_range(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        start_key: &[u8],
        end_key: &[u8],
        options: &ScanOptions,
    ) -> Result<ScanPage, MembusError> {
        self.inner
            .scan_range(snapshot, namespace, start_key, end_key, options)
    }
    fn query_ordered_index(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        index_id: &str,
        options: &OrderedQueryOptions,
    ) -> Result<OrderedIndexPage, MembusError> {
        self.inner
            .query_ordered_index(snapshot, namespace, index_id, options)
    }
    fn query_neighbors(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        index_id: &str,
        node_id: &[u8],
        options: &NeighborQueryOptions,
    ) -> Result<NeighborPage, MembusError> {
        self.inner
            .query_neighbors(snapshot, namespace, index_id, node_id, options)
    }
    fn bfs_traverse(
        &self,
        snapshot: &OwnedSnapshot,
        namespace: &str,
        index_id: &str,
        seed_node: &[u8],
        options: &BfsOptions,
    ) -> Result<BfsResult, MembusError> {
        self.inner
            .bfs_traverse(snapshot, namespace, index_id, seed_node, options)
    }
    fn put_blob(&self, digest: &[u8], data: &[u8], ack: Ack) -> Result<(), MembusError> {
        self.inner.put_blob(digest, data, ack)
    }
    fn get_blob(&self, digest: &[u8]) -> Result<Option<Vec<u8>>, MembusError> {
        self.inner.get_blob(digest)
    }
    fn put_cas_blob(
        &self,
        namespace: &str,
        data: &[u8],
        ack: Ack,
    ) -> Result<[u8; 32], MembusError> {
        self.inner.put_cas_blob(namespace, data, ack)
    }
    fn get_cas_blob(
        &self,
        namespace: &str,
        sha256: &[u8; 32],
    ) -> Result<Option<Vec<u8>>, MembusError> {
        self.inner.get_cas_blob(namespace, sha256)
    }
    fn has_cas_blob(&self, namespace: &str, sha256: &[u8; 32]) -> Result<bool, MembusError> {
        self.inner.has_cas_blob(namespace, sha256)
    }
    fn scan_log_after(&self, after_seq: u64, limit: usize) -> Result<LogScanPage, MembusError> {
        self.inner.scan_log_after(after_seq, limit)
    }
    fn append_search_log_op(
        &self,
        op: SearchLogOp,
        ack: Ack,
    ) -> Result<CommitResult, MembusError> {
        self.inner.append_search_log_op(op, ack)
    }
    fn scan_search_log_after(
        &self,
        after_seq: u64,
        limit: usize,
    ) -> Result<SearchLogPage, MembusError> {
        self.inner.scan_search_log_after(after_seq, limit)
    }
    fn scan_deltas_after(&self, after_seq: u64, limit: usize) -> Result<DeltaLogPage, MembusError> {
        self.inner.scan_deltas_after(after_seq, limit)
    }
    fn trim_log_before(&self, keep_from_seq: u64) -> Result<(), MembusError> {
        self.inner.trim_log_before(keep_from_seq)
    }
    fn list_log_watermarks(&self) -> Result<Vec<LogWatermark>, MembusError> {
        self.inner.list_log_watermarks()
    }
    fn safe_trim_log_before(&self, keep_from_seq: u64, force: bool) -> Result<(), MembusError> {
        self.inner.safe_trim_log_before(keep_from_seq, force)
    }
    fn meta_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MembusError> {
        self.inner.meta_get(key)
    }
    fn meta_put(&self, key: &[u8], value: &[u8], sync: bool) -> Result<(), MembusError> {
        self.inner.meta_put(key, value, sync)
    }
    fn meta_scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, MembusError> {
        self.inner.meta_scan_prefix(prefix)
    }
    fn meta_cas_batch(
        &self,
        guards: &[MetaCasGuard],
        puts: &[(Vec<u8>, Vec<u8>)],
        deletes: &[Vec<u8>],
        sync: bool,
    ) -> Result<(), MembusError> {
        self.inner.meta_cas_batch(guards, puts, deletes, sync)
    }
    fn lease_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, MembusError> {
        self.inner.lease_get(key)
    }
    fn lease_cas(
        &self,
        key: &[u8],
        expected: Option<&[u8]>,
        new_value: &[u8],
    ) -> Result<(), MembusError> {
        self.inner.lease_cas(key, expected, new_value)
    }
    fn create_checkpoint(&self, dest_dir: &Path) -> Result<u64, MembusError> {
        self.inner.create_checkpoint(dest_dir)
    }
    fn read_wal_since(&self, rocksdb_seq: u64, limit: usize) -> Result<Vec<WalBatch>, MembusError> {
        self.inner.read_wal_since(rocksdb_seq, limit)
    }
    fn flush_wal(&self, sync: bool) -> Result<(), MembusError> {
        self.wal_flush_called.store(true, Ordering::Release);
        if self.fail_wal_flush {
            return Err(MembusError::Io("injected WAL flush error".to_owned()));
        }
        self.inner.flush_wal(sync)
    }
}

#[tokio::test]
async fn resident_shutdown_propagates_flush_errors_and_completes_clean_shutdown() {
    // 1. Clean shutdown flushes search and WAL and returns Ok(()).
    {
        let dir = TempDir::new().expect("tempdir");
        let (storage, search, server) =
            open_test_stack(&dir, StorageConfig::default(), ServerConfig::default());
        let http_h = server
            .bind_http("127.0.0.1:0".parse().unwrap())
            .await
            .expect("bind http");
        let tcp_h = server
            .bind_tcp("127.0.0.1:0".parse().unwrap())
            .await
            .expect("bind tcp");

        let commit = search
            .index_document(
                SearchDocument {
                    namespace: "sd".to_owned(),
                    doc_id: "d1".to_owned(),
                    kind: "k".to_owned(),
                    title: "Clean Shutdown".to_owned(),
                    tags: vec![],
                    body: "Body".to_owned(),
                    visibility: SearchVisibility::Public,
                },
                Ack::Fast,
            )
            .expect("index");

        shutdown_resident(
            None,
            vec![http_h, tcp_h],
            Some(Arc::clone(&search)),
            storage as Arc<dyn StorageEngine>,
            Duration::from_secs(5),
        )
        .await
        .expect("clean shutdown should succeed");
        assert!(search.committed_seq() >= commit.seq);
    }

    // 2. Search flush failure returns Err(...) while STILL executing WAL flush!
    {
        let dir = TempDir::new().expect("tempdir");
        let raw_storage = Arc::new(
            RocksDbStorage::open(dir.path(), StorageConfig::default(), Arc::new(NoopPublisher))
                .expect("open storage"),
        );
        let wal_called = Arc::new(AtomicBool::new(false));
        let spy = Arc::new(SpyStorage {
            inner: Arc::clone(&raw_storage),
            wal_flush_called: Arc::clone(&wal_called),
            fail_wal_flush: false,
            get_barrier: None,
        });
        let search = Arc::new(
            SearchEngine::open(
                dir.path(),
                Arc::clone(&raw_storage) as Arc<dyn StorageEngine>,
                SearchConfig::default(),
            )
            .expect("open search"),
        );
        search.inject_flush_failure(1);

        let err = shutdown_resident(
            None,
            Vec::new(),
            Some(search),
            spy as Arc<dyn StorageEngine>,
            Duration::from_secs(5),
        )
        .await
        .expect_err("search flush failure must fail shutdown");
        assert!(
            err.to_string().contains("search flush failed"),
            "unexpected error: {err}"
        );
        assert!(
            wal_called.load(Ordering::Acquire),
            "WAL flush must still run even after search flush fails"
        );
    }

    // 3. WAL flush failure returns Err(...) while search flush still completes!
    {
        let dir = TempDir::new().expect("tempdir");
        let raw_storage = Arc::new(
            RocksDbStorage::open(dir.path(), StorageConfig::default(), Arc::new(NoopPublisher))
                .expect("open storage"),
        );
        let wal_called = Arc::new(AtomicBool::new(false));
        let spy = Arc::new(SpyStorage {
            inner: Arc::clone(&raw_storage),
            wal_flush_called: Arc::clone(&wal_called),
            fail_wal_flush: true,
            get_barrier: None,
        });
        let search = Arc::new(
            SearchEngine::open(
                dir.path(),
                Arc::clone(&raw_storage) as Arc<dyn StorageEngine>,
                SearchConfig::default(),
            )
            .expect("open search"),
        );

        let err = shutdown_resident(
            None,
            Vec::new(),
            Some(search),
            spy as Arc<dyn StorageEngine>,
            Duration::from_secs(5),
        )
        .await
        .expect_err("WAL flush failure must fail shutdown");
        assert!(
            err.to_string().contains("WAL flush failed"),
            "unexpected error: {err}"
        );
        assert!(wal_called.load(Ordering::Acquire));
    }
}

// ============================================================================
// 3. Security: HTTP TLS/mTLS + Fail-Closed Non-Loopback Guard
// ============================================================================

struct TestPki {
    ca_pem: std::path::PathBuf,
    server_cert_pem: std::path::PathBuf,
    server_key_pem: std::path::PathBuf,
    client_cert_pem: std::path::PathBuf,
    client_key_pem: std::path::PathBuf,
}

fn generate_test_pki(dir: &Path) -> TestPki {
    let mut ca_params = CertificateParams::new(vec!["membus-test-ca".to_owned()]).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();
    let issuer = Issuer::from_params(&ca_params, &ca_key);

    let mut srv_params = CertificateParams::new(vec!["localhost".to_owned()]).unwrap();
    srv_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let srv_key = KeyPair::generate().unwrap();
    let srv_cert = srv_params.signed_by(&srv_key, &issuer).unwrap();

    let mut cli_params = CertificateParams::new(vec!["membus-client".to_owned()]).unwrap();
    cli_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let cli_key = KeyPair::generate().unwrap();
    let cli_cert = cli_params.signed_by(&cli_key, &issuer).unwrap();

    let ca_pem = dir.join("ca.pem");
    let server_cert_pem = dir.join("server.pem");
    let server_key_pem = dir.join("server.key");
    let client_cert_pem = dir.join("client.pem");
    let client_key_pem = dir.join("client.key");

    std::fs::write(&ca_pem, ca_cert.pem()).unwrap();
    std::fs::write(&server_cert_pem, srv_cert.pem()).unwrap();
    std::fs::write(&server_key_pem, srv_key.serialize_pem()).unwrap();
    std::fs::write(&client_cert_pem, cli_cert.pem()).unwrap();
    std::fs::write(&client_key_pem, cli_key.serialize_pem()).unwrap();

    TestPki {
        ca_pem,
        server_cert_pem,
        server_key_pem,
        client_cert_pem,
        client_key_pem,
    }
}

#[tokio::test]
async fn http_tls_and_mtls_enforce_client_cert_and_serve_health() {
    let dir = TempDir::new().expect("tempdir");
    let pki = generate_test_pki(dir.path());
    let (_storage, _search, server) =
        open_test_stack(&dir, StorageConfig::default(), ServerConfig::default());

    let tls_cfg = TlsServerConfig {
        cert_pem_path: pki.server_cert_pem.clone(),
        key_pem_path: pki.server_key_pem.clone(),
        client_ca_pem_path: Some(pki.ca_pem.clone()),
    };
    let handle = server
        .bind_http_tls("127.0.0.1:0".parse().unwrap(), &tls_cfg)
        .await
        .expect("bind_http_tls");
    let Endpoint::HttpTls(addr) = *handle.endpoint() else {
        panic!("expected HttpTls endpoint");
    };

    // 1. Valid mTLS client succeeds on HTTPS /v2/health.
    let (connector, server_name) = build_tls_connector(&TlsClientConfig {
        ca_pem_path: pki.ca_pem.clone(),
        server_name: "localhost".to_owned(),
        client_cert_pem_path: Some(pki.client_cert_pem.clone()),
        client_key_pem_path: Some(pki.client_key_pem.clone()),
    })
    .expect("build connector");

    let tcp = TcpStream::connect(addr).await.expect("connect");
    let tls_stream = connector
        .connect(server_name, tcp)
        .await
        .expect("mTLS handshake");
    let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tls_stream))
        .await
        .expect("http1 handshake over TLS");
    tokio::spawn(async move {
        let _ = conn.await;
    });
    let req = Request::builder()
        .method(Method::GET)
        .uri("/v2/health")
        .header("host", "localhost")
        .body(Empty::<Bytes>::new())
        .unwrap();
    let resp = sender.send_request(req).await.expect("send https request");
    assert_eq!(resp.status(), StatusCode::OK);

    // 2. Client WITHOUT client certificate fails mTLS authentication.
    let (no_cert_connector, server_name_2) = build_tls_connector(&TlsClientConfig {
        ca_pem_path: pki.ca_pem.clone(),
        server_name: "localhost".to_owned(),
        client_cert_pem_path: None,
        client_key_pem_path: None,
    })
    .expect("build no-cert connector");
    let tcp2 = TcpStream::connect(addr).await.expect("connect 2");
    let hs = no_cert_connector.connect(server_name_2, tcp2).await;
    // In TLS 1.3 the server sends CertificateRequired alert either during handshake or first read.
    if let Ok(tls_stream2) = hs {
        let res = hyper::client::conn::http1::handshake(TokioIo::new(tls_stream2)).await;
        if let Ok((mut sender2, conn2)) = res {
            tokio::spawn(async move {
                let _ = conn2.await;
            });
            let req2 = Request::builder()
                .method(Method::GET)
                .uri("/v2/health")
                .header("host", "localhost")
                .body(Empty::<Bytes>::new())
                .unwrap();
            assert!(
                sender2.send_request(req2).await.is_err(),
                "mTLS server must reject client without certificate"
            );
        }
    }

    handle.shutdown().await;
}

#[tokio::test]
async fn non_loopback_bind_fails_closed_unless_auth_and_tls_or_break_glass() {
    let loopback_v4: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let loopback_v6: SocketAddr = "[::1]:0".parse().unwrap();
    let any_v4: SocketAddr = "0.0.0.0:0".parse().unwrap();
    let lan_v4: SocketAddr = "192.168.1.50:8080".parse().unwrap();

    // Loopback is always zero-friction.
    assert!(validate_listener_bind_security("HTTP", loopback_v4, false, false, false).is_ok());
    assert!(validate_listener_bind_security("HTTP", loopback_v6, false, false, false).is_ok());

    // Non-loopback without BOTH auth & TLS fails closed.
    assert!(validate_listener_bind_security("HTTP", any_v4, false, false, false).is_err());
    assert!(validate_listener_bind_security("HTTP", any_v4, true, false, false).is_err());
    assert!(validate_listener_bind_security("HTTP", any_v4, false, true, false).is_err());
    assert!(validate_listener_bind_security("binary TCP", lan_v4, false, false, false).is_err());

    // Non-loopback with BOTH auth AND TLS succeeds.
    assert!(validate_listener_bind_security("HTTPS", any_v4, true, true, false).is_ok());

    // Break-glass override succeeds.
    assert!(validate_listener_bind_security("HTTP", any_v4, false, false, true).is_ok());

    // Zenoh mesh listen endpoints fail closed on non-loopback, metadata, or malformed hosts.
    assert!(validate_mesh_listen_endpoints(&["tcp/127.0.0.1:7447".to_owned()], false).is_ok());
    assert!(validate_mesh_listen_endpoints(&["tcp/[::1]:7447".to_owned()], false).is_ok());
    assert!(validate_mesh_listen_endpoints(&["tcp/localhost:7447".to_owned()], false).is_ok());
    assert!(validate_mesh_listen_endpoints(&["tcp/localhost:notaport".to_owned()], false).is_err());
    assert!(validate_mesh_listen_endpoints(&["tcp/[::1]extra".to_owned()], false).is_err());
    assert!(
        validate_mesh_listen_endpoints(&["tcp/127.0.0.1:7447#iface=eth0".to_owned()], false)
            .is_err()
    );
    assert!(
        validate_mesh_listen_endpoints(&["tcp/127.0.0.1:7447?rel=0".to_owned()], false).is_err()
    );
    assert!(validate_mesh_listen_endpoints(&["tcp/0.0.0.0:7447".to_owned()], false).is_err());
    assert!(validate_mesh_listen_endpoints(&["quic/10.0.0.2:7447".to_owned()], false).is_err());
    assert!(validate_mesh_listen_endpoints(&["tcp/0.0.0.0:7447".to_owned()], true).is_ok());

    // Verify end-to-end MembusServer + MeshNode enforcement.
    let dir = TempDir::new().expect("tempdir");
    let pki = generate_test_pki(dir.path());
    let (storage, _search, server) =
        open_test_stack(&dir, StorageConfig::default(), ServerConfig::default());

    assert!(server.bind_http(any_v4).await.is_err());
    assert!(server.bind_tcp(any_v4).await.is_err());

    let tls_cfg = TlsServerConfig {
        cert_pem_path: pki.server_cert_pem.clone(),
        key_pem_path: pki.server_key_pem.clone(),
        client_ca_pem_path: None,
    };
    // TLS alone without auth still fails on non-loopback.
    assert!(server.bind_http_tls(any_v4, &tls_cfg).await.is_err());
    assert!(server.bind_tcp_tls(any_v4, &tls_cfg).await.is_err());

    // MeshNode::start refuses non-loopback without break-glass.
    let mesh_err = MeshNode::start(
        MeshConfig {
            node_id: "node-nonloop".to_owned(),
            listen_endpoints: vec!["tcp/0.0.0.0:0".to_owned()],
            ..Default::default()
        },
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        None,
    )
    .await;
    assert!(mesh_err.is_err());

    // Auth + TLS succeeds on non-loopback.
    let auth = AuthConfig::enforced(vec![TokenBinding {
        token: "secret-tok".to_owned(),
        principal: Principal {
            id: "admin".to_owned(),
            grants: vec![AclGrant {
                namespace_pattern: "*".to_owned(),
                key_prefix: Vec::new(),
                permissions: vec![Permission::Admin],
            }],
        },
    }]);
    let secured_server = server.with_auth(auth);
    let h = secured_server
        .bind_http_tls(any_v4, &tls_cfg)
        .await
        .expect("auth + TLS must allow non-loopback bind");
    h.shutdown().await;
}

// ============================================================================
// 4. Global Resource Admission Saturation Tests
// ============================================================================

#[tokio::test]
async fn global_snapshot_stream_http_and_scan_admission_limits_enforce_caps() {
    // 4a. Global MVCC snapshot cap + /v2/health gauge.
    {
        let dir = TempDir::new().expect("tempdir");
        let storage_cfg = StorageConfig {
            max_active_snapshots: 2,
            max_scan_limit: 5,
            ..StorageConfig::default()
        };
        let (storage, _search, server) = open_test_stack(&dir, storage_cfg, ServerConfig::default());
        let handle = server
            .bind_http("127.0.0.1:0".parse().unwrap())
            .await
            .expect("bind http");
        let Endpoint::Http(addr) = *handle.endpoint() else {
            panic!("expected Http");
        };

        let s1 = storage.open_snapshot().expect("snap 1");
        let s2 = storage.open_snapshot().expect("snap 2");
        assert_eq!(storage.active_snapshots(), 2);

        let (st, _, hb) = http1_call_with_headers(addr, Method::GET, "/v2/health", None).await;
        assert_eq!(st, StatusCode::OK);
        let h: serde_json::Value = serde_json::from_slice(&hb).unwrap();
        assert_eq!(h["active_snapshots"].as_u64(), Some(2));

        // 3rd snapshot fails closed with Busy.
        let busy = storage.open_snapshot().err().expect("3rd snapshot must fail");
        assert!(matches!(busy, MembusError::Busy { .. }));

        // Scan limit bound check (> max_scan_limit rejected cleanly).
        let over_limit = storage.scan_prefix(
            &s1,
            "ns",
            b"",
            &ScanOptions {
                after_key: None,
                limit: 6,
                max_bytes: 1024,
            },
        );
        assert!(matches!(over_limit, Err(MembusError::InvalidArgument(_))));

        drop(s1);
        assert_eq!(storage.active_snapshots(), 1);
        let s3 = storage.open_snapshot().expect("snap 3 after drop");
        drop(s2);
        drop(s3);
        assert_eq!(storage.active_snapshots(), 0);
        handle.shutdown().await;
    }

    // 4b. Global active streams cap (SSE) + 503 on saturation + /v2/health gauge.
    {
        let dir = TempDir::new().expect("tempdir");
        let server_cfg = ServerConfig {
            max_active_streams: 2,
            ..ServerConfig::default()
        };
        let (_storage, _search, server) = open_test_stack(&dir, StorageConfig::default(), server_cfg);
        let handle = server
            .bind_http("127.0.0.1:0".parse().unwrap())
            .await
            .expect("bind http");
        let Endpoint::Http(addr) = *handle.endpoint() else {
            panic!("expected Http");
        };

        // Open 2 live SSE streams (keep response bodies alive so streams stay open).
        async fn open_sse(
            addr: SocketAddr,
        ) -> (
            StatusCode,
            hyper::body::Incoming,
            tokio::task::JoinHandle<()>,
        ) {
            let stream = TcpStream::connect(addr).await.expect("connect");
            let (mut sender, conn) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
                .await
                .expect("handshake");
            let task = tokio::spawn(async move {
                let _ = conn.await;
            });
            let req = Request::builder()
                .method(Method::GET)
                .uri("/v2/signals/s/stream?mode=live")
                .header("host", addr.to_string())
                .body(Empty::<Bytes>::new())
                .unwrap();
            let resp = sender.send_request(req).await.expect("sse req");
            (resp.status(), resp.into_body(), task)
        }

        let (st1, _body1, _t1) = open_sse(addr).await;
        let (st2, _body2, _t2) = open_sse(addr).await;
        assert_eq!(st1, StatusCode::OK);
        assert_eq!(st2, StatusCode::OK);

        let (st_h, _, hb) = http1_call_with_headers(addr, Method::GET, "/v2/health", None).await;
        assert_eq!(st_h, StatusCode::OK);
        let h: serde_json::Value = serde_json::from_slice(&hb).unwrap();
        assert_eq!(h["active_streams"].as_u64(), Some(2));

        // 3rd SSE stream MUST be rejected with 503 Service Unavailable.
        let (st3, _body3, _t3) = open_sse(addr).await;
        assert_eq!(st3, StatusCode::SERVICE_UNAVAILABLE);

        handle.shutdown().await;
    }

    // 4c. Global HTTP in-flight request cap returns 503 while /v2/health stays 200 OK.
    {
        let dir = TempDir::new().expect("tempdir");
        let raw_storage = Arc::new(
            RocksDbStorage::open(dir.path(), StorageConfig::default(), Arc::new(NoopPublisher))
                .expect("open"),
        );
        raw_storage
            .put("cap", b"hold", b"val", Ack::Flush, ExpectedVersion::Any)
            .expect("put");
        let barrier = Arc::new(Barrier::new(2));
        let spy = Arc::new(SpyStorage {
            inner: raw_storage,
            wal_flush_called: Arc::new(AtomicBool::new(false)),
            fail_wal_flush: false,
            get_barrier: Some(Arc::clone(&barrier)),
        });
        let signal_cfg = SignalConfig::default();
        let ring = Arc::new(SignalRing::new(signal_cfg));
        let broker = Arc::new(SignalBroker::new(
            Arc::clone(&spy) as Arc<dyn StorageEngine>,
            ring,
            signal_cfg,
        ));
        let server = MembusServer::new(
            spy as Arc<dyn StorageEngine>,
            broker,
            ServerConfig {
                max_http_in_flight: 1,
                ..ServerConfig::default()
            },
        );
        let handle = server
            .bind_http("127.0.0.1:0".parse().unwrap())
            .await
            .expect("bind");
        let Endpoint::Http(addr) = *handle.endpoint() else {
            panic!("expected Http");
        };

        // Start request #1 which blocks inside SpyStorage::get("cap", b"hold").
        let req1_task = tokio::spawn(async move {
            http1_call_with_headers(addr, Method::GET, "/v2/kv/cap/hold", None).await
        });

        // Synchronize until request #1 is definitely inside the handler holding the sole permit.
        let b_enter = Arc::clone(&barrier);
        tokio::task::spawn_blocking(move || b_enter.wait())
            .await
            .expect("enter barrier");

        // Request #2 on data-plane MUST receive 503 Service Unavailable.
        let (st2, _, _) =
            http1_call_with_headers(addr, Method::GET, "/v2/kv/cap/other", None).await;
        assert_eq!(st2, StatusCode::SERVICE_UNAVAILABLE);

        // /v2/health remains 200 OK during data-plane saturation!
        let (st_health, _, _) =
            http1_call_with_headers(addr, Method::GET, "/v2/health", None).await;
        assert_eq!(st_health, StatusCode::OK);

        // Release request #1 and verify completion.
        let b_exit = Arc::clone(&barrier);
        tokio::task::spawn_blocking(move || b_exit.wait())
            .await
            .expect("exit barrier");
        let (st1, _, _) = req1_task.await.expect("req1 join");
        assert_eq!(st1, StatusCode::OK);

        handle.shutdown().await;
    }
}

// ============================================================================
// 5. Multi-Threaded OwnedSnapshot Lifetime / Concurrency / Compaction Stress
// ============================================================================

#[test]
fn owned_snapshot_multithreaded_concurrency_compaction_and_outliving_storage() {
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
        .expect("open storage"),
    );

    // Seed baseline v0 dataset.
    for i in 0..100u32 {
        let key = format!("k{i:04}");
        let val = format!("baseline-v0-{i:04}");
        storage
            .put("stress", key.as_bytes(), val.as_bytes(), Ack::Fast, ExpectedVersion::Any)
            .expect("seed put");
    }

    // Pin 8 concurrent MVCC snapshots at baseline state.
    let snapshots: Vec<OwnedSnapshot> = (0..8)
        .map(|_| storage.open_snapshot().expect("open snapshot"))
        .collect();
    assert_eq!(storage.active_snapshots(), 8);

    let start_barrier = Arc::new(Barrier::new(12)); // 8 readers + 4 writers
    let done_writing = Arc::new(AtomicBool::new(false));

    // Spawn 4 writers hammering overwrites and manual RocksDB compactions.
    let mut writer_handles = Vec::new();
    for w in 0..4u32 {
        let st = Arc::clone(&storage);
        let b = Arc::clone(&start_barrier);
        writer_handles.push(thread::spawn(move || {
            b.wait();
            for round in 0..25u32 {
                for i in 0..100u32 {
                    let key = format!("k{i:04}");
                    let val = format!("writer-{w}-round-{round}-{i:04}");
                    st.put("stress", key.as_bytes(), val.as_bytes(), Ack::Fast, ExpectedVersion::Any)
                        .expect("concurrent put");
                }
                if round % 8 == 0 {
                    st.compact_all().expect("manual compaction");
                }
            }
        }));
    }

    // Spawn 8 snapshot readers continuously verifying snapshot isolation under heavy compaction,
    // returning their OwnedSnapshot handles so we can drop them AFTER RocksDbStorage is dropped.
    let mut reader_handles = Vec::new();
    for snap in snapshots {
        let st = Arc::clone(&storage);
        let b = Arc::clone(&start_barrier);
        let done = Arc::clone(&done_writing);
        reader_handles.push(thread::spawn(move || {
            b.wait();
            let mut iterations = 0usize;
            while !done.load(Ordering::Acquire) || iterations < 5 {
                let page = st
                    .scan_prefix(
                        &snap,
                        "stress",
                        b"k",
                        &ScanOptions {
                            after_key: None,
                            limit: 200,
                            max_bytes: 1024 * 1024,
                        },
                    )
                    .expect("scan_prefix in snapshot");
                assert_eq!(page.records.len(), 100);
                for (idx, rec) in page.records.iter().enumerate() {
                    let expected = format!("baseline-v0-{idx:04}");
                    assert_eq!(rec.value, expected.as_bytes());
                }
                iterations += 1;
            }
            snap
        }));
    }

    for wh in writer_handles {
        wh.join().expect("writer thread");
    }
    done_writing.store(true, Ordering::Release);

    let surviving_snaps: Vec<OwnedSnapshot> = reader_handles
        .into_iter()
        .map(|rh| rh.join().expect("reader thread"))
        .collect();
    assert_eq!(surviving_snaps.len(), 8);

    // Drop the final RocksDbStorage handle COMPLETELY while all 8 OwnedSnapshots are still alive!
    drop(storage);

    // Concurrently drop all 8 surviving snapshots across 8 threads after RocksDbStorage is gone.
    let drop_barrier = Arc::new(Barrier::new(8));
    let drop_threads: Vec<_> = surviving_snaps
        .into_iter()
        .map(|snap| {
            let b = Arc::clone(&drop_barrier);
            thread::spawn(move || {
                b.wait();
                assert!(snap.snapshot_seq() >= 100);
                drop(snap);
            })
        })
        .collect();
    for dt in drop_threads {
        dt.join().expect("snapshot drop thread");
    }
}

// ============================================================================
// 6. CLI Help Exit Code 0 + New Hardening Flag Parsing
// ============================================================================

#[test]
fn cli_help_exits_zero_to_stdout_and_new_serve_flags_parse() {
    let bin = env!("CARGO_BIN_EXE_membus");
    for args in [
        vec!["--help"],
        vec!["-h"],
        vec!["help"],
        vec!["serve", "--help"],
        vec!["health", "--help"],
        vec!["checkpoint", "--help"],
        vec!["diag", "--help"],
    ] {
        let out = ProcessCommand::new(bin)
            .args(&args)
            .output()
            .expect("spawn membus --help");
        assert!(
            out.status.success(),
            "membus {:?} should exit 0, got {:?}; stderr={}",
            args,
            out.status.code(),
            String::from_utf8_lossy(&out.stderr)
        );
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(
            stdout.contains("Usage:"),
            "membus {:?} stdout missing Usage header: {stdout}",
            args
        );
        assert!(
            out.stderr.is_empty(),
            "membus {:?} wrote unexpected stderr: {}",
            args,
            String::from_utf8_lossy(&out.stderr)
        );
    }

    // `diag inspect --data-dir help` must NOT treat positional "help" as a help flag.
    let diag_help_dir = parse_args(["diag", "inspect", "--data-dir", "help"])
        .expect("parse diag inspect --data-dir help");
    assert!(matches!(
        diag_help_dir,
        Command::Diag(membus::cli::diag::DiagCommand::Inspect { ref data_dir })
            if data_dir == Path::new("help")
    ));
    assert!(matches!(
        parse_args(["diag", "help"]).expect("parse diag help"),
        Command::Help(_)
    ));

    let parsed = parse_args([
        "serve",
        "--data-dir",
        "state",
        "--http-listen",
        "0.0.0.0:8443",
        "--http-tls-cert",
        "srv.pem",
        "--http-tls-key",
        "srv.key",
        "--http-tls-client-ca",
        "ca.pem",
        "--allow-insecure-non-loopback",
        "--public-health-probe",
        "--max-http-in-flight",
        "512",
        "--max-active-streams",
        "256",
        "--max-active-snapshots",
        "128",
        "--query-timeout-ms",
        "15000",
    ])
    .expect("parse hardened serve flags");
    match parsed {
        Command::Serve(a) => {
            assert_eq!(a.http_tls_cert.as_deref(), Some(Path::new("srv.pem")));
            assert_eq!(a.http_tls_key.as_deref(), Some(Path::new("srv.key")));
            assert_eq!(a.http_tls_client_ca.as_deref(), Some(Path::new("ca.pem")));
            assert!(a.allow_insecure_non_loopback);
            assert!(a.public_health_probe);
            assert_eq!(a.max_http_in_flight, Some(512));
            assert_eq!(a.max_active_streams, Some(256));
            assert_eq!(a.max_active_snapshots, Some(128));
            assert_eq!(a.query_timeout_ms, Some(15000));
        }
        other => panic!("expected Serve, got {other:?}"),
    }
}
