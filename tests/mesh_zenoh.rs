use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use membus::{
    Ack, ExpectedVersion, MembusError, MeshConfig, MeshNode, NoopPublisher, RocksDbStorage,
    SearchConfig, SearchDocument, SearchEngine, SearchLogOp, SearchMode, SearchQuery,
    SearchVisibility, StorageConfig, StorageEngine,
};
use tempfile::TempDir;

fn free_tcp_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral tcp port");
    listener.local_addr().unwrap().port()
}

fn open_node_storage(dir: &std::path::Path) -> Arc<RocksDbStorage> {
    let cfg = StorageConfig {
        bg_sync_interval: None,
        ..Default::default()
    };
    Arc::new(RocksDbStorage::open(dir, cfg, Arc::new(NoopPublisher)).expect("open rocksdb"))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_node_live_delta_pub_sub_and_loop_prevention() {
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();

    let storage_a = open_node_storage(dir_a.path());
    let storage_b = open_node_storage(dir_b.path());
    let search_a = Arc::new(
        SearchEngine::open(
            dir_a.path(),
            Arc::clone(&storage_a) as Arc<dyn StorageEngine>,
            SearchConfig::default(),
        )
        .unwrap(),
    );
    let search_b = Arc::new(
        SearchEngine::open(
            dir_b.path(),
            Arc::clone(&storage_b) as Arc<dyn StorageEngine>,
            SearchConfig::default(),
        )
        .unwrap(),
    );

    let port_a = free_tcp_port();
    let port_b = free_tcp_port();
    let cluster = format!("membus/test_live_{port_a}");

    let node_a = MeshNode::start(
        MeshConfig {
            node_id: "node-a".to_owned(),
            cluster_prefix: cluster.clone(),
            listen_endpoints: vec![format!("tcp/127.0.0.1:{port_a}")],
            connect_endpoints: vec![format!("tcp/127.0.0.1:{port_b}")],
            publish_poll_interval_ms: 15,
            ..Default::default()
        },
        Arc::clone(&storage_a) as Arc<dyn StorageEngine>,
        Some(Arc::clone(&search_a)),
    )
    .await
    .unwrap();

    let node_b = MeshNode::start(
        MeshConfig {
            node_id: "node-b".to_owned(),
            cluster_prefix: cluster,
            listen_endpoints: vec![format!("tcp/127.0.0.1:{port_b}")],
            connect_endpoints: vec![format!("tcp/127.0.0.1:{port_a}")],
            publish_poll_interval_ms: 15,
            ..Default::default()
        },
        Arc::clone(&storage_b) as Arc<dyn StorageEngine>,
        Some(Arc::clone(&search_b)),
    )
    .await
    .unwrap();

    // Allow Zenoh peer session handshake and subscriber declaration propagation.
    tokio::time::sleep(Duration::from_millis(300)).await;

    let doc = SearchDocument {
        namespace: "mesh_ns".to_owned(),
        doc_id: "k1".to_owned(),
        kind: "note".to_owned(),
        title: "Distributed Replication".to_owned(),
        tags: vec!["zenoh".to_owned()],
        body: "Live delta replication body".to_owned(),
        visibility: SearchVisibility::Public,
    };
    storage_a
        .put_indexed(
            "mesh_ns",
            b"k1",
            b"hello-from-a",
            Ack::Fast,
            ExpectedVersion::Any,
            None,
            Some(&SearchLogOp::Index(doc)),
        )
        .unwrap();
    node_a.flush_outbound().await.unwrap();

    // Poll until Node B receives the live delta via Zenoh Pub/Sub.
    let mut replicated = false;
    for _ in 0..80 {
        if let Some(rec) = storage_b.get("mesh_ns", b"k1").unwrap() {
            if rec.value == b"hello-from-a" {
                replicated = true;
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(replicated, "Node B should receive live delta from Node A");

    // Verify search op also landed on Node B.
    let hits = search_b
        .search(&SearchQuery {
            query: "Distributed Replication".to_owned(),
            mode: SearchMode::Bm25,
            namespace: Some("mesh_ns".to_owned()),
            kind: None,
            limit: 10,
        })
        .unwrap();
    assert_eq!(hits.hits.len(), 1);
    assert_eq!(hits.hits[0].doc_id, "k1");

    // Verify strict loop prevention: sequences stabilize and never spin upward.
    let seq_a_before = storage_a.latest_seq();
    let seq_b_before = storage_b.latest_seq();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(storage_a.latest_seq(), seq_a_before);
    assert_eq!(storage_b.latest_seq(), seq_b_before);

    // Verify persisted peer watermark on Node B.
    let wm = node_b
        .peer_watermark("node-a")
        .unwrap()
        .expect("peer watermark persisted");
    assert_eq!(wm.peer_id, "node-a");
    assert!(wm.last_origin_seq >= 1);

    node_a.shutdown().await.unwrap();
    node_b.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_diverge_xor_digest_catchup_and_lww_convergence() {
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let storage_a = open_node_storage(dir_a.path());
    let storage_b = open_node_storage(dir_b.path());

    let port_a = free_tcp_port();
    let port_b = free_tcp_port();
    let cluster = format!("membus/test_part_{port_a}");

    // Start Node A alone while Node B is partitioned (offline).
    let node_a = MeshNode::start(
        MeshConfig {
            node_id: "node-a".to_owned(),
            cluster_prefix: cluster.clone(),
            listen_endpoints: vec![format!("tcp/127.0.0.1:{port_a}")],
            connect_endpoints: vec![format!("tcp/127.0.0.1:{port_b}")],
            publish_poll_interval_ms: 20,
            ..Default::default()
        },
        Arc::clone(&storage_a) as Arc<dyn StorageEngine>,
        None,
    )
    .await
    .unwrap();

    // Diverging writes during partition:
    // Node A writes `only_a` and `shared_key` (earlier timestamp).
    storage_a
        .put("sync", b"only_a", b"val-a", Ack::Flush, ExpectedVersion::Any)
        .unwrap();
    storage_a
        .put(
            "sync",
            b"shared_key",
            b"from-a-earlier",
            Ack::Flush,
            ExpectedVersion::Any,
        )
        .unwrap();
    node_a.flush_outbound().await.unwrap();

    // Ensure Node B's write to `shared_key` has a strictly higher timestamp so LWW is deterministic.
    tokio::time::sleep(Duration::from_millis(25)).await;
    storage_b
        .put("sync", b"only_b", b"val-b", Ack::Flush, ExpectedVersion::Any)
        .unwrap();
    storage_b
        .put(
            "sync",
            b"shared_key",
            b"from-b-newer",
            Ack::Flush,
            ExpectedVersion::Any,
        )
        .unwrap();

    // Partition heals: Node B comes online and connects to Node A.
    let node_b = MeshNode::start(
        MeshConfig {
            node_id: "node-b".to_owned(),
            cluster_prefix: cluster,
            listen_endpoints: vec![format!("tcp/127.0.0.1:{port_b}")],
            connect_endpoints: vec![format!("tcp/127.0.0.1:{port_a}")],
            publish_poll_interval_ms: 20,
            ..Default::default()
        },
        Arc::clone(&storage_b) as Arc<dyn StorageEngine>,
        None,
    )
    .await
    .unwrap();

    // Wait briefly for TCP peering to establish.
    for _ in 0..40 {
        if node_b.query_peer_digest("node-a", "sync", b"").await.is_ok()
            && node_a.query_peer_digest("node-b", "sync", b"").await.is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Anti-entropy digest detects divergence and reconciles both directions.
    let rep_b = node_b.reconcile_with_peer("node-a", "sync", b"").await.unwrap();
    assert!(rep_b.was_divergent, "nodes must initially be divergent");

    let rep_a = node_a.reconcile_with_peer("node-b", "sync", b"").await.unwrap();
    assert!(rep_a.converged, "node A must converge to node B after pull");

    let digest_a = node_a.local_digest("sync", b"").unwrap();
    let digest_b = node_b.local_digest("sync", b"").unwrap();
    assert_eq!(digest_a.entry_count, 3);
    assert_eq!(digest_b.entry_count, 3);
    assert_eq!(
        digest_a.xor_digest, digest_b.xor_digest,
        "XOR support-digests must match bit-for-bit after reconciliation"
    );

    // Both nodes converged to Node B's newer write on `shared_key` and have both unique keys.
    assert_eq!(
        storage_a.get("sync", b"shared_key").unwrap().unwrap().value,
        b"from-b-newer"
    );
    assert_eq!(
        storage_b.get("sync", b"shared_key").unwrap().unwrap().value,
        b"from-b-newer"
    );
    assert_eq!(
        storage_b.get("sync", b"only_a").unwrap().unwrap().value,
        b"val-a"
    );
    assert_eq!(
        storage_a.get("sync", b"only_b").unwrap().unwrap().value,
        b"val-b"
    );

    node_a.shutdown().await.unwrap();
    node_b.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mesh_gap_on_trimmed_horizon_and_sealed_visibility_isolation() {
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let storage_a = open_node_storage(dir_a.path());
    let storage_b = open_node_storage(dir_b.path());
    let search_b = Arc::new(
        SearchEngine::open(
            dir_b.path(),
            Arc::clone(&storage_b) as Arc<dyn StorageEngine>,
            SearchConfig::default(),
        )
        .unwrap(),
    );

    // Populate 4 entries on Node A including a Sealed search document, then trim before seq 3.
    storage_a
        .put("sec", b"k1", b"v1", Ack::Flush, ExpectedVersion::Any)
        .unwrap();
    storage_a
        .put("sec", b"k2", b"v2", Ack::Flush, ExpectedVersion::Any)
        .unwrap();
    storage_a
        .put("sec", b"k3", b"v3", Ack::Flush, ExpectedVersion::Any)
        .unwrap();
    let sealed_doc = SearchDocument {
        namespace: "sec".to_owned(),
        doc_id: "secret-doc".to_owned(),
        kind: "secret".to_owned(),
        title: "TOPSECRET_TITLE_XYZ".to_owned(),
        tags: vec!["classified".to_owned()],
        body: "TOPSECRET_PAYLOAD_9999".to_owned(),
        visibility: SearchVisibility::Sealed,
    };
    storage_a
        .put_indexed(
            "sec",
            b"k4",
            b"opaque",
            Ack::Flush,
            ExpectedVersion::Any,
            None,
            Some(&SearchLogOp::Index(sealed_doc)),
        )
        .unwrap();
    storage_a.trim_log_before(3).unwrap();

    let port_a = free_tcp_port();
    let port_b = free_tcp_port();
    let cluster = format!("membus/test_gap_{port_a}");

    let node_a = MeshNode::start(
        MeshConfig {
            node_id: "node-a".to_owned(),
            cluster_prefix: cluster.clone(),
            listen_endpoints: vec![format!("tcp/127.0.0.1:{port_a}")],
            connect_endpoints: vec![format!("tcp/127.0.0.1:{port_b}")],
            publish_poll_interval_ms: 500,
            ..Default::default()
        },
        Arc::clone(&storage_a) as Arc<dyn StorageEngine>,
        None,
    )
    .await
    .unwrap();

    let node_b = MeshNode::start(
        MeshConfig {
            node_id: "node-b".to_owned(),
            cluster_prefix: cluster,
            listen_endpoints: vec![format!("tcp/127.0.0.1:{port_b}")],
            connect_endpoints: vec![format!("tcp/127.0.0.1:{port_a}")],
            publish_poll_interval_ms: 500,
            ..Default::default()
        },
        Arc::clone(&storage_b) as Arc<dyn StorageEngine>,
        Some(Arc::clone(&search_b)),
    )
    .await
    .unwrap();

    for _ in 0..40 {
        if node_b.query_peer_digest("node-a", "sec", b"").await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Requesting catch-up from seq 0 must fail closed with MembusError::Gap!
    let gap_err = node_b
        .catchup_from_peer("node-a", Some(0))
        .await
        .expect_err("trimmed horizon must return explicit Gap");
    assert!(
        matches!(
            gap_err,
            MembusError::Gap {
                earliest_available_seq: 3,
                ..
            }
        ),
        "unexpected error: {gap_err:?}"
    );

    // Requesting catch-up from seq 2 succeeds for seq 3..=4, and Sealed plaintext NEVER reaches Node B search.
    let stats = node_b.catchup_from_peer("node-a", Some(2)).await.unwrap();
    assert_eq!(stats.deltas_applied, 2);
    assert_eq!(storage_b.get("sec", b"k4").unwrap().unwrap().value, b"opaque");

    let hits = search_b
        .search(&SearchQuery {
            query: "TOPSECRET_PAYLOAD_9999".to_owned(),
            mode: SearchMode::LiteralSubstring,
            namespace: Some("sec".to_owned()),
            kind: None,
            limit: 10,
        })
        .unwrap();
    assert!(
        hits.hits.is_empty(),
        "Sealed search plaintext must never be transmitted or indexed on peers"
    );

    node_a.shutdown().await.unwrap();
    node_b.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn out_of_order_pubsub_preserves_contiguous_watermark_and_reconcile_recovers_gap() {
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let storage_a = open_node_storage(dir_a.path());
    let storage_b = open_node_storage(dir_b.path());

    // Node A writes seq 1 BEFORE Node B is even started (and before Node A publishes).
    storage_a
        .put("ooo", b"k1", b"missed-1", Ack::Flush, ExpectedVersion::Any)
        .unwrap();

    let port_a = free_tcp_port();
    let port_b = free_tcp_port();
    let cluster = format!("membus/test_ooo_{port_a}");

    // Start Node A alone so it publishes seq 1 while Node B is offline.
    let node_a = MeshNode::start(
        MeshConfig {
            node_id: "node-a".to_owned(),
            cluster_prefix: cluster.clone(),
            listen_endpoints: vec![format!("tcp/127.0.0.1:{port_a}")],
            connect_endpoints: vec![format!("tcp/127.0.0.1:{port_b}")],
            publish_poll_interval_ms: 15,
            ..Default::default()
        },
        Arc::clone(&storage_a) as Arc<dyn StorageEngine>,
        None,
    )
    .await
    .unwrap();
    node_a.flush_outbound().await.unwrap();
    assert_eq!(node_a.published_seq(), 1);

    // Now start Node B so it receives seq 2 live via Pub/Sub while having missed seq 1!
    let node_b = MeshNode::start(
        MeshConfig {
            node_id: "node-b".to_owned(),
            cluster_prefix: cluster,
            listen_endpoints: vec![format!("tcp/127.0.0.1:{port_b}")],
            connect_endpoints: vec![format!("tcp/127.0.0.1:{port_a}")],
            publish_poll_interval_ms: 15,
            ..Default::default()
        },
        Arc::clone(&storage_b) as Arc<dyn StorageEngine>,
        None,
    )
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    storage_a
        .put("ooo", b"k2", b"live-2", Ack::Flush, ExpectedVersion::Any)
        .unwrap();
    node_a.flush_outbound().await.unwrap();

    // Wait for k2 (seq 2) to arrive live on Node B.
    let mut got_k2 = false;
    for _ in 0..80 {
        if storage_b.get("ooo", b"k2").unwrap().is_some() {
            got_k2 = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(got_k2, "Node B should receive live out-of-order k2");
    assert!(storage_b.get("ooo", b"k1").unwrap().is_none());

    // M1: peer watermark MUST NOT advance past contiguous floor 0 on out-of-order seq 2!
    let wm_before = node_b.peer_watermark("node-a").unwrap().unwrap();
    assert_eq!(wm_before.last_origin_seq, 0);
    assert!(
        wm_before.outbound_acked_seq.is_none(),
        "inbound receive must never populate outbound_acked_seq"
    );

    // Reconcile pulls the missed gap (seq 1) and converges bit-for-bit.
    let report = node_b.reconcile_with_peer("node-a", "ooo", b"").await.unwrap();
    assert!(report.was_divergent);
    assert!(report.converged);
    assert_eq!(
        storage_b.get("ooo", b"k1").unwrap().unwrap().value,
        b"missed-1"
    );
    let wm_after = node_b.peer_watermark("node-a").unwrap().unwrap();
    assert_eq!(wm_after.last_origin_seq, 2);

    node_a.shutdown().await.unwrap();
    node_b.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_transitive_chain_and_standalone_search_replication() {
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let dir_c = TempDir::new().unwrap();
    let storage_a = open_node_storage(dir_a.path());
    let storage_b = open_node_storage(dir_b.path());
    let storage_c = open_node_storage(dir_c.path());

    let search_a = Arc::new(
        SearchEngine::open(
            dir_a.path(),
            Arc::clone(&storage_a) as Arc<dyn StorageEngine>,
            SearchConfig::default(),
        )
        .unwrap(),
    );
    let search_b = Arc::new(
        SearchEngine::open(
            dir_b.path(),
            Arc::clone(&storage_b) as Arc<dyn StorageEngine>,
            SearchConfig::default(),
        )
        .unwrap(),
    );
    let search_c = Arc::new(
        SearchEngine::open(
            dir_c.path(),
            Arc::clone(&storage_c) as Arc<dyn StorageEngine>,
            SearchConfig::default(),
        )
        .unwrap(),
    );

    let port_a = free_tcp_port();
    let port_b = free_tcp_port();
    let port_c = free_tcp_port();
    let cluster = format!("membus/test_chain_{port_a}");

    // Linear topology: A <-> B <-> C (A and C do NOT connect directly!).
    let node_a = MeshNode::start(
        MeshConfig {
            node_id: "node-a".to_owned(),
            cluster_prefix: cluster.clone(),
            listen_endpoints: vec![format!("tcp/127.0.0.1:{port_a}")],
            connect_endpoints: vec![format!("tcp/127.0.0.1:{port_b}")],
            publish_poll_interval_ms: 15,
            ..Default::default()
        },
        Arc::clone(&storage_a) as Arc<dyn StorageEngine>,
        Some(Arc::clone(&search_a)),
    )
    .await
    .unwrap();

    let node_b = MeshNode::start(
        MeshConfig {
            node_id: "node-b".to_owned(),
            cluster_prefix: cluster.clone(),
            listen_endpoints: vec![format!("tcp/127.0.0.1:{port_b}")],
            connect_endpoints: vec![
                format!("tcp/127.0.0.1:{port_a}"),
                format!("tcp/127.0.0.1:{port_c}"),
            ],
            publish_poll_interval_ms: 15,
            ..Default::default()
        },
        Arc::clone(&storage_b) as Arc<dyn StorageEngine>,
        Some(Arc::clone(&search_b)),
    )
    .await
    .unwrap();

    let node_c = MeshNode::start(
        MeshConfig {
            node_id: "node-c".to_owned(),
            cluster_prefix: cluster,
            listen_endpoints: vec![format!("tcp/127.0.0.1:{port_c}")],
            connect_endpoints: vec![format!("tcp/127.0.0.1:{port_b}")],
            publish_poll_interval_ms: 15,
            ..Default::default()
        },
        Arc::clone(&storage_c) as Arc<dyn StorageEngine>,
        Some(Arc::clone(&search_c)),
    )
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(350)).await;

    // Write KV + standalone SearchDocument on Node A.
    storage_a
        .put("chain", b"from_a", b"hop-across-b", Ack::Flush, ExpectedVersion::Any)
        .unwrap();
    search_a
        .index_document(
            SearchDocument {
                namespace: "chain".to_owned(),
                doc_id: "standalone-1".to_owned(),
                kind: "doc".to_owned(),
                title: "Transitive Search Doc".to_owned(),
                tags: vec!["relay".to_owned()],
                body: "UniqueToken_Chain_7788".to_owned(),
                visibility: SearchVisibility::Public,
            },
            Ack::Flush,
        )
        .unwrap();
    node_a.flush_outbound().await.unwrap();

    // Poll until Node C receives BOTH the KV write AND the standalone search doc via B!
    let mut converged_c = false;
    for _ in 0..120 {
        let kv_ok = storage_c
            .get("chain", b"from_a")
            .unwrap()
            .is_some_and(|r| r.value == b"hop-across-b");
        let search_ok = !search_c
            .search(&SearchQuery {
                query: "UniqueToken_Chain_7788".to_owned(),
                mode: SearchMode::LiteralSubstring,
                namespace: Some("chain".to_owned()),
                kind: None,
                limit: 5,
            })
            .unwrap()
            .hits
            .is_empty();
        if kv_ok && search_ok {
            converged_c = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(
        converged_c,
        "Node C must receive A's KV and standalone search doc transitively via B"
    );

    // Ensure zero broadcast loops across all 3 nodes.
    let seqs_before = (
        storage_a.latest_seq(),
        storage_b.latest_seq(),
        storage_c.latest_seq(),
    );
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        (
            storage_a.latest_seq(),
            storage_b.latest_seq(),
            storage_c.latest_seq()
        ),
        seqs_before
    );

    node_a.shutdown().await.unwrap();
    node_b.shutdown().await.unwrap();
    node_c.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn negative_clock_skew_hlc_and_forced_trim_recovery_and_epoch_reset() {
    use membus::{MutationOp, ReplicatedDelta, ReplicationStamp};
    use sha2::{Digest, Sha256};

    // 1. M5: Negative clock skew HLC test — local write AFTER observing future-dated peer delta wins!
    let dir_hlc = TempDir::new().unwrap();
    let st_hlc = open_node_storage(dir_hlc.path());
    let future_ts = st_hlc.hlc_now_ms().saturating_add(10_000_000);
    let remote_val = b"remote-future-skew".to_vec();
    let delta = ReplicatedDelta {
        stamp: ReplicationStamp {
            timestamp_ms: future_ts,
            origin_node_id: "peer-fast-clock".to_owned(),
            origin_seq: 1,
            value_sha256: Sha256::digest(&remote_val).into(),
        },
        namespace: "hlc".to_owned(),
        key: b"k".to_vec(),
        op: MutationOp::Put { value: remote_val },
        indexes: None,
        search_op: None,
    };
    let applied = st_hlc
        .apply_replicated_delta("local-slow-clock", &delta, Ack::Flush)
        .unwrap();
    assert!(applied.applied);

    // Local write causally after observing future remote delta must get strictly higher HLC ts_ms!
    st_hlc
        .put("hlc", b"k", b"local-causal-update", Ack::Flush, ExpectedVersion::Any)
        .unwrap();
    let rec = st_hlc.get("hlc", b"k").unwrap().unwrap();
    assert!(
        rec.meta.ts_ms > future_ts,
        "HLC local ts_ms {} must exceed observed future remote ts_ms {}",
        rec.meta.ts_ms,
        future_ts
    );

    // 2. M7 + M8: Forced-trim publisher recovery AND peer epoch reset!
    let dir_a1 = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let storage_a1 = open_node_storage(dir_a1.path());
    let storage_b = open_node_storage(dir_b.path());

    let port_a = free_tcp_port();
    let port_b = free_tcp_port();
    let cluster = format!("membus/test_m7m8_{port_a}");

    // Populate and force-trim Node A BEFORE flushing outbound so publisher encounters Gap.
    let node_a1 = MeshNode::start(
        MeshConfig {
            node_id: "node-a".to_owned(),
            cluster_prefix: cluster.clone(),
            listen_endpoints: vec![format!("tcp/127.0.0.1:{port_a}")],
            connect_endpoints: vec![format!("tcp/127.0.0.1:{port_b}")],
            publish_poll_interval_ms: 5_000,
            ..Default::default()
        },
        Arc::clone(&storage_a1) as Arc<dyn StorageEngine>,
        None,
    )
    .await
    .unwrap();
    storage_a1
        .put("m7", b"k1", b"v1", Ack::Flush, ExpectedVersion::Any)
        .unwrap();
    storage_a1
        .put("m7", b"k2", b"v2", Ack::Flush, ExpectedVersion::Any)
        .unwrap();
    storage_a1.safe_trim_log_before(2, true).unwrap();
    storage_a1
        .put("m7", b"k3", b"v3", Ack::Flush, ExpectedVersion::Any)
        .unwrap();

    // Publisher must recover from Gap (horizon=1) and advance published_seq to 3 without wedging!
    let pub_seq = node_a1.flush_outbound().await.unwrap();
    assert_eq!(pub_seq, 3);

    // Connect Node B and pull from Node A1 so B records A1's epoch & watermark.
    let node_b = MeshNode::start(
        MeshConfig {
            node_id: "node-b".to_owned(),
            cluster_prefix: cluster.clone(),
            listen_endpoints: vec![format!("tcp/127.0.0.1:{port_b}")],
            connect_endpoints: vec![format!("tcp/127.0.0.1:{port_a}")],
            publish_poll_interval_ms: 500,
            ..Default::default()
        },
        Arc::clone(&storage_b) as Arc<dyn StorageEngine>,
        None,
    )
    .await
    .unwrap();
    for _ in 0..40 {
        if node_b.query_peer_digest("node-a", "m7", b"").await.is_ok() {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    node_b.catchup_from_peer("node-a", Some(1)).await.unwrap();
    let old_epoch = storage_a1.epoch();
    assert_eq!(
        node_b.peer_watermark("node-a").unwrap().unwrap().remote_epoch,
        old_epoch
    );
    node_a1.shutdown().await.unwrap();
    drop(storage_a1);

    // M8: Node A is wiped and restarted with a brand-new DB (new db_epoch)!
    // Simulate Node A2 having already published seq 1 before Node B reconnected so Node B
    // still holds `old_epoch` and MUST trigger `CatchupResponse::EpochMismatch` -> reset -> catchup.
    let dir_a2 = TempDir::new().unwrap();
    let storage_a2 = open_node_storage(dir_a2.path());
    assert_ne!(storage_a2.epoch(), old_epoch);
    let c_a2 = storage_a2
        .put("m7", b"fresh_key", b"after-epoch-reset", Ack::Flush, ExpectedVersion::Any)
        .unwrap();
    let mut pre_pub = [0u8; 16];
    pre_pub[0..8].copy_from_slice(&storage_a2.epoch().to_be_bytes());
    pre_pub[8..16].copy_from_slice(&c_a2.seq.to_be_bytes());
    storage_a2
        .meta_put(membus::mesh::META_KEY_MESH_PUB_SEQ, &pre_pub, true)
        .unwrap();

    let node_a2 = MeshNode::start(
        MeshConfig {
            node_id: "node-a".to_owned(),
            cluster_prefix: cluster,
            listen_endpoints: vec![format!("tcp/127.0.0.1:{port_a}")],
            connect_endpoints: vec![format!("tcp/127.0.0.1:{port_b}")],
            publish_poll_interval_ms: 5_000,
            ..Default::default()
        },
        Arc::clone(&storage_a2) as Arc<dyn StorageEngine>,
        None,
    )
    .await
    .unwrap();

    for _ in 0..40 {
        if let Ok(d) = node_b.query_peer_digest("node-a", "m7", b"").await {
            if d.epoch == storage_a2.epoch() {
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // `catchup_from_peer(..., None)` detects EpochMismatch, resets cursor to 0, and catches up!
    let stats = node_b.catchup_from_peer("node-a", None).await.unwrap();
    assert_eq!(stats.deltas_applied, 1);
    assert_eq!(
        storage_b.get("m7", b"fresh_key").unwrap().unwrap().value,
        b"after-epoch-reset"
    );
    let new_wm = node_b.peer_watermark("node-a").unwrap().unwrap();
    assert_eq!(new_wm.remote_epoch, storage_a2.epoch());
    assert_eq!(new_wm.last_origin_seq, 1);

    node_a2.shutdown().await.unwrap();
    node_b.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cold_tier_swap_and_rehydrate_never_poison_lww_signals_or_mesh() {
    use membus::types::now_unix_ms;
    use membus::{
        ColdStore, FileColdStore, MutationOp, ReplicatedDelta, ReplicationStamp, SignalBroker,
        SignalConfig, SignalPublisher, SignalRing, TieringManager, TieringPolicy,
    };
    use sha2::{Digest, Sha256};

    let db_dir = TempDir::new().unwrap();
    let cold_dir = TempDir::new().unwrap();
    let ring = Arc::new(SignalRing::new(SignalConfig::default()));
    let cfg = StorageConfig {
        bg_sync_interval: None,
        ..Default::default()
    };
    let storage = Arc::new(
        RocksDbStorage::open(
            db_dir.path(),
            cfg,
            Arc::clone(&ring) as Arc<dyn SignalPublisher>,
        )
        .unwrap(),
    );
    let broker = SignalBroker::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        ring,
        SignalConfig::default(),
    );
    let cold_store = Arc::new(FileColdStore::open(cold_dir.path()).unwrap());
    let tiering = TieringManager::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        Arc::clone(&cold_store) as Arc<dyn ColdStore>,
    );

    let payload = vec![0x42u8; 2048];
    let c1 = storage
        .put("tier_mesh", b"big", &payload, Ack::Flush, ExpectedVersion::Any)
        .unwrap();
    let original_ts = storage.get("tier_mesh", b"big").unwrap().unwrap().meta.ts_ms;

    // Subscribe AFTER initial put so we catch any spurious ColdRef/rehydrate signals.
    let mut sub = broker
        .subscribe_each_change("tier_mesh", Vec::new(), storage.epoch(), c1.seq)
        .unwrap();

    // Archive to cold tier AND rehydrate back to hot.
    let rep = tiering
        .archive_cold_entries(
            &TieringPolicy {
                namespace: "tier_mesh".to_owned(),
                key_prefix: Vec::new(),
                min_age_ms: 0,
                min_value_bytes: 512,
                max_entries_per_run: 10,
            },
            now_unix_ms().saturating_add(5_000),
            Ack::Flush,
        )
        .unwrap();
    assert_eq!(rep.archived, 1);
    assert_eq!(
        storage.get("tier_mesh", b"big").unwrap().unwrap().meta.ts_ms,
        original_ts,
        "ColdRef archival must preserve original logical timestamp_ms"
    );

    tiering
        .rehydrate_cold("tier_mesh", b"big", Ack::Flush)
        .unwrap();
    assert_eq!(
        storage.get("tier_mesh", b"big").unwrap().unwrap().meta.ts_ms,
        original_ts,
        "rehydrate_cold must preserve original logical timestamp_ms"
    );

    // M3: ZERO signals emitted during archive + rehydrate!
    let next_sig = tokio::time::timeout(Duration::from_millis(80), sub.next_event()).await;
    assert!(
        next_sig.is_err(),
        "tier archive/rehydrate must NOT emit SignalKind::Put events"
    );

    // M3: A peer delta at `original_ts + 1` MUST still win LWW over the archived/rehydrated record!
    let peer_val = b"peer-wins-lww".to_vec();
    let peer_delta = ReplicatedDelta {
        stamp: ReplicationStamp {
            timestamp_ms: original_ts + 1,
            origin_node_id: "peer-b".to_owned(),
            origin_seq: 1,
            value_sha256: Sha256::digest(&peer_val).into(),
        },
        namespace: "tier_mesh".to_owned(),
        key: b"big".to_vec(),
        op: MutationOp::Put {
            value: peer_val.clone(),
        },
        indexes: None,
        search_op: None,
    };
    let apply_res = storage
        .apply_replicated_delta("node-a", &peer_delta, Ack::Flush)
        .unwrap();
    assert!(
        apply_res.applied,
        "peer delta with ts = original_ts + 1 must beat archived/rehydrated record"
    );
    assert_eq!(
        storage.get("tier_mesh", b"big").unwrap().unwrap().value,
        peer_val
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn catchup_batch_byte_budget_and_outbound_ack_first_no_spurious_epoch_mismatch() {
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let storage_a = open_node_storage(dir_a.path());
    let storage_b = open_node_storage(dir_b.path());

    // Populate 6 records (~1.2 KiB each -> ~1.6 KiB base64 JSON) on Node A so a 4 KiB
    // payload cap forces multi-round byte-budgeted catchup batches without exceeding the cap!
    for i in 0..6u8 {
        let key = format!("k{i}");
        let val = vec![b'A' + i; 1200];
        storage_a
            .put("bud", key.as_bytes(), &val, Ack::Flush, ExpectedVersion::Any)
            .unwrap();
    }
    // Pre-mark published_seq on Node A & B so live Pub/Sub doesn't race with explicit catchup.
    let mut pre_a = [0u8; 16];
    pre_a[0..8].copy_from_slice(&storage_a.epoch().to_be_bytes());
    pre_a[8..16].copy_from_slice(&storage_a.latest_seq().to_be_bytes());
    storage_a
        .meta_put(membus::mesh::META_KEY_MESH_PUB_SEQ, &pre_a, true)
        .unwrap();

    storage_b
        .put("bud", b"from_b", b"hello-from-b", Ack::Flush, ExpectedVersion::Any)
        .unwrap();
    let mut pre_b = [0u8; 16];
    pre_b[0..8].copy_from_slice(&storage_b.epoch().to_be_bytes());
    pre_b[8..16].copy_from_slice(&storage_b.latest_seq().to_be_bytes());
    storage_b
        .meta_put(membus::mesh::META_KEY_MESH_PUB_SEQ, &pre_b, true)
        .unwrap();

    let port_a = free_tcp_port();
    let port_b = free_tcp_port();
    let cluster = format!("membus/test_budget_{port_a}");

    let node_a = MeshNode::start(
        MeshConfig {
            node_id: "node-a".to_owned(),
            cluster_prefix: cluster.clone(),
            listen_endpoints: vec![format!("tcp/127.0.0.1:{port_a}")],
            connect_endpoints: vec![format!("tcp/127.0.0.1:{port_b}")],
            publish_poll_interval_ms: 5_000,
            max_mesh_payload_bytes: 4096,
            ..Default::default()
        },
        Arc::clone(&storage_a) as Arc<dyn StorageEngine>,
        None,
    )
    .await
    .unwrap();

    let node_b = MeshNode::start(
        MeshConfig {
            node_id: "node-b".to_owned(),
            cluster_prefix: cluster,
            listen_endpoints: vec![format!("tcp/127.0.0.1:{port_b}")],
            connect_endpoints: vec![format!("tcp/127.0.0.1:{port_a}")],
            publish_poll_interval_ms: 5_000,
            max_mesh_payload_bytes: 4096,
            ..Default::default()
        },
        Arc::clone(&storage_b) as Arc<dyn StorageEngine>,
        None,
    )
    .await
    .unwrap();

    for _ in 0..40 {
        if node_b.query_peer_digest("node-a", "bud", b"").await.is_ok()
            && node_a.query_peer_digest("node-b", "bud", b"").await.is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // 1. Byte-budgeted multi-round catchup from Node A under strict 4096-byte payload cap.
    let stats_b = node_b.catchup_from_peer("node-a", Some(0)).await.unwrap();
    assert_eq!(stats_b.deltas_applied, 6);
    for i in 0..6u8 {
        let rec = storage_b
            .get("bud", format!("k{i}").as_bytes())
            .unwrap()
            .unwrap();
        assert_eq!(rec.value, vec![b'A' + i; 1200]);
    }

    // Also test oversized since_origin_seq clamping in `record_peer_outbound_ack` on Node A.
    let _ = node_b.catchup_from_peer("node-a", Some(999_999)).await.unwrap();
    let wm_on_a_before = node_a
        .peer_watermark("node-b")
        .unwrap()
        .expect("outbound ack record created on Node A");
    assert_eq!(wm_on_a_before.remote_epoch, 0);
    assert_eq!(
        wm_on_a_before.outbound_acked_seq,
        Some(storage_a.latest_seq()),
        "outbound_acked_seq must be clamped to storage.latest_seq()"
    );

    // 2. Now Node A pulls from Node B with `since_origin_seq = None` while its stored
    //    watermark has sentinel `remote_epoch == 0`: must NOT trigger spurious EpochMismatch!
    let stats_a = node_a.catchup_from_peer("node-b", None).await.unwrap();
    assert_eq!(stats_a.deltas_applied, 1);
    assert_eq!(
        storage_a.get("bud", b"from_b").unwrap().unwrap().value,
        b"hello-from-b"
    );
    let wm_on_a_after = node_a.peer_watermark("node-b").unwrap().unwrap();
    assert_eq!(wm_on_a_after.remote_epoch, storage_b.epoch());
    assert!(wm_on_a_after.last_origin_seq >= 1);

    node_a.shutdown().await.unwrap();
    node_b.shutdown().await.unwrap();
}
