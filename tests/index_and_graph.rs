use std::net::SocketAddr;
use std::sync::Arc;

use membus::{
    Ack, BfsOptions, Client, ExpectedVersion, GraphDirection, GraphEdgeEntry, IndexMutations,
    IndexedMutation, MembusError, MembusServer, MutationOp, NeighborQueryOptions, NoopPublisher,
    OrderedIndexEntry, OrderedKeyFilter, OrderedQueryOptions, RocksDbStorage, ServerConfig,
    SignalBroker, SignalConfig, SignalPublisher, SignalRing, StorageConfig, StorageEngine,
    VersionGuard, WriteMutation,
};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

fn open_storage(dir: &TempDir) -> Arc<RocksDbStorage> {
    Arc::new(
        RocksDbStorage::open(dir.path(), StorageConfig::default(), Arc::new(NoopPublisher))
            .expect("open storage"),
    )
}

#[test]
fn ordered_partial_queue_insert_update_delete_leaves_zero_orphans() {
    let dir = TempDir::new().expect("tempdir");
    let storage = open_storage(&dir);

    // Emit 5 tasks into a partial "queue_ready" ordered index sorted by priority+id.
    let mut versions = std::collections::HashMap::new();
    for (id, prio) in [
        ("t5", 5u8),
        ("t2", 2u8),
        ("t4", 4u8),
        ("t1", 1u8),
        ("t3", 3u8),
    ] {
        let idx = IndexMutations {
            ordered: vec![OrderedIndexEntry {
                index_id: "queue_ready".to_owned(),
                sort_key: vec![prio, b':', id.as_bytes()[1]],
                summary: format!("summary:{id}").into_bytes(),
            }],
            edges: Vec::new(),
        };
        let commit = storage
            .put_indexed(
                "tasks",
                id.as_bytes(),
                format!("payload:{id}").as_bytes(),
                Ack::Fast,
                ExpectedVersion::Absent,
                Some(&idx),
                None,
            )
            .expect("put_indexed");
        versions.insert(id, commit.versions[0]);
    }

    // Verify full keyset-paginated scan order (limit=2 per page).
    let snap = storage.open_snapshot().expect("snapshot");
    let p1 = storage
        .query_ordered_index(
            &snap,
            "tasks",
            "queue_ready",
            &OrderedQueryOptions {
                filter: OrderedKeyFilter::All,
                after_cursor: None,
                limit: 2,
                max_bytes: 1024 * 1024,
            },
        )
        .expect("page 1");
    assert_eq!(p1.entries.len(), 2);
    assert!(p1.truncated);
    assert_eq!(p1.entries[0].primary_key, b"t1");
    assert_eq!(p1.entries[1].primary_key, b"t2");

    let p2 = storage
        .query_ordered_index(
            &snap,
            "tasks",
            "queue_ready",
            &OrderedQueryOptions {
                filter: OrderedKeyFilter::All,
                after_cursor: p1.next_cursor.clone(),
                limit: 2,
                max_bytes: 1024 * 1024,
            },
        )
        .expect("page 2");
    assert_eq!(p2.entries.len(), 2);
    assert!(p2.truncated);
    assert_eq!(p2.entries[0].primary_key, b"t3");
    assert_eq!(p2.entries[1].primary_key, b"t4");

    let p3 = storage
        .query_ordered_index(
            &snap,
            "tasks",
            "queue_ready",
            &OrderedQueryOptions {
                filter: OrderedKeyFilter::All,
                after_cursor: p2.next_cursor.clone(),
                limit: 2,
                max_bytes: 1024 * 1024,
            },
        )
        .expect("page 3");
    assert_eq!(p3.entries.len(), 1);
    assert!(!p3.truncated);
    assert_eq!(p3.entries[0].primary_key, b"t5");

    // Prefix & range filter checks.
    let prefix_page = storage
        .query_ordered_index(
            &snap,
            "tasks",
            "queue_ready",
            &OrderedQueryOptions {
                filter: OrderedKeyFilter::Prefix(vec![2, b':']),
                ..OrderedQueryOptions::default()
            },
        )
        .expect("prefix query");
    assert_eq!(prefix_page.entries.len(), 1);
    assert_eq!(prefix_page.entries[0].primary_key, b"t2");

    let range_page = storage
        .query_ordered_index(
            &snap,
            "tasks",
            "queue_ready",
            &OrderedQueryOptions {
                filter: OrderedKeyFilter::Range {
                    start: vec![2, b':'],
                    end: vec![4, b':'],
                },
                ..OrderedQueryOptions::default()
            },
        )
        .expect("range query");
    let range_keys: Vec<Vec<u8>> = range_page
        .entries
        .iter()
        .map(|h| h.primary_key.clone())
        .collect();
    assert_eq!(range_keys, vec![b"t2".to_vec(), b"t3".to_vec()]);
    drop(snap);

    // 1. Reprioritize t5 to priority 0 -> old priority-5 posting MUST be retracted.
    storage
        .put_indexed(
            "tasks",
            b"t5",
            b"payload:t5:urgent",
            Ack::Fast,
            ExpectedVersion::Version(versions["t5"]),
            Some(&IndexMutations {
                ordered: vec![OrderedIndexEntry {
                    index_id: "queue_ready".to_owned(),
                    sort_key: vec![0u8, b':', b'5'],
                    summary: b"urgent:t5".to_vec(),
                }],
                edges: Vec::new(),
            }),
            None,
        )
        .expect("reprioritize t5");

    // 2. Claim/complete t2 by updating with empty/None indexes -> drops out of partial queue.
    storage
        .put_indexed(
            "tasks",
            b"t2",
            b"payload:t2:done",
            Ack::Fast,
            ExpectedVersion::Version(versions["t2"]),
            None,
            None,
        )
        .expect("complete t2");

    // 3. Overwrite t3 via plain `put` -> prior secondary postings MUST still retract cleanly!
    storage
        .put(
            "tasks",
            b"t3",
            b"payload:t3:plain",
            Ack::Fast,
            ExpectedVersion::Version(versions["t3"]),
        )
        .expect("plain put t3");

    // 4. Delete t4 via `remove` -> posting MUST retract cleanly!
    storage
        .remove(
            "tasks",
            b"t4",
            Ack::Fast,
            ExpectedVersion::Version(versions["t4"]),
        )
        .expect("remove t4");

    // 5. Failed CAS tx must leak ZERO postings!
    let bad_tx = storage.commit_indexed_tx(
        &[VersionGuard {
            namespace: "tasks".to_owned(),
            key: b"t1".to_vec(),
            expected: ExpectedVersion::Version(999),
        }],
        &[IndexedMutation {
            mutation: WriteMutation {
                namespace: "tasks".to_owned(),
                key: b"t_ghost".to_vec(),
                op: MutationOp::Put {
                    value: b"ghost".to_vec(),
                },
            },
            indexes: Some(IndexMutations {
                ordered: vec![OrderedIndexEntry {
                    index_id: "queue_ready".to_owned(),
                    sort_key: vec![0u8, b':', b'0'],
                    summary: b"ghost".to_vec(),
                }],
                edges: Vec::new(),
            }),
            search_op: None,
        }],
        Ack::Fast,
    );
    assert!(matches!(bad_tx, Err(MembusError::Conflict { .. })));

    // Only t5 (prio 0) and t1 (prio 1) may remain in `queue_ready`.
    let snap2 = storage.open_snapshot().expect("snapshot 2");
    let final_page = storage
        .query_ordered_index(
            &snap2,
            "tasks",
            "queue_ready",
            &OrderedQueryOptions::default(),
        )
        .expect("final ordered query");
    let remaining: Vec<(Vec<u8>, Vec<u8>)> = final_page
        .entries
        .iter()
        .map(|h| (h.primary_key.clone(), h.summary.clone()))
        .collect();
    assert_eq!(
        remaining,
        vec![
            (b"t5".to_vec(), b"urgent:t5".to_vec()),
            (b"t1".to_vec(), b"summary:t1".to_vec()),
        ]
    );
}

#[test]
fn bidirectional_graph_edges_external_targets_and_multi_hop_bfs() {
    let dir = TempDir::new().expect("tempdir");
    let storage = open_storage(&dir);

    let emit = |node: &str, edges: Vec<(&str, &str, &str)>| {
        let edge_entries = edges
            .into_iter()
            .map(|(rel, dst, meta)| GraphEdgeEntry {
                index_id: "deps".to_owned(),
                src_id: node.as_bytes().to_vec(),
                rel_label: rel.as_bytes().to_vec(),
                dst_id: dst.as_bytes().to_vec(),
                meta: meta.as_bytes().to_vec(),
            })
            .collect();
        storage
            .put_indexed(
                "graph",
                node.as_bytes(),
                format!("node:{node}").as_bytes(),
                Ack::Fast,
                ExpectedVersion::Any,
                Some(&IndexMutations {
                    ordered: Vec::new(),
                    edges: edge_entries,
                }),
                None,
            )
            .expect("put node");
    };

    // a -> b, a -> e, a -> ext:artifact (external non-record target), b -> c, b -> f (rel="ref"), c -> d
    emit(
        "a",
        vec![
            ("links", "b", "ab"),
            ("links", "e", "ae"),
            ("cites", "ext:artifact", "a_ext"),
        ],
    );
    emit("b", vec![("links", "c", "bc"), ("ref", "f", "bf")]);
    emit("c", vec![("links", "d", "cd")]);

    let snap = storage.open_snapshot().expect("snap");

    // Forward neighbors of a with rel="links".
    let out_a = storage
        .query_neighbors(
            &snap,
            "graph",
            "deps",
            b"a",
            &NeighborQueryOptions {
                direction: GraphDirection::Out,
                rel_filter: Some(b"links".to_vec()),
                after_cursor: None,
                limit: 16,
            },
        )
        .expect("out_a");
    let out_dsts: Vec<Vec<u8>> = out_a.edges.iter().map(|e| e.dst_id.clone()).collect();
    assert_eq!(out_dsts, vec![b"b".to_vec(), b"e".to_vec()]);

    // Reverse neighbors of external non-record target `ext:artifact` MUST find `a`!
    let in_ext = storage
        .query_neighbors(
            &snap,
            "graph",
            "deps",
            b"ext:artifact",
            &NeighborQueryOptions {
                direction: GraphDirection::In,
                rel_filter: Some(b"cites".to_vec()),
                after_cursor: None,
                limit: 16,
            },
        )
        .expect("in_ext");
    assert_eq!(in_ext.edges.len(), 1);
    assert_eq!(in_ext.edges[0].src_id, b"a");
    assert_eq!(in_ext.edges[0].dst_id, b"ext:artifact");
    assert_eq!(in_ext.edges[0].direction, GraphDirection::In);

    // Bidirectional `Both` query on `b` returns both `b -> c`, `b -> f` (Out) and `a -> b` (In).
    let both_b = storage
        .query_neighbors(
            &snap,
            "graph",
            "deps",
            b"b",
            &NeighborQueryOptions {
                direction: GraphDirection::Both,
                rel_filter: None,
                after_cursor: None,
                limit: 16,
            },
        )
        .expect("both_b");
    assert_eq!(both_b.edges.len(), 3);
    assert!(both_b
        .edges
        .iter()
        .any(|e| e.direction == GraphDirection::In && e.src_id == b"a"));
    assert!(both_b
        .edges
        .iter()
        .any(|e| e.direction == GraphDirection::Out && e.dst_id == b"c"));

    // 2-hop BFS from `a` filtered to rel="links": visits a (0), b, e (1), c (2); does NOT reach d (3) or f (rel="ref").
    let bfs = storage
        .bfs_traverse(
            &snap,
            "graph",
            "deps",
            b"a",
            &BfsOptions {
                direction: GraphDirection::Out,
                rel_filter: Some(b"links".to_vec()),
                max_hops: 2,
                max_nodes: 100,
            },
        )
        .expect("bfs 2-hop");
    assert!(!bfs.truncated);
    assert!(bfs.visited_nodes.contains(&b"a".to_vec()));
    assert!(bfs.visited_nodes.contains(&b"b".to_vec()));
    assert!(bfs.visited_nodes.contains(&b"e".to_vec()));
    assert!(bfs.visited_nodes.contains(&b"c".to_vec()));
    assert!(!bfs.visited_nodes.contains(&b"d".to_vec()));
    assert!(!bfs.visited_nodes.contains(&b"f".to_vec()));
    assert_eq!(bfs.frontier_nodes, vec![b"c".to_vec()]);
    assert_eq!(bfs.traversed_edges.len(), 3);

    // Bounded max_nodes truncation.
    let truncated_bfs = storage
        .bfs_traverse(
            &snap,
            "graph",
            "deps",
            b"a",
            &BfsOptions {
                direction: GraphDirection::Out,
                rel_filter: Some(b"links".to_vec()),
                max_hops: 4,
                max_nodes: 2,
            },
        )
        .expect("truncated bfs");
    assert!(truncated_bfs.truncated);
    assert_eq!(truncated_bfs.visited_nodes.len(), 2);

    // Hard cap max_hops > 8 rejected.
    let err = storage.bfs_traverse(
        &snap,
        "graph",
        "deps",
        b"a",
        &BfsOptions {
            direction: GraphDirection::Out,
            rel_filter: None,
            max_hops: 9,
            max_nodes: 100,
        },
    );
    assert!(matches!(err, Err(MembusError::InvalidArgument(_))));
    drop(snap);

    // Update `a` dropping edge `a -> e` and `a -> ext:artifact`: BOTH forward and reverse vanish.
    emit("a", vec![("links", "b", "ab_v2")]);
    let snap2 = storage.open_snapshot().expect("snap2");
    let in_ext_after = storage
        .query_neighbors(
            &snap2,
            "graph",
            "deps",
            b"ext:artifact",
            &NeighborQueryOptions {
                direction: GraphDirection::In,
                ..NeighborQueryOptions::default()
            },
        )
        .expect("in_ext_after");
    assert!(in_ext_after.edges.is_empty());
    let in_e_after = storage
        .query_neighbors(
            &snap2,
            "graph",
            "deps",
            b"e",
            &NeighborQueryOptions {
                direction: GraphDirection::In,
                ..NeighborQueryOptions::default()
            },
        )
        .expect("in_e_after");
    assert!(in_e_after.edges.is_empty());
}

#[tokio::test]
async fn mvcc_snapshot_isolation_and_cas_blobs_over_binary_client() {
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

    // Seed indexed record + graph edge.
    client
        .put_indexed(
            "iso",
            b"doc1",
            b"v1",
            Ack::Fast,
            ExpectedVersion::Absent,
            Some(IndexMutations {
                ordered: vec![OrderedIndexEntry {
                    index_id: "by_ts".to_owned(),
                    sort_key: b"0001".to_vec(),
                    summary: b"v1".to_vec(),
                }],
                edges: vec![GraphEdgeEntry {
                    index_id: "rel".to_owned(),
                    src_id: b"doc1".to_vec(),
                    rel_label: b"parent".to_vec(),
                    dst_id: b"root".to_vec(),
                    meta: b"m1".to_vec(),
                }],
            }),
            None,
        )
        .await
        .expect("put_indexed");

    // Pin MVCC snapshot before mutating.
    let snap = client.open_snapshot().await.expect("open_snapshot");

    // Mutate after snapshot: delete doc1 (retracts ordered + edge).
    client
        .remove_indexed("iso", b"doc1", Ack::Fast, ExpectedVersion::Version(1), None)
        .await
        .expect("remove_indexed");

    // Pinned snapshot STILL observes pre-delete ordered index & graph edge.
    let snap_ord = snap
        .query_ordered_index("iso", "by_ts", OrderedQueryOptions::default())
        .await
        .expect("snap ord");
    assert_eq!(snap_ord.entries.len(), 1);
    assert_eq!(snap_ord.entries[0].primary_key, b"doc1");

    let snap_bfs = snap
        .bfs_traverse("iso", "rel", b"doc1", BfsOptions::default())
        .await
        .expect("snap bfs");
    assert_eq!(snap_bfs.visited_nodes.len(), 2);

    // Unpinned fresh query sees empty indexes.
    let live_ord = client
        .query_ordered_index("iso", "by_ts", OrderedQueryOptions::default(), None)
        .await
        .expect("live ord");
    assert!(live_ord.entries.is_empty());

    snap.close().await.expect("close snap");

    // Namespace-scoped CAS blobs (including > 32 KiB hitting BlobDB).
    let payload = vec![0x5Cu8; 40 * 1024];
    let expected_sha: [u8; 32] = Sha256::digest(&payload).into();
    let digest1 = client
        .put_cas_blob("artifacts", &payload, Ack::Flush)
        .await
        .expect("put_cas_blob");
    assert_eq!(digest1, expected_sha);

    // Idempotent repeat put returns identical digest.
    let digest2 = client
        .put_cas_blob("artifacts", &payload, Ack::Fast)
        .await
        .expect("idempotent put_cas_blob");
    assert_eq!(digest2, expected_sha);

    assert!(client
        .has_cas_blob("artifacts", expected_sha)
        .await
        .expect("has"));
    assert_eq!(
        client
            .get_cas_blob("artifacts", expected_sha)
            .await
            .expect("get"),
        Some(payload)
    );

    // Isolated across namespaces.
    assert!(!client
        .has_cas_blob("other_ns", expected_sha)
        .await
        .expect("other has"));
    assert_eq!(
        client
            .get_cas_blob("other_ns", expected_sha)
            .await
            .expect("other get"),
        None
    );

    drop(client);
    handle.shutdown().await;
}

#[test]
fn graph_edge_pk_ownership_prevents_sequential_and_intra_tx_clobbering() {
    let dir = TempDir::new().expect("tempdir");
    let storage = open_storage(&dir);

    let shared_edge = GraphEdgeEntry {
        index_id: "deps".to_owned(),
        src_id: b"u".to_vec(),
        rel_label: b"links".to_vec(),
        dst_id: b"v".to_vec(),
        meta: b"m".to_vec(),
    };

    // Two distinct primary keys emit the same logical edge `u -> v`.
    storage
        .put_indexed(
            "g",
            b"pk1",
            b"1",
            Ack::Fast,
            ExpectedVersion::Absent,
            Some(&IndexMutations {
                ordered: Vec::new(),
                edges: vec![shared_edge.clone()],
            }),
            None,
        )
        .expect("pk1");
    storage
        .put_indexed(
            "g",
            b"pk2",
            b"2",
            Ack::Fast,
            ExpectedVersion::Absent,
            Some(&IndexMutations {
                ordered: Vec::new(),
                edges: vec![shared_edge.clone()],
            }),
            None,
        )
        .expect("pk2");

    // Deleting pk1 sequentially must NOT delete pk2's edge!
    storage
        .remove("g", b"pk1", Ack::Fast, ExpectedVersion::Any)
        .expect("remove pk1");
    let snap = storage.open_snapshot().expect("snap");
    let out = storage
        .query_neighbors(&snap, "g", "deps", b"u", &NeighborQueryOptions::default())
        .expect("neighbors");
    assert_eq!(out.edges.len(), 1);
    assert_eq!(out.edges[0].dst_id, b"v");
    drop(snap);

    // Intra-tx `[Put(pk3, E), Remove(pk2)]` must also leave pk3's edge intact!
    storage
        .commit_indexed_tx(
            &[],
            &[
                IndexedMutation {
                    mutation: WriteMutation {
                        namespace: "g".to_owned(),
                        key: b"pk3".to_vec(),
                        op: MutationOp::Put {
                            value: b"3".to_vec(),
                        },
                    },
                    indexes: Some(IndexMutations {
                        ordered: Vec::new(),
                        edges: vec![shared_edge],
                    }),
                    search_op: None,
                },
                IndexedMutation {
                    mutation: WriteMutation {
                        namespace: "g".to_owned(),
                        key: b"pk2".to_vec(),
                        op: MutationOp::Remove,
                    },
                    indexes: None,
                    search_op: None,
                },
            ],
            Ack::Fast,
        )
        .expect("intra-tx handoff");

    let snap2 = storage.open_snapshot().expect("snap2");
    let out2 = storage
        .query_neighbors(&snap2, "g", "deps", b"u", &NeighborQueryOptions::default())
        .expect("neighbors2");
    assert_eq!(out2.edges.len(), 1);
    assert_eq!(out2.edges[0].dst_id, b"v");
}

#[test]
fn blob_domain_separation_and_idempotent_cas_flush() {
    let dir = TempDir::new().expect("tempdir");
    let storage = open_storage(&dir);

    let cas_payload = b"cas-blob-content";
    let digest = storage
        .put_cas_blob("ns1", cas_payload, Ack::Fast)
        .expect("put_cas_blob fast");

    // Craft a legacy raw blob whose raw digest matches `[ns_len][ns][sha256]`.
    let mut colliding_raw_digest = Vec::new();
    colliding_raw_digest.extend_from_slice(&(3u32).to_be_bytes());
    colliding_raw_digest.extend_from_slice(b"ns1");
    colliding_raw_digest.extend_from_slice(&digest);
    storage
        .put_blob(&colliding_raw_digest, b"raw-blob-content", Ack::Fast)
        .expect("put_blob");

    // Domain prefixes keep legacy raw blobs and CAS blobs completely isolated.
    assert_eq!(
        storage
            .get_cas_blob("ns1", &digest)
            .expect("get_cas")
            .as_deref(),
        Some(cas_payload.as_slice())
    );
    assert_eq!(
        storage
            .get_blob(&colliding_raw_digest)
            .expect("get_raw")
            .as_deref(),
        Some(b"raw-blob-content".as_slice())
    );

    // Idempotent Ack::Flush hit flushes WAL and returns identical digest.
    let digest_flush = storage
        .put_cas_blob("ns1", cas_payload, Ack::Flush)
        .expect("put_cas_blob idempotent flush");
    assert_eq!(digest_flush, digest);
}

#[test]
fn bfs_max_nodes_bounds_traversed_edges_and_frontier_nodes() {
    let dir = TempDir::new().expect("tempdir");
    let storage = open_storage(&dir);

    // High-degree star hub -> 40 distinct neighbors.
    let edges: Vec<GraphEdgeEntry> = (0..40u32)
        .map(|i| GraphEdgeEntry {
            index_id: "star".to_owned(),
            src_id: b"hub".to_vec(),
            rel_label: b"e".to_vec(),
            dst_id: format!("n{i:03}").into_bytes(),
            meta: b"m".to_vec(),
        })
        .collect();
    storage
        .put_indexed(
            "g",
            b"hub",
            b"hub",
            Ack::Fast,
            ExpectedVersion::Absent,
            Some(&IndexMutations {
                ordered: Vec::new(),
                edges,
            }),
            None,
        )
        .expect("seed hub");

    let snap = storage.open_snapshot().expect("snap");
    let bfs = storage
        .bfs_traverse(
            &snap,
            "g",
            "star",
            b"hub",
            &BfsOptions {
                direction: GraphDirection::Out,
                rel_filter: None,
                max_hops: 2,
                max_nodes: 5,
            },
        )
        .expect("bfs");
    assert!(bfs.truncated);
    assert_eq!(bfs.visited_nodes.len(), 5);
    assert!(bfs.traversed_edges.len() <= 4);
    assert!(bfs.frontier_nodes.len() <= 5);
    for edge in &bfs.traversed_edges {
        assert!(bfs.visited_nodes.contains(&edge.dst_id));
    }
    for f in &bfs.frontier_nodes {
        assert!(bfs.visited_nodes.contains(f));
    }
}

#[test]
fn query_neighbors_rejects_invalid_and_mismatched_direction_cursors() {
    let dir = TempDir::new().expect("tempdir");
    let storage = open_storage(&dir);
    let snap = storage.open_snapshot().expect("snap");

    // Invalid direction tag byte.
    let err_bad = storage.query_neighbors(
        &snap,
        "g",
        "deps",
        b"u",
        &NeighborQueryOptions {
            direction: GraphDirection::Both,
            rel_filter: None,
            after_cursor: Some(vec![b'X', 0x00]),
            limit: 10,
        },
    );
    assert!(matches!(err_bad, Err(MembusError::InvalidArgument(_))));

    // Reverse cursor supplied to Out query.
    let err_out = storage.query_neighbors(
        &snap,
        "g",
        "deps",
        b"u",
        &NeighborQueryOptions {
            direction: GraphDirection::Out,
            rel_filter: None,
            after_cursor: Some(vec![b'R', 0x00]),
            limit: 10,
        },
    );
    assert!(matches!(err_out, Err(MembusError::InvalidArgument(_))));

    // Forward cursor supplied to In query.
    let err_in = storage.query_neighbors(
        &snap,
        "g",
        "deps",
        b"u",
        &NeighborQueryOptions {
            direction: GraphDirection::In,
            rel_filter: None,
            after_cursor: Some(vec![b'F', 0x00]),
            limit: 10,
        },
    );
    assert!(matches!(err_in, Err(MembusError::InvalidArgument(_))));
}
