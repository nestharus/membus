use std::net::SocketAddr;
use std::sync::Arc;

use membus::{
    Ack, Client, ExpectedVersion, MembusServer, NoopPublisher, RocksDbStorage, SearchConfig,
    SearchDocument, SearchEngine, SearchLogOp, SearchMode, SearchQuery, SearchVisibility,
    ServerConfig, SignalBroker, SignalConfig, SignalPublisher, SignalRing, StorageConfig,
    StorageEngine,
};
use tempfile::TempDir;

fn make_doc(
    ns: &str,
    id: &str,
    kind: &str,
    title: &str,
    tags: &[&str],
    body: &str,
    visibility: SearchVisibility,
) -> SearchDocument {
    SearchDocument {
        namespace: ns.to_owned(),
        doc_id: id.to_owned(),
        kind: kind.to_owned(),
        title: title.to_owned(),
        tags: tags.iter().map(|s| (*s).to_owned()).collect(),
        body: body.to_owned(),
        visibility,
    }
}

#[test]
fn tier0_ram_overlay_provides_zero_ms_search_before_tantivy_commit() {
    let dir = TempDir::new().expect("tempdir");
    let storage = Arc::new(
        RocksDbStorage::open(dir.path(), StorageConfig::default(), Arc::new(NoopPublisher))
            .expect("open"),
    );
    // Very long commit interval so background Tantivy commit will NOT run during Tier-0 checks.
    let engine = SearchEngine::open(
        dir.path(),
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        SearchConfig {
            commit_interval_ms: 60_000,
            max_buffered_docs: 10_000,
            ..SearchConfig::default()
        },
    )
    .expect("open search");

    engine
        .index_document(
            make_doc(
                "kb",
                "d1",
                "note",
                "Quantum Routing Engine",
                &["network"],
                "fast packet switching",
                SearchVisibility::Public,
            ),
            Ack::Fast,
        )
        .expect("d1");
    engine
        .index_document(
            make_doc(
                "kb",
                "d2",
                "note",
                "Packet Switch",
                &["quantum"],
                "general networking",
                SearchVisibility::Public,
            ),
            Ack::Fast,
        )
        .expect("d2");
    engine
        .index_document(
            make_doc(
                "kb",
                "d3",
                "note",
                "Field Report",
                &["misc"],
                "mentions quantum routing briefly",
                SearchVisibility::Public,
            ),
            Ack::Fast,
        )
        .expect("d3");

    // Prove Tantivy has NOT committed yet while Tier-0 overlay serves all 3 docs at 0 ms!
    assert_eq!(engine.committed_seq(), 0);
    assert_eq!(engine.overlay_len(), 3);

    let page = engine
        .search(&SearchQuery {
            namespace: Some("kb".to_owned()),
            kind: Some("note".to_owned()),
            query: "quantum".to_owned(),
            mode: SearchMode::Bm25,
            limit: 10,
        })
        .expect("tier0 bm25");
    let ids: Vec<&str> = page.hits.iter().map(|h| h.doc_id.as_str()).collect();
    assert_eq!(ids, vec!["d1", "d2", "d3"]);
    assert!(page.hits[0].score > page.hits[1].score);
    assert!(page.hits[1].score > page.hits[2].score);

    // Update d1 and delete d2 BEFORE Tantivy commits -> Tier-0 immediately reflects both!
    engine
        .index_document(
            make_doc(
                "kb",
                "d1",
                "note",
                "Classical Router",
                &["network"],
                "fast packet switching",
                SearchVisibility::Public,
            ),
            Ack::Fast,
        )
        .expect("update d1");
    engine.delete_document("kb", "d2", Ack::Fast).expect("del d2");

    let page_after = engine
        .search(&SearchQuery {
            namespace: Some("kb".to_owned()),
            kind: None,
            query: "quantum".to_owned(),
            mode: SearchMode::Bm25,
            limit: 10,
        })
        .expect("tier0 after update/delete");
    assert_eq!(page_after.hits.len(), 1);
    assert_eq!(page_after.hits[0].doc_id, "d3");

    // Flush to disk-persisted Tantivy -> overlay drains cleanly and Tantivy serves identical hits.
    let flushed_seq = engine.flush_sync().expect("flush_sync");
    assert!(flushed_seq > 0);
    assert_eq!(engine.overlay_len(), 0);

    let disk_page = engine
        .search(&SearchQuery {
            namespace: Some("kb".to_owned()),
            kind: None,
            query: "quantum".to_owned(),
            mode: SearchMode::Bm25,
            limit: 10,
        })
        .expect("disk bm25");
    assert_eq!(disk_page.hits.len(), 1);
    assert_eq!(disk_page.hits[0].doc_id, "d3");
}

#[test]
fn literal_substring_case_sensitive_unicode_and_short_queries() {
    let dir = TempDir::new().expect("tempdir");
    let storage = Arc::new(
        RocksDbStorage::open(dir.path(), StorageConfig::default(), Arc::new(NoopPublisher))
            .expect("open"),
    );
    let engine = SearchEngine::open(
        dir.path(),
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        SearchConfig {
            commit_interval_ms: 60_000,
            ..SearchConfig::default()
        },
    )
    .expect("open search");

    engine
        .index_document(
            make_doc(
                "code",
                "upper",
                "src",
                "Handler",
                &[],
                "fn AlphaBeta_Gamma() -> Result<()>",
                SearchVisibility::Public,
            ),
            Ack::Fast,
        )
        .expect("upper");
    engine
        .index_document(
            make_doc(
                "code",
                "lower",
                "src",
                "handler",
                &[],
                "fn alphabeta_gamma() -> Result<()>",
                SearchVisibility::Public,
            ),
            Ack::Fast,
        )
        .expect("lower");
    engine
        .index_document(
            make_doc(
                "code",
                "jp",
                "doc",
                "日本語ドキュメント",
                &[],
                "これは日本語テストデータです。",
                SearchVisibility::Public,
            ),
            Ack::Fast,
        )
        .expect("jp");
    engine
        .index_document(
            make_doc(
                "code",
                "tiny",
                "src",
                "xy",
                &[],
                "ab",
                SearchVisibility::Public,
            ),
            Ack::Fast,
        )
        .expect("tiny");

    let run_checks = |stage: &str| {
        let q = |needle: &str| {
            engine
                .search(&SearchQuery {
                    namespace: Some("code".to_owned()),
                    kind: None,
                    query: needle.to_owned(),
                    mode: SearchMode::LiteralSubstring,
                    limit: 10,
                })
                .expect("literal search")
                .hits
                .into_iter()
                .map(|h| h.doc_id)
                .collect::<Vec<_>>()
        };

        assert_eq!(q("Beta_Gam"), vec!["upper"], "{stage}: case-sensitive upper");
        assert_eq!(q("beta_gam"), vec!["lower"], "{stage}: case-sensitive lower");
        assert_eq!(q("テスト"), vec!["jp"], "{stage}: unicode trigram");
        assert_eq!(q("xy"), vec!["tiny"], "{stage}: <3 char short query");
        assert!(q("Beta_XYZ").is_empty(), "{stage}: false positive check");
    };

    // Check in Tier-0 RAM overlay BEFORE commit.
    run_checks("tier0");

    // Flush to disk-persisted Tantivy and check identical behavior on Tantivy segments.
    engine.flush_sync().expect("flush");
    assert_eq!(engine.overlay_len(), 0);
    run_checks("tantivy");
}

#[test]
fn visibility_isolation_sealed_and_metadata_only() {
    let dir = TempDir::new().expect("tempdir");
    let storage = Arc::new(
        RocksDbStorage::open(dir.path(), StorageConfig::default(), Arc::new(NoopPublisher))
            .expect("open"),
    );
    let engine = SearchEngine::open(
        dir.path(),
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        SearchConfig {
            commit_interval_ms: 60_000,
            ..SearchConfig::default()
        },
    )
    .expect("open search");

    engine
        .index_document(
            make_doc(
                "vault",
                "d_pub",
                "doc",
                "Public Doc",
                &["open"],
                "PUBLIC_MARKER_123",
                SearchVisibility::Public,
            ),
            Ack::Fast,
        )
        .expect("pub");
    engine
        .index_document(
            make_doc(
                "vault",
                "d_meta",
                "doc",
                "MetadataOnly Title",
                &["meta_tag"],
                "TOP_SECRET_BODY_TOKEN",
                SearchVisibility::MetadataOnly,
            ),
            Ack::Fast,
        )
        .expect("meta");
    engine
        .index_document(
            make_doc(
                "vault",
                "d_sealed",
                "doc",
                "Sealed Title",
                &["sealed_tag"],
                "SEALED_SECRET_TOKEN",
                SearchVisibility::Sealed,
            ),
            Ack::Fast,
        )
        .expect("sealed");

    for stage in ["tier0", "tantivy"] {
        if stage == "tantivy" {
            engine.flush_sync().expect("flush");
        }
        // MetadataOnly body MUST NEVER match BM25 or LiteralSubstring.
        for mode in [SearchMode::Bm25, SearchMode::LiteralSubstring] {
            let body_hits = engine
                .search(&SearchQuery {
                    namespace: Some("vault".to_owned()),
                    kind: None,
                    query: "TOP_SECRET_BODY_TOKEN".to_owned(),
                    mode,
                    limit: 10,
                })
                .expect("search");
            assert!(body_hits.hits.is_empty(), "{stage}/{mode:?}: body leaked");

            let sealed_hits = engine
                .search(&SearchQuery {
                    namespace: Some("vault".to_owned()),
                    kind: None,
                    query: "Sealed".to_owned(),
                    mode,
                    limit: 10,
                })
                .expect("search");
            assert!(sealed_hits.hits.is_empty(), "{stage}/{mode:?}: sealed leaked");
        }

        // MetadataOnly title & tags MUST match.
        let meta_title = engine
            .search(&SearchQuery {
                namespace: Some("vault".to_owned()),
                kind: None,
                query: "MetadataOnly".to_owned(),
                mode: SearchMode::Bm25,
                limit: 10,
            })
            .expect("meta title");
        assert_eq!(meta_title.hits.len(), 1);
        assert_eq!(meta_title.hits[0].doc_id, "d_meta");
    }

    // Overwriting previously Public doc with Sealed MUST scrub it immediately in Tier-0 AND Tantivy!
    engine
        .index_document(
            make_doc(
                "vault",
                "d_pub",
                "doc",
                "Now Sealed",
                &[],
                "PUBLIC_MARKER_123",
                SearchVisibility::Sealed,
            ),
            Ack::Fast,
        )
        .expect("seal d_pub");

    let after_seal_tier0 = engine
        .search(&SearchQuery {
            namespace: Some("vault".to_owned()),
            kind: None,
            query: "PUBLIC_MARKER_123".to_owned(),
            mode: SearchMode::LiteralSubstring,
            limit: 10,
        })
        .expect("after seal tier0");
    assert!(after_seal_tier0.hits.is_empty());

    engine.flush_sync().expect("flush seal");
    let after_seal_disk = engine
        .search(&SearchQuery {
            namespace: Some("vault".to_owned()),
            kind: None,
            query: "PUBLIC_MARKER_123".to_owned(),
            mode: SearchMode::LiteralSubstring,
            limit: 10,
        })
        .expect("after seal disk");
    assert!(after_seal_disk.hits.is_empty());
}

#[test]
fn restart_replay_from_cf_log_and_epoch_fenced_prefix_revocation() {
    let dir = TempDir::new().expect("tempdir");

    // Phase 1: Index initial docs, flush to Tantivy, then append one more search op directly
    // into CF_LOG simulating an abrupt crash after WAL commit before Tantivy flushed.
    {
        let storage = Arc::new(
            RocksDbStorage::open(dir.path(), StorageConfig::default(), Arc::new(NoopPublisher))
                .expect("open 1"),
        );
        let engine = SearchEngine::open(
            dir.path(),
            Arc::clone(&storage) as Arc<dyn StorageEngine>,
            SearchConfig::default(),
        )
        .expect("search 1");

        engine
            .index_document(
                make_doc(
                    "docs",
                    "tenant/a/1",
                    "doc",
                    "Alpha One",
                    &[],
                    "searchable_payload",
                    SearchVisibility::Public,
                ),
                Ack::Flush,
            )
            .expect("a1");
        engine
            .index_document(
                make_doc(
                    "docs",
                    "tenant/a/2",
                    "doc",
                    "Alpha Two",
                    &[],
                    "searchable_payload",
                    SearchVisibility::Public,
                ),
                Ack::Flush,
            )
            .expect("a2");
        engine
            .index_document(
                make_doc(
                    "docs",
                    "tenant/b/1",
                    "doc",
                    "Beta One",
                    &[],
                    "searchable_payload",
                    SearchVisibility::Public,
                ),
                Ack::Flush,
            )
            .expect("b1");
        engine.flush_sync().expect("flush 1");

        // Append directly to CF_LOG without touching SearchEngine (simulates crash before commit).
        storage
            .append_search_log_op(
                SearchLogOp::Index(make_doc(
                    "docs",
                    "tenant/a/3",
                    "doc",
                    "Alpha Three CrashReplay",
                    &[],
                    "searchable_payload",
                    SearchVisibility::Public,
                )),
                Ack::Flush,
            )
            .expect("append log op");
        drop(engine);
        storage.flush_wal(true).expect("wal flush");
    }

    // Phase 2: Reopen -> SearchEngine MUST deterministically replay `tenant/a/3` from CF_LOG.
    {
        let storage = Arc::new(
            RocksDbStorage::open(dir.path(), StorageConfig::default(), Arc::new(NoopPublisher))
                .expect("open 2"),
        );
        let engine = SearchEngine::open(
            dir.path(),
            Arc::clone(&storage) as Arc<dyn StorageEngine>,
            SearchConfig {
                commit_interval_ms: 60_000,
                ..SearchConfig::default()
            },
        )
        .expect("search 2");

        let replayed = engine
            .search(&SearchQuery {
                namespace: Some("docs".to_owned()),
                kind: None,
                query: "CrashReplay".to_owned(),
                mode: SearchMode::Bm25,
                limit: 10,
            })
            .expect("search replayed");
        assert_eq!(replayed.hits.len(), 1);
        assert_eq!(replayed.hits[0].doc_id, "tenant/a/3");

        // Revoke prefix `tenant/a/` -> instantly scrubs a/1, a/2, a/3 while preserving b/1.
        engine
            .revoke_search_prefix("docs", "tenant/a/", Ack::Flush)
            .expect("revoke");

        let after_revoke = engine
            .search(&SearchQuery {
                namespace: Some("docs".to_owned()),
                kind: None,
                query: "searchable_payload".to_owned(),
                mode: SearchMode::LiteralSubstring,
                limit: 10,
            })
            .expect("after revoke");
        assert_eq!(after_revoke.hits.len(), 1);
        assert_eq!(after_revoke.hits[0].doc_id, "tenant/b/1");

        // New doc under revoked prefix indexed AFTER revocation IS visible.
        engine
            .index_document(
                make_doc(
                    "docs",
                    "tenant/a/4",
                    "doc",
                    "Alpha Four PostRevoke",
                    &[],
                    "searchable_payload",
                    SearchVisibility::Public,
                ),
                Ack::Flush,
            )
            .expect("a4");
        engine.flush_sync().expect("flush 2");

        let mut post_ids: Vec<String> = engine
            .search(&SearchQuery {
                namespace: Some("docs".to_owned()),
                kind: None,
                query: "searchable_payload".to_owned(),
                mode: SearchMode::LiteralSubstring,
                limit: 10,
            })
            .expect("post revoke")
            .hits
            .into_iter()
            .map(|h| h.doc_id)
            .collect();
        post_ids.sort();
        assert_eq!(post_ids, vec!["tenant/a/4", "tenant/b/1"]);
    }

    // Phase 3: Reopen again and verify revoked docs stayed scrubbed on disk.
    let storage = Arc::new(
        RocksDbStorage::open(dir.path(), StorageConfig::default(), Arc::new(NoopPublisher))
            .expect("open 3"),
    );
    let engine = SearchEngine::open(
        dir.path(),
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        SearchConfig::default(),
    )
    .expect("search 3");
    let mut persisted_ids: Vec<String> = engine
        .search(&SearchQuery {
            namespace: Some("docs".to_owned()),
            kind: None,
            query: "searchable_payload".to_owned(),
            mode: SearchMode::LiteralSubstring,
            limit: 10,
        })
        .expect("persisted")
        .hits
        .into_iter()
        .map(|h| h.doc_id)
        .collect();
    persisted_ids.sort();
    assert_eq!(persisted_ids, vec!["tenant/a/4", "tenant/b/1"]);
}

#[tokio::test]
async fn put_indexed_with_search_op_is_immediately_searchable_via_client() {
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
    let search = Arc::new(
        SearchEngine::open(
            dir.path(),
            Arc::clone(&storage) as Arc<dyn StorageEngine>,
            SearchConfig {
                commit_interval_ms: 60_000,
                ..SearchConfig::default()
            },
        )
        .expect("search"),
    );
    let server = MembusServer::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        broker,
        ServerConfig::default(),
    )
    .with_search(search);
    let handle = server
        .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind");
    let client = Client::connect(handle.endpoint()).await.expect("connect");

    client
        .put_indexed(
            "notes",
            b"n1",
            b"raw-body",
            Ack::Fast,
            ExpectedVersion::Absent,
            None,
            Some(SearchLogOp::Index(make_doc(
                "notes",
                "n1",
                "memo",
                "Zero Latency Overlay",
                &["tier0"],
                "instant recall across tcp",
                SearchVisibility::Public,
            ))),
        )
        .await
        .expect("put_indexed");

    let page = client
        .search(SearchQuery {
            namespace: Some("notes".to_owned()),
            kind: Some("memo".to_owned()),
            query: "Overlay".to_owned(),
            mode: SearchMode::Bm25,
            limit: 10,
        })
        .await
        .expect("search");
    assert_eq!(page.hits.len(), 1);
    assert_eq!(page.hits[0].doc_id, "n1");

    drop(client);
    handle.shutdown().await;
}

#[test]
fn visibility_sanitization_keeps_sealed_and_metadata_only_body_out_of_cf_log() {
    let dir = TempDir::new().expect("tempdir");
    let storage = Arc::new(
        RocksDbStorage::open(dir.path(), StorageConfig::default(), Arc::new(NoopPublisher))
            .expect("open"),
    );

    // Invalid search_op rejected at the storage boundary.
    assert!(matches!(
        storage.append_search_log_op(
            SearchLogOp::Index(make_doc(
                "sec",
                "",
                "doc",
                "t",
                &[],
                "b",
                SearchVisibility::Public
            )),
            Ack::Fast
        ),
        Err(membus::MembusError::InvalidArgument(_))
    ));

    // Persist Sealed and MetadataOnly docs directly through storage APIs.
    storage
        .put_indexed(
            "sec",
            b"s1",
            b"kv",
            Ack::Fast,
            ExpectedVersion::Any,
            None,
            Some(&SearchLogOp::Index(make_doc(
                "sec",
                "s1",
                "doc",
                "TOP_SECRET_SEALED_TITLE",
                &["secret"],
                "TOP_SECRET_SEALED_BODY",
                SearchVisibility::Sealed,
            ))),
        )
        .expect("put sealed");

    storage
        .append_search_log_op(
            SearchLogOp::Index(make_doc(
                "sec",
                "m1",
                "doc",
                "Visible Meta Title",
                &["ok"],
                "TOP_SECRET_METADATA_BODY",
                SearchVisibility::MetadataOnly,
            )),
            Ack::Fast,
        )
        .expect("append meta");

    // Inspect persisted CF_LOG entries: Sealed became Delete; MetadataOnly body is empty!
    let log_page = storage.scan_search_log_after(0, 100).expect("scan log");
    assert_eq!(log_page.records.len(), 2);
    assert!(matches!(
        &log_page.records[0].op,
        SearchLogOp::Delete { namespace, doc_id } if namespace == "sec" && doc_id == "s1"
    ));
    match &log_page.records[1].op {
        SearchLogOp::Index(doc) => {
            assert_eq!(doc.doc_id, "m1");
            assert_eq!(doc.title, "Visible Meta Title");
            assert!(
                doc.body.is_empty(),
                "MetadataOnly body leaked into persisted CF_LOG!"
            );
        }
        other => panic!("unexpected op: {other:?}"),
    }
}

#[test]
fn out_of_order_ingest_and_same_seq_tx_revoke_prefix_determinism() {
    let dir = TempDir::new().expect("tempdir");
    let storage = Arc::new(
        RocksDbStorage::open(dir.path(), StorageConfig::default(), Arc::new(NoopPublisher))
            .expect("open"),
    );
    let engine = SearchEngine::open(
        dir.path(),
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        SearchConfig {
            commit_interval_ms: 60_000,
            ..SearchConfig::default()
        },
    )
    .expect("open search");

    // B2: Commit seq1 ("v1") then seq2 ("v2"), but call ingest_committed_op out of order (seq2 then seq1).
    let op1 = SearchLogOp::Index(make_doc(
        "ord",
        "d1",
        "doc",
        "OldVersionAlpha",
        &[],
        "old",
        SearchVisibility::Public,
    ));
    let op2 = SearchLogOp::Index(make_doc(
        "ord",
        "d1",
        "doc",
        "NewVersionBeta",
        &[],
        "new",
        SearchVisibility::Public,
    ));
    let c1 = storage
        .append_search_log_op(op1.clone(), Ack::Fast)
        .expect("c1");
    let c2 = storage
        .append_search_log_op(op2.clone(), Ack::Fast)
        .expect("c2");
    assert!(c2.seq > c1.seq);

    // Deliberately deliver higher seq BEFORE lower seq to Tier-0 overlay!
    engine
        .ingest_committed_op(c2.seq, 0, c2.epoch, op2)
        .expect("ingest c2");
    engine
        .ingest_committed_op(c1.seq, 0, c1.epoch, op1)
        .expect("ingest c1");

    // Tier-0 MUST keep NewVersionBeta and reject stale OldVersionAlpha!
    let hit_old = engine
        .search(&SearchQuery {
            namespace: Some("ord".to_owned()),
            kind: None,
            query: "OldVersionAlpha".to_owned(),
            mode: SearchMode::LiteralSubstring,
            limit: 10,
        })
        .expect("old");
    assert!(hit_old.hits.is_empty());

    let hit_new = engine
        .search(&SearchQuery {
            namespace: Some("ord".to_owned()),
            kind: None,
            query: "NewVersionBeta".to_owned(),
            mode: SearchMode::LiteralSubstring,
            limit: 10,
        })
        .expect("new");
    assert_eq!(hit_new.hits.len(), 1);

    // B4: Same-seq CommitIndexedTx containing [Index(p/old), RevokePrefix(p/), Index(p/new)].
    let tx_ops = [
        SearchLogOp::Index(make_doc(
            "same_tx",
            "p/old",
            "doc",
            "SameSeqMarker",
            &[],
            "old",
            SearchVisibility::Public,
        )),
        SearchLogOp::RevokePrefix {
            namespace: "same_tx".to_owned(),
            prefix: "p/".to_owned(),
        },
        SearchLogOp::Index(make_doc(
            "same_tx",
            "p/new",
            "doc",
            "SameSeqMarker",
            &[],
            "new",
            SearchVisibility::Public,
        )),
    ];
    let tx_commit = storage
        .commit_indexed_tx(
            &[],
            &[
                membus::IndexedMutation {
                    mutation: membus::WriteMutation {
                        namespace: "same_tx".to_owned(),
                        key: b"k0".to_vec(),
                        op: membus::MutationOp::Put {
                            value: b"0".to_vec(),
                        },
                    },
                    indexes: None,
                    search_op: Some(tx_ops[0].clone()),
                },
                membus::IndexedMutation {
                    mutation: membus::WriteMutation {
                        namespace: "same_tx".to_owned(),
                        key: b"k1".to_vec(),
                        op: membus::MutationOp::Put {
                            value: b"1".to_vec(),
                        },
                    },
                    indexes: None,
                    search_op: Some(tx_ops[1].clone()),
                },
                membus::IndexedMutation {
                    mutation: membus::WriteMutation {
                        namespace: "same_tx".to_owned(),
                        key: b"k2".to_vec(),
                        op: membus::MutationOp::Put {
                            value: b"2".to_vec(),
                        },
                    },
                    indexes: None,
                    search_op: Some(tx_ops[2].clone()),
                },
            ],
            Ack::Fast,
        )
        .expect("same_tx commit");
    for (idx, op) in tx_ops.into_iter().enumerate() {
        engine
            .ingest_committed_op(tx_commit.seq, idx as u32, tx_commit.epoch, op)
            .expect("ingest tx op");
    }

    // Both Tier-0 (before flush) and Tantivy (after flush) MUST keep only `p/new`!
    for _stage in 0..2 {
        let res = engine
            .search(&SearchQuery {
                namespace: Some("same_tx".to_owned()),
                kind: None,
                query: "SameSeqMarker".to_owned(),
                mode: SearchMode::LiteralSubstring,
                limit: 10,
            })
            .expect("same_tx search");
        assert_eq!(res.hits.len(), 1);
        assert_eq!(res.hits[0].doc_id, "p/new");
        engine.flush_sync().expect("flush");
    }
}

#[test]
fn bm25_recall_survives_mass_prefix_revocation_and_open_fails_closed_on_trimmed_log() {
    let dir = TempDir::new().expect("tempdir");
    {
        let storage = Arc::new(
            RocksDbStorage::open(dir.path(), StorageConfig::default(), Arc::new(NoopPublisher))
                .expect("open"),
        );
        let engine = SearchEngine::open(
            dir.path(),
            Arc::clone(&storage) as Arc<dyn StorageEngine>,
            SearchConfig {
                commit_interval_ms: 60_000,
                ..SearchConfig::default()
            },
        )
        .expect("open search");

        // 120 high-scoring docs under `revoked/` + 5 lower-scoring docs under `keep/`.
        for i in 0..120u32 {
            engine
                .index_document(
                    make_doc(
                        "recall",
                        &format!("revoked/{i:03}"),
                        "doc",
                        "HyperNeedle HyperNeedle HyperNeedle",
                        &["HyperNeedle"],
                        "HyperNeedle",
                        SearchVisibility::Public,
                    ),
                    Ack::Fast,
                )
                .expect("idx revoked");
        }
        for i in 0..5u32 {
            engine
                .index_document(
                    make_doc(
                        "recall",
                        &format!("keep/{i:03}"),
                        "doc",
                        "Quiet Document",
                        &[],
                        "mentions HyperNeedle once in body",
                        SearchVisibility::Public,
                    ),
                    Ack::Fast,
                )
                .expect("idx keep");
        }
        engine.flush_sync().expect("flush");

        // Revoke all 120 top-scoring docs (pending in Tier-0 before Tantivy commit!).
        engine
            .revoke_search_prefix("recall", "revoked/", Ack::Fast)
            .expect("revoke");

        // B4: FilteredBm25Collector must return ALL 5 `keep/` docs with zero recall collapse!
        let page = engine
            .search(&SearchQuery {
                namespace: Some("recall".to_owned()),
                kind: None,
                query: "HyperNeedle".to_owned(),
                mode: SearchMode::Bm25,
                limit: 5,
            })
            .expect("bm25 after mass revoke");
        assert_eq!(page.hits.len(), 5);
        for hit in &page.hits {
            assert!(hit.doc_id.starts_with("keep/"));
        }

        // Flush, append one more unindexed op, drop engine, then trim CF_LOG past checkpoint!
        engine.flush_sync().expect("flush 2");
        let saved_ckpt = engine.committed_seq();
        drop(engine);

        let extra = storage
            .append_search_log_op(
                SearchLogOp::Index(make_doc(
                    "recall",
                    "after_trim",
                    "doc",
                    "Extra",
                    &[],
                    "extra",
                    SearchVisibility::Public,
                )),
                Ack::Flush,
            )
            .expect("extra");
        // Trim CF_LOG strictly past `saved_ckpt` so `saved_ckpt < log_horizon_seq`.
        storage
            .trim_log_before(extra.seq.max(saved_ckpt + 2))
            .expect("trim");
    }

    // B3: Reopening SearchEngine when CF_LOG was trimmed past search checkpoint MUST fail closed with Gap!
    let storage = Arc::new(
        RocksDbStorage::open(dir.path(), StorageConfig::default(), Arc::new(NoopPublisher))
            .expect("reopen"),
    );
    let err = SearchEngine::open(
        dir.path(),
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        SearchConfig::default(),
    );
    assert!(matches!(err, Err(membus::MembusError::Gap { .. })));
}

#[test]
fn commit_worker_yields_cleanly_when_pending_hint_exceeds_settled_seq() {
    let dir = TempDir::new().expect("tempdir");
    let storage = Arc::new(
        RocksDbStorage::open(dir.path(), StorageConfig::default(), Arc::new(NoopPublisher))
            .expect("open"),
    );
    let engine = SearchEngine::open(
        dir.path(),
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        SearchConfig {
            commit_interval_ms: 10,
            max_buffered_docs: 1,
            ..SearchConfig::default()
        },
    )
    .expect("open search");

    // Ingest a higher-seq hint while settled_seq is still 0 (simulating an in-flight lower-seq writer).
    let epoch = storage.epoch();
    engine
        .ingest_committed_op(
            10,
            0,
            epoch,
            SearchLogOp::Index(make_doc(
                "spin",
                "d1",
                "doc",
                "YieldCheck",
                &[],
                "body",
                SearchVisibility::Public,
            )),
        )
        .expect("ingest ahead of settled_seq");

    // Worker yields (sleep 1ms) instead of 100% hot-spinning, and normal subsequent ops still commit cleanly.
    std::thread::sleep(std::time::Duration::from_millis(40));
    engine
        .index_document(
            make_doc(
                "spin",
                "d2",
                "doc",
                "NormalCommit",
                &[],
                "body",
                SearchVisibility::Public,
            ),
            Ack::Fast,
        )
        .expect("normal index");
    let hit = engine
        .search(&SearchQuery {
            namespace: Some("spin".to_owned()),
            kind: None,
            query: "NormalCommit".to_owned(),
            mode: SearchMode::Bm25,
            limit: 5,
        })
        .expect("search");
    assert_eq!(hit.hits.len(), 1);
    drop(engine);
}
