use std::fs;
use std::sync::Arc;

use membus::{
    create_full_backup, export_incremental_delta, restore_clean_room, verify_backup_dir, Ack,
    ExpectedVersion, GraphDirection, GraphEdgeEntry, IndexMutations, MembusError,
    NeighborQueryOptions, NoopPublisher, OrderedIndexEntry, OrderedQueryOptions, RocksDbStorage,
    SearchConfig, SearchDocument, SearchEngine, SearchLogOp, SearchMode, SearchQuery,
    SearchVisibility, StorageConfig, StorageEngine,
};
use tempfile::TempDir;

#[test]
fn full_backup_incremental_delta_and_clean_room_restore_with_tantivy() {
    let src_dir = TempDir::new().unwrap();
    let backup_dir = TempDir::new().unwrap();
    let delta_dir = TempDir::new().unwrap();
    let restore_dir = TempDir::new().unwrap();

    let cfg = StorageConfig {
        bg_sync_interval: None,
        ..Default::default()
    };
    let storage = Arc::new(
        RocksDbStorage::open(src_dir.path(), cfg, Arc::new(NoopPublisher)).expect("open src db"),
    );

    // Pre-checkpoint state: KV + secondary ordered/graph indexes + CAS blob + search doc.
    let pre_idx = IndexMutations {
        ordered: vec![OrderedIndexEntry {
            index_id: "queue".to_owned(),
            sort_key: b"0001".to_vec(),
            summary: b"first".to_vec(),
        }],
        edges: vec![GraphEdgeEntry {
            index_id: "graph".to_owned(),
            src_id: b"A".to_vec(),
            rel_label: b"links".to_vec(),
            dst_id: b"B".to_vec(),
            meta: b"m1".to_vec(),
        }],
    };
    let pre_doc = SearchDocument {
        namespace: "app".to_owned(),
        doc_id: "doc1".to_owned(),
        kind: "spec".to_owned(),
        title: "PreCheckpoint Document".to_owned(),
        tags: vec!["backup".to_owned()],
        body: "AlphaToken_1111 before checkpoint".to_owned(),
        visibility: SearchVisibility::Public,
    };
    storage
        .put_indexed(
            "app",
            b"k1",
            b"v1-pre",
            Ack::Flush,
            ExpectedVersion::Any,
            Some(&pre_idx),
            Some(&SearchLogOp::Index(pre_doc)),
        )
        .unwrap();
    let blob_sha = storage
        .put_cas_blob("app", b"immutable-artifact-bytes", Ack::Flush)
        .unwrap();

    let full_dest = backup_dir.path().join("snap_full");
    let manifest = create_full_backup(storage.as_ref(), &full_dest).unwrap();
    assert_eq!(manifest.db_epoch, storage.epoch());
    assert!(manifest.checkpoint_seq >= 1);
    assert!(!manifest.files.is_empty());
    assert!(manifest.logical_counts.kv_entries >= 1);
    assert!(manifest.logical_counts.blob_entries >= 1);

    // Post-checkpoint mutations exported as an incremental delta archive.
    let post_idx = IndexMutations {
        ordered: vec![OrderedIndexEntry {
            index_id: "queue".to_owned(),
            sort_key: b"0002".to_vec(),
            summary: b"second".to_vec(),
        }],
        edges: Vec::new(),
    };
    let post_doc = SearchDocument {
        namespace: "app".to_owned(),
        doc_id: "doc2".to_owned(),
        kind: "spec".to_owned(),
        title: "PostCheckpoint Incremental Delta".to_owned(),
        tags: vec!["delta".to_owned()],
        body: "OmegaToken_2222 after checkpoint".to_owned(),
        visibility: SearchVisibility::Public,
    };
    storage
        .put_indexed(
            "app",
            b"k2",
            b"v2-post",
            Ack::Flush,
            ExpectedVersion::Any,
            Some(&post_idx),
            Some(&SearchLogOp::Index(post_doc)),
        )
        .unwrap();
    storage
        .put("app", b"k1", b"v1-updated", Ack::Flush, ExpectedVersion::Any)
        .unwrap();

    let delta_file = delta_dir.path().join("delta_001.json");
    let bundle =
        export_incremental_delta(storage.as_ref(), manifest.checkpoint_seq, &delta_file).unwrap();
    assert_eq!(bundle.db_epoch, manifest.db_epoch);
    assert_eq!(bundle.entries.len(), 2);

    // Clean-room restore into fresh target directory.
    let target = restore_dir.path().join("restored_db");
    let report = restore_clean_room(&full_dest, &[delta_file], &target).unwrap();
    assert_eq!(report.db_epoch, manifest.db_epoch);
    assert_eq!(report.files_verified, manifest.files.len());
    assert_eq!(report.deltas_replayed, 2);

    // Open restored DB + SearchEngine and confirm every single record, index, blob, and doc.
    let rcfg = StorageConfig {
        bg_sync_interval: None,
        ..Default::default()
    };
    let restored_storage = Arc::new(
        RocksDbStorage::open(&target, rcfg, Arc::new(NoopPublisher)).expect("open restored db"),
    );
    assert_eq!(restored_storage.epoch(), manifest.db_epoch);
    assert_eq!(
        restored_storage.get("app", b"k1").unwrap().unwrap().value,
        b"v1-updated"
    );
    assert_eq!(
        restored_storage.get("app", b"k2").unwrap().unwrap().value,
        b"v2-post"
    );
    assert_eq!(
        restored_storage.get_cas_blob("app", &blob_sha).unwrap(),
        Some(b"immutable-artifact-bytes".to_vec())
    );

    let snap = restored_storage.open_snapshot().unwrap();
    let ord = restored_storage
        .query_ordered_index(&snap, "app", "queue", &OrderedQueryOptions::default())
        .unwrap();
    // `k1` was overwritten without indexes in delta #2, so `k2` remains in `queue`.
    assert_eq!(ord.entries.len(), 1);
    assert_eq!(ord.entries[0].primary_key, b"k2");
    let neighbors = restored_storage
        .query_neighbors(
            &snap,
            "app",
            "graph",
            b"A",
            &NeighborQueryOptions {
                direction: GraphDirection::Out,
                ..Default::default()
            },
        )
        .unwrap();
    assert!(neighbors.edges.is_empty());

    let restored_search = SearchEngine::open(
        &target,
        Arc::clone(&restored_storage) as Arc<dyn StorageEngine>,
        SearchConfig::default(),
    )
    .unwrap();
    let pre_hit = restored_search
        .search(&SearchQuery {
            query: "AlphaToken_1111".to_owned(),
            mode: SearchMode::LiteralSubstring,
            namespace: Some("app".to_owned()),
            kind: None,
            limit: 10,
        })
        .unwrap();
    assert_eq!(pre_hit.hits.len(), 1);
    assert_eq!(pre_hit.hits[0].doc_id, "doc1");

    let post_hit = restored_search
        .search(&SearchQuery {
            query: "OmegaToken_2222".to_owned(),
            mode: SearchMode::LiteralSubstring,
            namespace: Some("app".to_owned()),
            kind: None,
            limit: 10,
        })
        .unwrap();
    assert_eq!(post_hit.hits.len(), 1);
    assert_eq!(post_hit.hits[0].doc_id, "doc2");
}

#[test]
fn restore_fails_closed_on_tampered_checkpoint_and_delta_bundle() {
    let src_dir = TempDir::new().unwrap();
    let backup_dir = TempDir::new().unwrap();
    let delta_dir = TempDir::new().unwrap();
    let restore_dir = TempDir::new().unwrap();

    let cfg = StorageConfig {
        bg_sync_interval: None,
        ..Default::default()
    };
    let storage = RocksDbStorage::open(src_dir.path(), cfg, Arc::new(NoopPublisher)).unwrap();
    storage
        .put("t", b"k1", b"original", Ack::Flush, ExpectedVersion::Any)
        .unwrap();

    let full_dest = backup_dir.path().join("snap_tamper");
    let manifest = create_full_backup(&storage, &full_dest).unwrap();

    storage
        .put("t", b"k2", b"delta-val", Ack::Flush, ExpectedVersion::Any)
        .unwrap();
    let delta_file = delta_dir.path().join("delta.json");
    export_incremental_delta(&storage, manifest.checkpoint_seq, &delta_file).unwrap();

    // Tamper 1 byte in a checkpoint file and confirm verify/restore fail closed with Corruption.
    let target_file = full_dest.join("checkpoint").join(&manifest.files[0].rel_path);
    let original_bytes = fs::read(&target_file).unwrap();
    let mut tampered = original_bytes.clone();
    if tampered.is_empty() {
        tampered.push(0xAA);
    } else {
        tampered[0] ^= 0xFF;
    }
    fs::write(&target_file, &tampered).unwrap();

    let err = verify_backup_dir(&full_dest).expect_err("tampered checkpoint file must fail");
    assert!(matches!(err, MembusError::Corruption(_)), "got {err:?}");

    let err = restore_clean_room(
        &full_dest,
        std::slice::from_ref(&delta_file),
        &restore_dir.path().join("t1"),
    )
    .expect_err("restore must fail closed on tampered checkpoint");
    assert!(matches!(err, MembusError::Corruption(_)), "got {err:?}");
    assert!(
        !restore_dir.path().join("t1").exists(),
        "failed verification must not create partial target dir"
    );

    // Restore valid checkpoint file, then tamper one byte of the delta bundle payload.
    fs::write(&target_file, &original_bytes).unwrap();
    let mut bundle_json: serde_json::Value =
        serde_json::from_slice(&fs::read(&delta_file).unwrap()).unwrap();
    let first_byte = bundle_json["entries"][0]["mutations"][0]["op"]["Put"]["value"][0]
        .as_u64()
        .expect("put value byte");
    bundle_json["entries"][0]["mutations"][0]["op"]["Put"]["value"][0] =
        serde_json::json!(first_byte ^ 0xFF);
    fs::write(&delta_file, serde_json::to_vec_pretty(&bundle_json).unwrap()).unwrap();

    let err = restore_clean_room(&full_dest, &[delta_file], &restore_dir.path().join("t2"))
        .expect_err("restore must fail closed on tampered delta bundle");
    assert!(matches!(err, MembusError::Corruption(_)), "got {err:?}");
    // B5: Failed restore must leave neither target dir nor leftover staging dir!
    assert!(!restore_dir.path().join("t2").exists());
    let leftover: Vec<_> = fs::read_dir(restore_dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .collect();
    assert!(
        leftover.is_empty(),
        "failed restore must clean up staging dirs, found {leftover:?}"
    );
}

#[test]
fn backup_and_restore_after_safe_trim_log_before_preserves_search_and_cold_indexes() {
    use membus::types::now_unix_ms;
    use membus::{
        create_full_backup_with_search, ColdStore, FileColdStore, TieringManager, TieringPolicy,
    };

    let src_dir = TempDir::new().unwrap();
    let cold_dir = TempDir::new().unwrap();
    let backup_dir = TempDir::new().unwrap();
    let delta_dir = TempDir::new().unwrap();
    let restore_dir = TempDir::new().unwrap();

    let cfg = StorageConfig {
        bg_sync_interval: None,
        ..Default::default()
    };
    let storage = Arc::new(
        RocksDbStorage::open(src_dir.path(), cfg, Arc::new(NoopPublisher)).expect("open src"),
    );
    let search = SearchEngine::open(
        src_dir.path(),
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        SearchConfig::default(),
    )
    .unwrap();
    let cold_store = Arc::new(FileColdStore::open(cold_dir.path()).unwrap());
    let tiering = TieringManager::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        Arc::clone(&cold_store) as Arc<dyn ColdStore>,
    );

    // Pre-trim writes + indexed record + search doc.
    let idx = IndexMutations {
        ordered: vec![OrderedIndexEntry {
            index_id: "q".to_owned(),
            sort_key: b"001".to_vec(),
            summary: b"keep-me".to_vec(),
        }],
        edges: Vec::new(),
    };
    storage
        .put_indexed(
            "b1",
            b"tiered_key",
            &vec![0x77u8; 2048],
            Ack::Flush,
            ExpectedVersion::Any,
            Some(&idx),
            None,
        )
        .unwrap();
    let orig_tiered_ts = storage
        .get("b1", b"tiered_key")
        .unwrap()
        .unwrap()
        .meta
        .ts_ms;
    search
        .index_document(
            SearchDocument {
                namespace: "b1".to_owned(),
                doc_id: "pre-trim-doc".to_owned(),
                kind: "k".to_owned(),
                title: "PreTrimSearch".to_owned(),
                tags: Vec::new(),
                body: "BeforeLogTrim_8811".to_owned(),
                visibility: SearchVisibility::Public,
            },
            Ack::Flush,
        )
        .unwrap();
    search.flush_sync().unwrap();

    // Trim CF_LOG BEFORE taking backup so restored checkpoint has `log_horizon_seq > 0` (B1)!
    let pre_trim_seq = storage.latest_seq();
    storage
        .safe_trim_log_before(pre_trim_seq + 1, false)
        .unwrap();
    assert!(storage.log_horizon_seq() >= pre_trim_seq);

    // Full backup WITH Tantivy `search/` included in manifest!
    let full_dest = backup_dir.path().join("snap_trimmed");
    let manifest = create_full_backup_with_search(storage.as_ref(), Some(&search), &full_dest).unwrap();
    assert!(!manifest.search_files.is_empty());

    // Post-checkpoint: archive & rehydrate `tiered_key` (B3) + add new search doc!
    tiering
        .archive_cold_entries(
            &TieringPolicy {
                namespace: "b1".to_owned(),
                key_prefix: Vec::new(),
                min_age_ms: 0,
                min_value_bytes: 512,
                max_entries_per_run: 10,
            },
            now_unix_ms().saturating_add(5_000),
            Ack::Flush,
        )
        .unwrap();
    tiering
        .rehydrate_cold("b1", b"tiered_key", Ack::Flush)
        .unwrap();
    search
        .index_document(
            SearchDocument {
                namespace: "b1".to_owned(),
                doc_id: "post-ckpt-doc".to_owned(),
                kind: "k".to_owned(),
                title: "PostCkptSearch".to_owned(),
                tags: Vec::new(),
                body: "AfterCheckpoint_9922".to_owned(),
                visibility: SearchVisibility::Public,
            },
            Ack::Flush,
        )
        .unwrap();

    let delta_file = delta_dir.path().join("delta_post.json");
    export_incremental_delta(storage.as_ref(), manifest.checkpoint_seq, &delta_file).unwrap();

    // Clean-room restore MUST succeed even though `log_horizon_seq > 0`!
    let target = restore_dir.path().join("restored_trimmed");
    let report = restore_clean_room(&full_dest, &[delta_file], &target).unwrap();
    assert!(report.deltas_replayed >= 3);

    let rcfg = StorageConfig {
        bg_sync_interval: None,
        ..Default::default()
    };
    let restored_st = Arc::new(RocksDbStorage::open(&target, rcfg, Arc::new(NoopPublisher)).unwrap());
    // B3 + tier-swap replay fix: secondary index, original ts_ms, LWW identity, and `is_tier_swap`
    // mesh exclusion MUST all survive replayed ColdRef + rehydrate_cold deltas!
    let restored_rec = restored_st.get("b1", b"tiered_key").unwrap().unwrap();
    assert_eq!(
        restored_rec.meta.ts_ms, orig_tiered_ts,
        "restored tier-swapped record must preserve original logical ts_ms"
    );
    let delta_page = restored_st
        .scan_deltas_after(restored_st.log_horizon_seq(), 64)
        .unwrap();
    let replayed_tier_swaps = delta_page
        .entries
        .iter()
        .flat_map(|e| e.mutations.iter())
        .filter(|m| m.key == b"tiered_key" && m.is_tier_swap)
        .count();
    assert_eq!(
        replayed_tier_swaps, 2,
        "both archive and rehydrate deltas must stay tagged is_tier_swap=true after restore"
    );
    let snap = restored_st.open_snapshot().unwrap();
    let ord = restored_st
        .query_ordered_index(&snap, "b1", "q", &OrderedQueryOptions::default())
        .unwrap();
    assert_eq!(ord.entries.len(), 1);
    assert_eq!(ord.entries[0].primary_key, b"tiered_key");

    // B1: BOTH pre-trim and post-checkpoint Tantivy docs MUST be searchable!
    let restored_search = SearchEngine::open(
        &target,
        Arc::clone(&restored_st) as Arc<dyn StorageEngine>,
        SearchConfig::default(),
    )
    .unwrap();
    let h1 = restored_search
        .search(&SearchQuery {
            query: "BeforeLogTrim_8811".to_owned(),
            mode: SearchMode::LiteralSubstring,
            namespace: Some("b1".to_owned()),
            kind: None,
            limit: 5,
        })
        .unwrap();
    assert_eq!(h1.hits.len(), 1);
    let h2 = restored_search
        .search(&SearchQuery {
            query: "AfterCheckpoint_9922".to_owned(),
            mode: SearchMode::LiteralSubstring,
            namespace: Some("b1".to_owned()),
            kind: None,
            limit: 5,
        })
        .unwrap();
    assert_eq!(h2.hits.len(), 1);
}
