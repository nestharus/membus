use std::fs;
use std::sync::Arc;

use membus::types::now_unix_ms;
use membus::{
    create_full_backup, Ack, ColdStore, ExpectedVersion, FileColdStore, IndexMutations,
    MembusError, NoopPublisher, OrderedIndexEntry, OrderedQueryOptions, ReadMode, RocksDbStorage,
    SearchConfig, SearchDocument, SearchEngine, StorageConfig, StorageEngine, TieringManager,
    TieringPolicy,
};
use tempfile::TempDir;

#[test]
fn archive_cold_entries_read_modes_rehydrate_and_index_preservation() {
    let db_dir = TempDir::new().unwrap();
    let cold_dir = TempDir::new().unwrap();

    let cfg = StorageConfig {
        bg_sync_interval: None,
        ..Default::default()
    };
    let storage = Arc::new(
        RocksDbStorage::open(db_dir.path(), cfg, Arc::new(NoopPublisher)).expect("open db"),
    );
    let cold_store = Arc::new(FileColdStore::open(cold_dir.path()).unwrap());
    let tiering = TieringManager::new(Arc::clone(&storage) as Arc<dyn StorageEngine>, cold_store);

    let big_payload = vec![0x5Au8; 4096];
    let idx = IndexMutations {
        ordered: vec![OrderedIndexEntry {
            index_id: "by_age".to_owned(),
            sort_key: b"001".to_vec(),
            summary: b"cold-candidate".to_vec(),
        }],
        edges: Vec::new(),
    };
    storage
        .put_indexed(
            "archive_ns",
            b"doc/large",
            &big_payload,
            Ack::Flush,
            ExpectedVersion::Any,
            Some(&idx),
            None,
        )
        .unwrap();
    storage
        .put(
            "archive_ns",
            b"doc/small",
            b"tiny",
            Ack::Flush,
            ExpectedVersion::Any,
        )
        .unwrap();

    let future_now = now_unix_ms().saturating_add(60_000);
    let report = tiering
        .archive_cold_entries(
            &TieringPolicy {
                namespace: "archive_ns".to_owned(),
                key_prefix: b"doc/".to_vec(),
                min_age_ms: 1_000,
                min_value_bytes: 1024,
                max_entries_per_run: 100,
            },
            future_now,
            Ack::Flush,
        )
        .unwrap();
    assert_eq!(report.scanned, 2);
    assert_eq!(report.archived, 1);
    assert_eq!(report.bytes_offloaded, 4096);

    // HotOnly returns compact ColdRef lineage tombstone.
    let hot_view = tiering
        .get("archive_ns", b"doc/large", ReadMode::HotOnly)
        .unwrap()
        .unwrap();
    assert!(TieringManager::is_cold_ref(&hot_view));
    assert!(hot_view.value.len() < 512);
    let cold_ref = TieringManager::decode_cold_ref(&hot_view).unwrap();
    assert_eq!(cold_ref.original_bytes, 4096);
    assert!(cold_ref.tier_uri.starts_with("file-cold://"));

    // Secondary ordered index MUST still be preserved after cold archival!
    let snap = storage.open_snapshot().unwrap();
    let ord = storage
        .query_ordered_index(&snap, "archive_ns", "by_age", &OrderedQueryOptions::default())
        .unwrap();
    assert_eq!(ord.entries.len(), 1);
    assert_eq!(ord.entries[0].primary_key, b"doc/large");

    // Small record stayed hot.
    let small = tiering
        .get("archive_ns", b"doc/small", ReadMode::HotOnly)
        .unwrap()
        .unwrap();
    assert!(!TieringManager::is_cold_ref(&small));
    assert_eq!(small.value, b"tiny");

    // HydrateCold transparently fetches and SHA-256 verifies original bytes.
    let hydrated = tiering
        .get("archive_ns", b"doc/large", ReadMode::HydrateCold)
        .unwrap()
        .unwrap();
    assert!(!TieringManager::is_cold_ref(&hydrated));
    assert_eq!(hydrated.value, big_payload);

    // Explicit rehydrate restores hot bytes into CF_KV and clears ColdRef flag.
    let rehydrated = tiering
        .rehydrate_cold("archive_ns", b"doc/large", Ack::Flush)
        .unwrap();
    assert!(rehydrated.is_some());
    let after_hot = tiering
        .get("archive_ns", b"doc/large", ReadMode::HotOnly)
        .unwrap()
        .unwrap();
    assert!(!TieringManager::is_cold_ref(&after_hot));
    assert_eq!(after_hot.value, big_payload);
}

#[test]
fn cold_tamper_fails_closed_corruption_and_safe_log_trim_floor() {
    let db_dir = TempDir::new().unwrap();
    let cold_dir = TempDir::new().unwrap();
    let backup_dir = TempDir::new().unwrap();

    let cfg = StorageConfig {
        bg_sync_interval: None,
        ..Default::default()
    };
    let storage = Arc::new(
        RocksDbStorage::open(db_dir.path(), cfg, Arc::new(NoopPublisher)).expect("open db"),
    );
    let cold_store = Arc::new(FileColdStore::open(cold_dir.path()).unwrap());
    let tiering = TieringManager::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        Arc::clone(&cold_store) as Arc<dyn ColdStore>,
    );
    let search = SearchEngine::open(
        db_dir.path(),
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        SearchConfig::default(),
    )
    .unwrap();

    let payload = vec![0x33u8; 2048];
    storage
        .put("t", b"k1", &payload, Ack::Flush, ExpectedVersion::Any)
        .unwrap();
    tiering
        .archive_cold_entries(
            &TieringPolicy {
                namespace: "t".to_owned(),
                key_prefix: Vec::new(),
                min_age_ms: 0,
                min_value_bytes: 512,
                max_entries_per_run: 10,
            },
            now_unix_ms().saturating_add(1_000),
            Ack::Flush,
        )
        .unwrap();

    // Tamper the cold blob file on disk -> HydrateCold & rehydrate_cold MUST fail closed!
    let hot_rec = tiering.get("t", b"k1", ReadMode::HotOnly).unwrap().unwrap();
    let cold_ref = TieringManager::decode_cold_ref(&hot_rec).unwrap();
    let obj_path = cold_store.object_path(&cold_ref.cold_sha256);
    let mut corrupted = fs::read(&obj_path).unwrap();
    corrupted[0] ^= 0xFF;
    fs::write(&obj_path, corrupted).unwrap();

    let err = tiering
        .get("t", b"k1", ReadMode::HydrateCold)
        .expect_err("tampered cold object must fail closed with Corruption");
    assert!(matches!(err, MembusError::Corruption(_)), "got {err:?}");

    let err = tiering
        .rehydrate_cold("t", b"k1", Ack::Flush)
        .expect_err("rehydrate must fail closed on tampered cold object");
    assert!(matches!(err, MembusError::Corruption(_)), "got {err:?}");

    // Safe log-trimming floor test: backup watermark pins seq, then more writes occur.
    let manifest = create_full_backup(storage.as_ref(), &backup_dir.path().join("snap1")).unwrap();
    let _ = search.index_document(
        SearchDocument {
            namespace: "t".to_owned(),
            doc_id: "d1".to_owned(),
            kind: "k".to_owned(),
            title: "Hello".to_owned(),
            tags: Vec::new(),
            body: "World".to_owned(),
            visibility: membus::SearchVisibility::Public,
        },
        Ack::Flush,
    );
    storage
        .put("t", b"k2", b"newer", Ack::Flush, ExpectedVersion::Any)
        .unwrap();
    storage
        .put("t", b"k3", b"newest", Ack::Flush, ExpectedVersion::Any)
        .unwrap();

    let trim_target = manifest.checkpoint_seq + 2;
    let err = tiering
        .safe_trim_log_before(trim_target, false)
        .expect_err("safe trim must block below active backup/search watermarks");
    assert!(matches!(err, MembusError::Conflict { .. }), "got {err:?}");

    // Force override succeeds when explicitly requested.
    tiering.safe_trim_log_before(trim_target, true).unwrap();
    assert_eq!(storage.log_horizon_seq(), trim_target - 1);
}

#[test]
fn concurrent_same_process_cold_put_and_hydrated_crc32() {
    use membus::storage::codec::compute_record_crc32;
    use sha2::{Digest, Sha256};

    let db_dir = TempDir::new().unwrap();
    let cold_dir = TempDir::new().unwrap();
    let cold_store = Arc::new(FileColdStore::open(cold_dir.path()).unwrap());

    // B4: 16 concurrent threads writing the exact same cold object in the same process.
    let shared_bytes = Arc::new(vec![0xABu8; 8192]);
    let sha: [u8; 32] = Sha256::digest(&*shared_bytes).into();
    let mut handles = Vec::new();
    for _ in 0..16 {
        let cs = Arc::clone(&cold_store);
        let b = Arc::clone(&shared_bytes);
        handles.push(std::thread::spawn(move || cs.put_object(&sha, &b)));
    }
    for h in handles {
        h.join().unwrap().expect("concurrent put_object must never collide");
    }
    assert_eq!(cold_store.get_object(&sha).unwrap(), Some((*shared_bytes).clone()));

    // Hardening: HydrateCold must recompute meta.crc32 over the hydrated payload bytes.
    let cfg = StorageConfig {
        bg_sync_interval: None,
        ..Default::default()
    };
    let storage = Arc::new(RocksDbStorage::open(db_dir.path(), cfg, Arc::new(NoopPublisher)).unwrap());
    let tiering = TieringManager::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        Arc::clone(&cold_store) as Arc<dyn ColdStore>,
    );
    storage
        .put("crc_ns", b"k", &shared_bytes, Ack::Flush, ExpectedVersion::Any)
        .unwrap();
    tiering
        .archive_cold_entries(
            &TieringPolicy {
                namespace: "crc_ns".to_owned(),
                key_prefix: Vec::new(),
                min_age_ms: 0,
                min_value_bytes: 1024,
                max_entries_per_run: 10,
            },
            now_unix_ms().saturating_add(5_000),
            Ack::Flush,
        )
        .unwrap();
    let hydrated = tiering
        .get("crc_ns", b"k", ReadMode::HydrateCold)
        .unwrap()
        .unwrap();
    let expected_crc = compute_record_crc32(
        hydrated.meta.version,
        hydrated.meta.flags,
        hydrated.meta.ts_ms,
        &hydrated.value,
    );
    assert_eq!(hydrated.meta.crc32, expected_crc);
}
