use std::sync::Arc;

use membus::cli::diag::{inspect_data_dir, run_diag, verify_data_dir, DiagCommand};
use membus::cli::{parse_args, Command};
use membus::storage::{ALL_COLUMN_FAMILIES, CF_BLOB, CF_IDX_ORD};
use membus::{
    create_full_backup, Ack, CollectionCatalog, CollectionDescriptor, CompatibilityMode,
    ExpectedVersion, IndexMutations, LeaseManager, MembusError, MigrationState, NoopPublisher,
    OrderedIndexEntry, RetentionPolicy, RocksDbStorage, SearchConfig, SearchDocument, SearchEngine,
    SearchVisibility, StorageConfig, StorageEngine, StoreProfile,
};
use rocksdb::{Options, DB, DEFAULT_COLUMN_FAMILY_NAME};
use sha2::{Digest, Sha256};
use tempfile::TempDir;

#[test]
fn diag_inspect_works_against_live_open_and_closed_primary_without_secret_tokens() {
    let dir = TempDir::new().unwrap();

    let cfg = StorageConfig {
        bg_sync_interval: None,
        ..Default::default()
    };
    let storage = Arc::new(
        RocksDbStorage::open(dir.path(), cfg, Arc::new(NoopPublisher)).expect("open primary"),
    );
    let leases = LeaseManager::new(Arc::clone(&storage) as Arc<dyn StorageEngine>);
    let catalog = CollectionCatalog::new(Arc::clone(&storage) as Arc<dyn StorageEngine>);
    let search = SearchEngine::open(
        dir.path(),
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        SearchConfig::default(),
    )
    .unwrap();

    let schema_bytes = b"{\"type\":\"object\"}".to_vec();
    let schema_sha256: [u8; 32] = Sha256::digest(&schema_bytes).into();
    catalog
        .register_collection(
            None,
            CollectionDescriptor {
                namespace: "diag_ns".to_owned(),
                store_profile: StoreProfile::Mixed,
                schema_version: 1,
                schema_sha256,
                schema_bytes,
                declared_indexes: Vec::new(),
                retention_policy: RetentionPolicy::default(),
                migration_state: MigrationState::Active,
                compatible_versions: Vec::new(),
                compatibility_mode: CompatibilityMode::Strict,
            },
        )
        .unwrap();

    let grant = leases
        .acquire_lease("diag_ns", "primary-lock", "worker-1", 60_000)
        .unwrap();
    storage
        .put("diag_ns", b"k1", b"val1", Ack::Flush, ExpectedVersion::Any)
        .unwrap();
    search
        .index_document(
            SearchDocument {
                namespace: "diag_ns".to_owned(),
                doc_id: "d1".to_owned(),
                kind: "k".to_owned(),
                title: "Diag Test".to_owned(),
                tags: Vec::new(),
                body: "Body".to_owned(),
                visibility: SearchVisibility::Public,
            },
            Ack::Flush,
        )
        .unwrap();
    search.flush_sync().unwrap();
    storage.flush_wal(true).unwrap();

    // 1. Inspect while primary `RocksDbStorage` is LIVE OPEN holding the exclusive write lock!
    let live_report = inspect_data_dir(dir.path()).expect("secondary inspect on live primary");
    assert_eq!(live_report.db_epoch, storage.epoch());
    assert!(live_report.latest_seq >= 1);
    assert_eq!(live_report.collections.len(), 1);
    assert_eq!(live_report.collections[0].namespace, "diag_ns");
    assert_eq!(live_report.active_leases.len(), 1);
    assert_eq!(live_report.active_leases[0].holder, "worker-1");
    assert_eq!(live_report.active_leases[0].generation, grant.generation);

    // Secret lease token MUST NEVER appear anywhere in serialized diagnostic output!
    let json = serde_json::to_string(&live_report).unwrap();
    assert!(
        !json.contains(&grant.token),
        "diag inspect must NEVER leak lease secret token"
    );

    // 2. Drop primary and verify inspect ALSO works cleanly on a closed DB.
    drop(search);
    drop(catalog);
    drop(leases);
    drop(storage);

    let closed_report = inspect_data_dir(dir.path()).expect("inspect on closed db");
    assert_eq!(closed_report.db_epoch, live_report.db_epoch);
    assert_eq!(closed_report.latest_seq, live_report.latest_seq);
    assert_eq!(closed_report.active_leases.len(), 1);
}

#[test]
fn diag_verify_passes_healthy_and_catches_corrupt_blob_and_orphan_posting() {
    let dir = TempDir::new().unwrap();
    {
        let cfg = StorageConfig {
            bg_sync_interval: None,
            ..Default::default()
        };
        let storage = RocksDbStorage::open(dir.path(), cfg, Arc::new(NoopPublisher)).unwrap();
        let idx = IndexMutations {
            ordered: vec![OrderedIndexEntry {
                index_id: "q".to_owned(),
                sort_key: b"01".to_vec(),
                summary: b"ok".to_vec(),
            }],
            edges: Vec::new(),
        };
        storage
            .put_indexed(
                "v",
                b"k1",
                b"good",
                Ack::Flush,
                ExpectedVersion::Any,
                Some(&idx),
                None,
            )
            .unwrap();
        storage.put_cas_blob("v", b"blob-content", Ack::Flush).unwrap();
        storage.flush_wal(true).unwrap();
    }

    let healthy = verify_data_dir(dir.path()).unwrap();
    assert!(healthy.ok, "expected healthy db, got {:?}", healthy.errors);
    assert_eq!(healthy.kv_records_checked, 1);
    assert_eq!(healthy.cas_blobs_checked, 1);
    assert_eq!(healthy.log_entries_checked, 1);
    assert_eq!(healthy.idx_rev_keys_checked, 1);
    assert_eq!(healthy.idx_ord_checked, 1);

    // Inject a corrupted CAS blob value AND an orphan idx_ord entry directly into RocksDB.
    {
        let mut opts = Options::default();
        opts.create_if_missing(false);
        let mut cfs = vec![DEFAULT_COLUMN_FAMILY_NAME];
        cfs.extend_from_slice(&ALL_COLUMN_FAMILIES);
        let raw_db = DB::open_cf(&opts, dir.path(), cfs).unwrap();

        let blob_cf = raw_db.cf_handle(CF_BLOB).unwrap();
        let (blob_k, _) = raw_db
            .iterator_cf(&blob_cf, rocksdb::IteratorMode::Start)
            .next()
            .unwrap()
            .unwrap();
        raw_db.put_cf(&blob_cf, &blob_k, b"tampered-blob!").unwrap();

        let ord_cf = raw_db.cf_handle(CF_IDX_ORD).unwrap();
        raw_db
            .put_cf(&ord_cf, b"orphan-posting-key", b"orphan")
            .unwrap();
        raw_db.flush_wal(true).unwrap();
    }

    let broken = verify_data_dir(dir.path()).unwrap();
    assert!(!broken.ok, "verify must fail on corrupted blob + orphan posting");
    assert!(
        broken.errors.iter().any(|e| e.contains("SHA-256 mismatch")),
        "expected CAS blob SHA-256 error, got {:?}",
        broken.errors
    );
    assert!(
        broken.errors.iter().any(|e| e.contains("orphan idx_ord")),
        "expected orphan idx_ord error, got {:?}",
        broken.errors
    );

    let cli_err = run_diag(DiagCommand::Verify {
        data_dir: dir.path().to_path_buf(),
    })
    .expect_err("CLI diag verify must return Err on corruption");
    assert!(matches!(cli_err, MembusError::Corruption(_)));
}

#[test]
fn diag_cli_parse_and_restore_roundtrip() {
    let src_dir = TempDir::new().unwrap();
    let backup_dir = TempDir::new().unwrap();
    let target_dir = TempDir::new().unwrap();

    let cfg = StorageConfig {
        bg_sync_interval: None,
        ..Default::default()
    };
    let storage = RocksDbStorage::open(src_dir.path(), cfg, Arc::new(NoopPublisher)).unwrap();
    storage
        .put("r", b"key", b"restored-via-cli", Ack::Flush, ExpectedVersion::Any)
        .unwrap();

    let snap = backup_dir.path().join("bk");
    create_full_backup(&storage, &snap).unwrap();

    let dest = target_dir.path().join("clean");
    let cmd = parse_args([
        "diag",
        "restore",
        "--backup-dir",
        snap.to_str().unwrap(),
        "--target-dir",
        dest.to_str().unwrap(),
    ])
    .unwrap();
    assert!(matches!(cmd, Command::Diag(DiagCommand::Restore { .. })));

    if let Command::Diag(diag_cmd) = cmd {
        run_diag(diag_cmd).unwrap();
    }

    let rcfg = StorageConfig {
        bg_sync_interval: None,
        ..Default::default()
    };
    let reopened = RocksDbStorage::open(&dest, rcfg, Arc::new(NoopPublisher)).unwrap();
    assert_eq!(
        reopened.get("r", b"key").unwrap().unwrap().value,
        b"restored-via-cli"
    );
}

#[test]
fn diag_verify_catches_orphan_idx_rev_corrupt_cold_ref_and_zero_epoch() {
    use membus::storage::codec::{encode_kv_key, encode_record, RECORD_FLAG_COLD_REF};
    use membus::storage::{CF_IDX_REV, CF_KV, CF_META};

    let dir = TempDir::new().unwrap();
    {
        let cfg = StorageConfig {
            bg_sync_interval: None,
            ..Default::default()
        };
        let storage = RocksDbStorage::open(dir.path(), cfg, Arc::new(NoopPublisher)).unwrap();
        storage
            .put("h", b"live", b"ok", Ack::Flush, ExpectedVersion::Any)
            .unwrap();
        storage.flush_wal(true).unwrap();
    }

    // Inject: (1) cold-flagged KV row with invalid ColdRef JSON, (2) orphan idx_rev key not in KV, (3) zero db_epoch.
    {
        let mut opts = Options::default();
        opts.create_if_missing(false);
        let mut cfs = vec![DEFAULT_COLUMN_FAMILY_NAME];
        cfs.extend_from_slice(&ALL_COLUMN_FAMILIES);
        let raw_db = DB::open_cf(&opts, dir.path(), cfs).unwrap();

        let kv_cf = raw_db.cf_handle(CF_KV).unwrap();
        let bad_cold_val = encode_record(99, RECORD_FLAG_COLD_REF, 1234, b"not-valid-coldref-json");
        raw_db
            .put_cf(&kv_cf, encode_kv_key("h", b"bad_cold"), bad_cold_val)
            .unwrap();

        let rev_cf = raw_db.cf_handle(CF_IDX_REV).unwrap();
        let empty_postings = membus::index::ReversePostings::default().encode();
        raw_db
            .put_cf(&rev_cf, encode_kv_key("h", b"ghost_pk"), empty_postings)
            .unwrap();

        let meta_cf = raw_db.cf_handle(CF_META).unwrap();
        raw_db
            .put_cf(&meta_cf, b"db_epoch", 0u64.to_be_bytes())
            .unwrap();
        raw_db.flush_wal(true).unwrap();
    }

    let report = verify_data_dir(dir.path()).unwrap();
    assert!(!report.ok);
    assert!(
        report.errors.iter().any(|e| e.contains("corrupt ColdRef JSON")),
        "expected ColdRef JSON error, got {:?}",
        report.errors
    );
    assert!(
        report.errors.iter().any(|e| e.contains("orphan idx_rev")),
        "expected orphan idx_rev error, got {:?}",
        report.errors
    );
    assert!(
        report.errors.iter().any(|e| e.contains("zero db_epoch")),
        "expected zero db_epoch error, got {:?}",
        report.errors
    );
}
