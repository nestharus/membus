use std::sync::Arc;
use std::thread;

use membus::storage::codec::{decode_log_entry, decode_record, encode_log_entry, encode_record};
use membus::storage::{
    AdmissionConfig, AdmissionGate, NoopPublisher, RocksDbStorage, StorageConfig, StorageEngine,
    ALL_COLUMN_FAMILIES,
};
use membus::{
    Ack, ExpectedVersion, MembusError, MutationOp, ScanOptions, SignalEvent, SignalKind,
    VersionGuard, WriteMutation,
};
use tempfile::TempDir;

fn open_test_storage(dir: &TempDir, cfg: StorageConfig) -> Arc<RocksDbStorage> {
    Arc::new(
        RocksDbStorage::open(dir.path(), cfg, Arc::new(NoopPublisher))
            .expect("open rocksdb storage"),
    )
}

#[test]
fn provisions_all_eight_column_families_and_persists_epoch() {
    let dir = TempDir::new().expect("tempdir");
    let epoch = {
        let storage = open_test_storage(&dir, StorageConfig::default());
        let cfs = storage.column_family_names();
        for expected in ALL_COLUMN_FAMILIES {
            assert!(cfs.contains(&expected), "missing column family {expected}");
        }
        storage
            .put("ns", b"k1", b"v1", Ack::Flush, ExpectedVersion::Absent)
            .expect("put");
        storage.epoch()
    };

    let reopened = open_test_storage(&dir, StorageConfig::default());
    assert_eq!(reopened.epoch(), epoch);
    assert_eq!(reopened.latest_seq(), 1);
    let rec = reopened.get("ns", b"k1").expect("get").expect("present");
    assert_eq!(rec.value, b"v1");
    assert_eq!(rec.meta.version, 1);
}

#[test]
fn point_ops_get_many_and_namespace_isolation() {
    let dir = TempDir::new().expect("tempdir");
    let storage = open_test_storage(&dir, StorageConfig::default());

    let c1 = storage
        .put("app", b"item", b"val-app", Ack::Fast, ExpectedVersion::Any)
        .expect("put app");
    let c2 = storage
        .put(
            "app2",
            b"item",
            b"val-app2",
            Ack::Flush,
            ExpectedVersion::Any,
        )
        .expect("put app2");
    assert!(c2.seq > c1.seq);

    let r1 = storage.get("app", b"item").expect("get").expect("some");
    let r2 = storage.get("app2", b"item").expect("get").expect("some");
    assert_eq!(r1.value, b"val-app");
    assert_eq!(r2.value, b"val-app2");

    let many = storage
        .get_many("app", &[b"item".to_vec(), b"missing".to_vec()])
        .expect("get_many");
    assert_eq!(many.len(), 2);
    assert_eq!(many[0].as_ref().unwrap().value, b"val-app");
    assert!(many[1].is_none());

    storage
        .remove("app", b"item", Ack::Fast, ExpectedVersion::Version(c1.seq))
        .expect("remove");
    assert!(storage.get("app", b"item").expect("get").is_none());
    // Distinct namespace untouched.
    assert_eq!(
        storage.get("app2", b"item").expect("get").unwrap().value,
        b"val-app2"
    );
}

#[test]
fn single_key_cas_conflicts_and_aba_monotonic_versions() {
    let dir = TempDir::new().expect("tempdir");
    let storage = open_test_storage(&dir, StorageConfig::default());

    let err = storage
        .put("cas", b"k", b"v0", Ack::Fast, ExpectedVersion::Version(1))
        .unwrap_err();
    assert!(matches!(
        err,
        MembusError::Conflict {
            actual: None,
            expected: ExpectedVersion::Version(1),
            ..
        }
    ));

    let c1 = storage
        .put("cas", b"k", b"v1", Ack::Fast, ExpectedVersion::Absent)
        .expect("create");
    let err = storage
        .put("cas", b"k", b"v2", Ack::Fast, ExpectedVersion::Absent)
        .unwrap_err();
    assert!(matches!(
        err,
        MembusError::Conflict {
            actual: Some(v),
            expected: ExpectedVersion::Absent,
            ..
        } if v == c1.seq
    ));

    let c2 = storage
        .put(
            "cas",
            b"k",
            b"v2",
            Ack::Fast,
            ExpectedVersion::Version(c1.seq),
        )
        .expect("update");
    assert!(c2.seq > c1.seq);

    storage
        .remove("cas", b"k", Ack::Fast, ExpectedVersion::Version(c2.seq))
        .expect("delete");

    // Recreating after deletion allocates a strictly higher version (no ABA version reuse).
    let c3 = storage
        .put("cas", b"k", b"v3", Ack::Fast, ExpectedVersion::Absent)
        .expect("recreate");
    assert!(c3.seq > c2.seq);
    assert_ne!(c3.seq, c1.seq);
}

#[test]
fn scoped_multi_key_tx_atomicity_guard_conflict_and_concurrency() {
    let dir = TempDir::new().expect("tempdir");
    let storage = open_test_storage(&dir, StorageConfig::default());

    let init = storage
        .commit_tx(
            &[
                VersionGuard {
                    namespace: "bank".into(),
                    key: b"acct:a".to_vec(),
                    expected: ExpectedVersion::Absent,
                },
                VersionGuard {
                    namespace: "bank".into(),
                    key: b"acct:b".to_vec(),
                    expected: ExpectedVersion::Absent,
                },
            ],
            &[
                WriteMutation {
                    namespace: "bank".into(),
                    key: b"acct:a".to_vec(),
                    op: MutationOp::Put {
                        value: b"100".to_vec(),
                    },
                },
                WriteMutation {
                    namespace: "bank".into(),
                    key: b"acct:b".to_vec(),
                    op: MutationOp::Put {
                        value: b"100".to_vec(),
                    },
                },
            ],
            Ack::Flush,
        )
        .expect("init accounts");
    assert_eq!(init.versions.len(), 2);

    // Guard mismatch must abort the entire transaction atomically.
    let mismatch = storage
        .commit_tx(
            &[
                VersionGuard {
                    namespace: "bank".into(),
                    key: b"acct:a".to_vec(),
                    expected: ExpectedVersion::Version(init.seq),
                },
                VersionGuard {
                    namespace: "bank".into(),
                    key: b"acct:b".to_vec(),
                    expected: ExpectedVersion::Version(9999),
                },
            ],
            &[
                WriteMutation {
                    namespace: "bank".into(),
                    key: b"acct:a".to_vec(),
                    op: MutationOp::Put {
                        value: b"50".to_vec(),
                    },
                },
                WriteMutation {
                    namespace: "bank".into(),
                    key: b"acct:b".to_vec(),
                    op: MutationOp::Put {
                        value: b"150".to_vec(),
                    },
                },
            ],
            Ack::Fast,
        )
        .unwrap_err();
    assert!(matches!(mismatch, MembusError::Conflict { .. }));
    assert_eq!(storage.get("bank", b"acct:a").unwrap().unwrap().value, b"100");
    assert_eq!(storage.get("bank", b"acct:b").unwrap().unwrap().value, b"100");

    // Duplicate target rejection before touching RocksDB.
    let dup = storage
        .commit_tx(
            &[],
            &[
                WriteMutation {
                    namespace: "bank".into(),
                    key: b"dup".to_vec(),
                    op: MutationOp::Put {
                        value: b"1".to_vec(),
                    },
                },
                WriteMutation {
                    namespace: "bank".into(),
                    key: b"dup".to_vec(),
                    op: MutationOp::Put {
                        value: b"2".to_vec(),
                    },
                },
            ],
            Ack::Fast,
        )
        .unwrap_err();
    assert!(matches!(dup, MembusError::InvalidArgument(_)));

    // 8 concurrent threads executing optimistic CAS increments on a shared counter key.
    storage
        .put(
            "cnt",
            b"shared",
            &0u64.to_be_bytes(),
            Ack::Fast,
            ExpectedVersion::Absent,
        )
        .expect("init counter");
    let mut handles = Vec::new();
    for _ in 0..8 {
        let s = Arc::clone(&storage);
        handles.push(thread::spawn(move || {
            for _ in 0..25 {
                loop {
                    let rec = s.get("cnt", b"shared").unwrap().unwrap();
                    let val = u64::from_be_bytes(rec.value.as_slice().try_into().unwrap());
                    let next = val + 1;
                    match s.commit_tx(
                        &[VersionGuard {
                            namespace: "cnt".into(),
                            key: b"shared".to_vec(),
                            expected: ExpectedVersion::Version(rec.meta.version),
                        }],
                        &[WriteMutation {
                            namespace: "cnt".into(),
                            key: b"shared".to_vec(),
                            op: MutationOp::Put {
                                value: next.to_be_bytes().to_vec(),
                            },
                        }],
                        Ack::Fast,
                    ) {
                        Ok(_) => break,
                        Err(MembusError::Conflict { .. }) | Err(MembusError::TxConflict(_)) => {
                            continue
                        }
                        Err(other) => panic!("unexpected tx error: {other}"),
                    }
                }
            }
        }));
    }
    for h in handles {
        h.join().expect("thread join");
    }
    let final_rec = storage.get("cnt", b"shared").unwrap().unwrap();
    let final_val = u64::from_be_bytes(final_rec.value.as_slice().try_into().unwrap());
    assert_eq!(final_val, 8 * 25);
}

#[test]
fn mvcc_snapshot_isolation_and_keyset_pagination() {
    let dir = TempDir::new().expect("tempdir");
    let storage = open_test_storage(&dir, StorageConfig::default());

    for i in 0..120u32 {
        let key = format!("doc/{i:04}");
        let val = format!("initial-{i:04}");
        storage
            .put("docs", key.as_bytes(), val.as_bytes(), Ack::Fast, ExpectedVersion::Any)
            .expect("seed");
    }
    // Add a neighbor namespace that shares a string prefix ("docs2").
    storage
        .put("docs2", b"doc/0000", b"other-ns", Ack::Fast, ExpectedVersion::Any)
        .expect("seed other ns");

    let snap = storage.open_snapshot().expect("open snapshot");
    let snap_seq = snap.snapshot_seq();
    assert!(snap_seq >= 120);

    // Mutate and delete keys concurrently after snapshot is pinned.
    for i in 0..60u32 {
        let key = format!("doc/{i:04}");
        storage
            .put("docs", key.as_bytes(), b"mutated", Ack::Fast, ExpectedVersion::Any)
            .expect("mutate");
    }
    for i in 60..120u32 {
        let key = format!("doc/{i:04}");
        storage
            .remove("docs", key.as_bytes(), Ack::Fast, ExpectedVersion::Any)
            .expect("delete");
    }
    storage
        .put("docs", b"doc/9999", b"new-after-snap", Ack::Fast, ExpectedVersion::Any)
        .expect("insert");

    // Paginate entire prefix inside the frozen snapshot using both limit and byte caps.
    let mut collected = Vec::new();
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let page = storage
            .scan_prefix(
                &snap,
                "docs",
                b"doc/",
                &ScanOptions {
                    after_key: cursor.clone(),
                    limit: 17,
                    max_bytes: 600,
                },
            )
            .expect("scan_prefix");
        assert_eq!(page.snapshot_seq, snap_seq);
        for rec in &page.records {
            collected.push((rec.key.clone(), rec.value.clone()));
        }
        if !page.truncated {
            assert!(page.next_cursor.is_none());
            break;
        }
        cursor = page.next_cursor;
        assert!(cursor.is_some());
    }

    assert_eq!(collected.len(), 120);
    for (idx, (k, v)) in collected.iter().enumerate() {
        assert_eq!(k, format!("doc/{idx:04}").as_bytes());
        assert_eq!(v, format!("initial-{idx:04}").as_bytes());
    }

    // Range scan subset [doc/0010, doc/0020).
    let range_page = storage
        .scan_range(
            &snap,
            "docs",
            b"doc/0010",
            b"doc/0020",
            &ScanOptions {
                after_key: None,
                limit: 100,
                max_bytes: 1024 * 1024,
            },
        )
        .expect("scan_range");
    assert_eq!(range_page.records.len(), 10);
    assert!(!range_page.truncated);
    assert_eq!(range_page.records[0].key, b"doc/0010");
    assert_eq!(range_page.records[9].key, b"doc/0019");

    // Verify forward progress even when max_bytes is smaller than a single record.
    let tiny_page = storage
        .scan_prefix(
            &snap,
            "docs",
            b"doc/",
            &ScanOptions {
                after_key: None,
                limit: 10,
                max_bytes: 1,
            },
        )
        .expect("tiny max_bytes scan");
    assert_eq!(tiny_page.records.len(), 1);
    assert!(tiny_page.truncated);
}

#[test]
fn blobdb_large_values_blob_cf_and_wal_tailing() {
    let dir = TempDir::new().expect("tempdir");
    let big_value: Vec<u8> = (0..(64 * 1024)).map(|i| (i % 251) as u8).collect();
    let blob_payload: Vec<u8> = (0..(48 * 1024)).map(|i| ((i * 7) % 251) as u8).collect();

    {
        let storage = open_test_storage(&dir, StorageConfig::default());
        let wal_start = storage.rocksdb_latest_sequence();

        storage
            .put("blobs", b"large-kv", &big_value, Ack::Flush, ExpectedVersion::Absent)
            .expect("put large kv");
        storage
            .put_blob(b"sha256:demo", &blob_payload, Ack::Flush)
            .expect("put blob");

        let wal_batches = storage.read_wal_since(wal_start, 64).expect("read_wal_since");
        assert!(!wal_batches.is_empty(), "expected WAL updates after writes");
        assert!(wal_batches.iter().all(|b| b.op_count > 0 && !b.raw_bytes.is_empty()));
    }

    // Reopen and verify large BlobDB values survive cleanly.
    let reopened = open_test_storage(&dir, StorageConfig::default());
    let rec = reopened.get("blobs", b"large-kv").unwrap().unwrap();
    assert_eq!(rec.value, big_value);
    let blob = reopened.get_blob(b"sha256:demo").unwrap().unwrap();
    assert_eq!(blob, blob_payload);
}

#[test]
fn admission_gate_enforces_op_and_byte_limits_and_releases_on_drop() {
    let gate = AdmissionGate::new(AdmissionConfig {
        max_in_flight_ops: 2,
        max_in_flight_bytes: 256,
        retry_after_ms: 7,
    });

    let p1 = gate.try_acquire(100).expect("p1");
    let p2 = gate.try_acquire(100).expect("p2");
    assert_eq!(gate.in_flight_ops(), 2);
    assert_eq!(gate.in_flight_bytes(), 200);

    // Op limit exceeded -> Busy.
    let busy_ops = gate.try_acquire(10).err().expect("busy ops");
    assert!(matches!(busy_ops, MembusError::Busy { retry_after_ms: 7, .. }));

    drop(p2);
    assert_eq!(gate.in_flight_ops(), 1);
    assert_eq!(gate.in_flight_bytes(), 100);

    // Byte limit exceeded -> Busy (and op counter rolls back cleanly!).
    let busy_bytes = gate.try_acquire(200).err().expect("busy bytes");
    assert!(matches!(busy_bytes, MembusError::Busy { .. }));
    assert_eq!(gate.in_flight_ops(), 1);
    assert_eq!(gate.in_flight_bytes(), 100);

    drop(p1);
    assert_eq!(gate.in_flight_ops(), 0);
    assert_eq!(gate.in_flight_bytes(), 0);
}

#[test]
fn codec_detects_record_and_log_crc_corruption() {
    let mut encoded = encode_record(42, 0, 1_700_000_000_000, b"payload-integrity");
    assert_eq!(
        decode_record(b"k".to_vec(), &encoded).unwrap().value,
        b"payload-integrity"
    );
    let last = encoded.len() - 1;
    encoded[last] ^= 0xFF;
    assert!(matches!(
        decode_record(b"k".to_vec(), &encoded),
        Err(MembusError::Corruption(_))
    ));

    let ev = SignalEvent {
        seq: 7,
        epoch: 1,
        namespace: "ns".into(),
        key: b"k".to_vec(),
        version: 7,
        kind: SignalKind::Put,
        value_hint: Some(b"hint".to_vec()),
    };
    let mut encoded_log = encode_log_entry(1, 7, &[ev]);
    assert_eq!(decode_log_entry(&encoded_log).unwrap().len(), 1);
    let last_log = encoded_log.len() - 1;
    encoded_log[last_log] ^= 0x01;
    assert!(matches!(
        decode_log_entry(&encoded_log),
        Err(MembusError::Corruption(_))
    ));
}

#[test]
fn trim_log_before_rejects_future_sequence_horizon() {
    let dir = TempDir::new().expect("tempdir");
    let storage = open_test_storage(&dir, StorageConfig::default());

    for i in 1..=5u64 {
        storage
            .put("trim", format!("k{i}").as_bytes(), b"v", Ack::Fast, ExpectedVersion::Any)
            .expect("put");
    }

    let err = storage.trim_log_before(100).unwrap_err();
    assert!(matches!(err, MembusError::InvalidArgument(_)));
    assert_eq!(storage.log_horizon_seq(), 0);

    // Trimming up to latest_seq + 1 (keep_from_seq = 6) is valid and sets horizon = 5.
    storage.trim_log_before(6).expect("valid trim");
    assert_eq!(storage.log_horizon_seq(), 5);
}

#[test]
fn cross_db_snapshot_is_rejected_safely() {
    let dir1 = TempDir::new().expect("tempdir 1");
    let dir2 = TempDir::new().expect("tempdir 2");
    let db1 = open_test_storage(&dir1, StorageConfig::default());
    let db2 = open_test_storage(&dir2, StorageConfig::default());

    db1.put("ns", b"k1", b"v1", Ack::Fast, ExpectedVersion::Any)
        .expect("put db1");
    let snap1 = db1.open_snapshot().expect("snap1");

    assert!(matches!(
        db2.get_in_snapshot(&snap1, "ns", b"k1"),
        Err(MembusError::InvalidArgument(_))
    ));
    assert!(matches!(
        db2.get_many_in_snapshot(&snap1, "ns", &[b"k1".to_vec()]),
        Err(MembusError::InvalidArgument(_))
    ));
    assert!(matches!(
        db2.scan_prefix(&snap1, "ns", b"", &ScanOptions::default()),
        Err(MembusError::InvalidArgument(_))
    ));
    assert!(matches!(
        db2.scan_range(&snap1, "ns", b"", b"", &ScanOptions::default()),
        Err(MembusError::InvalidArgument(_))
    ));
}

#[test]
fn concurrent_trim_log_before_is_strictly_monotonic() {
    let dir = TempDir::new().expect("tempdir");
    {
        let storage = open_test_storage(&dir, StorageConfig::default());
        for i in 1..=100u64 {
            storage
                .put("t", format!("k{i}").as_bytes(), b"v", Ack::Fast, ExpectedVersion::Any)
                .expect("put");
        }

        let targets = [15u64, 95, 30, 80, 10, 60, 95, 45];
        let mut handles = Vec::new();
        for target in targets {
            let s = Arc::clone(&storage);
            handles.push(thread::spawn(move || {
                s.trim_log_before(target).expect("concurrent trim");
            }));
        }
        for h in handles {
            h.join().expect("join");
        }
        assert_eq!(storage.log_horizon_seq(), 94);
    }

    // Persisted meta horizon must equal the maximum trimmed watermark (never regressed).
    let reopened = open_test_storage(&dir, StorageConfig::default());
    assert_eq!(reopened.log_horizon_seq(), 94);
}

#[test]
fn fresh_databases_initialize_unique_non_constant_epochs() {
    let d1 = TempDir::new().expect("d1");
    let d2 = TempDir::new().expect("d2");
    let d3 = TempDir::new().expect("d3");
    let e1 = open_test_storage(&d1, StorageConfig::default()).epoch();
    let e2 = open_test_storage(&d2, StorageConfig::default()).epoch();
    let e3 = open_test_storage(&d3, StorageConfig::default()).epoch();
    assert_ne!(e1, 0);
    assert_ne!(e2, 0);
    assert_ne!(e3, 0);
    assert_ne!(e1, e2);
    assert_ne!(e2, e3);
    assert_ne!(e1, e3);
}
