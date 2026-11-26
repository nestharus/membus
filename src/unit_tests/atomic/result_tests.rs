use super::*;
use crate::{
    atomic::{FinalChange, PublicationError},
    Config, ManualClock, Mutation, Outcome,
};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Condvar, Mutex,
    },
    time::{Duration, Instant},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new(label: &str) -> Self {
        let root = PathBuf::from(
            std::env::var_os("MEMBUS_PROVIDER_TEST_ROOT").expect("explicit authored fixture root"),
        );
        assert!(root.is_absolute() && root.is_dir());
        let path = root.join(format!(
            "result-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        println!("retained authored result fixture {}", path.display());
        Self(path)
    }
    fn state(&self) -> PathBuf {
        self.0.join("state")
    }
}
fn config(clock: &Arc<ManualClock>) -> Config {
    Config {
        clock: Some(clock.clone()),
        cache_bytes: 0,
        signal_bytes: 0,
        signal_history: 0,
        tick_ms: 3_600_000,
        retire_ms: 3_600_000,
        flush_ms: 0,
        retry_ms: 10_000,
        snapshot_ttl_ms: 2_000,
        ..Config::default()
    }
}
fn request(s: &Store, id: &str, ack: AtomicAck) -> AtomicRequest {
    s.capture_namespace_funded("authored", 64 * 1024)
        .unwrap()
        .prepare(
            id,
            ack,
            vec![
                FinalChange::put("a", vec![0, 255, 10]),
                FinalChange::put("b", b"two".to_vec()),
            ],
        )
        .unwrap()
}
fn publish(s: &Store, r: &AtomicRequest) -> Result<Outcome, PublicationError> {
    s.publish_atomic(r, s.clock.utc_ms() + 30_000)
}
#[test]
fn expired_local_publication_cutoff_keeps_store_unchanged() {
    let fixture = Fixture::new("atomic-local-cutoff");
    let clock = ManualClock::new(1000);
    let store = Store::open(&fixture.state(), config(&clock)).unwrap();
    let original = request(&store, "local-cutoff", AtomicAck::Flush);
    let deadline = clock.utc_ms() + 30_000;
    let cutoff = Instant::now();
    let next_before = store.next_issued.load(Ordering::SeqCst);

    assert!(matches!(
        store.publish_atomic_before(&original, deadline, cutoff),
        Err(PublicationError::Expired)
    ));
    assert!(store.try_get("authored", "a").unwrap().is_none());
    assert!(store.try_get("authored", "b").unwrap().is_none());
    assert_eq!(store.next_issued.load(Ordering::SeqCst), next_before);

    let outcome = store.publish_atomic(&original, deadline).unwrap();
    assert_eq!(
        store.publish_atomic_before(&original, deadline, cutoff).unwrap(),
        outcome
    );
}

#[test]
fn expired_local_mutation_cutoff_keeps_store_unchanged() {
    let fixture = Fixture::new("mutation-local-cutoff");
    let clock = ManualClock::new(1000);
    let store = Store::open(&fixture.state(), config(&clock)).unwrap();
    let original = Mutation {
        ns: "authored".into(),
        key: "custody".into(),
        op_id: "mutation-cutoff".into(),
        deadline: clock.utc_ms() + 30_000,
        ack: 2,
        expected: Some(0),
        bytes: vec![1],
        remove: false,
    };
    let cutoff = Instant::now();
    let next_before = store.next_issued.load(Ordering::SeqCst);

    assert_eq!(store.mutate_before(original.clone(), cutoff), Outcome::error(5));
    assert!(store.try_get("authored", "custody").unwrap().is_none());
    assert_eq!(store.next_issued.load(Ordering::SeqCst), next_before);

    let result = store.mutate(original.clone());
    assert_eq!(result.code, 0);
    assert_eq!(store.mutate_before(original, cutoff), result);
}

fn inspect(s: &Store, r: &AtomicRequest) -> AtomicResultInspection {
    inspect_input(s, OriginalRef::Request(r))
}
fn inspect_input(s: &Store, original: OriginalRef<'_>) -> AtomicResultInspection {
    let scratch = super::OriginalBudget::new(RESULT_INSPECTION_SCRATCH_BYTES);
    let result = s.inspect_atomic_result(original, &scratch).unwrap();
    assert_eq!(scratch.used(), 0);
    result
}

#[derive(Debug, PartialEq, Eq)]
struct Observation {
    state: String,
    committed: String,
    io: (u64, u64, u64),
    flags: (bool, bool),
    next_issued: u64,
    scan_usage: (usize, usize),
    cache: String,
    files: Vec<(String, usize, String)>,
}
fn files(root: &Path, dir: &Path, out: &mut Vec<(String, usize, String)>) {
    let mut paths: Vec<_> = fs::read_dir(dir)
        .unwrap()
        .map(|p| p.unwrap().path())
        .collect();
    paths.sort();
    for path in paths {
        if path.is_dir() {
            files(root, &path, out)
        } else {
            let bytes = match fs::read(&path) {
                Ok(bytes) => bytes,
                Err(e)
                    if e.raw_os_error() == Some(32)
                        && path.file_name().is_some_and(|n| {
                            n == "owner.lock" || n == "chunks.catalogue-owner"
                        }) =>
                {
                    // Existing exclusive ownership is itself observed. Never
                    // suppress an unreadable data, catalogue or outcome file.
                    out.push((
                        path.strip_prefix(root)
                            .unwrap()
                            .to_string_lossy()
                            .into_owned(),
                        fs::metadata(&path).unwrap().len() as usize,
                        "exclusive-owner-unreadable-os32".into(),
                    ));
                    continue;
                }
                Err(e) => panic!("fixture observation {}: {e}", path.display()),
            };
            let mut h = crate::chunks::Sha256::new();
            h.update(&bytes);
            out.push((
                path.strip_prefix(root)
                    .unwrap()
                    .to_string_lossy()
                    .into_owned(),
                bytes.len(),
                h.hex(),
            ));
        }
    }
}
fn observe(s: &Store, f: &Fixture) -> Observation {
    let committed = s.committed.read().unwrap();
    let state = s.state.lock().unwrap();
    let mut dedup: Vec<_> = state
        .dedup
        .iter()
        .map(|(k, d)| {
            (
                k.clone(),
                d.fingerprint,
                d.at,
                d.until_mono,
                d.replayed_chunk,
                d.outcome.as_ref().map(|o| (o.code, o.version, o.ack)),
            )
        })
        .collect();
    dedup.sort();
    let mut pending: Vec<_> = state
        .pending
        .iter()
        .map(|(k, m)| (k.clone(), m.version, m.tombstone, m.bytes, m.chunk))
        .collect();
    pending.sort();
    let mut metadata: Vec<_> = state
        .metadata
        .iter()
        .map(|(k, m)| (k.clone(), m.version, m.tombstone, m.bytes, m.chunk))
        .collect();
    metadata.sort();
    let mut namespaces: Vec<_> = state.namespace_counts.iter().collect();
    namespaces.sort();
    let mut retired_set: Vec<_> = state.retired_set.iter().collect();
    retired_set.sort();
    let mut policy: Vec<_> = state.policy_publishing.iter().collect();
    policy.sort();
    let mut tomb: Vec<_> = state.tomb_candidates.iter().collect();
    tomb.sort();
    let numbers = (
        state.keys,
        state.committed_bytes,
        state.dedup_bytes,
        state.next,
        state.reserved_bytes,
        state.memory_reserved,
        state.stripe_reserved,
        state.stripe_items,
        state.persisted_bytes,
        state.pending_disk,
        state.read_only,
        state.warming,
    );
    let state_image=format!("dedup={dedup:?};pending={pending:?};metadata={metadata:?};namespaces={namespaces:?};retired={:?};retired_set={retired_set:?};expiry={:?};policy={policy:?};tomb={tomb:?};numbers={numbers:?}",state.retired,state.expiry);
    let committed_image = format!(
        "{:?}",
        committed
            .iter()
            .map(|(k, e)| (
                k,
                e.version,
                e.ack,
                e.tombstone,
                e.len,
                e.location.offset,
                e.location.record_len,
                e.location.subrecord
            ))
            .collect::<Vec<_>>()
    );
    drop(state);
    drop(committed);
    let mut captured = Vec::new();
    files(&f.state(), &f.state(), &mut captured);
    Observation {
        state: state_image,
        committed: committed_image,
        io: s.io_metrics(),
        flags: (
            s.flags.storage_error.load(Ordering::SeqCst),
            s.flags.expiry_degraded.load(Ordering::SeqCst),
        ),
        next_issued: s.next_issued.load(Ordering::SeqCst),
        scan_usage: (
            s.scan_usage.count.load(Ordering::Acquire),
            s.scan_usage.bytes.load(Ordering::Acquire),
        ),
        cache: format!("{:?}", s.cache_stats()),
        files: captured,
    }
}
fn unchanged(s: &Store, f: &Fixture, label: &str, action: impl FnOnce()) {
    let before = observe(s, f);
    fs::write(
        f.0.join(format!("{label}-before.txt")),
        format!("{before:#?}"),
    )
    .unwrap();
    action();
    let after = observe(s, f);
    fs::write(
        f.0.join(format!("{label}-after.txt")),
        format!("{after:#?}"),
    )
    .unwrap();
    assert_eq!(before, after, "inspection mutated Store or its files");
    println!(
        "no-effect {label}: files={} io={:?} next={} scans={:?}",
        after.files.len(),
        after.io,
        after.next_issued,
        after.scan_usage
    );
}
fn assert_committed(
    i: AtomicResultInspection,
    ack: AtomicAck,
    version: u64,
    retention: RetryRetention,
) {
    assert!(
        matches!(i.evidence,AtomicResultEvidence::Matching{retention:r,result:RetainedAtomicResult::Committed{version:v,achieved:a},..} if (r,v,a)==(retention,version,ack)),
        "{i:?}"
    );
}

#[test]
fn absent_exact_original_and_changed_epoch_queries_never_admit_or_create_success() {
    let f = Fixture::new("absent");
    let clock = ManualClock::new(crate::now_ms());
    let s = Store::open(&f.state(), config(&clock)).unwrap();
    let r = request(&s, "unsent", AtomicAck::Flush);
    let bytes = r.to_bytes();
    let borrowed = BorrowedOriginal::from_bytes(&bytes).unwrap();
    unchanged(&s, &f, "absent", || {
        for _ in 0..32 {
            let i = inspect(&s, &r);
            assert_eq!(i.evidence, AtomicResultEvidence::NotRetained);
            assert_eq!(i.store_epoch, i.original_epoch);
            assert_eq!(inspect_input(&s, OriginalRef::Borrowed(&borrowed)), i);
        }
    });
    clock.advance(2000);
    unchanged(&s, &f, "expired-unsent", || {
        assert_eq!(inspect(&s, &r).evidence, AtomicResultEvidence::NotRetained)
    });
    drop(s);
    let s = Store::open(&f.state(), config(&clock)).unwrap();
    unchanged(&s, &f, "changed-epoch-absent", || {
        let i = inspect(&s, &r);
        assert_ne!(i.store_epoch, i.original_epoch);
        assert_eq!(i.evidence, AtomicResultEvidence::NotRetained);
    });
    assert_eq!(s.slots("authored").1, 0);
}

#[test]
fn committed_os_flush_exact_bytes_and_historical_success_are_not_new_grants() {
    for ack in [AtomicAck::Os, AtomicAck::Flush] {
        let f = Fixture::new("success");
        let clock = ManualClock::new(crate::now_ms());
        let s = Store::open(&f.state(), config(&clock)).unwrap();
        let r = request(&s, "original", ack);
        assert_eq!(
            publish(&s, &r).unwrap(),
            Outcome {
                code: 0,
                version: 1,
                ack: ack.byte()
            }
        );
        let bytes = r.to_bytes();
        let borrowed = BorrowedOriginal::from_bytes(&bytes).unwrap();
        clock.advance(2000); // Capture horizon passed, retry evidence still retained.
        assert_eq!(
            s.mutate(Mutation {
                ns: "authored".into(),
                key: "a".into(),
                op_id: "newer".into(),
                deadline: clock.utc_ms() + 30_000,
                ack: 1,
                expected: None,
                bytes: b"newer".to_vec(),
                remove: false
            })
            .code,
            0
        );
        unchanged(&s, &f, "known", || {
            for _ in 0..16 {
                let i = inspect(&s, &r);
                assert_eq!(i.observed_mono_ms, i.original_expires_mono_ms);
                assert_committed(i, ack, 1, RetryRetention::WithinWindow);
                assert_eq!(inspect_input(&s, OriginalRef::Borrowed(&borrowed)), i);
            }
        });
        assert_eq!(s.try_get("authored", "a").unwrap().unwrap().bytes, b"newer");
        s.set_read_only(true);
        s.set_warming(true);
        s.flags.storage_error.store(true, Ordering::SeqCst);
        unchanged(&s, &f, "fenced-known", || {
            assert_committed(inspect(&s, &r), ack, 1, RetryRetention::WithinWindow)
        });
    }
}

#[test]
fn differing_value_ack_and_canonical_original_identity_never_borrow_the_known_result() {
    let f = Fixture::new("identity");
    let clock = ManualClock::new(crate::now_ms());
    let s = Store::open(&f.state(), config(&clock)).unwrap();
    let r = request(&s, "original", AtomicAck::Flush);
    publish(&s, &r).unwrap();
    let bytes = r.to_bytes();
    let mut values = bytes.clone();
    *values.last_mut().unwrap() ^= 1;
    let mut ack = bytes.clone();
    ack[4 + 4 + "authored".len() + 4 + "original".len() + 16] = 1;
    let mut epoch = bytes.clone();
    epoch[4 + 4 + "authored".len() + 4 + "original".len()] ^= 1;
    clock.advance(10_001);
    unchanged(&s, &f, "mismatch-even-after-expiry", || {
        for changed in [&values, &ack, &epoch] {
            let original = BorrowedOriginal::from_bytes(changed).unwrap();
            assert_eq!(
                inspect_input(&s, OriginalRef::Borrowed(&original)).evidence,
                AtomicResultEvidence::IdentityMismatch
            );
        }
    });
}

#[test]
fn real_pre_admission_refusal_leaves_missing_evidence_without_proving_no_prior_effect() {
    let f = Fixture::new("refused");
    let clock = ManualClock::new(crate::now_ms());
    let s = Store::open(
        &f.state(),
        Config {
            quota: 0,
            ..config(&clock)
        },
    )
    .unwrap();
    let r = request(&s, "quota-refused", AtomicAck::Os);
    assert_eq!(publish(&s, &r), Err(PublicationError::Resource(8)));
    unchanged(&s, &f, "refused-query", || {
        assert_eq!(inspect(&s, &r).evidence, AtomicResultEvidence::NotRetained)
    });
}

#[test]
fn pending_query_does_not_join_or_wait_for_the_publication_gate() {
    let f = Fixture::new("pending");
    let clock = ManualClock::new(crate::now_ms());
    let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
    let s = Arc::new(
        Store::open(
            &f.state(),
            Config {
                sync_test_gate: Some(gate.clone()),
                ..config(&clock)
            },
        )
        .unwrap(),
    );
    let r = request(&s, "pending-original", AtomicAck::Flush);
    let missing = request(&s, "not-sent", AtomicAck::Os);
    let writer = {
        let (s, r) = (s.clone(), r.clone());
        std::thread::spawn(move || publish(&s, &r))
    };
    let mut held = gate.0.lock().unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    while !held.0 && Instant::now() < until {
        held = gate
            .1
            .wait_timeout(held, Duration::from_millis(100))
            .unwrap()
            .0;
    }
    let reached = held.0;
    drop(held);
    let before = observe(&s, &f);
    let (tx, rx) = std::sync::mpsc::channel();
    let reader = {
        let (s, r, missing) = (s.clone(), r.clone(), missing.clone());
        std::thread::spawn(move || tx.send((inspect(&s, &r), inspect(&s, &missing))).unwrap())
    };
    let result = rx.recv_timeout(Duration::from_secs(2));
    let after = observe(&s, &f);
    {
        let mut held = gate.0.lock().unwrap();
        held.1 = true;
        gate.1.notify_all();
    }
    let written = writer.join().unwrap();
    reader.join().unwrap();
    assert!(reached);
    assert_eq!(before, after);
    let (known, missing) = result.expect("read-only inspection waited on admitted publication");
    assert!(matches!(
        known.evidence,
        AtomicResultEvidence::Matching {
            result: RetainedAtomicResult::Pending,
            retention: RetryRetention::WithinWindow,
            ..
        }
    ));
    assert_eq!(missing.evidence, AtomicResultEvidence::NotRetained);
    assert_eq!(written.unwrap().version, 1);
    assert_committed(
        inspect(&s, &r),
        AtomicAck::Flush,
        1,
        RetryRetention::WithinWindow,
    );
    fs::write(f.0.join("pending-before.txt"), format!("{before:#?}")).unwrap();
    fs::write(f.0.join("pending-after.txt"), format!("{after:#?}")).unwrap();
}

#[test]
fn failed_admitted_sync_stays_uncertain_including_after_retry_expiry() {
    let f = Fixture::new("uncertain");
    let clock = ManualClock::new(crate::now_ms());
    let s = Store::open(
        &f.state(),
        Config {
            fail_sync: true,
            ..config(&clock)
        },
    )
    .unwrap();
    let r = request(&s, "failed-sync", AtomicAck::Flush);
    assert_eq!(publish(&s, &r), Err(PublicationError::Uncertain));
    for (advance, expected) in [
        (0, RetryRetention::WithinWindow),
        (10_001, RetryRetention::Expired),
    ] {
        clock.advance(advance);
        unchanged(&s, &f, &format!("uncertain-{advance}"), || {
            assert!(matches!(inspect(&s,&r).evidence,
            AtomicResultEvidence::Matching{retention,result:RetainedAtomicResult::Uncertain{recorded_code:16},..} if retention==expected))
        });
    }
    assert!(s.flags.storage_error.load(Ordering::SeqCst));
    assert!(s.state.lock().unwrap().read_only);
}

#[test]
fn exact_retry_cutoff_expiry_retired_marker_and_eviction_are_distinct_without_pruning() {
    let f = Fixture::new("retention");
    let clock = ManualClock::new(crate::now_ms());
    let s = Store::open(
        &f.state(),
        Config {
            dedup_cap: 1,
            ..config(&clock)
        },
    )
    .unwrap();
    let r = request(&s, "first", AtomicAck::Os);
    publish(&s, &r).unwrap();
    clock.advance(10_000);
    unchanged(&s, &f, "cutoff", || {
        assert_committed(
            inspect(&s, &r),
            AtomicAck::Os,
            1,
            RetryRetention::WithinWindow,
        )
    });
    clock.advance(1);
    unchanged(&s, &f, "expired-record", || {
        assert_committed(inspect(&s, &r), AtomicAck::Os, 1, RetryRetention::Expired)
    });
    let second = request(&s, "second", AtomicAck::Os);
    publish(&s, &second).unwrap();
    unchanged(&s, &f, "retired-marker", || {
        assert_eq!(
            inspect(&s, &r).evidence,
            AtomicResultEvidence::RetiredIdentity
        )
    });
    clock.advance(10_001);
    let third = request(&s, "third", AtomicAck::Os);
    publish(&s, &third).unwrap();
    unchanged(&s, &f, "evicted-marker", || {
        assert_eq!(inspect(&s, &r).evidence, AtomicResultEvidence::NotRetained)
    });
}

#[test]
fn complete_replay_requires_covering_barrier_and_preserves_os_versus_flush() {
    for ack in [AtomicAck::Os, AtomicAck::Flush] {
        let f = Fixture::new("replay");
        let clock = ManualClock::new(crate::now_ms());
        let s = Store::open(
            &f.state(),
            Config {
                fail_after_append: true,
                ..config(&clock)
            },
        )
        .unwrap();
        let r = request(&s, "lost-result", ack);
        assert_eq!(publish(&s, &r), Err(PublicationError::Uncertain));
        drop(s);
        assert!(Store::open(
            &f.state(),
            Config {
                fail_recovery_sync: true,
                ..config(&clock)
            }
        )
        .is_err());
        let s = Store::open(&f.state(), config(&clock)).unwrap();
        unchanged(&s, &f, "replayed", || {
            let i = inspect(&s, &r);
            assert_ne!(i.store_epoch, i.original_epoch);
            assert_committed(i, ack, 1, RetryRetention::WithinWindow);
        });
    }
}

#[test]
fn torn_replay_and_expired_replay_cannot_infer_an_original_success() {
    for torn in [true, false] {
        let f = Fixture::new(if torn { "torn" } else { "expired-replay" });
        let clock = ManualClock::new(crate::now_ms());
        let s = Store::open(
            &f.state(),
            Config {
                fail_partial_append: torn,
                ..config(&clock)
            },
        )
        .unwrap();
        let r = request(&s, "history", AtomicAck::Flush);
        if torn {
            assert_eq!(publish(&s, &r), Err(PublicationError::Uncertain));
        } else {
            publish(&s, &r).unwrap();
        }
        drop(s);
        if !torn {
            clock.advance(10_001);
        }
        let s = Store::open(&f.state(), config(&clock)).unwrap();
        unchanged(&s, &f, "missing-after-replay", || {
            let i = inspect(&s, &r);
            assert_ne!(i.store_epoch, i.original_epoch);
            assert_eq!(i.evidence, AtomicResultEvidence::NotRetained);
        });
        if torn {
            assert_eq!(s.slots("authored").1, 0);
        }
    }
}

#[test]
fn invalid_retained_success_association_stays_uncertain_and_poisoned_state_is_not_repaired() {
    let f = Fixture::new("invalid-outcome");
    let clock = ManualClock::new(crate::now_ms());
    let s = Arc::new(Store::open(&f.state(), config(&clock)).unwrap());
    let r = request(&s, "association", AtomicAck::Flush);
    publish(&s, &r).unwrap();
    for (version, ack) in [(0, 2), (1, 1), (1, 255), (u64::MAX, 2)] {
        s.state
            .lock()
            .unwrap()
            .dedup
            .get_mut(&("authored".into(), "association".into()))
            .unwrap()
            .outcome = Some(Outcome {
            code: 0,
            version,
            ack,
        });
        unchanged(&s, &f, &format!("invalid-{version}-{ack}"), || {
            assert!(matches!(
                inspect(&s, &r).evidence,
                AtomicResultEvidence::Matching {
                    result: RetainedAtomicResult::InvalidOutcome,
                    ..
                }
            ))
        });
    }
    let worker = {
        let s = s.clone();
        std::thread::spawn(move || {
            let _held = s.state.lock().unwrap();
            panic!("authored state poison");
        })
    };
    assert!(worker.join().is_err());
    assert!(s.state.is_poisoned());
    let scratch = super::OriginalBudget::new(RESULT_INSPECTION_SCRATCH_BYTES);
    assert_eq!(
        s.inspect_atomic_result(OriginalRef::Request(&r), &scratch),
        Err(InspectionError::StateUnavailable)
    );
    assert!(s.state.is_poisoned());
    assert_eq!(scratch.used(), 0);
    // The test fixture owner clears its injected poison solely for Store's Drop.
    // The inspection above did not clear it.
    s.state.clear_poison();
}

#[test]
fn scratch_refusal_and_shared_account_pressure_are_no_effect_and_release_on_return() {
    let f = Fixture::new("scratch");
    let clock = ManualClock::new(crate::now_ms());
    let s = Store::open(&f.state(), config(&clock)).unwrap();
    let r = request(&s, "not-admitted", AtomicAck::Flush);
    let small = super::OriginalBudget::new(RESULT_INSPECTION_SCRATCH_BYTES - 1);
    let exact = super::OriginalBudget::new(RESULT_INSPECTION_SCRATCH_BYTES);
    unchanged(&s, &f, "scratch-refused", || {
        assert_eq!(
            s.inspect_atomic_result(OriginalRef::Request(&r), &small),
            Err(InspectionError::ScratchUnavailable)
        )
    });
    assert_eq!(small.used(), 0);
    let held = exact.reserve(RESULT_INSPECTION_SCRATCH_BYTES).unwrap();
    unchanged(&s, &f, "scratch-in-use", || {
        assert_eq!(
            s.inspect_atomic_result(OriginalRef::Request(&r), &exact),
            Err(InspectionError::ScratchUnavailable)
        )
    });
    assert_eq!(exact.used(), RESULT_INSPECTION_SCRATCH_BYTES);
    drop(held);
    unchanged(&s, &f, "scratch-exact", || {
        assert_eq!(
            s.inspect_atomic_result(OriginalRef::Request(&r), &exact)
                .unwrap()
                .evidence,
            AtomicResultEvidence::NotRetained
        )
    });
    assert_eq!(exact.used(), 0);
}

#[test]
fn borrowed_identity_matches_tuple_hash_boundaries_without_table_scans() {
    use std::collections::{HashMap, HashSet};
    let keys = [
        ("a", "bc"),
        ("ab", "c"),
        ("a/b", "c"),
        ("a", "b/c"),
        ("é", "e\u{301}"),
        ("e\u{301}", "é"),
    ];
    let map: HashMap<Key, usize> = keys
        .iter()
        .enumerate()
        .map(|(i, (ns, id))| ((ns.to_string(), id.to_string()), i))
        .collect();
    let set: HashSet<Key> = map.keys().cloned().collect();
    for (i, key) in keys.iter().enumerate() {
        let query = key as &dyn Identity;
        assert_eq!(map.get(query), Some(&i));
        assert!(set.contains(query));
    }
    assert!(!map.contains_key(&("a", "b") as &dyn Identity));
}

#[test]
fn independent_absent_original_oracle_detects_the_effecting_publish_substitute() {
    let f = Fixture::new("negative-control");
    let clock = ManualClock::new(crate::now_ms());
    let s = Store::open(&f.state(), config(&clock)).unwrap();
    let r = request(&s, "unsent", AtomicAck::Os);
    let before = observe(&s, &f);
    assert_eq!(inspect(&s, &r).evidence, AtomicResultEvidence::NotRetained);
    // The tempting substitute really first-admits on a miss. The same unchanged
    // state/missing-evidence oracle must reject it, rather than calling it lookup.
    publish(&s, &r).unwrap();
    let after = observe(&s, &f);
    assert_ne!(before, after);
    assert_ne!(inspect(&s, &r).evidence, AtomicResultEvidence::NotRetained);
    fs::write(
        f.0.join("effecting-substitute-before.txt"),
        format!("{before:#?}"),
    )
    .unwrap();
    fs::write(
        f.0.join("effecting-substitute-after.txt"),
        format!("{after:#?}"),
    )
    .unwrap();
}

fn copy_authored_history(source: &Path, target: &Path) {
    fs::create_dir_all(target).unwrap();
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let to = target.join(entry.file_name());
        if entry.path().is_dir() {
            copy_authored_history(&entry.path(), &to)
        } else {
            fs::copy(entry.path(), to).unwrap();
        }
    }
}

#[test]
fn equal_epoch_copied_histories_both_report_receipts_without_authenticating_an_incarnation() {
    let source = Fixture::new("clone-source");
    let a = Fixture::new("clone-a");
    let b = Fixture::new("clone-b");
    let clock = ManualClock::new(crate::now_ms());
    let s = Store::open(&source.state(), config(&clock)).unwrap();
    let r = request(&s, "copied-receipt", AtomicAck::Flush);
    publish(&s, &r).unwrap();
    drop(s);
    // Deliberate authored counterexample, not a recovery/serving procedure.
    // The operation under test only inspects the two already-open fixtures.
    copy_authored_history(&source.state(), &a.state());
    copy_authored_history(&source.state(), &b.state());
    let sa = Store::open(&a.state(), config(&clock)).unwrap();
    let sb = Store::open(&b.state(), config(&clock)).unwrap();
    assert_eq!(sa.epoch, sb.epoch);
    unchanged(&sa, &a, "clone-a-query", || {
        assert_committed(
            inspect(&sa, &r),
            AtomicAck::Flush,
            1,
            RetryRetention::WithinWindow,
        )
    });
    unchanged(&sb, &b, "clone-b-query", || {
        assert_committed(
            inspect(&sb, &r),
            AtomicAck::Flush,
            1,
            RetryRetention::WithinWindow,
        )
    });
    assert_eq!(inspect(&sa, &r), inspect(&sb, &r));
}
