//! Persisted counter bounds and exhausted allocation remain recoverable without wraparound.
use super::*;

mod catalogue_identities {
    use super::*;

    fn repeated_open(state: chunks::ChunkState, changed_label: bool) {
        let f = Fixture::new();
        let manual = ManualClock::new(UTC);
        let path = raw_chunk(&f, &manual, 8, false);
        let bytes = fs::read(&path).unwrap();
        let cat_path = f.0.join("chunks/catalogue.log");
        let mut cat = chunks::Catalogue::open(&cat_path).unwrap();
        let rec = match state {
            chunks::ChunkState::Closed => Some(chunks::Rec::Close {
                seq: 1, bytes: (bytes.len() - 3) as u64, next: 9,
            }),
            chunks::ChunkState::Retired => {
                for rec in [
                    chunks::Rec::Close { seq: 1, bytes: (bytes.len() - 3) as u64, next: 9 },
                    chunks::Rec::Retiring { seq: 1, next: 9, horizons: Vec::new(), expired: Vec::new() },
                ] { cat.append(&chunks::CatRecord { at_ms: UTC, rec }).unwrap(); }
                Some(chunks::Rec::Retired { seq: 1 })
            },
            _ => None,
        };
        if let Some(rec) = rec { cat.append(&chunks::CatRecord { at_ms: UTC, rec }).unwrap(); }
        let m = mutation("counter-record", u64::MAX);
        cat.append(&chunks::CatRecord { at_ms: UTC + 1, rec: chunks::Rec::Open {
            seq: 1, stripe: stripe(&m.ns, &m.key) as u8, class: Class::FOREVER,
            policy: if changed_label { "changed-label" } else { "forever" }.into(), next: 9,
        }}).unwrap();
        drop(cat);
        let catalogue = fs::read(&cat_path).unwrap();
        let error = checked_open(&f, f.config(&manual)).err()
            .unwrap_or_else(|| panic!("duplicate Open must refuse; original torn tail remains={}",
                                      fs::read(&path).unwrap() == bytes));
        assert_refused(&f, &error, "duplicate catalogue Open for chunk 1");
        assert_eq!(fs::read(cat_path).unwrap(), catalogue);
        assert_eq!(fs::read(path).unwrap(), bytes, "ambiguous Open cannot authorize tail repair");
        assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 1u64.to_le_bytes());
    }

    #[test]
    fn repeated_open_refuses_without_repair_or_identity_revival() {
        for (state, changed_label) in [
            (chunks::ChunkState::Open, false), (chunks::ChunkState::Closed, false),
            (chunks::ChunkState::Retired, false), (chunks::ChunkState::Open, true),
        ] { repeated_open(state, changed_label); }
    }

    #[test]
    fn live_list_rewrites_keep_one_open_and_original_flush_value_and_retry() {
        let f = Fixture::new();
        let manual = ManualClock::new(UTC);
        let mut store = f.open(&manual);
        let m = Mutation { ack: 2, ..mutation("rewritten-open", u64::MAX) };
        let outcome = store.mutate(m.clone());
        assert_eq!(outcome.code, 0);
        for epoch in [2, 3] {
            let next = store.state.lock().unwrap().next;
            store.chunks.lock().unwrap().rewrite_live(UTC, next).unwrap();
            let (records, torn) = chunks::read_catalogue(&f.0.join("chunks/catalogue.log")).unwrap();
            assert!(torn.is_none());
            assert_eq!(records.iter().filter(|r| matches!(&r.rec, chunks::Rec::Open { seq: 1, .. })).count(), 1);
            drop(store);
            f.unlock();
            store = f.open(&manual);
            assert_eq!(store.epoch, epoch);
            assert_eq!(store.get(&m.ns, &m.key).unwrap().version, outcome.version);
            assert_eq!(store.mutate(m.clone()), outcome);
        }
    }
}

mod catalogue_stripes {
    use super::*;

    fn invalid_stripe(si: u8, state: chunks::ChunkState) {
        let f = Fixture::new();
        let manual = ManualClock::new(UTC);
        drop(f.open(&manual));
        f.unlock();
        let cat_path = f.0.join("chunks/catalogue.log");
        let mut cat = chunks::Catalogue::open(&cat_path).unwrap();
        cat.append(&chunks::CatRecord { at_ms: UTC, rec: chunks::Rec::Open {
            seq: 1, stripe: si, class: Class::FOREVER, policy: "forever".into(), next: 1,
        }}).unwrap();
        let rec = match state {
            chunks::ChunkState::Open => None,
            chunks::ChunkState::Closed => Some(chunks::Rec::Close { seq: 1, bytes: 0, next: 1 }),
            chunks::ChunkState::Retiring => Some(chunks::Rec::Retiring {
                seq: 1, next: 1, horizons: Vec::new(), expired: Vec::new(),
            }),
            chunks::ChunkState::Retired => Some(chunks::Rec::Retired { seq: 1 }),
        };
        if let Some(rec) = rec { cat.append(&chunks::CatRecord { at_ms: UTC, rec }).unwrap(); }
        drop(cat);
        let before = fs::read(&cat_path).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            Store::open(&f.0, f.config(&manual))
        }));
        assert!(result.is_ok(), "invalid catalogue stripe must return an error without panicking");
        let error = result.unwrap().err().expect("out-of-range stripe must refuse");
        assert_refused(&f, &error, "catalogue Open stripe out of range");
        assert_eq!(fs::read(cat_path).unwrap(), before);
        assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 1u64.to_le_bytes());
        assert!(chunks::list_chunk_files(&f.0.join("chunks")).unwrap().is_empty());
    }

    #[test]
    fn invalid_stripes_refuse_before_recovery_effects_in_every_state() {
        for (stripe, state) in [
            (4, chunks::ChunkState::Open), (255, chunks::ChunkState::Closed),
            (4, chunks::ChunkState::Retired), (255, chunks::ChunkState::Retiring),
        ] { invalid_stripe(stripe, state); }
    }

    #[test]
    fn all_four_valid_retired_stripes_keep_their_original_report_slots() {
        let f = Fixture::new();
        let manual = ManualClock::new(UTC);
        drop(f.open(&manual));
        f.unlock();
        let mut cat = chunks::Catalogue::open(&f.0.join("chunks/catalogue.log")).unwrap();
        for si in 0..4u8 {
            let seq = u64::from(si) + 1;
            cat.append(&chunks::CatRecord { at_ms: UTC, rec: chunks::Rec::Open {
                seq, stripe: si, class: Class::FOREVER, policy: "forever".into(), next: 1,
            }}).unwrap();
            cat.append(&chunks::CatRecord { at_ms: UTC, rec: chunks::Rec::Retired { seq } }).unwrap();
        }
        drop(cat);
        let store = f.open(&manual);
        assert!(store.replay_report().stripes.iter().all(|row| row.retired_skipped == 1));
        assert_eq!(store.epoch, 2);
        assert_eq!(store.mutate(mutation("after-retired-stripes", u64::MAX)).code, 0);
    }

    #[test]
    fn ordinary_values_and_retries_survive_restart_on_every_valid_stripe() {
        let f = Fixture::new();
        let manual = ManualClock::new(UTC);
        let store = f.open(&manual);
        let mut originals = Vec::new();
        for si in 0..4 {
            let key = (0..200).map(|n| format!("stripe-key-{n}"))
                .find(|key| stripe("clock", key) == si).unwrap();
            let m = Mutation { key, ack: 2, ..mutation(&format!("stripe-{si}"), u64::MAX) };
            let outcome = store.mutate(m.clone());
            assert_eq!(outcome.code, 0);
            originals.push((m, outcome));
        }
        drop(store);
        f.unlock();
        let store = f.open(&manual);
        for (m, outcome) in originals {
            assert_eq!(store.get(&m.ns, &m.key).unwrap().version, outcome.version);
            assert_eq!(store.mutate(m), outcome);
        }
    }
}

mod zero_chunks {
    use super::*;

    fn reserved_open(state: chunks::ChunkState, materialized: bool) {
        let f = Fixture::new();
        let manual = ManualClock::new(UTC);
        drop(f.open(&manual));
        f.unlock();
        let m = mutation("zero-chunk", u64::MAX);
        let si = stripe(&m.ns, &m.key);
        let path = chunks::chunk_path(&f.0.join("chunks"), Class::FOREVER, si, 0);
        let bytes = frame(&m, 7);
        if materialized {
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(&path, &bytes).unwrap();
        }
        let cat_path = f.0.join("chunks/catalogue.log");
        let mut cat = chunks::Catalogue::open(&cat_path).unwrap();
        cat.append(&chunks::CatRecord { at_ms: UTC, rec: chunks::Rec::Open {
            seq: 0, stripe: si as u8, class: Class::FOREVER, policy: "forever".into(), next: 8,
        }}).unwrap();
        if state == chunks::ChunkState::Closed {
            cat.append(&chunks::CatRecord { at_ms: UTC, rec: chunks::Rec::Close {
                seq: 0, bytes: bytes.len() as u64, next: 8,
            }}).unwrap();
        } else if state == chunks::ChunkState::Retired {
            cat.append(&chunks::CatRecord { at_ms: UTC, rec: chunks::Rec::Retired { seq: 0 }}).unwrap();
        }
        drop(cat);
        let catalogue = fs::read(&cat_path).unwrap();
        let result = checked_open(&f, f.config(&manual));
        let ready = result.is_ok();
        let error = result.err().unwrap_or_else(|| panic!(
            "reserved Open must refuse; startup ready={ready}, materialized input remains={}", path.exists()
        ));
        assert_refused(&f, &error, "reserved legacy chunk identifier zero");
        assert_eq!(fs::read(cat_path).unwrap(), catalogue);
        assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 1u64.to_le_bytes());
        if materialized {
            assert_eq!(fs::read(&path).unwrap(), bytes, "refusal must not delete a populated file as empty");
        } else {
            assert!(!path.exists());
        }
    }

    #[test]
    fn zero_open_with_data_refuses_instead_of_deleting_it_as_empty() {
        reserved_open(chunks::ChunkState::Open, true);
    }
    #[test]
    fn zero_open_without_a_file_refuses_before_epoch_or_catalogue_changes() {
        reserved_open(chunks::ChunkState::Open, false);
    }
    #[test]
    fn zero_closed_open_refuses_instead_of_publishing_a_missing_value() {
        reserved_open(chunks::ChunkState::Closed, true);
    }
    #[test]
    fn zero_retired_open_still_refuses_before_publication() {
        reserved_open(chunks::ChunkState::Retired, false);
    }

    #[test]
    fn internal_legacy_aggregate_zero_remains_valid_without_a_catalogue_open() {
        let f = Fixture::new();
        let manual = ManualClock::new(UTC);
        let m = mutation("legacy-aggregate", u64::MAX);
        f.legacy(&[(m.clone(), 7, UTC)]);
        let dir = f.0.join("chunks");
        let mut rp = Replayed::empty(dir.clone());
        replay_into(&mut rp, &f.0, &dir, true, &f.config(&manual),
                    &chunks::Clock::new(Some(manual))).unwrap();
        assert!(rp.chunks.table.contains_key(&chunks::LEGACY_CHUNK));
        let value = rp.committed[&("clock".into(), "key".into())].read().unwrap();
        assert_eq!(value.version, 7);
        assert_eq!(value.bytes, m.bytes);
        assert_eq!(rp.next, 8);
        assert_eq!(rp.dedup[&dedup_identity(&m)].outcome.as_ref().unwrap().version, 7);
    }

}

mod zero_revisions {
    use super::*;

    #[test]
    fn zero_legacy_revision_refuses_before_import_or_epoch() {
        let f = Fixture::new();
        let manual = ManualClock::new(UTC);
        f.legacy(&[(mutation("reserved-zero", u64::MAX), 0, UTC)]);
        let originals: Vec<_> = legacy_files(&f.0).into_iter()
            .map(|(_, p)| (p.clone(), fs::read(p).unwrap())).collect();
        let error = checked_open(&f, f.config(&manual)).err().expect("revision zero is reserved");
        assert_refused(&f, &error, "reserved zero record version");
        assert!(!f.0.join("epoch").exists());
        assert!(!f.0.join("legacy-1").exists());
        assert!(!f.0.join("chunks.import").exists());
        for (path, bytes) in originals {
            assert_eq!(fs::read(path).unwrap(), bytes);
        }
    }

    fn chunk_revision_zero(hidden: bool, closed: bool) {
        let f = Fixture::new();
        let manual = ManualClock::new(UTC);
        let path = raw_chunk(&f, &manual, 0, hidden);
        if closed {
            let mut bytes = fs::read(&path).unwrap();
            bytes.truncate(bytes.len() - 3);
            fs::write(&path, &bytes).unwrap();
            let mut cat = chunks::Catalogue::open(&f.0.join("chunks/catalogue.log")).unwrap();
            cat.append(&chunks::CatRecord { at_ms: UTC,
                rec: chunks::Rec::Close { seq: 1, bytes: bytes.len() as u64, next: 8 }
            }).unwrap();
        }
        let bytes = fs::read(&path).unwrap();
        let cat_path = f.0.join("chunks/catalogue.log");
        let catalogue = fs::read(&cat_path).unwrap();
        let error = checked_open(&f, f.config(&manual)).err().expect("revision zero must refuse");
        assert_refused(&f, &error, "reserved zero record version");
        assert!(error.to_string().contains(&path.display().to_string()));
        assert_eq!(fs::read(&path).unwrap(), bytes, "input and torn tail stay intact");
        assert_eq!(fs::read(cat_path).unwrap(), catalogue);
        assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 1u64.to_le_bytes());
    }

    #[test]
    fn zero_open_chunk_revision_refuses_with_valid_prefix_and_torn_tail() {
        chunk_revision_zero(false, false);
    }

    #[test]
    fn zero_hidden_revision_still_refuses_with_its_original_bytes() {
        chunk_revision_zero(true, false);
    }

    #[test]
    fn zero_closed_chunk_revision_refuses_without_changing_inputs() {
        chunk_revision_zero(false, true);
    }

}

fn checked_open(f: &Fixture, config: Config) -> io::Result<Store> {
    let result =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| Store::open(&f.0, config)));
    assert!(
        result.is_ok(),
        "counter exhaustion must return an error, not panic"
    );
    result.unwrap()
}
fn assert_refused(f: &Fixture, error: &io::Error, reason: &str) {
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains(reason), "{error}");
    let report = fs::read_to_string(f.0.join("replay.json")).unwrap();
    assert!(report.contains("\"stage\":\"refused\""), "{report}");
    assert!(report.contains(reason), "{report}");
    assert_eq!(elapsed_json_number(&report, "ready_ms"), 0);
    let history = fs::read_to_string(f.0.join("replay.log")).unwrap();
    assert_eq!(history.lines().last(), Some(report.trim_end()));
}
fn frame(m: &Mutation, version: u64) -> Vec<u8> {
    let body = mutation_bytes(m, version, UTC);
    let mut frame = b"MBK1".to_vec();
    frame.extend((body.len() as u32).to_le_bytes());
    frame.extend((!(body.len() as u32)).to_le_bytes());
    frame.extend(crc32(&body).to_le_bytes());
    frame.extend(body);
    frame
}
fn raw_chunk(f: &Fixture, manual: &Arc<ManualClock>, version: u64, hidden: bool) -> PathBuf {
    drop(f.open(manual));
    f.unlock();
    let m = mutation("counter-record", u64::MAX);
    let stripe = stripe(&m.ns, &m.key);
    let path = chunks::chunk_path(&f.0.join("chunks"), Class::FOREVER, stripe, 1);
    let mut cat = chunks::Catalogue::open(&f.0.join("chunks/catalogue.log")).unwrap();
    cat.append(&chunks::CatRecord {
        at_ms: UTC,
        rec: chunks::Rec::Open {
            seq: 1,
            stripe: stripe as u8,
            class: Class::FOREVER,
            policy: "forever".into(),
            next: 1,
        },
    })
    .unwrap();
    if hidden {
        cat.append(&chunks::CatRecord {
            at_ms: UTC,
            rec: chunks::Rec::Horizons(vec![(m.ns.clone(), m.key.clone(), u64::MAX)]),
        })
        .unwrap();
    }
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut bytes = frame(&mutation("valid-prefix", u64::MAX), 7);
    bytes.extend(frame(&m, version));
    bytes.extend(b"MBK");
    fs::write(&path, bytes).unwrap();
    path
}

#[test]
fn maximum_legacy_revision_refuses_without_moving_sources_or_publishing_epoch() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    f.legacy(&[(mutation("maximum", u64::MAX), u64::MAX, UTC)]);
    let paths = legacy_files(&f.0);
    let before: Vec<_> = paths.iter().map(|(_, p)| fs::read(p).unwrap()).collect();
    let error = checked_open(&f, f.config(&manual))
        .err()
        .expect("maximum revision has no successor");
    assert_refused(&f, &error, "record version has no successor");
    assert!(!f.0.join("epoch").exists());
    assert!(!f.0.join("legacy-1").exists());
    assert!(!f.0.join("chunks.import").exists());
    for ((_, path), bytes) in paths.iter().zip(before) {
        assert_eq!(fs::read(path).unwrap(), bytes);
        assert!(!fs::metadata(path).unwrap().permissions().readonly());
    }
}

fn maximum_chunk_revision(hidden: bool) {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let path = raw_chunk(&f, &manual, u64::MAX, hidden);
    let bytes = fs::read(&path).unwrap();
    let catalogue = fs::read(f.0.join("chunks/catalogue.log")).unwrap();
    let error = checked_open(&f, f.config(&manual))
        .err()
        .expect("maximum revision must refuse");
    assert_refused(&f, &error, "record version has no successor");
    assert_eq!(
        fs::read(&path).unwrap(),
        bytes,
        "refusal precedes torn-tail repair"
    );
    assert_eq!(
        fs::read(f.0.join("chunks/catalogue.log")).unwrap(),
        catalogue
    );
    assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 1u64.to_le_bytes());
    let report = fs::read_to_string(f.0.join("replay.json")).unwrap();
    assert_eq!(
        elapsed_json_number(&report, "keys"),
        if hidden { 0 } else { 1 }
    );
    assert_eq!(
        elapsed_json_number(&report, "records"),
        1,
        "only the valid prefix is observed"
    );
}
#[test]
fn maximum_chunk_revision_refuses_with_the_valid_prefix_and_unrepaired_tail() {
    maximum_chunk_revision(false);
}
#[test]
fn maximum_hidden_revision_refuses_instead_of_wrapping_the_high_water() {
    maximum_chunk_revision(true);
}

#[test]
fn exhausted_import_policy_revision_refuses_before_the_switch_and_can_retry_without_policy() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let original = mutation("last", u64::MAX);
    f.legacy(&[(original.clone(), u64::MAX - 1, UTC)]);
    let policy = f.0.join("import-policies.txt");
    fs::write(&policy, b"clock\t\tlatest\n").unwrap();
    let paths = legacy_files(&f.0);
    let before: Vec<_> = paths.iter().map(|(_, p)| fs::read(p).unwrap()).collect();
    let error = checked_open(&f, f.config(&manual))
        .err()
        .expect("synthetic revision exhausted");
    assert_refused(&f, &error, "import version exhausted");
    assert!(!f.0.join("epoch").exists());
    assert!(!f.0.join("legacy-1").exists());
    assert!(!f.0.join("chunks.import").exists());
    assert_eq!(fs::read(&policy).unwrap(), b"clock\t\tlatest\n");
    for ((_, path), bytes) in paths.iter().zip(before) {
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
    fs::remove_file(policy).unwrap();
    f.unlock();
    let store = checked_open(&f, f.config(&manual)).unwrap();
    assert_eq!(store.mutate(original).version, u64::MAX - 1);
}

#[test]
fn exhausted_live_revision_refuses_new_work_but_preserves_reads_retries_and_restart() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let original = Mutation {
        ack: 2,
        ..mutation("last", u64::MAX)
    };
    f.legacy(&[(original.clone(), u64::MAX - 1, UTC)]);
    for epoch in [1, 2] {
        let store = checked_open(&f, f.config(&manual)).unwrap();
        let catalogue = fs::read(f.0.join("chunks/catalogue.log")).unwrap();
        assert_eq!(store.epoch, epoch);
        assert_eq!(store.mutate(original.clone()).version, u64::MAX - 1);
        let fresh = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            store.mutate(mutation("exhausted", u64::MAX))
        }));
        assert!(
            fresh.is_ok(),
            "new work at an exhausted revision must not panic"
        );
        assert_eq!(
            fresh.unwrap().code,
            7,
            "no representable successor means capacity refusal"
        );
        assert_eq!(store.state.lock().unwrap().next, u64::MAX);
        assert_eq!(store.get("clock", "key").unwrap().version, u64::MAX - 1);
        assert!(!store.health2().read_only);
        assert_eq!(
            fs::read(f.0.join("chunks/catalogue.log")).unwrap(),
            catalogue
        );
        drop(store);
        f.unlock();
    }
}

#[test]
fn last_assignable_revision_and_ordinary_revisions_keep_their_original_outcomes() {
    for version in [7, u64::MAX - 2] {
        let f = Fixture::new();
        let manual = ManualClock::new(UTC);
        let original = Mutation {
            ack: 2,
            ..mutation("previous", u64::MAX)
        };
        f.legacy(&[(original.clone(), version, UTC)]);
        let store = checked_open(&f, f.config(&manual)).unwrap();
        assert_eq!(store.mutate(original).version, version);
        let next = Mutation {
            ack: 2,
            ..mutation("next", u64::MAX)
        };
        let outcome = store.mutate(next.clone());
        assert_eq!((outcome.code, outcome.version), (0, version + 1));
        drop(store);
        f.unlock();
        let store = checked_open(&f, f.config(&manual)).unwrap();
        assert_eq!(store.mutate(next).version, version + 1);
        assert_eq!(store.get("clock", "key").unwrap().version, version + 1);
    }
}

mod chunk_ids {
    use super::*;

    #[test]
    fn maximum_catalogue_chunk_id_refuses_before_cleanup_or_epoch() {
        let f = Fixture::new();
        let manual = ManualClock::new(UTC);
        drop(f.open(&manual));
        f.unlock();
        let cat_path = f.0.join("chunks/catalogue.log");
        let mut catalogue = chunks::Catalogue::open(&cat_path).unwrap();
        catalogue
            .append(&chunks::CatRecord {
                at_ms: UTC,
                rec: chunks::Rec::Open {
                    seq: u64::MAX,
                    stripe: 0,
                    class: Class::FOREVER,
                    policy: "forever".into(),
                    next: 1,
                },
            })
            .unwrap();
        drop(catalogue);
        let bytes = fs::read(&cat_path).unwrap();
        let error = checked_open(&f, f.config(&manual))
            .err()
            .expect("chunk id has no successor");
        assert_refused(&f, &error, "chunk identifier has no successor");
        assert_eq!(fs::read(&cat_path).unwrap(), bytes);
        assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 1u64.to_le_bytes());
        assert!(chunks::list_chunk_files(&f.0.join("chunks"))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn exhausted_live_chunk_id_refuses_before_open_publication_or_route_creation() {
        let f = Fixture::new();
        let manual = ManualClock::new(UTC);
        let store = f.open(&manual);
        store.chunks.lock().unwrap().next_id = u64::MAX;
        let before = fs::read(f.0.join("chunks/catalogue.log")).unwrap();
        for _ in 0..2 {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                store
                    .router
                    .open_chunk(0, Class::FOREVER, "forever".into(), &f.config(&manual))
            }));
            assert!(result.is_ok(), "chunk allocation must refuse, not panic");
            let error = result
                .unwrap()
                .err()
                .expect("exhausted chunk id must refuse");
            assert!(
                error.to_string().contains("chunk identifier exhausted"),
                "{error}"
            );
            assert_eq!(store.chunks.lock().unwrap().next_id, u64::MAX);
            assert!(store.router.table.read().unwrap().is_empty());
            assert_eq!(fs::read(f.0.join("chunks/catalogue.log")).unwrap(), before);
            assert!(chunks::list_chunk_files(&f.0.join("chunks"))
                .unwrap()
                .is_empty());
        }
    }

    #[test]
    fn public_mutation_at_an_exhausted_chunk_id_preserves_the_previous_flush_value() {
        let f = Fixture::new();
        let manual = ManualClock::new(UTC);
        let store = f.open(&manual);
        let original = Mutation {
            ack: 2,
            ..mutation("retained", u64::MAX)
        };
        let outcome = store.mutate(original.clone());
        assert_eq!(outcome.code, 0);
        let key = (0..100)
            .map(|n| format!("fresh-{n}"))
            .find(|key| stripe("clock", key) != stripe("clock", "key"))
            .unwrap();
        store.chunks.lock().unwrap().next_id = u64::MAX;
        let before = fs::read(f.0.join("chunks/catalogue.log")).unwrap();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            store.mutate(Mutation {
                key,
                ack: 2,
                ..mutation("exhausted-chunk", u64::MAX)
            })
        }));
        assert!(
            result.is_ok(),
            "allocation failure must leave healthy synchronization state"
        );
        assert_eq!(result.unwrap().code, 16);
        assert_eq!(store.get("clock", "key").unwrap().version, outcome.version);
        assert_eq!(store.mutate(original).version, outcome.version);
        assert!(store.health2().read_only);
        assert_eq!(fs::read(f.0.join("chunks/catalogue.log")).unwrap(), before);
    }

    fn writer(f: &Fixture, id: u64) -> ImportWriter {
        let dir = f.0.join("candidate");
        fs::create_dir_all(&dir).unwrap();
        let mut cs = chunks::ChunkStore::new(dir.clone());
        cs.cat = Some(chunks::Catalogue::open(&dir.join("catalogue.log")).unwrap());
        cs.next_id = id;
        ImportWriter {
            cs,
            open: HashMap::new(),
            chunk_bytes: u64::MAX,
            utc: UTC,
            next: 2,
            written: 0,
        }
    }

    #[test]
    fn exhausted_import_chunk_id_refuses_without_a_record_file_or_counter_wrap() {
        let f = Fixture::new();
        let mut writer = writer(&f, u64::MAX);
        let cat = writer.cs.dir.join("catalogue.log");
        let before = fs::read(&cat).unwrap();
        let m = mutation("candidate", u64::MAX);
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            writer.write(
                stripe(&m.ns, &m.key),
                Class::FOREVER,
                "forever",
                &mutation_bytes(&m, 1, UTC),
            )
        }));
        assert!(result.is_ok(), "candidate allocation must return an error");
        let error = result
            .unwrap()
            .err()
            .expect("exhausted chunk id must refuse");
        assert!(
            error.to_string().contains("chunk identifier exhausted"),
            "{error}"
        );
        assert_eq!(writer.cs.next_id, u64::MAX);
        assert_eq!(writer.written, 0);
        assert!(writer.open.is_empty());
        assert_eq!(fs::read(cat).unwrap(), before);
        assert!(chunks::list_chunk_files(&writer.cs.dir).unwrap().is_empty());
    }

    #[test]
    fn last_assignable_live_chunk_retains_value_and_retry_after_restart() {
        let f = Fixture::new();
        let manual = ManualClock::new(UTC);
        let store = f.open(&manual);
        store.chunks.lock().unwrap().next_id = u64::MAX - 1;
        let original = Mutation {
            ack: 2,
            ..mutation("last-chunk", u64::MAX)
        };
        let outcome = store.mutate(original.clone());
        assert_eq!(outcome.code, 0);
        assert_eq!(store.chunks.lock().unwrap().next_id, u64::MAX);
        drop(store);
        f.unlock();
        let store = checked_open(&f, f.config(&manual)).unwrap();
        assert_eq!(store.chunks.lock().unwrap().next_id, u64::MAX);
        assert_eq!(store.get("clock", "key").unwrap().version, outcome.version);
        assert_eq!(store.mutate(original).version, outcome.version);
    }

    #[test]
    fn last_assignable_import_chunk_replays_without_wrapping() {
        let f = Fixture::new();
        let manual = ManualClock::new(UTC);
        let mut writer = writer(&f, u64::MAX - 1);
        let m = mutation("candidate-last", u64::MAX);
        writer
            .write(
                stripe(&m.ns, &m.key),
                Class::FOREVER,
                "forever",
                &mutation_bytes(&m, 1, UTC),
            )
            .unwrap();
        writer.close_all().unwrap();
        assert_eq!(writer.cs.next_id, u64::MAX);
        let mut replayed = Replayed::empty(writer.cs.dir.clone());
        replay_into(
            &mut replayed,
            &f.0,
            &writer.cs.dir,
            false,
            &f.config(&manual),
            &chunks::Clock::new(Some(manual)),
        )
        .unwrap();
        assert_eq!(replayed.chunks.next_id, u64::MAX);
        assert_eq!(
            replayed.committed[&("clock".into(), "key".into())].version,
            1
        );
    }
}
