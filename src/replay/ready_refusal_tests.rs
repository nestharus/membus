//! Failed final report publication must leave non-ready diagnostics when reporting is possible.
use super::*;

fn seed(f: &Fixture, clock: &Arc<ManualClock>) -> Mutation {
    let original = Mutation {
        ack: 2,
        ..mutation("ready-original", u64::MAX)
    };
    let store = f.open(clock);
    assert_eq!(store.mutate(original.clone()).code, 0);
    drop(store);
    f.unlock();
    original
}

#[test]
fn ready_directory_failures_record_refusal_with_epoch_populations_and_elapsed_time() {
    for stop in 1..=3 {
        let f = Fixture::new();
        let manual = ManualClock::new(UTC);
        let original = seed(&f, &manual);
        fs::write(f.0.join("replay.log"), "previous history\n".repeat(70_000)).unwrap();
        let root = f.0.clone();
        let clock = manual.clone();
        let ready_calls = Arc::new(AtomicU64::new(0));
        let refused_calls = Arc::new(AtomicU64::new(0));
        let ready = ready_calls.clone();
        let refused = refused_calls.clone();
        let mut config = f.config(&manual);
        config.replay_report_dir_sync = Some(Arc::new(move |dir| {
            assert_eq!(dir, root);
            let json = fs::read_to_string(root.join("replay.json"))?;
            if json.contains("\"stage\":\"ready\"") {
                if ready.fetch_add(1, Ordering::SeqCst) + 1 == stop {
                    clock.advance(17);
                    clock.jump_utc(50_000);
                    return Err(io::Error::from_raw_os_error(5));
                }
            } else {
                assert!(json.contains("\"stage\":\"refused\""), "{json}");
                refused.fetch_add(1, Ordering::SeqCst);
                // The original startup still owns the directory while replacing diagnostics.
                Store::open(&root, Config::default())
                    .err()
                    .expect("ownership retained");
            }
            Ok(())
        }));
        let owners = Arc::downgrade(config.replay_report_dir_sync.as_ref().unwrap());
        let error = Store::open(&f.0, config)
            .err()
            .expect("ready publication must fail");
        assert_eq!(error.raw_os_error(), Some(5));
        assert_eq!(ready_calls.load(Ordering::SeqCst), stop);
        assert!(
            owners.upgrade().is_none(),
            "all worker configurations must be released"
        );
        let json = fs::read_to_string(f.0.join("replay.json")).unwrap();
        assert!(json.contains("\"stage\":\"refused\""), "{json}");
        assert!(
            json.contains(&format!("\"refusal\":{:?}", error.to_string())),
            "{json}"
        );
        assert_eq!(elapsed_json_number(&json, "epoch"), 2);
        assert_eq!(elapsed_json_number(&json, "start_ms"), UTC);
        assert_eq!(elapsed_json_number(&json, "ready_ms"), 0);
        assert_eq!(elapsed_json_number(&json, "duration_ms"), 17);
        assert_eq!(elapsed_json_number(&json, "keys"), 1);
        assert_eq!(elapsed_json_number(&json, "dedup_entries"), 1);
        assert!(json.contains("\"namespace_counts\":{\"clock\":1}"));
        assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 2u64.to_le_bytes());
        let history = fs::read_to_string(f.0.join("replay.log")).unwrap();
        assert_eq!(history.lines().last(), Some(json.trim_end()));
        assert!(refused_calls.load(Ordering::SeqCst) >= 2);
        f.unlock();
        // Correcting UTC does not change the elapsed duration captured in the refusal.
        manual.jump_utc(-50_000);
        let store = f.open(&manual);
        assert_eq!(store.epoch, 3);
        assert_eq!(store.get("clock", "key").unwrap().bytes, original.bytes);
        assert_eq!(store.mutate(original).version, 1);
    }
}

#[test]
fn secondary_refusal_failure_preserves_the_original_publication_error() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    seed(&f, &manual);
    let root = f.0.clone();
    let refused = Arc::new(AtomicU64::new(0));
    let seen = refused.clone();
    let mut config = f.config(&manual);
    config.replay_report_dir_sync = Some(Arc::new(move |_| {
        if fs::read_to_string(root.join("replay.json"))?.contains("\"stage\":\"ready\"") {
            Err(io::Error::from_raw_os_error(5))
        } else {
            seen.fetch_add(1, Ordering::SeqCst);
            Err(io::Error::from_raw_os_error(32))
        }
    }));
    let error = Store::open(&f.0, config)
        .err()
        .expect("publication must fail");
    assert_eq!(error.raw_os_error(), Some(5));
    assert_eq!(
        refused.load(Ordering::SeqCst),
        1,
        "one best-effort refusal attempt"
    );
    assert!(fs::read_to_string(f.0.join("replay.json"))
        .unwrap()
        .contains("\"stage\":\"refused\""));
}

#[cfg(windows)]
#[test]
fn actual_history_write_denial_replaces_ready_snapshot_and_allows_retry() {
    use std::os::windows::fs::OpenOptionsExt;
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let original = seed(&f, &manual);
    let previous = fs::read(f.0.join("replay.log")).unwrap();
    let held = OpenOptions::new()
        .read(true)
        .share_mode(1)
        .open(f.0.join("replay.log"))
        .unwrap();
    let error = Store::open(&f.0, f.config(&manual))
        .err()
        .expect("history append denied");
    assert_eq!(error.raw_os_error(), Some(32));
    assert_eq!(fs::read(f.0.join("replay.log")).unwrap(), previous);
    let json = fs::read_to_string(f.0.join("replay.json")).unwrap();
    assert!(json.contains("\"stage\":\"refused\""), "{json}");
    assert!(json.contains(&format!("\"refusal\":{:?}", error.to_string())));
    assert_eq!(elapsed_json_number(&json, "ready_ms"), 0);
    assert_eq!(elapsed_json_number(&json, "epoch"), 2);
    drop(held);
    let store = f.open(&manual);
    assert_eq!(store.epoch, 3);
    assert_eq!(store.get("clock", "key").unwrap().bytes, original.bytes);
    assert_eq!(store.mutate(original).version, 1);
}
