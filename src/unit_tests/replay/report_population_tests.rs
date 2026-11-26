//! Non-ready reports retain observed populations; final counts reflect startup cleanup.
use super::*;

fn number(text: &str, key: &str) -> usize {
    text.split(&format!("\"{key}\":")).nth(1).unwrap()
        .split(|c: char| !c.is_ascii_digit()).next().unwrap().parse().unwrap()
}
fn assert_report(f: &Fixture, stage: &str, keys: usize, dedup: usize, counts: &str) {
    let report = fs::read_to_string(f.0.join("replay.json")).unwrap();
    assert!(report.contains(&format!("\"stage\":\"{stage}\"")), "{report}");
    assert_eq!(number(&report, "keys"), keys, "{report}");
    assert_eq!(number(&report, "dedup_entries"), dedup, "{report}");
    assert!(report.contains(&format!("\"namespace_counts\":{{{counts}}}")), "{report}");
    let history = fs::read_to_string(f.0.join("replay.log")).unwrap();
    assert_eq!(history.lines().last(), Some(report.trim_end()));
}
fn put(store: &Store, ns: &str, key: &str, id: &str, remove: bool) {
    assert_eq!(store.mutate(Mutation {
        ns: ns.into(), key: key.into(), op_id: id.into(), deadline: u64::MAX,
        ack: 2, expected: None, bytes: if remove { vec![] } else { b"authored".to_vec() }, remove,
    }).code, 0);
}
fn seed(f: &Fixture, clock: &Arc<ManualClock>) -> Vec<PathBuf> {
    let store = f.open(clock);
    put(&store, "alpha", "a", "a1", false);
    put(&store, "alpha", "a", "a2", false);
    put(&store, "alpha", "b", "b1", false);
    put(&store, "beta", "c", "c1", false);
    put(&store, "beta", "gone", "tombstone", true);
    drop(store);
    f.unlock();
    chunks::list_chunk_files(&f.0.join("chunks")).unwrap().into_iter().map(|c| c.3).collect()
}

#[test]
fn capacity_refusal_reports_replayed_keys_retries_and_live_namespaces() {
    let f = Fixture::new();
    let clock = ManualClock::new(UTC);
    let files = seed(&f, &clock);
    let bytes: Vec<_> = files.iter().map(|p| fs::read(p).unwrap()).collect();
    let mut config = f.config(&clock);
    config.index_cap = 1;
    assert_eq!(Store::open(&f.0, config).err().unwrap().to_string(), "replayed state exceeds configured limits");
    assert_report(&f, "refused", 4, 5, "\"alpha\":2,\"beta\":1");
    assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 1u64.to_le_bytes());
    for (path, before) in files.iter().zip(bytes) { assert_eq!(fs::read(path).unwrap(), before); }
    f.unlock();
    let store = f.open(&clock);
    assert_eq!(store.epoch, 2);
    assert_eq!(store.replay_report().keys, 3, "unheld tombstone drops during final startup cleanup");
    assert_eq!(store.get("alpha", "a").unwrap().bytes, b"authored");
}

#[test]
fn truncation_pending_reports_the_complete_records_observed_before_repair() {
    let f = Fixture::new();
    let clock = ManualClock::new(UTC);
    let files = seed(&f, &clock);
    OpenOptions::new().append(true).open(&files[0]).unwrap().write_all(b"MB").unwrap();
    let mut config = f.config(&clock);
    config.stop_after_replay_truncation = true;
    assert_eq!(Store::open(&f.0, config).err().unwrap().to_string(), "test stop after replay truncation");
    assert_report(&f, "truncation_pending", 4, 5, "\"alpha\":2,\"beta\":1");
    assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 1u64.to_le_bytes());
}

#[test]
fn replay_failure_reports_only_the_valid_prefix_already_observed() {
    let f = Fixture::new();
    let clock = ManualClock::new(UTC);
    let mut config = f.config(&clock);
    config.chunk_bytes = 1;
    let store = Store::open(&f.0, config.clone()).unwrap();
    put(&store, "alpha", "a", "first", false);
    put(&store, "alpha", "a", "second", false);
    drop(store);
    f.unlock();
    let files = chunks::list_chunk_files(&f.0.join("chunks")).unwrap();
    assert_eq!(files.len(), 2);
    let second = &files.iter().max_by_key(|c| c.2).unwrap().3;
    let mut bytes = fs::read(second).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    fs::write(second, &bytes).unwrap();
    assert!(Store::open(&f.0, config).err().unwrap().to_string().contains("interior-corrupt"));
    assert_report(&f, "refused", 1, 1, "\"alpha\":1");
    assert_eq!(fs::read(second).unwrap(), bytes);
    assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 1u64.to_le_bytes());
}

fn advance_after_epoch(f: &Fixture, clock: &Arc<ManualClock>) -> Config {
    let mut config = f.config(clock);
    let root = f.0.clone();
    let advanced = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let manual = clock.clone();
    config.recovery_dir_sync = Some(Arc::new(move |dir| {
        if dir == root && fs::read(root.join("epoch"))? == 2u64.to_le_bytes()
            && !advanced.swap(true, Ordering::SeqCst) { manual.advance(WINDOW + 1); }
        Ok(())
    }));
    config
}

#[test]
fn late_control_refusal_reports_final_tombstone_and_retry_cleanup() {
    let f = Fixture::new();
    let clock = ManualClock::new(UTC);
    let store = f.open(&clock);
    put(&store, timers::NS, "s/caller\0timer", "invalid-schedule", false);
    put(&store, "beta", "gone", "tombstone", true);
    drop(store);
    f.unlock();
    let error = Store::open(&f.0, advance_after_epoch(&f, &clock)).err().unwrap();
    assert_eq!(error.to_string(), "invalid persisted timer schedule");
    assert_report(&f, "refused", 1, 0, "\"__membus_timers_v1\":1");
    assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 2u64.to_le_bytes());
}

#[test]
fn ready_report_keeps_final_cleaned_populations_after_time_advances_during_startup() {
    let f = Fixture::new();
    let clock = ManualClock::new(UTC);
    seed(&f, &clock);
    let store = Store::open(&f.0, advance_after_epoch(&f, &clock)).unwrap();
    assert_eq!(store.epoch, 2);
    assert_eq!((store.replay_report().keys, store.replay_report().dedup_entries), (3, 0));
    assert_report(&f, "ready", 3, 0, "\"alpha\":2,\"beta\":1");
}
