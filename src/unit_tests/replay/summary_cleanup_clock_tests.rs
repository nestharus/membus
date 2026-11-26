//! Startup summary cleanup observes UTC after publication I/O, before serving status or writes.
use super::*;

fn seed_summary(f: &Fixture, manual: &Arc<ManualClock>, key: &str, with_live: bool) {
    let store = f.open(manual);
    if with_live {
        assert_eq!(store.mutate(mutation("summary-live", u64::MAX)).code, 0);
    }
    store.chunks.lock().unwrap().log(UTC, chunks::Rec::Summary {
        retired_ms: UTC, keys: vec![("summary".into(), key.into())],
    }).unwrap();
    drop(store);
    f.unlock();
}

fn cleanup_after_publication(initial_elapsed: u64, advance: u64, jump: i64, retained: bool) {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    seed_summary(&f, &manual, "expired", true);
    manual.advance(initial_elapsed);
    let mut config = f.config(&manual);
    config.summary_ms = WINDOW;
    config.index_cap = 2;
    let directory = f.0.clone();
    let changed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = changed.clone();
    let during_publication = manual.clone();
    config.recovery_dir_sync = Some(Arc::new(move |path| {
        if path == directory && fs::read(directory.join("epoch"))? == 2u64.to_le_bytes()
            && !changed.swap(true, Ordering::SeqCst) {
            during_publication.advance(advance);
            during_publication.jump_utc(jump);
        }
        Ok(())
    }));
    let store = Store::open(&f.0, config).unwrap();
    assert!(observed.load(Ordering::SeqCst), "the epoch-publication barrier must move time");
    assert_eq!(store.get_status("summary", "expired"), (if retained { 1 } else { 2 }, 0));
    assert_eq!(store.get("clock", "key").unwrap().bytes, b"summary-live");
    let report = store.replay_report();
    assert_eq!(report.keys, 1 + usize::from(retained));
    assert_eq!(report.namespace_counts.get("clock"), Some(&1));
    assert_eq!(report.namespace_counts.len(), 1);
    assert_eq!(store.state.lock().unwrap().keys, report.keys);
    assert_eq!(store.chunks.lock().unwrap().summaries.charged, usize::from(retained));
    assert_eq!(store.mutate(Mutation { key: "fresh".into(), ..mutation("summary-fresh", u64::MAX) }).code,
               if retained { 9 } else { 0 }, "a retained summary owns an index slot");
    let current = fs::read_to_string(f.0.join("replay.json")).unwrap();
    assert!(current.contains(&format!("\"keys\":{}", report.keys)), "{current}");
}

#[test]
fn summary_cleanup_uses_current_utc_after_publication() {
    for (initial, advance, jump, retained) in [
        (0, WINDOW, 0, false), (0, WINDOW + 1, 0, false),
        (0, 0, WINDOW as i64, false), (WINDOW, 0, -1, true),
        (0, WINDOW - 1, 0, true), (0, WINDOW + 1, -(WINDOW as i64 + 1), true),
    ] { cleanup_after_publication(initial, advance, jump, retained); }
}
