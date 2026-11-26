//! The immutable legacy backup must retain the same inclusive retry bound as ordinary replay.
use super::*;

fn imported_retry_at_boundary(window: u64, remove: bool) {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let mut config = f.config(&manual);
    config.retry_ms = window;
    let mut original = mutation("superseded", u64::MAX);
    original.ack = 2;
    original.remove = remove;
    if remove {
        original.bytes.clear();
    }
    f.legacy(&[
        (original.clone(), 7, UTC),
        (mutation("latest", u64::MAX), 8, UTC),
    ]);
    drop(Store::open(&f.0, config.clone()).unwrap());
    f.unlock();
    // Restart with a fresh local monotonic epoch, exactly at the persisted UTC limit.
    let manual = ManualClock::new(UTC + window);
    config.clock = Some(manual.clone());
    let store = Store::open(&f.0, config).unwrap();
    assert_eq!(store.epoch, 2);
    let data: Vec<_> = chunks::list_chunk_files(&f.0.join("chunks")).unwrap()
        .into_iter().map(|(_, _, _, p)| (p.clone(), fs::read(p).unwrap())).collect();
    let catalogue = fs::read(f.0.join("chunks/catalogue.log")).unwrap();
    let expected = Outcome { code: 0, version: 7, ack: 2 };
    assert_eq!(store.mutate(original.clone()), expected);
    let report = store.replay_report();
    assert_eq!(report.dedup_entries, 2, "the superseded operation exists only in the backup");
    assert!(report.import.unwrap().contains("on (within 24 h of the import)"));
    manual.jump_utc(10_000);
    assert_eq!(store.mutate(original.clone()), expected, "UTC cannot expire the recovered local bound");
    manual.jump_utc(-20_000);
    assert_eq!(store.mutate(original.clone()), expected, "UTC cannot extend the recovered local bound");
    manual.advance(1);
    assert_eq!(store.mutate(original).code, 13, "no remaining elapsed window is granted at the boundary");
    let value = store.get("clock", "key").unwrap();
    assert_eq!((value.version, value.bytes), (8, b"latest".to_vec()));
    for (path, bytes) in data {
        assert_eq!(fs::read(path).unwrap(), bytes, "retry must append no data");
    }
    assert_eq!(fs::read(f.0.join("chunks/catalogue.log")).unwrap(), catalogue);
}

#[test]
fn legacy_backup_exact_retry_boundary_keeps_superseded_put_outcome() {
    imported_retry_at_boundary(WINDOW, false);
}

#[test]
fn legacy_backup_exact_retry_boundary_keeps_superseded_remove_outcome() {
    imported_retry_at_boundary(WINDOW, true);
}

#[test]
fn legacy_backup_zero_retry_window_keeps_the_same_instant_outcome() {
    imported_retry_at_boundary(0, false);
}

#[test]
fn legacy_backup_past_retry_boundary_stops_grafting_without_replacing_values() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    f.legacy(&[
        (mutation("superseded", u64::MAX), 7, UTC),
        (mutation("latest", u64::MAX), 8, UTC),
    ]);
    drop(f.open(&manual));
    f.unlock();
    let store = f.open(&ManualClock::new(UTC + WINDOW + 1));
    assert_eq!(store.replay_report().dedup_entries, 0);
    assert!(store.replay_report().import.unwrap().contains("off (24 h passed)"));
    assert_eq!(store.get("clock", "key").unwrap().version, 8);
}

#[test]
fn legacy_backup_boundary_does_not_revive_records_already_older_than_retry_window() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    f.legacy(&[
        (mutation("superseded", u64::MAX), 7, UTC - 1),
        (mutation("latest", u64::MAX), 8, UTC - 1),
    ]);
    drop(f.open(&manual));
    f.unlock();
    let store = f.open(&ManualClock::new(UTC + WINDOW));
    assert_eq!(store.replay_report().dedup_entries, 0);
    assert_eq!(store.get("clock", "key").unwrap().version, 8);
}
