//! Public manual-clock arithmetic must preserve elapsed-time consumers at its limits.
use super::*;

#[test]
fn manual_advance_saturates_elapsed_and_utc_without_wrapping_backward() {
    let manual = ManualClock::new(0);
    let clock = chunks::Clock::new(Some(manual.clone()));
    manual.advance(u64::MAX - 1);
    manual.advance(2);
    assert_eq!(clock.mono_ms(), u64::MAX);
    assert_eq!(clock.utc_ms(), u64::MAX);
    manual.advance(1);
    assert_eq!(clock.mono_ms(), u64::MAX);
    assert_eq!(clock.utc_ms(), u64::MAX);
}

#[test]
fn manual_advance_saturates_utc_while_elapsed_time_still_advances() {
    let manual = ManualClock::new(u64::MAX - 5);
    let clock = chunks::Clock::new(Some(manual.clone()));
    manual.advance(10);
    assert_eq!(clock.utc_ms(), u64::MAX);
    assert_eq!(clock.mono_ms(), 10);
    manual.jump_utc(-100);
    manual.advance(2);
    assert_eq!(clock.utc_ms(), u64::MAX - 98);
    assert_eq!(clock.mono_ms(), 12);
}

#[test]
fn overflowing_manual_advance_expires_an_admitted_retry() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let store = f.open(&manual);
    let original = mutation("overflowing-retry", u64::MAX);
    assert_eq!(store.mutate(original.clone()).code, 0);
    manual.advance(1);
    manual.advance(u64::MAX);
    assert_eq!(store.mutate(original).code, 13,
               "a large elapsed advance cannot restore the old retry window");
}

#[test]
fn overflowing_manual_advance_expires_a_snapshot() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let mut config = f.config(&manual);
    config.snapshot_ttl_ms = WINDOW;
    let store = Store::open(&f.0, config).unwrap();
    let (token, _) = store.snapshot_open("clock").unwrap();
    manual.advance(1);
    manual.advance(u64::MAX);
    assert!(matches!(store.snapshot_get("clock", &token), Err(20)),
            "a large elapsed advance cannot reopen the snapshot lease");
}

fn concurrent_positive_updates(mixed: bool) {
    const WORKERS: usize = 4;
    const ROUNDS: u64 = 128;
    const BATCH: u64 = 1024;
    let manual = ManualClock::new(UTC);
    let clock = chunks::Clock::new(Some(manual.clone()));
    let start = Arc::new(std::sync::Barrier::new(WORKERS));
    let workers: Vec<_> = (0..WORKERS).map(|worker| {
        let manual = manual.clone();
        let start = start.clone();
        std::thread::spawn(move || {
            for _ in 0..ROUNDS {
                start.wait();
                for _ in 0..BATCH {
                    if mixed && worker % 2 == 0 { manual.advance(1); }
                    else { manual.jump_utc(1); }
                }
            }
        })
    }).collect();
    for worker in workers { worker.join().unwrap(); }
    assert_eq!(clock.utc_ms(), UTC + WORKERS as u64 * ROUNDS * BATCH,
               "all positive UTC updates must survive, independent of their order");
    assert_eq!(clock.mono_ms(), if mixed { WORKERS as u64 / 2 * ROUNDS * BATCH } else { 0 });
}

#[test]
fn concurrent_positive_utc_corrections_preserve_every_increment() {
    concurrent_positive_updates(false);
}

#[test]
fn concurrent_advances_and_utc_corrections_preserve_both_domains() {
    concurrent_positive_updates(true);
}
