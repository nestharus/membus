use std::time::Instant;
use super::*;
use crate::{now_ms, CachePolicy, Mutation};
use std::path::PathBuf;

struct Fixture(PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn fixture(label: &str, ttl: u64) -> (Fixture, Store) {
    let root = PathBuf::from(
        std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()),
    );
    let f = Fixture(root.join(format!(
        "allowance-{label}-{}-{}",
        std::process::id(),
        now_ms()
    )));
    let s = Store::open(
        &f.0,
        Config {
            capacity: 1024 * 1024,
            cache_bytes: 512 * 1024,
            snapshot_bytes: 1024 * 1024,
            snapshot_max: 8,
            snapshot_ttl_ms: ttl,
            signal_bytes: 0,
            tick_ms: 3_600_000,
            retire_ms: 3_600_000,
            cache_rules: vec![],
            cache_policy: CachePolicy {
                initial_ttl: 0,
                active_ttl: 60_000,
                ..CachePolicy::default()
            },
            ..Config::default()
        },
    )
    .unwrap();
    for key in ["a", "b"] {
        assert_eq!(
            s.mutate(Mutation {
                ns: "ledger".into(),
                key: key.into(),
                op_id: format!("seed-{key}"),
                deadline: now_ms() + 30_000,
                ack: 1,
                expected: None,
                bytes: vec![7; 128 * 1024],
                remove: false,
            })
            .code,
            0
        );
    }
    (f, s)
}
fn pressure(s: &Store) -> Vec<(Vec<u8>, Arc<Snapshot>)> {
    let views: Vec<_> = (0..3).map(|_| s.snapshot_open("ledger").unwrap()).collect();
    assert!(s.cache_stats().limit < 128 * 1024);
    assert_eq!(s.cache_stats().entries, 0);
    views
}
fn prove_recovered(s: &Store) {
    let before = s.cache_stats();
    assert_eq!(
        s.try_get("ledger", "a").unwrap().unwrap().bytes,
        vec![7; 128 * 1024]
    );
    assert_eq!(
        s.try_get("ledger", "a").unwrap().unwrap().bytes,
        vec![7; 128 * 1024]
    );
    let after = s.cache_stats();
    println!(
        "recovered allowance={} cold={} hits={} entries={}",
        after.limit,
        after.cold_reads - before.cold_reads,
        after.hits - before.hits,
        after.entries
    );
    assert_eq!(
        after.cold_reads - before.cold_reads,
        1,
        "freed snapshot capacity must serve the next cold admission"
    );
    assert_eq!(after.hits - before.hits, 1);
    assert_eq!(after.entries, 1);
    assert_eq!(after.limit, s.config.cache_bytes);
    assert!(s.health2().memory_bytes <= s.config.capacity);
}
#[test]
fn released_allowance_restores_after_explicit_release() {
    let (f, s) = fixture("release", 60_000);
    let views = pressure(&s);
    for (token, _) in &views {
        s.snapshot_release("ledger", token).unwrap();
    }
    drop(views);
    assert_eq!(s.scan_usage.bytes.load(Ordering::Acquire), 0);
    prove_recovered(&s);
    drop(s);
    drop(f);
}
#[test]
fn released_allowance_waits_for_last_reader_and_current_reservations() {
    let (f, s) = fixture("pinned", 60_000);
    let views = pressure(&s);
    let reserved = s.scan_usage.bytes.load(Ordering::Acquire);
    for (token, _) in &views {
        s.snapshot_release("ledger", token).unwrap();
    }
    assert_eq!(s.scan_usage.bytes.load(Ordering::Acquire), reserved);
    let before = s.cache_stats();
    for _ in 0..2 {
        assert!(s.try_get("ledger", "a").unwrap().is_some());
    }
    let pinned = s.cache_stats();
    assert_eq!(pinned.cold_reads - before.cold_reads, 2);
    assert_eq!(pinned.entries, 0);
    drop(views);
    // A new capture can consume released capacity before a cold reader notices.
    let new_views = pressure(&s);
    assert!(s.try_get("ledger", "a").unwrap().is_some());
    assert_eq!(s.cache_stats().entries, 0);
    assert!(s.health2().memory_bytes <= s.config.capacity);
    for (token, _) in &new_views {
        s.snapshot_release("ledger", token).unwrap();
    }
    drop(new_views);
    prove_recovered(&s);
    drop(s);
    drop(f);
}
#[test]
fn released_allowance_restores_after_idle_expiry() {
    let (f, s) = fixture("expiry", 2_000);
    let views = pressure(&s);
    drop(views);
    let until = Instant::now() + Duration::from_secs(5);
    while s.scan_usage.count.load(Ordering::Acquire) != 0 && Instant::now() < until {
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(s.scan_usage.count.load(Ordering::Acquire), 0);
    prove_recovered(&s);
    drop(s);
    drop(f);
}
