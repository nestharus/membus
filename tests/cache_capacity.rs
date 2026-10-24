//! Shared cache capacity, bypasses and immutable-value behavior on authored data.
use membus::{now_ms, CachePolicy, Config, ManualClock, Mutation, Store};
use std::{path::PathBuf, sync::Arc};

struct Fixture(PathBuf);
impl Fixture {
    fn new(label: &str) -> Self {
        Self(
            PathBuf::from(
                std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()),
            )
            .join(format!(
                "shared-budget-{label}-{}-{}",
                std::process::id(),
                now_ms()
            )),
        )
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        if self.0.exists() {
            std::fs::remove_dir_all(&self.0).unwrap();
        }
    }
}
fn config(budget: usize) -> Config {
    Config {
        capacity: 16 * 1024 * 1024,
        cache_bytes: budget,
        signal_bytes: 0,
        tick_ms: 3_600_000,
        cache_rules: vec![],
        cache_policy: CachePolicy {
            initial_ttl: 0,
            ..CachePolicy::default()
        },
        ..Config::default()
    }
}
fn put(i: usize, generation: usize, size: usize) -> Mutation {
    Mutation {
        ns: "account".into(),
        key: format!("row/{i}"),
        op_id: format!("row-{i}-{generation}"),
        deadline: now_ms() + 30_000,
        ack: 1,
        expected: None,
        bytes: vec![((i + generation) % 251) as u8; size],
        remove: false,
    }
}
fn bounded(s: &Store) {
    let stats = s.cache_stats();
    assert!(stats.bytes <= stats.limit.min(stats.budget));
    assert!(s.health2().memory_bytes <= s.health2().memory_budget);
}

#[test]
fn shared_budget_large_rows_use_other_shards_unused_allowance() {
    let fixture = Fixture::new("large");
    let s = Store::open(&fixture.0, config(2 * 1024 * 1024)).unwrap();
    for i in 0..4 {
        assert_eq!(s.mutate(put(i, 0, 256 * 1024)).code, 0);
    }
    for _ in 0..2 {
        for i in 0..4 {
            assert_eq!(
                s.try_get("account", &format!("row/{i}"))
                    .unwrap()
                    .unwrap()
                    .bytes,
                vec![i as u8; 256 * 1024]
            );
        }
    }
    let stats = s.cache_stats();
    assert_eq!(
        stats.cold_reads, 4,
        "four first loads, then four cache hits"
    );
    assert_eq!((stats.entries, stats.hits, stats.misses), (4, 4, 4));
    bounded(&s);
}

#[test]
fn shared_budget_oversized_and_disabled_caches_still_bypass() {
    for budget in [0, 64 * 1024] {
        let fixture = Fixture::new(&format!("bypass-{budget}"));
        let s = Store::open(&fixture.0, config(budget)).unwrap();
        assert_eq!(s.mutate(put(0, 0, 128 * 1024)).code, 0);
        for _ in 0..2 {
            assert_eq!(
                s.try_get("account", "row/0").unwrap().unwrap().bytes.len(),
                128 * 1024
            );
        }
        assert_eq!(
            (s.cache_stats().entries, s.cache_stats().cold_reads),
            (0, 2)
        );
        bounded(&s);
    }
}

#[test]
fn shared_budget_replacement_and_expiry_release_charges() {
    let fixture = Fixture::new("lifetime");
    let clock = ManualClock::new(now_ms());
    let mut c = config(512 * 1024);
    c.clock = Some(clock.clone());
    c.cache_policy = CachePolicy {
        initial_ttl: 10,
        active_ttl: 20,
        ..CachePolicy::default()
    };
    let s = Store::open(&fixture.0, c).unwrap();
    assert_eq!(s.mutate(put(0, 0, 256 * 1024)).code, 0);
    let original = s.try_get("account", "row/0").unwrap().unwrap();
    assert_eq!(s.cache_stats().entries, 1);
    assert_eq!(s.mutate(put(0, 1, 256 * 1024)).code, 0);
    assert_eq!(original.bytes, vec![0; 256 * 1024]);
    assert_eq!(
        s.try_get("account", "row/0").unwrap().unwrap().bytes,
        vec![1; 256 * 1024]
    );
    assert_eq!(s.cache_stats().entries, 1);
    assert_eq!(s.cache_stats().evicted_replaced, 1);
    clock.advance(20);
    s.tick();
    assert_eq!((s.cache_stats().bytes, s.cache_stats().entries), (0, 0));
    assert_eq!(
        s.try_get("account", "row/0").unwrap().unwrap().bytes,
        vec![1; 256 * 1024]
    );
    let mut removed = put(0, 2, 0);
    removed.remove = true;
    assert_eq!(s.mutate(removed).code, 0);
    assert!(s.try_get("account", "row/0").unwrap().is_none());
    assert_eq!(s.cache_stats().bytes, 0);
    bounded(&s);
}

#[test]
fn shared_budget_parallel_misses_and_writes_stay_bounded() {
    let fixture = Fixture::new("parallel");
    let s = Arc::new(Store::open(&fixture.0, config(256 * 1024)).unwrap());
    for i in 0..32 {
        assert_eq!(s.mutate(put(i, 0, 64 * 1024)).code, 0);
    }
    let start = std::sync::Barrier::new(9);
    std::thread::scope(|scope| {
        for thread in 0..8 {
            let (s, start) = (&s, &start);
            scope.spawn(move || {
                start.wait();
                for read in 0..64 {
                    let i = (read * 17 + thread) % 32;
                    let value = s.try_get("account", &format!("row/{i}")).unwrap().unwrap();
                    assert_eq!(value.bytes, vec![i as u8; 64 * 1024]);
                    bounded(s);
                }
            });
        }
        start.wait();
        for i in 32..48 {
            assert_eq!(s.mutate(put(i, 0, 64 * 1024)).code, 0);
            bounded(&s);
        }
    });
    assert!(s.cache_stats().evicted_budget > 0);
    bounded(&s);
}
