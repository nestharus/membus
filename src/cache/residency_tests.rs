use super::*;
use std::collections::BTreeMap;

fn fixture() -> (Cache, Arc<crate::ManualClock>) {
    let config = Config {
        cache_bytes: 8 * 1024 * 1024,
        ..Config::default()
    };
    let clock = crate::ManualClock::new(crate::now_ms());
    let cache = Cache::new(&config, Clock::new(Some(clock.clone())));
    cache.trim(config.cache_bytes);
    (cache, clock)
}
fn add(cache: &Cache, id: u64, label: &str, initial: bool) {
    cache.insert(
        Arc::new(Value {
            version: id,
            ack: 1,
            tombstone: false,
            bytes: vec![7; 256],
        }),
        label.into(),
        CachePolicy {
            initial_ttl: 13,
            active_ttl: 17,
            ..CachePolicy::default()
        },
        initial,
        8 * 1024 * 1024,
        &AtomicBool::new(true),
    );
}
fn verify(cache: &Cache) {
    let actual = cache.stats();
    let mut expected = BTreeMap::<String, (usize, usize)>::new();
    for shard in &cache.shards {
        for r in shard.lock().unwrap().entries.values() {
            let total = expected.entry(r.label.to_string()).or_default();
            total.0 += r.charge;
            total.1 += 1;
        }
    }
    let expected: Vec<_> = expected
        .into_iter()
        .map(|(label, (bytes, count))| (label, bytes, count))
        .collect();
    assert_eq!(actual.residency, expected);
    assert_eq!(
        actual.bytes,
        actual
            .residency
            .iter()
            .map(|(_, bytes, _)| bytes)
            .sum::<usize>()
    );
    assert_eq!(
        actual.entries,
        actual
            .residency
            .iter()
            .map(|(_, _, count)| count)
            .sum::<usize>()
    );
    assert!(actual.residency.iter().all(|(_, _, count)| *count > 0));
    assert!(actual.bytes <= actual.limit);
}
#[test]
fn residency_totals_follow_promotion_expiry_pressure_and_invalidation() {
    let (cache, clock) = fixture();
    for id in 1..=80 {
        add(
            &cache,
            id,
            ["α", "class-b", "class-c"][id as usize % 3],
            true,
        );
        verify(&cache);
    }
    let before = cache.stats().residency;
    clock.advance(5);
    assert!(cache.get(1).is_some());
    assert_eq!(cache.stats().residency, before);
    cache.remove(2, false);
    cache.remove(3, true);
    verify(&cache);
    add(&cache, 4, "ignored-duplicate", true);
    verify(&cache);
    cache.trim(4096);
    verify(&cache);
    clock.advance(100);
    cache.sweep();
    verify(&cache);
    assert!(cache.stats().residency.is_empty());
    cache.trim(8 * 1024 * 1024);
    add(&cache, 100, "reborn", false);
    verify(&cache);
    cache.trim(0);
    verify(&cache);
    assert!(cache.stats().residency.is_empty());
}
#[test]
fn residency_totals_remain_consistent_during_parallel_churn() {
    let (cache, _) = fixture();
    std::thread::scope(|scope| {
        for thread in 0..4 {
            let cache = &cache;
            scope.spawn(move || {
                for i in 0..128 {
                    let id = 1 + thread * 1000 + i;
                    add(cache, id, &format!("worker{}", thread % 2), false);
                    if i > 0 {
                        cache.remove(id - 1, i % 2 == 0);
                    }
                    let stats = cache.stats();
                    assert_eq!(
                        stats.bytes,
                        stats.residency.iter().map(|(_, b, _)| b).sum::<usize>()
                    );
                    assert_eq!(
                        stats.entries,
                        stats.residency.iter().map(|(_, _, n)| n).sum::<usize>()
                    );
                    assert!(stats.bytes <= stats.limit);
                }
            });
        }
    });
    verify(&cache);
    cache.trim(0);
    verify(&cache);
    assert!(cache.stats().residency.is_empty());
}
