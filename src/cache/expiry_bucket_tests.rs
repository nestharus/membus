use super::*;

fn cache() -> (Cache, Arc<crate::ManualClock>) {
    let config = Config { cache_bytes: 16*1024*1024, ..Config::default() };
    let clock = crate::ManualClock::new(crate::now_ms());
    let cache = Cache::new(&config, Clock::new(Some(clock.clone())));
    cache.trim(config.cache_bytes);
    (cache, clock)
}
fn add(cache: &Cache, id: u64, initial: bool, first: u64, active: u64) {
    cache.insert(Arc::new(Value { version:id,ack:1,tombstone:false,bytes:vec![7;128] }),
        "c".into(),CachePolicy {initial_ttl:first,active_ttl:active,..CachePolicy::default()},
        initial,16*1024*1024,&AtomicBool::new(true));
}

#[test]
fn expiry_bucket_skips_future_cohort_on_repeated_hot_reads() {
    let (cache, clock)=cache();
    for i in 1..=2048 { add(&cache,i*16,false,0,60_000); }
    cache.shards[0].lock().unwrap().expiry_visits=0;
    // The current wheel bucket holds every lease, one or more revolutions away.
    clock.advance(1120);
    for _ in 0..24 { assert!(cache.get(16).is_some()); }
    let visits=cache.shards[0].lock().unwrap().expiry_visits;
    assert_eq!(cache.stats().entries,2048);
    assert_eq!(visits,0,"hot reads examined {visits} future leases");
}

#[test]
fn expiry_bucket_promotion_removal_and_earlier_insert_keep_deadlines() {
    let (cache, clock)=cache();
    add(&cache,16,true,5,2565);
    add(&cache,32,false,0,2570);
    clock.advance(4);
    assert!(cache.get(16).is_some()); // promoted to 2569, in its original bucket
    clock.advance(1);
    cache.sweep(); // an obsolete initial minimum must be harmless
    assert!(cache.get(16).is_some());
    add(&cache,48,false,0,6); // deadline 11, before the existing 2570 in this bucket
    clock.advance(5);
    assert!(cache.get(48).is_some());
    clock.advance(1);
    assert!(cache.get(48).is_none());
    assert!(cache.get(32).is_some());
    cache.remove(16,false);
    add(&cache,64,false,0,2560); // deadline 2571
    clock.advance(2558);
    assert!(cache.get(16).is_none());
    assert!(cache.get(32).is_some());
    assert!(cache.get(64).is_some());
    clock.advance(1);
    assert!(cache.get(32).is_none());
    assert!(cache.get(64).is_some());
    clock.advance(1);
    assert!(cache.get(64).is_none());
    add(&cache,80,false,0,0);
    assert!(cache.get(80).is_none());
    add(&cache,96,false,0,u64::MAX);
    clock.advance(1_000_000);
    cache.sweep();
    assert!(cache.get(96).is_some());
    cache.remove(96,true);
    assert_eq!(cache.stats().entries,0);
    assert_eq!(cache.stats().bytes,0);
    assert_eq!(cache.shards[0].lock().unwrap().wheel.iter().map(HashSet::len).sum::<usize>(),0);
}

#[test]
fn expiry_bucket_exact_deadlines_share_one_tick() {
    let (cache, clock)=cache();
    add(&cache,16,false,0,13);
    add(&cache,32,false,0,17);
    clock.advance(12);
    assert!(cache.get(16).is_some());
    clock.advance(1);
    assert!(cache.get(16).is_none());
    assert!(cache.get(32).is_some());
    clock.advance(4);
    cache.sweep();
    assert_eq!(cache.stats().entries,0);
    assert_eq!(cache.stats().evicted_expired,2);
}
