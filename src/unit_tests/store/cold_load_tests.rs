use super::*;
use std::{
    path::PathBuf,
    sync::{Arc, Barrier},
    time::{Duration, Instant},
};

struct Fixture(PathBuf);
impl Fixture {
    fn new(label: &str) -> Self {
        let base = PathBuf::from(
            std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()),
        );
        std::fs::create_dir_all(&base).unwrap();
        Self(std::fs::canonicalize(base).unwrap().join(format!(
            "cold-load-{label}-{}-{}",
            std::process::id(),
            now_ms()
        )))
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn put(s: &Store, id: &str, value: u8) -> u64 {
    let o = s.mutate(Mutation {
        ns: "ledger".into(),
        key: "head".into(),
        op_id: id.into(),
        deadline: now_ms() + 30_000,
        ack: 1,
        expected: None,
        bytes: vec![value; 8192],
        remove: false,
    });
    assert_eq!(o.code, 0);
    o.version
}

#[test]
fn simultaneous_misses_load_one_revision_once_and_share_the_payload() {
    let f = Fixture::new("readers");
    let s = Arc::new(Store::open(&f.0, Config::default()).unwrap());
    let version = put(&s, "seed", 7);
    s.cache.remove(version, false);
    let entry = s
        .committed
        .read()
        .unwrap()
        .get(&("ledger".into(), "head".into()))
        .unwrap()
        .clone();
    let blocked = entry.location.file.lock().unwrap();
    let start = Arc::new(Barrier::new(17));
    let before = s.cache_stats();
    let workers: Vec<_> = (0..16)
        .map(|_| {
            let s = s.clone();
            let start = start.clone();
            std::thread::spawn(move || {
                start.wait();
                s.get_handle_result("ledger", "head").unwrap().unwrap()
            })
        })
        .collect();
    start.wait();
    let until = Instant::now() + Duration::from_secs(10);
    while s.cache_stats().misses - before.misses < 16 && Instant::now() < until {
        std::thread::yield_now();
    }
    let all_missed = s.cache_stats().misses - before.misses == 16;
    drop(blocked); // Release before assertions or joins, even on a scheduling failure.
    let values: Vec<_> = workers.into_iter().map(|w| w.join().unwrap()).collect();
    assert!(all_missed, "all readers must reach the cold-miss boundary");
    assert_eq!(
        s.cache_stats().cold_reads - before.cold_reads,
        1,
        "one immutable revision needs one disk load"
    );
    let stats = s.cache_stats();
    assert_eq!(stats.hits - before.hits, 0);
    assert_eq!(stats.misses - before.misses, 16);
    assert_eq!(stats.coalesced_reads - before.coalesced_reads, 15);
    let health: std::collections::HashMap<_, _> = s.health2().fields().into_iter().collect();
    assert_eq!(health.get("cache_coalesced_reads").unwrap(), "15");
    for value in &values {
        assert_eq!(value.version, version);
        assert_eq!(value.bytes, vec![7; 8192]);
        assert!(
            Arc::ptr_eq(value, &values[0]),
            "waiting readers reuse the admitted immutable Arc"
        );
    }
}

#[test]
fn waiting_old_revision_does_not_populate_the_new_revision_cache_slot() {
    let f = Fixture::new("replace");
    let s = Arc::new(Store::open(&f.0, Config::default()).unwrap());
    let version = put(&s, "old", 1);
    s.cache.remove(version, false);
    let entry = s
        .committed
        .read()
        .unwrap()
        .get(&("ledger".into(), "head".into()))
        .unwrap()
        .clone();
    let blocked = entry.location.file.lock().unwrap();
    let reader = {
        let s = s.clone();
        std::thread::spawn(move || s.get_handle_result("ledger", "head").unwrap().unwrap())
    };
    let until = Instant::now() + Duration::from_secs(10);
    while s.cache_stats().cold_reads == 0 && Instant::now() < until {
        std::thread::yield_now();
    }
    let started = s.cache_stats().cold_reads > 0;
    let new = put(&s, "new", 2);
    drop(blocked);
    let old = reader.join().unwrap();
    assert!(started);
    assert_eq!(old.version, version);
    assert_eq!(old.bytes, vec![1; 8192]);
    assert_eq!(s.get("ledger", "head").unwrap().version, new);
    assert_eq!(s.get("ledger", "head").unwrap().bytes, vec![2; 8192]);
    assert!(
        s.cache.get(version).is_none(),
        "old loads cannot reinsert an invalidated revision"
    );
}

#[test]
fn bypass_policies_do_not_acquire_a_hidden_payload_cache() {
    for mode in 0..3 {
        let f = Fixture::new(&format!("bypass{mode}"));
        let s = Store::open(
            &f.0,
            Config {
                cache_bytes: if mode == 0 { 0 } else { 8 * 1024 * 1024 },
                cache_rules: Vec::new(),
                cache_policy: CachePolicy {
                    initial_ttl: 0,
                    active_ttl: if mode == 2 { 0 } else { 60_000 },
                    refresh_on_read: false,
                    never_cache: mode == 1,
                },
                ..Config::default()
            },
        )
        .unwrap();
        put(&s, "seed", 3);
        for _ in 0..8 {
            assert_eq!(s.get("ledger", "head").unwrap().bytes, vec![3; 8192]);
        }
        let stats = s.cache_stats();
        assert_eq!(stats.cold_reads, 8);
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.coalesced_reads, 0);
    }
}

#[test]
fn failed_loads_remain_errors_and_release_the_load_gate() {
    use std::io::{Read, Seek, SeekFrom, Write};
    let f = Fixture::new("errors");
    let s = Store::open(&f.0, Config::default()).unwrap();
    let version = put(&s, "seed", 4);
    s.cache.remove(version, false);
    let file = std::fs::read_dir(f.0.join("chunks/forever"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|x| x == "log"))
        .unwrap();
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(file)
        .unwrap();
    file.seek(SeekFrom::End(-1)).unwrap();
    let mut byte = [0];
    file.read_exact(&mut byte).unwrap();
    byte[0] ^= 1;
    file.seek(SeekFrom::End(-1)).unwrap();
    file.write_all(&byte).unwrap();
    drop(file);
    for _ in 0..3 {
        assert!(s.try_get("ledger", "head").is_err());
    }
    assert!(s.storage_error());
    let stats = s.cache_stats();
    assert_eq!(stats.cold_reads, 3);
    assert_eq!(stats.entries, 0);
    assert_eq!(stats.coalesced_reads, 0);
}

fn bounded_store(f: &Fixture, budget: usize) -> Arc<Store> {
    Arc::new(
        Store::open(
            &f.0,
            Config {
                cache_bytes: budget,
                signal_bytes: 0,
                cache_rules: Vec::new(),
                tick_ms: 3_600_000,
                retire_ms: 3_600_000,
                clock: Some(crate::ManualClock::new(now_ms())),
                cache_policy: CachePolicy {
                    initial_ttl: 0,
                    active_ttl: 60_000,
                    ..CachePolicy::default()
                },
                ..Config::default()
            },
        )
        .unwrap(),
    )
}
fn sized_put(s: &Store, key: &str, operation: &str, size: usize, byte: u8) -> u64 {
    let result = s.mutate(Mutation {
        ns: "ledger".into(),
        key: key.into(),
        op_id: operation.into(),
        deadline: now_ms() + 30_000,
        ack: 1,
        expected: None,
        bytes: vec![byte; size],
        remove: false,
    });
    assert_eq!(result.code, 0);
    result.version
}
fn oversized_collision_progress(size: usize, budget: usize, label: &str) {
    let f = Fixture::new(label);
    let s = bounded_store(&f, budget);
    let large = sized_put(&s, "large", "seed-large", size, 7);
    let small_key = (0..32)
        .map(|i| format!("small/{i}"))
        .find(|key| stripe("ledger", key) != stripe("ledger", "large"))
        .unwrap();
    let mut small = 0;
    for i in 0..128 {
        small = sized_put(&s, &small_key, &format!("seed-small-{i}"), 1024, 9);
        if small % 64 == large % 64 {
            break;
        }
    }
    assert_eq!(
        small % 64,
        large % 64,
        "revisions must collide in the fixed load-gate table"
    );
    let large_entry = s
        .committed
        .read()
        .unwrap()
        .get(&("ledger".into(), "large".into()))
        .unwrap()
        .clone();
    let small_entry = s
        .committed
        .read()
        .unwrap()
        .get(&("ledger".into(), small_key.clone()))
        .unwrap()
        .clone();
    assert!(!Arc::ptr_eq(
        &large_entry.location.file,
        &small_entry.location.file
    ));
    let blocked = large_entry.location.file.lock().unwrap();
    let before = s.cache_stats();
    let first = {
        let s = s.clone();
        std::thread::spawn(move || s.get_handle_result("ledger", "large").unwrap().unwrap())
    };
    let until = Instant::now() + Duration::from_secs(10);
    while s.cache_stats().cold_reads == before.cold_reads && Instant::now() < until {
        std::thread::yield_now();
    }
    let started = s.cache_stats().cold_reads == before.cold_reads + 1;
    let (tx, rx) = std::sync::mpsc::channel();
    let second = {
        let s = s.clone();
        let key = small_key.clone();
        std::thread::spawn(move || {
            tx.send(s.get_handle_result("ledger", &key).unwrap().unwrap())
                .unwrap();
        })
    };
    let early = rx.recv_timeout(Duration::from_secs(5)).ok();
    let progressed = early.is_some();
    // Release blocked I/O and reap both readers before asserting on old code.
    drop(blocked);
    let large_value = first.join().unwrap();
    second.join().unwrap();
    let small_value = early.unwrap_or_else(|| rx.recv().unwrap());
    assert_eq!(large_value.bytes, vec![7; size]);
    assert_eq!(large_value.version, large);
    assert_eq!(small_value.bytes, vec![9; 1024]);
    assert_eq!(small_value.version, small);
    let after = s.cache_stats();
    assert_eq!(
        s.get_handle_result("ledger", &small_key)
            .unwrap()
            .unwrap()
            .bytes,
        vec![9; 1024]
    );
    assert_eq!(s.cache_stats().hits, after.hits + 1);
    println!("oversized={size} budget={budget} colliding_small_progress={progressed} cold={} entries={} coalesced={}",after.cold_reads-before.cold_reads,after.entries,after.coalesced_reads-before.coalesced_reads);
    assert!(started);
    assert_eq!(after.entries, 1);
    assert!(after.bytes <= budget);
    assert_eq!(after.cold_reads - before.cold_reads, 2);
    assert!(
        progressed,
        "an uncacheable stalled value blocked a cacheable revision on another file"
    );
}
#[test]
fn oversized_gate_large_value_does_not_block_colliding_cacheable_read() {
    oversized_collision_progress(256 * 1024, 64 * 1024, "oversize-large");
}
#[test]
fn oversized_gate_metadata_charge_is_included_in_bypass() {
    oversized_collision_progress(8192, 8192, "oversize-overhead");
}
#[test]
fn oversized_gate_bypass_still_has_two_global_decode_slots() {
    // Part A: large frames (> memory_margin() / 2) remain capped at 2 global decode slots.
    {
        let f = Fixture::new("oversize-slots-large");
        let s = Arc::new(
            Store::open(
                &f.0,
                Config {
                    capacity: 1024 * 1024, // memory_margin = 128 KiB, half = 64 KiB
                    cache_bytes: 1024,
                    signal_bytes: 0,
                    cache_rules: Vec::new(),
                    tick_ms: 3_600_000,
                    retire_ms: 3_600_000,
                    clock: Some(crate::ManualClock::new(now_ms())),
                    cache_policy: CachePolicy {
                        initial_ttl: 0,
                        active_ttl: 60_000,
                        ..CachePolicy::default()
                    },
                    ..Config::default()
                },
            )
            .unwrap(),
        );
        let size = 70 * 1024;
        let version = sized_put(&s, "large", "seed-large", size, 7);
        let entry = s
            .committed
            .read()
            .unwrap()
            .get(&("ledger".into(), "large".into()))
            .unwrap()
            .clone();
        let blocked = entry.location.file.lock().unwrap();
        let before = s.cache_stats();
        let start = Arc::new(Barrier::new(4));
        let workers: Vec<_> = (0..3)
            .map(|_| {
                let s = s.clone();
                let start = start.clone();
                std::thread::spawn(move || {
                    start.wait();
                    s.get_handle_result("ledger", "large").unwrap().unwrap()
                })
            })
            .collect();
        start.wait();
        let until = Instant::now() + Duration::from_secs(5);
        while (s.cache_stats().misses - before.misses < 3
            || s.cache_stats().cold_reads - before.cold_reads < 2)
            && Instant::now() < until
        {
            std::thread::yield_now();
        }
        let waiting = s.cache_stats();
        let active = s.cold_readers.active();
        drop(blocked);
        for worker in workers {
            let v = worker.join().unwrap();
            assert_eq!(v.version, version);
            assert_eq!(v.bytes, vec![7; size]);
        }
        println!(
            "oversized-large readers=3 active_before_io_release={active} cold_started={}",
            waiting.cold_reads - before.cold_reads
        );
        assert_eq!(waiting.misses - before.misses, 3);
        assert_eq!(
            active, 2,
            "large frames (> half memory_margin) must keep the two-slot decode cap"
        );
        assert_eq!(waiting.cold_reads - before.cold_reads, 2);
        let after = s.cache_stats();
        assert_eq!(after.entries, 0);
        assert_eq!(after.cold_reads - before.cold_reads, 3);
        assert_eq!(after.coalesced_reads, before.coalesced_reads);
    }
    // Part B: small frames scale concurrently within memory_margin() up to 8+ parallel readers.
    {
        let f = Fixture::new("oversize-slots-small-parallel");
        let s = bounded_store(&f, 1024);
        let version = sized_put(&s, "small-uncached", "seed-small", 8192, 9);
        let entry = s
            .committed
            .read()
            .unwrap()
            .get(&("ledger".into(), "small-uncached".into()))
            .unwrap()
            .clone();
        let blocked = entry.location.file.lock().unwrap();
        let before = s.cache_stats();
        let start = Arc::new(Barrier::new(9));
        let workers: Vec<_> = (0..8)
            .map(|_| {
                let s = s.clone();
                let start = start.clone();
                std::thread::spawn(move || {
                    start.wait();
                    s.get_handle_result("ledger", "small-uncached").unwrap().unwrap()
                })
            })
            .collect();
        start.wait();
        let until = Instant::now() + Duration::from_secs(5);
        while s.cold_readers.active() < 8 && Instant::now() < until {
            std::thread::yield_now();
        }
        let active = s.cold_readers.active();
        let waiting = s.cache_stats();
        drop(blocked);
        for worker in workers {
            let v = worker.join().unwrap();
            assert_eq!(v.version, version);
            assert_eq!(v.bytes, vec![9; 8192]);
        }
        assert_eq!(
            active, 8,
            "small frames within memory_margin() must admit concurrent cold decodes"
        );
        assert_eq!(waiting.cold_reads - before.cold_reads, 8);
    }
}
