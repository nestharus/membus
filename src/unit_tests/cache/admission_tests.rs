use super::*;
use crate::{Mutation, Store};
use std::{
    path::PathBuf,
    sync::mpsc,
    time::{Duration, Instant},
};

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let base = PathBuf::from(
            std::env::var_os("MEMBUS_TEST_OUTPUT").unwrap_or_else(|| ".test-output".into()),
        );
        std::fs::create_dir_all(&base).unwrap();
        Self(std::fs::canonicalize(base).unwrap().join(format!(
            "cache-admission-{}-{}-{}",
            std::process::id(),
            crate::now_ms(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn read_during_stalled_trim(cache_bytes: usize, value_bytes: usize, can_fit: bool) {
    let fixture = Fixture::new();
    let store = Arc::new(
        Store::open(
            &fixture.0,
            Config {
                capacity: 64 * 1024 * 1024,
                cache_bytes,
                signal_bytes: 0,
                tick_ms: 3_600_000,
                retire_ms: 3_600_000,
                cache_rules: Vec::new(),
                cache_policy: CachePolicy {
                    initial_ttl: 0,
                    active_ttl: 3_600_000,
                    refresh_on_read: false,
                    never_cache: false,
                },
                ..Config::default()
            },
        )
        .unwrap(),
    );
    let outcome = store.mutate(Mutation {
        ns: "ledger".into(),
        key: "head".into(),
        op_id: "seed".into(),
        deadline: crate::now_ms() + 30_000,
        ack: 1,
        expected: None,
        bytes: vec![0x71; value_bytes],
        remove: false,
    });
    assert_eq!(outcome.code, 0);
    assert_ne!(
        outcome.version as usize % SHARDS,
        0,
        "the read's shard must differ from the stalled trim shard"
    );
    // Real allowance reduction takes admission, then waits on this other
    // shard. The reader can perform its initial lookup and bounded cold load.
    let stalled = store.cache.shards[0].lock().unwrap();
    let trim = {
        let store = store.clone();
        std::thread::spawn(move || store.cache.trim(0))
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    let trim_has_admission = loop {
        if matches!(
            store.cache.admission.try_lock(),
            Err(std::sync::TryLockError::WouldBlock)
        ) {
            break true;
        }
        if Instant::now() >= deadline {
            break false;
        }
        std::thread::yield_now();
    };
    let (done, finished) = mpsc::channel();
    let reader = {
        let store = store.clone();
        std::thread::spawn(move || {
            let value = store.try_get("ledger", "head");
            done.send(()).unwrap();
            value
        })
    };
    let deadline = Instant::now() + Duration::from_secs(5);
    while store.cache.cold_reads.load(Ordering::Acquire) == 0 && Instant::now() < deadline {
        std::thread::yield_now();
    }
    let entered = store.cache.cold_reads.load(Ordering::Acquire) == 1;
    let completed = finished
        .recv_timeout(if can_fit {
            Duration::from_millis(200)
        } else {
            Duration::from_secs(5)
        })
        .is_ok();
    // Settle both threads before assertions, also on the old blocked path.
    drop(stalled);
    let value = reader.join().unwrap().unwrap().unwrap();
    trim.join().unwrap();
    assert_eq!(value.version, outcome.version);
    assert_eq!(value.ack, 1);
    assert!(!value.tombstone);
    assert_eq!(value.bytes, vec![0x71; value_bytes]);
    let reduced = store.cache_stats();
    assert_eq!(reduced.limit, 0);
    assert_eq!(reduced.bytes, 0);
    assert_eq!(reduced.entries, 0);
    assert_eq!(reduced.cold_reads, 1);
    assert_eq!(reduced.misses, 1);
    assert_eq!(reduced.coalesced_reads, 0);
    assert!(!store.storage_error());
    // A value that could fit the configured maximum still waits for actual
    // admission, and becomes retainable again after the allowance is restored.
    store.cache.trim(cache_bytes);
    let second = store.try_get("ledger", "head").unwrap().unwrap();
    let third = store.try_get("ledger", "head").unwrap().unwrap();
    assert_eq!(second.bytes, value.bytes);
    assert_eq!(third.bytes, value.bytes);
    let restored = store.cache_stats();
    assert_eq!(restored.cold_reads, if can_fit { 2 } else { 3 });
    assert_eq!(restored.hits, if can_fit { 1 } else { 0 });
    assert_eq!(restored.entries, if can_fit { 1 } else { 0 });
    assert!(restored.bytes <= restored.limit && restored.limit <= cache_bytes);
    eprintln!("admission cache_bytes={cache_bytes} value_bytes={value_bytes} can_fit={can_fit} trim_holds_admission={trim_has_admission} cold_entered={entered} completed_before_trim_release={completed}");
    assert!(
        trim_has_admission && entered,
        "trim and cold read must reach their coordination boundaries"
    );
    assert_eq!(completed,!can_fit,"known-unfit values must return without waiting for admission; potentially retainable values must still coordinate");
    drop(store);
}

#[test]
fn admission_bypass_payload_beyond_configured_cache_during_trim() {
    read_during_stalled_trim(64 * 1024, 256 * 1024, false);
}
#[test]
fn admission_bypass_charge_overhead_beyond_configured_cache_during_trim() {
    read_during_stalled_trim(8192, 8192, false);
}
#[test]
fn admission_bypass_disabled_cache_during_trim() {
    read_during_stalled_trim(0, 8192, false);
}
#[test]
fn admission_bypass_fitting_value_still_coordinates_and_admits_after_restore() {
    read_during_stalled_trim(64 * 1024, 8192, true);
}

#[test]
fn obsolete_revision_bypasses_admission_even_when_its_charge_fits() {
    let config = Config {
        cache_bytes: 64 * 1024,
        ..Config::default()
    };
    let cache = Cache::new(
        &config,
        Clock::new(Some(crate::ManualClock::new(crate::now_ms()))),
    );
    cache.trim(config.cache_bytes);
    let value = Arc::new(Value {
        version: 1,
        ack: 1,
        tombstone: false,
        bytes: vec![0x71; 8192],
    });
    let admission = cache.admission.lock().unwrap();
    let (done, finished) = mpsc::channel();
    let completed = std::thread::scope(|scope| {
        let (cache, value) = (&cache, value.clone());
        let reader = scope.spawn(move || {
            cache.insert(
                value,
                "snapshot".into(),
                CachePolicy::default(),
                false,
                config.cache_bytes,
                &AtomicBool::new(false),
            );
            done.send(()).unwrap();
        });
        let completed = finished.recv_timeout(Duration::from_secs(5)).is_ok();
        // Release coordination and join before asserting, including on old code.
        drop(admission);
        reader.join().unwrap();
        completed
    });
    assert!(completed, "obsolete revisions must bypass admission coordination");
    assert_eq!(cache.stats().entries, 0);
    cache.insert(
        value,
        "snapshot".into(),
        CachePolicy::default(),
        false,
        config.cache_bytes,
        &AtomicBool::new(true),
    );
    assert_eq!(cache.stats().entries, 1);
}
