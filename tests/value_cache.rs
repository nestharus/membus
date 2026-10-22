use membus::*;
use std::{collections::HashMap, path::PathBuf};

fn path(label: &str) -> PathBuf {
    PathBuf::from(std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()))
        .join(format!("cache-{label}-{}-{}", std::process::id(), now_ms()))
}
fn put(ns: &str, i: usize, size: usize) -> Mutation {
    Mutation {
        ns: ns.into(),
        key: format!("k{i}"),
        op_id: format!("put{i}"),
        deadline: now_ms() + 30_000,
        ack: 1,
        expected: None,
        bytes: vec![(i % 251) as u8; size],
        remove: false,
    }
}
fn stat(s: &Store, name: &str) -> usize {
    let fields: HashMap<_, _> = s.health2().fields().into_iter().collect();
    fields
        .get(name)
        .unwrap_or_else(|| panic!("missing statistic {name}"))
        .parse()
        .unwrap()
}

#[test]
fn larger_than_process_budget_reads_back_exactly() {
    let p = path("budget");
    let s = Store::open(
        &p,
        Config {
            capacity: 256 * 1024,
            signal_bytes: 0,
            ..Config::default()
        },
    )
    .unwrap();
    for i in 0..64 {
        assert_eq!(s.mutate(put("ledger", i, 8192)).code, 0, "write {i}");
    }
    assert!(stat(&s, "cache_bytes") <= 256 * 1024);
    assert!(s.health2().memory_bytes <= s.health2().memory_budget);
    for i in 0..64 {
        assert_eq!(
            s.get("ledger", &format!("k{i}")).unwrap().bytes,
            put("ledger", i, 8192).bytes
        );
    }
    assert!(stat(&s, "cache_bytes") <= 256 * 1024);
    assert!(s.health2().memory_bytes <= s.health2().memory_budget);
    assert!(stat(&s, "cache_cold_reads") > 0);
}

#[test]
fn restart_rebuilds_index_without_warming_values() {
    let p = path("restart");
    let c = Config::default();
    {
        let s = Store::open(&p, c.clone()).unwrap();
        for i in 0..32 {
            assert_eq!(s.mutate(put("ledger", i, 8192)).code, 0);
        }
    }
    #[cfg(not(windows))]
    std::fs::remove_file(p.join("owner.lock")).unwrap();
    let s = Store::open(&p, c).unwrap();
    assert_eq!(stat(&s, "cache_bytes"), 0);
    assert_eq!(s.slots("ledger").1, 32);
    for i in 0..32 {
        assert_eq!(
            s.get("ledger", &format!("k{i}")).unwrap().bytes,
            put("ledger", i, 8192).bytes
        );
    }
    assert_eq!(stat(&s, "cache_cold_reads"), 32);
}

fn policy(initial_ttl: u64, active_ttl: u64) -> CachePolicy {
    CachePolicy {
        initial_ttl,
        active_ttl,
        refresh_on_read: true,
        never_cache: false,
    }
}
#[test]
fn per_class_leases_refresh_once_and_cold_reads_use_active_ttl() {
    let clock = ManualClock::new(now_ms());
    let mut c = Config {
        clock: Some(clock.clone()),
        tick_ms: 3_600_000,
        ..Config::default()
    };
    c.cache_rules = vec![
        CacheRule::namespace("ledger", policy(20, 100)),
        CacheRule::namespace("logs", policy(5, 30)),
    ];
    let s = Store::open(&path("leases"), c).unwrap();
    for ns in ["ledger", "logs"] {
        for i in 0..2 {
            assert_eq!(s.mutate(put(ns, i, 1024)).code, 0);
        }
    }
    clock.advance(4);
    assert!(s.get("logs", "k0").is_some()); // promoted until 34
    clock.advance(1);
    s.tick();
    assert_eq!(s.cache_stats().entries, 3); // untouched log expired at 5
    assert!(s.get("ledger", "k0").is_some()); // promoted until 105
    clock.advance(15);
    s.tick();
    assert_eq!(s.cache_stats().entries, 2); // untouched ledger expired at 20
    assert!(s.get("logs", "k0").is_some()); // does not refresh again
    clock.advance(14);
    s.tick();
    assert_eq!(s.cache_stats().entries, 1);
    let cold = s.cache_stats().cold_reads;
    assert!(s.get("logs", "k0").is_some()); // cold, active until 64
    assert_eq!(s.cache_stats().cold_reads, cold + 1);
    clock.jump_utc(1_000_000);
    s.tick();
    assert_eq!(
        s.cache_stats().entries,
        2,
        "wall clock jumps do not expire leases"
    );
    clock.advance(30);
    s.tick();
    assert_eq!(s.cache_stats().entries, 1);
    clock.advance(41);
    s.tick();
    assert_eq!(s.cache_stats().entries, 0);
    assert_eq!(s.cache_stats().evicted_expired, 5);
}

#[test]
fn never_cache_keeps_only_initial_readback_and_disabled_refresh_expires() {
    let clock = ManualClock::new(now_ms());
    let c = Config {
        clock: Some(clock.clone()),
        cache_rules: vec![
            CacheRule::namespace(
                "cold",
                CachePolicy {
                    never_cache: true,
                    ..policy(7, 1000)
                },
            ),
            CacheRule::namespace(
                "plain",
                CachePolicy {
                    refresh_on_read: false,
                    ..policy(7, 1000)
                },
            ),
        ],
        ..Config::default()
    };
    let s = Store::open(&path("never"), c).unwrap();
    for ns in ["cold", "plain"] {
        assert_eq!(s.mutate(put(ns, 0, 1024)).code, 0);
    }
    clock.advance(6);
    for ns in ["cold", "plain"] {
        assert!(s.get(ns, "k0").is_some());
    }
    assert_eq!(s.cache_stats().hits, 2);
    clock.advance(1);
    s.tick();
    assert_eq!(s.cache_stats().entries, 0);
    for _ in 0..3 {
        assert!(s.get("cold", "k0").is_some());
    }
    assert_eq!(s.cache_stats().cold_reads, 3);
    assert_eq!(s.cache_stats().entries, 0);
    assert!(s.get("plain", "k0").is_some());
    assert_eq!(s.cache_stats().entries, 1);
    clock.advance(999);
    assert!(s.get("plain", "k0").is_some());
    clock.advance(1);
    s.tick();
    assert_eq!(s.cache_stats().entries, 0);
}

#[test]
fn namespace_prefix_and_retention_class_rules_are_configurable() {
    let clock = ManualClock::new(now_ms());
    let rule = CacheRule {
        namespace: Some("custom".into()),
        prefix: "k".into(),
        class: Some(Class::LATEST),
        label: "rapid".into(),
        policy: policy(11, 5000),
    };
    let s = Store::open(
        &path("class"),
        Config {
            clock: Some(clock.clone()),
            cache_rules: vec![rule],
            ..Config::default()
        },
    )
    .unwrap();
    assert_eq!(
        s.set_policy("custom", "", Some(Class::LATEST), now_ms() + 10000)
            .code,
        0
    );
    assert_eq!(s.mutate(put("custom", 0, 1024)).code, 0);
    assert!(s
        .cache_stats()
        .residency
        .iter()
        .any(|(c, _, n)| c == "rapid" && *n == 1));
    clock.advance(11);
    s.tick();
    assert!(!s
        .cache_stats()
        .residency
        .iter()
        .any(|(c, _, _)| c == "rapid"));
    assert_eq!(
        s.set_policy("custom", "", Some(Class::FOREVER), now_ms() + 10000)
            .code,
        0
    );
    assert!(s.get("custom", "k0").is_some());
    assert!(
        s.cache_stats()
            .residency
            .iter()
            .any(|(c, _, n)| c == "rapid" && *n == 1),
        "a cold read uses the record's immutable class"
    );
}

#[test]
fn readers_keep_exact_values_during_eviction() {
    use std::sync::Arc;
    let s = Arc::new(
        Store::open(
            &path("concurrent"),
            Config {
                cache_bytes: 64 * 1024,
                signal_bytes: 0,
                ..Config::default()
            },
        )
        .unwrap(),
    );
    for i in 0..64 {
        assert_eq!(s.mutate(put("readers", i, 1024)).code, 0);
    }
    std::thread::scope(|scope| {
        for thread in 0..4 {
            let s = s.clone();
            scope.spawn(move || {
                for n in 0..1000 {
                    let i = (n * 17 + thread) % 64;
                    let v = s.get("readers", &format!("k{i}")).unwrap();
                    assert_eq!(v.bytes, put("readers", i, 1024).bytes);
                }
            });
        }
        for i in 64..320 {
            assert_eq!(s.mutate(put("readers", i, 1024)).code, 0);
            assert!(s.cache_stats().bytes <= 64 * 1024);
        }
    });
    assert!(s.cache_stats().evicted_budget > 0);
    assert!(s.cache_stats().cold_reads > 0);
}

#[test]
fn cold_retired_value_is_expired() {
    let clock = ManualClock::new(now_ms());
    let s = Store::open(
        &path("retired"),
        Config {
            cache_bytes: 0,
            clock: Some(clock.clone()),
            retry_ms: 10,
            rotation_ms: 1,
            tick_ms: 3_600_000,
            retire_ms: 3_600_000,
            ..Config::default()
        },
    )
    .unwrap();
    assert_eq!(
        s.set_policy("expiry", "", Some(Class::expire(20)), now_ms() + 10000)
            .code,
        0
    );
    assert_eq!(s.mutate(put("expiry", 0, 1024)).code, 0);
    clock.advance(2);
    s.tick();
    clock.advance(20);
    s.retirement_pass();
    assert_eq!(s.get_status("expiry", "k0"), (1, 0));
    assert!(s.get("expiry", "k0").is_none());
    assert_eq!(s.cache_stats().bytes, 0);
}

#[test]
fn failed_append_and_mem_ack_never_populate_committed_cache() {
    for (label, c) in [
        (
            "failed",
            Config {
                fail_partial_append: true,
                ..Config::default()
            },
        ),
        (
            "mem",
            Config {
                allow_mem: true,
                ..Config::default()
            },
        ),
    ] {
        let s = Store::open(&path(label), c).unwrap();
        let mut m = put("write", 0, 1024);
        if label == "mem" {
            m.ack = 0;
        }
        assert_eq!(s.mutate(m).code, if label == "mem" { 0 } else { 16 });
        assert!(s.get("write", "k0").is_none());
        assert_eq!(s.cache_stats().bytes, 0);
    }
}

#[test]
fn wheel_handles_many_revolutions_and_reads_do_not_slide_active_expiry() {
    let clock = ManualClock::new(now_ms());
    let s = Store::open(
        &path("wheel"),
        Config {
            clock: Some(clock.clone()),
            cache_rules: vec![],
            cache_policy: policy(20_000, 50_000),
            ..Config::default()
        },
    )
    .unwrap();
    assert_eq!(s.mutate(put("wheel", 0, 1024)).code, 0);
    clock.advance(10_000);
    s.tick();
    assert_eq!(s.cache_stats().entries, 1);
    assert!(s.get("wheel", "k0").is_some());
    for _ in 0..4 {
        clock.advance(10_000);
        s.tick();
        assert!(s.get("wheel", "k0").is_some());
    }
    clock.advance(10_000);
    s.tick();
    assert_eq!(s.cache_stats().entries, 0);
}

#[test]
fn eviction_writes_nothing_and_disk_corruption_is_an_error() {
    use std::{
        fs::OpenOptions,
        io::{Read, Seek, SeekFrom, Write},
    };
    let p = path("disk-error");
    let s = Store::open(
        &p,
        Config {
            cache_bytes: 0,
            ..Config::default()
        },
    )
    .unwrap();
    assert_eq!(s.mutate(put("disk", 0, 1024)).code, 0);
    let before = s.io_metrics();
    assert_eq!(
        s.try_get("disk", "k0").unwrap().unwrap().bytes,
        vec![0; 1024]
    );
    assert_eq!(
        s.io_metrics(),
        before,
        "cold reads and eviction never append or sync"
    );
    let chunks = p.join("chunks/forever");
    let file = std::fs::read_dir(chunks)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|s| s == "log"))
        .unwrap();
    let mut f = OpenOptions::new()
        .read(true)
        .write(true)
        .open(file)
        .unwrap();
    f.seek(SeekFrom::End(-1)).unwrap();
    let mut b = [0];
    f.read_exact(&mut b).unwrap();
    b[0] ^= 1;
    f.seek(SeekFrom::End(-1)).unwrap();
    f.write_all(&b).unwrap();
    drop(f);
    assert!(s.try_get("disk", "k0").is_err());
    assert!(s.storage_error());
    let mut payload = (now_ms() + 10000).to_le_bytes().to_vec();
    lp(&mut payload, b"disk-error-read");
    lp(&mut payload, b"disk");
    lp(&mut payload, b"k0");
    let r = s.dispatch(&frame(GET, 1, &payload)[4..]);
    assert_eq!(u16::from_le_bytes(r[17..19].try_into().unwrap()), 16);
}

#[test]
fn retry_and_index_bytes_cause_admission_refusal_before_budget_overflow() {
    let s = Store::open(
        &path("metadata-budget"),
        Config {
            capacity: 64 * 1024,
            cache_bytes: 0,
            signal_bytes: 0,
            ..Config::default()
        },
    )
    .unwrap();
    let mut admitted = 0;
    for i in 0..1000 {
        let code = s.mutate(put("index", i, 0)).code;
        if code == 7 {
            break;
        }
        assert_eq!(code, 0);
        admitted += 1;
        assert!(s.health2().memory_bytes <= 64 * 1024);
    }
    assert!(admitted > 0 && admitted < 1000);
    for i in 0..admitted {
        assert_eq!(
            s.get("index", &format!("k{i}")).unwrap().bytes,
            Vec::<u8>::new()
        );
    }
}
