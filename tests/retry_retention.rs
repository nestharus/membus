//! Public-store qualification of compact retry retention, not a workload promise.
use membus::{now_ms, Config, Mutation, Store};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

fn path(label: &str) -> PathBuf {
    PathBuf::from(std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()))
        .join(format!("retry-{label}-{}-{}", std::process::id(), now_ms()))
}
fn mutation(id: &str, bytes: Vec<u8>) -> Mutation {
    Mutation {
        ns: "retry-test".into(),
        key: "state".into(),
        op_id: id.into(),
        deadline: now_ms() + 30_000,
        ack: 2,
        expected: None,
        bytes,
        remove: false,
    }
}
fn reopen(path: &std::path::Path, config: Config) -> Store {
    #[cfg(not(windows))]
    std::fs::remove_file(path.join("owner.lock")).unwrap();
    Store::open(path, config).unwrap()
}

#[test]
fn growing_overwrites_fit_and_original_outcomes_survive_replay() {
    let p = path("growth");
    let config = Config {
        capacity: 512 * 1024,
        flush_ms: 0,
        ..Config::default()
    };
    let s = Store::open(&p, config.clone()).unwrap();
    let mut originals = Vec::new();
    for i in 1..=256 {
        let m = mutation(&format!("history-{i}"), vec![b'x'; i * 256]);
        let outcome = s.mutate(m.clone());
        assert_eq!(outcome.code, 0, "history {i}");
        if i == 1 || i % 16 == 0 {
            originals.push((m, outcome));
        }
    }
    assert_eq!(s.get("retry-test", "state").unwrap().bytes.len(), 65_536);
    let persisted = s.state_bytes();
    drop(s);
    let s = reopen(&p, config);
    for (mut m, outcome) in originals {
        m.deadline = now_ms() + 30_000;
        assert_eq!(s.mutate(m.clone()), outcome);
        m.bytes.push(0);
        assert_eq!(s.mutate(m).code, 14);
    }
    assert_eq!(s.metrics().0, 0, "retries must not append");
    assert_eq!(s.state_bytes(), persisted);
    assert_eq!(s.get("retry-test", "state").unwrap().bytes.len(), 65_536);
}

#[test]
fn pending_retry_joins_and_changed_fields_conflict_before_and_after_restart() {
    let p = path("join");
    let config = Config {
        write_delay_ms: 1000,
        flush_ms: 0,
        allow_mem: true,
        ..Config::default()
    };
    let s = Arc::new(Store::open(&p, config.clone()).unwrap());
    let original = mutation("original", vec![b'x'; 4096]);
    let outcome = std::thread::scope(|scope| {
        let writer = scope.spawn(|| s.mutate(original.clone()));
        let limit = Instant::now() + Duration::from_secs(5);
        while !s.pending("retry-test", "state") {
            assert!(Instant::now() < limit, "original admission not observed");
            std::thread::yield_now();
        }
        let mut retry = original.clone();
        retry.deadline = now_ms() + 20;
        assert_eq!(
            s.mutate(retry).code,
            17,
            "duplicate must wait for pending outcome"
        );
        let mut changed = original.clone();
        changed.bytes[0] ^= 1;
        assert_eq!(s.mutate(changed).code, 14);
        writer.join().unwrap()
    });
    assert_eq!(outcome.code, 0);
    let before = s.metrics().0;
    for field in 0..5 {
        let mut changed = original.clone();
        match field {
            0 => changed.key.push('2'),
            1 => changed.expected = Some(outcome.version),
            2 => changed.ack = 1,
            3 => changed.remove = true,
            _ => changed.bytes.push(0),
        }
        assert_eq!(s.mutate(changed).code, 14, "changed field {field}");
    }
    let mut retry = original.clone();
    retry.deadline = now_ms() + 30_000;
    assert_eq!(s.mutate(retry.clone()), outcome);
    assert_eq!(s.metrics().0, before);
    drop(s);
    let s = reopen(&p, config);
    assert_eq!(s.mutate(retry.clone()), outcome);
    retry.bytes[0] ^= 1;
    assert_eq!(s.mutate(retry).code, 14);
    assert_eq!(s.metrics().0, 0);
}

#[test]
fn compact_comparator_does_not_relax_full_payload_queue_admission() {
    let s = Store::open(
        &path("queue"),
        Config {
            queue_bytes: 4 * (2 * 4096 + 128) - 4,
            flush_ms: 0,
            ..Config::default()
        },
    )
    .unwrap();
    let mut m = mutation("queue", vec![0; 4096]);
    assert_eq!(s.mutate(m.clone()).code, 6);
    assert_eq!(s.metrics().0, 0);
    assert!(s.get("retry-test", "state").is_none());
    m.bytes.clear();
    assert_eq!(
        s.mutate(m).code,
        0,
        "refusal must not consume retry identity"
    );
}
