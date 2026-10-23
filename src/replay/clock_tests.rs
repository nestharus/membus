//! Deterministic recovery and runtime clock witnesses; fixtures use authored MBK1 records.
use super::*;
use crate::*;
use std::sync::atomic::AtomicU64;
use std::sync::{Condvar, Mutex};
use std::time::Duration;

const UTC: u64 = 1_000_000;
const WINDOW: u64 = 100;
static SEQ: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".test-output");
        Self(root.join(format!(
            "injected-clock-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::SeqCst)
        )))
    }
    fn config(&self, manual: &Arc<ManualClock>) -> Config {
        Config {
            clock: Some(manual.clone()),
            retry_ms: WINDOW,
            flush_ms: 0,
            tick_ms: 3_600_000,
            retire_ms: 3_600_000,
            ..Config::default()
        }
    }
    fn open(&self, manual: &Arc<ManualClock>) -> Store {
        Store::open(&self.0, self.config(manual)).unwrap()
    }
    fn unlock(&self) {
        #[cfg(not(windows))]
        fs::remove_file(self.0.join("owner.lock")).unwrap();
    }
    fn legacy(&self, records: &[(Mutation, u64, u64)]) {
        fs::create_dir_all(&self.0).unwrap();
        let mut stripes = vec![Vec::new(); 4];
        for (m, version, at) in records {
            let b = mutation_bytes(m, *version, *at);
            let out = &mut stripes[stripe(&m.ns, &m.key)];
            out.extend(b"MBK1");
            out.extend((b.len() as u32).to_le_bytes());
            out.extend((!(b.len() as u32)).to_le_bytes());
            out.extend(crc32(&b).to_le_bytes());
            out.extend(b);
        }
        for (i, bytes) in stripes.iter().enumerate() {
            fs::write(self.0.join(format!("stripe-{i}.log")), bytes).unwrap();
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        // Imported backups are read-only on Windows.
        fn writable(dir: &Path) {
            for e in fs::read_dir(dir).unwrap() {
                let p = e.unwrap().path();
                if p.is_dir() {
                    writable(&p);
                } else {
                    let mut perms = fs::metadata(&p).unwrap().permissions();
                    if perms.readonly() {
                        perms.set_readonly(false);
                        fs::set_permissions(p, perms).unwrap();
                    }
                }
            }
        }
        if self.0.exists() {
            writable(&self.0);
            fs::remove_dir_all(&self.0).unwrap();
        }
    }
}
fn mutation(id: &str, deadline: u64) -> Mutation {
    Mutation {
        ns: "clock".into(),
        key: "key".into(),
        op_id: id.into(),
        deadline,
        ack: 1,
        expected: None,
        bytes: id.as_bytes().to_vec(),
        remove: false,
    }
}

#[test]
fn injected_clock_replay_dedup_before_and_after_window() {
    for elapsed in [WINDOW - 1, WINDOW, WINDOW + 1] {
        let f = Fixture::new();
        let manual = ManualClock::new(UTC);
        // Import authored records so this test isolates replay from runtime recording.
        let m = mutation("replay", u64::MAX);
        f.legacy(&[(m.clone(), 7, UTC)]);
        drop(f.open(&manual));
        f.unlock();
        // A new process has a new monotonic epoch.
        let manual = ManualClock::new(UTC + elapsed);
        let s = f.open(&manual);
        assert_eq!(
            s.replay_report().dedup_entries,
            usize::from(elapsed <= WINDOW),
            "elapsed={elapsed}"
        );
        if elapsed <= WINDOW {
            assert_eq!(s.mutate(m).version, 7);
            manual.jump_utc(10_000);
            assert_eq!(s.mutate(mutation("replay", u64::MAX)).version, 7);
            manual.jump_utc(-10_000);
            // Recovery must retain only the remaining window, not grant a new one.
            manual.advance(WINDOW - elapsed + 1);
            assert_eq!(s.mutate(mutation("replay", u64::MAX)).code, 13);
        }
    }
}

#[test]
fn injected_clock_replay_summary_expiry_before_and_after_window() {
    for elapsed in [WINDOW - 1, WINDOW, WINDOW + 1] {
        let f = Fixture::new();
        let manual = ManualClock::new(UTC);
        let mut config = f.config(&manual);
        config.summary_ms = WINDOW;
        let s = Store::open(&f.0, config.clone()).unwrap();
        {
            let mut cs = s.chunks.lock().unwrap();
            cs.log(
                UTC,
                chunks::Rec::Summary {
                    retired_ms: UTC,
                    keys: vec![("clock".into(), "expired".into())],
                },
            )
            .unwrap();
        }
        drop(s);
        f.unlock();
        manual.advance(elapsed);
        let s = Store::open(&f.0, config).unwrap();
        assert_eq!(
            s.get_status("clock", "expired").0,
            if elapsed < WINDOW { 1 } else { 2 }
        );
    }
}

#[test]
fn injected_clock_legacy_import_horizon() {
    for elapsed in [WINDOW - 1, WINDOW + 1] {
        let f = Fixture::new();
        let manual = ManualClock::new(UTC + elapsed);
        f.legacy(&[
            (mutation("old", u64::MAX), 7, UTC),
            (mutation("latest", u64::MAX), 8, UTC),
        ]);
        let config = f.config(&manual);
        let clock = chunks::Clock::new(Some(manual));
        import_legacy(&f.0, &config, &clock, 1).unwrap().unwrap();
        let rp = replay(&f.0, &config, &clock).unwrap_or_else(|(e, _)| panic!("{e}"));
        assert_eq!(
            rp.dedup.contains_key(&("clock".into(), "old".into())),
            elapsed <= WINDOW
        );
        assert_eq!(rp.committed[&("clock".into(), "key".into())].version, 8);
    }
}

#[test]
fn injected_clock_legacy_backup_graft_horizon_after_restart() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    f.legacy(&[
        (mutation("superseded", u64::MAX), 7, UTC),
        (mutation("latest", u64::MAX), 8, UTC),
    ]);
    drop(f.open(&manual));
    f.unlock();
    manual.advance(WINDOW - 1);
    let s = f.open(&manual);
    assert_eq!(s.mutate(mutation("superseded", u64::MAX)).version, 7);
    drop(s);
    f.unlock();
    manual.advance(2);
    let s = f.open(&manual);
    assert_eq!(s.replay_report().dedup_entries, 0);
    assert_eq!(s.get("clock", "key").unwrap().version, 8);
}

#[test]
fn injected_clock_runtime_records_utc() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let s = f.open(&manual);
    let m = mutation("record", UTC + 1000);
    assert_eq!(s.mutate(m.clone()).code, 0);
    let entry = s.committed.read().unwrap()[&("clock".into(), "key".into())].clone();
    let (_, _, at) = parse_record(&entry.location.payload().unwrap()).unwrap();
    assert_eq!(at, UTC);
}

#[test]
fn injected_clock_retry_window_uses_elapsed_time_and_ignores_utc_jumps() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let s = f.open(&manual);
    let m = mutation("retry", u64::MAX);
    let first = s.mutate(m.clone());
    assert_eq!(first.code, 0);
    manual.jump_utc(1_000_000);
    assert_eq!(
        s.mutate(m.clone()),
        first,
        "forward UTC must not close retry"
    );
    manual.jump_utc(-1_500_000);
    manual.advance(WINDOW);
    assert_eq!(s.mutate(m.clone()), first, "retry includes exact boundary");
    manual.advance(1);
    assert_eq!(s.mutate(m.clone()).code, 13, "elapsed window must close");
    manual.jump_utc(-500_000);
    assert_eq!(s.mutate(m).code, 13, "backward UTC must not reopen retry");
}

#[test]
fn injected_clock_policy_retry_and_dedup_cleanup_use_elapsed_time() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let s = f.open(&manual);
    let first = s.set_policy_with_id("clock", "", Some(Class::LATEST), "policy", u64::MAX);
    assert_eq!(first.code, 0);
    manual.jump_utc(1_000_000);
    assert_eq!(
        s.set_policy_with_id("clock", "", Some(Class::LATEST), "policy", 0),
        first
    );
    manual.advance(WINDOW + 1);
    manual.jump_utc(-1_000_000);
    assert_eq!(
        s.set_policy_with_id("clock", "", Some(Class::LATEST), "policy", 0)
            .code,
        13
    );
    assert_eq!(s.mutate(mutation("fresh", u64::MAX)).code, 0);
    let state = s.state.lock().unwrap();
    assert!(!state.dedup.contains_key(&("clock".into(), "policy".into())));
    assert!(state
        .retired_set
        .contains(&("clock".into(), "policy".into())));
}

#[test]
fn injected_clock_codec_deadline_is_client_utc() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let s = f.open(&manual);
    let call = |deadline: u64| {
        let mut p = deadline.to_le_bytes().to_vec();
        lp(&mut p, b"read");
        lp(&mut p, b"clock");
        lp(&mut p, b"key");
        let r = s.dispatch(&frame(GET, 1, &p)[4..]);
        u16::from_le_bytes(r[17..19].try_into().unwrap())
    };
    assert_eq!(call(UTC + 1), 0);
    manual.jump_utc(1);
    assert_eq!(call(UTC + 1), 5);
    manual.jump_utc(-1);
    assert_eq!(call(UTC + 1), 0);
}

#[test]
fn injected_clock_store_deadline_is_client_utc() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let s = f.open(&manual);
    assert_eq!(s.mutate(mutation("on-time", UTC + 1)).code, 0);
    manual.jump_utc(1);
    assert_eq!(s.mutate(mutation("late", UTC + 1)).code, 5);
    manual.jump_utc(-1);
    assert_eq!(s.mutate(mutation("after-correction", UTC + 1)).code, 0);
    assert_eq!(
        s.set_policy_with_id("clock", "", Some(Class::LATEST), "on-time-policy", UTC + 1)
            .code,
        0
    );
    manual.jump_utc(1);
    assert_eq!(
        s.set_policy_with_id("clock", "", Some(Class::LATEST), "late-policy", UTC + 1)
            .code,
        5
    );
}

#[test]
fn injected_clock_pending_mutation_deadline() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let mut config = f.config(&manual);
    let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
    config.sync_test_gate = Some(gate.clone());
    let s = Arc::new(Store::open(&f.0, config).unwrap());
    let first = s.clone();
    let writer = std::thread::spawn(move || {
        first.mutate(Mutation {
            ack: 2,
            ..mutation("pending", u64::MAX)
        })
    });
    {
        let mut entered = gate.0.lock().unwrap();
        while !entered.0 {
            let (next, timeout) = gate
                .1
                .wait_timeout(entered, Duration::from_secs(5))
                .unwrap();
            entered = next;
            assert!(!timeout.timed_out());
        }
    }
    manual.jump_utc(1);
    let duplicate = s.mutate(Mutation {
        ack: 2,
        ..mutation("pending", UTC + 1)
    });
    gate.0.lock().unwrap().1 = true;
    gate.1.notify_all();
    let original = writer.join().unwrap();
    assert_eq!(original.code, 0);
    assert_eq!(duplicate.code, 17);
}

#[test]
fn injected_clock_replay_report_start_is_utc() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let s = f.open(&manual);
    assert_eq!(s.replay_report().start_ms, UTC);
    drop(s);
    f.unlock();
    manual.advance(71);
    let s = f.open(&manual);
    assert_eq!(s.replay_report().start_ms, UTC + 71);
}

#[test]
fn injected_clock_replay_report_ready_is_utc() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let s = f.open(&manual);
    assert_eq!(s.replay_report().ready_ms, UTC);
    drop(s);
    f.unlock();
    manual.advance(71);
    let s = f.open(&manual);
    assert_eq!(s.replay_report().ready_ms, UTC + 71);
}

#[test]
fn injected_clock_pending_policy_wait_uses_utc() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let s = Arc::new(f.open(&manual));
    let original = s.set_policy_with_id("clock", "", Some(Class::LATEST), "policy-wait", u64::MAX);
    assert_eq!(original.code, 0);
    s.state
        .lock()
        .unwrap()
        .policy_publishing
        .insert(("clock".into(), "policy-wait".into()));
    let (sent, received) = std::sync::mpsc::channel();
    let other = s.clone();
    let join = std::thread::spawn(move || {
        sent.send(other.set_policy_with_id(
            "clock",
            "",
            Some(Class::LATEST),
            "policy-wait",
            UTC + 1,
        ))
        .unwrap();
    });
    let early = received.recv_timeout(Duration::from_millis(50));
    manual.jump_utc(1);
    s.done.notify_all();
    let outcome = early
        .as_ref()
        .cloned()
        .unwrap_or_else(|_| received.recv_timeout(Duration::from_secs(5)).unwrap());
    join.join().unwrap();
    assert!(early.is_err(), "injected future deadline must wait");
    assert_eq!(outcome.code, 17);
}

#[test]
fn injected_clock_timer_admission_rechecks_client_utc() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let s = f.open(&manual);
    let mut payload = Vec::new();
    lp(&mut payload, b"missing");
    assert_eq!(
        s.timer_call(
            TIMER_STATUS,
            "clock".into(),
            "timer-read".into(),
            UTC + 1,
            &mut Cursor::new(&payload)
        ),
        Ok(vec![0])
    );
    manual.jump_utc(1);
    assert_eq!(
        s.timer_call(
            TIMER_STATUS,
            "clock".into(),
            "timer-read".into(),
            UTC + 1,
            &mut Cursor::new(&payload)
        ),
        Err(5)
    );
}

#[test]
fn injected_clock_snapshot_ttl_is_monotonic() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let mut config = f.config(&manual);
    config.snapshot_ttl_ms = WINDOW;
    let s = Store::open(&f.0, config).unwrap();
    let (token, _) = s.snapshot_open("clock").unwrap();
    manual.jump_utc(1_000_000);
    assert!(s.snapshot_get("clock", &token).is_ok());
    manual.advance(WINDOW);
    assert!(matches!(s.snapshot_get("clock", &token), Err(20)));
}

#[test]
fn injected_clock_recovered_expiry_queue_uses_remaining_elapsed_window() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    f.legacy(&[(mutation("recovered", u64::MAX), 7, UTC)]);
    drop(f.open(&manual));
    f.unlock();
    let manual = ManualClock::new(UTC + WINDOW - 1);
    let s = f.open(&manual);
    assert!(s
        .state
        .lock()
        .unwrap()
        .dedup
        .contains_key(&("clock".into(), "recovered".into())));
    assert_eq!(s.mutate(mutation("fresh", u64::MAX)).code, 0);
    manual.advance(2);
    manual.jump_utc(-10_000);
    assert_eq!(s.mutate(mutation("cleanup", u64::MAX)).code, 0);
    let state = s.state.lock().unwrap();
    assert!(!state
        .dedup
        .contains_key(&("clock".into(), "recovered".into())));
    assert!(state
        .retired_set
        .contains(&("clock".into(), "recovered".into())));
    assert!(state.dedup.contains_key(&("clock".into(), "fresh".into())));
}

#[test]
fn injected_clock_final_mutation_admission_rechecks_utc() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let s = Arc::new(f.open(&manual));
    // Memory admission needs the chunk index while holding State. Hold it to cross the deadline
    // after the first check, without delaying or cancelling an admitted append.
    let chunks = s.chunks.lock().unwrap();
    let (sent, received) = std::sync::mpsc::channel();
    let other = s.clone();
    let writer = std::thread::spawn(move || {
        sent.send(other.mutate(mutation("final-check", UTC + 1)))
            .unwrap();
    });
    let bound = std::time::Instant::now() + Duration::from_secs(5);
    let reached = loop {
        if received.try_recv().is_ok() {
            break false;
        }
        if s.state.try_lock().is_err() {
            break true;
        }
        assert!(std::time::Instant::now() < bound);
        std::thread::yield_now();
    };
    manual.jump_utc(1);
    drop(chunks);
    writer.join().unwrap();
    assert!(
        reached,
        "future deadline must reach memory admission before the UTC jump"
    );
    assert_eq!(
        received.recv_timeout(Duration::from_secs(5)).unwrap().code,
        5
    );
    assert!(s.get("clock", "key").is_none());
}
