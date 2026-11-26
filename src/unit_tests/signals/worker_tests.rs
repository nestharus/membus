//! Funded active fan-out through the production publisher and delivery worker.
use super::*;
use crate::{
    chunks::{Clock, ManualClock},
    subscription_contract::*,
    subscription_resources::SubscriptionAccount,
};

static SERIAL: Mutex<()> = Mutex::new(());
struct Capture {
    frames: Vec<Vec<u8>>,
    entered: bool,
    release: bool,
    fail_first: bool,
}
static CAPTURE: Mutex<Capture> = Mutex::new(Capture {
    frames: Vec::new(),
    entered: false,
    release: false,
    fail_first: false,
});
static WAKE: Condvar = Condvar::new();
fn sink(_: &str, frame: &[u8], _: u32, _: &mut Option<Connection>, _: &Clock) -> io::Result<()> {
    let mut c = CAPTURE.lock().unwrap();
    c.frames.push(frame.to_vec());
    if !c.entered {
        c.entered = true;
        WAKE.notify_all();
        while !c.release {
            let (next, timeout) = WAKE.wait_timeout(c, Duration::from_secs(3)).unwrap();
            c = next;
            if timeout.timed_out() {
                c.release = true;
                return Err(io::Error::other("authored gate timed out"));
            }
        }
        if c.fail_first {
            return Err(io::Error::other("authored old-generation failure"));
        }
    }
    Ok(())
}
fn inert(_: &str, _: &[u8], _: u32, _: &mut Option<Connection>, _: &Clock) -> io::Result<()> {
    Ok(())
}
fn fixture(sink: Sink) -> (Hub, Arc<Mutex<crate::store::State>>, Arc<ManualClock>) {
    let config = Config {
        capacity: 16 * 1024 * 1024,
        signal_history: 0,
        signal_bytes: 0,
        ..Config::default()
    };
    let clock = ManualClock::new(1000);
    let h = Hub::with_sink(7, &config, sink, Clock::new(Some(clock.clone())));
    let owner = Arc::new(Mutex::new(crate::store::State::default()));
    h.bind_account(SubscriptionAccount::for_capture(
        &owner,
        &config,
        Arc::new(crate::snapshots::ScanUsage::default()),
        0,
    ));
    (h, owner, clock)
}
fn add(h: &Hub, lifetime: u64) -> HintReceipt {
    let endpoint = r"\\.\pipe\authored";
    let options = HintOptions {
        profile: 1,
        lifetime_ms: lifetime,
        write_ms: 20,
        incarnation: [3; 16],
        recipient: [4; 16],
    };
    let grant = h
        .account
        .get()
        .unwrap()
        .reserve(finite::demand(2, 1, endpoint.len()).unwrap(), false)
        .unwrap();
    h.install_hint("ns", "p", endpoint, options, grant).unwrap()
}
fn wait_for(mut condition: impl FnMut() -> bool) -> bool {
    let until = std::time::Instant::now() + Duration::from_secs(3);
    while !condition() {
        if std::time::Instant::now() >= until {
            return false;
        }
        thread::yield_now();
    }
    true
}
fn value() -> Arc<Value> {
    Arc::new(Value {
        version: 1,
        ack: 1,
        bytes: vec![1],
        tombstone: false,
    })
}

#[test]
fn funded_active_broadcast_has_linear_worker_visits_with_mixed_legacy() {
    let _serial = SERIAL.lock().unwrap();
    let mut measurements = Vec::new();
    for population in [8usize, 32, 128, 512] {
        *CAPTURE.lock().unwrap() = Capture {
            frames: Vec::new(),
            entered: false,
            release: false,
            fail_first: false,
        };
        let (mut h, owner, _) = fixture(sink);
        let mut receipts = vec![add(&h, 10_000)];
        h.publish("ns", "p/warm", value());
        assert!(wait_for(|| CAPTURE.lock().unwrap().entered));
        for _ in 1..population {
            receipts.push(add(&h, 10_000));
        }
        let legacy = h
            .subscribe(
                "ns",
                "p".into(),
                r"\\.\pipe\legacy".into(),
                false,
                &[],
                "membus.change/1",
                1024,
            )
            .unwrap();
        h.publish("ns", "p/broadcast", value());
        let funded = owner.lock().unwrap().total_reserved();
        {
            let mut s = h.state.0.lock().unwrap();
            assert_eq!(s.finite.len(), population);
            s.worker_visits = (0, 0);
        }
        CAPTURE.lock().unwrap().release = true;
        WAKE.notify_all();
        let drained = wait_for(|| {
            let s = h.state.0.lock().unwrap();
            s.finite
                .iter()
                .all(|row| row.pending.is_none() && Arc::strong_count(&row.slot) == 1)
                && s.queue_bytes == 0
                && CAPTURE.lock().unwrap().frames.len() == population + 2
        });
        let visits = h.state.0.lock().unwrap().worker_visits;
        let frames = CAPTURE.lock().unwrap().frames.clone();
        for receipt in &receipts {
            h.control_hint("ns", *receipt, true).unwrap();
        }
        h.unsubscribe("ns", &legacy[4..20]).unwrap();
        h.stop_for_capture().unwrap();
        assert_eq!(owner.lock().unwrap().total_reserved(), 0);
        assert!(drained, "complete funded population did not drain");
        assert!(funded > population * 64);
        for receipt in &receipts {
            assert_eq!(
                frames
                    .iter()
                    .filter(|f| decode_hint(f, receipt.binding) == Ok(2))
                    .count(),
                1
            );
        }
        assert_eq!(frames.iter().filter(|f| &f[4..8] == b"MBC1").count(), 1);
        println!("population={population} funded={funded} expiry_visits={} selection_visits={} finite_writes={population} legacy_writes=1",visits.0,visits.1);
        measurements.push((population, visits));
    }
    assert!(
        measurements
            .iter()
            .all(|(n, (expiry, selection))| expiry + selection <= 4 * n + 4),
        "quiescent successful fan-out revisited the full registry per write: {measurements:?}"
    );
}

#[test]
fn expiry_bound_tracks_earlier_install_renewal_and_cancelled_minimum() {
    let (mut h, owner, clock) = fixture(inert);
    let long = add(&h, 1000);
    let old = add(&h, 40);
    clock.advance(10);
    let renewed = h.control_hint("ns", old, false).unwrap();
    assert_eq!(h.control_hint("ns", old, true), Err(24));
    clock.advance(30);
    h.state.1.notify_all();
    assert_eq!(h.state.0.lock().unwrap().finite.len(), 2);
    let earlier = add(&h, 5);
    clock.advance(5);
    h.state.1.notify_all();
    let earlier_expired = wait_for(|| h.state.0.lock().unwrap().finite.len() == 2);
    let earlier_result = h.control_hint("ns", earlier, true);
    clock.advance(5);
    h.state.1.notify_all();
    let renewed_expired = wait_for(|| h.state.0.lock().unwrap().finite.len() == 1);
    let renewed_result = h.control_hint("ns", renewed, true);
    h.control_hint("ns", long, true).unwrap();
    // A removed minimum may leave only an earlier conservative wakeup.
    let removed = add(&h, 3);
    h.control_hint("ns", removed, true).unwrap();
    let survivor = add(&h, 100);
    clock.advance(3);
    h.state.1.notify_all();
    let next = h.control_hint("ns", survivor, false).unwrap();
    h.control_hint("ns", next, true).unwrap();
    h.stop_for_capture().unwrap();
    assert_eq!(owner.lock().unwrap().total_reserved(), 0);
    assert!(earlier_expired && renewed_expired);
    assert_eq!(earlier_result, Err(24));
    assert_eq!(renewed_result, Err(24));
}

#[test]
fn active_cohort_expiry_cancel_renew_and_late_failure_keep_exact_owners() {
    let _serial = SERIAL.lock().unwrap();
    for population in [8usize, 64, 256] {
        *CAPTURE.lock().unwrap() = Capture {
            frames: Vec::new(),
            entered: false,
            release: false,
            fail_first: true,
        };
        let (mut h, owner, clock) = fixture(sink);
        let first = add(&h, 100);
        h.publish("ns", "p/warm", value());
        assert!(wait_for(|| CAPTURE.lock().unwrap().entered));
        let mut short = Vec::new();
        let mut live = Vec::new();
        for i in 1..population {
            if i % 2 == 0 {
                short.push(add(&h, 40));
            } else {
                live.push(add(&h, 100));
            }
        }
        h.publish("ns", "p/broadcast", value());
        let funded = owner.lock().unwrap().total_reserved();
        clock.advance(10);
        let renewed = h.control_hint("ns", first, false).unwrap();
        let stale = h.control_hint("ns", first, true);
        let cancelled = live.pop().unwrap();
        h.control_hint("ns", cancelled, true).unwrap();
        clock.advance(30);
        h.state.1.notify_all();
        for receipt in &short {
            assert_eq!(h.control_hint("ns", *receipt, false), Err(24));
        }
        let replacement = add(&h, 100);
        h.publish("ns", "p/latest", value());
        let retained = owner.lock().unwrap().total_reserved();
        let selected_owned = {
            let s = h.state.0.lock().unwrap();
            s.finite
                .iter()
                .find(|r| r.binding == first.binding)
                .map(|r| Arc::strong_count(&r.slot))
        };
        CAPTURE.lock().unwrap().release = true;
        WAKE.notify_all();
        let drained = wait_for(|| {
            let s = h.state.0.lock().unwrap();
            s.finite
                .iter()
                .all(|r| r.pending.is_none() && Arc::strong_count(&r.slot) == 1)
        });
        let frames = CAPTURE.lock().unwrap().frames.clone();
        let renewed_result = h.control_hint("ns", renewed, true);
        for receipt in &live {
            h.control_hint("ns", *receipt, true).unwrap();
        }
        h.control_hint("ns", replacement, true).unwrap();
        h.stop_for_capture().unwrap();
        assert_eq!(owner.lock().unwrap().total_reserved(), 0);
        assert!(drained);
        assert_eq!(stale, Err(24));
        assert_eq!(renewed_result, Ok(renewed));
        assert_eq!(selected_owned, Some(2));
        assert!(retained < funded);
        assert!(frames
            .iter()
            .any(|f| decode_hint(f, renewed.binding) == Ok(3)));
        assert!(frames
            .iter()
            .any(|f| decode_hint(f, replacement.binding) == Ok(3)));
        assert!(!frames
            .iter()
            .any(|f| decode_hint(f, cancelled.binding).is_ok()));
        for receipt in &short {
            assert!(!frames
                .iter()
                .any(|f| decode_hint(f, receipt.binding).is_ok()));
        }
        println!("active_lifecycle_population={population} funded={funded} retained_after_expiry_cancel_replacement={retained} charge_after_join=0");
    }
}
