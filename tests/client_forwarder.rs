//! Independent expected frames for the additive client contracts.
use membus::client::*;
use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
const NAMES: [&str; 25] = [
    "incompatible_version",
    "unsupported",
    "malformed",
    "too_large",
    "deadline",
    "busy",
    "capacity",
    "quota",
    "index_cap",
    "read_only",
    "warming",
    "conflict",
    "retry_window_expired",
    "op_id_mismatch",
    "mem_not_enabled",
    "io_failed_uncertain",
    "pending_timeout_uncertain",
    "dedup_full",
    "pending_conflict",
    "cursor_expired",
    "invalid_cursor",
    "snapshot_full",
    "resume_gap",
    "subscription_missing",
    "timer_missing",
];
fn blob(b: &mut Vec<u8>, v: &[u8]) {
    b.extend((v.len() as u32).to_le_bytes());
    b.extend(v);
}
fn frame(op: u8, id: u64, p: &[u8]) -> Vec<u8> {
    let mut b = vec![1, 0, 0, 0, op | 128];
    b.extend(id.to_le_bytes());
    b.extend(0u16.to_le_bytes());
    b.extend(p);
    let mut a = (b.len() as u32).to_le_bytes().to_vec();
    a.extend(b);
    a
}
fn hello(codes: &[u16], retry: u64) -> Vec<u8> {
    let mut p = 7u64.to_le_bytes().to_vec();
    p.extend(65536u32.to_le_bytes());
    p.extend(4194304u32.to_le_bytes());
    p.extend(128u16.to_le_bytes());
    p.push(0);
    p.extend(retry.to_le_bytes());
    p.extend((codes.len() as u16).to_le_bytes());
    for &code in codes {
        p.extend(code.to_le_bytes());
        p.push(matches!(code, 5 | 6 | 10 | 11 | 16 | 17 | 18 | 19 | 22) as u8);
        blob(&mut p, NAMES[code as usize - 1].as_bytes());
    }
    if codes.contains(&24) {
        p.extend(1u16.to_le_bytes());
        p.extend(16777216u32.to_le_bytes());
        p.extend(0u64.to_le_bytes());
        p.extend(0u64.to_le_bytes());
        p.extend(64u16.to_le_bytes());
        p.extend(256u16.to_le_bytes());
        for v in [8388608u64, 16777216] {
            p.extend(v.to_le_bytes());
        }
        p.extend(100u32.to_le_bytes());
    }
    if codes.contains(&25) {
        p.extend(1u16.to_le_bytes());
        p.extend(4096u32.to_le_bytes());
        p.extend(8388608u64.to_le_bytes());
        p.extend(65536u32.to_le_bytes());
        p.extend(100u64.to_le_bytes());
        p.extend(86400000u64.to_le_bytes());
    }
    frame(1, 1, &p)
}
struct Script {
    input: VecDeque<u8>,
    output: Arc<Mutex<Vec<u8>>>,
}
impl Read for Script {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        let n = b.len().min(self.input.len()).min(3);
        for v in &mut b[..n] {
            *v = self.input.pop_front().unwrap();
        }
        Ok(n)
    }
}
impl Write for Script {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.output.lock().unwrap().extend(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Transport for Script {
    fn deadline(&mut self, d: Instant) -> io::Result<()> {
        if Instant::now() >= d {
            Err(io::ErrorKind::TimedOut.into())
        } else {
            Ok(())
        }
    }
}
fn d() -> Deadline {
    Deadline {
        unix_ms: 123456,
        local: Instant::now() + Duration::from_secs(5),
    }
}
fn connect(
    codes: Vec<u16>,
    scans: bool,
    subscriptions: bool,
    timers: bool,
    rest: Vec<Vec<u8>>,
    retry: u64,
) -> (Client<Script>, Arc<Mutex<Vec<u8>>>) {
    let out = Arc::new(Mutex::new(vec![]));
    let input = std::iter::once(hello(&codes, retry))
        .chain(rest)
        .flatten()
        .collect();
    let c = Client::handshake_with_features(
        Script {
            input,
            output: out.clone(),
        },
        Options {
            identity: "expected".into(),
            max_frame: 65536,
            scans,
            subscriptions,
        },
        AdditionalFeatures { timers },
        d().local,
    )
    .unwrap();
    (c, out)
}
fn combined(rest: Vec<Vec<u8>>) -> Client<Script> {
    connect((1..=25).collect(), true, true, true, rest, 86400000).0
}
fn set() -> TimerSetRequest {
    TimerSetRequest {
        operation_id: "ns/reopen/new".into(),
        namespace: "ns".into(),
        timer_id: "reopen".into(),
        expected: TimerExpectation::Absent,
        key: "timer/reopen".into(),
        anchor_ms: 10000,
        period_ms: 60000,
        jitter_low_ms: -9000,
        jitter_high_ms: 9000,
        payload: b"tick".to_vec(),
        ack: Ack::Flush,
    }
}
#[test]
fn exact_feature_vocabularies_and_old_struct_literals() {
    for (codes, scans, subs, timers) in [
        (19, false, false, false),
        (22, true, false, false),
        (24, false, true, false),
        (23, false, false, true),
        (25, true, true, true),
    ] {
        let vocabulary = if codes == 23 {
            (1..=22).chain([25]).collect()
        } else {
            (1..=codes).collect()
        };
        let (c, _) = connect(vocabulary, scans, subs, timers, vec![], 86400000);
        assert_eq!(c.feature_limits().timers.is_some(), timers);
        assert_eq!(c.feature_limits().signals.is_some(), subs);
        assert_eq!(c.negotiated().refusals.len(), codes as usize);
    }
    let baseline = Options {
        identity: "literal".into(),
        max_frame: 65536,
        scans: false,
        subscriptions: false,
    };
    let _: Negotiated = Negotiated {
        epoch: 1,
        max_frame: 65536,
        max_value: 4194304,
        max_batch: 128,
        server_mem_enabled: false,
        retry_window_ms: 1,
        scans: false,
        subscriptions: false,
        refusals: Default::default(),
    };
    let _: Subscription = Subscription {
        token: [0; 16],
        cursor: [0; 16],
    };
    let output = Arc::new(Mutex::new(vec![]));
    let c = Client::handshake(
        Script {
            input: hello(&(1..=19).collect::<Vec<_>>(), 10).into(),
            output: output.clone(),
        },
        baseline,
        d().local,
    )
    .unwrap();
    assert!(c.feature_limits().timers.is_none());
    let bytes = output.lock().unwrap();
    assert!(bytes.windows(7).any(|s| s == b"literal"));
    assert!(!bytes.windows(6).any(|s| s == b"timers"));
}
#[test]
fn malformed_or_unsolicited_features_are_refused() {
    for codes in [
        (1..=23).collect::<Vec<_>>(),
        (1..=25).collect(),
        (1..=22).chain([24]).collect(),
    ] {
        let out = Arc::new(Mutex::new(vec![]));
        let r = Client::handshake_with_features(
            Script {
                input: hello(&codes, 10).into(),
                output: out,
            },
            Options::new("x", 65536),
            AdditionalFeatures { timers: true },
            d().local,
        );
        assert!(r.is_err());
    }
    let mut bytes = hello(&(1..=25).collect::<Vec<_>>(), 10);
    bytes.pop();
    let out = Arc::new(Mutex::new(vec![]));
    assert!(Client::handshake_with_features(
        Script {
            input: bytes.into(),
            output: out
        },
        Options {
            identity: "x".into(),
            max_frame: 65536,
            scans: true,
            subscriptions: true
        },
        AdditionalFeatures { timers: true },
        d().local
    )
    .is_err());
}
#[test]
fn subscribe_serializes_values_cursor_and_exact_bounds() {
    let mut reply = vec![];
    blob(&mut reply, &[1; 16]);
    blob(&mut reply, &[2; 16]);
    let (mut c, out) = connect(
        (1..=24).collect(),
        true,
        true,
        false,
        vec![frame(16, 2, &reply)],
        10,
    );
    let resume = [2; 16];
    let s = c
        .subscribe_with(
            "ns",
            "event/",
            r"\\.\pipe\owned",
            SubscribeOptions {
                include_values: true,
                resume: Some(resume),
                max_frame: 65536,
            },
            "sub",
            d(),
        )
        .unwrap();
    assert_eq!(s.token, [1; 16]);
    let b = out.lock().unwrap();
    let hello_len = 4 + u32::from_le_bytes(b[..4].try_into().unwrap()) as usize;
    let mut suffix = vec![];
    blob(&mut suffix, b"event/");
    blob(&mut suffix, br"\\.\pipe\owned");
    suffix.push(1);
    blob(&mut suffix, &resume);
    blob(&mut suffix, b"membus.change/1");
    suffix.extend(65536u32.to_le_bytes());
    assert!(b[hello_len..].ends_with(&suffix));
}
#[test]
fn timer_attempts_keep_complete_identity_uncertainty_epoch_and_horizon() {
    let (mut c, out) = connect(
        (1..=25).collect(),
        true,
        true,
        true,
        vec![frame(21, 2, &9u64.to_le_bytes())],
        0,
    );
    let request = set();
    let mut a = c.prepare_timer_set(request.clone()).unwrap();
    assert_eq!(c.timer_set(&mut a, d()).unwrap(), 9);
    assert_eq!(a.request(), &request);
    assert!(matches!(
        c.timer_set(&mut a, d()),
        Err(Error::RetryWindowExpired)
    ));
    let sent = out.lock().unwrap().len();
    assert!(sent > 0);
    let mut unknown = combined(vec![]);
    let mut u = unknown.prepare_timer_set(request.clone()).unwrap();
    assert!(
        matches!(unknown.timer_set(&mut u,d()),Err(Error::Uncertain{epoch:7,operation_id,..}) if operation_id==request.operation_id)
    );
    assert_eq!(u.request(), &request);
    assert!(!unknown.is_usable());
    let mut newhello = hello(&(1..=25).collect::<Vec<_>>(), 86400000);
    newhello[19..27].copy_from_slice(&8u64.to_le_bytes());
    let output = Arc::new(Mutex::new(vec![]));
    let mut other = Client::handshake_with_features(
        Script {
            input: newhello.into(),
            output: output.clone(),
        },
        Options {
            identity: "x".into(),
            max_frame: 65536,
            scans: true,
            subscriptions: true,
        },
        AdditionalFeatures { timers: true },
        d().local,
    )
    .unwrap();
    let before = output.lock().unwrap().len();
    assert!(matches!(
        other.timer_set(&mut u, d()),
        Err(Error::EpochChanged {
            expected: 7,
            actual: 8
        })
    ));
    assert_eq!(output.lock().unwrap().len(), before);
}
#[test]
fn malformed_timer_results_poison_and_absence_is_explicit() {
    let mut c = combined(vec![frame(23, 2, &[0]), frame(21, 3, &[0; 8])]);
    assert!(c
        .timer_status("ns", "reopen", "status", d())
        .unwrap()
        .is_none());
    let mut a = c.prepare_timer_set(set()).unwrap();
    assert!(matches!(
        c.timer_set(&mut a, d()),
        Err(Error::Uncertain { .. })
    ));
    assert!(!c.is_usable());
    let mut c = connect((1..=19).collect(), false, false, false, vec![], 10).0;
    assert!(matches!(
        c.prepare_timer_set(set()),
        Err(Error::Invalid("timers unavailable"))
    ));
    assert!(matches!(
        c.timer_status("ns", "reopen", "status", d()),
        Err(Error::Invalid("timers unavailable"))
    ));
}
#[test]
fn timer_envelope_validation_keeps_signed_draw_and_due_identity() {
    let mut b = b"MBT1\x01\x00".to_vec();
    blob(&mut b, b"pass");
    for v in [3u64, 4, 1000, 900, (-100i64) as u64, 2] {
        b.extend(v.to_le_bytes());
    }
    blob(&mut b, b"payload");
    let w = TimerEnvelope::decode(&b).unwrap();
    assert_eq!(
        (w.generation, w.index, w.draw_ms, w.missed_count),
        (3, 4, -100, 2)
    );
    assert_eq!(w.payload, b"payload");
    let mut bad = b.clone();
    bad.push(0);
    assert!(TimerEnvelope::decode(&bad).is_err());
    let at = 6 + 4 + 4 + 5 * 8;
    let mut bad = b;
    bad[at..at + 8].fill(0);
    assert!(TimerEnvelope::decode(&bad).is_err());
}
