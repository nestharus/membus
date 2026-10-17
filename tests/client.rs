//! Independent expected frames; these helpers do not use the production frame/HELLO encoder.
use membus::client::*;
use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    time::{Duration, Instant},
};

const VOCAB: &[(&str, bool)] = &[
    ("incompatible_version", false),
    ("unsupported", false),
    ("malformed", false),
    ("too_large", false),
    ("deadline", true),
    ("busy", true),
    ("capacity", false),
    ("quota", false),
    ("index_cap", false),
    ("read_only", true),
    ("warming", true),
    ("conflict", false),
    ("retry_window_expired", false),
    ("op_id_mismatch", false),
    ("mem_not_enabled", false),
    ("io_failed_uncertain", true),
    ("pending_timeout_uncertain", true),
    ("dedup_full", true),
    ("pending_conflict", true),
    ("cursor_expired", false),
    ("invalid_cursor", false),
    ("snapshot_full", true),
];
fn blob(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend((bytes.len() as u32).to_le_bytes());
    out.extend(bytes);
}
fn wire(op: u8, id: u64, code: Option<u16>, p: &[u8]) -> Vec<u8> {
    let mut b = vec![1, 0, 0, 0, op];
    b.extend(id.to_le_bytes());
    if let Some(code) = code {
        b.extend(code.to_le_bytes());
    }
    b.extend(p);
    let mut out = (b.len() as u32).to_le_bytes().to_vec();
    out.extend(b);
    out
}
fn hello(epoch: u64, count: usize) -> Vec<u8> {
    let mut p = epoch.to_le_bytes().to_vec();
    p.extend(65536u32.to_le_bytes());
    p.extend(4194304u32.to_le_bytes());
    p.extend(128u16.to_le_bytes());
    p.push(0);
    p.extend(86400000u64.to_le_bytes());
    p.extend((count as u16).to_le_bytes());
    for (i, (name, transient)) in VOCAB[..count].iter().enumerate() {
        p.extend((i as u16 + 1).to_le_bytes());
        p.push(*transient as u8);
        blob(&mut p, name.as_bytes());
    }
    wire(129, 1, Some(0), &p)
}
struct Script {
    input: VecDeque<u8>,
    written: Vec<u8>,
    fail: Option<io::ErrorKind>,
}
impl Script {
    fn new(mut frames: Vec<Vec<u8>>) -> Self {
        Self {
            input: frames.drain(..).flatten().collect(),
            written: vec![],
            fail: None,
        }
    }
}
impl Read for Script {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        if self.input.is_empty() {
            if let Some(kind) = self.fail {
                return Err(io::Error::new(kind, "script timeout"));
            }
        }
        // Fragment every frame so incremental deadline enforcement is exercised.
        let n = b.len().min(self.input.len()).min(7);
        for v in &mut b[..n] {
            *v = self.input.pop_front().unwrap();
        }
        Ok(n)
    }
}
impl Write for Script {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.written.extend(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Transport for Script {
    fn deadline(&mut self, until: Instant) -> io::Result<()> {
        if Instant::now() >= until {
            Err(io::Error::new(io::ErrorKind::TimedOut, "deadline"))
        } else {
            Ok(())
        }
    }
}
fn deadline() -> Deadline {
    Deadline {
        unix_ms: 1234567890,
        local: Instant::now() + Duration::from_secs(2),
    }
}
fn connect(frames: Vec<Vec<u8>>) -> Client<Script> {
    Client::handshake(
        Script::new(frames),
        Options::new("independent-client", 65536),
        deadline().local,
    )
    .unwrap()
}
fn mutation(ack: Ack, expected: Option<u64>) -> MutationRequest {
    MutationRequest {
        operation_id: "caller-stable-operation".into(),
        namespace: "owned".into(),
        key: "key".into(),
        value: b"payload".to_vec(),
        ack,
        expected,
    }
}
fn success(ack: Ack) -> Vec<u8> {
    let mut p = 77u64.to_le_bytes().to_vec();
    p.push(ack as u8);
    p
}
fn value(bytes: &[u8], ack: u8) -> Vec<u8> {
    let mut p = vec![1];
    p.extend(41u64.to_le_bytes());
    p.push(ack);
    blob(&mut p, bytes);
    p
}
#[test]
fn legacy_hello_and_limits_are_real_not_defaulted() {
    let c = connect(vec![hello(51, 19)]);
    assert_eq!(c.negotiated().epoch, 51);
    assert_eq!(c.negotiated().refusals.len(), 19);
    assert_eq!(c.negotiated().max_frame, 65536);
    assert_eq!(c.negotiated().max_value, 4194304);
    assert_eq!(c.negotiated().max_batch, 128);
    assert!(!c.negotiated().scans);
    let mut options = Options::new("independent-client", 65536);
    options.scans = true;
    let c = Client::handshake(Script::new(vec![hello(51, 22)]), options, deadline().local).unwrap();
    assert!(c.negotiated().scans);
    assert!(Client::handshake(
        Script::new(vec![hello(51, 22)]),
        Options::new("independent-client", 65536),
        deadline().local
    )
    .is_err());
    let mut bad = hello(51, 19);
    bad[27..31].copy_from_slice(&65537u32.to_le_bytes());
    assert!(Client::handshake(
        Script::new(vec![bad]),
        Options::new("independent-client", 65536),
        deadline().local
    )
    .is_err());
    let mut bad = hello(51, 19);
    let last = bad.len() - 1;
    bad[last] ^= 1;
    assert!(Client::handshake(
        Script::new(vec![bad]),
        Options::new("independent-client", 65536),
        deadline().local
    )
    .is_err());
}
#[test]
fn committed_get_many_order_count_and_ack() {
    let mut batch = 3u16.to_le_bytes().to_vec();
    batch.extend(value(b"one", 1));
    batch.push(0);
    batch.extend(value(b"two", 2));
    let mut c = connect(vec![
        hello(51, 19),
        wire(130, 2, Some(0), &[0]),
        wire(131, 3, Some(0), &batch),
    ]);
    assert_eq!(
        c.get("owned", "absent", "read-a", deadline()).unwrap(),
        None
    );
    let v = c
        .get_many("owned", &["one", "absent", "two"], "read-b", deadline())
        .unwrap();
    assert_eq!(v[0].as_ref().unwrap().bytes, b"one");
    assert_eq!(v[0].as_ref().unwrap().ack, Ack::Os);
    assert_eq!(v[1], None);
    assert_eq!(v[2].as_ref().unwrap().ack, Ack::Flush);
    for bad in [vec![2, 0], value(b"x", 0), vec![2]] {
        let mut c = connect(vec![hello(51, 19), wire(130, 2, Some(0), &bad)]);
        assert!(c.get("owned", "one", "read", deadline()).is_err());
        assert!(!c.is_usable());
    }
    let mut c = connect(vec![hello(51, 19), wire(131, 2, Some(0), &[0, 0])]);
    assert!(c.get_many("owned", &["one"], "read", deadline()).is_err());
    assert!(!c.is_usable());
}
#[test]
fn os_put_and_flush_cas_encode_caller_identity_exactly() {
    // Capture independently and compare full operation bytes, including expected version and ack.
    struct Capture {
        script: Script,
        writes: std::sync::Arc<std::sync::Mutex<Vec<u8>>>,
    }
    impl Read for Capture {
        fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
            self.script.read(b)
        }
    }
    impl Write for Capture {
        fn write(&mut self, b: &[u8]) -> io::Result<usize> {
            self.writes.lock().unwrap().extend(b);
            Ok(b.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    impl Transport for Capture {
        fn deadline(&mut self, t: Instant) -> io::Result<()> {
            self.script.deadline(t)
        }
    }
    for (ack, expected, op) in [(Ack::Os, None, 4), (Ack::Flush, Some(41), 5)] {
        let writes = std::sync::Arc::new(std::sync::Mutex::new(vec![]));
        let io = Capture {
            script: Script::new(vec![
                hello(51, 19),
                wire(op | 128, 2, Some(0), &success(ack)),
            ]),
            writes: writes.clone(),
        };
        let mut c = Client::handshake(
            io,
            Options::new("independent-client", 65536),
            deadline().local,
        )
        .unwrap();
        writes.lock().unwrap().clear();
        let mut a = c.prepare(mutation(ack, expected)).unwrap();
        let got = c.mutate(&mut a, deadline()).unwrap();
        assert_eq!(got.epoch, 51);
        assert_eq!(got.achieved, ack);
        let mut p = 1234567890u64.to_le_bytes().to_vec();
        blob(&mut p, b"caller-stable-operation");
        blob(&mut p, b"owned");
        blob(&mut p, b"key");
        p.push(ack as u8);
        if let Some(v) = expected {
            p.extend(v.to_le_bytes());
        }
        blob(&mut p, b"payload");
        assert_eq!(*writes.lock().unwrap(), wire(op, 2, None, &p));
    }
}
#[test]
fn refusals_preserve_conflict_pending_and_uncertainty() {
    for code in [5, 12, 19, 16, 17] {
        let mut c = connect(vec![hello(51, 19), wire(133, 2, Some(code), &[])]);
        let mut a = c.prepare(mutation(Ack::Flush, Some(0))).unwrap();
        match c.mutate(&mut a, deadline()).unwrap_err() {
            Error::Refused(r) => {
                assert!(matches!(code, 5 | 12 | 19));
                assert_eq!(r.code, code);
                assert_eq!(r.transient, matches!(code, 5 | 19));
                assert!(c.is_usable());
            }
            Error::Uncertain {
                operation_id,
                epoch,
                ..
            } => {
                assert!(matches!(code, 16 | 17));
                assert_eq!(operation_id, a.request().operation_id);
                assert_eq!(epoch, 51);
                assert!(!c.is_usable());
            }
            e => panic!("wrong classification {e:?}"),
        }
    }
}
#[test]
fn mutation_association_partial_unknown_refusal_and_achieved_ack_are_uncertain() {
    let normal = wire(132, 2, Some(0), &success(Ack::Os));
    let mut wrong_id = normal.clone();
    wrong_id[9] = 99;
    let mut wrong_op = normal.clone();
    wrong_op[8] = 133;
    let mut wrong_major = normal.clone();
    wrong_major[4] = 2;
    let mut partial = normal.clone();
    partial.truncate(partial.len() - 1);
    let mut trailing = success(Ack::Os);
    trailing.push(0);
    let mut zero = 0u64.to_le_bytes().to_vec();
    zero.push(1);
    for reply in [
        wrong_id,
        wrong_op,
        wrong_major,
        partial,
        wire(132, 2, Some(999), &[]),
        wire(132, 2, Some(0), &success(Ack::Flush)),
        wire(132, 2, Some(0), &trailing),
        wire(132, 2, Some(0), &zero),
    ] {
        let mut c = connect(vec![hello(51, 19), reply]);
        let mut a = c.prepare(mutation(Ack::Os, None)).unwrap();
        assert!(matches!(
            c.mutate(&mut a, deadline()),
            Err(Error::Uncertain { .. })
        ));
        assert!(!c.is_usable());
        assert!(matches!(
            c.mutate(&mut a, deadline()),
            Err(Error::Invalid(_))
        ));
    }
}
#[test]
fn explicit_reconnect_same_attempt_epoch_and_horizon_no_auto_retry() {
    let mut io = Script::new(vec![hello(51, 19)]);
    io.fail = Some(io::ErrorKind::TimedOut);
    let mut first = Client::handshake(
        io,
        Options::new("independent-client", 65536),
        deadline().local,
    )
    .unwrap();
    let mut a = first.prepare(mutation(Ack::Os, None)).unwrap();
    assert!(matches!(
        first.mutate(&mut a, deadline()),
        Err(Error::Uncertain { .. })
    ));
    let mut next = connect(vec![
        hello(51, 19),
        wire(132, 2, Some(0), &success(Ack::Os)),
    ]);
    assert_eq!(next.mutate(&mut a, deadline()).unwrap().version, 77);
    let mut changed = connect(vec![hello(52, 19)]);
    assert!(matches!(
        changed.mutate(&mut a, deadline()),
        Err(Error::EpochChanged {
            expected: 51,
            actual: 52
        })
    ));
    // Exact same ID cannot be sent through an old Attempt after its conservative horizon.
    let mut h = hello(51, 19);
    h[38..46].copy_from_slice(&0u64.to_le_bytes());
    let mut expired = connect(vec![h, wire(132, 2, Some(0), &success(Ack::Os))]);
    let mut a = expired.prepare(mutation(Ack::Os, None)).unwrap();
    assert!(expired.mutate(&mut a, deadline()).is_ok());
    assert!(matches!(
        expired.mutate(&mut a, deadline()),
        Err(Error::RetryWindowExpired)
    ));
}
#[test]
fn preflight_never_marks_mutation_uncertain() {
    let mut c = connect(vec![hello(51, 19)]);
    let mut a = c.prepare(mutation(Ack::Os, None)).unwrap();
    let d = Deadline {
        unix_ms: 1,
        local: Instant::now() - Duration::from_millis(1),
    };
    assert!(matches!(c.mutate(&mut a, d), Err(Error::Invalid(_))));
    assert!(c.is_usable());
    let mut m = mutation(Ack::Os, None);
    m.key.clear();
    assert!(c.prepare(m).is_err());
    assert!(c.get_many("owned", &[], "get", deadline()).is_err());
}
fn subscription_hello(history: u64, history_bytes: u64) -> Vec<u8> {
    let mut h = hello(51, 22);
    h[46..48].copy_from_slice(&24u16.to_le_bytes());
    for (code, name) in [(23u16, "resume_gap"), (24, "subscription_missing")] {
        h.extend(code.to_le_bytes());
        h.push(0);
        blob(&mut h, name.as_bytes());
    }
    h.extend(1u16.to_le_bytes());
    h.extend(65536u32.to_le_bytes());
    h.extend(history.to_le_bytes());
    h.extend(history_bytes.to_le_bytes());
    h.extend(64u16.to_le_bytes());
    h.extend(256u16.to_le_bytes());
    h.extend(8388608u64.to_le_bytes());
    h.extend(16777216u64.to_le_bytes());
    h.extend(100u32.to_le_bytes());
    let size = (h.len() - 4) as u32;
    h[..4].copy_from_slice(&size.to_le_bytes());
    h
}
struct CountScript(Script, std::sync::Arc<std::sync::atomic::AtomicUsize>);
impl Read for CountScript {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        self.0.read(b)
    }
}
impl Write for CountScript {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.1
            .fetch_add(b.len(), std::sync::atomic::Ordering::SeqCst);
        self.0.write(b)
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Transport for CountScript {
    fn deadline(&mut self, t: Instant) -> io::Result<()> {
        self.0.deadline(t)
    }
}
#[test]
fn m3_malformed_replies_poison_without_later_send() {
    for op in [10u8, 11, 13, 16, 17, 18] {
        let subscriptions = op >= 16;
        let h = if subscriptions {
            subscription_hello(0, 0)
        } else {
            hello(51, 22)
        };
        let mut options = Options::new("m3-independent-control", 65536);
        options.scans = true;
        options.subscriptions = subscriptions;
        let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut c = Client::handshake(
            CountScript(
                Script::new(vec![h, wire(op | 128, 2, Some(0), &[99])]),
                count.clone(),
            ),
            options,
            deadline().local,
        )
        .unwrap();
        let token = [0; 16];
        let bad = match op {
            10 => c.snapshot_open("owned", "open", deadline()).map(|_| ()),
            11 => c
                .scan_prefix("owned", "head/", &token, &[], 1, 65536, "page", deadline())
                .map(|_| ()),
            13 => c.snapshot_release("owned", &token, "release", deadline()),
            16 => c
                .subscribe("owned", "", r"\\.\pipe\owned", "sub", deadline())
                .map(|_| ()),
            17 => c.unsubscribe("owned", &token, "unsub", deadline()),
            _ => c
                .subscription_status("owned", &token, "status", deadline())
                .map(|_| ()),
        };
        assert!(matches!(bad, Err(Error::Protocol(_))), "opcode {op}");
        assert!(!c.is_usable());
        let before = count.load(std::sync::atomic::Ordering::SeqCst);
        assert!(c.get("owned", "key", "later", deadline()).is_err());
        assert_eq!(count.load(std::sync::atomic::Ordering::SeqCst), before);
    }
    // Fully formed but internally incomplete progress must also poison.
    let token = [7; 16];
    let mut p = vec![];
    blob(&mut p, &token);
    blob(&mut p, b"next");
    p.extend([0, 0, 0]);
    let mut o = Options::new("m3-independent-control", 65536);
    o.scans = true;
    let mut c = Client::handshake(
        Script::new(vec![hello(51, 22), wire(139, 2, Some(0), &p)]),
        o,
        deadline().local,
    )
    .unwrap();
    assert!(c
        .scan_prefix("owned", "head/", &token, &[], 1, 65536, "page", deadline())
        .is_err());
    assert!(!c.is_usable());
}
