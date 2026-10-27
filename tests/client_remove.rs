//! Authored independent REMOVE frames; no resident or backing state is opened.
use membus::client::*;
use std::{
    collections::VecDeque,
    io::{self, Read, Write},
    sync::{Arc, Mutex},
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
];
fn blob(b: &mut Vec<u8>, v: &[u8]) {
    b.extend((v.len() as u32).to_le_bytes());
    b.extend(v);
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
fn hello(epoch: u64, retry: u64) -> Vec<u8> {
    let mut b = epoch.to_le_bytes().to_vec();
    b.extend(65536u32.to_le_bytes());
    b.extend(4194304u32.to_le_bytes());
    b.extend(128u16.to_le_bytes());
    b.push(0);
    b.extend(retry.to_le_bytes());
    b.extend(19u16.to_le_bytes());
    for (i, (n, t)) in VOCAB.iter().enumerate() {
        b.extend((i as u16 + 1).to_le_bytes());
        b.push(*t as u8);
        blob(&mut b, n.as_bytes());
    }
    wire(129, 1, Some(0), &b)
}
fn ok(ack: Ack) -> Vec<u8> {
    let mut b = 77u64.to_le_bytes().to_vec();
    b.push(ack as u8);
    wire(134, 2, Some(0), &b)
}
struct Script {
    input: VecDeque<u8>,
    writes: Arc<Mutex<Vec<u8>>>,
}
impl Read for Script {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        let n = b.len().min(self.input.len()).min(7);
        for v in &mut b[..n] {
            *v = self.input.pop_front().unwrap();
        }
        Ok(n)
    }
}
impl Write for Script {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.writes.lock().unwrap().extend(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Transport for Script {
    fn deadline(&mut self, t: Instant) -> io::Result<()> {
        if Instant::now() >= t {
            Err(io::ErrorKind::TimedOut.into())
        } else {
            Ok(())
        }
    }
}
fn d() -> Deadline {
    Deadline {
        unix_ms: 1234567890,
        local: Instant::now() + Duration::from_secs(5),
    }
}
fn connect(frames: Vec<Vec<u8>>) -> (Client<Script>, Arc<Mutex<Vec<u8>>>) {
    let writes = Arc::new(Mutex::new(Vec::new()));
    let client = Client::handshake(
        Script {
            input: frames.into_iter().flatten().collect(),
            writes: writes.clone(),
        },
        Options::new("remove-control", 65536),
        d().local,
    )
    .unwrap();
    writes.lock().unwrap().clear();
    (client, writes)
}
fn request(ack: Ack, expected: u64) -> RemoveRequest {
    RemoveRequest {
        operation_id: "original-remove".into(),
        namespace: "authored".into(),
        key: "item".into(),
        ack,
        expected,
    }
}
#[test]
fn exact_remove_frame_has_no_value_and_keeps_full_revision() {
    for ack in [Ack::Os, Ack::Flush] {
        for expected in [0, 41, u64::MAX - 1, u64::MAX] {
            let (mut c, writes) = connect(vec![hello(51, 86400000), ok(ack)]);
            let req = request(ack, expected);
            let size = c.validate_remove(&req).unwrap();
            let mut a = c.prepare_remove(req.clone()).unwrap();
            assert_eq!(a.request(), &req);
            assert_eq!(a.epoch(), 51);
            let result = c.remove(&mut a, d()).unwrap();
            assert_eq!(result.version, 77);
            assert_eq!(result.achieved, ack);
            let mut p = d().unix_ms.to_le_bytes().to_vec();
            blob(&mut p, b"original-remove");
            blob(&mut p, b"authored");
            blob(&mut p, b"item");
            p.push(ack as u8);
            p.extend(expected.to_le_bytes());
            assert_eq!(*writes.lock().unwrap(), wire(6, 2, None, &p));
            assert_eq!(size, p.len() + 13);
        }
    }
}
#[test]
fn remove_refusals_distinguish_no_new_admission_from_uncertainty() {
    for code in [2, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 16, 17, 18, 19] {
        let (mut c, _) = connect(vec![hello(51, 86400000), wire(134, 2, Some(code), &[])]);
        let mut a = c.prepare_remove(request(Ack::Flush, 0)).unwrap();
        match c.remove(&mut a, d()).unwrap_err() {
            Error::Refused(r) => {
                assert!(!matches!(code, 16 | 17));
                assert_eq!(r.code, code);
                assert!(c.is_usable());
            }
            Error::Uncertain {
                epoch,
                operation_id,
                ..
            } => {
                assert!(matches!(code, 16 | 17));
                assert_eq!(epoch, 51);
                assert_eq!(operation_id, a.request().operation_id);
                assert!(!c.is_usable());
            }
            e => panic!("unexpected {e:?}"),
        }
    }
}
#[test]
fn lost_remove_reply_reuses_original_token_without_resetting_epoch_or_horizon() {
    let (mut first, _) = connect(vec![hello(51, 86400000)]);
    let mut a = first.prepare_remove(request(Ack::Os, 41)).unwrap();
    assert!(matches!(
        first.remove(&mut a, d()),
        Err(Error::Uncertain { .. })
    ));
    let (mut next, writes) = connect(vec![hello(51, 86400000), ok(Ack::Os)]);
    assert_eq!(next.remove(&mut a, d()).unwrap().version, 77);
    let sent = writes.lock().unwrap().clone();
    assert_eq!(sent[8], 6);
    let (mut changed, writes) = connect(vec![hello(52, 86400000)]);
    assert!(matches!(
        changed.remove(&mut a, d()),
        Err(Error::EpochChanged {
            expected: 51,
            actual: 52
        })
    ));
    assert!(writes.lock().unwrap().is_empty());
    let (mut zero, _) = connect(vec![hello(51, 0), ok(Ack::Os)]);
    let mut a = zero.prepare_remove(request(Ack::Os, 0)).unwrap();
    zero.remove(&mut a, d()).unwrap();
    assert!(matches!(
        zero.remove(&mut a, d()),
        Err(Error::RetryWindowExpired)
    ));
}
#[test]
fn remove_preflight_refuses_without_writes_or_consuming_retry_window() {
    let (mut c, writes) = connect(vec![hello(51, 0), ok(Ack::Os)]);
    for field in 0..3 {
        let mut m = request(Ack::Os, 0);
        match field {
            0 => m.operation_id.clear(),
            1 => m.namespace.clear(),
            _ => m.key.clear(),
        }
        assert!(c.validate_remove(&m).is_err());
        assert!(c.prepare_remove(m).is_err());
    }
    let mut m = request(Ack::Os, 0);
    m.key = "x".repeat(1025);
    assert!(c.validate_remove(&m).is_err());
    assert!(c.prepare_remove(m).is_err());
    let mut a = c.prepare_remove(request(Ack::Os, 0)).unwrap();
    assert!(matches!(
        c.remove(
            &mut a,
            Deadline {
                unix_ms: 0,
                local: Instant::now() - Duration::from_secs(1)
            }
        ),
        Err(Error::Invalid(_))
    ));
    assert!(writes.lock().unwrap().is_empty());
    assert!(c.is_usable());
    c.remove(&mut a, d()).unwrap();
}
#[test]
fn malformed_foreign_partial_or_wrong_ack_remove_reply_is_uncertain() {
    let mut wrong_id = ok(Ack::Os);
    wrong_id[9] = 99;
    let mut wrong_op = ok(Ack::Os);
    wrong_op[8] = 133;
    let mut partial = ok(Ack::Os);
    partial.pop();
    let mut trailing = 77u64.to_le_bytes().to_vec();
    trailing.extend([1, 0]);
    let mut zero = 0u64.to_le_bytes().to_vec();
    zero.push(1);
    for reply in [
        wrong_id,
        wrong_op,
        partial,
        ok(Ack::Flush),
        wire(134, 2, Some(0), &zero),
        wire(134, 2, Some(0), &trailing),
        wire(134, 2, Some(999), &[]),
    ] {
        let (mut c, _) = connect(vec![hello(51, 86400000), reply]);
        let mut a = c.prepare_remove(request(Ack::Os, 41)).unwrap();
        assert!(matches!(
            c.remove(&mut a, d()),
            Err(Error::Uncertain { .. })
        ));
        assert!(!c.is_usable());
    }
}

#[test]
fn remove_preflight_uses_negotiated_exact_frame_limit_after_reconnect() {
    let mut small = hello(51, 0);
    small[27..31].copy_from_slice(&1024u32.to_le_bytes());
    let (large, _) = connect(vec![hello(51, 0)]);
    let (mut c, writes) = connect(vec![small, ok(Ack::Os)]);
    // Body overhead is 42 bytes; the identifiers contribute 1 + 1 + 980.
    let fitting = RemoveRequest {
        operation_id: "o".into(),
        namespace: "n".into(),
        key: "k".repeat(980),
        ack: Ack::Os,
        expected: 0,
    };
    let mut excess = fitting.clone();
    excess.key.push('k');
    assert_eq!(c.validate_remove(&fitting).unwrap(), 1024);
    assert!(matches!(
        c.validate_remove(&excess),
        Err(Error::Invalid("frame limit"))
    ));
    assert!(matches!(
        c.prepare_remove(excess.clone()),
        Err(Error::Invalid("frame limit"))
    ));
    // A token prepared on a larger connection must also honor the new limit.
    let mut prepared = large.prepare_remove(excess).unwrap();
    assert!(matches!(
        c.remove(&mut prepared, d()),
        Err(Error::Invalid("frame limit"))
    ));
    assert!(writes.lock().unwrap().is_empty());
    let mut prepared = c.prepare_remove(fitting).unwrap();
    c.remove(&mut prepared, d()).unwrap();
    assert_eq!(writes.lock().unwrap().len(), 1028);
    assert_eq!(&writes.lock().unwrap()[9..17], &2u64.to_le_bytes());
}
