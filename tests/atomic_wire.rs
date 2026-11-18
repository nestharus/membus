//! Authored provider-bound transports; every selected Store is retained explicitly.
use membus::{
    atomic::{AtomicAck, AtomicRequest, FinalChange, PublicationError},
    atomic_population::PopulationLimits,
    client::*,
    Config, Store,
};
use std::{
    io::{self, Read, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
fn root(name: &str) -> PathBuf {
    let base = PathBuf::from(
        std::env::var_os("MEMBUS_PROVIDER_TEST_ROOT").expect("explicit retained fixture root"),
    );
    assert!(base.is_absolute() && base.is_dir());
    let p = base.join(format!(
        "wire-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&p).unwrap();
    println!("retained {}", p.display());
    p
}
fn config() -> Config {
    Config {
        flush_ms: 0,
        cache_bytes: 0,
        signal_bytes: 0,
        tick_ms: 60000,
        retire_ms: 60000,
        ..Default::default()
    }
}
fn d() -> Deadline {
    Deadline {
        unix_ms: membus::now_ms() + 30000,
        local: Instant::now() + Duration::from_secs(30),
    }
}
struct Peer {
    s: Arc<Store>,
    pending: Vec<u8>,
    reply: Vec<u8>,
    damage: Arc<Mutex<u8>>,
    max: usize,
    max_value: u32,
}
impl Read for Peer {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        let n = b.len().min(self.reply.len()).min(37);
        b[..n].copy_from_slice(&self.reply[..n]);
        self.reply.drain(..n);
        Ok(n)
    }
}
impl Write for Peer {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.pending.extend(b);
        if self.pending.len() >= 4 {
            let n = u32::from_le_bytes(self.pending[..4].try_into().unwrap()) as usize;
            if self.pending.len() == n + 4 {
                let op = self.pending[8];
                self.reply = self
                    .s
                    .dispatch_for_limit(&self.pending[4..], false, self.max);
                self.pending.clear();
                if op == membus::HELLO {
                    // Successful HELLO body starts after the frame/status, then epoch and max_frame.
                    self.reply[31..35].copy_from_slice(&self.max_value.to_le_bytes());
                }
                if op == membus::ATOMIC_CAPTURE {
                    match *self.damage.lock().unwrap() {
                        5 => self.reply[19..23].copy_from_slice(b"????"),
                        6 => self.reply[56..60].copy_from_slice(&1u32.to_le_bytes()),
                        7 => self.reply[56..60].copy_from_slice(&4097u32.to_le_bytes()),
                        8 => self.reply[40..48].fill(0),
                        9 => self.reply[23] = 255,
                        10 => self.reply[32..40].copy_from_slice(b"other___"),
                        11 => {
                            self.reply[..4].copy_from_slice(&((self.max + 1) as u32).to_le_bytes())
                        }
                        _ => (),
                    }
                }
                if op == membus::ATOMIC_PUBLISH {
                    let damage = *self.damage.lock().unwrap();
                    match damage {
                        1 => self.reply.clear(),
                        2 => {
                            *self.reply.last_mut().unwrap() = 99;
                        }
                        3 => {
                            self.reply[19..23].copy_from_slice(b"????");
                        }
                        4 => {
                            self.reply.truncate(20);
                        }
                        _ => (),
                    }
                }
            }
        }
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Transport for Peer {
    fn deadline(&mut self, _: Instant) -> io::Result<()> {
        Ok(())
    }
}
fn client(s: &Arc<Store>, damage: Arc<Mutex<u8>>, max: usize) -> Client<Peer> {
    client_with_value_limit(s, damage, max, membus::MAX_VALUE as u32)
}

fn client_with_value_limit(
    s: &Arc<Store>,
    damage: Arc<Mutex<u8>>,
    max: usize,
    max_value: u32,
) -> Client<Peer> {
    Client::handshake(
        Peer {
            s: s.clone(),
            pending: vec![],
            reply: vec![],
            damage,
            max,
            max_value,
        },
        Options::new("authored-wire", max),
        d().local,
    )
    .unwrap()
}
fn request(c: &mut Client<Peer>, id: &str, changes: Vec<FinalChange>) -> AtomicRequest {
    c.capture_atomic_namespace("authored", "capture", d())
        .unwrap()
        .prepare(id, AtomicAck::Flush, changes)
        .unwrap()
}
#[test]
fn remote_phantom_conflict_and_whole_set_success() {
    let s = Arc::new(Store::open(&root("phantom"), config()).unwrap());
    let fault = Arc::new(Mutex::new(0));
    let mut a = client(&s, fault.clone(), membus::MAX_FRAME);
    let mut b = client(&s, fault, membus::MAX_FRAME);
    let x = request(
        &mut a,
        "one",
        vec![
            FinalChange::put("a", b"A".to_vec()),
            FinalChange::put("b", b"B".to_vec()),
        ],
    );
    let y = request(
        &mut b,
        "two",
        vec![FinalChange::put("phantom", b"p".to_vec())],
    );
    b.publish_atomic(&y, d()).unwrap();
    assert!(matches!(
        a.publish_atomic(&x, d()),
        Err(Error::Atomic(PublicationError::Conflict))
    ));
    assert!(s.get("authored", "a").is_none());
    let x = request(
        &mut a,
        "three",
        vec![
            FinalChange::put("a", vec![1]),
            FinalChange::remove("phantom"),
        ],
    );
    assert_eq!(a.publish_atomic(&x, d()).unwrap().achieved, Ack::Flush);
    assert!(s.get("authored", "phantom").is_none());
}
#[test]
fn lost_reply_reconnect_and_restart_reconcile_exact_original() {
    let p = root("replay");
    let original;
    let version;
    {
        let s = Arc::new(Store::open(&p, config()).unwrap());
        let f = Arc::new(Mutex::new(1));
        let mut c = client(&s, f.clone(), membus::MAX_FRAME);
        original = request(
            &mut c,
            "original",
            vec![
                FinalChange::put("a", vec![7]),
                FinalChange::put("b", vec![8]),
            ],
        );
        original.persist_new(&p.join("caller-original")).unwrap();
        assert!(matches!(
            c.publish_atomic(&original, d()),
            Err(Error::Uncertain { .. })
        ));
        assert!(!c.is_usable());
        *f.lock().unwrap() = 0;
        let mut c = client(&s, f, membus::MAX_FRAME);
        version = c.publish_atomic(&original, d()).unwrap().version;
        let newer = request(&mut c, "newer", vec![FinalChange::put("a", vec![9])]);
        c.publish_atomic(&newer, d()).unwrap();
    }
    let s = Arc::new(Store::open(&p, config()).unwrap());
    let r = AtomicRequest::from_bytes(&std::fs::read(p.join("caller-original")).unwrap()).unwrap();
    assert_eq!(r.to_bytes(), original.to_bytes());
    let mut c = client(&s, Arc::new(Mutex::new(0)), membus::MAX_FRAME);
    assert_eq!(c.publish_atomic(&r, d()).unwrap().version, version);
    assert_eq!(s.get("authored", "a").unwrap().bytes, vec![9]);
}
#[test]
fn malformed_unknown_truncated_and_wrong_ack_replies_are_uncertain() {
    for damage in 2..=4 {
        let s = Arc::new(Store::open(&root("malformed"), config()).unwrap());
        let mut c = client(&s, Arc::new(Mutex::new(damage)), membus::MAX_FRAME);
        let r = request(&mut c, "original", vec![FinalChange::put("a", vec![7])]);
        let bytes = r.to_bytes();
        assert!(matches!(
            c.publish_atomic(&r, d()),
            Err(Error::Uncertain { .. })
        ));
        assert_eq!(r.to_bytes(), bytes);
        assert!(!c.is_usable());
        assert!(s.get("authored", "a").is_some());
    }
}
#[test]
fn bounded_capture_refuses_complete_population_without_truncation() {
    let s = Arc::new(Store::open(&root("bound"), config()).unwrap());
    let mut c = client(&s, Arc::new(Mutex::new(0)), membus::MAX_FRAME);
    let r = request(&mut c, "big", vec![FinalChange::put("a", vec![1; 2048])]);
    c.publish_atomic(&r, d()).unwrap();
    let mut small = client(&s, Arc::new(Mutex::new(0)), 1024);
    assert!(matches!(
        small.capture_atomic_namespace("authored", "bounded", d()),
        Err(Error::Atomic(PublicationError::Resource(4)))
    ));
    assert_eq!(
        c.capture_atomic_namespace("authored", "complete", d())
            .unwrap()
            .rows()
            .len(),
        1
    );
}
#[test]
fn restart_without_outcome_never_renews_original() {
    let p = root("absent");
    let r;
    {
        let s = Arc::new(Store::open(&p, config()).unwrap());
        let mut c = client(&s, Arc::new(Mutex::new(0)), membus::MAX_FRAME);
        r = request(&mut c, "unsent", vec![FinalChange::put("a", vec![1])]);
    }
    let s = Arc::new(Store::open(&p, config()).unwrap());
    let mut c = client(&s, Arc::new(Mutex::new(0)), membus::MAX_FRAME);
    assert!(matches!(
        c.publish_atomic(&r, d()),
        Err(Error::Uncertain { .. })
    ));
    assert!(s.get("authored", "a").is_none());
}
#[test]
fn atomic_wire_constants_are_additive() {
    assert_eq!((membus::BATCH, membus::REMOVE, membus::HEALTH2), (7, 6, 27));
    assert_eq!(
        (
            membus::ATOMIC_CAPTURE,
            membus::ATOMIC_PUBLISH,
            membus::ATOMIC_POPULATION,
            membus::SNAPSHOT_REQUIRE_COMPLETE
        ),
        (28, 29, 30, 31)
    );
}

#[test]
fn finite_hint_offer_receives_no_affirmative_contract() {
    let s = Store::open(&root("wire-profile"), config()).unwrap();
    let hello = |name: &[u8]| {
        let mut p = (membus::MAX_FRAME as u32).to_le_bytes().to_vec();
        p.push(0);
        membus::lp(&mut p, name);
        s.dispatch(&membus::frame(1, 1, &p)[4..])
    };
    let legacy_subscription = hello(b"exact;subscribe=1;scans=1");
    let finite_offer = hello(b"exact;subscribe=1;subscription-contract=2;scans=1");
    assert_eq!(finite_offer, legacy_subscription);
    assert!(!finite_offer.ends_with(b"MSH2\x01\x00\x01\x00"));

    let mut prefix = d().unix_ms.to_le_bytes().to_vec();
    membus::lp(&mut prefix, b"read");
    membus::lp(&mut prefix, b"authored");
    let capture = s.dispatch(&membus::frame(28, 2, &prefix)[4..]);
    assert_eq!(&capture[17..28], b"\0\0MAW1\0MAV1");
    let mut finite_receipt = prefix;
    membus::lp(&mut finite_receipt, &[1; 64]);
    let renew = s.dispatch(&membus::frame(29, 3, &finite_receipt)[4..]);
    assert_eq!(&renew[17..], b"\0\0MAW1\x05");
    let cancel = s.dispatch(&membus::frame(30, 4, &finite_receipt)[4..]);
    assert_eq!(&cancel[17..], &3u16.to_le_bytes());
}

#[test]
fn legacy_mar1_wire_limit_preserves_native_funded_population() {
    let fixture = root("mar1-wire-limit");
    let clock = membus::ManualClock::new(membus::now_ms());
    clock.advance(1);
    let s = Store::open(
        &fixture,
        Config {
            clock: Some(clock),
            ..config()
        },
    )
    .unwrap();
    let limits = PopulationLimits {
        max_keys: 4097,
        max_capture_bytes: 48 * 1024 * 1024,
        max_changes: 4096,
        max_original_bytes: (membus::MAX_FRAME / 3) as u32,
    };
    let seed = s
        .capture_namespace_bounded("authored", limits)
        .unwrap()
        .prepare(
            "seed",
            AtomicAck::Flush,
            (0..4096)
                .map(|n| FinalChange::put(&format!("key-{n:04}"), vec![1]))
                .collect(),
        )
        .unwrap();
    s.publish_atomic(&seed, d().unix_ms).unwrap();
    let first = s
        .capture_namespace_funded("authored", 2 * 1024 * 1024)
        .unwrap();
    let first = first
        .prepare(
            "wire-4096",
            AtomicAck::Flush,
            vec![FinalChange::put("key-4096", vec![1])],
        )
        .unwrap();
    let make_frame = |r: &AtomicRequest| {
        let mut p = d().unix_ms.to_le_bytes().to_vec();
        membus::lp(&mut p, r.operation_id().as_bytes());
        membus::lp(&mut p, r.namespace().as_bytes());
        membus::lp(&mut p, &r.to_bytes());
        membus::frame(29, 1, &p)
    };
    let wire = make_frame(&first);
    let reply = s.dispatch(&wire[4..]);
    assert_eq!(&reply[17..24], b"\0\0MAW1\0");
    assert_eq!(u64::from_le_bytes(reply[24..32].try_into().unwrap()), 4097);
    assert_eq!(reply[32..], [2]);
    std::fs::write(fixture.join("mar1-4096.bin"), first.to_bytes()).unwrap();
    std::fs::write(fixture.join("request-4096.bin"), wire).unwrap();
    std::fs::write(fixture.join("reply-4096.bin"), reply).unwrap();

    let view = s
        .capture_namespace_funded("authored", 2 * 1024 * 1024)
        .unwrap();
    assert_eq!(view.rows().len(), 4097);
    let original = view
        .prepare(
            "native-4097",
            AtomicAck::Flush,
            vec![FinalChange::put("marker", vec![2])],
        )
        .unwrap();
    let raw = original.to_bytes();
    assert_eq!(
        membus::atomic::BorrowedOriginal::from_bytes(&raw)
            .unwrap()
            .population()
            .count(),
        4097
    );
    assert_eq!(AtomicRequest::from_bytes(&raw).unwrap().to_bytes(), raw);
    let wire = make_frame(&original);
    let before = s.io_metrics();
    let reply = s.dispatch(&wire[4..]);
    assert_eq!(&reply[17..], b"\0\0MAW1\x05");
    assert_eq!(s.io_metrics(), before);
    assert!(s.get("authored", "marker").is_none());
    let outcome = s.publish_atomic(&original, d().unix_ms).unwrap();
    assert_eq!((outcome.version, outcome.ack), (4098, 2));
    assert_eq!(s.io_metrics().0 - before.0, (raw.len() + 40) as u64);
    assert_eq!(s.io_metrics().2 - before.2, 1);
    std::fs::write(fixture.join("mar1-4097.bin"), raw).unwrap();
    std::fs::write(fixture.join("request-4097.bin"), wire).unwrap();
    std::fs::write(fixture.join("reply-4097.bin"), reply).unwrap();
}

#[test]
fn legacy_capture_funds_small_reply_and_obeys_shared_snapshot_limits() {
    let request = |ns: &[u8]| {
        let mut p = d().unix_ms.to_le_bytes().to_vec();
        membus::lp(&mut p, b"capture");
        membus::lp(&mut p, ns);
        membus::frame(28, 2, &p)
    };
    let wire = request(b"empty");
    let fixture = root("legacy-capture-budget");
    std::fs::write(fixture.join("request-empty.bin"), &wire).unwrap();
    for (label, bytes, success) in [
        ("incumbent", 1024 * 1024, true),
        ("exact", 1086, true),
        ("short", 1085, false),
    ] {
        let s = Store::open(
            &fixture.join(label),
            Config {
                snapshot_bytes: bytes,
                snapshot_max: 1,
                ..config()
            },
        )
        .unwrap();
        let before = s.health2().memory_bytes;
        let reply = s.dispatch(&wire[4..]);
        if success {
            assert_eq!(reply.len(), 57);
            assert_eq!(&reply[17..28], b"\0\0MAW1\0MAV1");
            let mut c = membus::Cursor::new(&reply[28..]);
            assert_eq!(c.text().unwrap(), "empty");
            assert_eq!(c.u64().unwrap(), s.epoch);
            assert_ne!(c.u64().unwrap(), 0);
            assert_eq!(c.u32().unwrap(), 0);
            c.end().unwrap();
            if label == "incumbent" {
                let held = s.capture_namespace_funded("other", 1029).unwrap();
                assert_eq!(&s.dispatch(&wire[4..])[17..], b"\0\0MAW1\x04\x07\0");
                drop(held);
                assert_eq!(s.dispatch(&wire[4..]).len(), 57);
            }
        } else {
            assert_eq!(&reply[17..], b"\0\0MAW1\x04\x07\0");
        }
        assert_eq!(s.health2().memory_bytes, before);
        std::fs::write(fixture.join(format!("{label}-reply.bin")), reply).unwrap();
    }
}

#[test]
fn wire_original_admission_keeps_retained_result_and_refuses_unfunded_copy() {
    let s = Arc::new(Store::open(&root("wire-funded"), config()).unwrap());
    let mut c = client(&s, Arc::new(Mutex::new(0)), membus::MAX_FRAME);
    let first = request(&mut c, "known", vec![FinalChange::put("one", vec![1])]);
    let receipt = c.publish_atomic(&first, d()).unwrap();
    let next = request(&mut c, "next", vec![FinalChange::put("two", vec![2])]);
    let health = s.health2();
    let pressure = s
        .read_cohort(health.memory_budget - health.memory_bytes)
        .unwrap();
    assert_eq!(
        c.publish_atomic(&first, d()).unwrap().version,
        receipt.version
    );
    assert!(matches!(
        c.publish_atomic(&next, d()),
        Err(Error::Atomic(PublicationError::Resource(7)))
    ));
    assert!(s.get("authored", "two").is_none());
    drop(pressure);
    assert_eq!(s.read_cohort_bytes(), 0);
    c.publish_atomic(&next, d()).unwrap();
    assert_eq!(s.read_cohort_bytes(), 0);
}

#[test]
fn malformed_capture_and_unsupported_policy_refuse_without_partial_view() {
    let s = Arc::new(Store::open(&root("capture-refuse"), config()).unwrap());
    let mut c = client(&s, Arc::new(Mutex::new(5)), membus::MAX_FRAME);
    assert!(matches!(
        c.capture_atomic_namespace("authored", "bad", d()),
        Err(Error::Protocol(_))
    ));
    assert!(!c.is_usable());
    let outcome = s.set_policy_with_id(
        "authored",
        "",
        Some(membus::Class::LATEST),
        "policy",
        membus::now_ms() + 30000,
    );
    assert_eq!(outcome.code, 0);
    let mut c = client(&s, Arc::new(Mutex::new(0)), membus::MAX_FRAME);
    assert!(matches!(
        c.capture_atomic_namespace("authored", "unsupported", d()),
        Err(Error::Atomic(PublicationError::Unsupported))
    ));
}
#[test]
fn whole_request_quota_refusal_has_no_prefix_and_exact_os_ack() {
    let mut cfg = config();
    cfg.quota = 1;
    let s = Arc::new(Store::open(&root("quota"), cfg).unwrap());
    let mut c = client(&s, Arc::new(Mutex::new(0)), membus::MAX_FRAME);
    let view = c
        .capture_atomic_namespace("authored", "capture", d())
        .unwrap();
    let r = view
        .prepare(
            "too-many",
            AtomicAck::Flush,
            vec![
                FinalChange::put("a", vec![1]),
                FinalChange::put("b", vec![2]),
            ],
        )
        .unwrap();
    assert!(matches!(
        c.publish_atomic(&r, d()),
        Err(Error::Atomic(PublicationError::Resource(8)))
    ));
    assert!(s.get("authored", "a").is_none() && s.get("authored", "b").is_none());
    let r = view
        .prepare("os", AtomicAck::Os, vec![FinalChange::put("a", vec![1])])
        .unwrap();
    assert_eq!(c.publish_atomic(&r, d()).unwrap().achieved, Ack::Os);
    let expired = Deadline {
        unix_ms: 1,
        local: d().local,
    };
    assert_eq!(c.publish_atomic(&r, expired).unwrap().achieved, Ack::Os);
}
#[test]
fn local_frame_admission_and_identity_mismatch_preserve_original() {
    let s = Arc::new(Store::open(&root("identity"), config()).unwrap());
    let mut c = client(&s, Arc::new(Mutex::new(0)), membus::MAX_FRAME);
    let view = c
        .capture_atomic_namespace("authored", "capture", d())
        .unwrap();
    let r = view
        .prepare(
            "original",
            AtomicAck::Flush,
            vec![FinalChange::put("a", vec![1; 2048])],
        )
        .unwrap();
    let mut small = client(&s, Arc::new(Mutex::new(0)), 1024);
    assert!(matches!(
        small.publish_atomic(&r, d()),
        Err(Error::Invalid(_))
    ));
    assert!(s.get("authored", "a").is_none());
    c.publish_atomic(&r, d()).unwrap();
    let mismatch = view
        .prepare(
            "original",
            AtomicAck::Flush,
            vec![FinalChange::put("b", vec![2])],
        )
        .unwrap();
    assert!(matches!(
        c.publish_atomic(&mismatch, d()),
        Err(Error::Atomic(PublicationError::IdentityMismatch))
    ));
    assert!(s.get("authored", "b").is_none());
}

#[test]
fn unpaged_capture_obeys_the_connection_value_limit() {
    let s = Arc::new(Store::open(&root("capture-value-limit"), config()).unwrap());
    let damage = Arc::new(Mutex::new(0));
    let mut writer = client(&s, damage.clone(), membus::MAX_FRAME);
    let original = request(
        &mut writer,
        "original",
        vec![FinalChange::put("row", b"abc".to_vec())],
    );
    let receipt = writer.publish_atomic(&original, d()).unwrap();

    let mut too_small = client_with_value_limit(&s, damage.clone(), membus::MAX_FRAME, 2);
    assert_eq!(too_small.negotiated().max_value, 2);
    assert!(matches!(
        too_small.capture_atomic_namespace("authored", "refuse", d()),
        Err(Error::Protocol(_))
    ));
    assert!(!too_small.is_usable());

    let mut exact = client_with_value_limit(&s, damage, membus::MAX_FRAME, 3);
    assert_eq!(exact.negotiated().max_value, 3);
    let view = exact
        .capture_atomic_namespace("authored", "accept", d())
        .unwrap();
    let [(key, value)] = view.rows() else {
        panic!("expected the entire one-row view");
    };
    assert_eq!(key, "row");
    assert_eq!(value.bytes, b"abc");
    assert_eq!(value.version, receipt.version);
    assert_eq!(value.ack, 2);
    assert_eq!(receipt.achieved, Ack::Flush);
    assert!(exact.is_usable());
}

#[test]
fn unknown_incomplete_overlimit_and_misbound_capture_replies_return_no_view() {
    for fault in 6..=11 {
        let s = Arc::new(Store::open(&root("bad-capture"), config()).unwrap());
        let mut c = client(&s, Arc::new(Mutex::new(fault)), membus::MAX_FRAME);
        assert!(matches!(
            c.capture_atomic_namespace("authored", "capture", d()),
            Err(Error::Protocol(_))
        ));
        assert!(!c.is_usable());
    }
}
