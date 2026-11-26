//! Public admission and independently decoded retained-policy obligations.
use super::*;
use chunks::{CatRecord, Rec};
#[path = "../../../tools/catalogue_receiver.rs"]
mod receiver;

struct Fixture(PathBuf);
impl Fixture {
    fn new(label: &str) -> Self {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        Self(PathBuf::from(std::env::var_os("MEMBUS_TEST_OUTPUT")
            .unwrap_or_else(|| ".test-output".into())).join(format!(
            "policy-{label}-{}-{}-{}", std::process::id(), now_ms(),
            SEQ.fetch_add(1, Ordering::SeqCst))))
    }
    fn catalogue(&self) -> PathBuf { self.0.join("chunks/catalogue.log") }
}
impl Drop for Fixture {
    fn drop(&mut self) { let _ = fs::remove_dir_all(&self.0); }
}
fn config() -> Config {
    Config { flush_ms: 0, tick_ms: 3_600_000, retire_ms: 3_600_000,
        catalogue_rewrite_bytes: 0, ..Config::default() }
}
fn put(s: &Store, key: &str) -> Outcome {
    s.mutate(Mutation { ns: "authored".into(), key: key.into(), op_id: key.into(),
        deadline: now_ms() + 30_000, ack: 2, expected: None,
        bytes: b"retained value".to_vec(), remove: false })
}
fn policy(s: &Store, prefix: &str) -> Option<Value> {
    s.get(POLICY_NS, &chunks::policy_key("authored", prefix).unwrap())
}

#[test]
fn public_invalid_classes_preserve_publication_and_reopen() {
    let f = Fixture::new("invalid");
    let cfg = Config { retry_ms: 0, ..config() };
    let s = Store::open(&f.0, cfg.clone()).unwrap();
    let preserved = put(&s, "protected");
    assert_eq!((preserved.code, preserved.ack), (0, 2));
    let before = fs::read(f.catalogue()).unwrap();
    for class in [Class { kind: 3, retention_ms: 99 }, Class::expire(0)] {
        assert_eq!(s.set_policy("authored", "invalid/", Some(class), now_ms()+30_000).code, 3);
        assert_eq!(fs::read(f.catalogue()).unwrap(), before);
        assert!(policy(&s, "invalid/").is_none());
        assert!(s.policies().is_empty());
    }
    let later = put(&s, "invalid/later");
    assert_eq!((later.code, later.ack), (0, 2));
    assert!(receiver::read(&f.catalogue()).is_ok());
    drop(s);
    let reopened = Store::open(&f.0, cfg).unwrap();
    assert_eq!(reopened.get("authored", "protected").unwrap().version, preserved.version);
    assert_eq!(reopened.get("authored", "invalid/later").unwrap().version, later.version);
}

#[test]
fn public_builtins_normalize_before_retry_identity_and_durable_routing() {
    for (canonical, text) in [(Class::FOREVER, b"forever".as_slice()),
                              (Class::LATEST, b"latest".as_slice())] {
        let f = Fixture::new("builtin");
        let cfg = config();
        let s = Store::open(&f.0, cfg.clone()).unwrap();
        let ack = s.set_policy_with_id("authored", "builtin/",
            Some(Class { kind: canonical.kind, retention_ms: 99 }), "policy", now_ms()+30_000);
        assert_eq!((ack.code, ack.ack), (0, 2));
        assert_eq!(s.set_policy_with_id("authored", "builtin/", Some(canonical), "policy", 0), ack);
        let saved = policy(&s, "builtin/").unwrap();
        assert_eq!((saved.bytes.as_slice(), saved.version, saved.ack), (text, ack.version, 2));
        assert_eq!(s.policies(), vec![("authored".into(), "builtin/".into(), canonical)]);
        let written = put(&s, "builtin/value");
        assert_eq!((written.code, written.ack), (0, 2));
        let (placed, catalogue) = s.inspect_durable_placement(
            "authored", "builtin/value", written.version, b"retained value",
        ).unwrap();
        assert_eq!(placed.class, canonical);
        assert_eq!(placed.stripe, stripe("authored", "builtin/value"));
        if canonical == Class::FOREVER {
            assert!(placed.offset > 0);
            assert!(receiver::decode(&catalogue).unwrap().iter().any(|record| matches!(
                &record.rec, Rec::Open { seq, policy, .. }
                    if *seq == placed.sequence && policy == "(built in: policies)"
            )));
        }
        drop(s);
        let reopened = Store::open(&f.0, cfg).unwrap();
        assert_eq!(reopened.policies(), vec![("authored".into(), "builtin/".into(), canonical)]);
        let retained = reopened.get("authored", "builtin/value").unwrap();
        assert_eq!((retained.version, retained.ack, retained.bytes.as_slice()),
            (written.version, 2, b"retained value".as_slice()));
        let (closed, _) = reopened.inspect_durable_placement(
            "authored", "builtin/value", written.version, b"retained value",
        ).unwrap();
        assert_eq!((closed.sequence, closed.stripe, closed.offset, closed.frame_bytes),
            (placed.sequence, placed.stripe, placed.offset, placed.frame_bytes));
        assert_eq!((closed.class, closed.state), (canonical, chunks::ChunkState::Closed));
    }
}

// Expectations come from the received acknowledged prefix, never the producer's live table.
fn preserves_survivors(prefix: &[CatRecord], replacement: &[CatRecord]) -> bool {
    let mut survivors = std::collections::BTreeSet::new();
    for r in prefix {
        match r.rec {
            Rec::Open { seq, .. } => { survivors.insert(seq); }
            Rec::Retiring { seq, .. } | Rec::Retired { seq } => { survivors.remove(&seq); }
            _ => {}
        }
    }
    !replacement.iter().any(|r| matches!(r.rec,
        Rec::Retiring { seq, .. } | Rec::RetiringPart { seq, .. } | Rec::Retired { seq }
            if survivors.contains(&seq)))
}

#[test]
fn recovery_preserves_acknowledged_policy_and_subsequent_route() {
    let f = Fixture::new("recovery");
    let cfg = config();
    let s = Store::open(&f.0, cfg.clone()).unwrap();
    let ack = s.set_policy("authored", "retained/", Some(Class::LATEST), now_ms()+30_000);
    assert_eq!((ack.code, ack.ack), (0, 2));
    let value = put(&s, "retained/before");
    assert_eq!((value.code, value.ack), (0, 2));
    let before = receiver::read(&f.catalogue()).unwrap();
    {
        let mut cs = s.chunks.lock().unwrap();
        let cat = cs.cat.as_mut().unwrap();
        cat.fail_sync_once = true;
        cat.fail_recovery_once = true;
        assert!(cs.log(now_ms(), Rec::Resume).is_err());
    }
    let uncertain = fs::read(f.catalogue()).unwrap();
    let marker = fs::read(f.0.join("chunks/catalogue.log.append-pending")).unwrap();
    let CatalogueMaintenance::Recovered { evidence } = s.maintain_catalogue().unwrap() else {
        panic!("resident recovery required")
    };
    assert_eq!(fs::read(evidence.join("catalogue.log")).unwrap(), uncertain);
    assert_eq!(fs::read(evidence.join("append-pending")).unwrap(), marker);
    let replacement = receiver::read(&f.catalogue()).unwrap();
    assert!(preserves_survivors(&before, &replacement));
    for expected in &before {
        if let Rec::Open { seq, stripe, class, policy, .. } = &expected.rec {
            assert!(replacement.iter().any(|r| matches!(&r.rec,
                Rec::Open { seq: s, stripe: t, class: c, policy: p, .. }
                    if (s, t, c, p) == (seq, stripe, class, policy))));
        }
    }
    // An extra policy retirement must fail the same discriminator; a retirement already
    // acknowledged in the prefix is legitimate.
    let seq = before.iter().find_map(|r| match &r.rec {
        Rec::Open { seq, policy, .. } if policy == "(built in: policies)" => Some(*seq),
        _ => None,
    }).unwrap();
    let mut destructive = replacement.clone();
    destructive.push(CatRecord { at_ms: now_ms(), rec: Rec::Retiring {
        seq, next: 1, horizons: vec![], expired: vec![] } });
    assert!(!preserves_survivors(&before, &destructive));
    let mut retired_prefix = before.clone();
    retired_prefix.push(destructive.last().unwrap().clone());
    assert!(preserves_survivors(&retired_prefix, &destructive));
    drop(s);
    let reopened = Store::open(&f.0, cfg).unwrap();
    let retained = policy(&reopened, "retained/").unwrap();
    assert_eq!((retained.bytes, retained.version, retained.ack), (b"latest".to_vec(), ack.version, 2));
    assert_eq!(reopened.policies(), vec![("authored".into(), "retained/".into(), Class::LATEST)]);
    assert_eq!(reopened.get("authored", "retained/before").unwrap().version, value.version);
    let previous = receiver::read(&f.catalogue()).unwrap().iter().filter_map(|r| match r.rec {
        Rec::Open { seq, .. } => Some(seq), _ => None,
    }).max().unwrap();
    let routed = put(&reopened, "retained/after");
    assert_eq!((routed.code, routed.ack), (0, 2));
    let (placed, _) = reopened.inspect_durable_placement(
        "authored", "retained/after", routed.version, b"retained value",
    ).unwrap();
    assert_eq!(placed.class, Class::LATEST);
    assert!(placed.sequence > previous);
}
