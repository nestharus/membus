//! Authored bounded profiles; private roots are explicit and retained.
use membus::{
    atomic::{AtomicAck, AtomicRequest, FinalChange, PublicationError as P},
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
fn root(label: &str) -> PathBuf {
    let b = PathBuf::from(
        std::env::var_os("MEMBUS_PROVIDER_TEST_ROOT").expect("explicit retained root"),
    );
    assert!(b.is_absolute() && b.is_dir());
    let p = b.join(format!(
        "population-{label}-{}-{}",
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
        cache_bytes: 0,
        signal_bytes: 0,
        tick_ms: 60000,
        retire_ms: 60000,
        flush_ms: 0,
        ..Default::default()
    }
}
fn limits() -> PopulationLimits {
    PopulationLimits {
        max_keys: 60000,
        max_capture_bytes: 96 * 1024 * 1024,
        max_changes: 10000,
        max_original_bytes: (membus::MAX_FRAME / 3) as u32,
    }
}
fn d() -> Deadline {
    Deadline {
        unix_ms: membus::now_ms() + 120000,
        local: Instant::now() + Duration::from_secs(120),
    }
}
fn publish(s: &Store, id: &str, changes: Vec<FinalChange>) -> membus::Outcome {
    let r = s
        .capture_namespace_bounded("authored", limits())
        .unwrap()
        .prepare(id, AtomicAck::Flush, changes)
        .unwrap();
    s.publish_atomic(&r, d().unix_ms).unwrap()
}
fn metrics(s: &Store) -> [u64; 3] {
    let mut p = d().unix_ms.to_le_bytes().to_vec();
    membus::lp(&mut p, b"metrics");
    membus::lp(&mut p, b"authored");
    let f = membus::frame(membus::IO_METRICS, 1, &p);
    let b = s.dispatch_for_limit(&f[4..], false, membus::MAX_FRAME);
    assert_eq!(b.len(), 43);
    std::array::from_fn(|n| u64::from_le_bytes(b[19 + 8 * n..27 + 8 * n].try_into().unwrap()))
}
// Independently authored expected bytes, never derived from the request,
// captured values, original custody, or the projection under examination.
fn assert_authored_burst(view: &membus::atomic_population::PopulationView) {
    let mut expected = std::collections::BTreeMap::new();
    for n in 1u64..=3065 {
        let length = match n {
            1533 => 59499,
            n if n % 2 == 1 => 1039,
            _ => 1311,
        };
        let text = format!("row-{n}:");
        let mut bytes = vec![b'x'; length];
        bytes[..text.len()].copy_from_slice(text.as_bytes());
        expected.insert(format!("row/{n:016}"), bytes);
        expected.insert(format!("id/{n:016}"), n.to_le_bytes().to_vec());
        expected.insert(format!("source/{n:016}"), n.to_le_bytes().to_vec());
    }
    assert_eq!(view.rows().len(), expected.len());
    for ((actual_key, actual), (key, bytes)) in view.rows().iter().zip(&expected) {
        assert_eq!(actual_key, key);
        assert_eq!(&actual.bytes, bytes, "complete captured bytes for {key}");
        assert!(actual.version > 0);
        assert_eq!(actual.ack, 2);
        assert!(!actual.tombstone);
    }
}
#[test]
fn recorded_generator_burst_and_metadata_publish_whole_with_retained_original() {
    let p = root("burst");
    let s = Store::open(&p, config()).unwrap();
    let mut changes = Vec::new();
    let mut values = 0;
    for n in 1usize..=3065 {
        let size = if n == 1533 {
            59499
        } else if n % 2 == 1 {
            1039
        } else {
            1311
        };
        let prefix = format!("row-{n}:");
        let mut v = prefix.into_bytes();
        v.resize(size, b'x');
        values += v.len();
        changes.push(FinalChange::put(&format!("row/{n:016}"), v));
        changes.push(FinalChange::put(
            &format!("id/{n:016}"),
            n.to_le_bytes().to_vec(),
        ));
        changes.push(FinalChange::put(
            &format!("source/{n:016}"),
            n.to_le_bytes().to_vec(),
        ));
    }
    assert_eq!(changes.len(), 9195);
    let legacy = s.capture_namespace("authored").unwrap();
    assert!(matches!(
        legacy.prepare("old", AtomicAck::Flush, changes.clone()),
        Err(P::Invalid)
    ));
    let view = s.capture_namespace_bounded("authored", limits()).unwrap();
    let r = view.prepare("burst", AtomicAck::Flush, changes).unwrap();
    let raw = r.to_bytes();
    assert!(raw.len() <= membus::MAX_FRAME / 3);
    r.persist_new(&p.join("original")).unwrap();
    let before = metrics(&s);
    let start = Instant::now();
    let o = s.publish_atomic(&r, d().unix_ms).unwrap();
    let after = metrics(&s);
    println!("profile rows=3065 changes=9195 value_bytes={values} original_bytes={} publication_us={} logical_append_bytes={} append_calls={} covering_flush_calls={}",raw.len(),start.elapsed().as_micros(),after[0]-before[0],after[1]-before[1],after[2]-before[2]);
    assert_eq!(o.ack, 2);
    assert_eq!(after[0] - before[0], (raw.len() + 40) as u64);
    assert_eq!(after[2] - before[2], 1);
    let full = s.capture_namespace_bounded("authored", limits()).unwrap();
    assert_eq!(full.rows().len(), 9195);
    for n in 1usize..=3065 {
        assert!(full.rows().iter().any(|(k, v)| k == &format!("row/{n:016}")
            && v.bytes.len()
                == if n == 1533 {
                    59499
                } else if n % 2 == 1 {
                    1039
                } else {
                    1311
                }));
    }
    assert_authored_burst(&full);
    drop(full);
    drop(view);
    drop(legacy);
    drop(s);
    let s = Store::open(&p, config()).unwrap();
    let exact = AtomicRequest::from_bytes(&std::fs::read(p.join("original")).unwrap()).unwrap();
    assert_eq!(exact.to_bytes(), raw);
    assert_eq!(s.publish_atomic(&exact, 0).unwrap(), o);
    let reopened = s.capture_namespace_bounded("authored", limits()).unwrap();
    assert_authored_burst(&reopened);
}
#[test]
fn complete_population_exceeds_legacy_keys_and_single_reply_without_partial_view() {
    let s = Arc::new(Store::open(&root("population"), config()).unwrap());
    for batch in 0..5 {
        publish(
            &s,
            &format!("seed-{batch}"),
            (0..1000)
                .map(|n| FinalChange::put(&format!("k{:05}", batch * 1000 + n), vec![3; 4096]))
                .collect(),
        );
    }
    assert!(matches!(
        s.capture_namespace("authored"),
        Err(P::Resource(4))
    ));
    let damage = Arc::new(Mutex::new(0));
    let mut c = client(s.clone(), damage.clone(), 32768);
    let view = c
        .capture_atomic_namespace_bounded("authored", "complete", limits(), d())
        .unwrap();
    assert_eq!(view.rows().len(), 5000);
    assert_eq!(
        view.rows()
            .iter()
            .map(|(_, v)| v.bytes.len())
            .sum::<usize>(),
        20480000
    );
    let original = view
        .prepare(
            "guard",
            AtomicAck::Flush,
            vec![FinalChange::put("z", vec![9])],
        )
        .unwrap();
    publish(&s, "phantom", vec![FinalChange::put("new", vec![1])]);
    assert!(matches!(
        s.publish_atomic(&original, d().unix_ms),
        Err(P::Conflict)
    ));
    assert!(s.get("authored", "z").is_none());
    let mut c = client(s.clone(), damage.clone(), 32768);
    *damage.lock().unwrap() = 1;
    assert!(c
        .capture_atomic_namespace_bounded("authored", "truncated", limits(), d())
        .is_err());
}
#[test]
fn existing_large_namespace_has_compact_guard_and_exact_count_byte_refusals() {
    let s = Store::open(&root("large"), config()).unwrap();
    for batch in 0..6 {
        let first = batch * 9000;
        let end = (first + 9000).min(50536);
        if first >= end {
            break;
        }
        publish(
            &s,
            &format!("populate-{batch}"),
            (first..end)
                .map(|n| FinalChange::put(&format!("metadata/{n:08}"), vec![1]))
                .collect(),
        );
    }
    let start = Instant::now();
    let v = s.capture_namespace_bounded("authored", limits()).unwrap();
    println!(
        "profile existing_keys={} charge_bytes={} capture_us={}",
        v.rows().len(),
        v.charged_bytes(),
        start.elapsed().as_micros()
    );
    assert_eq!(v.rows().len(), 50536);
    let r = v
        .prepare(
            "compact",
            AtomicAck::Os,
            vec![FinalChange::put("one", vec![1])],
        )
        .unwrap();
    assert!(r.to_bytes().len() < 200);
    assert!(s.publish_atomic(&r, d().unix_ms).is_ok());
    let mut l = limits();
    l.max_keys = 50536;
    assert!(matches!(
        s.capture_namespace_bounded("authored", l),
        Err(P::Resource(7))
    ));
    l = limits();
    l.max_capture_bytes = 1;
    assert!(matches!(
        s.capture_namespace_bounded("authored", l),
        Err(P::Resource(7))
    ));
}
#[test]
fn original_bounds_and_whole_admission_refusals_do_not_change_neighbors() {
    let mut cfg = config();
    cfg.quota = 2;
    let s = Store::open(&root("refusal"), cfg).unwrap();
    publish(&s, "one", vec![FinalChange::put("one", vec![1])]);
    let v = s.capture_namespace_bounded("authored", limits()).unwrap();
    let r = v
        .prepare(
            "too-many",
            AtomicAck::Flush,
            vec![
                FinalChange::put("two", vec![2]),
                FinalChange::put("three", vec![3]),
            ],
        )
        .unwrap();
    assert!(matches!(
        s.publish_atomic(&r, d().unix_ms),
        Err(P::Resource(8))
    ));
    assert!(s.get("authored", "two").is_none());
    assert_eq!(s.get("authored", "one").unwrap().bytes, vec![1]);
    let mut l = limits();
    l.max_changes = 1;
    let v = s.capture_namespace_bounded("authored", l).unwrap();
    assert!(matches!(
        v.prepare(
            "count",
            AtomicAck::Os,
            vec![FinalChange::remove("one"), FinalChange::put("two", vec![2])]
        ),
        Err(P::Invalid)
    ));
    let mut l = limits();
    l.max_original_bytes = 100;
    let v = s.capture_namespace_bounded("authored", l).unwrap();
    assert!(matches!(
        v.prepare(
            "bytes",
            AtomicAck::Os,
            vec![FinalChange::put("two", vec![2; 100])]
        ),
        Err(P::Resource(4))
    ));
    let mut raw = r.to_bytes();
    raw.push(0);
    assert!(AtomicRequest::from_bytes(&raw).is_err());
}
#[test]
fn exact_capture_charge_boundary_and_guard_revision_removal_neighbors() {
    let s = Store::open(&root("guards"), config()).unwrap();
    publish(&s, "seed", vec![FinalChange::put("one", vec![1])]);
    let v = s.capture_namespace_bounded("authored", limits()).unwrap();
    let charge = v.charged_bytes();
    drop(v);
    let mut l = limits();
    l.max_capture_bytes = charge - 1;
    assert!(matches!(
        s.capture_namespace_bounded("authored", l),
        Err(P::Resource(7))
    ));
    l.max_capture_bytes = charge;
    let v = s.capture_namespace_bounded("authored", l).unwrap();
    assert_eq!(v.charged_bytes(), charge);
    let stale = v
        .prepare(
            "stale",
            AtomicAck::Flush,
            vec![FinalChange::put("two", vec![2])],
        )
        .unwrap();
    publish(&s, "revise", vec![FinalChange::put("one", vec![9])]);
    assert!(matches!(
        s.publish_atomic(&stale, d().unix_ms),
        Err(P::Conflict)
    ));
    assert!(s.get("authored", "two").is_none());
    drop(v);
    let v = s.capture_namespace_bounded("authored", limits()).unwrap();
    let stale = v
        .prepare(
            "removed",
            AtomicAck::Flush,
            vec![FinalChange::put("two", vec![2])],
        )
        .unwrap();
    publish(&s, "remove", vec![FinalChange::remove("one")]);
    assert!(matches!(
        s.publish_atomic(&stale, d().unix_ms),
        Err(P::Conflict)
    ));
    let v = s.capture_namespace_bounded("authored", limits()).unwrap();
    let r = v
        .prepare(
            "independent",
            AtomicAck::Os,
            vec![FinalChange::put("two", vec![2])],
        )
        .unwrap();
    assert_eq!(
        s.mutate(membus::Mutation {
            ns: "other".into(),
            key: "key".into(),
            op_id: "other".into(),
            deadline: d().unix_ms,
            ack: 1,
            expected: None,
            bytes: vec![8],
            remove: false
        })
        .code,
        0
    );
    assert!(s.publish_atomic(&r, d().unix_ms).is_ok());
}

#[test]
fn wide_envelope_uses_existing_read_cohort_account() {
    let s = Store::open(&root("wide-cohort"), config()).unwrap();
    let view = s.capture_namespace_bounded("authored", limits()).unwrap();
    let original = view
        .prepare(
            "wide",
            AtomicAck::Flush,
            (0..129)
                .map(|n| FinalChange::put(&format!("key-{n:03}"), vec![n as u8]))
                .collect(),
        )
        .unwrap();
    let encoded = original.to_bytes();
    assert!(membus::atomic::BorrowedOriginal::from_bytes(&encoded).is_err());
    s.publish_atomic(&original, d().unix_ms).unwrap();
    let base = 512 + 4 * (encoded.len() + 40) + 128 * 256 + 4096;
    let short = s.read_cohort(base).unwrap();
    assert!(
        matches!(short.try_get("authored", "key-128"), Err(e) if e.kind() == io::ErrorKind::WouldBlock)
    );
    drop(short);
    let exact = s.read_cohort(base + 256).unwrap();
    let value = exact.try_get("authored", "key-128").unwrap().unwrap();
    assert_eq!(value.bytes, [128]);
    drop(exact);
    assert_eq!(s.read_cohort_bytes(), base + 256);
    drop(value);
    assert_eq!(s.read_cohort_bytes(), 0);
}
#[test]
fn corrupt_cold_envelope_refuses_complete_view_and_records_storage_error() {
    use std::io::{Seek, SeekFrom};
    let p = root("corrupt-cold");
    let s = Store::open(&p, config()).unwrap();
    publish(
        &s,
        "seed",
        vec![
            FinalChange::put("one", vec![1]),
            FinalChange::put("two", vec![2]),
        ],
    );
    let log = std::fs::read_dir(p.join("chunks/forever"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|x| x == "log"))
        .unwrap();
    let bytes = std::fs::read(&log).unwrap();
    std::fs::write(p.join("authored-pristine-envelope.bin"), &bytes).unwrap();
    let mut f = std::fs::OpenOptions::new().write(true).open(&log).unwrap();
    f.seek(SeekFrom::Start(16)).unwrap();
    f.write_all(&[bytes[16] ^ 1]).unwrap();
    f.flush().unwrap();
    assert!(matches!(
        s.capture_namespace_bounded("authored", limits()),
        Err(P::Uncertain)
    ));
    assert!(s.storage_error());
}
struct Peer {
    s: Arc<Store>,
    pending: Vec<u8>,
    reply: Vec<u8>,
    damage: Arc<Mutex<u8>>,
    max: usize,
}
impl Read for Peer {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        let n = b.len().min(self.reply.len());
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
                if op == membus::SCAN_PREFIX && *self.damage.lock().unwrap() == 1 {
                    self.reply.clear();
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
fn client(s: Arc<Store>, damage: Arc<Mutex<u8>>, max: usize) -> Client<Peer> {
    let mut options = Options::new("population-qualified", max);
    options.scans = true;
    Client::handshake(
        Peer {
            s,
            pending: vec![],
            reply: vec![],
            damage,
            max,
        },
        options,
        d().local,
    )
    .unwrap()
}
