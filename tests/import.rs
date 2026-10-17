//! CR4 (docs/CHUNK-RETIREMENT.md section 10): the one-time legacy import (5.6). Test T11 of section 7 on synthetic
//! legacy logs; tests/import_production.py runs it on a copy of production state with the previous binary.
use membus::*;
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
const HOUR: u64 = 3_600_000;
const DAY: u64 = 24 * HOUR;
fn path(label: &str) -> PathBuf {
    let p = std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into());
    PathBuf::from(p).join(format!("{label}-{}-{}", std::process::id(), now_ms()))
}
/// One legacy record, as the stripe-log build wrote it.
struct Rec {
    ns: &'static str,
    key: String,
    op_id: String,
    at: u64,
    version: u64,
    ack: u8,
    remove: bool,
    expected: Option<u64>,
    bytes: Vec<u8>,
}
fn stripe_of(ns: &str, key: &str) -> usize {
    let mut b = ns.as_bytes().to_vec();
    b.push(0);
    b.extend(key.as_bytes());
    crc32(&b) as usize % 4
}
fn payload(r: &Rec) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend(r.at.to_le_bytes());
    b.extend(r.version.to_le_bytes());
    b.push(r.ack);
    b.push(r.remove as u8);
    lp(&mut b, r.ns.as_bytes());
    lp(&mut b, r.key.as_bytes());
    lp(&mut b, r.op_id.as_bytes());
    b.extend(r.expected.unwrap_or(u64::MAX).to_le_bytes());
    lp(&mut b, &r.bytes);
    b
}
/// Write legacy stripe logs, and return the model: each key's latest (version, tombstone, bytes).
fn write_legacy(dir: &Path, records: &[Rec]) -> HashMap<(String, String), (u64, bool, Vec<u8>)> {
    fs::create_dir_all(dir).unwrap();
    let mut files: Vec<Vec<u8>> = vec![Vec::new(); 4];
    let mut model = HashMap::new();
    for r in records {
        let p = payload(r);
        let f = &mut files[stripe_of(r.ns, &r.key)];
        f.extend(b"MBK1");
        f.extend((p.len() as u32).to_le_bytes());
        f.extend((!(p.len() as u32)).to_le_bytes());
        f.extend(crc32(&p).to_le_bytes());
        f.extend(&p);
        model.insert((r.ns.to_string(), r.key.clone()), (r.version, r.remove, r.bytes.clone()));
    }
    for (i, f) in files.iter().enumerate() {
        fs::write(dir.join(format!("stripe-{i}.log")), f).unwrap();
    }
    fs::write(dir.join("epoch"), 7u64.to_le_bytes()).unwrap();
    model
}
/// A production-shaped legacy state: lane 88's families, a ledger, a removed key; recent and older records.
fn legacy_state(dir: &Path) -> (HashMap<(String, String), (u64, bool, Vec<u8>)>, Rec) {legacy_state_with_probe_id(dir,None)}
fn legacy_state_with_probe_id(dir: &Path, probe_id: Option<&str>) -> (HashMap<(String, String), (u64, bool, Vec<u8>)>, Rec) {
    legacy_state_with_probe_ids(dir,probe_id,None)
}
fn legacy_state_with_probe_ids(dir: &Path, probe_id: Option<&str>, next_id: Option<&str>) -> (HashMap<(String, String), (u64, bool, Vec<u8>)>, Rec) {
    let now = now_ms();
    let mut v = 0;
    let mut rec = |ns: &'static str, key: &str, op: &str, age: u64, remove: bool, bytes: &[u8]| {
        v += 1;
        Rec {
            ns,
            key: key.into(),
            op_id: op.into(),
            at: now - age,
            version: v,
            ack: 1,
            remove,
            expected: None,
            bytes: bytes.to_vec(),
        }
    };
    let mut records = Vec::new();
    for i in 0..40 {
        records.push(rec("quota", &format!("event/{i}"), &format!("qe{i}"), 3 * DAY - i * HOUR, false, b"event"));
    }
    for i in 0..10 {
        records.push(rec("slots", &format!("event/{i}"), &format!("se{i}"), 2 * HOUR, false, b"event"));
    }
    records.push(rec("quota", "state/p1", "s1", 2 * DAY, false, b"old"));
    // A put made within 24 h before the import, then superseded: its retry must still be recognised.
    records.push(rec("quota", "state/p1", "s2", HOUR, false, b"mid"));
    records.push(rec("quota", "state/p1", "s3", HOUR / 2, false, b"new"));
    records.push(rec("quota", "open", "o1", HOUR, false, b"open"));
    records.push(rec("slots", "gpu", "g1", HOUR, false, b"free"));
    records.push(rec("ledger", "row/1", "l1", 5 * DAY, false, b"forever"));
    records.push(rec("ledger", "gone", "l2", 5 * DAY, false, b"x"));
    records.push(rec("ledger", "gone", "l3", HOUR, true, b""));
    if let Some(id)=probe_id {records[51].op_id=id.into();}
    if let Some(id)=next_id {records[53].op_id=id.into();}
    let model = write_legacy(dir, &records);
    let r = &records[51];
    let probe = Rec { ns: r.ns, key: r.key.clone(), op_id: r.op_id.clone(), bytes: r.bytes.clone(), ..*r };
    (model, probe)
}
fn policies(dir: &Path) {
    fs::write(
        dir.join("import-policies.txt"),
        "# lane 88, claude6-eco row 2029 items 5-6\nquota\tevent/\texpire:259200000\nslots\tevent/\texpire:259200000\nquota\tstate/\tlatest\nquota\topen\tlatest\nslots\t\tlatest\n",
    )
    .unwrap();
}
fn config(clock: &Arc<ManualClock>) -> Config {
    Config { clock: Some(clock.clone()), tick_ms: 3_600_000, retire_ms: 3_600_000, chunk_bytes: 1024, ..Config::default() }
}
fn reopen(p: &Path, c: Config) -> Store {
    #[cfg(not(windows))]
    let _ = fs::remove_file(p.join("owner.lock"));
    Store::open(p, c).unwrap()
}
fn shas(dir: &Path) -> BTreeMap<String, String> {
    (0..4)
        .map(|i| format!("stripe-{i}.log"))
        .filter(|n| dir.join(n).exists())
        .map(|n| {
            let sha = chunks::sha256_file(&dir.join(&n)).unwrap();
            (n, sha)
        })
        .collect()
}
fn check_model(s: &Store, model: &HashMap<(String, String), (u64, bool, Vec<u8>)>) {
    for ((ns, key), (version, tombstone, bytes)) in model {
        match s.get(ns, key) {
            Some(v) => {
                assert!(!tombstone, "{ns}/{key} came back");
                assert_eq!((v.version, &v.bytes), (*version, bytes), "{ns}/{key}");
            }
            None => assert!(tombstone, "{ns}/{key} missing"),
        }
    }
    let mut live: BTreeMap<String, usize> = BTreeMap::new();
    for ((ns, _), (_, tombstone, _)) in model {
        if !tombstone {
            *live.entry(ns.clone()).or_default() += 1;
        }
    }
    for (ns, n) in live {
        assert_eq!(s.slots(&ns).1, n, "{ns} live count");
    }
}

#[test]
fn t11_import_equal_and_retries_recognised() {
    let p = path("t11-import");
    let (model, probe) = legacy_state(&p);
    policies(&p);
    let before = shas(&p);
    let clock = ManualClock::new(now_ms());
    let s = Store::open(&p, config(&clock)).unwrap();
    assert_eq!(s.epoch, 8);
    // The switch: chunks/ in place, chunks.import/ gone, the legacy logs moved unmodified and read-only.
    assert!(!p.join("chunks.import").exists());
    assert!(!p.join("stripe-0.log").exists());
    let legacy = p.join("legacy-8");
    assert_eq!(shas(&legacy), before);
    assert!(fs::metadata(legacy.join("stripe-0.log")).unwrap().permissions().readonly());
    let report = s.replay_report();
    let line = report.import.clone().unwrap();
    assert!(line.contains("imported 55 keys") && line.contains("legacy-8") && line.contains("dedup replay on"), "{line}");
    assert_eq!(report.legacy.len(), before.len());
    // Every key's latest value, version and namespace count equal the legacy model.
    check_model(&s, &model);
    // Each key took its class.
    let classes: Vec<String> = s.chunk_rows().iter().map(|r| r.2.clone()).collect();
    assert!(classes.iter().any(|c| c == "expire(259200000ms)"));
    assert!(classes.iter().any(|c| c == "latest"));
    assert!(classes.iter().any(|c| c == "forever"));
    assert_eq!(s.policies().len(), 5);
    // A retry of a put made within 24 h before the import returns its original outcome and writes nothing.
    let retry = Mutation {
        ns: probe.ns.into(),
        key: probe.key.clone(),
        op_id: probe.op_id.clone(),
        deadline: now_ms() + 10000,
        ack: probe.ack,
        expected: probe.expected,
        bytes: probe.bytes.clone(),
        remove: probe.remove,
    };
    assert_eq!(s.mutate(retry.clone()), Outcome { code: 0, version: probe.version, ack: 1 });
    assert_eq!(s.metrics().0, 0);
    drop(s);
    // After a further restart: the same, and nothing imported again.
    let s = reopen(&p, config(&clock));
    check_model(&s, &model);
    assert_eq!(s.mutate(retry.clone()), Outcome { code: 0, version: probe.version, ack: 1 });
    assert_eq!(s.metrics().0, 0);
    assert!(s.replay_report().import.unwrap().starts_with("import of epoch 8"));
    assert!(!s.storage_error());
    drop(s);
    // Past 24 h the legacy logs are no longer replayed.
    clock.advance(DAY + 1);
    let s = reopen(&p, config(&clock));
    let report = s.replay_report();
    assert!(report.import.unwrap().contains("off (24 h passed)"));
    assert!(report.legacy.is_empty());
    check_model(&s, &model);
    assert_eq!(shas(&legacy), before, "legacy files changed");
    drop(s);
    // A stop between the switch and the move: the next start finishes the move.
    for name in before.keys() {
        let f = legacy.join(name);
        let mut perm = fs::metadata(&f).unwrap().permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        perm.set_readonly(false);
        fs::set_permissions(&f, perm).unwrap();
        fs::rename(&f, p.join(name)).unwrap();
    }
    let s = reopen(&p, config(&clock));
    assert!(!p.join("stripe-0.log").exists());
    assert_eq!(shas(&legacy), before);
    assert!(!s.storage_error());
    check_model(&s, &model);
    println!("T11: 55 keys imported equal; retry recognised after import and restart; legacy replay off past 24 h");
}

#[test]
fn t11_changed_legacy_file_starts_read_only() {
    let p = path("t11-changed");
    legacy_state(&p);
    let clock = ManualClock::new(now_ms());
    drop(Store::open(&p, config(&clock)).unwrap());
    let f = p.join("legacy-8").join("stripe-1.log");
    let mut perm = fs::metadata(&f).unwrap().permissions();
    #[allow(clippy::permissions_set_readonly_false)]
    perm.set_readonly(false);
    fs::set_permissions(&f, perm).unwrap();
    let mut b = fs::read(&f).unwrap();
    b.push(0);
    fs::write(&f, b).unwrap();
    let s = reopen(&p, config(&clock));
    assert!(s.storage_error());
    assert!(s.health2().read_only);
    assert!(s.replay_report().storage_errors[0].contains("legacy file changed"));
    let m = Mutation {
        ns: "ledger".into(),
        key: "k".into(),
        op_id: "k".into(),
        deadline: now_ms() + 10000,
        ack: 1,
        expected: None,
        bytes: b"v".to_vec(),
        remove: false,
    };
    assert_eq!(s.mutate(m).code, 10);
    println!("T11: a changed legacy file starts read_only with storage_error");
}

#[test]
fn t11_injected_difference_leaves_the_legacy_logs() {
    let p = path("t11-difference");
    legacy_state(&p);
    let before = shas(&p);
    let clock = ManualClock::new(now_ms());
    let e = Store::open(&p, Config { import_inject_difference: true, ..config(&clock) }).err().unwrap();
    assert!(e.to_string().contains("import refused, legacy logs left in place") && e.to_string().contains("missing from the import"), "{e}");
    assert_eq!(shas(&p), before, "the legacy logs must stay untouched");
    assert!(!p.join("chunks.import").exists());
    assert!(!p.join("legacy-8").exists());
    let json = fs::read_to_string(p.join("replay.json")).unwrap();
    assert!(json.contains("missing from the import"), "{json}");
    assert_eq!(fs::read(p.join("epoch")).unwrap(), 7u64.to_le_bytes(), "a refused start takes no epoch");
    // The same state imports cleanly once the difference is gone.
    let s = reopen(&p, config(&clock));
    assert!(s.replay_report().import.unwrap().contains("imported 55 keys"));
    println!("T11: an injected difference refused the import and left the legacy logs: {e}");
}

#[test]
fn t11_rollback_restores_the_legacy_logs() {
    let p = path("t11-rollback");
    legacy_state(&p);
    let before = shas(&p);
    let clock = ManualClock::new(now_ms());
    {
        let s = Store::open(&p, config(&clock)).unwrap();
        let m = Mutation {
            ns: "ledger".into(),
            key: "after-switch".into(),
            op_id: "after".into(),
            deadline: now_ms() + 10000,
            ack: 1,
            expected: None,
            bytes: b"v".to_vec(),
            remove: false,
        };
        assert_eq!(s.mutate(m).code, 0);
    }
    // The rollback steps (5.6, 3): chunks/ aside, the legacy files back, read-only cleared after the move.
    fs::rename(p.join("chunks"), p.join("chunks.rolled-back-8")).unwrap();
    for name in before.keys() {
        fs::rename(p.join("legacy-8").join(name), p.join(name)).unwrap();
        let f = p.join(name);
        let mut perm = fs::metadata(&f).unwrap().permissions();
        #[allow(clippy::permissions_set_readonly_false)]
        perm.set_readonly(false);
        fs::set_permissions(&f, perm).unwrap();
    }
    assert_eq!(shas(&p), before, "the restored legacy logs are the originals");
    println!("T11: rollback restored the legacy logs byte for byte (the previous binary's run is in import_production.py)");
}


fn refuse_unexamined_chunks(label: &str, catalogue: Option<&[u8]>) {
    let p = path(label);
    legacy_state(&p);
    let legacy_before = shas(&p);
    let dir = p.join("chunks").join("forever");
    fs::create_dir_all(&dir).unwrap();
    let protected = dir.join("0-99.log");
    let bytes = b"existing chunk whose catalogue evidence is unavailable";
    fs::write(&protected, bytes).unwrap();
    if let Some(bytes) = catalogue { fs::write(p.join("chunks/catalogue.log"), bytes).unwrap(); }
    let clock = ManualClock::new(now_ms());
    assert!(Store::open(&p, config(&clock)).is_err(), "import must refuse rather than replace unexplained chunks");
    assert_eq!(fs::read(&protected).unwrap(), bytes);
    assert_eq!(shas(&p), legacy_before);
    assert!(!p.join("chunks.import").exists());
}
#[test]
fn import_refuses_chunks_with_missing_catalogue() { refuse_unexamined_chunks("missing-catalogue", None); }
#[test]
fn import_refuses_chunks_with_empty_catalogue() { refuse_unexamined_chunks("empty-catalogue", Some(b"")); }
#[test]
fn import_refuses_chunks_with_torn_catalogue() { refuse_unexamined_chunks("torn-catalogue", Some(b"MBC1")); }

#[test]
fn imported_policy_does_not_displace_superseded_legacy_operation_identity() {
    let p=path("policy-import-alias");let(model,probe)=legacy_state_with_probe_ids(&p,Some("import-policy/8/0"),Some("import-policy/8/0/1"));
    let before=shas(&p);policies(&p);let clock=ManualClock::new(now_ms());let mut separate=None;
    for _ in 0..2 {
        let s=reopen(&p,config(&clock));check_model(&s,&model);
        let retry=Mutation{ns:probe.ns.into(),key:probe.key.clone(),op_id:probe.op_id.clone(),deadline:now_ms()+10000,
            ack:probe.ack,expected:probe.expected,bytes:probe.bytes.clone(),remove:probe.remove};
        assert_eq!(s.mutate(retry.clone()),Outcome{code:0,version:probe.version,ack:probe.ack},"retained original legacy outcome must remain authoritative");
        let mut mismatch=retry.clone();mismatch.bytes.push(0);assert_eq!(s.mutate(mismatch).code,14);
        let mut other=retry;other.ns="separate".into();let result=s.mutate(other);
        assert_eq!(result.code,0);assert!(result.version>model.values().map(|x|x.0).max().unwrap());
        if let Some(original)=&separate {assert_eq!(&result,original)} else {separate=Some(result);}
        assert_eq!(shas(&p.join("legacy-8")),before);
    }
}
