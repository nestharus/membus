use membus::*;
use std::path::PathBuf;

struct Fixture(PathBuf);
impl Fixture {
    fn new(label: &str) -> Self {
        let root = PathBuf::from(
            std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()),
        );
        std::fs::create_dir_all(&root).unwrap();
        Self(std::fs::canonicalize(root).unwrap().join(format!(
            "snapshot-cache-{label}-{}-{}",
            std::process::id(),
            now_ms()
        )))
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn request(s: &Store, op: u8, ns: &str, suffix: &[u8]) -> Result<Vec<u8>, u16> {
    let mut p = (now_ms() + 10_000).to_le_bytes().to_vec();
    lp(&mut p, b"snapshot-cache-read");
    lp(&mut p, ns.as_bytes());
    p.extend(suffix);
    let r = s.dispatch(&frame(op, 1, &p)[4..]);
    let code = u16::from_le_bytes(r[17..19].try_into().unwrap());
    if code == 0 {
        Ok(r[19..].to_vec())
    } else {
        Err(code)
    }
}
fn open(s: &Store, ns: &str) -> Vec<u8> {
    let b = request(s, SNAPSHOT_OPEN, ns, &[]).unwrap();
    Cursor::new(&b).bytes(16).unwrap().to_vec()
}
fn release(s: &Store, ns: &str, token: &[u8]) {
    let mut p = Vec::new();
    lp(&mut p, token);
    assert!(request(s, SNAPSHOT_RELEASE, ns, &p).unwrap().is_empty());
}
fn scan(
    s: &Store,
    ns: &str,
    token: &[u8],
    prefix: &str,
    keys: bool,
    budget: u32,
) -> Result<Vec<u8>, u16> {
    let mut p = Vec::new();
    lp(&mut p, token);
    lp(&mut p, &[]);
    p.extend(128u16.to_le_bytes());
    p.extend(budget.to_le_bytes());
    lp(&mut p, prefix.as_bytes());
    request(s, if keys { SCAN_KEYS_PREFIX } else { SCAN_PREFIX }, ns, &p)
}
fn put(s: &Store, ns: &str, key: &str, id: &str, bytes: Vec<u8>, remove: bool) -> u64 {
    let o = s.mutate(Mutation {
        ns: ns.into(),
        key: key.into(),
        op_id: id.into(),
        deadline: now_ms() + 10_000,
        ack: 1,
        expected: None,
        bytes,
        remove,
    });
    assert_eq!(o.code, 0);
    o.version
}
fn single_value(b: &[u8], key: &str, version: u64, bytes: &[u8]) {
    let mut c = Cursor::new(b);
    c.bytes(16).unwrap();
    assert!(c.bytes(4096).unwrap().is_empty());
    assert_eq!(c.u8().unwrap(), 1);
    assert_eq!(c.u16().unwrap(), 1);
    assert_eq!(c.text().unwrap(), key);
    assert_eq!(c.u8().unwrap(), 1);
    assert_eq!(c.u64().unwrap(), version);
    assert_eq!(c.u8().unwrap(), 1);
    assert_eq!(c.bytes(MAX_VALUE).unwrap(), bytes);
    c.end().unwrap();
}

#[test]
fn cold_snapshot_and_key_pages_leave_payloads_on_disk() {
    let f = Fixture::new("keys");
    {
        let s = Store::open(&f.0, Config::default()).unwrap();
        for i in 0..64 {
            put(
                &s,
                "ledger",
                &format!("row/{i:04}"),
                &format!("seed{i}"),
                vec![i as u8; 8192],
                false,
            );
        }
    }
    let s = Store::open(&f.0, Config::default()).unwrap();
    let token = open(&s, "ledger");
    let b = scan(&s, "ledger", &token, "row/", true, 4096).unwrap();
    let mut c = Cursor::new(&b);
    assert_eq!(c.bytes(16).unwrap(), token);
    assert!(c.bytes(4096).unwrap().is_empty());
    assert_eq!(c.u8().unwrap(), 1);
    assert_eq!(c.u16().unwrap(), 64);
    for i in 0..64 {
        assert_eq!(c.text().unwrap(), format!("row/{i:04}"));
    }
    c.end().unwrap();
    assert_eq!(
        s.cache_stats().cold_reads,
        0,
        "key discovery must not read value frames"
    );
    assert_eq!(s.cache_stats().entries, 0);
    release(&s, "ledger", &token);
    let b = scan(&s, "ledger", &[], "row/0001", true, 4096).unwrap();
    let token = Cursor::new(&b).bytes(16).unwrap().to_vec();
    assert_eq!(
        s.cache_stats().cold_reads,
        0,
        "implicit snapshots must also remain cold"
    );
    release(&s, "ledger", &token);
}

#[test]
fn narrow_snapshot_queries_load_only_returned_values_and_reuse_cache() {
    let f = Fixture::new("narrow");
    let config = Config {
        cache_bytes: 256 * 1024,
        signal_bytes: 0,
        ..Config::default()
    };
    let version;
    {
        let s = Store::open(&f.0, config.clone()).unwrap();
        version = put(&s, "ledger", "head", "head", vec![7; 8192], false);
        for i in 0..128 {
            put(
                &s,
                "ledger",
                &format!("rows/{i:04}"),
                &format!("seed{i}"),
                vec![i as u8; 8192],
                false,
            );
        }
    }
    let s = Store::open(&f.0, config).unwrap();
    for _ in 0..8 {
        let token = open(&s, "ledger");
        assert_eq!(scan(&s, "ledger", &token, "head", false, 1024), Err(4));
        let b = scan(&s, "ledger", &token, "head", false, 16384).unwrap();
        single_value(&b, "head", version, &vec![7; 8192]);
        release(&s, "ledger", &token);
    }
    let stats = s.cache_stats();
    assert_eq!(
        stats.cold_reads, 1,
        "unselected rows and rejected pages must not churn the cache"
    );
    assert_eq!(stats.hits, 7);
    assert_eq!(stats.entries, 1);
}

#[test]
fn first_page_after_replacement_and_removal_keeps_original_revision() {
    let f = Fixture::new("revision");
    let s = Store::open(
        &f.0,
        Config {
            cache_bytes: 0,
            ..Config::default()
        },
    )
    .unwrap();
    let version = put(&s, "ledger", "row", "original", b"original".to_vec(), false);
    let token = open(&s, "ledger");
    put(&s, "ledger", "row", "replacement", b"new".to_vec(), false);
    put(&s, "ledger", "row", "remove", Vec::new(), true);
    let b = scan(&s, "ledger", &token, "row", false, 1024).unwrap();
    single_value(&b, "row", version, b"original");
    assert!(s.get("ledger", "row").is_none());
    release(&s, "ledger", &token);
}

#[test]
fn unread_snapshot_survives_physical_chunk_retirement_without_readmission() {
    let f = Fixture::new("retired");
    let clock = ManualClock::new(now_ms());
    let s = Store::open(
        &f.0,
        Config {
            clock: Some(clock.clone()),
            retry_ms: 10,
            rotation_ms: 1,
            tick_ms: 3_600_000,
            retire_ms: 3_600_000,
            ..Config::default()
        },
    )
    .unwrap();
    assert_eq!(
        s.set_policy("archive", "", Some(Class::expire(20)), now_ms() + 10_000)
            .code,
        0
    );
    let version = put(&s, "archive", "row", "seed", b"retained".to_vec(), false);
    let token = open(&s, "archive");
    clock.advance(2);
    s.tick();
    clock.advance(20);
    s.retirement_pass();
    assert_eq!(s.get_status("archive", "row"), (1, 0));
    let b = scan(&s, "archive", &token, "row", false, 1024).unwrap();
    single_value(&b, "row", version, b"retained");
    assert!(!s
        .cache_stats()
        .residency
        .iter()
        .any(|(class, _, _)| class == "expire(20ms)"));
    release(&s, "archive", &token);
}

fn corrupt_last_frame(root: &std::path::Path) {
    use std::io::{Read, Seek, SeekFrom, Write};
    let file = std::fs::read_dir(root.join("chunks/forever"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|s| s == "log"))
        .unwrap();
    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(file)
        .unwrap();
    f.seek(SeekFrom::End(-1)).unwrap();
    let mut byte = [0];
    f.read_exact(&mut byte).unwrap();
    byte[0] ^= 1;
    f.seek(SeekFrom::End(-1)).unwrap();
    f.write_all(&byte).unwrap();
}

#[test]
fn value_errors_are_reported_on_demand_and_failed_implicit_views_are_released() {
    let f = Fixture::new("io");
    let s = Store::open(
        &f.0,
        Config {
            cache_bytes: 0,
            snapshot_max: 1,
            ..Config::default()
        },
    )
    .unwrap();
    put(&s, "ledger", "row", "seed", vec![3; 8192], false);
    corrupt_last_frame(&f.0);
    assert_eq!(scan(&s, "ledger", &[], "row", false, 1024), Err(4));
    assert!(
        !s.storage_error(),
        "a page that cannot fit must not read a frame"
    );
    let b = scan(&s, "ledger", &[], "row", true, 1024).unwrap();
    let token = Cursor::new(&b).bytes(16).unwrap().to_vec();
    release(&s, "ledger", &token);
    assert_eq!(scan(&s, "ledger", &[], "row", false, 16384), Err(16));
    assert!(s.storage_error());
    assert_eq!(s.cache_stats().entries, 0);
    let token = open(&s, "ledger"); // Failed implicit page did not exhaust snapshot_max.
    release(&s, "ledger", &token);
}

#[test]
fn a_materialized_snapshot_value_stays_pinned_without_the_resident_cache() {
    let f = Fixture::new("pinned");
    let s = Store::open(
        &f.0,
        Config {
            cache_bytes: 0,
            ..Config::default()
        },
    )
    .unwrap();
    let version = put(&s, "ledger", "row", "seed", b"pinned".to_vec(), false);
    let token = open(&s, "ledger");
    single_value(
        &scan(&s, "ledger", &token, "row", false, 1024).unwrap(),
        "row",
        version,
        b"pinned",
    );
    let reads = s.cache_stats().cold_reads;
    corrupt_last_frame(&f.0);
    single_value(
        &scan(&s, "ledger", &token, "row", false, 1024).unwrap(),
        "row",
        version,
        b"pinned",
    );
    assert_eq!(s.cache_stats().cold_reads, reads);
    release(&s, "ledger", &token);
    assert_eq!(scan(&s, "ledger", &[], "row", false, 1024), Err(16));
}
