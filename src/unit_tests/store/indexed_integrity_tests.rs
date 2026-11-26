//! Real replay of a new authored incomplete tree, preserving the complete source.
use super::*;
use crate::{Config, Store};
use std::{
    fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{atomic::Ordering, Arc},
    time::{Duration, Instant},
};
struct Peer {
    store: Arc<Store>,
    pending: Vec<u8>,
    reply: Vec<u8>,
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
                self.reply =
                    self.store
                        .dispatch_for_limit(&self.pending[4..], false, crate::MAX_FRAME);
                self.pending.clear();
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
fn d() -> Deadline {
    Deadline {
        unix_ms: crate::now_ms() + 30_000,
        local: Instant::now() + Duration::from_secs(30),
    }
}
fn client(s: &Arc<Store>) -> Client<Peer> {
    let mut o = client::Options::new("authored-index-integrity", crate::MAX_FRAME);
    o.scans = true;
    Client::handshake(
        Peer {
            store: s.clone(),
            pending: vec![],
            reply: vec![],
        },
        o,
        d().local,
    )
    .unwrap()
}
fn root(label: &str) -> PathBuf {
    let base = PathBuf::from(
        std::env::var_os("MEMBUS_PROVIDER_TEST_ROOT").expect("explicit retained root"),
    );
    assert!(base.is_absolute() && base.is_dir());
    let p = base.join(format!(
        "snapshot-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&p).unwrap();
    println!("retained {}", p.display());
    p
}
fn config() -> Config {
    Config {
        cache_bytes: 0,
        signal_bytes: 0,
        flush_ms: 0,
        chunk_bytes: 1,
        tick_ms: 3_600_000,
        retire_ms: 3_600_000,
        ..Default::default()
    }
}
fn col() -> Collection {
    Collection::new(
        "fixture",
        Schema {
            fields: vec!["kind".into()],
        },
    )
    .unwrap()
}
fn limits() -> Limits {
    Limits {
        max_rows: 8,
        max_bytes: 1024 * 1024,
    }
}
fn copy_except(source: &Path, dest: &Path, missing: &[PathBuf]) {
    for entry in fs::read_dir(source).unwrap() {
        let entry = entry.unwrap();
        let p = entry.path();
        let to = dest.join(entry.file_name());
        if missing.contains(&p) {
            println!("authored absent closed posting {}", p.display());
            continue;
        }
        if p.is_dir() {
            fs::create_dir(&to).unwrap();
            copy_except(&p, &to, missing)
        } else {
            fs::copy(&p, &to).unwrap();
        }
    }
}
fn complete_check(s: &Store, ns: &str, token: &[u8]) -> u16 {
    let mut p = (crate::now_ms() + 30_000).to_le_bytes().to_vec();
    crate::lp(&mut p, b"eligibility");
    crate::lp(&mut p, ns.as_bytes());
    crate::lp(&mut p, token);
    let f = crate::frame(31, 9, &p);
    let reply = s.dispatch_for_limit(&f[4..], false, crate::MAX_FRAME);
    u16::from_le_bytes(reply[17..19].try_into().unwrap())
}
#[test]
fn known_missing_posting_storage_refuses_equal_instead_of_partial_success() {
    missing_case(1);
}
#[test]
fn known_missing_entire_posting_bucket_refuses_empty_success() {
    missing_case(2);
}
fn missing_case(missing_count: usize) {
    let original = root("complete-source");
    let s = Arc::new(Store::open(&original, config()).unwrap());
    let mut c = client(&s);
    let col = col();
    for n in 1..=2 {
        col.append(
            &mut c,
            "authored",
            &Record {
                ordinal: n,
                fields: vec![vec![b"match".to_vec()]],
                payload: vec![n as u8],
            },
            &format!("append-{n}"),
            Ack::Os,
            d(),
        )
        .unwrap();
    }
    let token = c.snapshot_open("authored", "complete", d()).unwrap();
    assert_eq!(
        col.query(
            &mut c,
            "authored",
            &token,
            &Query::Equal {
                field: "kind".into(),
                value: b"match".to_vec()
            },
            limits(),
            "original",
            d()
        )
        .unwrap()
        .records
        .len(),
        2
    );
    c.snapshot_release("authored", &token, "release", d())
        .unwrap();
    let missing = {
        let committed = s.committed.read().unwrap();
        let state = s.state.lock().unwrap();
        let chunks = s.chunks.lock().unwrap();
        committed
            .iter()
            .filter(|((ns, key), _)| ns == "authored" && key.contains("/i/"))
            .take(missing_count)
            .map(|(key, entry)| {
                let path = chunks.table[&state.metadata[key].chunk].path.clone();
                assert_eq!(
                    entry.location.record_len,
                    fs::metadata(&path).unwrap().len()
                );
                path
            })
            .collect::<Vec<_>>()
    };
    assert_eq!(missing.len(), missing_count);
    drop(c);
    drop(s);
    let incomplete = root("missing-posting");
    copy_except(&original, &incomplete, &missing);
    let s = Arc::new(Store::open(&incomplete, config()).unwrap());
    assert!(s.storage_error());
    assert!(s.health2().read_only);
    let mut c = client(&s);
    let token = c.snapshot_open("authored", "incomplete", d()).unwrap();
    // Surviving schema/head/rows make an absent posting look like a valid subset.
    assert!(s.get("authored", &col.schema_key).is_some());
    assert_eq!(
        count(&s.get("authored", &col.head_key).unwrap().bytes).unwrap(),
        2
    );
    assert!(s.get("authored", &col.row_key(1)).is_some());
    for query in [
        Query::Equal {
            field: "kind".into(),
            value: b"match".to_vec(),
        },
        Query::All,
        Query::Ordinals { first: 1, last: 2 },
    ] {
        let result = col.query(
            &mut c,
            "authored",
            &token,
            &query,
            limits(),
            "known-missing",
            d(),
        );
        println!("known missing query {query:?}: {result:?}");
        assert!(
            matches!(result,Err(Error::Client(client::Error::Refused(ref r))) if r.code==16),
            "complete query must refuse known missing storage on its captured token"
        );
    }
    c.snapshot_release("authored", &token, "release", d())
        .unwrap();
}
#[test]
fn snapshot_eligibility_is_captured_not_guessed_from_later_health() {
    let p = root("immutable-eligibility");
    let s = Store::open(&p, config()).unwrap();
    let (first, _) = s.snapshot_open("authored").unwrap();
    assert_eq!(complete_check(&s, "authored", &first), 0);
    s.flags.storage_error.store(true, Ordering::SeqCst);
    assert_eq!(
        complete_check(&s, "authored", &first),
        0,
        "later health cannot change an existing captured eligibility"
    );
    let (second, _) = s.snapshot_open("authored").unwrap();
    s.flags.storage_error.store(false, Ordering::SeqCst);
    assert_eq!(
        complete_check(&s, "authored", &second),
        16,
        "later health cannot turn an incomplete view complete"
    );
    assert_eq!(complete_check(&s, "other", &first), 21);
    s.snapshot_release("authored", &first).unwrap();
    assert_eq!(complete_check(&s, "authored", &first), 20);
    s.snapshot_release("authored", &second).unwrap();
}

#[test]
fn eligible_queries_preserve_full_field_bytes_ordinals_and_candidate_limits() {
    let p = root("eligible-queries");
    let s = Arc::new(Store::open(&p, config()).unwrap());
    let mut c = client(&s);
    let col = col();
    let exact = vec![0, 255, 1, 0, 128];
    for n in 1..=2 {
        col.append(
            &mut c,
            "authored",
            &Record {
                ordinal: n,
                fields: vec![vec![exact.clone()]],
                payload: vec![n as u8, 255],
            },
            &format!("append-{n}"),
            Ack::Os,
            d(),
        )
        .unwrap();
    }
    let token = c.snapshot_open("authored", "capture", d()).unwrap();
    c.snapshot_require_complete("authored", &token, "typed-complete", d())
        .unwrap();
    for query in [
        Query::All,
        Query::Ordinals { first: 1, last: 2 },
        Query::Equal {
            field: "kind".into(),
            value: exact.clone(),
        },
    ] {
        let result = col
            .query(&mut c, "authored", &token, &query, limits(), "healthy", d())
            .unwrap();
        assert_eq!(result.through, 2);
        assert_eq!(
            result.records.iter().map(|r| r.ordinal).collect::<Vec<_>>(),
            vec![1, 2]
        );
        assert!(result
            .records
            .iter()
            .all(|r| r.fields == vec![vec![exact.clone()]]));
        assert!(matches!(
            col.query(
                &mut c,
                "authored",
                &token,
                &query,
                Limits {
                    max_rows: 1,
                    max_bytes: 1024 * 1024
                },
                "limited",
                d()
            ),
            Err(Error::Limit)
        ));
    }
    let empty = col
        .query(
            &mut c,
            "authored",
            &token,
            &Query::Equal {
                field: "kind".into(),
                value: b"absent".to_vec(),
            },
            limits(),
            "empty",
            d(),
        )
        .unwrap();
    assert!(empty.records.is_empty());
    c.snapshot_release("authored", &token, "release", d())
        .unwrap();
}
