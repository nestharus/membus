//! Query-byte refusal must bound acquisition without evicting a useful value.
use membus::{
    client::{Ack, Client, Deadline, Error as ClientError, Options, Transport},
    indexed::{Collection, Error, Limits, Query, Record, Schema},
    CachePolicy, Config, Cursor, Store, MAX_FRAME, SCAN_PREFIX, SCAN_RANGE,
};
use std::{
    io::{self, Read, Write},
    path::PathBuf,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
#[derive(Clone, Debug)]
struct Page {
    budget: usize,
    code: u16,
    reply_bytes: usize,
}
struct Local {
    store: Arc<Store>,
    input: Vec<u8>,
    output: io::Cursor<Vec<u8>>,
    pages: Arc<Mutex<Vec<Page>>>,
}
impl Write for Local {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        self.input.extend(input);
        if self.input.len() >= 4 {
            let n = u32::from_le_bytes(self.input[..4].try_into().unwrap()) as usize;
            if self.input.len() == n + 4 {
                let body = &self.input[4..];
                let reply = self.store.dispatch(body);
                if matches!(body[4], SCAN_PREFIX | SCAN_RANGE) {
                    let mut c = Cursor::new(&body[13..]);
                    c.u64().unwrap();
                    c.text().unwrap();
                    c.text().unwrap();
                    c.bytes(16).unwrap();
                    c.bytes(2 * membus::MAX_ID + 48).unwrap();
                    c.u16().unwrap();
                    let budget = c.u32().unwrap() as usize;
                    if c.text().unwrap().starts_with("x/rows/r/") {
                        self.pages.lock().unwrap().push(Page {
                            budget,
                            code: u16::from_le_bytes(reply[17..19].try_into().unwrap()),
                            reply_bytes: reply.len(),
                        });
                    }
                }
                self.output = io::Cursor::new(reply);
                self.input.clear();
            }
        }
        Ok(input.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Read for Local {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        self.output.read(out)
    }
}
impl Transport for Local {
    fn deadline(&mut self, until: Instant) -> io::Result<()> {
        if Instant::now() >= until {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "indexed admission deadline",
            ))
        } else {
            Ok(())
        }
    }
}
fn deadline() -> Deadline {
    Deadline {
        unix_ms: membus::now_ms() + 30_000,
        local: Instant::now() + Duration::from_secs(30),
    }
}
struct Fixture(PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn fixture(label: &str) -> (Fixture, Arc<Store>, Arc<Mutex<Vec<Page>>>) {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = PathBuf::from(
        std::env::var_os("MEMBUS_TEST_OUTPUT").unwrap_or_else(|| ".test-output".into()),
    )
    .join(format!(
        "indexed-admission-{label}-{}-{stamp}",
        std::process::id()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let f = Fixture(root);
    let store = Arc::new(
        Store::open(
            &f.0,
            Config {
                capacity: 64 * 1024 * 1024,
                cache_bytes: 256 * 1024,
                snapshot_bytes: 32 * 1024 * 1024,
                signal_bytes: 0,
                cache_rules: vec![],
                tick_ms: 3_600_000,
                retire_ms: 3_600_000,
                clock: Some(membus::ManualClock::new(membus::now_ms())),
                cache_policy: CachePolicy {
                    initial_ttl: 0,
                    active_ttl: 3_600_000,
                    ..CachePolicy::default()
                },
                ..Config::default()
            },
        )
        .unwrap(),
    );
    (f, store, Arc::new(Mutex::new(Vec::new())))
}
fn client(store: Arc<Store>, pages: Arc<Mutex<Vec<Page>>>, max: usize) -> Client<Local> {
    let mut options = Options::new("indexed-admission-control", max);
    options.scans = true;
    Client::handshake(
        Local {
            store,
            pages,
            input: Vec::new(),
            output: io::Cursor::new(Vec::new()),
        },
        options,
        deadline().local,
    )
    .unwrap()
}
fn row() -> Record {
    Record {
        ordinal: 1,
        fields: vec![],
        payload: vec![0x71; 200_000],
    }
}
fn put(s: &Store, ns: &str, key: &str, op: &str, bytes: Vec<u8>) {
    assert_eq!(
        s.mutate(membus::Mutation {
            ns: ns.into(),
            key: key.into(),
            op_id: op.into(),
            deadline: membus::now_ms() + 30_000,
            ack: 1,
            expected: None,
            bytes,
            remove: false
        })
        .code,
        0
    );
}
fn rejected_query(query: Query, allowance: usize) {
    let (_fixture, store, pages) = fixture(if matches!(query, Query::All) {
        "all"
    } else {
        "ordinals"
    });
    let mut c = client(store.clone(), pages.clone(), MAX_FRAME);
    let collection = Collection::new("rows", Schema { fields: vec!["source".into()] }).unwrap();
    let mut original = row();
    original.fields = vec![vec![b"plumless".to_vec()]];
    collection
        .append(&mut c, "ledger", &original, "append", Ack::Os, deadline())
        .unwrap();
    let token = c.snapshot_open("ledger", "open", deadline()).unwrap();
    // An empty interval validates and pins only declaration/boundary, leaving
    // the large captured row cold for the actual admission encounter.
    assert!(collection
        .query(
            &mut c,
            "ledger",
            &token,
            &Query::Ordinals { first: 2, last: 2 },
            Limits {
                max_rows: 1,
                max_bytes: 0
            },
            "prime",
            deadline()
        )
        .unwrap()
        .records
        .is_empty());
    if let Query::Equal { value, .. } = &query {
        let prefix = format!("x/rows/i/00/{:08x}/", membus::crc32(value));
        c.scan_prefix("ledger", &prefix, &token, &[], 1, MAX_FRAME, "prime-postings", deadline())
            .unwrap();
    }
    pages.lock().unwrap().clear();
    put(
        &store,
        "working",
        "head",
        "seed-working",
        vec![0x92; 65_536],
    );
    assert_eq!(
        store.try_get("working", "head").unwrap().unwrap().bytes,
        vec![0x92; 65_536]
    );
    let before = store.cache_stats();
    let result = collection.query(
        &mut c,
        "ledger",
        &token,
        &query,
        Limits {
            max_rows: 1,
            max_bytes: allowance,
        },
        "bounded",
        deadline(),
    );
    assert!(
        matches!(result, Err(Error::Limit)),
        "a refused complete query must remain Limit"
    );
    assert!(c.is_usable());
    let after = store.cache_stats();
    let wire = pages.lock().unwrap().clone();
    assert_eq!(wire.len(), 1);
    assert_eq!(
        store.try_get("working", "head").unwrap().unwrap().bytes,
        vec![0x92; 65_536]
    );
    let probe = store.cache_stats();
    // A refused query leaves the caller-owned view available for an exact retry.
    let exact = collection.encoded_len(&original).unwrap();
    let recovered = collection
        .query(
            &mut c,
            "ledger",
            &token,
            &query,
            Limits {
                max_rows: 1,
                max_bytes: exact,
            },
            "exact-retry",
            deadline(),
        )
        .unwrap();
    assert_eq!(recovered.through, 1);
    let matches = !matches!(&query, Query::Equal { value, .. } if value.as_slice() != b"plumless");
    assert_eq!(recovered.records, if matches { vec![original] } else { vec![] });
    assert_eq!(recovered.encoded_bytes_read, exact);
    assert!(matches!(
        collection.query(
            &mut c,
            "ledger",
            &token,
            &query,
            Limits {
                max_rows: 1,
                max_bytes: exact - 1
            },
            "one-short",
            deadline()
        ),
        Err(Error::Limit)
    ));
    c.snapshot_release("ledger", &token, "release", deadline())
        .unwrap();
    assert_eq!(
        after.cold_reads, before.cold_reads,
        "oversized required row was loaded before query-byte refusal"
    );
    assert_eq!(
        after.evicted_budget, before.evicted_budget,
        "refused acquisition churned the working set"
    );
    assert_eq!(probe.cold_reads, after.cold_reads);
    assert_eq!(probe.hits - after.hits, 1);
    assert!(
        wire[0].budget <= 8192,
        "a 1KiB row allowance must not request a full negotiated page"
    );
    assert_eq!(wire[0].code, 4);
    assert_eq!(wire[0].reply_bytes, 19);
}
#[test]
fn range_admission_refuses_before_loading_an_oversized_row() {
    for query in [Query::All, Query::Ordinals { first: 1, last: 1 }] {
        rejected_query(query, 1024);
    }
}
#[test]
fn equality_admission_bounds_matching_and_collision_candidates() {
    assert_eq!(membus::crc32(b"plumless"), membus::crc32(b"buckeroo"));
    for value in [b"plumless", b"buckeroo"] {
        for allowance in [0, 1024] {
            rejected_query(Query::Equal { field: "source".into(), value: value.to_vec() }, allowance);
        }
    }
}
#[test]
fn range_admission_keeps_transport_refusals_and_unbounded_arithmetic() {
    let (_fixture, store, pages) = fixture("transport");
    let mut c = client(store.clone(), pages.clone(), MAX_FRAME);
    let collection = Collection::new("rows", Schema { fields: vec![] }).unwrap();
    let original = row();
    collection
        .append(&mut c, "ledger", &original, "append", Ack::Os, deadline())
        .unwrap();
    let token = c.snapshot_open("ledger", "open", deadline()).unwrap();
    let limits = Limits {
        max_rows: usize::MAX,
        max_bytes: usize::MAX,
    };
    let mut small = client(store.clone(), pages, 1024);
    assert!(
        matches!(collection.query(&mut small,"ledger",&token,&Query::All,limits,"transport-bound",deadline()),Err(Error::Client(ClientError::Refused(r))) if r.code==4)
    );
    assert!(small.is_usable());
    let result = collection
        .query(
            &mut c,
            "ledger",
            &token,
            &Query::All,
            limits,
            "large-allowance",
            deadline(),
        )
        .unwrap();
    assert_eq!(result.records, vec![original]);
    // A non-participating writer inserts an extra key inside the committed
    // interval. Smaller pages must still examine and refuse it on continuation.
    let encoded = store
        .try_get("ledger", "x/rows/r/00000000000000000001")
        .unwrap()
        .unwrap()
        .bytes;
    put(
        &store,
        "ledger",
        "x/rows/r/00000000000000000001z",
        "extra-row",
        encoded,
    );
    let extra = c.snapshot_open("ledger", "extra-open", deadline()).unwrap();
    assert!(matches!(
        collection.query(
            &mut c,
            "ledger",
            &extra,
            &Query::All,
            limits,
            "extra-read",
            deadline()
        ),
        Err(Error::Corrupt(_))
    ));
    c.snapshot_release("ledger", &extra, "extra-release", deadline())
        .unwrap();
    c.snapshot_release("ledger", &token, "release", deadline())
        .unwrap();
}

#[test]
fn minimum_range_bounds_acquisition_before_initial_and_continuation_pages() {
    let (_fixture, store, pages) = fixture("minimum-range");
    let mut c = client(store.clone(), pages.clone(), MAX_FRAME);
    let collection = Collection::new("rows", Schema {
        fields: (0..4).map(|index| format!("field{index}")).collect(),
    }).unwrap();
    let rows: Vec<_> = (1..=32).map(|ordinal| Record {
        ordinal, fields: vec![vec![]; 4],
        payload: if ordinal == 1 { vec![0x61; 600] } else { vec![] },
    }).collect();
    for row in &rows {
        collection.append(&mut c, "ledger", row, &format!("seed-{}", row.ordinal), Ack::Os, deadline()).unwrap();
    }
    let token = c.snapshot_open("ledger", "open", deadline()).unwrap();
    let mut small = client(store.clone(), pages.clone(), 1024);
    let exact: usize = rows.iter().map(|row| collection.encoded_len(row).unwrap()).sum();
    for query in [Query::All, Query::Ordinals { first: 1, last: u64::MAX }] {
        for (max_bytes, expected_pages) in [(32 * 28 - 1, 0), (exact - 1, 1)] {
            pages.lock().unwrap().clear();
            assert!(matches!(collection.query(
                &mut small, "ledger", &token, &query,
                Limits { max_rows: usize::MAX, max_bytes }, "bounded", deadline(),
            ), Err(Error::Limit)));
            assert_eq!(pages.lock().unwrap().len(), expected_pages);
            assert!(small.is_usable());
        }
        let read = collection.query(
            &mut small, "ledger", &token, &query,
            Limits { max_rows: usize::MAX, max_bytes: exact }, "retry", deadline(),
        ).unwrap();
        assert_eq!(read.records, rows);
        assert_eq!(read.encoded_bytes_read, exact);
    }
    c.snapshot_release("ledger", &token, "release", deadline()).unwrap();
    let mut head = b"MBIX1H".to_vec();
    head.extend((u64::MAX - 1).to_le_bytes());
    put(&store, "ledger", "x/rows/head", "extreme", head);
    let token = c.snapshot_open("ledger", "extreme", deadline()).unwrap();
    pages.lock().unwrap().clear();
    assert!(matches!(collection.query(
        &mut c, "ledger", &token, &Query::All,
        Limits { max_rows: usize::MAX, max_bytes: usize::MAX }, "extreme-read", deadline(),
    ), Err(Error::Limit)));
    assert!(pages.lock().unwrap().is_empty());
    c.snapshot_release("ledger", &token, "extreme-release", deadline()).unwrap();
    assert!(!store.storage_error());
}
