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
#[derive(Clone, Copy, Debug)]
enum Receipt {
    Loss,
    BadProof,
    Refusal(u16),
}
#[derive(Default)]
struct PublicationWire {
    value_cap: Option<u32>,
    receipt: Option<Receipt>,
    requests: Vec<(u8, usize)>,
    drops: usize,
}
struct Local {
    store: Arc<Store>,
    input: Vec<u8>,
    output: io::Cursor<Vec<u8>>,
    pages: Arc<Mutex<Vec<Page>>>,
    publication: Option<Arc<Mutex<PublicationWire>>>,
    lose_receipt: bool,
}
impl Drop for Local {
    fn drop(&mut self) {
        if let Some(wire) = &self.publication {
            wire.lock().unwrap().drops += 1;
        }
    }
}
impl Write for Local {
    fn write(&mut self, input: &[u8]) -> io::Result<usize> {
        self.input.extend(input);
        if self.input.len() >= 4 {
            let n = u32::from_le_bytes(self.input[..4].try_into().unwrap()) as usize;
            if self.input.len() == n + 4 {
                let body = &self.input[4..];
                let mut reply = None;
                if let Some(wire) = &self.publication {
                    let mut wire = wire.lock().unwrap();
                    wire.requests.push((body[4], n));
                    if body[4] == membus::HELLO {
                        let mut hello = self.store.dispatch(body);
                        if let Some(cap) = wire.value_cap {
                            hello[31..35].copy_from_slice(&cap.to_le_bytes());
                        }
                        reply = Some(hello);
                    } else if body[4] == membus::CONDITIONAL_PUT {
                        let mut fields = Cursor::new(&body[13..]);
                        fields.u64().unwrap();
                        if fields.text().unwrap().ends_with(":commit") {
                            match wire.receipt {
                                Some(Receipt::Loss) => self.lose_receipt = true,
                                Some(Receipt::BadProof) => {
                                    let mut malformed = self.store.dispatch(body);
                                    malformed[19..27].fill(0);
                                    reply = Some(malformed);
                                }
                                Some(Receipt::Refusal(code)) => {
                                    let mut refusal = 15u32.to_le_bytes().to_vec();
                                    refusal.extend_from_slice(&body[..13]);
                                    refusal[8] |= 128;
                                    refusal.extend_from_slice(&code.to_le_bytes());
                                    reply = Some(refusal);
                                }
                                None => (),
                            }
                        }
                    }
                }
                let reply = reply.unwrap_or_else(|| self.store.dispatch(body));
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
        if self.lose_receipt && self.output.position() == 4 {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "authored head receipt loss",
            ));
        }
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
    local_client(store, pages, max, None)
}
fn local_client(
    store: Arc<Store>,
    pages: Arc<Mutex<Vec<Page>>>,
    max: usize,
    publication: Option<Arc<Mutex<PublicationWire>>>,
) -> Client<Local> {
    let mut options = Options::new("indexed-admission-control", max);
    options.scans = true;
    Client::handshake(
        Local {
            store,
            pages,
            input: Vec::new(),
            output: io::Cursor::new(Vec::new()),
            publication,
            lose_receipt: false,
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

fn mutation_frames(wire: &Mutex<PublicationWire>) -> Vec<usize> {
    wire.lock()
        .unwrap()
        .requests
        .iter()
        .filter_map(|&(op, size)| (op == membus::CONDITIONAL_PUT).then_some(size))
        .collect()
}

#[test]
fn indexed_publication_admits_complete_values_before_any_request() {
    let (_fixture, store, pages) = fixture("publication-values");
    let collection = Collection::new("small", Schema { fields: vec![] }).unwrap();
    let row = Record {
        ordinal: 1,
        fields: vec![],
        payload: vec![],
    };
    for append in [false, true] {
        for cap in [7, 8, 13, 14, 15, 19, 20, 21] {
            let ns = format!("small-{append}-{cap}");
            let wire = Arc::new(Mutex::new(PublicationWire {
                value_cap: Some(cap),
                ..Default::default()
            }));
            let mut c = local_client(store.clone(), pages.clone(), MAX_FRAME, Some(wire.clone()));
            assert_eq!(c.negotiated().max_value, cap as usize);
            let result = if append {
                collection
                    .append(&mut c, &ns, &row, "original", Ack::Flush, deadline())
                    .map(|_| ())
            } else {
                collection.declare(&mut c, &ns, "original", Ack::Flush, deadline())
            };
            let fits = cap >= if append { 20 } else { 14 };
            if fits {
                result.unwrap();
                let frames = mutation_frames(&wire);
                assert_eq!(frames.len(), if append { 4 } else { 2 });
                assert_eq!(
                    store.try_get(&ns, "x/small/schema").unwrap().unwrap().bytes,
                    b"MBIX1S\0\0"
                );
                let head = store.try_get(&ns, "x/small/head").unwrap().unwrap();
                assert_eq!(
                    head.bytes,
                    [b"MBIX1H".as_slice(), &u64::from(append).to_le_bytes()].concat()
                );
                if append {
                    assert!(
                        collection
                            .append(&mut c, &ns, &row, "original", Ack::Flush, deadline())
                            .unwrap()
                            .already_committed
                    );
                } else {
                    collection
                        .declare(&mut c, &ns, "original", Ack::Flush, deadline())
                        .unwrap();
                }
                assert_eq!(mutation_frames(&wire), frames);
            } else {
                assert!(matches!(
                    result,
                    Err(Error::Client(ClientError::Invalid("value limit")))
                ));
                assert_eq!(wire.lock().unwrap().requests.len(), 1);
                for key in [
                    "x/small/schema",
                    "x/small/head",
                    "x/small/r/00000000000000000001",
                ] {
                    assert!(store.try_get(&ns, key).unwrap().is_none());
                }
            }
            assert!(c.is_usable());
        }
    }
}

#[test]
fn indexed_publication_admits_exact_mutation_frame_boundary_before_any_request() {
    let (_fixture, store, pages) = fixture("publication-frames");
    for (case, exact) in [
        ("schema", 1024),
        ("row", 1024),
        ("posting", 1024),
        ("schema", 1025),
        ("row", 1025),
        ("posting", 1025),
    ] {
        let ns = format!("{case}-{exact}");
        let mut op = "original".to_string();
        let mut schema = Schema { fields: vec![] };
        let mut row = Record {
            ordinal: 1,
            fields: vec![],
            payload: vec![],
        };
        match case {
            "schema" => schema
                .fields
                .push("s".repeat(exact - (46 + ns.len() + 14 + 15 + 12))),
            "row" => row
                .payload
                .resize(exact - (46 + ns.len() + 30 + 12 + 20), 1),
            "posting" => {
                schema.fields.push("tag".into());
                row.fields.push(vec![vec![]]);
                op = "p".repeat(exact - (46 + ns.len() + 42 + 12 + 8));
            }
            _ => unreachable!(),
        }
        let collection = Collection::new("small", schema).unwrap();
        let wire = Arc::new(Mutex::new(PublicationWire::default()));
        let mut c = local_client(store.clone(), pages.clone(), 1024, Some(wire.clone()));
        let result = if case == "schema" {
            collection.declare(&mut c, &ns, &op, Ack::Os, deadline())
        } else {
            collection
                .append(&mut c, &ns, &row, &op, Ack::Os, deadline())
                .map(|_| ())
        };
        if exact == 1024 {
            result.unwrap();
            assert_eq!(mutation_frames(&wire).into_iter().max(), Some(exact));
        } else {
            assert!(matches!(
                result,
                Err(Error::Client(ClientError::Invalid("frame limit")))
            ));
            assert_eq!(wire.lock().unwrap().requests.len(), 1);
            assert!(store.try_get(&ns, "x/small/schema").unwrap().is_none());
            assert!(store.try_get(&ns, "x/small/head").unwrap().is_none());
        }
    }
}

#[test]
fn indexed_publication_retires_only_unsettled_transport() {
    let (_fixture, store, pages) = fixture("publication-custody");
    let collection = Collection::new("small", Schema { fields: vec![] }).unwrap();
    let row = Record {
        ordinal: 1,
        fields: vec![],
        payload: vec![],
    };
    let outcomes = [
        None,
        Some(Receipt::Loss),
        Some(Receipt::BadProof),
        Some(Receipt::Refusal(16)),
        Some(Receipt::Refusal(17)),
        Some(Receipt::Refusal(8)),
        Some(Receipt::Refusal(13)),
        Some(Receipt::Refusal(14)),
        Some(Receipt::Refusal(19)),
    ];
    for (index, receipt) in outcomes.into_iter().enumerate() {
        let ns = format!("receipt-{index}");
        let wire = Arc::new(Mutex::new(PublicationWire {
            receipt,
            ..Default::default()
        }));
        let mut c = local_client(store.clone(), pages.clone(), MAX_FRAME, Some(wire.clone()));
        let result = collection.append(&mut c, &ns, &row, "original", Ack::Flush, deadline());
        let uncertain = matches!(
            receipt,
            Some(Receipt::Loss | Receipt::BadProof | Receipt::Refusal(16 | 17))
        );
        assert_eq!(mutation_frames(&wire).len(), 4);
        assert_eq!(wire.lock().unwrap().drops, usize::from(uncertain));
        assert_eq!(c.is_usable(), !uncertain);
        let head = store.try_get(&ns, "x/small/head").unwrap().unwrap().bytes;
        assert_eq!(
            &head[6..],
            &u64::from(!matches!(receipt, Some(Receipt::Refusal(_)))).to_le_bytes()
        );
        if uncertain {
            let Err(Error::Client(ClientError::Uncertain {
                epoch,
                operation_id,
                cause,
            })) = result
            else {
                panic!("lost original uncertainty: {result:?}");
            };
            assert_eq!(operation_id, "original:commit");
            assert_eq!(epoch, c.negotiated().epoch);
            match (receipt, *cause) {
                (Some(Receipt::Loss), ClientError::Transport(e)) => {
                    assert_eq!(e.kind(), io::ErrorKind::ConnectionReset);
                    assert_eq!(e.to_string(), "authored head receipt loss");
                }
                (
                    Some(Receipt::BadProof),
                    ClientError::Protocol("mutation achieved acknowledgement"),
                ) => (),
                (Some(Receipt::Refusal(code)), ClientError::Refused(r)) => assert_eq!(r.code, code),
                (mode, cause) => panic!("unexpected cause for {mode:?}: {cause:?}"),
            }
            assert!(matches!(
                c.get(&ns, "x/small/head", "inspect", deadline()),
                Err(ClientError::Invalid(_))
            ));
        } else {
            if let Some(Receipt::Refusal(code)) = receipt {
                assert!(
                    matches!(result, Err(Error::Client(ClientError::Refused(r))) if r.code == code)
                );
            } else {
                assert_eq!(result.unwrap().through, 1);
            }
            assert_eq!(
                c.get(&ns, "x/small/head", "inspect", deadline())
                    .unwrap()
                    .unwrap()
                    .bytes,
                head
            );
        }
    }
}
