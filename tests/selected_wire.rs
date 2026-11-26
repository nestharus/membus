//! Independent request encoding and real Store dispatch through a bounded in-memory transport.
use membus::{
    client::{Client, Deadline, Error, Options, Transport},
    selected_view::{AcquisitionBudget, Selection},
    Config, ManualClock, Mutation, Store,
};
use std::{
    collections::VecDeque,
    fs,
    io::{self, Read, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
#[cfg(windows)]
#[path = "support/child.rs"]
mod child;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = PathBuf::from(
            std::env::var_os("MEMBUS_TEST_OUTPUT").unwrap_or_else(|| ".test-output".into()),
        );
        fs::create_dir_all(&root).unwrap();
        Self(fs::canonicalize(root).unwrap().join(format!(
            "selected-wire-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn config() -> Config {
    Config {
        cache_bytes: 0,
        signal_bytes: 0,
        snapshot_max: 1,
        snapshot_ttl_ms: 60_000,
        tick_ms: 3_600_000,
        retire_ms: 3_600_000,
        ..Config::default()
    }
}
fn d() -> Deadline {
    Deadline {
        unix_ms: membus::now_ms() + 60_000,
        local: Instant::now() + Duration::from_secs(10),
    }
}
fn budget() -> AcquisitionBudget {
    AcquisitionBudget {
        max_candidates: 100,
        max_metadata_bytes: 100_000,
        max_retained_bytes: 100_000,
    }
}
fn blob(p: &mut Vec<u8>, b: &[u8]) {
    p.extend((b.len() as u32).to_le_bytes());
    p.extend(b);
}
fn wire(op: u8, id: u64, p: &[u8]) -> Vec<u8> {
    let mut b = (13u32 + p.len() as u32).to_le_bytes().to_vec();
    b.extend([1, 0, 0, 0, op]);
    b.extend(id.to_le_bytes());
    b.extend(p);
    b
}
fn request(
    kind: u8,
    lower: &[u8],
    upper: Option<&[u8]>,
    b: AcquisitionBudget,
    deadline: u64,
) -> Vec<u8> {
    let mut p = deadline.to_le_bytes().to_vec();
    blob(&mut p, b"acquire");
    blob(&mut p, b"ledger");
    p.push(kind);
    blob(&mut p, lower);
    if let Some(u) = upper {
        blob(&mut p, u);
    }
    p.extend(b.max_candidates.to_le_bytes());
    p.extend(b.max_metadata_bytes.to_le_bytes());
    p.extend(b.max_retained_bytes.to_le_bytes());
    wire(membus::SNAPSHOT_OPEN_SELECTED, 2, &p)
}
#[derive(Clone, Copy)]
enum Fault {
    None,
    OldPeer,
    DropReply,
    Short,
    Trailing,
    Epoch,
    Population,
    Work,
    Bytes,
}
struct Local {
    store: Arc<Store>,
    incoming: VecDeque<u8>,
    requests: Arc<Mutex<Vec<Vec<u8>>>>,
    fault: Fault,
}
impl Read for Local {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        let n = b.len().min(self.incoming.len()).min(7);
        for byte in &mut b[..n] {
            *byte = self.incoming.pop_front().unwrap();
        }
        Ok(n)
    }
}
impl Write for Local {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        assert_eq!(
            u32::from_le_bytes(b[..4].try_into().unwrap()) as usize,
            b.len() - 4
        );
        self.requests.lock().unwrap().push(b.to_vec());
        let mut r = if b[8] == membus::SNAPSHOT_OPEN_SELECTED && matches!(self.fault, Fault::OldPeer) {
            let mut r = wire(
                membus::SNAPSHOT_OPEN_SELECTED | 128,
                u64::from_le_bytes(b[9..17].try_into().unwrap()),
                &2u16.to_le_bytes(),
            );
            r[..4].copy_from_slice(&15u32.to_le_bytes());
            r
        } else {
            self.store.dispatch(&b[4..])
        };
        if b[8] == membus::SNAPSHOT_OPEN_SELECTED {
            match self.fault {
                Fault::OldPeer => {
                    r.truncate(19);
                    r[..4].copy_from_slice(&15u32.to_le_bytes());
                    r[17..19].copy_from_slice(&2u16.to_le_bytes());
                }
                Fault::DropReply => {
                    return Ok(b.len());
                }
                Fault::Short => {
                    r.pop();
                    let n = (r.len() - 4) as u32;
                    r[..4].copy_from_slice(&n.to_le_bytes());
                }
                Fault::Trailing => {
                    r.push(0);
                    let n = (r.len() - 4) as u32;
                    r[..4].copy_from_slice(&n.to_le_bytes());
                }
                Fault::Epoch => {
                    r[23] ^= 1;
                }
                Fault::Population => {
                    r[39..47].copy_from_slice(&1000u64.to_le_bytes());
                }
                Fault::Work => {
                    r[47..55].copy_from_slice(&101u64.to_le_bytes());
                }
                Fault::Bytes => {
                    r[63..71].copy_from_slice(&100_001u64.to_le_bytes());
                }
                Fault::None => {}
            }
        }
        self.incoming.extend(r);
        Ok(b.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Transport for Local {
    fn deadline(&mut self, until: Instant) -> io::Result<()> {
        if Instant::now() < until {
            Ok(())
        } else {
            Err(io::ErrorKind::TimedOut.into())
        }
    }
}
fn connect(s: &Arc<Store>, fault: Fault, scans: bool) -> (Client<Local>, Arc<Mutex<Vec<Vec<u8>>>>) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let mut options = Options::new("selected-test", 65_536);
    options.scans = scans;
    let c = Client::handshake(
        Local {
            store: s.clone(),
            incoming: VecDeque::new(),
            requests: requests.clone(),
            fault,
        },
        options,
        d().local,
    )
    .unwrap();
    (c, requests)
}
fn put(s: &Store, key: &str, bytes: &[u8]) {
    assert_eq!(
        s.mutate(Mutation {
            ns: "ledger".into(),
            key: key.into(),
            op_id: format!("seed-{key}"),
            bytes: bytes.to_vec(),
            deadline: s.clock().utc_ms() + 60_000,
            ack: 1,
            expected: None,
            remove: false
        })
        .code,
        0
    );
}

#[test]
fn typed_acquisition_encodes_exact_wire_and_uses_existing_same_token_pages_and_release() {
    let f = Fixture::new();
    let s = Arc::new(Store::open(&f.0, config()).unwrap());
    put(&s, "pick/a", &[0, 255, 10]);
    put(&s, "pick/b", b"");
    put(&s, "z", b"ignored");
    let (mut c, requests) = connect(&s, Fault::None, true);
    let deadline = d();
    let v = c
        .snapshot_open_selected(
            "ledger",
            Selection::Prefix("pick/"),
            budget(),
            "acquire",
            deadline,
        )
        .unwrap();
    assert_eq!(
        requests.lock().unwrap()[1],
        request(0, b"pick/", None, budget(), deadline.unix_ms)
    );
    assert_eq!((v.population, v.candidates), (2, 6));
    let first = c
        .scan_prefix("ledger", "pick/", &v.snapshot, &[], 1, 65_536, "page", d())
        .unwrap();
    assert!(!first.done);
    assert_eq!(first.items[0].1.bytes, [0, 255, 10]);
    let last = c
        .scan_prefix(
            "ledger",
            "pick/",
            &v.snapshot,
            &first.cursor,
            1,
            65_536,
            "last",
            d(),
        )
        .unwrap();
    assert!(last.done);
    assert!(last.items[0].1.bytes.is_empty());
    c.snapshot_release("ledger", &v.snapshot, "release", d())
        .unwrap();
    let range = c
        .snapshot_open_selected(
            "ledger",
            Selection::Range {
                lower: "pick/",
                upper: "q",
            },
            budget(),
            "acquire",
            deadline,
        )
        .unwrap();
    assert_eq!(requests.lock().unwrap().last().unwrap(), &{
        let mut expected = request(1, b"pick/", Some(b"q"), budget(), deadline.unix_ms);
        expected[9..17].copy_from_slice(&6u64.to_le_bytes());
        expected
    });
    assert_eq!(range.population, 2);
    assert!(
        c.scan_range(
            "ledger",
            "pick/",
            "q",
            &range.snapshot,
            &[],
            128,
            65_536,
            "range",
            d()
        )
        .unwrap()
        .done
    );
    c.snapshot_release("ledger", &range.snapshot, "release", d())
        .unwrap();
}

#[test]
fn old_peer_refusal_is_explicit_and_never_triggers_whole_namespace_fallback() {
    let f = Fixture::new();
    let s = Arc::new(Store::open(&f.0, config()).unwrap());
    // Old-peer fixture refuses before dispatching opcode 32, hence no token.
    let (mut c, requests) = connect(&s, Fault::OldPeer, true);
    assert!(
        matches!(c.snapshot_open_selected("ledger",Selection::Prefix("p"),budget(),"acquire",d()),Err(Error::Refused(r)) if r.code==2 && r.name=="unsupported")
    );
    assert!(c.is_usable());
    assert_eq!(requests.lock().unwrap().len(), 2);
    let legacy = c.snapshot_open("ledger", "legacy", d()).unwrap();
    c.snapshot_release("ledger", &legacy, "release", d())
        .unwrap();
    let requests = requests.lock().unwrap();
    assert_eq!(
        requests.iter().map(|b| b[8]).collect::<Vec<_>>(),
        [1, membus::SNAPSHOT_OPEN_SELECTED, 10, 13]
    );
}

#[test]
fn local_validation_and_disabled_scans_perform_no_acquisition_io() {
    let f = Fixture::new();
    let s = Arc::new(Store::open(&f.0, config()).unwrap());
    let (mut c, requests) = connect(&s, Fault::None, false);
    assert!(matches!(
        c.snapshot_open_selected("ledger", Selection::Prefix("p"), budget(), "acquire", d()),
        Err(Error::Invalid(_))
    ));
    assert_eq!(requests.lock().unwrap().len(), 1);
    let (mut c, requests) = connect(&s, Fault::None, true);
    assert!(matches!(
        c.snapshot_open_selected(
            "ledger",
            Selection::Range {
                lower: "z",
                upper: "a"
            },
            budget(),
            "acquire",
            d()
        ),
        Err(Error::Invalid(_))
    ));
    assert!(matches!(
        c.snapshot_open_selected(
            "ledger",
            Selection::Prefix(&"x".repeat(1025)),
            budget(),
            "acquire",
            d()
        ),
        Err(Error::Invalid(_))
    ));
    assert_eq!(requests.lock().unwrap().len(), 1);
}

#[test]
fn malformed_acquisition_success_never_returns_an_empty_population_or_retries() {
    for fault in [
        Fault::Short,
        Fault::Trailing,
        Fault::Epoch,
        Fault::Population,
        Fault::Work,
        Fault::Bytes,
    ] {
        let f = Fixture::new();
        let s = Arc::new(Store::open(&f.0, config()).unwrap());
        put(&s, "p", b"opaque");
        let (mut c, requests) = connect(&s, fault, true);
        assert!(matches!(
            c.snapshot_open_selected("ledger", Selection::Prefix("p"), budget(), "acquire", d()),
            Err(Error::Protocol(_))
        ));
        assert!(!c.is_usable());
        assert_eq!(requests.lock().unwrap().len(), 2);
    }
}

#[test]
fn lost_open_keeps_remote_reservation_until_ttl_and_does_not_deduplicate_op_id() {
    let f = Fixture::new();
    let clock = ManualClock::new(membus::now_ms());
    let s = Arc::new(
        Store::open(
            &f.0,
            Config {
                clock: Some(clock.clone()),
                ..config()
            },
        )
        .unwrap(),
    );
    let (mut c, requests) = connect(&s, Fault::DropReply, true);
    assert!(matches!(
        c.snapshot_open_selected("ledger", Selection::Prefix("p"), budget(), "acquire", d()),
        Err(Error::Transport(_))
    ));
    assert!(!c.is_usable());
    assert_eq!(requests.lock().unwrap().len(), 2);
    let (mut next, _) = connect(&s, Fault::None, true);
    assert!(
        matches!(next.snapshot_open_selected("ledger",Selection::Prefix("p"),budget(),"acquire",d()),Err(Error::Refused(r)) if r.code==22)
    );
    clock.advance(60_000);
    let deadline = Deadline {
        unix_ms: clock_utc(&s) + 60_000,
        ..d()
    };
    let v = next
        .snapshot_open_selected(
            "ledger",
            Selection::Prefix("p"),
            budget(),
            "acquire",
            deadline,
        )
        .unwrap();
    next.snapshot_release("ledger", &v.snapshot, "release", deadline)
        .unwrap();
}
fn clock_utc(s: &Store) -> u64 {
    s.clock().utc_ms()
}

#[test]
fn raw_request_validation_and_reply_budget_refuse_before_open() {
    let f = Fixture::new();
    let s = Store::open(&f.0, config()).unwrap();
    let good = request(0, b"p", None, budget(), d().unix_ms);
    let mut bads = vec![
        request(2, b"p", None, budget(), d().unix_ms),
        request(1, b"z", Some(b"a"), budget(), d().unix_ms),
        request(0, &[255], None, budget(), d().unix_ms),
    ];
    let mut trailing = good.clone();
    trailing.push(0);
    bads.push(trailing);
    let mut short = good.clone();
    short.pop();
    bads.push(short);
    for b in bads {
        let r = s.dispatch(&b[4..]);
        assert_eq!(u16::from_le_bytes(r[17..19].try_into().unwrap()), 3);
    }
    let mut tiny = d().unix_ms.to_le_bytes().to_vec();
    blob(&mut tiny, b"o");
    blob(&mut tiny, b"n");
    tiny.push(0);
    blob(&mut tiny, b"");
    for n in [100u64, 100_000, 100_000] {
        tiny.extend(n.to_le_bytes());
    }
    let tiny = wire(membus::SNAPSHOT_OPEN_SELECTED, 2, &tiny);
    let r = s.dispatch_for_limit(&tiny[4..], false, 66);
    assert_eq!(u16::from_le_bytes(r[17..19].try_into().unwrap()), 4);
    let r = s.dispatch_for_limit(&tiny[4..], false, 67);
    assert_eq!(u16::from_le_bytes(r[17..19].try_into().unwrap()), 0);
    assert_eq!(r.len(), 71);
    let token: &[u8] = &r[23..39];
    s.snapshot_release("n", token).unwrap();
}

#[test]
fn opcode_31_refuses_selected_view_tokens_as_ineligible_for_complete_query() {
    let f = Fixture::new();
    let s = Arc::new(Store::open(&f.0, config()).unwrap());
    put(&s, "pick/a", b"one");
    put(&s, "z", b"two");
    let (mut c, _) = connect(&s, Fault::None, true);
    let view = c
        .snapshot_open_selected(
            "ledger",
            Selection::Prefix("pick/"),
            budget(),
            "acquire",
            d(),
        )
        .unwrap();
    assert!(matches!(
        c.snapshot_require_complete("ledger", &view.snapshot, "require-complete", d()),
        Err(Error::Refused(r)) if r.code == 16
    ));
    c.snapshot_release("ledger", &view.snapshot, "release", d())
        .unwrap();
    let whole = c.snapshot_open("ledger", "whole", d()).unwrap();
    c.snapshot_require_complete("ledger", &whole, "require-complete-whole", d())
        .unwrap();
    c.snapshot_release("ledger", &whole, "release-whole", d())
        .unwrap();
}

#[cfg(windows)]
#[test]
fn package_resident_selected_operation_preserves_capture_across_pipe_mutation() {
    use membus::client::{Ack, MutationRequest};
    use std::process::{Command, Stdio};
    let f = Fixture::new();
    fs::create_dir_all(&f.0).unwrap();
    let endpoint = format!(
        r"\\.\pipe\membus-selected-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    );
    let mut command = Command::new(env!("CARGO_BIN_EXE_membus"));
    command
        .arg(f.0.join("state"))
        .args(["pipe", &endpoint, "--heartbeat-ms", "0", "--flush-ms", "0"])
        .stdin(Stdio::null())
        .stdout(fs::File::create(f.0.join("stdout")).unwrap())
        .stderr(fs::File::create(f.0.join("stderr")).unwrap());
    let mut owner = child::Owned::spawn(&mut command).unwrap();
    let mut options = Options::new("selected-resident", 65_536);
    options.scans = true;
    let mut c = Client::connect_pipe(
        std::path::Path::new(&endpoint),
        options,
        Instant::now() + Duration::from_secs(30),
    )
    .unwrap();
    for (key, value, id) in [
        ("pick", vec![0, 255, 10], "seed-pick"),
        ("z", b"ignored".to_vec(), "seed-z"),
    ] {
        let mut attempt = c
            .prepare(MutationRequest {
                namespace: "ledger".into(),
                key: key.into(),
                value,
                operation_id: id.into(),
                ack: Ack::Os,
                expected: None,
            })
            .unwrap();
        c.mutate(&mut attempt, d()).unwrap();
    }
    let view = c
        .snapshot_open_selected(
            "ledger",
            Selection::Prefix("pick"),
            AcquisitionBudget {
                max_candidates: 4,
                max_metadata_bytes: 546,
                max_retained_bytes: 401,
            },
            "selected",
            d(),
        )
        .unwrap();
    assert_eq!(
        (
            view.population,
            view.candidates,
            view.metadata_bytes,
            view.retained_bytes
        ),
        (1, 4, 546, 401)
    );
    let mut replacement = c
        .prepare(MutationRequest {
            namespace: "ledger".into(),
            key: "pick".into(),
            value: b"newer".to_vec(),
            operation_id: "replace-pick".into(),
            ack: Ack::Os,
            expected: None,
        })
        .unwrap();
    c.mutate(&mut replacement, d()).unwrap();
    let page = c
        .scan_prefix(
            "ledger",
            "pick",
            &view.snapshot,
            &[],
            128,
            65_536,
            "page",
            d(),
        )
        .unwrap();
    assert!(page.done);
    assert_eq!(page.items.len(), 1);
    assert_eq!(page.items[0].1.bytes, [0, 255, 10]);
    c.snapshot_release("ledger", &view.snapshot, "release", d())
        .unwrap();
    drop(c);
    let (status, forced) = owner.wait(Duration::ZERO).unwrap();
    println!(
        "selected package resident directwait=true Jobempty=true exit={status} forced={forced}"
    );
}
