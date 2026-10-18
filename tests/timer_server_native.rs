//! Private Windows timer server witnesses. Run with `cargo test --test timer_server_native
//! -- --test-threads=1 --nocapture` under a finite owned process Job.
//! MEMBUS_TIMER_TEST_ROOT selects retained fixture output; MEMBUS_TIMER_BINARY optionally
//! selects an explicitly identified resident. Python publisher/forwarder and old-image
//! combinations remain separate producers. Stripe lengths are logical bytes, not device I/O.
#![cfg(windows)]
#[path = "support/child.rs"]
mod child;
use membus::{client::*, pipe_io::Pipe, Cursor};
use std::{
    fs::{self, OpenOptions},
    io::Write,
    os::windows::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
const NS: &str = "quota";
const INTERNAL: &str = "__membus_timers_v1";
fn d(ms: u64) -> Deadline {
    Deadline {
        unix_ms: membus::now_ms() + ms,
        local: Instant::now() + Duration::from_millis(ms),
    }
}
fn fixture(label: &str) -> (PathBuf, String) {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let name = format!("timer-{}-{stamp}-{label}", std::process::id());
    let base = std::env::var_os("MEMBUS_TIMER_TEST_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/timer-fixtures"));
    let root = base.join(&name);
    fs::create_dir_all(&root).unwrap();
    (root, format!(r"\\.\pipe\{name}"))
}
struct Server {
    owner: child::Owned,
    stopped: bool,
}
impl Server {
    fn start(root: &Path, pipe: &str, label: &str, cap: Option<usize>) -> Self {
        let image = std::env::var_os("MEMBUS_TIMER_BINARY")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_membus")));
        let mut cmd = Command::new(image);
        cmd.arg(root.join("state"))
            .arg("pipe")
            .arg(pipe)
            .args([
                "--heartbeat-ms",
                "0",
                "--flush-ms",
                "0",
                "--idle-ms",
                "5000",
            ])
            .stdin(Stdio::null())
            .stdout(fs::File::create(root.join(format!("{label}.stdout"))).unwrap())
            .stderr(fs::File::create(root.join(format!("{label}.stderr"))).unwrap());
        if let Some(n) = cap {
            cmd.args(["--dedup-cap", &n.to_string()]);
        }
        Self {
            owner: child::Owned::spawn(&mut cmd).unwrap(),
            stopped: false,
        }
    }
    fn kill_wait(&mut self) {
        if !self.stopped {
            let (status, forced) = self.owner.wait(Duration::ZERO).unwrap();
            self.stopped = true;
            assert!(
                forced && !status.success(),
                "expected physical private resident kill"
            );
            println!(
                "private resident pid={} killed, directly waited/reaped, Job empty: {status}",
                self.owner.child.id()
            );
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.kill_wait();
    }
}
fn connect(pipe: &str) -> Client<Pipe> {
    let mut options = Options::new("timer-server-witness", membus::MAX_FRAME);
    options.scans = true;
    options.subscriptions = true;
    Client::connect_pipe_with_features(
        Path::new(pipe),
        options,
        AdditionalFeatures { timers: true },
        d(5000).local,
    )
    .unwrap()
}
fn open_raw(pipe: &str) -> Pipe {
    let until = d(5000).local;
    loop {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(0x40000000)
            .open(pipe)
        {
            Ok(f) => return Pipe::new(f, 5000),
            Err(e) if matches!(e.raw_os_error(), Some(2 | 231)) && Instant::now() < until => {
                std::thread::sleep(Duration::from_millis(2))
            }
            Err(e) => panic!("private pipe open: {e}"),
        }
    }
}
// Finite fixture exchanges only for negotiation and deliberately invalid requests.
fn exchange(pipe: &mut Pipe, opcode: u8, id: u64, payload: &[u8]) -> (u16, Vec<u8>) {
    pipe.set_deadline(d(2000).local).unwrap();
    pipe.write_all(&membus::frame(opcode, id, payload)).unwrap();
    let b = membus::read_frame_limit(pipe, membus::MAX_FRAME).unwrap();
    let mut c = Cursor::new(&b);
    assert_eq!(
        (
            c.u16().unwrap(),
            c.u16().unwrap(),
            c.u8().unwrap(),
            c.u64().unwrap()
        ),
        (1, 0, opcode | 128, id)
    );
    let code = c.u16().unwrap();
    if code != 0 {
        c.end().unwrap();
    }
    (code, b[15..].to_vec())
}
fn prefix(ns: &str, op: &str) -> Vec<u8> {
    let mut p = d(2000).unix_ms.to_le_bytes().to_vec();
    membus::lp(&mut p, op.as_bytes());
    membus::lp(&mut p, ns.as_bytes());
    p
}
fn hello(pipe: &str, identity: &str, codes: &[u16], extension: usize) -> Pipe {
    let mut raw = open_raw(pipe);
    let mut p = (membus::MAX_FRAME as u32).to_le_bytes().to_vec();
    p.push(0);
    membus::lp(&mut p, identity.as_bytes());
    let (code, b) = exchange(&mut raw, membus::HELLO, 1, &p);
    assert_eq!(code, 0);
    let mut c = Cursor::new(&b);
    assert_ne!(c.u64().unwrap(), 0);
    assert_eq!(c.u32().unwrap() as usize, membus::MAX_FRAME);
    assert_eq!(c.u32().unwrap() as usize, membus::MAX_VALUE);
    assert_eq!(c.u16().unwrap() as usize, membus::MAX_BATCH);
    assert_eq!(c.u8().unwrap(), 0);
    assert!(c.u64().unwrap() > 0);
    assert_eq!(c.u16().unwrap() as usize, codes.len());
    let mut vocabulary_bytes = 0;
    for expected in codes {
        let code = c.u16().unwrap();
        let transient = c.u8().unwrap();
        let name = c.text().unwrap();
        let declared = membus::CODES.iter().find(|r| r.0 == *expected).unwrap();
        assert_eq!(
            (code, name.as_str(), transient),
            (declared.0, declared.1, declared.2 as u8)
        );
        vocabulary_bytes += 7 + name.len();
    }
    assert_eq!(b.len(), 29 + vocabulary_bytes + extension);
    if identity.contains("subscribe=1") {
        assert_eq!(c.u16().unwrap(), 1);
        assert!(c.u32().unwrap() >= 1024);
        assert!(c.u64().unwrap() > 0);
        assert!(c.u64().unwrap() > 0);
        assert!(c.u16().unwrap() > 0);
        assert!(c.u16().unwrap() > 0);
        assert!(c.u64().unwrap() > 0);
        assert!(c.u64().unwrap() > 0);
        assert!(c.u32().unwrap() > 0);
    }
    if identity.contains("timers=1") {
        assert_eq!(c.u16().unwrap(), 1);
        assert_eq!(c.u32().unwrap(), 4096);
        assert_eq!(c.u64().unwrap(), 8 * 1024 * 1024);
        assert_eq!(c.u32().unwrap(), 65536);
        assert_eq!(c.u64().unwrap(), 100);
        assert_eq!(c.u64().unwrap(), 86_400_000);
    }
    c.end().unwrap();
    println!("HELLO identity={identity:?} exact_codes={codes:?} extension_bytes={extension}");
    raw
}
fn negotiation_and_invalid(c: &Client<Pipe>, pipe: &str, endpoint: &str) {
    for (identity, count, extension) in [
        ("python", 19, 0),
        ("python;scans=1", 22, 0),
        ("python;subscribe=1;scans=1", 24, 46),
        ("python;timers=1", 23, 34),
        ("python;timers=1;subscribe=1;scans=1", 25, 80),
    ] {
        let codes: Vec<u16> = if count == 23 {
            (1..=22).chain([25]).collect()
        } else {
            (1..=count).collect()
        };
        let mut raw = hello(pipe, identity, &codes, extension);
        let mut p = prefix(NS, "unnegotiated");
        membus::lp(&mut p, b"absent");
        let (code, body) = exchange(&mut raw, membus::TIMER_STATUS, 2, &p);
        if identity.contains("timers=1") {
            assert_eq!((code, body), (0, vec![0]));
            if !identity.contains("subscribe=1") {
                assert_eq!(
                    exchange(&mut raw, membus::SIGNAL_USAGE, 3, &prefix(NS, "no-signals")).0,
                    2
                );
            }
        } else {
            assert_eq!(code, 2);
        }
    }
    let codes: Vec<_> = (1..=25).collect();
    let mut raw = hello(pipe, "raw;timers=1;subscribe=1;scans=1", &codes, 80);
    let mut get = prefix(INTERNAL, "reserved-get");
    membus::lp(&mut get, b"x");
    let mut put = prefix(INTERNAL, "reserved-put");
    membus::lp(&mut put, b"x");
    put.push(1);
    membus::lp(&mut put, b"x");
    let mut sub = prefix(INTERNAL, "reserved-sub");
    membus::lp(&mut sub, b"");
    membus::lp(&mut sub, endpoint.as_bytes());
    sub.push(1);
    membus::lp(&mut sub, b"");
    membus::lp(&mut sub, b"membus.change/1");
    sub.extend(1024u32.to_le_bytes());
    for (id, opcode, p) in [
        (4, membus::GET, get),
        (5, membus::PUT, put),
        (6, membus::SUBSCRIBE, sub),
    ] {
        assert_eq!(exchange(&mut raw, opcode, id, &p).0, 3);
    }
    for (n, period, low, high, size, expected_code) in [
        (0, 99, 0, 0, 0, 3),
        (1, 100, -50, 50, 0, 3),
        (2, 0, 5, -5, 0, 3),
        (3, 0, 0, 0, 65537, 4),
    ] {
        let mut request = schedule(&format!("bad-{n}"), membus::now_ms() + 1000, period, b"");
        request.jitter_low_ms = low;
        request.jitter_high_ms = high;
        request.payload = vec![b'x'; size];
        assert!(
            matches!(c.prepare_timer_set(request.clone()), Err(Error::Invalid(_))),
            "local boundary {n}"
        );
        let mut p = prefix(NS, &request.operation_id);
        membus::lp(&mut p, request.timer_id.as_bytes());
        p.extend(0u64.to_le_bytes());
        membus::lp(&mut p, request.key.as_bytes());
        for v in [request.anchor_ms, period, low as u64, high as u64] {
            p.extend(v.to_le_bytes());
        }
        membus::lp(&mut p, &request.payload);
        p.push(1);
        assert_eq!(
            exchange(&mut raw, membus::TIMER_SET, 10 + n, &p).0,
            expected_code,
            "actual server boundary {n}"
        );
        println!("invalid schedule {n}: separate local Invalid and actual server refusal {expected_code}");
    }
}
fn schedule(id: &str, anchor: u64, period: u64, payload: &[u8]) -> TimerSetRequest {
    TimerSetRequest {
        operation_id: format!("set-{id}"),
        namespace: NS.into(),
        timer_id: id.into(),
        expected: TimerExpectation::Absent,
        key: format!("timer/{id}"),
        anchor_ms: anchor,
        period_ms: period,
        jitter_low_ms: 0,
        jitter_high_ms: 0,
        payload: payload.into(),
        ack: Ack::Os,
    }
}
fn set(c: &mut Client<Pipe>, r: TimerSetRequest) -> u64 {
    let mut a = c.prepare_timer_set(r).unwrap();
    c.timer_set(&mut a, d(2000)).unwrap()
}
fn cancel(c: &mut Client<Pipe>, id: &str, generation: u64, op: &str) -> Result<(), Error> {
    let mut a = c
        .prepare_timer_cancel(TimerCancelRequest {
            operation_id: op.into(),
            namespace: NS.into(),
            timer_id: id.into(),
            expected_generation: generation,
        })
        .unwrap();
    c.timer_cancel(&mut a, d(2000))
}
fn status(c: &mut Client<Pipe>, id: &str) -> Option<TimerStatus> {
    c.timer_status(NS, id, "status", d(2000)).unwrap()
}
fn settled_status(c: &mut Client<Pipe>, id: &str, index: u64) -> TimerStatus {
    let until = d(2000).local;
    loop {
        let s = status(c, id).unwrap();
        if s.next_index == index + 1 {
            assert_eq!(s.error, 0);
            return s;
        }
        assert!(Instant::now() < until, "progress status not settled: {s:?}");
        std::thread::sleep(Duration::from_millis(2));
    }
}
struct Watcher {
    listener: ChangeListener,
    subscription: Subscription,
    prefix: String,
    received: u64,
}
impl Watcher {
    fn new(c: &mut Client<Pipe>, pipe: &str, suffix: &str, prefix: &str, replay: bool) -> Self {
        let listener =
            ChangeListener::bind(&format!("{pipe}-{suffix}"), membus::MAX_FRAME).unwrap();
        let resume = if replay {
            let mut b = [0; 16];
            b[..8].copy_from_slice(&c.negotiated().epoch.to_le_bytes());
            Some(b)
        } else {
            None
        };
        let subscription = c
            .subscribe_with(
                NS,
                prefix,
                listener.endpoint(),
                SubscribeOptions {
                    include_values: true,
                    resume,
                    max_frame: membus::MAX_FRAME,
                },
                suffix,
                d(2000),
            )
            .unwrap();
        Self {
            listener,
            subscription,
            prefix: prefix.into(),
            received: 0,
        }
    }
    fn receive(&mut self) -> (TimerEnvelope, u64) {
        let e = self
            .listener
            .receive(&self.subscription, NS, &self.prefix, d(3000).local)
            .unwrap();
        let received = membus::now_ms();
        let w = TimerEnvelope::decode(e.value.as_ref().unwrap()).unwrap();
        assert_eq!(e.version, w.generation);
        assert_eq!(e.key, format!("timer/{}", w.timer_id));
        assert_eq!(e.ack, Ack::Os);
        assert!(!e.tombstone);
        self.received += 1;
        println!("timer wake received_ms={received} {w:?}");
        (w, received)
    }
    fn healthy(&self, c: &mut Client<Pipe>) {
        let until = d(2000).local;
        loop {
            let s = c
                .subscription_status(NS, &self.subscription.token, "quiet-status", d(2000))
                .unwrap();
            assert!(
                s.active && s.failed == 0 && s.dropped == 0 && s.queued == 0,
                "quietness requires an exposed, healthy subscription: {s:?}"
            );
            assert!(
                s.written <= self.received,
                "unexpected unread delivery: {s:?}"
            );
            if s.written == self.received {
                println!("quiet subscription received={} status={s:?}", self.received);
                return;
            }
            assert!(
                Instant::now() < until,
                "delivery status did not settle: {s:?}"
            );
            std::thread::sleep(Duration::from_millis(2));
        }
    }
    fn quiet(&mut self, c: &mut Client<Pipe>, ms: u64) {
        assert!(
            self.received > 0,
            "quiet outcome requires actual associated delivery exposure"
        );
        self.healthy(c);
        assert!(
            matches!(
                self.listener
                    .receive(&self.subscription, NS, &self.prefix, d(ms).local),
                Err(ReceiveError::QuietTimeout)
            ),
            "unexpected wake or transport failure during quiet interval"
        );
        self.healthy(c);
    }
    fn close(&mut self) {
        self.listener.close();
    }
}
fn pair(a: &mut Watcher, b: &mut Watcher) -> TimerEnvelope {
    let (x, ta) = a.receive();
    let (y, tb) = b.receive();
    assert_eq!(x, y);
    for received in [ta, tb] {
        assert!(
            received >= x.due_ms && received - x.due_ms < 1000,
            "independent due/receipt bound"
        );
    }
    x
}
#[derive(Debug)]
struct Record {
    namespace: String,
    key: String,
    op: String,
    value: Vec<u8>,
    total: usize,
    removed: u8,
    ack: u8,
    version: u64,
}
fn records(state: &Path) -> (Vec<Record>, u64) {
    let mut paths = Vec::new();
    for dir in fs::read_dir(state.join("chunks")).unwrap() {
        let dir = dir.unwrap().path();
        if !dir.is_dir() {
            continue;
        }
        for entry in fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|x| x == "log") {
                paths.push(path);
            }
        }
    }
    let mut rows = Vec::new();
    let mut bytes = 0;
    for path in paths {
        let data = fs::read(&path).unwrap();
        bytes += data.len() as u64;
        let mut at = 0;
        while at < data.len() {
            assert!(data.len() - at >= 16, "incomplete MBK1 header");
            assert_eq!(&data[at..at + 4], b"MBK1");
            let size = u32::from_le_bytes(data[at + 4..at + 8].try_into().unwrap()) as usize;
            assert_eq!(
                u32::from_le_bytes(data[at + 8..at + 12].try_into().unwrap()),
                !(size as u32)
            );
            assert!(size <= membus::MAX_FRAME && at + 16 + size <= data.len());
            let body = &data[at + 16..at + 16 + size];
            assert_eq!(
                membus::crc32(body),
                u32::from_le_bytes(data[at + 12..at + 16].try_into().unwrap())
            );
            let mut c = Cursor::new(body);
            let _stamp = c.u64().unwrap();
            let version = c.u64().unwrap();
            let ack = c.u8().unwrap();
            let removed = c.u8().unwrap();
            let namespace = c.text().unwrap();
            let key = c.text().unwrap();
            let op = c.text().unwrap();
            let _expected = c.u64().unwrap();
            let value = c.bytes(membus::MAX_VALUE).unwrap().to_vec();
            c.end().unwrap();
            rows.push(Record {
                namespace,
                key,
                op,
                value,
                total: 16 + size,
                removed,
                ack,
                version,
            });
            at += 16 + size;
        }
    }
    assert_eq!(rows.iter().map(|r| r.total as u64).sum::<u64>(), bytes);
    (rows, bytes)
}
// Expected identities come from schedule operands and fully associated deliveries;
// actual disk bytes must independently contain exactly one record per expected fire.
fn check_progress(rows: &[Record], expected: &[(u64, TimerEnvelope)]) {
    let progress: Vec<_> = rows
        .iter()
        .filter(|r| r.namespace == INTERNAL && r.key.starts_with("p/"))
        .collect();
    assert_eq!(
        progress.len(),
        expected.len(),
        "one checkpoint per fire, independent of watcher count"
    );
    for (epoch, w) in expected {
        let op = format!("tf-{epoch}-{}-{}", w.generation, w.index);
        let matches: Vec<_> = progress.iter().filter(|r| r.op == op).collect();
        assert_eq!(matches.len(), 1, "exact persisted identity {op}");
        let r = matches[0];
        assert_eq!(r.key, format!("p/{NS}\0{}", w.timer_id));
        assert_eq!((r.ack, r.removed), (1, 0));
        assert!(r.version > w.generation);
        let expected_bytes: Vec<u8> = [
            w.generation,
            w.index,
            w.nominal_ms,
            w.due_ms,
            w.draw_ms as u64,
            w.missed_count,
        ]
        .into_iter()
        .flat_map(u64::to_le_bytes)
        .collect();
        assert_eq!(r.value, expected_bytes);
        assert_eq!(r.value.len(), 48);
        let independent_length = 58 + INTERNAL.len() + r.key.len() + op.len() + 48;
        assert_eq!(r.total, independent_length);
        println!("compact checkpoint op={op} key={:?} bytes={} value={:?}; 1440-fire/day extrapolation={} logical bytes, setup/cancel extra",r.key,r.total,r.value,r.total*1440);
    }
}
fn ordinary(c: &mut Client<Pipe>, key: &str) -> Result<MutationReply, Error> {
    let mut a = c
        .prepare(MutationRequest {
            operation_id: format!("ordinary-{key}"),
            namespace: NS.into(),
            key: key.into(),
            value: b"works".to_vec(),
            ack: Ack::Os,
            expected: None,
        })
        .unwrap();
    c.mutate(&mut a, d(2000))
}
#[test]
fn complete_private_timer_server_cohort() {
    let (root, pipe) = fixture("complete");
    println!("fixture={}", root.display());
    let mut server = Server::start(&root, &pipe, "initial", None);
    let mut c = connect(&pipe);
    let epoch = c.negotiated().epoch;
    let mut a = Watcher::new(&mut c, &pipe, "a", "timer/", false);
    negotiation_and_invalid(&c, &pipe, a.listener.endpoint());
    let limits = c.feature_limits().timers.as_ref().unwrap();
    assert_eq!((limits.identities, limits.minimum_period_ms), (4096, 100));
    let before = records(&root.join("state")).1;
    let due = membus::now_ms() + 500;
    let generation = set(&mut c, schedule("once", due, 0, b"tick"));
    let after_set = records(&root.join("state")).1;
    assert!(membus::now_ms() < due);
    let (w, received) = a.receive();
    assert_eq!(
        (
            w.timer_id.as_str(),
            w.generation,
            w.index,
            w.nominal_ms,
            w.due_ms,
            w.draw_ms,
            w.missed_count,
            w.payload.as_slice()
        ),
        ("once", generation, 0, due, due, 0, 1, &b"tick"[..])
    );
    assert!(received >= due && received - due < 1000);
    let s = settled_status(&mut c, "once", 0);
    assert!(!s.active);
    assert_eq!(s.last.as_ref(), Some(&w));
    let mut expected = vec![(epoch, w)];
    let (rows, after_fire) = records(&root.join("state"));
    check_progress(&rows, &expected);
    let progress = rows.iter().find(|r| r.key.starts_with("p/")).unwrap();
    assert_eq!(after_fire - after_set, progress.total as u64);
    assert!(after_set > before);
    println!(
        "isolated one-off stripe before={before} after_set={after_set} after_fire={after_fire}"
    );
    a.quiet(&mut c, 150);
    let mut b = Watcher::new(&mut c, &pipe, "b", "timer/", false);
    let anchor = membus::now_ms() + 350;
    let mut request = schedule("jitter", anchor, 200, b"periodic");
    request.jitter_low_ms = -30;
    request.jitter_high_ms = 30;
    // Fixed-vector oracle from arbitrary-precision modulo-2^64 arithmetic for
    // the version-one seed/index rejection rules. These are admitted operands,
    // not fields copied from a delivered envelope or a checkpoint.
    let expected_seed = ((membus::crc32(request.operation_id.as_bytes()) as u64) << 32)
        | membus::crc32(format!("{NS}\0{}", request.timer_id).as_bytes()) as u64;
    assert_eq!(expected_seed, 4_762_860_646_524_312_049);
    let expected_draws = [-9, 9, -2, 21, 10, -12, 28, -5, 0, -25, 7, -1];
    let generation = set(&mut c, request);
    assert_eq!(
        status(&mut c, "jitter").unwrap().schedule.seed,
        expected_seed
    );
    for index in 0..12 {
        let w = pair(&mut a, &mut b);
        assert_eq!(
            (
                w.timer_id.as_str(),
                w.generation,
                w.index,
                w.nominal_ms,
                w.missed_count,
                w.payload.as_slice()
            ),
            (
                "jitter",
                generation,
                index,
                anchor + index * 200,
                1,
                &b"periodic"[..]
            )
        );
        assert!((-30..=30).contains(&w.draw_ms));
        let expected_draw = expected_draws[index as usize];
        assert_eq!(w.draw_ms, expected_draw);
        assert_eq!(
            w.due_ms,
            (anchor + index * 200)
                .checked_add_signed(expected_draw)
                .unwrap()
        );
        expected.push((epoch, w));
    }
    cancel(&mut c, "jitter", generation, "cancel-jitter").unwrap();
    assert!(status(&mut c, "jitter").is_none());
    a.quiet(&mut c, 300);
    b.quiet(&mut c, 300);
    let old = set(
        &mut c,
        schedule("replace", membus::now_ms() + 300, 0, b"old"),
    );
    let replacement_anchor = membus::now_ms() + 600;
    let mut replacement = schedule("replace", replacement_anchor, 0, b"new");
    replacement.operation_id = "replace-new".into();
    replacement.expected = TimerExpectation::Generation(old);
    let new = set(&mut c, replacement);
    assert!(new > old);
    assert!(matches!(
        cancel(&mut c, "replace", old, "stale-cancel"),
        Err(Error::Refused(Refusal { code: 12, .. }))
    ));
    let w = pair(&mut a, &mut b);
    assert_eq!(
        (
            w.timer_id.as_str(),
            w.generation,
            w.index,
            w.payload.as_slice()
        ),
        ("replace", new, 0, &b"new"[..])
    );
    assert_eq!(
        (w.nominal_ms, w.due_ms, w.draw_ms),
        (replacement_anchor, replacement_anchor, 0)
    );
    settled_status(&mut c, "replace", 0);
    expected.push((epoch, w));
    let request = schedule("retry", membus::now_ms() + 200, 0, b"retry");
    let mut attempt = c.prepare_timer_set(request).unwrap();
    let one = c.timer_set(&mut attempt, d(2000)).unwrap();
    let w = pair(&mut a, &mut b);
    assert_eq!(
        (w.timer_id.as_str(), w.generation, w.index),
        ("retry", one, 0)
    );
    let completed = settled_status(&mut c, "retry", 0);
    expected.push((epoch, w));
    assert_eq!(c.timer_set(&mut attempt, d(2000)).unwrap(), one);
    let retried = status(&mut c, "retry").unwrap();
    assert!(!retried.active);
    assert_eq!(retried.next_index, completed.next_index);
    assert_eq!(retried.last, completed.last);
    a.quiet(&mut c, 150);
    b.quiet(&mut c, 150);
    let mut replacement = schedule("retry", membus::now_ms() + 10000, 0, b"future");
    replacement.operation_id = "retry-new".into();
    replacement.expected = TimerExpectation::Generation(one);
    let newer = set(&mut c, replacement);
    assert!(matches!(
        c.timer_set(&mut attempt, d(2000)),
        Err(Error::Refused(Refusal { code: 12, .. }))
    ));
    assert_eq!(status(&mut c, "retry").unwrap().generation, newer);
    cancel(&mut c, "retry", newer, "cancel-retry").unwrap();
    let anchor = membus::now_ms() + 350;
    let generation = set(&mut c, schedule("restart", anchor, 1000, b"restart"));
    let w = pair(&mut a, &mut b);
    assert_eq!((w.generation, w.index, w.missed_count), (generation, 0, 1));
    let prior = settled_status(&mut c, "restart", 0);
    expected.push((epoch, w));
    server.kill_wait();
    drop(c);
    a.close();
    b.close();
    std::thread::sleep(Duration::from_millis(3300));
    let restart_before = membus::now_ms();
    let mut restarted = Server::start(&root, &pipe, "restart", None);
    let mut c = connect(&pipe);
    let restart_after = membus::now_ms();
    let new_epoch = c.negotiated().epoch;
    assert_ne!(new_epoch, epoch);
    let mut a = Watcher::new(&mut c, &pipe, "restart-a", "timer/restart", true);
    let mut b = Watcher::new(&mut c, &pipe, "restart-b", "timer/restart", true);
    // Catch-up is intentionally old relative to receipt, so use its scheduling window,
    // not the normal per-due latency bound used by pair().
    let (w, _) = a.receive();
    let (other, _) = b.receive();
    assert_eq!(w, other);
    assert_eq!(
        (w.timer_id.as_str(), w.generation, w.payload.as_slice()),
        ("restart", generation, &b"restart"[..])
    );
    assert!(w.index >= 3);
    assert_eq!(w.missed_count, w.index - prior.next_index + 1);
    assert_eq!(w.nominal_ms, anchor + w.index * 1000);
    assert_eq!(w.due_ms, w.nominal_ms);
    assert_eq!(w.draw_ms, 0);
    assert!(
        w.due_ms <= restart_after && w.due_ms + 1000 > restart_before,
        "catch-up is latest due inside independently observed restart window"
    );
    settled_status(&mut c, "restart", w.index);
    let mut last = w.index;
    expected.push((new_epoch, w));
    for _ in 0..2 {
        let w = pair(&mut a, &mut b);
        assert_eq!(
            (
                w.generation,
                w.index,
                w.nominal_ms,
                w.draw_ms,
                w.missed_count
            ),
            (generation, last + 1, anchor + (last + 1) * 1000, 0, 1)
        );
        assert_eq!(w.payload, b"restart");
        last = w.index;
        settled_status(&mut c, "restart", last);
        expected.push((new_epoch, w));
    }
    cancel(&mut c, "restart", generation, "cancel-restart").unwrap();
    assert!(status(&mut c, "restart").is_none());
    a.quiet(&mut c, 300);
    b.quiet(&mut c, 300);
    assert!(status(&mut c, "jitter").is_none());
    assert!(status(&mut c, "retry").is_none());
    for key in ["once", "jitter", "replace", "retry", "restart"] {
        assert!(c
            .get(NS, &format!("timer/{key}"), "no-event-key", d(2000))
            .unwrap()
            .is_none());
    }
    let (rows, bytes) = records(&root.join("state"));
    check_progress(&rows, &expected);
    println!(
        "complete cohort logical stripe bytes={bytes}; expected fires={}",
        expected.len()
    );
    restarted.kill_wait();
    drop(c);
    a.close();
    b.close();
    fs::write(root.join("completed.txt"),format!("complete private cohort; {} exact progress records; all resident children directly waited, Jobs empty\n",expected.len())).unwrap();
}
#[test]
fn progress_preserves_small_ordinary_dedup_capacity_across_physical_restart() {
    let (root, pipe) = fixture("dedup");
    println!("fixture={}", root.display());
    let mut server = Server::start(&root, &pipe, "initial", Some(3));
    let mut c = connect(&pipe);
    let epoch = c.negotiated().epoch;
    let mut a = Watcher::new(&mut c, &pipe, "dedup-watch", "timer/", false);
    let anchor = membus::now_ms() + 150;
    let generation = set(&mut c, schedule("fixed", anchor, 100, b"small"));
    let mut expected = Vec::new();
    for index in 0..8 {
        let (w, t) = a.receive();
        assert_eq!(
            (
                w.generation,
                w.index,
                w.nominal_ms,
                w.due_ms,
                w.missed_count
            ),
            (
                generation,
                index,
                anchor + index * 100,
                anchor + index * 100,
                1
            )
        );
        assert!(t >= w.due_ms && t - w.due_ms < 1000);
        expected.push((epoch, w));
    }
    cancel(&mut c, "fixed", generation, "cancel-fixed").unwrap();
    a.quiet(&mut c, 150);
    server.kill_wait();
    drop(c);
    a.close();
    let mut reopened = Server::start(&root, &pipe, "reopened", Some(3));
    let mut c = connect(&pipe);
    assert_ne!(c.negotiated().epoch, epoch);
    assert!(status(&mut c, "fixed").is_none());
    let reply = ordinary(&mut c, "ordinary").unwrap();
    assert_eq!(reply.achieved, Ack::Os);
    let got = c.get(NS, "ordinary", "read", d(2000)).unwrap().unwrap();
    assert_eq!((got.version, got.bytes), (reply.version, b"works".to_vec()));
    // Schedule + cancellation + this ordinary write exhaust three slots. Progress
    // records must consume none; a fourth identity is a reached capacity control.
    assert!(matches!(
        ordinary(&mut c, "over-cap"),
        Err(Error::Refused(Refusal { code: 18, .. }))
    ));
    assert!(c.get(NS, "over-cap", "absent", d(2000)).unwrap().is_none());
    assert!(c
        .get(NS, "timer/fixed", "no-event-key", d(2000))
        .unwrap()
        .is_none());
    check_progress(&records(&root.join("state")).0, &expected);
    reopened.kill_wait();
    drop(c);
    fs::write(root.join("completed.txt"),"eight progress records; physical restart; ordinary third slot succeeds, fourth refused18; all resident children directly waited, Jobs empty\n").unwrap();
}
