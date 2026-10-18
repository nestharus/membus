//! Physical subscription-server encounters on fresh owned Windows pipe/state fixtures.
//! Cargo discovers this test. Set MEMBUS_SUBSCRIPTION_TEST_ROOT to retain its measurements.
//! Rust participants exercise server semantics; Python encoding and automatic reconnect
//! are separate contracts. Logical log growth is not physical-device or power-loss evidence.
#![cfg(windows)]
#[path = "support/child.rs"]
mod child;
use membus::{client::*, pipe_io::Pipe, Cursor};
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    os::windows::{
        fs::OpenOptionsExt,
        io::{AsRawHandle, FromRawHandle},
    },
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

fn deadline() -> Deadline {
    Deadline {
        unix_ms: membus::now_ms() + 5000,
        local: Instant::now() + Duration::from_secs(5),
    }
}
struct Server {
    owner: child::Owned,
    stopped: bool,
}
impl Server {
    fn start(root: &Path, endpoint: &str, generation: u8) -> Self {
        let mut command = Command::new(env!("CARGO_BIN_EXE_membus"));
        command
            .arg(root.join("state"))
            .args([
                "pipe",
                endpoint,
                "--heartbeat-ms",
                "0",
                "--flush-ms",
                "0",
                "--idle-ms",
                "30000",
                "--signal-history",
                "32",
                "--allow-mem",
            ])
            .stdin(Stdio::null())
            .stdout(File::create(root.join(format!("server-{generation}.stdout"))).unwrap())
            .stderr(File::create(root.join(format!("server-{generation}.stderr"))).unwrap());
        Self {
            owner: child::Owned::spawn(&mut command).unwrap(),
            stopped: false,
        }
    }
    fn stop(&mut self) {
        if !self.stopped {
            let (status, forced) = self.owner.wait(Duration::ZERO).unwrap();
            self.stopped = true;
            println!("resident pid={} generation stopped/waited/reaped/Job-empty status={status} forced={forced}", self.owner.child.id());
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
    }
}
fn open(endpoint: &str) -> Pipe {
    let until = deadline().local;
    loop {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(0x40000000)
            .open(endpoint)
        {
            Ok(file) => return Pipe::new(file, 5000),
            Err(e) if matches!(e.raw_os_error(), Some(2 | 231)) && Instant::now() < until => {
                std::thread::sleep(Duration::from_millis(2))
            }
            Err(e) => panic!("fixture connection: {e}"),
        }
    }
}
fn connect(endpoint: &str) -> Client<Pipe> {
    let mut options = Options::new("subscription-server-fixture", membus::MAX_FRAME);
    options.subscriptions = true;
    Client::handshake(open(endpoint), options, deadline().local).unwrap()
}
fn put(c: &mut Client<Pipe>, ns: &str, key: &str, value: &[u8], id: &str) -> MutationReply {
    let mut a = c
        .prepare(MutationRequest {
            operation_id: id.into(),
            namespace: ns.into(),
            key: key.into(),
            value: value.to_vec(),
            ack: Ack::Os,
            expected: None,
        })
        .unwrap();
    let r = c.mutate(&mut a, deadline()).unwrap();
    assert!(r.version > 0);
    assert_eq!(r.achieved, Ack::Os);
    r
}

// One finite fixture connection for operations absent from the public application SDK.
// No reconnection, retry, listener, mutation policy or reusable client is implemented here.
struct Wire {
    pipe: Pipe,
    id: u64,
}
impl Wire {
    fn start(endpoint: &str) -> Self {
        let mut w = Self {
            pipe: open(endpoint),
            id: 0,
        };
        let mut p = (membus::MAX_FRAME as u32).to_le_bytes().to_vec();
        p.push(1); // Actual client memory opt-in; the private resident also admits it.
        membus::lp(&mut p, b"subscription-wire-fixture;subscribe=1");
        let b = w.call(membus::HELLO, &p).unwrap();
        let mut r = Cursor::new(&b);
        assert!(r.u64().unwrap() > 0);
        assert_eq!(r.u32().unwrap() as usize, membus::MAX_FRAME);
        assert_eq!(r.u32().unwrap() as usize, membus::MAX_VALUE);
        assert_eq!(r.u16().unwrap() as usize, membus::MAX_BATCH);
        assert_eq!(r.u8().unwrap(), 1);
        assert!(r.u64().unwrap() > 0);
        assert_eq!(r.u16().unwrap(), 24);
        for &(code, name, transient) in &membus::CODES[..24] {
            assert_eq!(r.u16().unwrap(), code);
            assert_eq!(r.u8().unwrap(), transient as u8);
            assert_eq!(r.text().unwrap(), name);
        }
        assert_eq!(r.u16().unwrap(), 1);
        assert_eq!(r.u32().unwrap() as usize, membus::MAX_FRAME);
        assert_eq!(r.u64().unwrap(), 32);
        assert!(r.u64().unwrap() > 0);
        assert_eq!(r.u16().unwrap(), 64);
        assert_eq!(r.u16().unwrap(), 256);
        assert_eq!(r.u64().unwrap(), 8 * 1024 * 1024);
        assert_eq!(r.u64().unwrap(), 16 * 1024 * 1024);
        assert_eq!(r.u32().unwrap(), 100);
        r.end().unwrap();
        w
    }
    fn call(&mut self, op: u8, payload: &[u8]) -> Result<Vec<u8>, u16> {
        self.id += 1;
        self.pipe.set_deadline(deadline().local).unwrap();
        self.pipe
            .write_all(&membus::frame(op, self.id, payload))
            .unwrap();
        let b = membus::read_frame_limit(&mut self.pipe, membus::MAX_FRAME).unwrap();
        let mut r = Cursor::new(&b);
        assert_eq!(r.u16().unwrap(), 1);
        assert_eq!(r.u16().unwrap(), 0);
        assert_eq!(r.u8().unwrap(), op | 128);
        assert_eq!(r.u64().unwrap(), self.id);
        let code = r.u16().unwrap();
        if code != 0 {
            r.end().unwrap();
            Err(code)
        } else {
            Ok(b[15..].to_vec())
        }
    }
    fn prefix(id: &str, ns: &str) -> Vec<u8> {
        let mut p = deadline().unix_ms.to_le_bytes().to_vec();
        membus::lp(&mut p, id.as_bytes());
        membus::lp(&mut p, ns.as_bytes());
        p
    }
    fn numbers<const N: usize>(&mut self, op: u8) -> [u64; N] {
        let b = self.call(op, &Self::prefix("observe", "quota")).unwrap();
        let mut c = Cursor::new(&b);
        let result = std::array::from_fn(|_| c.u64().unwrap());
        c.end().unwrap();
        result
    }
    fn memory_put(&mut self) -> u64 {
        let mut p = Self::prefix("memory-exclusion", "quota");
        membus::lp(&mut p, b"event/memory");
        p.push(0);
        membus::lp(&mut p, b"memory-only");
        let b = self.call(membus::PUT, &p).unwrap();
        let mut r = Cursor::new(&b);
        let version = r.u64().unwrap();
        assert!(version > 0);
        assert_eq!(r.u8().unwrap(), 0);
        r.end().unwrap();
        version
    }
    fn remove(&mut self, key: &str, version: u64) -> u64 {
        let mut p = Self::prefix("remove-exact", "quota");
        membus::lp(&mut p, key.as_bytes());
        p.push(1);
        p.extend(version.to_le_bytes());
        let b = self.call(membus::REMOVE, &p).unwrap();
        let mut r = Cursor::new(&b);
        let v = r.u64().unwrap();
        assert!(v > version);
        assert_eq!(r.u8().unwrap(), 1);
        r.end().unwrap();
        v
    }
}
fn subscribe(
    c: &mut Client<Pipe>,
    endpoint: &str,
    prefix: &str,
    values: bool,
    resume: Option<[u8; 16]>,
) -> Subscription {
    c.subscribe_with(
        "quota",
        prefix,
        endpoint,
        SubscribeOptions {
            include_values: values,
            resume,
            max_frame: membus::MAX_FRAME,
        },
        "register",
        deadline(),
    )
    .unwrap()
}
fn receive(l: &mut ChangeListener, s: &Subscription, prefix: &str) -> Change {
    l.receive(s, "quota", prefix, deadline().local).unwrap()
}
fn quiet(l: &mut ChangeListener, s: &Subscription, prefix: &str) {
    assert!(matches!(
        l.receive(
            s,
            "quota",
            prefix,
            Instant::now() + Duration::from_millis(25)
        ),
        Err(ReceiveError::QuietTimeout)
    ));
}
fn exact(
    e: &Change,
    s: &Subscription,
    key: &str,
    version: u64,
    value: Option<&[u8]>,
    removed: bool,
) {
    assert_eq!(e.subscription, s.token);
    assert_eq!(e.namespace, "quota");
    assert_eq!(e.key, key);
    assert_eq!(e.version, version);
    assert_eq!(e.ack, Ack::Os);
    assert_eq!(e.tombstone, removed);
    assert_eq!(e.value.as_deref(), value);
    assert_eq!(&e.cursor[..8], &s.cursor[..8]);
}
fn sequence(cursor: &[u8; 16]) -> u64 {
    u64::from_le_bytes(cursor[8..].try_into().unwrap())
}
fn status(
    c: &mut Client<Pipe>,
    s: &Subscription,
    ready: impl Fn(&SubscriptionStatus) -> bool,
) -> SubscriptionStatus {
    let until = deadline().local;
    loop {
        let r = c
            .subscription_status("quota", &s.token, "observe-status", deadline())
            .unwrap();
        if ready(&r) {
            return r;
        }
        assert!(Instant::now() < until, "subscription status timeout: {r:?}");
        std::thread::sleep(Duration::from_millis(2));
    }
}
fn bounded(w: &mut Wire) -> [u64; 9] {
    let u = w.numbers::<9>(membus::SIGNAL_USAGE);
    assert!(u[2] <= u[6] && u[3] <= u[7] && u[4] <= u[8]);
    u
}
fn chunk_bytes(root: &Path) -> u64 {
    let chunks = root.join("state/chunks");
    let mut sum = 0;
    for class in fs::read_dir(chunks).unwrap() {
        let class = class.unwrap();
        if class.file_type().unwrap().is_dir() {
            for f in fs::read_dir(class.path()).unwrap() {
                let f = f.unwrap();
                if f.path().extension().is_some_and(|e| e == "log") {
                    // Windows directory enumeration can retain an open writer's old
                    // length. Query the actual file handle, not its directory entry.
                    sum += File::open(f.path()).unwrap().metadata().unwrap().len();
                }
            }
        }
    }
    sum
}
fn record_bytes(ns: &str, key: &str, id: &str, value: &[u8]) -> u64 {
    // Independent layout arithmetic: MBK1/length/complement/CRC; timestamp/version;
    // ACK/tombstone; three identifiers; expected-version; length-prefixed value.
    (16 + 8 + 8 + 1 + 1 + 4 + ns.len() + 4 + key.len() + 4 + id.len() + 8 + 4 + value.len()) as u64
}
fn writers(c: &mut Client<Pipe>, root: &Path, label: &str) {
    let mut samples = Vec::new();
    for i in 0..200 {
        let begin = Instant::now();
        put(
            c,
            "quota",
            "bench/fixed",
            b"fixed",
            &format!("{label}-{i:03}"),
        );
        samples.push(begin.elapsed().as_secs_f64() * 1000.0);
    }
    fs::write(
        root.join(format!("writer-{label}.tsv")),
        samples
            .iter()
            .map(|x| format!("{x:.9}\n"))
            .collect::<String>(),
    )
    .unwrap();
    let mut sorted = samples;
    sorted.sort_by(f64::total_cmp);
    println!(
        "writer {label} successful={} median_ms={} p99_ms={} max_ms={}",
        sorted.len(),
        (sorted[99] + sorted[100]) / 2.0,
        sorted[197],
        sorted[199]
    );
}
fn unread(endpoint: &str) -> Pipe {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn CreateNamedPipeW(
            n: *const u16,
            a: u32,
            m: u32,
            i: u32,
            o: u32,
            b: u32,
            t: u32,
            s: *mut std::ffi::c_void,
        ) -> *mut std::ffi::c_void;
    }
    let n: Vec<u16> = endpoint.encode_utf16().chain(Some(0)).collect();
    let h = unsafe {
        CreateNamedPipeW(
            n.as_ptr(),
            1 | 0x40000000 | 0x00080000,
            8,
            1,
            4096,
            4096,
            0,
            std::ptr::null_mut(),
        )
    };
    assert_ne!(h as isize, -1);
    Pipe::new(unsafe { File::from_raw_handle(h) }, 5000)
}
#[repr(C)]
#[derive(Default, Debug)]
struct Memory {
    cb: u32,
    faults: u32,
    peak_rss: usize,
    rss: usize,
    peak_paged: usize,
    paged: usize,
    peak_nonpaged: usize,
    nonpaged: usize,
    pagefile: usize,
    peak_pagefile: usize,
    private: usize,
}
fn memory(server: &Server) -> Memory {
    #[link(name = "kernel32")]
    unsafe extern "system" {
        fn K32GetProcessMemoryInfo(h: *mut std::ffi::c_void, p: *mut Memory, n: u32) -> i32;
    }
    let mut m = Memory::default();
    m.cb = std::mem::size_of::<Memory>() as u32;
    assert_ne!(
        unsafe { K32GetProcessMemoryInfo(server.owner.child.as_raw_handle(), &mut m, m.cb) },
        0,
        "owned resident memory observation: {}",
        std::io::Error::last_os_error()
    );
    m
}
fn paced(
    c: &mut Client<Pipe>,
    w: &mut Wire,
    root: &Path,
    l: &mut ChangeListener,
    s: &Subscription,
    n: usize,
    key: &str,
    value: &[u8],
    label: &str,
) -> ([u64; 9], [u64; 9]) {
    let before = w.numbers::<3>(membus::IO_METRICS);
    let history = bounded(w);
    let files = chunk_bytes(root);
    let mut expected = 0;
    for i in 0..n {
        let id = format!("{label}-{i:02}");
        let r = put(c, "quota", key, value, &id);
        exact(&receive(l, s, key), s, key, r.version, Some(value), false);
        expected += record_bytes("quota", key, &id, value);
    }
    status(c, s, |x| {
        x.active && x.written == n as u64 && x.queued == 0 && x.charged_bytes == 0
    });
    let after = w.numbers::<3>(membus::IO_METRICS);
    let retained = bounded(w);
    let end_files = chunk_bytes(root);
    let append = after[0] - before[0];
    let growth = end_files - files;
    assert_eq!(append, expected);
    assert_eq!(growth, expected);
    assert_eq!(after[1] - before[1], n as u64);
    println!("paced {label} n={n} before_io={before:?} after_io={after:?} chunks_before={files} chunks_after={end_files} expected_origin_record_bytes={expected} measured_append_residual={} measured_chunk_residual={} history_before={history:?} history_after={retained:?}",append as i128-expected as i128,growth as i128-expected as i128);
    (history, retained)
}

#[test]
fn complete_private_subscription_server_encounters() {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let base = std::env::var_os("MEMBUS_SUBSCRIPTION_TEST_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/subscription-fixtures")
        });
    let root = base.join(format!("subscription-{}-{stamp}", std::process::id()));
    fs::create_dir_all(&root).unwrap();
    let endpoint = format!(
        r"\\.\pipe\subscription-server-{}-{stamp}",
        std::process::id()
    );
    let sink = |name: &str| {
        format!(
            r"\\.\pipe\subscription-{name}-{}-{stamp}",
            std::process::id()
        )
    };
    println!(
        "private fixture={} endpoint={endpoint} image={}",
        root.display(),
        env!("CARGO_BIN_EXE_membus")
    );
    let mut server = Server::start(&root, &endpoint, 1);
    let mut c = connect(&endpoint);
    let mut w = Wire::start(&endpoint);
    let epoch = c.negotiated().epoch;
    let limits = c.feature_limits().signals.as_ref().unwrap().clone();
    assert_eq!(limits.history_items, 32);
    // Same fixed-key writer workload precedes every subscriber and supplies an observed no-subscriber counter control.
    let baseline_io = w.numbers::<3>(membus::IO_METRICS);
    let baseline_files = chunk_bytes(&root);
    writers(&mut c, &root, "baseline");
    let baseline_after = w.numbers::<3>(membus::IO_METRICS);
    let baseline_expected = (0..200)
        .map(|i| {
            record_bytes(
                "quota",
                "bench/fixed",
                &format!("baseline-{i:03}"),
                b"fixed",
            )
        })
        .sum::<u64>();
    assert_eq!(baseline_after[0] - baseline_io[0], baseline_expected);
    assert_eq!(baseline_after[1] - baseline_io[1], 200);
    assert_eq!(chunk_bytes(&root) - baseline_files, baseline_expected);
    println!("baseline no_subscribers_io={baseline_io:?}->{baseline_after:?} expected_record_bytes={baseline_expected}");
    let mut l = ChangeListener::bind(&sink("filter"), membus::MAX_FRAME).unwrap();
    let s = subscribe(&mut c, l.endpoint(), "event/", true, None);
    let initial = s.cursor;
    put(
        &mut c,
        "other",
        "event/ignore",
        b"ignore",
        "other-namespace",
    );
    put(&mut c, "quota", "state/ignore", b"ignore", "other-prefix");
    let head_before = bounded(&mut w);
    let io_before = w.numbers::<3>(membus::IO_METRICS);
    let memory_version = w.memory_put();
    let head_after = bounded(&mut w);
    assert_eq!(head_after[0], head_before[0]);
    assert_eq!(w.numbers::<3>(membus::IO_METRICS), io_before);
    quiet(&mut l, &s, "event/");
    assert_eq!(status(&mut c, &s, |x| x.active).written, 0);
    println!("successful memory ACK0 version={memory_version} no publication head={head_before:?}->{head_after:?}");
    let payload = b"{\"kind\":\"reopening\"}";
    let mut a = c
        .prepare(MutationRequest {
            operation_id: "reopen-1".into(),
            namespace: "quota".into(),
            key: "event/reopen".into(),
            value: payload.to_vec(),
            ack: Ack::Os,
            expected: None,
        })
        .unwrap();
    let begin = Instant::now();
    let r = c.mutate(&mut a, deadline()).unwrap();
    let event = receive(&mut l, &s, "event/");
    exact(&event, &s, "event/reopen", r.version, Some(payload), false);
    assert!(begin.elapsed() < Duration::from_secs(1));
    let duplicate_head = bounded(&mut w)[0];
    assert_eq!(c.mutate(&mut a, deadline()).unwrap(), r);
    assert_eq!(bounded(&mut w)[0], duplicate_head);
    quiet(&mut l, &s, "event/");
    assert_eq!(
        status(&mut c, &s, |x| x.active && x.written == 1).written,
        1
    );
    for n in 1..=2 {
        put(
            &mut c,
            "quota",
            &format!("event/handling-{n}"),
            format!("queued-{n}").as_bytes(),
            &format!("handling-{n}"),
        );
    }
    std::thread::sleep(Duration::from_millis(200));
    for n in 1..=2 {
        let e = receive(&mut l, &s, "event/");
        assert_eq!(e.key, format!("event/handling-{n}"));
        assert_eq!(e.value.unwrap(), format!("queued-{n}").as_bytes());
    }
    quiet(&mut l, &s, "event/");
    assert!(status(&mut c, &s, |x| x.active).active);
    let after_quiet = put(
        &mut c,
        "quota",
        "event/after-quiet",
        b"survived",
        "after-quiet",
    );
    let e = receive(&mut l, &s, "event/");
    exact(
        &e,
        &s,
        "event/after-quiet",
        after_quiet.version,
        Some(b"survived"),
        false,
    );
    let saved = e.cursor;
    let old_sink = l.endpoint().to_owned();
    l.close();
    let missed1 = put(
        &mut c,
        "quota",
        "event/disconnected-1",
        b"missed-1",
        "disconnected-1",
    );
    let missed2 = put(
        &mut c,
        "quota",
        "event/disconnected-2",
        b"missed-2",
        "disconnected-2",
    );
    let failed = status(&mut c, &s, |x| !x.active && x.failed > 0);
    println!("disconnect observed={failed:?}");
    let mut resumed_l = ChangeListener::bind(&old_sink, membus::MAX_FRAME).unwrap();
    let resumed = subscribe(&mut c, resumed_l.endpoint(), "event/", true, Some(saved));
    let e1 = receive(&mut resumed_l, &resumed, "event/");
    let e2 = receive(&mut resumed_l, &resumed, "event/");
    exact(
        &e1,
        &resumed,
        "event/disconnected-1",
        missed1.version,
        Some(b"missed-1"),
        false,
    );
    exact(
        &e2,
        &resumed,
        "event/disconnected-2",
        missed2.version,
        Some(b"missed-2"),
        false,
    );
    assert_eq!(sequence(&e1.cursor), sequence(&saved) + 1);
    assert_eq!(sequence(&e2.cursor), sequence(&e1.cursor) + 1);
    quiet(&mut resumed_l, &resumed, "event/");
    c.unsubscribe("quota", &resumed.token, "cancel-resume", deadline())
        .unwrap();
    resumed_l.close();
    let mut key_l = ChangeListener::bind(&sink("key-only"), membus::MAX_FRAME).unwrap();
    let key_s = subscribe(&mut c, key_l.endpoint(), "remove/", false, None);
    let created = put(&mut c, "quota", "remove/x", b"x", "remove-create");
    exact(
        &receive(&mut key_l, &key_s, "remove/"),
        &key_s,
        "remove/x",
        created.version,
        None,
        false,
    );
    let removed = w.remove("remove/x", created.version);
    exact(
        &receive(&mut key_l, &key_s, "remove/"),
        &key_s,
        "remove/x",
        removed,
        None,
        true,
    );
    assert_eq!(
        c.get("quota", "remove/x", "removed-committed", deadline())
            .unwrap(),
        None
    );
    c.unsubscribe("quota", &key_s.token, "cancel-key", deadline())
        .unwrap();
    key_l.close();
    let dead = subscribe(&mut c, &sink("missing"), "bench/", true, None);
    writers(&mut c, &root, "dead");
    let dead_status = status(&mut c, &dead, |x| !x.active && x.failed > 0);
    println!("missing endpoint observed={dead_status:?}");
    let slow_endpoint = sink("unread");
    let mut slow_pipe = unread(&slow_endpoint);
    let slow = subscribe(&mut c, &slow_endpoint, "bench/", true, None);
    put(&mut c, "quota", "bench/prime", b"connected", "slow-prime");
    slow_pipe.connect_until(deadline().local).unwrap();
    let exposure = status(&mut c, &slow, |x| x.active && x.written == 1);
    println!("connected unread endpoint exposed before pressure={exposure:?}; zero payload reads performed");
    let large = vec![b'x'; 262144];
    let large_reply = put(&mut c, "quota", "bench/large", &large, "slow-large");
    writers(&mut c, &root, "slow");
    let slow_status = status(&mut c, &slow, |x| !x.active && x.failed + x.dropped > 0);
    assert_eq!(
        c.get("quota", "bench/large", "slow-origin", deadline())
            .unwrap()
            .unwrap()
            .version,
        large_reply.version
    );
    println!(
        "connected unread pressure result={slow_status:?} usage={:?}",
        bounded(&mut w)
    );
    drop(slow_pipe);
    for i in 0..33 {
        put(&mut c, "noise", "k", b"noise", &format!("age-{i}"));
    }
    assert!(matches!(
        c.subscribe_with(
            "quota",
            "event/",
            &sink("old-cursor"),
            SubscribeOptions {
                include_values: true,
                resume: Some(initial),
                max_frame: membus::MAX_FRAME
            },
            "old-cursor",
            deadline()
        ),
        Err(Error::Refused(Refusal { code: 23, .. }))
    ));
    println!(
        "global cross-namespace retention old cursor refused23 usage={:?}",
        bounded(&mut w)
    );
    let overflow_endpoint = sink("overflow");
    let mut overflow_pipe = unread(&overflow_endpoint);
    let overflow = subscribe(&mut c, &overflow_endpoint, "overflow/", true, None);
    put(
        &mut c,
        "quota",
        "overflow/prime",
        b"connected",
        "overflow-prime",
    );
    overflow_pipe.connect_until(deadline().local).unwrap();
    status(&mut c, &overflow, |x| x.active && x.written == 1);
    let payload = vec![b'z'; 262144];
    let mut max_queued = 0;
    let mut max_charge = 0;
    let mut last_overflow = 0;
    for i in 0..36 {
        last_overflow = put(
            &mut c,
            "quota",
            "overflow/k",
            &payload,
            &format!("overflow-{i:02}"),
        )
        .version;
        let q = c
            .subscription_status("quota", &overflow.token, "overflow-exposure", deadline())
            .unwrap();
        max_queued = max_queued.max(q.queued);
        max_charge = max_charge.max(q.charged_bytes);
        bounded(&mut w);
    }
    let over = status(&mut c, &overflow, |x| !x.active && x.failed + x.dropped > 0);
    assert!(
        max_queued >= 2 && max_charge >= 2 * 262144,
        "queue pressure was not exposed: {max_queued}/{max_charge}/{over:?}"
    );
    assert!(
        over.dropped > 0,
        "large queue overflow must actually drop admitted queued work: {over:?}"
    );
    assert_eq!(
        c.get("quota", "overflow/k", "overflow-origin", deadline())
            .unwrap()
            .unwrap()
            .version,
        last_overflow
    );
    assert!(over.charged_bytes <= limits.subscription_bytes);
    assert!(max_charge <= limits.subscription_bytes);
    println!("large overflow origin_successes=36 max_queue={max_queued} max_charge={max_charge} final={over:?} usage={:?}",bounded(&mut w));
    drop(overflow_pipe);
    let mut meter_l = ChangeListener::bind(&sink("meter"), membus::MAX_FRAME).unwrap();
    let meter = subscribe(&mut c, meter_l.endpoint(), "meter/", true, None);
    paced(
        &mut c,
        &mut w,
        &root,
        &mut meter_l,
        &meter,
        32,
        "meter/fixed",
        b"meter",
        "meter",
    );
    c.unsubscribe("quota", &meter.token, "cancel-meter", deadline())
        .unwrap();
    meter_l.close();
    drop(c);
    drop(w);
    server.stop();
    let mut restarted = Server::start(&root, &endpoint, 2);
    let mut c = connect(&endpoint);
    let mut w = Wire::start(&endpoint);
    assert_ne!(c.negotiated().epoch, epoch);
    assert_eq!(
        c.get("quota", "event/reopen", "preserved", deadline())
            .unwrap()
            .unwrap()
            .version,
        r.version
    );
    assert_eq!(
        c.get("quota", "overflow/k", "preserved-overflow", deadline())
            .unwrap()
            .unwrap()
            .bytes,
        payload
    );
    assert_eq!(
        c.get("quota", "remove/x", "preserved-remove", deadline())
            .unwrap(),
        None
    );
    let mut fresh_l = ChangeListener::bind(&sink("fresh"), membus::MAX_FRAME).unwrap();
    assert!(matches!(
        c.subscribe_with(
            "quota",
            "event/",
            fresh_l.endpoint(),
            SubscribeOptions {
                include_values: true,
                resume: Some(saved),
                max_frame: membus::MAX_FRAME
            },
            "old-epoch",
            deadline()
        ),
        Err(Error::Refused(Refusal { code: 23, .. }))
    ));
    let fresh = subscribe(&mut c, fresh_l.endpoint(), "event/", true, None);
    let new = put(
        &mut c,
        "quota",
        "event/restarted",
        b"restarted",
        "restarted",
    );
    exact(
        &receive(&mut fresh_l, &fresh, "event/"),
        &fresh,
        "event/restarted",
        new.version,
        Some(b"restarted"),
        false,
    );
    c.unsubscribe("quota", &fresh.token, "cancel-fresh", deadline())
        .unwrap();
    fresh_l.close();
    println!("physical restart epoch={epoch}->{} old-epoch resume refused23; committed state preserved; Rust explicit comparison, not Python automatic EpochChanged",c.negotiated().epoch);
    let mut memory_l = ChangeListener::bind(&sink("memory"), membus::MAX_FRAME).unwrap();
    let memory_s = subscribe(&mut c, memory_l.endpoint(), "memory/", true, None);
    let before = memory(&restarted);
    let (h0, h1) = paced(
        &mut c,
        &mut w,
        &root,
        &mut memory_l,
        &memory_s,
        16,
        "memory/fixed",
        b"memory",
        "memory",
    );
    let after = memory(&restarted);
    assert_eq!(h1[0] - h0[0], 16);
    assert_eq!(h1[2] - h0[2], 16);
    assert_eq!(h1[1], h0[1]);
    assert_eq!(
        h1[3] - h0[3],
        16 * (256 + "quota".len() + "memory/fixed".len() + b"memory".len()) as u64
    );
    println!("fresh16 memory_before={before:?} memory_after={after:?} combined_rss_delta={} combined_private_delta={} charged_history_delta={} (resident/KV/retry/transport/allocator combined; not isolated signal allocation)",
        after.rss as i128-before.rss as i128,after.private as i128-before.private as i128,h1[3]-h0[3]);
    fs::write(
        root.join("fresh-memory.txt"),
        format!(
            "before={before:?}\nafter={after:?}\nhistory_before={h0:?}\nhistory_after={h1:?}\n"
        ),
    )
    .unwrap();
    c.unsubscribe("quota", &memory_s.token, "cancel-memory", deadline())
        .unwrap();
    memory_l.close();
    drop(c);
    drop(w);
    restarted.stop();
    fs::write(root.join("complete.txt"),"Complete selected subscription server encounters passed; Python participant contracts are separate.\n").unwrap();
}
