//! Physical public-client contracts on an explicitly source-pinned private resident.
#![cfg(windows)]
#[path = "support/child.rs"]
mod child;
use membus::{client::*, pipe_io::Pipe};
use std::{
    fs,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
static SEQ: AtomicU64 = AtomicU64::new(1);
fn unique() -> String {
    format!(
        "{}-{}-{}",
        std::process::id(),
        membus::now_ms(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}
fn d() -> Deadline {
    Deadline {
        unix_ms: membus::now_ms() + 5000,
        local: Instant::now() + Duration::from_secs(5),
    }
}
fn root() -> PathBuf {
    let p = PathBuf::from(
        std::env::var_os("MEMBUS_FORWARDER_TEST_ROOT").expect("explicit private root"),
    )
    .join(unique());
    fs::create_dir_all(&p).unwrap();
    p
}
struct Server {
    owner: child::Owned,
    pipe: String,
}
impl Server {
    fn start(root: &Path, pipe: String, extra: &[&str]) -> Self {
        let image = PathBuf::from(
            std::env::var_os("MEMBUS_FORWARDER_TEST_SERVER")
                .expect("explicit pinned resident image"),
        );
        assert!(image.is_absolute());
        let mut cmd = Command::new(image);
        cmd.arg(root.join("state"))
            .args([
                "pipe",
                &pipe,
                "--heartbeat-ms",
                "0",
                "--flush-ms",
                "0",
                "--idle-ms",
                "10000",
            ])
            .args(extra)
            .stdin(Stdio::null())
            .stdout(fs::File::create(root.join(format!("{}.stdout", unique()))).unwrap())
            .stderr(fs::File::create(root.join(format!("{}.stderr", unique()))).unwrap());
        let out = Self {
            owner: child::Owned::spawn(&mut cmd).unwrap(),
            pipe,
        };
        let _ = out.connect();
        out
    }
    fn connect(&self) -> Client<Pipe> {
        Client::connect_pipe_with_features(
            Path::new(&self.pipe),
            Options {
                identity: "public-native-test".into(),
                max_frame: membus::MAX_FRAME,
                scans: true,
                subscriptions: true,
            },
            AdditionalFeatures { timers: true },
            d().local,
        )
        .unwrap()
    }
    fn stop(&mut self) {
        let (_, stopped) = self.owner.wait(Duration::ZERO).unwrap();
        assert!(stopped);
        println!("resident directly waited and Job observed empty");
    }
}
fn put(c: &mut Client<Pipe>, key: &str, value: &[u8]) -> MutationReply {
    put_as(c, key, value, Ack::Os)
}
fn put_as(c: &mut Client<Pipe>, key: &str, value: &[u8], ack: Ack) -> MutationReply {
    let mut a = c
        .prepare(MutationRequest {
            operation_id: unique(),
            namespace: "owned".into(),
            key: key.into(),
            value: value.to_vec(),
            ack,
            expected: None,
        })
        .unwrap();
    c.mutate(&mut a, d()).unwrap()
}
fn exact_row(row: &(String, CommittedValue), key: &str, bytes: &[u8], reply: &MutationReply) {
    assert_eq!(row.0, key);
    assert_eq!(row.1.bytes, bytes);
    assert_eq!(row.1.version, reply.version);
    assert_eq!(row.1.ack, reply.achieved);
}
fn listen() -> ChangeListener {
    ChangeListener::bind(
        &format!(r"\\.\pipe\sdk-receive-{}", unique()),
        membus::MAX_FRAME,
    )
    .unwrap()
}
fn sub(
    c: &mut Client<Pipe>,
    l: &ChangeListener,
    prefix: &str,
    resume: Option<[u8; 16]>,
    values: bool,
) -> Result<Subscription, Error> {
    c.subscribe_with(
        "owned",
        prefix,
        l.endpoint(),
        SubscribeOptions {
            include_values: values,
            resume,
            max_frame: membus::MAX_FRAME,
        },
        &unique(),
        d(),
    )
}
#[test]
fn physical_values_resume_floor_gap_restart_and_range_view() {
    let p = root();
    let pipe = format!(r"\\.\pipe\sdk-forward-{}", unique());
    let mut server = Server::start(&p, pipe.clone(), &["--signal-history", "2"]);
    let mut c = server.connect();
    let mut l = listen();
    let s = sub(&mut c, &l, "event/", None, true).unwrap();
    let first = put(&mut c, "event/1", b"original");
    let one = l.receive(&s, "owned", "event/", d().local).unwrap();
    assert_eq!(one.value.as_deref(), Some(&b"original"[..]));
    assert_eq!(one.version, first.version);
    c.unsubscribe("owned", &s.token, &unique(), d()).unwrap();
    l.close();
    let second_bytes = [0, 255, 7, 19];
    let second = put_as(&mut c, "event/2", &second_bytes, Ack::Flush);
    let third = put(&mut c, "event/3", b"third");
    let mut l = listen();
    let s = sub(&mut c, &l, "event/", Some(one.cursor), true).unwrap(); // floor is valid
    for (key, value) in [
        ("event/2", second_bytes.as_slice()),
        ("event/3", b"third".as_slice()),
    ] {
        let v = l.receive(&s, "owned", "event/", d().local).unwrap();
        assert_eq!(v.key, key);
        assert_eq!(v.value.as_deref(), Some(value));
    }
    let mut cursor0 = one.cursor;
    cursor0[8..].fill(0);
    let bad = listen();
    assert!(matches!(
        sub(&mut c, &bad, "event/", Some(cursor0), true),
        Err(Error::Refused(Refusal { code: 23, .. }))
    ));
    let snap = c.snapshot_open("owned", &unique(), d()).unwrap();
    let page = c
        .scan_range(
            "owned",
            "event/1",
            "event0",
            &snap,
            &[],
            1,
            65536,
            &unique(),
            d(),
        )
        .unwrap();
    assert_eq!(page.items.len(), 1);
    exact_row(&page.items[0], "event/1", b"original", &first);
    assert!(!page.done);
    assert!(matches!(
        c.scan_range(
            "owned",
            "event/2",
            "event0",
            &snap,
            &page.cursor,
            1,
            65536,
            &unique(),
            d()
        ),
        Err(Error::Refused(Refusal { code: 21, .. }))
    ));
    let replacement = put_as(&mut c, "event/2", b"replacement", Ack::Os);
    let fourth = put(&mut c, "event/4", b"after-view");
    let page = c
        .scan_range(
            "owned",
            "event/1",
            "event0",
            &snap,
            &page.cursor,
            10,
            65536,
            &unique(),
            d(),
        )
        .unwrap();
    assert_eq!(
        page.items.iter().map(|v| v.0.as_str()).collect::<Vec<_>>(),
        ["event/2", "event/3"]
    );
    assert!(page.done);
    exact_row(&page.items[0], "event/2", &second_bytes, &second);
    exact_row(&page.items[1], "event/3", b"third", &third);
    let fresh = c.snapshot_open("owned", &unique(), d()).unwrap();
    let current = c
        .scan_range(
            "owned",
            "event/1",
            "event0",
            &fresh,
            &[],
            10,
            65536,
            &unique(),
            d(),
        )
        .unwrap();
    assert!(current.done);
    assert_eq!(current.items.len(), 4);
    exact_row(&current.items[0], "event/1", b"original", &first);
    exact_row(&current.items[1], "event/2", b"replacement", &replacement);
    exact_row(&current.items[2], "event/3", b"third", &third);
    exact_row(&current.items[3], "event/4", b"after-view", &fourth);
    c.snapshot_release("owned", &fresh, &unique(), d()).unwrap();
    c.snapshot_release("owned", &snap, &unique(), d()).unwrap();
    assert!(matches!(
        c.scan_range(
            "owned",
            "event/1",
            "event0",
            &snap,
            &[],
            1,
            65536,
            &unique(),
            d()
        ),
        Err(Error::Refused(Refusal { code: 20, .. }))
    ));
    c.unsubscribe("owned", &s.token, &unique(), d()).unwrap();
    l.close();
    let mut omitted = listen();
    let omitted_sub = sub(&mut c, &omitted, "event/", None, false).unwrap();
    put(&mut c, "event/omitted", b"not-included");
    assert!(omitted
        .receive(&omitted_sub, "owned", "event/", d().local)
        .unwrap()
        .value
        .is_none());
    c.unsubscribe("owned", &omitted_sub.token, &unique(), d())
        .unwrap();
    omitted.close();
    drop(c);
    server.stop();
    let mut server = Server::start(&p, pipe, &[]);
    let mut c = server.connect();
    let l = listen();
    assert!(matches!(
        sub(&mut c, &l, "event/", Some(one.cursor), true),
        Err(Error::Refused(Refusal { code: 23, .. }))
    ));
    assert_eq!(
        c.get("owned", "event/4", &unique(), d())
            .unwrap()
            .unwrap()
            .bytes,
        b"after-view"
    );
    drop(c);
    server.stop();
}

/// Loses bytes read from the REAL resident after the intended request is sent.
/// This transport is a fault instrument, never an application endpoint.
struct LoseReply {
    inner: Pipe,
    armed: Arc<AtomicBool>,
    consumed: Arc<AtomicUsize>,
}
impl Read for LoseReply {
    fn read(&mut self, b: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(b)?;
        if n > 0 && self.armed.swap(false, Ordering::SeqCst) {
            self.consumed.fetch_add(n, Ordering::SeqCst);
            return Err(io::Error::new(
                io::ErrorKind::ConnectionReset,
                "authored loss after resident bytes",
            ));
        }
        Ok(n)
    }
}
impl Write for LoseReply {
    fn write(&mut self, b: &[u8]) -> io::Result<usize> {
        self.inner.write(b)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
impl Transport for LoseReply {
    fn deadline(&mut self, until: Instant) -> io::Result<()> {
        self.inner.set_deadline(until)
    }
}
fn lossy(server: &Server) -> (Client<LoseReply>, Arc<AtomicBool>, Arc<AtomicUsize>) {
    use std::os::windows::fs::OpenOptionsExt;
    let f = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(0x40000000)
        .open(&server.pipe)
        .unwrap();
    let armed = Arc::new(AtomicBool::new(false));
    let consumed = Arc::new(AtomicUsize::new(0));
    let client = Client::handshake_with_features(
        LoseReply {
            inner: Pipe::new(f, 5000),
            armed: armed.clone(),
            consumed: consumed.clone(),
        },
        Options::new("actual-loss", membus::MAX_FRAME),
        AdditionalFeatures { timers: true },
        d().local,
    )
    .unwrap();
    (client, armed, consumed)
}
#[test]
fn physical_timer_reply_loss_retains_original_attempt_and_reconciles_without_replacement() {
    let p = root();
    let pipe = format!(r"\\.\pipe\sdk-loss-{}", unique());
    let mut server = Server::start(&p, pipe.clone(), &[]);
    let (mut c, arm, bytes) = lossy(&server);
    let request = TimerSetRequest {
        operation_id: unique(),
        namespace: "owned".into(),
        timer_id: "reopen".into(),
        expected: TimerExpectation::Absent,
        key: "timer/reopen".into(),
        anchor_ms: membus::now_ms() + 60000,
        period_ms: 0,
        jitter_low_ms: 0,
        jitter_high_ms: 0,
        payload: b"original-operation".to_vec(),
        ack: Ack::Flush,
    };
    let mut a = c.prepare_timer_set(request.clone()).unwrap();
    let epoch = a.epoch();
    arm.store(true, Ordering::SeqCst);
    let original = c.timer_set(&mut a, d()).unwrap_err();
    assert!(
        matches!(&original,Error::Uncertain{epoch:e,operation_id,..} if *e==epoch && operation_id==&request.operation_id)
    );
    assert!(bytes.load(Ordering::SeqCst) > 0);
    assert_eq!(a.request(), &request);
    let mut current = server.connect();
    let status = current
        .timer_status("owned", "reopen", &unique(), d())
        .unwrap()
        .unwrap();
    assert_eq!(status.schedule.payload, request.payload);
    assert_eq!(status.schedule.anchor_ms, request.anchor_ms);
    assert_eq!(current.timer_set(&mut a, d()).unwrap(), status.generation); // intentional SAME attempt
    assert!(matches!(original, Error::Uncertain { .. }));
    let (mut cancel_client, arm, bytes) = lossy(&server);
    let cancel_request = TimerCancelRequest {
        operation_id: unique(),
        namespace: "owned".into(),
        timer_id: "reopen".into(),
        expected_generation: status.generation,
    };
    let mut cancel = cancel_client
        .prepare_timer_cancel(cancel_request.clone())
        .unwrap();
    arm.store(true, Ordering::SeqCst);
    let original_cancel = cancel_client.timer_cancel(&mut cancel, d()).unwrap_err();
    assert!(
        matches!(&original_cancel,Error::Uncertain{operation_id,..} if operation_id==&cancel_request.operation_id)
    );
    assert!(bytes.load(Ordering::SeqCst) > 0);
    assert!(current
        .timer_status("owned", "reopen", &unique(), d())
        .unwrap()
        .is_none());
    assert!(matches!(
        current.timer_cancel(&mut cancel, d()),
        Err(Error::Refused(Refusal { code: 25, .. }))
    ));
    assert!(matches!(original_cancel, Error::Uncertain { .. }));
    drop(c);
    drop(cancel_client);
    drop(current);
    server.stop();
    let mut server = Server::start(&p, pipe, &[]);
    let mut changed = server.connect();
    assert!(
        matches!(changed.timer_set(&mut a,d()),Err(Error::EpochChanged{expected,..}) if expected==epoch)
    );
    assert!(changed
        .timer_status("owned", "reopen", &unique(), d())
        .unwrap()
        .is_none());
    drop(changed);
    server.stop();
}
#[test]
fn physical_timer_generation_status_cancel_and_live_only_values() {
    let p = root();
    let mut server = Server::start(
        &p,
        format!(r"\\.\pipe\sdk-timers-{}", unique()),
        &["--signal-history", "0"],
    );
    let mut c = server.connect();
    let mut l = listen();
    let s = sub(&mut c, &l, "timer/", None, true).unwrap();
    assert!(c
        .timer_status("owned", "reopen", &unique(), d())
        .unwrap()
        .is_none());
    let req = TimerSetRequest {
        operation_id: unique(),
        namespace: "owned".into(),
        timer_id: "reopen".into(),
        expected: TimerExpectation::Absent,
        key: "timer/reopen".into(),
        anchor_ms: membus::now_ms() + 150,
        period_ms: 0,
        jitter_low_ms: 0,
        jitter_high_ms: 0,
        payload: br#"{"kind":"tick"}"#.to_vec(),
        ack: Ack::Os,
    };
    let mut a = c.prepare_timer_set(req.clone()).unwrap();
    let generation = c.timer_set(&mut a, d()).unwrap();
    let change = l.receive(&s, "owned", "timer/", d().local).unwrap();
    let w = TimerEnvelope::decode(change.value.as_ref().unwrap()).unwrap();
    assert_eq!(
        (
            change.version,
            w.generation,
            w.index,
            w.draw_ms,
            w.missed_count
        ),
        (generation, generation, 0, 0, 1)
    );
    assert!(c
        .get("owned", "timer/reopen", &unique(), d())
        .unwrap()
        .is_none());
    let st = c
        .timer_status("owned", "reopen", &unique(), d())
        .unwrap()
        .unwrap();
    assert!(!st.active);
    assert_eq!(st.last.as_ref().unwrap(), &w);
    assert_eq!(st.schedule.payload, req.payload);
    assert_eq!(st.schedule.namespace, "owned");
    assert_eq!(st.schedule.period_ms, 0);
    // Same operation does not rewind a completed one-off.
    assert_eq!(c.timer_set(&mut a, d()).unwrap(), generation);
    assert!(
        !c.timer_status("owned", "reopen", &unique(), d())
            .unwrap()
            .unwrap()
            .active
    );
    let mut replacement = req;
    replacement.operation_id = unique();
    replacement.expected = TimerExpectation::Generation(generation);
    replacement.anchor_ms = membus::now_ms() + 30000;
    replacement.period_ms = 60000;
    replacement.jitter_low_ms = -9000;
    replacement.jitter_high_ms = 9000;
    let expected_replacement = replacement.clone();
    let mut b = c.prepare_timer_set(replacement).unwrap();
    let next = c.timer_set(&mut b, d()).unwrap();
    assert!(next > generation);
    let replaced = c
        .timer_status("owned", "reopen", &unique(), d())
        .unwrap()
        .unwrap();
    assert_eq!(replaced.generation, next);
    assert_eq!(replaced.schedule.namespace, expected_replacement.namespace);
    assert_eq!(replaced.schedule.timer_id, expected_replacement.timer_id);
    assert_eq!(replaced.schedule.key, expected_replacement.key);
    assert_eq!(replaced.schedule.anchor_ms, expected_replacement.anchor_ms);
    assert_eq!(replaced.schedule.period_ms, 60000);
    assert_eq!(replaced.schedule.jitter_low_ms, -9000);
    assert_eq!(replaced.schedule.jitter_high_ms, 9000);
    assert_eq!(replaced.schedule.payload, expected_replacement.payload);
    assert!(matches!(
        c.timer_set(&mut a, d()),
        Err(Error::Refused(Refusal { code: 12, .. }))
    ));
    let mut stale = c
        .prepare_timer_cancel(TimerCancelRequest {
            operation_id: unique(),
            namespace: "owned".into(),
            timer_id: "reopen".into(),
            expected_generation: generation,
        })
        .unwrap();
    assert!(matches!(
        c.timer_cancel(&mut stale, d()),
        Err(Error::Refused(Refusal { code: 12, .. }))
    ));
    let mut cancel = c
        .prepare_timer_cancel(TimerCancelRequest {
            operation_id: unique(),
            namespace: "owned".into(),
            timer_id: "reopen".into(),
            expected_generation: next,
        })
        .unwrap();
    c.timer_cancel(&mut cancel, d()).unwrap();
    assert!(c
        .timer_status("owned", "reopen", &unique(), d())
        .unwrap()
        .is_none());
    assert!(matches!(
        c.timer_cancel(&mut cancel, d()),
        Err(Error::Refused(Refusal { code: 25, .. }))
    ));
    c.unsubscribe("owned", &s.token, &unique(), d()).unwrap();
    l.close();
    drop(c);
    server.stop();
}
#[test]
fn physical_listener_quiet_partial_wrong_association_and_endpoint_custody() {
    let mut l = listen();
    assert!(matches!(
        l.receive(
            &Subscription {
                token: [0; 16],
                cursor: [0; 16]
            },
            "owned",
            "event/",
            Instant::now() + Duration::from_millis(20)
        ),
        Err(ReceiveError::QuietTimeout)
    ));
    assert!(ChangeListener::bind(l.endpoint(), 65536).is_err());
    for payload in [vec![1, 0], {
        let mut b = 100u32.to_le_bytes().to_vec();
        b.extend([1; 10]);
        b
    }] {
        let endpoint = l.endpoint().to_owned();
        let (ready, wait) = std::sync::mpsc::channel();
        let (stop, hold) = std::sync::mpsc::channel();
        let join = std::thread::spawn(move || {
            let until = Instant::now() + Duration::from_secs(2);
            let mut f = loop {
                match fs::OpenOptions::new().write(true).open(&endpoint) {
                    Ok(f) => break f,
                    Err(e) if e.raw_os_error() == Some(231) && Instant::now() < until => {
                        std::thread::sleep(Duration::from_millis(1))
                    }
                    Err(e) => panic!("fixture pipe open: {e}"),
                }
            };
            f.write_all(&payload).unwrap();
            ready.send(()).unwrap();
            let _ = hold.recv_timeout(Duration::from_secs(2));
        });
        assert!(matches!(
            l.receive(
                &Subscription {
                    token: [0; 16],
                    cursor: [0; 16]
                },
                "owned",
                "event/",
                Instant::now() + Duration::from_millis(250)
            ),
            Err(ReceiveError::Interrupted(_))
        ));
        wait.recv_timeout(Duration::from_secs(2)).unwrap();
        let _ = stop.send(());
        join.join().unwrap();
    }
    l.cancel();
    assert!(matches!(
        l.receive(
            &Subscription {
                token: [0; 16],
                cursor: [0; 16]
            },
            "owned",
            "",
            d().local
        ),
        Err(ReceiveError::Closed)
    ));
    assert!(ChangeListener::bind(l.endpoint(), 65536).is_ok());
}

#[test]
fn physical_listener_rejects_old_token_wrong_namespace_prefix_and_oversize() {
    fn blob(b: &mut Vec<u8>, v: &[u8]) {
        b.extend((v.len() as u32).to_le_bytes());
        b.extend(v);
    }
    for (token, ns, key, oversize) in [
        ([0; 16], "owned", "event/x", false),
        ([1; 16], "other", "event/x", false),
        ([1; 16], "owned", "state/x", false),
        ([1; 16], "owned", "event/x", true),
    ] {
        let mut l = listen();
        let endpoint = l.endpoint().to_owned();
        let mut body = b"MBC1\x01\x00".to_vec();
        body.extend(token);
        body.extend([0; 16]);
        blob(&mut body, ns.as_bytes());
        blob(&mut body, key.as_bytes());
        body.extend(1u64.to_le_bytes());
        body.extend([1, 0, 0]);
        let mut bytes = if oversize {
            (membus::MAX_FRAME as u32 + 1).to_le_bytes().to_vec()
        } else {
            (body.len() as u32).to_le_bytes().to_vec()
        };
        if !oversize {
            bytes.extend(body);
        }
        let (stop, hold) = std::sync::mpsc::channel();
        let writer = std::thread::spawn(move || {
            let end = Instant::now() + Duration::from_secs(2);
            let mut f = loop {
                match fs::OpenOptions::new().write(true).open(&endpoint) {
                    Ok(f) => break f,
                    Err(e) if e.raw_os_error() == Some(231) && Instant::now() < end => {
                        std::thread::sleep(Duration::from_millis(1))
                    }
                    Err(e) => panic!("fixture open {e}"),
                }
            };
            f.write_all(&bytes).unwrap();
            let _ = hold.recv_timeout(Duration::from_secs(2));
        });
        let result = l.receive(
            &Subscription {
                token: [1; 16],
                cursor: [0; 16],
            },
            "owned",
            "event/",
            d().local,
        );
        assert!(matches!(result, Err(ReceiveError::Protocol(_))));
        let _ = stop.send(());
        writer.join().unwrap();
    }
}
