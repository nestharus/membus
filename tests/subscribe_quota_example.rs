//! Exercise the example's actual caller composition on private resident state.
#![cfg(windows)]
#[path = "support/child.rs"]
mod child;
#[path = "../examples/subscribe_quota.rs"]
#[allow(dead_code)]
mod example;
use example::quota::{self, Cleanup, End};
use membus::{client::*, pipe_io::Pipe};
use std::{
    fs,
    io::{self, Read, Write},
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
const BUDGET: Duration = Duration::from_secs(5);
fn limits(events: Option<usize>) -> quota::Limits {
    quota::Limits {
        quiet: Duration::from_secs(2),
        budget: BUDGET,
        events,
        until: Some(Instant::now() + Duration::from_secs(8)),
    }
}
fn assert_delivery(line: &str, key: &str, value: &[u8]) {
    assert!(line.contains(&format!("key={key} ")), "wrong key: {line}");
    assert!(
        line.contains(&format!("value=Some({value:?})")),
        "wrong/missing bytes: {line}"
    );
}
struct Fixture {
    root: PathBuf,
    pipe: String,
    listener: String,
    owner: Option<child::Owned>,
    generation: usize,
    store: usize,
    idle_ms: u64,
}
impl Fixture {
    fn new() -> Self {
        Self::with_idle(30_000)
    }
    fn with_idle(idle_ms: u64) -> Self {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = std::env::var_os("MEMBUS_EXAMPLE_TEST_ROOT")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/example-fixtures")
            })
            .join(format!("quota-{}-{stamp}", std::process::id()));
        fs::create_dir_all(&root).unwrap();
        let mut f = Self {
            root,
            pipe: format!(r"\\.\pipe\quota-resident-{stamp}"),
            listener: format!(r"\\.\pipe\quota-listener-{stamp}"),
            owner: None,
            generation: 0,
            store: 0,
            idle_ms,
        };
        f.start();
        f
    }
    fn start(&mut self) {
        self.generation += 1;
        let mut command = Command::new(env!("CARGO_BIN_EXE_membus"));
        command
            .arg(self.root.join(format!("state-{}", self.store)))
            .args([
                "pipe",
                &self.pipe,
                "--heartbeat-ms",
                "0",
                "--flush-ms",
                "0",
                "--idle-ms",
                &self.idle_ms.to_string(),
            ])
            .stdin(Stdio::null())
            .stdout(
                fs::File::create(
                    self.root
                        .join(format!("resident-{}.stdout", self.generation)),
                )
                .unwrap(),
            )
            .stderr(
                fs::File::create(
                    self.root
                        .join(format!("resident-{}.stderr", self.generation)),
                )
                .unwrap(),
            );
        self.owner = Some(child::Owned::spawn(&mut command).unwrap());
    }
    fn stop(&mut self) {
        if let Some(mut owner) = self.owner.take() {
            let (status, stopped) = owner.wait(Duration::ZERO).unwrap();
            println!(
                "resident generation={} pid={} stopped={stopped} waited={status} Job settled",
                self.generation,
                owner.child.id()
            );
        }
    }
    fn client(&self) -> Client<Pipe> {
        quota::connect(std::path::Path::new(&self.pipe), BUDGET).unwrap()
    }
    fn listener(&self) -> ChangeListener {
        ChangeListener::bind(&self.listener, membus::MAX_FRAME).unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.stop();
    }
}
fn put(client: &mut Client<Pipe>, key: &str, value: &[u8], id: &str) -> MutationReply {
    let mut attempt = client
        .prepare(MutationRequest {
            operation_id: id.into(),
            namespace: "quota".into(),
            key: key.into(),
            value: value.into(),
            ack: Ack::Os,
            expected: None,
        })
        .unwrap();
    client
        .mutate(&mut attempt, quota::deadline(BUDGET))
        .unwrap()
}
struct Output {
    bytes: Vec<u8>,
    sent: usize,
    tx: mpsc::Sender<String>,
    fail_change: bool,
    arm: Option<Arc<AtomicBool>>,
    registration_gate: Option<mpsc::Receiver<()>>,
}
impl Output {
    fn channel() -> (Self, mpsc::Receiver<String>) {
        let (tx, rx) = mpsc::channel();
        (
            Self {
                bytes: vec![],
                sent: 0,
                tx,
                fail_change: false,
                arm: None,
                registration_gate: None,
            },
            rx,
        )
    }
}
impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.bytes.extend(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        let text = String::from_utf8(self.bytes[self.sent..].to_vec()).unwrap();
        if text.starts_with("change ") {
            if self.fail_change {
                return Err(io::Error::other("authored handler flush failure"));
            }
            if let Some(arm) = &self.arm {
                arm.store(true, Ordering::SeqCst);
            }
        }
        self.sent = self.bytes.len();
        let registered = text.starts_with("registered ");
        self.tx.send(text).unwrap();
        if registered {
            if let Some(gate) = self.registration_gate.take() {
                gate.recv_timeout(Duration::from_secs(8)).unwrap();
            }
        }
        Ok(())
    }
}
fn line(rx: &mpsc::Receiver<String>) -> String {
    rx.recv_timeout(Duration::from_secs(8)).unwrap()
}

#[test]
fn quiet_delivery_handler_failure_resume_and_restart_gap_use_actual_example() {
    let mut f = Fixture::new();
    let mut writer = f.client();
    let mut cursor = None;
    let mut c = f.client();
    let mut listener = f.listener();
    let mut quiet_limits = limits(Some(1));
    quiet_limits.quiet = Duration::from_millis(25);
    let quiet = quota::session(
        &mut c,
        &mut listener,
        &mut cursor,
        &mut Vec::new(),
        quiet_limits,
    );
    assert_eq!(quiet.end, End::Renew);
    assert_eq!(quiet.quiet_timeouts, 1);
    assert_eq!(quiet.handled, 0);
    assert!(matches!(quiet.cleanup, Cleanup::Confirmed { .. }));
    // Continue the same cursor after actual quiet-time maintenance.
    let mut listener = f.listener();
    let (mut out, rx) = Output::channel();
    let report = std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            quota::session(
                &mut c,
                &mut listener,
                &mut cursor,
                &mut out,
                limits(Some(1)),
            )
        });
        assert!(line(&rx).contains("live"));
        let committed = put(&mut writer, "event/first", b"first", "first");
        let delivered = line(&rx);
        assert_delivery(&delivered, "event/first", b"first");
        assert_eq!(
            delivered
                .split_whitespace()
                .filter(|field| field.starts_with("version="))
                .collect::<Vec<_>>(),
            vec![format!("version={}", committed.version)]
        );
        worker.join().unwrap()
    });
    assert_eq!(report.end, End::Complete);
    let token = match report.cleanup {
        Cleanup::Confirmed { token } => token,
        other => panic!("{other:?}"),
    };
    assert!(
        !writer
            .subscription_status("quota", &token, "settled", quota::deadline(BUDGET))
            .unwrap()
            .active
    );
    let handled_cursor = cursor;
    put(&mut writer, "event/second", b"second", "second");
    drop(c);

    // The example's real handler fails after seeing bytes but before flush.
    let mut c = f.client();
    let mut listener = f.listener();
    let (mut out, rx) = Output::channel();
    out.fail_change = true;
    let failed = quota::session(
        &mut c,
        &mut listener,
        &mut cursor,
        &mut out,
        limits(Some(1)),
    );
    assert!(line(&rx).contains("live"));
    assert!(matches!(failed.end, End::Stop(ref e) if e.contains("authored handler flush failure")));
    assert_eq!(cursor, handled_cursor, "failed work must remain replayable");
    assert!(matches!(failed.cleanup, Cleanup::Confirmed { .. }));
    drop(c);
    let mut c = f.client();
    let mut listener = f.listener();
    let (mut out, rx) = Output::channel();
    let resumed = quota::session(
        &mut c,
        &mut listener,
        &mut cursor,
        &mut out,
        limits(Some(1)),
    );
    assert!(line(&rx).contains("live"));
    assert_delivery(&line(&rx), "event/second", b"second");
    assert_eq!(resumed.end, End::Complete);
    assert_ne!(cursor, handled_cursor);
    drop(c);

    // A connected signal writer is physically killed. Reconnect is distinct
    // from quiet; known cleanup failure keeps its token and original cause.
    let mut c = f.client();
    let mut listener = f.listener();
    let (mut out, rx) = Output::channel();
    let interrupted = std::thread::scope(|scope| {
        let worker = scope
            .spawn(|| quota::session(&mut c, &mut listener, &mut cursor, &mut out, limits(None)));
        assert!(line(&rx).contains("live"));
        put(&mut writer, "event/third", b"third", "third");
        assert_delivery(&line(&rx), "event/third", b"third");
        f.stop();
        worker.join().unwrap()
    });
    assert!(matches!(interrupted.end, End::Reconnect(_)));
    assert!(
        matches!(interrupted.cleanup, Cleanup::KnownUnresolved { token, ref endpoint, ref error }
        if token != [0;16] && endpoint == &f.listener && !error.is_empty())
    );
    let old_cursor = cursor;
    drop(c);
    drop(writer);
    f.start();
    let mut writer = f.client();
    put(&mut writer, "open", b"current-open", "open");
    put(&mut writer, "resets", b"current-resets", "resets");
    let mut c = f.client();
    let mut listener = f.listener();
    let (mut out, rx) = Output::channel();
    let (release, gate) = mpsc::channel();
    out.registration_gate = Some(gate);
    let reconciled = std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            quota::session(
                &mut c,
                &mut listener,
                &mut cursor,
                &mut out,
                limits(Some(1)),
            )
        });
        // This flushed registration is produced before the example enters GET.
        assert!(line(&rx).contains("live"));
        let open = put(&mut writer, "open", b"after-live-open", "open-after-live");
        let resets = put(
            &mut writer,
            "resets",
            b"after-live-resets",
            "resets-after-live",
        );
        put(&mut writer, "event/during-reconcile", b"covered", "during");
        release.send(()).unwrap();
        let current = line(&rx);
        let open = Some(CommittedValue {
            version: open.version,
            ack: Ack::Os,
            bytes: b"after-live-open".to_vec(),
        });
        let resets = Some(CommittedValue {
            version: resets.version,
            ack: Ack::Os,
            bytes: b"after-live-resets".to_vec(),
        });
        assert_eq!(
            current,
            format!("reconcile current-state open={open:?} resets={resets:?}\n")
        );
        assert_delivery(&line(&rx), "event/during-reconcile", b"covered");
        worker.join().unwrap()
    });
    assert_eq!(reconciled.end, End::Complete);
    assert_ne!(cursor, old_cursor);
    assert!(matches!(reconciled.cleanup, Cleanup::Confirmed { .. }));
}

// Fault only the client's reply read, after its actual request write. No fake
// server, response decoder or registration implementation is introduced.
struct LoseReply {
    pipe: Pipe,
    fail: Arc<AtomicBool>,
    registrations: Arc<AtomicUsize>,
}
impl Read for LoseReply {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        if self.fail.swap(false, Ordering::SeqCst) {
            return Err(io::Error::other("authored lost reply"));
        }
        self.pipe.read(bytes)
    }
}
impl Write for LoseReply {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.get(8) == Some(&membus::SUBSCRIBE) {
            self.registrations.fetch_add(1, Ordering::SeqCst);
        }
        self.pipe.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.pipe.flush()
    }
}
impl Transport for LoseReply {
    fn deadline(&mut self, until: Instant) -> io::Result<()> {
        self.pipe.set_deadline(until)
    }
}
fn fault_client(f: &Fixture) -> (Client<LoseReply>, Arc<AtomicBool>, Arc<AtomicUsize>) {
    drop(f.client()); // Wait for the fresh resident through the public connector.
    let fail = Arc::new(AtomicBool::new(false));
    let registrations = Arc::new(AtomicUsize::new(0));
    use std::os::windows::fs::OpenOptionsExt;
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .share_mode(0)
        .custom_flags(0x40000000)
        .open(&f.pipe)
        .unwrap();
    let pipe = Pipe::new(file, 1);
    let mut options = Options::new("quota-fault-control", membus::MAX_FRAME);
    options.subscriptions = true;
    let c = Client::handshake(
        LoseReply {
            pipe,
            fail: fail.clone(),
            registrations: registrations.clone(),
        },
        options,
        quota::deadline(BUDGET).local,
    )
    .unwrap();
    (c, fail, registrations)
}
#[test]
fn unknown_registration_stops_without_retry_and_retains_endpoint() {
    let f = Fixture::new();
    let (mut c, fail, registrations) = fault_client(&f);
    fail.store(true, Ordering::SeqCst);
    let mut listener = f.listener();
    let mut cursor = None;
    let report = quota::session(
        &mut c,
        &mut listener,
        &mut cursor,
        &mut Vec::new(),
        limits(Some(1)),
    );
    assert!(matches!(report.end, End::Stop(_)));
    assert_eq!(registrations.load(Ordering::SeqCst), 1);
    assert_eq!(cursor, None);
    assert!(
        matches!(report.cleanup, Cleanup::UnknownRegistration { ref endpoint, ref error }
        if endpoint == &f.listener && error.contains("authored lost reply"))
    );
    let error = quota::continue_on_original(&report, &mut Some(1)).unwrap_err();
    assert!(
        error.contains("UnknownRegistration")
            && error.contains(&format!("endpoint: {:?}", f.listener))
    );
    assert!(error.contains("authored lost reply"));
    assert!(matches!(
        listener.receive(
            &Subscription {
                token: [0; 16],
                cursor: [0; 16]
            },
            "quota",
            "event/",
            Instant::now()
        ),
        Err(ReceiveError::Closed)
    ));
    // Endpoint can be locally rebound; that is deliberately no remote receipt.
    let _rebound = f.listener();
}
#[test]
fn failed_known_cleanup_retains_identity_even_when_reply_loss_can_hide_effect() {
    let f = Fixture::new();
    let (mut c, fail, registrations) = fault_client(&f);
    let mut writer = f.client();
    let mut listener = f.listener();
    let mut cursor = None;
    let (mut out, rx) = Output::channel();
    out.arm = Some(fail);
    let report = std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            quota::session(
                &mut c,
                &mut listener,
                &mut cursor,
                &mut out,
                limits(Some(1)),
            )
        });
        assert!(line(&rx).contains("live"));
        put(&mut writer, "event/cleanup", b"handled", "cleanup");
        assert_delivery(&line(&rx), "event/cleanup", b"handled");
        worker.join().unwrap()
    });
    assert_eq!(report.end, End::Complete);
    assert_eq!(report.handled, 1);
    assert!(cursor.is_some());
    assert_eq!(registrations.load(Ordering::SeqCst), 1);
    let outer = quota::continue_on_original(&report, &mut Some(1)).unwrap_err();
    assert!(outer.contains("KnownUnresolved") && outer.contains("authored lost reply"));
    assert!(outer.contains(&format!("endpoint: {:?}", f.listener)));
    let token = match report.cleanup {
        Cleanup::KnownUnresolved {
            token,
            endpoint,
            error,
        } => {
            assert_eq!(endpoint, f.listener);
            assert!(error.contains("authored lost reply"));
            token
        }
        other => panic!("{other:?}"),
    };
    // The request can settle remotely despite caller uncertainty; only an
    // independent later status establishes that, not the local endpoint close.
    let until = Instant::now() + BUDGET;
    loop {
        let active = writer
            .subscription_status(
                "quota",
                &token,
                "independent-cleanup",
                quota::deadline(BUDGET),
            )
            .unwrap()
            .active;
        if !active {
            break;
        }
        assert!(
            Instant::now() < until,
            "written unsubscribe did not settle within the observed bound"
        );
        std::thread::sleep(Duration::from_millis(1));
    }
}

#[test]
fn absent_expected_delivery_has_a_finite_session_owner_bound() {
    let f = Fixture::new();
    let mut c = f.client();
    let mut listener = f.listener();
    let mut cursor = None;
    let mut bounds = limits(Some(1));
    bounds.until = Some(Instant::now() + Duration::from_millis(100));
    let report = quota::session(&mut c, &mut listener, &mut cursor, &mut Vec::new(), bounds);
    assert!(matches!(report.end, End::Stop(ref error) if error == "session deadline"));
    assert_eq!(report.handled, 0);
    assert!(report.quiet_timeouts > 0);
    assert!(matches!(report.cleanup, Cleanup::Confirmed { .. }));
}

struct ExampleProcess {
    owner: child::Owned,
    stdout: PathBuf,
    stderr: PathBuf,
}
impl ExampleProcess {
    fn start(f: &Fixture, name: &str, events: usize) -> Self {
        Self::configured(f, name, events, Some(250))
    }
    fn configured(f: &Fixture, name: &str, events: usize, quiet_ms: Option<u64>) -> Self {
        Self::with_cursor(f, name, events, quiet_ms, None)
    }
    fn with_cursor(
        f: &Fixture,
        name: &str,
        events: usize,
        quiet_ms: Option<u64>,
        cursor: Option<[u8; 16]>,
    ) -> Self {
        let image = PathBuf::from(
            std::env::var_os("MEMBUS_QUOTA_EXAMPLE_EXE")
                .expect("explicit separately built example image"),
        );
        assert!(image.is_absolute());
        let stdout = f.root.join(format!("{name}.stdout"));
        let stderr = f.root.join(format!("{name}.stderr"));
        let mut command = Command::new(image);
        command
            .arg(&f.pipe)
            .arg(&f.listener)
            .args(["--events", &events.to_string()])
            .stdin(Stdio::null())
            .stdout(fs::File::create(&stdout).unwrap())
            .stderr(fs::File::create(&stderr).unwrap());
        if let Some(ms) = quiet_ms {
            command.args(["--quiet-ms", &ms.to_string()]);
        }
        if let Some(cursor) = cursor {
            command.args(["--cursor", &quota::hex(&cursor)]);
        }
        Self {
            owner: child::Owned::spawn(&mut command).unwrap(),
            stdout,
            stderr,
        }
    }
    fn output(&mut self, ready: impl Fn(&str) -> bool) -> String {
        let until = Instant::now() + Duration::from_secs(15);
        loop {
            let text = fs::read_to_string(&self.stdout).unwrap();
            if ready(&text) {
                return text;
            }
            if let Some(status) = self.owner.child.try_wait().unwrap() {
                panic!(
                    "example exited before required output: {status}; {}",
                    fs::read_to_string(&self.stderr).unwrap()
                );
            }
            assert!(
                Instant::now() < until,
                "example output deadline; stdout={text}; stderr={}",
                fs::read_to_string(&self.stderr).unwrap()
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }
    fn finish(&mut self, expected: i32) -> (String, String) {
        let (status, stopped) = self.owner.wait(Duration::from_secs(20)).unwrap();
        assert!(!stopped, "example required forced stop");
        assert_eq!(status.code(), Some(expected));
        println!(
            "example pid={} natural_exit={status} directwait=true Jobsettled=true",
            self.owner.child.id()
        );
        (
            fs::read_to_string(&self.stdout).unwrap(),
            fs::read_to_string(&self.stderr).unwrap(),
        )
    }
}
fn printed_bytes(line: &str, field: &str) -> [u8; 16] {
    let text = line
        .split_whitespace()
        .find_map(|word| word.strip_prefix(field))
        .unwrap();
    assert_eq!(text.len(), 32);
    let mut token = [0; 16];
    for (i, byte) in token.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[i * 2..i * 2 + 2], 16).unwrap();
    }
    token
}
fn printed_token(line: &str) -> [u8; 16] {
    printed_bytes(line, "token=")
}

#[test]
#[ignore = "requires MEMBUS_QUOTA_EXAMPLE_EXE selecting a separately built example"]
fn actual_executable_renews_healthy_lifetimes_and_stops_at_store_uncertainty() {
    // A real same-resident terminal registration, before any writer connected.
    // This does not pretend to reproduce the scheduler-dependent overflow path.
    {
        let f = Fixture::new();
        let mut writer = f.client();
        let mut cli = ExampleProcess::start(&f, "terminal", 1);
        let first = cli.output(|text| text.contains("registered token="));
        let token = printed_token(
            first
                .lines()
                .find(|l| l.starts_with("registered "))
                .unwrap(),
        );
        writer
            .unsubscribe(
                "quota",
                &token,
                "independent-terminal",
                quota::deadline(BUDGET),
            )
            .unwrap();
        assert!(
            !writer
                .subscription_status(
                    "quota",
                    &token,
                    "terminal-confirmed",
                    quota::deadline(BUDGET)
                )
                .unwrap()
                .active
        );
        let committed = put(
            &mut writer,
            "event/terminal",
            b"terminal-value",
            "terminal-value",
        );
        cli.output(|text| text.contains("change key=event/terminal "));
        let (stdout, stderr) = cli.finish(0);
        assert!(stdout.matches("registered token=").count() >= 2);
        let delivered = stdout
            .lines()
            .find(|l| l.starts_with("change key=event/terminal "))
            .unwrap();
        assert_delivery(delivered, "event/terminal", b"terminal-value");
        assert_eq!(
            delivered
                .split_whitespace()
                .filter(|field| field.starts_with("version="))
                .collect::<Vec<_>>(),
            vec![format!("version={}", committed.version)]
        );
        assert!(stderr.contains("quiet; continuing through known-registration renewal"));
        assert!(!stderr.contains("unresolved obligation="));
    }
    // Same-epoch continuation must keep the latest HANDLED cursor, not the
    // initial cursor or the later global head from a nonmatching mutation.
    {
        let f = Fixture::new();
        let mut writer = f.client();
        let mut cli = ExampleProcess::start(&f, "cursor", 2);
        cli.output(|text| text.contains("registered token="));
        put(&mut writer, "event/latest", b"latest", "latest");
        let text = cli.output(|text| text.contains("change key=event/latest "));
        let handled = text.lines().find(|l| l.starts_with("change ")).unwrap();
        let expected = printed_bytes(handled, "cursor=");
        let initial = printed_bytes(text.lines().next().unwrap(), "cursor=");
        assert_ne!(initial, expected);
        put(
            &mut writer,
            "nonmatching",
            b"head-advances",
            "head-advances",
        );
        let text = cli.output(|text| {
            let last_change = text.rfind("change key=event/latest ");
            let last_registration = text.rfind("registered token=");
            matches!((last_change, last_registration), (Some(c), Some(r)) if r > c)
        });
        let renewed = text
            .lines()
            .filter(|l| l.starts_with("registered "))
            .last()
            .unwrap();
        assert_eq!(
            printed_bytes(renewed, "cursor="),
            expected,
            "must resume the latest handled sequence"
        );
        put(&mut writer, "event/final", b"final", "final");
        cli.output(|text| text.contains("change key=event/final "));
        let (stdout, _) = cli.finish(0);
        assert_eq!(stdout.matches("change key=").count(), 2);
        assert_delivery(
            stdout
                .lines()
                .find(|l| l.starts_with("change key=event/final "))
                .unwrap(),
            "event/final",
            b"final",
        );
    }
    // Real address reuse, both before a signal writer ever connected and after
    // handled work. Store B has a different directory but equal numeric IDs.
    for after_handled in [false, true] {
        let mut f = Fixture::new();
        let mut cli = ExampleProcess::configured(&f, "replacement", 2, Some(2_000));
        let first = cli.output(|text| text.contains("registered token="));
        let registration = first
            .lines()
            .find(|l| l.starts_with("registered "))
            .unwrap();
        let old_token = printed_token(registration);
        let mut old_cursor = printed_bytes(registration, "cursor=");
        if after_handled {
            let mut writer = f.client();
            put(&mut writer, "event/old", b"old", "old");
            let text = cli.output(|text| text.contains("change key=event/old "));
            old_cursor = printed_bytes(
                text.lines().find(|l| l.starts_with("change ")).unwrap(),
                "cursor=",
            );
        }
        f.stop();
        f.store += 1;
        f.start();
        let mut observer = f.client();
        if after_handled {
            put(
                &mut observer,
                "nonmatching",
                b"different-history",
                "different-history",
            );
        }
        let mut b_listener =
            ChangeListener::bind(&format!("{}-b", f.listener), membus::MAX_FRAME).unwrap();
        let b = observer
            .subscribe_with(
                "quota",
                "event/",
                b_listener.endpoint(),
                SubscribeOptions {
                    include_values: true,
                    resume: None,
                    max_frame: membus::MAX_FRAME,
                },
                "b-registration",
                quota::deadline(BUDGET),
            )
            .unwrap();
        assert_eq!(
            b.token, old_token,
            "must actually encounter colliding tokens"
        );
        assert_eq!(
            b.cursor, old_cursor,
            "must actually encounter equal numeric cursors in different histories"
        );
        let (stdout, stderr) = cli.finish(1);
        assert_eq!(
            stdout.matches("registered token=").count(),
            1,
            "no fresh registration or cursor adoption"
        );
        assert!(!stdout.contains("reconcile current-state"));
        assert!(stderr.contains("automatic continuation stopped"));
        assert!(stderr.contains("KnownUnresolved"));
        assert!(stderr.contains(&format!("token: {old_token:?}")));
        assert!(stderr.contains(&format!("checkpoint={}", quota::hex(&old_cursor))));
        assert!(stderr.contains("error: \""));
        assert!(!stderr.contains("remote cleanup confirmed after reconnect"));
        assert!(
            observer
                .subscription_status("quota", &b.token, "b-still-active", quota::deadline(BUDGET))
                .unwrap()
                .active
        );
        put(&mut observer, "event/b", b"b-unaffected", "b-unaffected");
        let change = b_listener
            .receive(&b, "quota", "event/", Instant::now() + BUDGET)
            .unwrap();
        assert_eq!(change.key, "event/b");
        assert_eq!(change.value, Some(b"b-unaffected".to_vec()));
        observer
            .unsubscribe("quota", &b.token, "b-cleanup", quota::deadline(BUDGET))
            .unwrap();
        b_listener.close();
    }
    // Expired control need not mean a dead resident or cancelled subscription.
    {
        let f = Fixture::with_idle(100);
        let mut cli = ExampleProcess::start(&f, "idle", 1);
        let text = cli.output(|text| text.contains("registered token="));
        let token = printed_token(text.lines().next().unwrap());
        let (_, stderr) = cli.finish(1);
        assert!(
            stderr.contains("KnownUnresolved") && stderr.contains("automatic continuation stopped")
        );
        let mut observer = f.client();
        assert!(
            observer
                .subscription_status("quota", &token, "idle-active", quota::deadline(BUDGET))
                .unwrap()
                .active
        );
        observer
            .unsubscribe("quota", &token, "idle-cleanup", quota::deadline(BUDGET))
            .unwrap();
    }
    // Exercise the actual default 10s quiet interval against a 60s idle resident.
    {
        let f = Fixture::with_idle(60_000);
        let mut cli = ExampleProcess::configured(&f, "default", 1, None);
        cli.output(|text| text.matches("registered token=").count() >= 2);
        let mut writer = f.client();
        put(&mut writer, "event/default", b"default", "default");
        cli.output(|text| text.contains("change key=event/default "));
        let (stdout, stderr) = cli.finish(0);
        assert_delivery(
            stdout
                .lines()
                .find(|l| l.starts_with("change key=event/default "))
                .unwrap(),
            "event/default",
            b"default",
        );
        assert!(stderr.contains("original control connection"));
    }
    // Explicit caller-directed same-store resumption, not automatic reconnect.
    // The fixture establishes state-directory continuity across this restart.
    {
        let mut f = Fixture::new();
        let mut client = f.client();
        let mut old_listener = f.listener();
        let old = client
            .subscribe_with(
                "quota",
                "event/",
                old_listener.endpoint(),
                SubscribeOptions {
                    include_values: true,
                    resume: None,
                    max_frame: membus::MAX_FRAME,
                },
                "before-restart",
                quota::deadline(BUDGET),
            )
            .unwrap();
        client
            .unsubscribe(
                "quota",
                &old.token,
                "before-restart-cleanup",
                quota::deadline(BUDGET),
            )
            .unwrap();
        old_listener.close();
        drop(client);
        f.stop();
        f.start();
        let mut writer = f.client();
        let open = put(&mut writer, "open", b"gap-open", "gap-open");
        let resets = put(&mut writer, "resets", b"gap-resets", "gap-resets");
        let mut cli = ExampleProcess::with_cursor(&f, "gap", 1, Some(250), Some(old.cursor));
        let text = cli.output(|text| text.contains("reconcile current-state"));
        assert!(
            text.find("registered token=").unwrap() < text.find("reconcile current-state").unwrap()
        );
        let expected_open = Some(CommittedValue {
            version: open.version,
            ack: Ack::Os,
            bytes: b"gap-open".to_vec(),
        });
        let expected_resets = Some(CommittedValue {
            version: resets.version,
            ack: Ack::Os,
            bytes: b"gap-resets".to_vec(),
        });
        assert!(text.contains(&format!(
            "reconcile current-state open={expected_open:?} resets={expected_resets:?}"
        )));
        put(&mut writer, "event/after-gap", b"after-gap", "after-gap");
        cli.output(|text| text.contains("change key=event/after-gap "));
        let (stdout, _) = cli.finish(0);
        assert_delivery(
            stdout
                .lines()
                .find(|l| l.starts_with("change key=event/after-gap "))
                .unwrap(),
            "event/after-gap",
            b"after-gap",
        );
    }
}
