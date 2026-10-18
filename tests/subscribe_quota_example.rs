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
        atomic::{AtomicBool, AtomicUsize, Ordering},
        mpsc, Arc,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
const BUDGET: Duration = Duration::from_secs(5);
const QUIET: Duration = Duration::from_millis(25);
struct Fixture {
    root: PathBuf,
    pipe: String,
    listener: String,
    owner: Option<child::Owned>,
    generation: usize,
}
impl Fixture {
    fn new() -> Self {
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
        };
        f.start();
        f
    }
    fn start(&mut self) {
        self.generation += 1;
        let mut command = Command::new(env!("CARGO_BIN_EXE_membus"));
        command
            .arg(self.root.join("state"))
            .args([
                "pipe",
                &self.pipe,
                "--heartbeat-ms",
                "0",
                "--flush-ms",
                "0",
                "--idle-ms",
                "30000",
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
    let (mut out, rx) = Output::channel();
    let started = Instant::now();
    let report = std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            quota::session(
                &mut c,
                &mut listener,
                &mut cursor,
                &mut out,
                QUIET,
                BUDGET,
                Some(1),
            )
        });
        assert!(line(&rx).contains("live"));
        // Several real quiet deadlines pass before the sole publication.
        std::thread::sleep(Duration::from_millis(150));
        let committed = put(&mut writer, "event/first", b"first", "first");
        let delivered = line(&rx);
        assert!(
            delivered.contains("key=event/first")
                && delivered.contains(&format!("version={}", committed.version))
        );
        worker.join().unwrap()
    });
    assert!(started.elapsed() >= Duration::from_millis(150));
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
        QUIET,
        BUDGET,
        Some(1),
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
        QUIET,
        BUDGET,
        Some(1),
    );
    assert!(line(&rx).contains("live"));
    assert!(line(&rx).contains("key=event/second"));
    assert_eq!(resumed.end, End::Complete);
    assert_ne!(cursor, handled_cursor);
    drop(c);

    // A connected signal writer is physically killed. Reconnect is distinct
    // from quiet; known cleanup failure keeps its token and original cause.
    let mut c = f.client();
    let mut listener = f.listener();
    let (mut out, rx) = Output::channel();
    let interrupted = std::thread::scope(|scope| {
        let worker = scope.spawn(|| {
            quota::session(
                &mut c,
                &mut listener,
                &mut cursor,
                &mut out,
                QUIET,
                BUDGET,
                None,
            )
        });
        assert!(line(&rx).contains("live"));
        put(&mut writer, "event/third", b"third", "third");
        assert!(line(&rx).contains("key=event/third"));
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
                QUIET,
                BUDGET,
                Some(1),
            )
        });
        // This flushed registration is produced before the example enters GET.
        assert!(line(&rx).contains("live"));
        put(&mut writer, "open", b"after-live-open", "open-after-live");
        put(
            &mut writer,
            "resets",
            b"after-live-resets",
            "resets-after-live",
        );
        put(&mut writer, "event/during-reconcile", b"covered", "during");
        release.send(()).unwrap();
        let current = line(&rx);
        assert!(
            current.contains("reconcile current-state")
                && current.contains(&format!("{:?}", b"after-live-open".to_vec()))
                && current.contains(&format!("{:?}", b"after-live-resets".to_vec()))
        );
        assert!(line(&rx).contains("key=event/during-reconcile"));
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
        QUIET,
        BUDGET,
        Some(1),
    );
    assert!(matches!(report.end, End::Stop(_)));
    assert_eq!(registrations.load(Ordering::SeqCst), 1);
    assert_eq!(cursor, None);
    assert!(
        matches!(report.cleanup, Cleanup::UnknownRegistration { ref endpoint, ref error }
        if endpoint == &f.listener && error.contains("authored lost reply"))
    );
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
                QUIET,
                BUDGET,
                Some(1),
            )
        });
        assert!(line(&rx).contains("live"));
        put(&mut writer, "event/cleanup", b"handled", "cleanup");
        assert!(line(&rx).contains("key=event/cleanup"));
        worker.join().unwrap()
    });
    assert_eq!(report.end, End::Complete);
    assert_eq!(report.handled, 1);
    assert!(cursor.is_some());
    assert_eq!(registrations.load(Ordering::SeqCst), 1);
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
    assert!(
        !writer
            .subscription_status(
                "quota",
                &token,
                "independent-cleanup",
                quota::deadline(BUDGET)
            )
            .unwrap()
            .active
    );
}
