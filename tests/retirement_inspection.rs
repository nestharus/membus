//! Positive native IPC and ordinary-file controls with finite owned child custody.
#![cfg(windows)]
#![deny(clippy::all)]
#[path = "support/child.rs"]
mod child;
use std::{
    fs,
    io::{self, Read},
    path::Path,
    process::{Command, Output, Stdio},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
const CAPTURE_LIMIT: usize = 64 * 1024;
#[derive(Default)]
struct CaptureFlags {
    overflow: AtomicBool,
    failed: AtomicBool,
}
struct Stream(Option<JoinHandle<io::Result<Vec<u8>>>>);
impl Stream {
    fn start(mut source: impl Read + Send + 'static, flags: Arc<CaptureFlags>) -> io::Result<Self> {
        thread::Builder::new()
            .name("inspection-test-capture".into())
            .spawn(move || {
                let mut kept = Vec::new();
                let mut buffer = [0; 4096];
                loop {
                    let count = match source.read(&mut buffer) {
                        Ok(0) => return Ok(kept),
                        Ok(count) => count,
                        Err(error) => {
                            flags.failed.store(true, Ordering::SeqCst);
                            return Err(error);
                        }
                    };
                    let retain = count.min(CAPTURE_LIMIT.saturating_sub(kept.len()));
                    kept.extend_from_slice(&buffer[..retain]);
                    if retain != count {
                        flags.overflow.store(true, Ordering::SeqCst);
                    }
                }
            })
            .map(|join| Self(Some(join)))
    }
    fn finish(&mut self) -> Result<Vec<u8>, String> {
        self.0
            .take()
            .ok_or("capture already collected")?
            .join()
            .map_err(|_| "capture thread panicked".to_string())?
            .map_err(|e| e.to_string())
    }
    fn finish_fallback(&mut self) {
        if let Some(join) = self.0.take() {
            match join.join() {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => eprintln!("inspection fallback capture: {error}"),
                Err(_) => eprintln!("inspection fallback capture thread panicked"),
            }
        }
    }
}
struct Capture {
    owner: Option<child::Owned>,
    flags: Arc<CaptureFlags>,
    stdout: Option<Stream>,
    stderr: Option<Stream>,
    settled: bool,
}
impl Capture {
    fn start(command: &mut Command) -> Result<Self, String> {
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        let flags = Arc::new(CaptureFlags::default());
        let mut this = Self {
            owner: Some(child::Owned::spawn(command).map_err(|e| e.to_string())?),
            flags: flags.clone(),
            stdout: None,
            stderr: None,
            settled: false,
        };
        this.stdout = Some(
            Stream::start(
                this.owner
                    .as_mut()
                    .unwrap()
                    .child
                    .stdout
                    .take()
                    .ok_or("missing stdout")?,
                flags.clone(),
            )
            .map_err(|e| e.to_string())?,
        );
        this.stderr = Some(
            Stream::start(
                this.owner
                    .as_mut()
                    .unwrap()
                    .child
                    .stderr
                    .take()
                    .ok_or("missing stderr")?,
                flags.clone(),
            )
            .map_err(|e| e.to_string())?,
        );
        // The retained owner exists before either capture thread is admitted.
        Ok(this)
    }
    fn run(mut self, until: Instant) -> Result<Output, String> {
        let flags = self.flags.clone();
        let mut failure = None;
        loop {
            if flags.overflow.load(Ordering::SeqCst) || flags.failed.load(Ordering::SeqCst) {
                failure = Some("inspection output overflow or capture failure".to_string());
                break;
            }
            if Instant::now() >= until {
                failure = Some("inspection execution deadline".to_string());
                break;
            }
            match self.owner.as_mut().unwrap().child.try_wait() {
                Ok(Some(_)) => break,
                Ok(None) => thread::sleep(Duration::from_millis(5)),
                Err(e) => {
                    failure = Some(format!("inspection child observation: {e}"));
                    break;
                }
            }
        }
        let settled = self
            .owner
            .as_mut()
            .unwrap()
            .wait(Duration::ZERO)
            .map_err(|e| format!("inspection child settlement: {e}"));
        let (status, forced) = match settled {
            Ok(value) => {
                self.settled = true;
                value
            }
            Err(error) => {
                drop(self.owner.take());
                return Err(format!("{}; {error}", failure.unwrap_or_default()));
            }
        };
        let overflow = flags.overflow.load(Ordering::SeqCst);
        println!("inspection CLI pid={} status={status} forced={forced} overflow={overflow} directwait=true Jobempty=true", self.owner.as_mut().unwrap().child.id());
        let out = self
            .stdout
            .as_mut()
            .ok_or("missing stdout collector")?
            .finish();
        let err = self
            .stderr
            .as_mut()
            .ok_or("missing stderr collector")?
            .finish();
        let stdout =
            out.map_err(|e| format!("{}; stdout capture: {e}", failure.as_deref().unwrap_or("")))?;
        let stderr =
            err.map_err(|e| format!("{}; stderr capture: {e}", failure.as_deref().unwrap_or("")))?;
        if let Some(error) = failure {
            return Err(error);
        }
        // A child may exit before the final buffered bytes are drained. Recheck
        // after both joins so overflow cannot turn into an accepted prefix.
        if flags.overflow.load(Ordering::SeqCst) || forced || flags.failed.load(Ordering::SeqCst) {
            return Err(
                "inspection output incomplete or execution forced; no prefix accepted".into(),
            );
        }
        Ok(Output {
            status,
            stdout,
            stderr,
        })
    }
}
impl Drop for Capture {
    fn drop(&mut self) {
        if let (false, Some(owner)) = (self.settled, self.owner.as_mut()) {
            match owner.wait(Duration::ZERO) {
                Ok((status, forced)) => {
                    self.settled = true;
                    eprintln!("inspection fallback settlement status={status} forced={forced} directwait=true Jobempty=true");
                }
                Err(error) => eprintln!("inspection fallback settlement failed: {error}"),
            }
        }
        // Close the existing kill-on-close Job before joining readers if a
        // settlement error required the guard's own fallback.
        if !self.settled {
            drop(self.owner.take());
        }
        for stream in [&mut self.stdout, &mut self.stderr].into_iter().flatten() {
            stream.finish_fallback();
        }
    }
}
fn invoke(exe: &str, arguments: &[&str], work_until: Instant) -> Result<Output, String> {
    if Instant::now() >= work_until {
        return Err("fixture work deadline before spawn".into());
    }
    let until = work_until.min(Instant::now() + Duration::from_secs(10));
    let mut command = Command::new(exe);
    command.args(arguments);
    // Share the same flags between capture threads and the owner observation loop.
    Capture::start(&mut command)?.run(until)
}
#[test]
fn capture_drains_but_refuses_overflow_after_child_completion() {
    let flags = Arc::new(CaptureFlags::default());
    let mut stream = Stream::start(
        io::Cursor::new(vec![0x6a; CAPTURE_LIMIT + 4097]),
        flags.clone(),
    )
    .unwrap();
    let bytes = stream.finish().unwrap();
    assert_eq!(bytes, vec![0x6a; CAPTURE_LIMIT]);
    assert!(flags.overflow.load(Ordering::SeqCst));
    assert!(!flags.failed.load(Ordering::SeqCst));
}
#[test]
fn capture_read_failure_is_explicit() {
    struct Broken;
    impl Read for Broken {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("owned capture control"))
        }
    }
    let flags = Arc::new(CaptureFlags::default());
    let mut stream = Stream::start(Broken, flags.clone()).unwrap();
    assert!(stream
        .finish()
        .unwrap_err()
        .contains("owned capture control"));
    assert!(flags.failed.load(Ordering::SeqCst));
    assert!(!flags.overflow.load(Ordering::SeqCst));
}
#[test]
fn actual_local_server_answers_while_ordinary_file_stays_exact() {
    let work_until = Instant::now() + Duration::from_secs(45);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("var")
        .join(format!(
            "inspection-native-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
    fs::create_dir_all(&root).unwrap();
    let address = format!(
        r"\\.\pipe\inspection-native-{}-{}",
        std::process::id(),
        membus::now_ms()
    );
    let exe = env!("CARGO_BIN_EXE_membus");
    let log = fs::File::create(root.join("backend.log")).unwrap();
    let mut command = Command::new(exe);
    command
        .arg(root.join("state"))
        .args(["pipe", &address, "--heartbeat-ms", "0", "--flush-ms", "0"])
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone().unwrap()))
        .stderr(Stdio::from(log));
    let mut server = child::Owned::spawn(&mut command).unwrap();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let end = work_until.min(Instant::now() + Duration::from_secs(10));
        loop {
            assert!(Instant::now() < end, "inspection readiness deadline");
            if membus::client::Client::connect_pipe(
                Path::new(&address),
                membus::client::Options::new("inspection-native-ready", 65536),
                end.min(Instant::now() + Duration::from_millis(200)),
            )
            .is_ok()
            {
                break;
            }
            assert!(server.child.try_wait().unwrap().is_none());
            thread::sleep(Duration::from_millis(20));
        }
        let output = invoke(exe, &["retirement-status", &address], work_until).unwrap();
        fs::write(root.join("inspection.stdout"), &output.stdout).unwrap();
        fs::write(root.join("inspection.stderr"), &output.stderr).unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let text = String::from_utf8(output.stdout).unwrap();
        for expected in [
            "retire_blocked=0",
            "retired_chunks=0",
            "read_only=0",
            "storage_error=0",
            "expiry_degraded=0",
        ] {
            assert!(text.lines().any(|line| line == expected));
        }
        let file = root.join("ordinary-file.bin");
        let original = b"owned native negative remains exact";
        fs::write(&file, original).unwrap();
        let output = invoke(
            exe,
            &["retirement-status", file.to_str().unwrap()],
            work_until,
        )
        .unwrap();
        fs::write(root.join("file-negative.stderr"), &output.stderr).unwrap();
        assert!(!output.status.success());
        assert_eq!(fs::read(file).unwrap(), original);
        println!("native five-field positive and file-refusal/exact-byte negative passed");
    }));
    let cleanup = server.wait(Duration::ZERO);
    match &cleanup {
        Ok((status, forced)) => println!("inspection resident pid={} status={status} forced={forced} directwait=true Jobempty=true", server.child.id()),
        Err(error) => eprintln!("inspection resident cleanup failed: {error}"),
    }
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
    cleanup.expect("inspection resident must settle before passing");
}
