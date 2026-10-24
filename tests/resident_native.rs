//! Isolated Windows resident process witnesses using the public client, pipe and CLI.
//! `cargo test --test resident_native -- --test-threads=1 --nocapture` runs both modes.
//! MEMBUS_RESIDENT_TEST_ROOT selects retained fixture output; MEMBUS_RESIDENT_BINARY
//! optionally selects an explicitly identified resident instead of this package's binary.
//! No physical-device byte or power-loss claim follows from these process-kill tests.
//! Idle/reclamation witnesses use 200 ms. The physical-kill phase uses a separate
//! generation with 60 s idle, confirms a live peer before kill, and bounds kill
//! plus Job settling to 5 s so idle reclamation cannot explain the closure.
#![cfg(windows)]
#[path = "support/child.rs"]
mod child;
use membus::{client::*, pipe_io::Pipe};
use std::{
    fs::{self, OpenOptions},
    io::{self, Read, Write},
    os::windows::fs::OpenOptionsExt,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

fn deadline(ms: u64) -> Deadline {
    Deadline {
        unix_ms: membus::now_ms() + ms,
        local: Instant::now() + Duration::from_millis(ms),
    }
}
fn binary() -> PathBuf {
    std::env::var_os("MEMBUS_RESIDENT_BINARY")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_membus")))
}
struct Resident {
    owner: child::Owned,
    stopped: bool,
}
impl Resident {
    fn start(root: &Path, state: &Path, pipe: &str, label: &str, cap: bool) -> Self {
        Self::start_with_idle(root, state, pipe, label, cap, 200)
    }
    fn start_kill_witness(root: &Path, state: &Path, pipe: &str, label: &str) -> Self {
        // Keep idle reclamation separate from physical death and Job settling.
        Self::start_with_idle(root, state, pipe, label, false, 60_000)
    }
    fn start_with_idle(
        root: &Path,
        state: &Path,
        pipe: &str,
        label: &str,
        cap: bool,
        idle_ms: u32,
    ) -> Self {
        let mut cmd = Command::new(binary());
        cmd.arg(state)
            .arg("pipe")
            .arg(pipe)
            .arg("--idle-ms")
            .arg(idle_ms.to_string())
            .args(["--heartbeat-ms", "250", "--flush-ms", "0"])
            .stdin(Stdio::null())
            .stdout(fs::File::create(root.join(format!("{label}.stdout"))).unwrap())
            .stderr(fs::File::create(root.join(format!("{label}.stderr"))).unwrap());
        if cap {
            cmd.args(["--state-limit-bytes", "250"]);
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
                "resident must be physically terminated"
            );
            println!(
                "resident pid={} physically killed, directly waited; Job settled: {status}",
                self.owner.child.id()
            );
        }
    }
}
impl Drop for Resident {
    fn drop(&mut self) {
        self.kill_wait();
    }
}
fn connect(pipe: &str) -> Client<Pipe> {
    Client::connect_pipe(
        Path::new(pipe),
        Options::new("resident-witness", membus::MAX_FRAME),
        deadline(5000).local,
    )
    .unwrap()
}
fn raw(pipe: &str) -> Pipe {
    let until = deadline(5000).local;
    loop {
        match OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(0x40000000)
            .open(pipe)
        {
            Ok(file) => return Pipe::new(file, 1000),
            Err(e) if matches!(e.raw_os_error(), Some(2 | 231)) && Instant::now() < until => {
                std::thread::sleep(Duration::from_millis(2))
            }
            Err(e) => panic!("open isolated pipe: {e}"),
        }
    }
}
fn put(c: &mut Client<Pipe>, key: &str, value: &[u8], ack: Ack) -> Result<MutationReply, Error> {
    // Preserve the original producer's 36-byte UUID identity charge. In cap mode
    // the first complete record is 207 bytes: above high-water, below refusal.
    let operation_id = format!("{:0>36}", format!("resident-{key}"));
    assert_eq!(operation_id.len(), 36);
    let mut attempt = c
        .prepare(MutationRequest {
            operation_id,
            namespace: "resident".into(),
            key: key.into(),
            value: value.into(),
            ack,
            expected: None,
        })
        .unwrap();
    c.mutate(&mut attempt, deadline(2000))
}
// Heartbeat is the resident's flat numeric/boolean object, not a diagnostic SDK.
// Read a single complete object, including its actual byte length, across rename.
#[derive(Debug)]
struct Heartbeat {
    text: String,
    bytes: u64,
}
impl Heartbeat {
    fn read(state: &Path) -> io::Result<Self> {
        let bytes = fs::read(state.join("heartbeat.json"))?;
        let text = String::from_utf8(bytes.clone()).map_err(io::Error::other)?;
        if !text.trim().starts_with('{') || !text.trim().ends_with('}') {
            return Err(io::Error::other("incomplete heartbeat object"));
        }
        assert!(bytes.len() < 512, "heartbeat object must stay bounded");
        Ok(Self {
            text,
            bytes: bytes.len() as u64,
        })
    }
    fn field(&self, key: &str) -> &str {
        let needle = format!("\"{key}\":");
        let tail = self
            .text
            .split_once(&needle)
            .unwrap_or_else(|| panic!("missing {key}: {}", self.text))
            .1;
        assert!(!tail.contains(&needle), "duplicate heartbeat field {key}");
        tail.split([',', '}']).next().unwrap().trim()
    }
    fn n(&self, key: &str) -> u64 {
        self.field(key).parse().unwrap()
    }
}
fn heartbeat_ready(state: &Path) -> Heartbeat {
    heartbeat_until(state, |_| true)
}
fn heartbeat_until(state: &Path, ready: impl Fn(&Heartbeat) -> bool) -> Heartbeat {
    let until = deadline(2000).local;
    loop {
        match Heartbeat::read(state) {
            Ok(h) if ready(&h) => return h,
            Ok(h) => {
                assert!(Instant::now() < until, "heartbeat condition not published: {}", h.text);
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) if Instant::now() < until => {
                let _ = e;
                std::thread::sleep(Duration::from_millis(10));
            }
            Err(e) => panic!("heartbeat not ready: {e}"),
        }
    }
}
fn health(root: &Path, state: &Path, pipe: &str, label: &str) -> (bool, String, String, Duration) {
    let stdout = root.join(format!("{label}.stdout"));
    let stderr = root.join(format!("{label}.stderr"));
    let mut cmd = Command::new(binary());
    cmd.arg("health")
        .arg(pipe)
        .arg("--state-dir")
        .arg(state)
        .stdin(Stdio::null())
        .stdout(fs::File::create(&stdout).unwrap())
        .stderr(fs::File::create(&stderr).unwrap());
    let start = Instant::now();
    let mut owner = child::Owned::spawn(&mut cmd).unwrap();
    let (status, stopped) = owner.wait(Duration::from_secs(5)).unwrap();
    let elapsed = start.elapsed();
    assert!(!stopped, "native health exceeded finite execution bound");
    assert!(fs::metadata(&stdout).unwrap().len() < 65536);
    assert!(fs::metadata(&stderr).unwrap().len() < 65536);
    (
        status.success(),
        fs::read_to_string(stdout).unwrap(),
        fs::read_to_string(stderr).unwrap(),
        elapsed,
    )
}
fn assert_closed(pipe: &mut Pipe, label: &str) {
    pipe.set_deadline(deadline(1000).local).unwrap();
    let result = pipe.read(&mut [0]);
    match result {
        Ok(0) => println!("{label}: EOF"),
        Err(e) if matches!(e.raw_os_error(), Some(109 | 232 | 233)) => {
            println!("{label}: OS closure {e}")
        }
        other => panic!("{label}: expected server closure, never a client timeout: {other:?}"),
    }
}
fn newest_nonempty_log(state: &Path) -> PathBuf {
    let mut logs = vec![];
    for dir in fs::read_dir(state.join("chunks")).unwrap() {
        let dir = dir.unwrap();
        if !dir.file_type().unwrap().is_dir() {
            continue;
        }
        for entry in fs::read_dir(dir.path()).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|s| s == "log")
                && fs::metadata(&path).unwrap().len() != 0
            {
                let seq: u64 = path
                    .file_stem()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .split('-')
                    .nth(1)
                    .unwrap()
                    .parse()
                    .unwrap();
                logs.push((seq, path));
            }
        }
    }
    logs.into_iter().max_by_key(|(seq, _)| *seq).unwrap().1
}
fn silent_peer_health(
    root: &Path,
    state: &Path,
    pipe: &str,
    pid: u32,
    epoch: u64,
    setup_pause: Duration,
) {
    let setup_start = Instant::now();
    let mut peers: Vec<_> = (0..64)
        .map(|i| {
            let peer = raw(pipe);
            if i == 0 && !setup_pause.is_zero() {
                std::thread::sleep(setup_pause);
            }
            peer
        })
        .collect();
    let setup_elapsed = setup_start.elapsed();
    let busy = health(root, state, pipe, "health-busy");
    assert!(busy.0 && busy.3 < Duration::from_millis(2500), "{busy:?}");
    assert_eq!(busy.1.trim(), format!("healthy pid={pid} epoch={epoch}"));
    // Setup may exceed an idle window on a shared host. Witness actual server
    // reclamation of every opened peer instead of assuming simultaneous occupancy.
    for (index, peer) in peers.iter_mut().enumerate() {
        assert_closed(peer, &format!("silent peer {index} reclaimed"));
    }
    println!(
        "health after 64 silent opens: setup={}s health={}s; all 64 server-closed",
        setup_elapsed.as_secs_f64(),
        busy.3.as_secs_f64()
    );
    drop(peers);
}
fn kill_with_stale_peer(
    server: &mut Resident,
    pipe: &str,
    observer_delay: Duration,
) -> (Pipe, Duration) {
    let mut stale = raw(pipe);
    stale.write_all(&[13]).unwrap();
    if !observer_delay.is_zero() {
        std::thread::sleep(observer_delay);
    }
    stale
        .write_all(&[0])
        .expect("stale peer must still be live immediately before physical termination");
    let started = Instant::now();
    server.kill_wait();
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(5),
        "physical termination and Job settling exceeded the witness bound: {elapsed:?}"
    );
    (stale, elapsed)
}
fn run(state_cap: bool) {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = std::env::var_os("MEMBUS_RESIDENT_TEST_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/resident-fixtures")
        })
        .join(format!(
            "resident-{}-{stamp}-{state_cap}",
            std::process::id()
        ));
    fs::create_dir_all(&root).unwrap();
    let state = root.join("state");
    let pipe = format!(
        r"\\.\pipe\LOCAL\membus-resident-{}-{stamp}",
        std::process::id()
    );
    println!(
        "resident binary={} root={} state_cap={state_cap}",
        binary().display(),
        root.display()
    );
    let mut server = Resident::start(&root, &state, &pipe, "first", false);
    let mut c = connect(&pipe);
    for partial in [vec![13], vec![13, 0, 0, 0, 1, 0]] {
        let mut p = raw(&pipe);
        p.write_all(&partial).unwrap();
        let start = Instant::now();
        assert_closed(&mut p, "partial ingress idle");
        let elapsed = start.elapsed();
        assert!(
            elapsed >= Duration::from_millis(100) && elapsed < Duration::from_millis(1200),
            "idle closure {elapsed:?}"
        );
        println!(
            "partial={partial:?} server idle closure seconds={}",
            elapsed.as_secs_f64()
        );
    }
    // The 200ms idle bound also applies to this original client: reopen explicitly.
    drop(c);
    c = connect(&pipe);
    let epoch = c.negotiated().epoch;
    let expected = [
        ("ack/1", b"value/1".as_slice(), Ack::Os),
        ("ack/2", b"value/2".as_slice(), Ack::Flush),
    ];
    let mut versions = vec![];
    for (key, bytes, ack) in expected {
        let reply = put(&mut c, key, bytes, ack).unwrap();
        assert_eq!(reply.achieved, ack);
        assert_eq!(reply.epoch, epoch);
        versions.push(reply.version);
    }
    drop(c);
    let mut samples: Vec<Heartbeat> = vec![];
    let start = Instant::now();
    while start.elapsed() < Duration::from_millis(1100) {
        let h = heartbeat_ready(&state);
        assert_eq!(h.n("pid"), server.owner.child.id() as u64);
        assert_eq!(h.n("epoch"), epoch);
        if samples
            .last()
            .is_none_or(|last| last.n("heartbeat_writes") != h.n("heartbeat_writes"))
        {
            samples.push(h);
        }
        std::thread::sleep(Duration::from_millis(15));
    }
    assert!(samples.len() >= 3);
    for pair in samples.windows(2) {
        assert_eq!(
            pair[1].n("heartbeat_writes"),
            pair[0].n("heartbeat_writes") + 1
        );
        assert_eq!(
            pair[1].n("heartbeat_bytes") - pair[0].n("heartbeat_bytes"),
            pair[1].bytes
        );
    }
    for h in &samples {
        println!("heartbeat bytes={} {}", h.bytes, h.text.trim());
    }
    let good = health(&root, &state, &pipe, "health-good");
    assert!(good.0, "{good:?}");
    assert_eq!(
        good.1.trim(),
        format!("healthy pid={} epoch={epoch}", server.owner.child.id())
    );
    // Stable wrong-state controls discriminate PID and epoch independently.
    for (key, label) in [("pid", "wrong-pid"), ("epoch", "wrong-epoch")] {
        let wrong = root.join(label);
        fs::create_dir(&wrong).unwrap();
        let h = heartbeat_ready(&state);
        let old = format!("\"{key}\":{}", h.n(key));
        let replacement = format!("\"{key}\":0");
        fs::write(
            wrong.join("heartbeat.json"),
            h.text.replacen(&old, &replacement, 1),
        )
        .unwrap();
        let bad = health(&root, &wrong, &pipe, label);
        assert!(
            !bad.0
                && bad
                    .2
                    .contains("heartbeat pid/epoch/freshness does not match pipe"),
            "{bad:?}"
        );
    }
    // Deny delete sharing so the resident's real rename fails; RAII releases on panic.
    let lock = OpenOptions::new()
        .read(true)
        .share_mode(3)
        .open(state.join("heartbeat.json"))
        .unwrap();
    let before = heartbeat_ready(&state);
    std::thread::sleep(Duration::from_millis(600));
    let locked = heartbeat_ready(&state);
    assert_eq!(locked.n("heartbeat_writes"), before.n("heartbeat_writes"));
    assert_eq!(locked.n("heartbeat_bytes"), before.n("heartbeat_bytes"));
    assert!(fs::metadata(state.join("heartbeat.tmp")).unwrap().len() < 512);
    drop(lock);
    std::thread::sleep(Duration::from_millis(400));
    let recovered = heartbeat_ready(&state);
    assert!(recovered.n("heartbeat_failures") > before.n("heartbeat_failures"));
    assert!(recovered.n("heartbeat_writes") > before.n("heartbeat_writes"));
    assert!(recovered.n("heartbeat_attempts") > recovered.n("heartbeat_writes"));
    assert!(recovered.n("heartbeat_bytes_attempted") > recovered.n("heartbeat_bytes"));
    println!("heartbeat failure/recovery {}", recovered.text.trim());
    // Exercise delayed setup and observation in the existing cap-mode witness.
    let delay = Duration::from_millis(if state_cap { 300 } else { 0 });
    silent_peer_health(
        &root,
        &state,
        &pipe,
        server.owner.child.id(),
        epoch,
        delay,
    );
    server.kill_wait();
    let mut server = Resident::start_kill_witness(&root, &state, &pipe, "kill-witness");
    let mut current = connect(&pipe);
    let killed_epoch = current.negotiated().epoch;
    assert_ne!(killed_epoch, epoch);
    // The new generation must have a nonempty active chunk for its torn-tail
    // probe; previous-generation chunks are closed and must stay immutable.
    let kill_probe = put(
        &mut current,
        "kill/probe",
        b"active-kill-generation",
        Ack::Os,
    )
    .unwrap();
    drop(current);
    let (mut stale, kill_elapsed) = kill_with_stale_peer(&mut server, &pipe, delay);
    let stripe = newest_nonempty_log(&state);
    let before_tail = fs::metadata(&stripe).unwrap().len();
    OpenOptions::new()
        .append(true)
        .open(&stripe)
        .unwrap()
        .write_all(b"MBK")
        .unwrap();
    assert_eq!(fs::metadata(&stripe).unwrap().len(), before_tail + 3);
    let mut restarted = Resident::start(&root, &state, &pipe, "restarted", false);
    let mut c = connect(&pipe);
    assert_closed(
        &mut stale,
        "stale pipe after physical death and endpoint reuse",
    );
    drop(stale);
    assert_ne!(c.negotiated().epoch, killed_epoch);
    assert_eq!(fs::metadata(&stripe).unwrap().len(), before_tail);
    for (i, (key, bytes, ack)) in expected.iter().enumerate() {
        let value = c
            .get("resident", key, &format!("replay-{i}"), deadline(2000))
            .unwrap()
            .unwrap();
        assert_eq!(value.bytes, *bytes);
        assert_eq!(value.version, versions[i]);
        assert_eq!(value.ack, *ack);
    }
    let probe = c
        .get(
            "resident",
            "kill/probe",
            "replay-kill-probe",
            deadline(2000),
        )
        .unwrap()
        .unwrap();
    assert_eq!(probe.bytes, b"active-kill-generation");
    assert_eq!(probe.version, kill_probe.version);
    assert_eq!(probe.ack, Ack::Os);
    println!("same state/endpoint replay: epoch {killed_epoch} -> {}, authored 3-byte tail removed, exact OS/Flush bytes/version/ack; kill={kill_elapsed:?}", c.negotiated().epoch);
    drop(c);
    restarted.kill_wait();
    if state_cap {
        let cap_state = root.join("cap-state");
        let cap_pipe = format!("{pipe}-cap");
        let mut cap = Resident::start(&root, &cap_state, &cap_pipe, "cap", true);
        let mut c = connect(&cap_pipe);
        let prior = put(&mut c, "first", &[b'x'; 100], Ack::Os).unwrap();
        assert!(matches!(
            put(&mut c, "too-big", &[b'x'; 300], Ack::Os),
            Err(Error::Refused(Refusal { code: 7, .. }))
        ));
        assert_eq!(
            c.get("resident", "too-big", "refused-absent", deadline(2000))
                .unwrap(),
            None
        );
        let value = c
            .get("resident", "first", "prior-survives", deadline(2000))
            .unwrap()
            .unwrap();
        assert_eq!(value.bytes, [b'x'; 100]);
        assert_eq!(value.version, prior.version);
        assert_eq!(value.ack, Ack::Os);
        drop(c);
        let pressure = heartbeat_until(&cap_state, |h| h.field("state_high_water") == "true");
        assert_eq!(pressure.field("state_high_water"), "true");
        assert!((187..=250).contains(&pressure.n("state_bytes")));
        assert_eq!(pressure.n("state_limit_bytes"), 250);
        println!(
            "state cap: refusal7, refused key absent, prior exact read/version/ack, {}",
            pressure.text.trim()
        );
        cap.kill_wait();
    }
    for dir in [&state, &root.join("cap-state")] {
        if !dir.exists() {
            continue;
        }
        for entry in fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            if entry.file_name().to_string_lossy().starts_with("heartbeat") {
                assert!(matches!(
                    entry.file_name().to_str(),
                    Some("heartbeat.json" | "heartbeat.tmp")
                ));
                assert!(entry.metadata().unwrap().len() < 512);
            }
        }
    }
    fs::write(root.join("completed.txt"), format!("state_cap={state_cap}; all owned children directly waited and Jobs settled; isolated state only; physical device bytes unmeasured\n")).unwrap();
}
#[test]
fn complete_resident_process_witness() {
    run(false);
}
#[test]
fn complete_resident_process_witness_with_state_cap() {
    run(true);
}
