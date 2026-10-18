//! Positive native IPC and ordinary-file negative controls for read-only inspection.
#![cfg(windows)]
#![deny(clippy::all)]
use std::{
    fs,
    os::windows::process::CommandExt,
    path::Path,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
struct Server(Child);
impl Drop for Server {
    fn drop(&mut self) {
        if self.0.try_wait().unwrap().is_none() {
            self.0.kill().unwrap();
        }
        let status = self.0.wait().unwrap();
        println!(
            "owned inspection server pid={} waited status={status}",
            self.0.id()
        );
    }
}
#[test]
fn actual_local_server_answers_while_ordinary_file_stays_exact() {
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
    let mut server = Server(
        Command::new(exe)
            .arg(root.join("state"))
            .args(["pipe", &address, "--heartbeat-ms", "0", "--flush-ms", "0"])
            .creation_flags(0x08000000)
            .stdout(Stdio::from(log.try_clone().unwrap()))
            .stderr(Stdio::from(log))
            .spawn()
            .unwrap(),
    );
    println!("owned inspection server pid={}", server.0.id());
    let end = Instant::now() + Duration::from_secs(10);
    loop {
        if membus::client::Client::connect_pipe(
            Path::new(&address),
            membus::client::Options::new("inspection-native-ready", 65536),
            Instant::now() + Duration::from_millis(200),
        )
        .is_ok()
        {
            break;
        }
        assert!(server.0.try_wait().unwrap().is_none());
        assert!(Instant::now() < end);
        thread::sleep(Duration::from_millis(20));
    }
    let output = Command::new(exe)
        .args(["retirement-status", &address])
        .creation_flags(0x08000000)
        .output()
        .unwrap();
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
    let output = Command::new(exe)
        .arg("retirement-status")
        .arg(&file)
        .creation_flags(0x08000000)
        .output()
        .unwrap();
    fs::write(root.join("file-negative.stderr"), &output.stderr).unwrap();
    assert!(!output.status.success());
    assert_eq!(fs::read(file).unwrap(), original);
    println!("native five-field positive and file-refusal/exact-byte negative passed");
}
