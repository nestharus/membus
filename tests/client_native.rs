//! Physical Windows qualification against this package's current resident executable.
//! Only fresh explicit fixture pipes/state are used. Run under an owned finite process Job.
#![cfg(windows)]
use membus::{client::*, pipe_io::Pipe};
use std::{
    fs,
    os::windows::process::CommandExt,
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::{Arc, Barrier},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
struct Server {
    child: Child,
    stopped: bool,
}
impl Server {
    fn start(root: &std::path::Path, pipe: &str, delay_ms: u64, generation: u8) -> Self {
        let stderr = fs::File::create(root.join(format!("server-{generation}.stderr"))).unwrap();
        let stdout = fs::File::create(root.join(format!("server-{generation}.stdout"))).unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_membus"))
            .arg(root.join("state"))
            .arg("pipe")
            .arg(pipe)
            .args([
                "--heartbeat-ms",
                "0",
                "--flush-ms",
                "0",
                "--idle-ms",
                "5000",
                "--delay-ms",
            ])
            .arg(delay_ms.to_string())
            .stdin(Stdio::null())
            .stdout(stdout)
            .stderr(stderr)
            .creation_flags(0x08000000)
            .spawn()
            .unwrap();
        Self {
            child,
            stopped: false,
        }
    }
    fn stop(&mut self) {
        if self.stopped {
            return;
        }
        if self.child.try_wait().unwrap().is_none() {
            self.child.kill().unwrap();
        }
        let status = self.child.wait().unwrap();
        self.stopped = true;
        println!(
            "fixture child pid={} stopped/waited/reaped status={status}",
            self.child.id()
        );
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.stop();
    }
}
fn d(budget: u64) -> Deadline {
    Deadline {
        unix_ms: membus::now_ms() + budget,
        local: Instant::now() + Duration::from_millis(budget),
    }
}
fn connect(pipe: &str) -> Client<Pipe> {
    Client::connect_pipe(
        std::path::Path::new(pipe),
        Options::new("rust-client-owned-fixture", 65536),
        d(5000).local,
    )
    .unwrap()
}
fn mutation(id: &str, key: &str, ack: Ack, expected: Option<u64>) -> MutationRequest {
    MutationRequest {
        operation_id: id.into(),
        namespace: "rust-client-owned".into(),
        key: key.into(),
        value: id.as_bytes().to_vec(),
        ack,
        expected,
    }
}
#[test]
fn actual_resident_legacy_reads_os_flush_put_cas_pending_timeout_and_restart() {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let base = std::env::var_os("MEMBUS_CLIENT_TEST_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/client-fixtures")
        });
    let root = base.join(format!("physical-{}-{stamp}", std::process::id()));
    fs::create_dir_all(&root).unwrap();
    let pipe = format!(
        r"\\.\pipe\membus-client-owned-{}-{stamp}",
        std::process::id()
    );
    let mut server = Server::start(&root, &pipe, 200, 1);
    let mut c = connect(&pipe);
    let epoch = c.negotiated().epoch;
    assert_eq!(c.negotiated().refusals.len(), 19);
    assert!(!c.negotiated().scans);
    assert_eq!(
        c.get("rust-client-owned", "absent", "initial-read", d(2000))
            .unwrap(),
        None
    );
    let mut versions = vec![];
    for (i, (ack, expected)) in [
        (Ack::Os, None),
        (Ack::Flush, None),
        (Ack::Os, Some(0)),
        (Ack::Flush, Some(0)),
    ]
    .into_iter()
    .enumerate()
    {
        let key = format!("key-{i}");
        let id = format!("put-{i}");
        let mut a = c.prepare(mutation(&id, &key, ack, expected)).unwrap();
        let got = c.mutate(&mut a, d(2000)).unwrap();
        assert_eq!(got.epoch, epoch);
        assert_eq!(got.achieved, ack);
        assert_eq!(c.mutate(&mut a, d(2000)).unwrap(), got);
        versions.push(got.version);
    }
    let values = c
        .get_many(
            "rust-client-owned",
            &["key-3", "absent", "key-0"],
            "batch-read",
            d(2000),
        )
        .unwrap();
    assert_eq!(values[0].as_ref().unwrap().version, versions[3]);
    assert_eq!(values[0].as_ref().unwrap().ack, Ack::Flush);
    assert_eq!(values[1], None);
    assert_eq!(values[2].as_ref().unwrap().bytes, b"put-0");
    let mut conflict = c
        .prepare(mutation("conflict", "key-0", Ack::Os, Some(0)))
        .unwrap();
    assert!(matches!(
        c.mutate(&mut conflict, d(2000)),
        Err(Error::Refused(Refusal { code: 12, .. }))
    ));
    // The resident's real delayed admission distinguishes pending19 from committed12.
    let barrier = Arc::new(Barrier::new(3));
    let mut joins = vec![];
    for i in 0..2 {
        let pipe = pipe.clone();
        let barrier = barrier.clone();
        joins.push(std::thread::spawn(move || {
            let mut c = connect(&pipe);
            let mut a = c
                .prepare(mutation(
                    &format!("pending-{i}"),
                    "pending",
                    Ack::Os,
                    Some(0),
                ))
                .unwrap();
            barrier.wait();
            c.mutate(&mut a, d(2000))
        }));
    }
    barrier.wait();
    let mut results = vec![];
    for join in joins {
        results.push(join.join().unwrap());
    }
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Err(Error::Refused(Refusal { code: 19, .. }))))
            .count(),
        1
    );
    // Timeout after the physical write is uncertain; only an explicit same-token retry follows.
    let mut a = c
        .prepare(mutation(
            "physical-uncertain",
            "uncertain",
            Ack::Flush,
            Some(0),
        ))
        .unwrap();
    assert!(matches!(
        c.mutate(&mut a, d(20)),
        Err(Error::Uncertain { .. })
    ));
    assert!(!c.is_usable());
    std::thread::sleep(Duration::from_millis(300));
    drop(c);
    let mut c = connect(&pipe);
    let reconciled = c
        .get(
            "rust-client-owned",
            "uncertain",
            "reconcile-uncertain",
            d(2000),
        )
        .unwrap()
        .unwrap();
    assert_eq!(reconciled.bytes, b"physical-uncertain");
    assert_eq!(
        c.mutate(&mut a, d(2000)).unwrap().version,
        reconciled.version
    );
    drop(c);
    server.stop();
    let mut restarted = Server::start(&root, &pipe, 0, 2);
    let mut c = connect(&pipe);
    assert_ne!(c.negotiated().epoch, epoch);
    assert!(matches!(
        c.mutate(&mut a, d(2000)),
        Err(Error::EpochChanged { .. })
    ));
    let replayed = c
        .get("rust-client-owned", "uncertain", "replay-read", d(2000))
        .unwrap()
        .unwrap();
    assert_eq!(replayed, reconciled);
    let mut options = Options::new("rust-client-owned-fixture", 65536);
    options.scans = true;
    let scans = Client::connect_pipe(std::path::Path::new(&pipe), options, d(2000).local).unwrap();
    assert!(scans.negotiated().scans);
    assert_eq!(scans.negotiated().refusals.len(), 22);
    drop(scans);
    drop(c);
    restarted.stop();
    fs::write(root.join("custody.txt"),"two explicitly spawned NoWindow resident children stopped, directly waited and reaped; inherited finite enclosing Job; no production endpoint/state\n").unwrap();
    println!("physical resident: legacy19, scan22, GET/GET_MANY, OS/flush PUT/CAS, dedup, conflict12, pending19, real pipe timeout, same-epoch retry, restart epoch refusal/replay passed");
}
