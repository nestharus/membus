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
        Self::start_with(root, pipe, delay_ms, generation, &[])
    }
    fn start_with(
        root: &std::path::Path,
        pipe: &str,
        delay_ms: u64,
        generation: u8,
        extra: &[&str],
    ) -> Self {
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
            .args(extra)
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
fn actual_pinned_scan_and_subscription_failure_are_distinct_from_absence() {
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root =
        PathBuf::from(std::env::var_os("MEMBUS_CLIENT_TEST_ROOT").expect("owned fixture root"))
            .join(format!("scan-{stamp}"));
    fs::create_dir_all(&root).unwrap();
    let pipe = format!(r"\\.\pipe\membus-m3-owned-{stamp}");
    let mut server = Server::start(&root, &pipe, 0, 1);
    let mut options = Options::new("m3-owned", membus::MAX_FRAME);
    options.scans = true;
    options.subscriptions = true;
    let mut c = Client::connect_pipe(std::path::Path::new(&pipe), options, d(5000).local).unwrap();
    assert_eq!(c.negotiated().refusals.len(), 24);
    let ns = "rust-client-owned";
    let sub = c
        .subscribe(
            ns,
            "key-",
            &format!(r"\\.\pipe\missing-m3-owned-{stamp}"),
            "subscribe",
            d(2000),
        )
        .unwrap();
    let token = c.snapshot_open(ns, "open", d(2000)).unwrap();
    let mut a = c
        .prepare(mutation("after-snapshot", "key-after", Ack::Os, Some(0)))
        .unwrap();
    c.mutate(&mut a, d(2000)).unwrap();
    let page = c
        .scan_prefix(ns, "key-", &token, &[], 1, 65536, "scan", d(2000))
        .unwrap();
    assert!(page.done && page.items.is_empty());
    c.snapshot_release(ns, &token, "release", d(2000)).unwrap();
    assert!(matches!(
        c.scan_prefix(ns, "key-", &token, &[], 1, 65536, "expired", d(2000)),
        Err(Error::Refused(Refusal { code: 20, .. }))
    ));
    let token = c.snapshot_open(ns, "fresh", d(2000)).unwrap();
    let page = c
        .scan_prefix(ns, "key-", &token, &[], 1, 65536, "fresh-page", d(2000))
        .unwrap();
    assert_eq!(page.items[0].0, "key-after");
    assert_eq!(page.items[0].1.bytes, b"after-snapshot");
    c.snapshot_release(ns, &token, "fresh-release", d(2000))
        .unwrap();
    let prefix = "x".repeat(1000);
    for suffix in ["a", "b"] {
        let mut a = c
            .prepare(mutation(
                &format!("long-{suffix}"),
                &format!("{prefix}{suffix}"),
                Ack::Os,
                Some(0),
            ))
            .unwrap();
        c.mutate(&mut a, d(2000)).unwrap();
    }
    let t = c.snapshot_open(ns, "long-open", d(2000)).unwrap();
    let one = c
        .scan_prefix(ns, &prefix, &t, &[], 1, 65536, "long-one", d(2000))
        .unwrap();
    assert!(!one.done);
    assert!(one.cursor.len() > 1024);
    assert_eq!(one.items[0].0, format!("{prefix}a"));
    let two = c
        .scan_prefix(ns, &prefix, &t, &one.cursor, 1, 65536, "long-two", d(2000))
        .unwrap();
    assert!(two.done);
    assert_eq!(two.items[0].0, format!("{prefix}b"));
    c.snapshot_release(ns, &t, "long-release", d(2000)).unwrap();
    let until = Instant::now() + Duration::from_secs(2);
    loop {
        let status = c
            .subscription_status(ns, &sub.token, "status", d(2000))
            .unwrap();
        if status.failed > 0 {
            break;
        }
        assert!(
            Instant::now() < until,
            "missing endpoint was not observed as failed"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    c.unsubscribe(ns, &sub.token, "unsubscribe", d(2000))
        .unwrap();
    assert!(
        !c.subscription_status(ns, &sub.token, "cancelled", d(2000))
            .unwrap()
            .active
    );
    drop(c);
    server.stop();
}
#[test]
fn actual_zero_retention_negotiation_and_active_cancellation() {
    for flag in ["--signal-history", "--signal-bytes"] {
        let stamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let root = PathBuf::from(std::env::var_os("MEMBUS_CLIENT_TEST_ROOT").unwrap())
            .join(format!("zero-{stamp}"));
        fs::create_dir_all(&root).unwrap();
        let pipe = format!(r"\\.\pipe\m3-zero-{stamp}");
        let mut server = Server::start_with(&root, &pipe, 0, 1, &[flag, "0"]);
        let mut o = Options::new("zero-retention-control", 65536);
        o.subscriptions = true;
        let mut c = Client::connect_pipe(std::path::Path::new(&pipe), o, d(5000).local).unwrap();
        let s = c
            .subscribe(
                "owned",
                "",
                &format!(r"\\.\pipe\quiet-{stamp}"),
                "live-only",
                d(2000),
            )
            .unwrap();
        assert!(
            c.subscription_status("owned", &s.token, "active", d(2000))
                .unwrap()
                .active
        );
        c.unsubscribe("owned", &s.token, "cancel", d(2000)).unwrap();
        assert!(
            !c.subscription_status("owned", &s.token, "inactive", d(2000))
                .unwrap()
                .active
        );
        drop(c);
        server.stop();
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
