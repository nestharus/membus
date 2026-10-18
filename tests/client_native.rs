//! Physical Windows qualification against this package's current resident executable.
//! Only fresh explicit fixture pipes/state are used. Run under an owned finite process Job.
#![cfg(windows)]
use membus::{client::*, pipe_io::Pipe};
#[path = "support/child.rs"]
mod child;
use std::{
    fs,
    os::windows::process::CommandExt,
    path::PathBuf,
    process::{Command, Stdio},
    sync::{Arc, Barrier},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
fn fixture_base() -> PathBuf {
    std::env::var_os("MEMBUS_CLIENT_TEST_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/client-fixtures"))
}
struct Server {
    owner: child::Owned,
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
        let mut command = Command::new(env!("CARGO_BIN_EXE_membus"));
        command
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
            .creation_flags(0x08000000);
        let owner = child::Owned::spawn(&mut command).unwrap();
        Self {
            owner,
            stopped: false,
        }
    }
    fn stop(&mut self) {
        if self.stopped {
            return;
        }
        let (status, _) = self.owner.wait(Duration::ZERO).unwrap();
        self.stopped = true;
        println!(
            "fixture child pid={} stopped/waited/reaped status={status}",
            self.owner.child.id()
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
    let root = fixture_base().join(format!("scan-{stamp}"));
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
        let root = fixture_base().join(format!("zero-{stamp}"));
        fs::create_dir_all(&root).unwrap();
        let pipe = format!(r"\\.\pipe\m3-zero-{stamp}");
        let mut server = Server::start_with(&root, &pipe, 0, 1, &[flag, "0"]);
        let mut o = Options::new("zero-retention-control", 65536);
        o.subscriptions = true;
        let mut c = Client::connect_pipe(std::path::Path::new(&pipe), o, d(5000).local).unwrap();
        let endpoint = format!(r"\\.\pipe\quiet-{stamp}");
        let mut sink = receiver(&endpoint);
        let s = c
            .subscribe("owned", "", &endpoint, "live-only", d(2000))
            .unwrap();
        assert!(
            c.subscription_status("owned", &s.token, "active", d(2000))
                .unwrap()
                .active
        );
        let mut a = c
            .prepare(MutationRequest {
                operation_id: "zero-live".into(),
                namespace: "owned".into(),
                key: "live".into(),
                value: b"zero-live".to_vec(),
                ack: Ack::Os,
                expected: Some(0),
            })
            .unwrap();
        let published = c.mutate(&mut a, d(2000)).unwrap();
        sink.connect().unwrap();
        use std::io::Read;
        let mut size = [0; 4];
        sink.read_exact(&mut size).unwrap();
        let n = u32::from_le_bytes(size) as usize;
        assert!(n <= 65536);
        let mut body = vec![0; n];
        sink.read_exact(&mut body).unwrap();
        let hint = Change::decode(&body).unwrap();
        assert_eq!(hint.key, "live");
        assert_eq!(hint.version, published.version);
        assert_eq!(hint.subscription, s.token);
        assert_eq!(hint.namespace, "owned");
        assert_eq!(hint.ack, Ack::Os);
        assert!(
            c.subscription_status("owned", &s.token, "delivered-active", d(2000))
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
fn receiver(endpoint: &str) -> Pipe {
    use std::os::windows::io::FromRawHandle;
    #[link(name = "kernel32")]
    extern "system" {
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
    let n: Vec<_> = endpoint.encode_utf16().chain(Some(0)).collect();
    let h = unsafe {
        CreateNamedPipeW(
            n.as_ptr(),
            1 | 0x40000000,
            0,
            1,
            65536,
            65536,
            1000,
            std::ptr::null_mut(),
        )
    };
    assert_ne!(h as isize, -1);
    Pipe::new(unsafe { fs::File::from_raw_handle(h) }, 5000)
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

/// The recorded-shape burst uses the real batch dispatcher and process restart.
/// Transport is a private Windows pipe; this is not a rerun of the historical
/// Python/TCP timing measurement or a qualification of Python's client encoder.
#[test]
fn actual_grouped_flush_burst_survives_kill_and_full_replay() {
    use membus::{
        frame, lp, read_frame, Cursor, BATCH, CONDITIONAL_PUT, HEALTH, HELLO, IO_METRICS,
    };
    use std::{io::Write, os::windows::fs::OpenOptionsExt};

    // Test-local packets exercise BATCH, which has no public SDK method. Replay
    // uses the supported SDK, not another implementation of committed reads.
    fn exchange(pipe: &mut Pipe, op: u8, id: u64, body: &[u8]) -> Vec<u8> {
        let until = d(30_000).local;
        pipe.set_deadline(until).unwrap();
        pipe.write_all(&frame(op, id, body)).unwrap();
        pipe.set_deadline(until).unwrap();
        let reply = read_frame(pipe).unwrap();
        let mut c = Cursor::new(&reply);
        assert_eq!(c.u16().unwrap(), 1);
        assert_eq!(c.u16().unwrap(), 0);
        assert_eq!(c.u8().unwrap(), op | 128);
        assert_eq!(c.u64().unwrap(), id);
        assert_eq!(c.u16().unwrap(), 0, "operation {op} refused");
        reply[15..].to_vec()
    }
    fn prefix(id: &str) -> Vec<u8> {
        let mut body = (membus::now_ms() + 30_000).to_le_bytes().to_vec();
        lp(&mut body, id.as_bytes());
        lp(&mut body, b"ledger");
        body
    }
    fn row(seq: usize) -> (String, Vec<u8>) {
        let size = match seq {
            1533 => 59_499,
            3066..=3070 => 16,
            _ if seq % 2 == 1 => 1039,
            _ => 1311,
        };
        let mut value = format!("op-{seq}:").into_bytes();
        value.resize(size, b'x');
        (format!("row/{seq:016}"), value)
    }

    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = fixture_base().join(format!("flush-burst-{}-{stamp}", std::process::id()));
    fs::create_dir_all(&root).unwrap();
    let pipe_name = format!(
        r"\\.\pipe\membus-flush-burst-{}-{stamp}",
        std::process::id()
    );
    let mut server = Server::start_with(&root, &pipe_name, 0, 1, &["--flush-ms", "50"]);
    let ready = connect(&pipe_name);
    let epoch = ready.negotiated().epoch;
    drop(ready);
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .custom_flags(0x40000000)
        .open(&pipe_name)
        .unwrap();
    let mut wire = Pipe::new(file, 30_000);
    let mut hello = (membus::MAX_FRAME as u32).to_le_bytes().to_vec();
    hello.push(0);
    lp(&mut hello, b"rust-flush-burst");
    let hello = exchange(&mut wire, HELLO, 1, &hello);
    assert_eq!(Cursor::new(&hello).u64().unwrap(), epoch);

    let started = Instant::now();
    let mut acknowledged = Vec::new();
    let mut batch_seconds = Vec::new();
    for offset in (0..3070).step_by(128) {
        let end = (offset + 128).min(3070);
        let mut body = prefix(&format!("flush-batch-{offset}"));
        body.extend(((end - offset) as u16).to_le_bytes());
        let rows: Vec<_> = (offset + 1..=end).map(row).collect();
        for (seq, (key, value)) in (offset + 1..=end).zip(&rows) {
            body.push(CONDITIONAL_PUT);
            lp(&mut body, format!("flush-op-{seq}").as_bytes());
            lp(&mut body, key.as_bytes());
            body.push(Ack::Flush as u8);
            body.extend(0u64.to_le_bytes());
            lp(&mut body, value);
        }
        let tick = Instant::now();
        let reply = exchange(&mut wire, BATCH, 2 + offset as u64, &body);
        batch_seconds.push(tick.elapsed().as_secs_f64());
        let mut c = Cursor::new(&reply);
        assert_eq!(c.u16().unwrap() as usize, rows.len(), "batch reply count");
        for (key, value) in rows {
            assert_eq!(c.u16().unwrap(), 0, "batch item {key} refused");
            let version = c.u64().unwrap();
            assert_ne!(version, 0, "batch item {key} has no committed revision");
            assert_eq!(c.u8().unwrap(), Ack::Flush as u8, "batch item {key}");
            acknowledged.push((key, value, version));
        }
        c.end().unwrap();
    }
    let seconds = started.elapsed().as_secs_f64();
    assert_eq!(acknowledged.len(), 3070);
    assert_eq!(acknowledged[1532].1.len(), 59_499);
    assert!(acknowledged[3065..].iter().all(|row| row.1.len() == 16));
    let health = exchange(&mut wire, HEALTH, 4000, &prefix("burst-health"));
    let mut c = Cursor::new(&health);
    let health_counts: Vec<_> = (0..6).map(|_| c.u64().unwrap()).collect();
    let high_water = (c.u8().unwrap(), c.u8().unwrap());
    c.end().unwrap();
    assert_eq!(health_counts[3], 3070);
    let io = exchange(&mut wire, IO_METRICS, 4001, &prefix("burst-io"));
    let mut c = Cursor::new(&io);
    let io_counts = (c.u64().unwrap(), c.u64().unwrap(), c.u64().unwrap());
    c.end().unwrap();
    drop(wire);
    // Zero-duration Owned::wait terminates the resident Job, waits its direct
    // child and requires an empty Job. No graceful-store shutdown substitutes.
    server.stop();

    let mut restarted = Server::start_with(&root, &pipe_name, 0, 2, &["--flush-ms", "50"]);
    let mut client = Client::connect_pipe(
        std::path::Path::new(&pipe_name),
        Options::new("rust-flush-burst-replay", membus::MAX_FRAME),
        d(5000).local,
    )
    .unwrap();
    let replay_epoch = client.negotiated().epoch;
    assert_ne!(replay_epoch, epoch);
    let mut replayed = Vec::new();
    for (page, rows) in acknowledged.chunks(128).enumerate() {
        let keys: Vec<_> = rows.iter().map(|(key, _, _)| key.as_str()).collect();
        let values = client
            .get_many("ledger", &keys, &format!("replay-{page}"), d(5000))
            .unwrap();
        assert_eq!(values.len(), rows.len(), "complete replay reply count");
        for ((key, expected, version), value) in rows.iter().zip(values) {
            let value = value.unwrap_or_else(|| panic!("missing acknowledged key {key}"));
            assert_eq!(value.ack, Ack::Flush, "replay acknowledgement {key}");
            assert_eq!(value.version, *version, "replay revision {key}");
            assert_eq!(&value.bytes, expected, "replay bytes {key}");
            replayed.push(key.as_str());
        }
    }
    assert_eq!(replayed.len(), 3070);
    let keys: Vec<_> = acknowledged
        .iter()
        .map(|(key, _, _)| key.as_str())
        .collect();
    assert_eq!(replayed, keys);
    fs::write(root.join("acknowledged-flush-keys.txt"), keys.join("\n")).unwrap();
    fs::write(root.join("replayed-flush-keys.txt"), replayed.join("\n")).unwrap();
    drop(client);
    restarted.stop();
    println!(
        "burst: acknowledged=3070 replayed=3070 max_batch=128 flush_ms=50 \
         epoch_before={epoch} epoch_after={replay_epoch} seconds={seconds} \
         batch_seconds={batch_seconds:?} health_counts={health_counts:?} \
         high_water={high_water:?} io_append_bytes_write_calls_sync_attempts={io_counts:?}; \
         physical_device_bytes=unmeasured; no historical timing threshold"
    );
}
