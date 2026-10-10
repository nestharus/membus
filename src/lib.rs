//! membus.kv/1 slice 1: bounded framing, independent committed reads, durable replay.
mod signals;
#[cfg(windows)]
mod pipe_io;
use std::{
    collections::{hash_map::RandomState, HashMap, HashSet, VecDeque},
    fs::{self, File, OpenOptions},
    hash::BuildHasher,
    io::{self, Read, Write},
    path::Path,
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc, Arc, Condvar, Mutex, RwLock,
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
pub const MAX_FRAME: usize = 16 * 1024 * 1024;
pub const MAX_VALUE: usize = 4 * 1024 * 1024;
pub const MAX_ID: usize = 1024;
pub const MAX_BATCH: usize = 128;
pub const CODES: &[(u16, &str, bool)] = &[
    (1, "incompatible_version", false),
    (2, "unsupported", false),
    (3, "malformed", false),
    (4, "too_large", false),
    (5, "deadline", true),
    (6, "busy", true),
    (7, "capacity", false),
    (8, "quota", false),
    (9, "index_cap", false),
    (10, "read_only", true),
    (11, "warming", true),
    (12, "conflict", false),
    (13, "retry_window_expired", false),
    (14, "op_id_mismatch", false),
    (15, "mem_not_enabled", false),
    (16, "io_failed_uncertain", true),
    (17, "pending_timeout_uncertain", true),
    (18, "dedup_full", true),
    (19, "pending_conflict", true),
    (20, "cursor_expired", false),
    (21, "invalid_cursor", false),
    (22, "snapshot_full", true),
    (23, "resume_gap", false),
    (24, "subscription_missing", false),
];
pub const HELLO: u8 = 1;
pub const GET: u8 = 2;
pub const GET_MANY: u8 = 3;
pub const PUT: u8 = 4;
pub const CONDITIONAL_PUT: u8 = 5;
pub const REMOVE: u8 = 6;
pub const BATCH: u8 = 7;
pub const HEALTH: u8 = 8;
/// Additive kv/1 minor0 diagnostic operation; existing HEALTH layout is unchanged.
pub const IO_METRICS: u8 = 9;
pub const SNAPSHOT_OPEN: u8 = 10;
pub const SCAN_PREFIX: u8 = 11;
pub const SCAN_RANGE: u8 = 12;
pub const SNAPSHOT_RELEASE: u8 = 13;
pub const SCAN_KEYS_PREFIX: u8 = 14;
pub const SCAN_KEYS_RANGE: u8 = 15;
pub const SUBSCRIBE: u8 = 16;
pub const UNSUBSCRIBE: u8 = 17;
pub const SUBSCRIPTION_STATUS: u8 = 18;
pub const SIGNAL_USAGE: u8 = 19;
pub const CAPTURE_METRICS: u8 = 20;
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
pub fn crc32(bytes: &[u8]) -> u32 {
    let mut c = !0u32;
    for &b in bytes {
        c ^= b as u32;
        for _ in 0..8 {
            c = (c >> 1) ^ if c & 1 != 0 { 0xedb88320 } else { 0 };
        }
    }
    !c
}
pub fn lp(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend((bytes.len() as u32).to_le_bytes());
    out.extend(bytes);
}
pub struct Cursor<'a> {
    b: &'a [u8],
    p: usize,
}
impl<'a> Cursor<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        Self { b, p: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], u16> {
        let e = self.p.checked_add(n).ok_or(3u16)?;
        if e > self.b.len() {
            return Err(3);
        }
        let b = &self.b[self.p..e];
        self.p = e;
        Ok(b)
    }
    pub fn u8(&mut self) -> Result<u8, u16> {
        Ok(self.take(1)?[0])
    }
    pub fn u16(&mut self) -> Result<u16, u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub fn u32(&mut self) -> Result<u32, u16> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> Result<u64, u16> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn bytes(&mut self, max: usize) -> Result<&'a [u8], u16> {
        let n = self.u32()? as usize;
        if n > max {
            return Err(4);
        }
        self.take(n)
    }
    pub fn text(&mut self) -> Result<String, u16> {
        let s = self.bound()?;
        if s.is_empty() {
            return Err(3);
        }
        Ok(s)
    }
    /// Scan bounds may be empty; identifiers still use text().
    pub fn bound(&mut self) -> Result<String, u16> {
        let b = self.bytes(MAX_ID)?;
        Ok(std::str::from_utf8(b).map_err(|_| 3u16)?.to_owned())
    }
    pub fn end(&self) -> Result<(), u16> {
        if self.p == self.b.len() {
            Ok(())
        } else {
            Err(3)
        }
    }
}
pub fn frame(op: u8, id: u64, payload: &[u8]) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend(1u16.to_le_bytes());
    b.extend(0u16.to_le_bytes());
    b.push(op);
    b.extend(id.to_le_bytes());
    b.extend(payload);
    let mut out = (b.len() as u32).to_le_bytes().to_vec();
    out.extend(b);
    out
}
pub fn read_frame<R: Read>(r: &mut R) -> io::Result<Vec<u8>> {
    read_frame_limit(r, MAX_FRAME)
}
pub fn read_frame_limit<R: Read>(r: &mut R, max: usize) -> io::Result<Vec<u8>> {
    let mut n = [0; 4];
    r.read_exact(&mut n)?;
    let n = u32::from_le_bytes(n) as usize;
    if n < 13 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "malformed frame",
        ));
    }
    if n > max.min(MAX_FRAME) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "too_large frame; refused before body allocation",
        ));
    }
    let mut b = vec![0; n];
    r.read_exact(&mut b)?;
    Ok(b)
}
#[derive(Clone, Debug, PartialEq)]
pub struct Value {
    pub version: u64,
    pub ack: u8,
    pub bytes: Vec<u8>,
    pub tombstone: bool,
}
#[derive(Clone, Debug)]
pub struct Mutation {
    pub ns: String,
    pub key: String,
    pub op_id: String,
    pub deadline: u64,
    pub ack: u8,
    pub expected: Option<u64>,
    pub bytes: Vec<u8>,
    pub remove: bool,
}
#[derive(Clone, Debug, PartialEq)]
pub struct Outcome {
    pub code: u16,
    pub version: u64,
    pub ack: u8,
}
impl Outcome {
    fn error(code: u16) -> Self {
        Self {
            code,
            version: 0,
            ack: 0,
        }
    }
    pub fn bytes(&self) -> Vec<u8> {
        let mut b = self.version.to_le_bytes().to_vec();
        b.push(self.ack);
        b
    }
}
#[derive(Clone)]
struct Dedup {
    fingerprint: Vec<u8>,
    at: u64,
    outcome: Option<Outcome>,
}
struct State {
    pending: HashMap<(String, String), Meta>,
    metadata: HashMap<(String, String), Meta>,
    namespace_counts: HashMap<String, usize>,
    keys: usize,
    committed_bytes: usize,
    dedup_bytes: usize,
    dedup: HashMap<(String, String), Dedup>,
    retired: VecDeque<(String, String)>,
    retired_set: HashSet<(String, String)>,
    expiry: VecDeque<((String, String), u64)>,
    next: u64,
    reserved_bytes: usize,
    stripe_reserved: [usize; 4],
    stripe_items: [usize; 4],
    persisted_bytes: u64,
    pending_disk: u64,
    read_only: bool,
    warming: bool,
}
#[derive(Clone)]
struct Meta {
    version: u64,
    tombstone: bool,
    bytes: usize,
}
pub struct Log {
    file: File,
    pub bytes: u64,
    pub flushes: u64,
    pub write_calls: u64,
    poisoned: bool,
}
#[derive(Clone)]
pub struct Config {
    pub signal_history: usize,
    pub signal_bytes: usize,
    pub snapshot_bytes: usize,
    pub snapshot_max: usize,
    pub snapshot_ttl_ms: u64,
    pub state_limit_bytes: u64,
    pub quota: usize,
    pub index_cap: usize,
    pub capacity: usize,
    pub queue_bytes: usize,
    pub retry_ms: u64,
    pub dedup_cap: usize,
    pub flush_ms: u64,
    pub allow_mem: bool,
    pub write_delay_ms: u64,
    /// Test seam for an append-complete notification arriving after a covering sync.
    pub flush_notice_delay_ms: u64,
    pub fail_after_append: bool,
    pub fail_partial_append: bool,
    pub fail_sync: bool,
    #[cfg(test)]
    pub sync_test_gate: Option<Arc<(Mutex<(bool, bool)>, Condvar)>>,
    #[cfg(test)]
    pub sync_test_notice: Option<mpsc::Sender<()>>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            signal_history: 4096,
            signal_bytes: 16 * 1024 * 1024,
            snapshot_bytes: 256 * 1024 * 1024,
            snapshot_max: 8,
            snapshot_ttl_ms: 30_000,
            state_limit_bytes: 8 * 1024 * 1024 * 1024,
            quota: 500_000,
            index_cap: 917_000,
            capacity: 256 * 1024 * 1024,
            queue_bytes: 64 * 1024 * 1024,
            retry_ms: 24 * 3600 * 1000,
            dedup_cap: 262144,
            flush_ms: 50,
            allow_mem: false,
            write_delay_ms: 0,
            flush_notice_delay_ms: 0,
            fail_after_append: false,
            fail_partial_append: false,
            fail_sync: false,
            #[cfg(test)]
            sync_test_gate: None,
            #[cfg(test)]
            sync_test_notice: None,
        }
    }
}
pub struct Store {
    signals: signals::Hub,
    capture_metrics: [AtomicU64; 7],
    committed: RwLock<HashMap<(String, String), Arc<Value>>>,
    scans: Arc<Mutex<ScanState>>,
    scan_usage: Arc<ScanUsage>,
    scan_reaper: Option<(mpsc::Sender<()>, std::thread::JoinHandle<()>)>,
    state: Mutex<State>,
    done: Condvar,
    logs: Vec<Arc<Mutex<Log>>>,
    appenders: Vec<AppendOwner>,
    flush_owner: Option<std::thread::JoinHandle<()>>,
    batch_gate: Mutex<()>,
    pub config: Config,
    pub epoch: u64,
    _owner: File,
}
#[derive(Default)]
struct ScanUsage {
    bytes: AtomicUsize,
    count: AtomicUsize,
}
struct ScanCharge {
    usage: Arc<ScanUsage>,
    bytes: usize,
}
impl Drop for ScanCharge {
    fn drop(&mut self) {
        self.usage.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        self.usage.count.fetch_sub(1, Ordering::AcqRel);
    }
}
struct Snapshot {
    ns: String,
    created: Instant,
    rows: Vec<(String, Arc<Value>)>,
    cursor_hash: RandomState,
    // Charges survive release/expiry while a dispatcher still holds this Arc.
    _charge: ScanCharge,
}
struct ScanState {
    next: u64,
    snapshots: HashMap<u64, Arc<Snapshot>>,
}
struct Append {
    record: Vec<u8>,
    ack: u8,
    done: mpsc::Sender<io::Result<()>>,
}
struct AppendOwner {
    sender: Option<mpsc::Sender<Append>>,
    handle: Option<std::thread::JoinHandle<()>>,
}
struct FlushWait {
    stripe: usize,
    end: u64,
    bytes: usize,
    done: mpsc::Sender<io::Result<()>>,
    gathered: bool,
}

fn finish_flush(w: FlushWait, result: io::Result<()>, outstanding: &AtomicUsize) {
    outstanding.fetch_sub(1, Ordering::SeqCst);
    let _ = w.done.send(result);
}

fn enqueue_flush(
    mut wait: FlushWait,
    sender: &mpsc::Sender<FlushWait>,
    outstanding: &AtomicUsize,
    notice_gate: &Mutex<()>,
    after_classification: impl FnOnce(),
) {
    let _notice = notice_gate.lock().unwrap();
    wait.gathered = outstanding.fetch_add(1, Ordering::SeqCst) != 0;
    after_classification();
    if let Err(e) = sender.send(wait) {
        finish_flush(
            e.0,
            Err(io::Error::other("flush owner stopped")),
            outstanding,
        );
    }
}

// No marker code or environment lookup exists in the normal release build.
#[cfg(all(windows, feature = "sync-witness"))]
fn sync_file(file: &File) -> io::Result<()> {
    use std::{ffi::c_void, os::windows::io::AsRawHandle};
    #[link(name = "kernel32")]
    extern "system" {
        fn FlushFileBuffers(handle: *mut c_void) -> i32;
    }
    let marks = std::env::var_os("MEMBUS_SYNC_WITNESS").map(std::path::PathBuf::from);
    if let Some(path) = &marks {
        if let Err(error) = fs::write(path.with_extension("entering"), b"entering") {
            eprintln!("sync-witness entering mark failed: {error}");
        }
    }
    let started = std::time::Instant::now();
    let ok = unsafe { FlushFileBuffers(file.as_raw_handle()) };
    let elapsed = started.elapsed().as_nanos();
    let result = if ok == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    };
    if let Some(path) = &marks {
        if let Err(error) = fs::write(
            path.with_extension("returned"),
            format!("{{\"call_ns\":{elapsed},\"ok\":{}}}", ok != 0),
        ) {
            eprintln!("sync-witness returned mark failed: {error}");
        }
    }
    result
}
#[cfg(not(all(windows, feature = "sync-witness")))]
fn sync_file(file: &File) -> io::Result<()> {
    file.sync_all()
}

// Test-only kill checkpoints, enabled explicitly by a path-valued environment
// variable. The marker proves the process reached the injection point.
fn checkpoint(name: &str) {
    if let Some(path) = std::env::var_os(name) {
        let _ = fs::write(path, b"reached");
        std::thread::sleep(Duration::from_secs(40));
    }
}
fn write_record(log: &mut Log, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        log.write_calls += 1;
        match log.file.write(bytes) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::WriteZero, "append")),
            Ok(n) => {
                log.bytes += n as u64;
                bytes = &bytes[n..];
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
fn start_owners(
    logs: &[Arc<Mutex<Log>>],
    config: &Config,
) -> (Vec<AppendOwner>, std::thread::JoinHandle<()>) {
    let (flush_send, flush_recv) = mpsc::channel::<FlushWait>();
    // Includes queued and currently syncing waiters. Classify at arrival,
    // not receipt: a notice queued during a sync must retain its gather policy.
    let outstanding = Arc::new(AtomicUsize::new(0));
    // Keep arrival classification and channel order together across stripes.
    // Without this, a preempted idle sender could be overtaken by a gathered
    // sender and inherit that later sender's window.
    let notice_gate = Arc::new(Mutex::new(()));
    let flush_outstanding = outstanding.clone();
    let flush_logs = logs.to_vec();
    let interval = config.flush_ms;
    let fail_sync = config.fail_sync;
    #[cfg(test)]
    let sync_test_gate = config.sync_test_gate.clone();
    let flush_owner = std::thread::spawn(move || {
        let mut pending = VecDeque::new();
        let mut last: Option<std::time::Instant> = None;
        let mut covered = [0u64; 4];
        let mut sync_failed = [false; 4];
        loop {
            let mut gather = true;
            if pending.is_empty() {
                match flush_recv.recv() {
                    Ok(w) if sync_failed[w.stripe] => {
                        finish_flush(
                            w,
                            Err(io::Error::other("stripe fenced after failed sync")),
                            &flush_outstanding,
                        );
                        continue;
                    }
                    Ok(w) if w.end <= covered[w.stripe] => {
                        finish_flush(w, Ok(()), &flush_outstanding);
                        continue;
                    }
                    Ok(w) => {
                        gather = w.gathered;
                        pending.push_back(w);
                    }
                    Err(_) => break,
                }
                // The gather window closes on elapsed time; bounds are enforced
                // when selecting each handle's covering group below.
                let until = std::time::Instant::now()
                    + Duration::from_millis(if gather { interval } else { 0 });
                while let Some(left) = until.checked_duration_since(std::time::Instant::now()) {
                    match flush_recv.recv_timeout(left) {
                        Ok(w) if sync_failed[w.stripe] => {
                            finish_flush(
                                w,
                                Err(io::Error::other("stripe fenced after failed sync")),
                                &flush_outstanding,
                            );
                        }
                        Ok(w) if w.end <= covered[w.stripe] => {
                            finish_flush(w, Ok(()), &flush_outstanding);
                        }
                        Ok(w) => pending.push_back(w),
                        Err(_) => break,
                    }
                }
            }
            while let Ok(w) = flush_recv.try_recv() {
                if sync_failed[w.stripe] {
                    finish_flush(
                        w,
                        Err(io::Error::other("stripe fenced after failed sync")),
                        &flush_outstanding,
                    );
                } else if w.end <= covered[w.stripe] {
                    finish_flush(w, Ok(()), &flush_outstanding);
                } else {
                    pending.push_back(w);
                }
            }
            let stripe = pending.front().unwrap().stripe;
            let mut group = Vec::new();
            let mut bytes = 0;
            let mut retained = VecDeque::new();
            while let Some(w) = pending.pop_front() {
                if w.stripe == stripe
                    && group.len() < 64
                    && (group.is_empty() || bytes + w.bytes <= 144_192)
                {
                    bytes += w.bytes;
                    group.push(w);
                } else {
                    retained.push_back(w);
                }
            }
            pending = retained;
            if let Some(previous) = last.filter(|_| gather) {
                if let Some(delay) = Duration::from_millis(interval).checked_sub(previous.elapsed())
                {
                    std::thread::sleep(delay);
                }
            }
            let (file, through) = {
                let log = flush_logs[stripe].lock().unwrap();
                (log.file.try_clone(), log.bytes)
            };
            let result = file.and_then(|f| {
                checkpoint("MEMBUS_TEST_PAUSE_SYNC");
                #[cfg(test)]
                if let Some(gate) = &sync_test_gate {
                    let mut state = gate.0.lock().unwrap();
                    if !state.0 {
                        state.0 = true;
                        gate.1.notify_all();
                        while !state.1 {
                            state = gate.1.wait(state).unwrap();
                        }
                    }
                }
                if fail_sync {
                    Err(io::Error::other("test injected sync failure"))
                } else {
                    sync_file(&f)
                }
            });
            last = Some(std::time::Instant::now());
            if result.is_ok() {
                covered[stripe] = covered[stripe].max(through);
            } else {
                sync_failed[stripe] = true;
            }
            {
                let mut log = flush_logs[stripe].lock().unwrap();
                log.flushes += 1;
                if result.is_err() {
                    log.poisoned = true;
                }
            }
            // A sync covers every earlier complete append on this handle,
            // including queued waiters beyond the nominal selected group.
            let mut extra = VecDeque::new();
            while let Some(w) = pending.pop_front() {
                if w.stripe == stripe && w.end <= through {
                    group.push(w);
                } else {
                    extra.push_back(w);
                }
            }
            pending = extra;
            for w in group {
                let r = match &result {
                    Ok(()) => Ok(()),
                    Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
                };
                finish_flush(w, r, &flush_outstanding);
            }
        }
    });
    let owners = logs
        .iter()
        .enumerate()
        .map(|(stripe, log)| {
            let log = log.clone();
            let config = config.clone();
            let flush = flush_send.clone();
            let outstanding = outstanding.clone();
            let notice_gate = notice_gate.clone();
            let (sender, receiver) = mpsc::channel::<Append>();
            let handle = std::thread::spawn(move || {
                while let Ok(job) = receiver.recv() {
                    if config.write_delay_ms > 0 {
                        std::thread::sleep(Duration::from_millis(config.write_delay_ms));
                    }
                    let mut log = log.lock().unwrap();
                    let result = if log.poisoned {
                        Err(io::Error::other("stripe fenced after failed append"))
                    } else if config.fail_partial_append
                        || std::env::var_os("MEMBUS_TEST_PAUSE_PARTIAL").is_some()
                    {
                        let half = job.record.len() / 2;
                        let r = write_record(&mut log, &job.record[..half]);
                        checkpoint("MEMBUS_TEST_PAUSE_PARTIAL");
                        log.poisoned = true;
                        r.and(Err(io::Error::other(
                            "test injected partial append failure",
                        )))
                    } else {
                        write_record(&mut log, &job.record)
                    };
                    if result.is_err() {
                        log.poisoned = true;
                    }
                    let end = log.bytes;
                    let result = if result.is_ok() && config.fail_after_append {
                        Err(io::Error::other("test injected post-admission I/O failure"))
                    } else {
                        result
                    };
                    drop(log);
                    if result.is_ok() && job.ack == 2 {
                        if config.flush_notice_delay_ms > 0 {
                            std::thread::sleep(Duration::from_millis(config.flush_notice_delay_ms));
                        }
                        let wait = FlushWait {
                            stripe,
                            end,
                            bytes: job.record.len(),
                            done: job.done,
                            gathered: false,
                        };
                        enqueue_flush(wait, &flush, &outstanding, &notice_gate, || {});
                        #[cfg(test)]
                        if let Some(notice) = &config.sync_test_notice {
                            let _ = notice.send(());
                        }
                    } else {
                        let _ = job.done.send(result);
                    }
                }
            });
            AppendOwner {
                sender: Some(sender),
                handle: Some(handle),
            }
        })
        .collect();
    (owners, flush_owner)
}

#[cfg(test)]
mod flush_policy_tests {
    use super::*;
    fn mutation(key: &str) -> Mutation {
        Mutation {
            ns: "ledger".into(),
            key: key.into(),
            op_id: key.into(),
            deadline: now_ms() + 10000,
            ack: 2,
            expected: Some(0),
            bytes: vec![1; 1039],
            remove: false,
        }
    }
    fn path(label: &str) -> std::path::PathBuf {
        std::path::PathBuf::from(
            std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()),
        )
        .join(format!(
            "policy-{label}-{}-{}",
            std::process::id(),
            now_ms()
        ))
    }
    #[test]
    fn consecutive_idle_flushes_bypass_gather_and_spacing() {
        let store = Store::open(
            &path("idle"),
            Config {
                flush_ms: 2000,
                ..Config::default()
            },
        )
        .unwrap();
        for key in ["key1", "key3", "key4"] {
            let start = std::time::Instant::now();
            assert_eq!(store.mutate(mutation(key)).ack, 2);
            assert!(
                start.elapsed() < Duration::from_millis(1000),
                "idle flush waited gather interval"
            );
        }
        assert_eq!(store.io_metrics().2, 3);
    }
    #[test]
    fn notice_arriving_during_sync_keeps_its_gather_window() {
        let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
        let (notice, notices) = mpsc::channel();
        let store = Arc::new(
            Store::open(
                &path("during"),
                Config {
                    flush_ms: 200,
                    sync_test_gate: Some(gate.clone()),
                    sync_test_notice: Some(notice),
                    ..Config::default()
                },
            )
            .unwrap(),
        );
        assert_eq!(crc32(b"ledger\0key1") % 4, crc32(b"ledger\0key3") % 4);
        let a = store.clone();
        let first = std::thread::spawn(move || a.mutate(mutation("key1")));
        let mut state = gate.0.lock().unwrap();
        while !state.0 {
            state = gate
                .1
                .wait_timeout(state, Duration::from_secs(2))
                .unwrap()
                .0;
        }
        drop(state);
        notices.recv_timeout(Duration::from_secs(2)).unwrap();
        let b = store.clone();
        let second = std::thread::spawn(move || b.mutate(mutation("key3")));
        let timeout = std::time::Instant::now();
        while store.io_metrics().1 < 2 {
            assert!(timeout.elapsed() < Duration::from_secs(2));
            std::thread::yield_now();
        }
        // This witnesses actual enqueue, without relying on a scheduling sleep.
        notices.recv_timeout(Duration::from_secs(2)).unwrap();
        let released = std::time::Instant::now();
        gate.0.lock().unwrap().1 = true;
        gate.1.notify_all();
        assert_eq!(first.join().unwrap().ack, 2);
        assert_eq!(second.join().unwrap().ack, 2);
        assert!(released.elapsed() >= Duration::from_millis(190));
        assert_eq!(store.io_metrics().2, 2);
    }

    #[test]
    fn preempted_idle_sender_cannot_be_overtaken_by_queued_sender() {
        let (sender, receiver) = mpsc::channel();
        let outstanding = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Mutex::new(()));
        let (entered, arrived) = mpsc::channel();
        let (release, resume) = mpsc::channel();
        let (done, _) = mpsc::channel();
        let wait = |end| FlushWait {
            stripe: 0,
            end,
            bytes: 1,
            done: done.clone(),
            gathered: false,
        };
        let first = wait(1);
        let second = wait(2);
        let (s, o, g) = (sender.clone(), outstanding.clone(), gate.clone());
        let a = std::thread::spawn(move || {
            enqueue_flush(first, &s, &o, &g, || {
                entered.send(()).unwrap();
                resume.recv_timeout(Duration::from_secs(2)).unwrap();
            })
        });
        arrived.recv_timeout(Duration::from_secs(2)).unwrap();
        let (s, o, g) = (sender.clone(), outstanding.clone(), gate.clone());
        let b = std::thread::spawn(move || enqueue_flush(second, &s, &o, &g, || {}));
        assert!(matches!(
            receiver.recv_timeout(Duration::from_millis(20)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        release.send(()).unwrap();
        a.join().unwrap();
        b.join().unwrap();
        let first = receiver.recv().unwrap();
        let second = receiver.recv().unwrap();
        assert_eq!((first.end, first.gathered), (1, false));
        assert_eq!((second.end, second.gathered), (2, true));
        assert_eq!(outstanding.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn late_notice_for_already_covered_extent_needs_no_second_sync() {
        let dir = path("late-covered");
        fs::create_dir_all(&dir).unwrap();
        let logs: Vec<_> = (0..4)
            .map(|n| {
                Arc::new(Mutex::new(Log {
                    file: OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(dir.join(format!("stripe{n}")))
                        .unwrap(),
                    bytes: 0,
                    write_calls: 0,
                    flushes: 0,
                    poisoned: false,
                }))
            })
            .collect();
        let (mut appenders, worker) = start_owners(
            &logs,
            &Config {
                flush_ms: 2000,
                ..Config::default()
            },
        );
        let (done, complete) = mpsc::channel();
        appenders[0]
            .sender
            .as_ref()
            .unwrap()
            .send(Append {
                record: vec![1; 128],
                ack: 2,
                done: done.clone(),
            })
            .unwrap();
        complete
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        // An empty append creates a genuinely late notice for the same complete
        // extent. This owner-level seam tests coverage reuse, not wire admission.
        appenders[0]
            .sender
            .as_ref()
            .unwrap()
            .send(Append {
                record: vec![],
                ack: 2,
                done,
            })
            .unwrap();
        complete
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!(logs[0].lock().unwrap().flushes, 1);
        for owner in &mut appenders {
            owner.sender.take();
        }
        for owner in &mut appenders {
            owner.handle.take().unwrap().join().unwrap();
        }
        worker.join().unwrap();
    }
}
impl Drop for Store {
    fn drop(&mut self) {
        if let Some((stop, worker)) = self.scan_reaper.take() {
            let _ = stop.send(());
            let _ = worker.join();
        }
        for owner in &mut self.appenders {
            owner.sender.take();
        }
        for owner in &mut self.appenders {
            if let Some(h) = owner.handle.take() {
                let _ = h.join();
            }
        }
        if let Some(h) = self.flush_owner.take() {
            let _ = h.join();
        }
    }
}
fn mutation_bytes(m: &Mutation, version: u64, at: u64) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend(at.to_le_bytes());
    b.extend(version.to_le_bytes());
    b.push(m.ack);
    b.push(m.remove as u8);
    lp(&mut b, m.ns.as_bytes());
    lp(&mut b, m.key.as_bytes());
    lp(&mut b, m.op_id.as_bytes());
    b.extend(m.expected.unwrap_or(u64::MAX).to_le_bytes());
    lp(&mut b, &m.bytes);
    b
}
fn fingerprint(m: &Mutation) -> Vec<u8> {
    mutation_bytes(m, 0, 0)
}
fn stripe(ns: &str, key: &str) -> usize {
    let mut b = ns.as_bytes().to_vec();
    b.push(0);
    b.extend(key.as_bytes());
    crc32(&b) as usize % 4
}
impl Store {
    pub fn open(path: &Path, config: Config) -> io::Result<Self> {
        if config.snapshot_ttl_ms == 0 {
            return Err(io::Error::other("snapshot lifetime must be positive"));
        }
        fs::create_dir_all(path)?;
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(false);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options.share_mode(0);
        }
        #[cfg(not(windows))]
        {
            options.create_new(true);
        }
        let owner = options.open(path.join("owner.lock"))?;
        Self::open_owned(path, config, owner)
    }
    fn open_owned(path: &Path, config: Config, owner: File) -> io::Result<Self> {
        let mut committed: HashMap<(String, String), Arc<Value>> = HashMap::new();
        let mut dedup = HashMap::new();
        let mut next = 1u64;
        let mut logs = Vec::new();
        let mut torn = Vec::new();
        let mut persisted_bytes = 0;
        let now = now_ms();
        for i in 0..4 {
            let p = path.join(format!("stripe-{i}.log"));
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&p)?;
            let mut offset = 0u64;
            let len = f.metadata()?.len();
            while offset < len {
                let mut head = [0; 16];
                if len - offset < 16 {
                    torn.push((p.clone(), offset));
                    break;
                }
                f.read_exact(&mut head)?;
                if &head[0..4] != b"MBK1" {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("interior-corrupt {} offset {}: magic", p.display(), offset),
                    ));
                }
                let n = u32::from_le_bytes(head[4..8].try_into().unwrap()) as usize;
                if u32::from_le_bytes(head[8..12].try_into().unwrap()) != !(n as u32) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "interior-corrupt: length complement",
                    ));
                }
                if n > MAX_FRAME {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "interior-corrupt: invalid chunk length",
                    ));
                }
                if len - offset - 16 < n as u64 {
                    torn.push((p.clone(), offset));
                    break;
                }
                let mut b = vec![0; n];
                f.read_exact(&mut b)?;
                if crc32(&b) != u32::from_le_bytes(head[12..16].try_into().unwrap()) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("interior-corrupt {} offset {}: CRC", p.display(), offset),
                    ));
                }
                let parsed = (|| -> Result<_, u16> {
                    let mut c = Cursor::new(&b);
                    let at = c.u64()?;
                    let version = c.u64()?;
                    let ack = c.u8()?;
                    let remove = c.u8()? != 0;
                    let ns = c.text()?;
                    let key = c.text()?;
                    let op_id = c.text()?;
                    let expected = c.u64()?;
                    let bytes = c.bytes(MAX_VALUE)?.to_vec();
                    c.end()?;
                    if !matches!(ack, 1 | 2) {
                        return Err(3);
                    }
                    Ok((
                        Mutation {
                            ns,
                            key,
                            op_id,
                            deadline: 0,
                            ack,
                            expected: if expected == u64::MAX {
                                None
                            } else {
                                Some(expected)
                            },
                            bytes,
                            remove,
                        },
                        version,
                        at,
                    ))
                })();
                let (m, v, at) = parsed.map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "interior-corrupt: payload")
                })?;
                if stripe(&m.ns, &m.key) != i {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "interior-corrupt: wrong stripe",
                    ));
                }
                next = next.max(v + 1);
                let k = (m.ns.clone(), m.key.clone());
                let value = Value {
                    version: v,
                    ack: m.ack,
                    bytes: m.bytes.clone(),
                    tombstone: m.remove,
                };
                if committed
                    .get(&k)
                    .map_or(true, |old: &Arc<Value>| old.version < v)
                {
                    committed.insert(k, Arc::new(value));
                }
                if now.saturating_sub(at) <= config.retry_ms {
                    dedup.insert(
                        (m.ns.clone(), m.op_id.clone()),
                        Dedup {
                            fingerprint: fingerprint(&m),
                            at,
                            outcome: Some(Outcome {
                                code: 0,
                                version: v,
                                ack: m.ack,
                            }),
                        },
                    );
                }
                offset += 16 + n as u64;
            }
            persisted_bytes += offset;
            drop(f);
            let file = OpenOptions::new().append(true).open(&p)?;
            logs.push(Arc::new(Mutex::new(Log {
                file,
                bytes: 0,
                flushes: 0,
                write_calls: 0,
                poisoned: false,
            })));
        }
        if committed.len() > config.index_cap
            || committed.values().map(|v| v.bytes.len()).sum::<usize>()
                + dedup
                    .values()
                    .map(|d: &Dedup| d.fingerprint.len())
                    .sum::<usize>()
                > config.capacity
            || dedup.len() > config.dedup_cap
        {
            return Err(io::Error::other("replayed state exceeds configured limits"));
        }
        for (p, offset) in torn {
            OpenOptions::new().write(true).open(p)?.set_len(offset)?;
        }
        let epoch_path = path.join("epoch");
        let previous = if epoch_path.exists() {
            let b = fs::read(&epoch_path)?;
            if b.len() != 8 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid epoch"));
            }
            u64::from_le_bytes(b.try_into().unwrap())
        } else {
            0
        };
        let epoch = previous
            .checked_add(1)
            .ok_or_else(|| io::Error::other("epoch exhausted"))?;
        let epoch_tmp = path.join("epoch.tmp");
        {
            let mut f = File::create(&epoch_tmp)?;
            f.write_all(&epoch.to_le_bytes())?;
            f.sync_all()?;
        }
        fs::rename(epoch_tmp, epoch_path)?;
        let metadata: HashMap<_, _> = committed
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    Meta {
                        version: v.version,
                        tombstone: v.tombstone,
                        bytes: v.bytes.len(),
                    },
                )
            })
            .collect();
        let mut namespace_counts = HashMap::new();
        for (ns, _) in metadata.keys() {
            *namespace_counts.entry(ns.clone()).or_insert(0) += 1;
        }
        let keys = metadata.len();
        let committed_bytes = metadata.values().map(|m| m.bytes).sum();
        let dedup_bytes = dedup.values().map(|d| d.fingerprint.len()).sum();
        let mut expiry: Vec<_> = dedup.iter().map(|(k, d)| (k.clone(), d.at)).collect();
        expiry.sort_by_key(|(_, at)| *at);
        let (appenders, flush_owner) = start_owners(&logs, &config);
        let scans = Arc::new(Mutex::new(ScanState {
            next: 1,
            snapshots: HashMap::new(),
        }));
        let (stop, receiver) = mpsc::channel();
        let reaper_scans = scans.clone();
        let ttl = Duration::from_millis(config.snapshot_ttl_ms);
        let interval = ttl.min(Duration::from_secs(1));
        let reaper = std::thread::spawn(move || {
            while matches!(
                receiver.recv_timeout(interval),
                Err(mpsc::RecvTimeoutError::Timeout)
            ) {
                reaper_scans
                    .lock()
                    .unwrap()
                    .snapshots
                    .retain(|_, s| s.created.elapsed() < ttl);
            }
        });
        Ok(Self {
            signals: signals::Hub::new(epoch, &config),
            capture_metrics: std::array::from_fn(|_| AtomicU64::new(0)),
            committed: RwLock::new(committed),
            scans,
            scan_usage: Arc::new(ScanUsage::default()),
            scan_reaper: Some((stop, reaper)),
            state: Mutex::new(State {
                pending: HashMap::new(),
                metadata,
                namespace_counts,
                keys,
                committed_bytes,
                dedup_bytes,
                dedup,
                retired: VecDeque::new(),
                retired_set: HashSet::new(),
                expiry: expiry.into(),
                next,
                reserved_bytes: 0,
                stripe_reserved: [0; 4],
                stripe_items: [0; 4],
                persisted_bytes,
                pending_disk: 0,
                read_only: false,
                warming: false,
            }),
            done: Condvar::new(),
            logs,
            appenders,
            flush_owner: Some(flush_owner),
            batch_gate: Mutex::new(()),
            config,
            epoch,
            _owner: owner,
        })
    }
    pub fn get(&self, ns: &str, key: &str) -> Option<Value> {
        self.get_handle(ns, key).map(|v| (*v).clone())
    }
    fn get_handle(&self, ns: &str, key: &str) -> Option<Arc<Value>> {
        self.committed
            .read()
            .unwrap()
            .get(&(ns.to_owned(), key.to_owned()))
            .filter(|v| !v.tombstone)
            .cloned()
    }
    fn purge_scans(&self, state: &mut ScanState) {
        let ttl = Duration::from_millis(self.config.snapshot_ttl_ms);
        state.snapshots.retain(|_, s| s.created.elapsed() < ttl);
    }
    fn snapshot_open(&self, ns: &str) -> Result<(Vec<u8>, Arc<Snapshot>), u16> {
        let created = Instant::now();
        let mut scans = self.scans.lock().unwrap();
        self.purge_scans(&mut scans);
        if self.scan_usage.count.load(Ordering::Acquire) >= self.config.snapshot_max {
            return Err(22);
        }
        let lock_start = Instant::now();
        let committed = self.committed.read().unwrap();
        let lock_acquired = Instant::now();
        let mut charge = 256usize.checked_add(ns.len()).ok_or(22u16)?;
        let mut count = 0;
        for ((namespace, key), value) in committed.iter() {
            if namespace == ns && !value.tombstone {
                // Full payload capacity is charged even when shared with the live
                // index or another snapshot; 128 covers per-row handles/metadata.
                charge = charge
                    .checked_add(key.len())
                    .and_then(|n| n.checked_add(128))
                    .and_then(|n| n.checked_add(value.bytes.capacity()))
                    .ok_or(22u16)?;
                count += 1;
            }
        }
        let charge_end = Instant::now();
        let total = self
            .scan_usage
            .bytes
            .load(Ordering::Acquire)
            .checked_add(charge)
            .ok_or(22u16)?;
        if total > self.config.snapshot_bytes {
            return Err(22);
        }
        let id = scans.next;
        scans.next = id.checked_add(1).ok_or(22u16)?;
        // Reservation is serialized before any snapshot key/handle allocation.
        self.scan_usage.bytes.fetch_add(charge, Ordering::AcqRel);
        self.scan_usage.count.fetch_add(1, Ordering::AcqRel);
        let reservation = ScanCharge {
            usage: self.scan_usage.clone(),
            bytes: charge,
        };
        let selection_start = Instant::now();
        // Separate the second global index walk from namespace-owned copying.
        // These borrowed references stay under the same committed read lock.
        // The temporary pointers fit inside the conservative 128-byte row charge.
        let selected: Vec<_> = committed.iter()
            .filter(|((namespace, _), value)| namespace == ns && !value.tombstone)
            .collect();
        let selection_end = Instant::now();
        let mut rows = Vec::with_capacity(count);
        for ((_, key), value) in selected { rows.push((key.clone(), value.clone())); }
        let copy_end = Instant::now();
        drop(committed);
        drop(scans);
        rows.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let sort_end = Instant::now();
        let times = [1, lock_acquired.duration_since(lock_start).as_nanos() as u64,
            charge_end.duration_since(lock_acquired).as_nanos() as u64,
            selection_end.duration_since(selection_start).as_nanos() as u64,
            copy_end.duration_since(selection_end).as_nanos() as u64,
            copy_end.duration_since(lock_acquired).as_nanos() as u64,
            sort_end.duration_since(copy_end).as_nanos() as u64];
        for (metric, n) in self.capture_metrics.iter().zip(times) { metric.fetch_add(n, Ordering::Relaxed); }
        let snapshot = Arc::new(Snapshot {
            ns: ns.to_owned(),
            created,
            rows,
            cursor_hash: RandomState::new(),
            _charge: reservation,
        });
        if snapshot.created.elapsed() >= Duration::from_millis(self.config.snapshot_ttl_ms) {
            return Err(20);
        }
        self.scans
            .lock()
            .unwrap()
            .snapshots
            .insert(id, snapshot.clone());
        let mut token = self.epoch.to_le_bytes().to_vec();
        token.extend(id.to_le_bytes());
        Ok((token, snapshot))
    }
    fn snapshot_get(&self, ns: &str, token: &[u8]) -> Result<Arc<Snapshot>, u16> {
        if token.len() != 16 {
            return Err(21);
        }
        if u64::from_le_bytes(token[..8].try_into().unwrap()) != self.epoch {
            return Err(20);
        }
        let id = u64::from_le_bytes(token[8..].try_into().unwrap());
        let mut scans = self.scans.lock().unwrap();
        self.purge_scans(&mut scans);
        let snapshot = scans.snapshots.get(&id).ok_or(20u16)?;
        if snapshot.ns != ns {
            return Err(21);
        }
        Ok(snapshot.clone())
    }
    fn snapshot_release(&self, ns: &str, token: &[u8]) -> Result<(), u16> {
        let snapshot = self.snapshot_get(ns, token)?;
        let id = u64::from_le_bytes(token[8..].try_into().unwrap());
        self.scans.lock().unwrap().snapshots.remove(&id);
        drop(snapshot);
        Ok(())
    }
    fn scan_page(
        &self,
        ns: &str,
        c: &mut Cursor<'_>,
        op: u8,
        max_frame: usize,
    ) -> Result<Vec<u8>, u16> {
        let keys_only = matches!(op, SCAN_KEYS_PREFIX | SCAN_KEYS_RANGE);
        let is_range = matches!(op, SCAN_RANGE | SCAN_KEYS_RANGE);
        let supplied = c.bytes(16)?.to_vec();
        let cursor = c.bytes(2 * MAX_ID + 48)?;
        let limit = c.u16()? as usize;
        let budget = (c.u32()? as usize).min(max_frame);
        let lower = c.bound()?;
        let upper = if is_range { c.bound()? } else { String::new() };
        c.end()?;
        if limit == 0
            || limit > MAX_BATCH
            || budget < 42
            || (is_range && !upper.is_empty() && lower > upper)
        {
            return Err(3);
        }
        let mut query = vec![op];
        lp(&mut query, lower.as_bytes());
        if is_range {
            lp(&mut query, upper.as_bytes());
        }
        // A cursor must be accompanied by its snapshot, and binds the full
        // query plus an absolute index. Page size/budget can change on retry.
        let position = if cursor.is_empty() {
            None
        } else {
            if supplied.len() != 16
                || cursor.len() != 32 + query.len()
                || cursor[..16] != supplied
                || cursor[24..cursor.len() - 8] != query
            {
                return Err(21);
            }
            Some(
                usize::try_from(u64::from_le_bytes(cursor[16..24].try_into().unwrap()))
                    .map_err(|_| 21u16)?,
            )
        };
        let newly_opened = supplied.is_empty();
        let (token, snapshot) = if newly_opened {
            self.snapshot_open(ns)?
        } else {
            (supplied.clone(), self.snapshot_get(ns, &supplied)?)
        };
        let result = (|| {
            if !cursor.is_empty() {
                let split = cursor.len() - 8;
                let tag = u64::from_le_bytes(cursor[split..].try_into().unwrap());
                if snapshot.cursor_hash.hash_one(&cursor[..split]) != tag {
                    return Err(21);
                }
            }
            let rows = &snapshot.rows;
            let start = rows.partition_point(|(k, _)| k < &lower);
            let end = if !is_range {
                // The matching block is contiguous under UTF-8 byte order.
                start + rows[start..].partition_point(|(k, _)| k.starts_with(&lower))
            } else if upper.is_empty() {
                rows.len()
            } else {
                rows.partition_point(|(k, _)| k < &upper)
            };
            let at = position.unwrap_or(start);
            if at < start || at > end {
                return Err(21);
            }
            let mut next = at;
            let mut item_bytes = 0;
            while next < end && next - at < limit {
                let (key, value) = &rows[next];
                let bytes = 4 + key.len() + if keys_only { 0 } else { 14 + value.bytes.len() };
                let continuation = if next + 1 < end { 32 + query.len() } else { 0 };
                if 42 + item_bytes + bytes + continuation > budget {
                    if next == at {
                        return Err(4);
                    }
                    break;
                }
                item_bytes += bytes;
                next += 1;
            }
            let mut continuation = Vec::new();
            if next < end {
                continuation.extend(&token);
                continuation.extend((next as u64).to_le_bytes());
                continuation.extend(&query);
                let tag = snapshot.cursor_hash.hash_one(&continuation[..]);
                continuation.extend(tag.to_le_bytes());
            }
            let mut p = Vec::with_capacity(27 + item_bytes + continuation.len());
            lp(&mut p, &token);
            lp(&mut p, &continuation);
            p.push((next == end) as u8);
            p.extend(((next - at) as u16).to_le_bytes());
            for (key, value) in &rows[at..next] {
                lp(&mut p, key.as_bytes());
                if !keys_only {
                    encode_value(&mut p, Some(value));
                }
            }
            if snapshot.created.elapsed() >= Duration::from_millis(self.config.snapshot_ttl_ms) {
                return Err(20);
            }
            Ok(p)
        })();
        if result.is_err() && newly_opened {
            // Includes a first item too large for the negotiated frame/budget.
            let id = u64::from_le_bytes(token[8..].try_into().unwrap());
            self.scans.lock().unwrap().snapshots.remove(&id);
        }
        result
    }
    pub fn metrics(&self) -> (u64, u64, usize) {
        let mut b = 0;
        let mut f = 0;
        for l in &self.logs {
            let l = l.lock().unwrap();
            b += l.bytes;
            f += l.flushes;
        }
        (b, f, self.committed.read().unwrap().len())
    }
    pub fn state_bytes(&self) -> u64 {
        self.state.lock().unwrap().persisted_bytes
    }
    pub fn io_metrics(&self) -> (u64, u64, u64) {
        let (mut bytes, mut writes, mut flushes) = (0, 0, 0);
        for log in &self.logs {
            let log = log.lock().unwrap();
            bytes += log.bytes;
            writes += log.write_calls;
            flushes += log.flushes;
        }
        (bytes, writes, flushes)
    }
    pub fn set_read_only(&self, b: bool) {
        self.state.lock().unwrap().read_only = b
    }
    pub fn set_warming(&self, b: bool) {
        self.state.lock().unwrap().warming = b
    }
    pub fn pending(&self, ns: &str, key: &str) -> bool {
        self.state
            .lock()
            .unwrap()
            .pending
            .contains_key(&(ns.into(), key.into()))
    }
    pub fn mutate(&self, m: Mutation) -> Outcome {
        if m.ns.is_empty() || m.key.is_empty() || m.op_id.is_empty() {
            return Outcome::error(3);
        }
        if m.ns.len() > MAX_ID
            || m.key.len() > MAX_ID
            || m.op_id.len() > MAX_ID
            || m.bytes.len() > MAX_VALUE
        {
            return Outcome::error(4);
        }
        if m.ack > 2 {
            return Outcome::error(3);
        }
        if m.ack == 0 && !self.config.allow_mem {
            return Outcome::error(15);
        }
        let k = (m.ns.clone(), m.key.clone());
        let id = (m.ns.clone(), m.op_id.clone());
        let fp = fingerprint(&m);
        let at = now_ms();
        let log_index = stripe(&m.ns, &m.key);
        let disk_bytes = if m.ack == 0 {
            0
        } else {
            (16 + mutation_bytes(&m, 0, 0).len()) as u64
        };
        // Charge complete queued representation, not just value payloads. Empty
        // values still consume queue slots and framing/identifier memory.
        let queue_charge = fp.len() + m.bytes.len() + 128;
        let mut state = self.state.lock().unwrap();
        if let Some(d) = state.dedup.get(&id) {
            if d.fingerprint != fp {
                return Outcome::error(14);
            }
            if at.saturating_sub(d.at) > self.config.retry_ms {
                return Outcome::error(13);
            }
            loop {
                if let Some(o) = state.dedup.get(&id).and_then(|d| d.outcome.clone()) {
                    return o;
                }
                let wait = m.deadline.saturating_sub(now_ms());
                if wait == 0 {
                    return Outcome::error(17);
                }
                let (s, _) = self
                    .done
                    .wait_timeout(state, Duration::from_millis(wait))
                    .unwrap();
                state = s;
            }
        }
        if state.retired_set.contains(&id) {
            return Outcome::error(13);
        }
        if now_ms() >= m.deadline {
            return Outcome::error(5);
        }
        if state.warming {
            return Outcome::error(11);
        }
        if state.read_only {
            return Outcome::error(10);
        }
        let old = state.metadata.get(&k).cloned();
        if let Some(expected) = m.expected {
            let pending = state.pending.get(&k);
            if pending.is_some() {
                return Outcome::error(19);
            }
            let visible = pending.or(old.as_ref());
            let matches = if expected == 0 {
                pending.is_none() && old.as_ref().map_or(true, |v| v.tombstone)
            } else {
                visible.is_some_and(|v| !v.tombstone && v.version == expected)
            };
            if !matches {
                return Outcome::error(12);
            }
        }
        if state.pending.contains_key(&k) {
            return Outcome::error(6);
        }
        let new = old.is_none();
        if new && state.keys >= self.config.index_cap {
            return Outcome::error(9);
        }
        if new && state.namespace_counts.get(&m.ns).copied().unwrap_or(0) >= self.config.quota {
            return Outcome::error(8);
        }
        while let Some((key, time)) = state.expiry.front().cloned() {
            if at.saturating_sub(time) <= self.config.retry_ms {
                break;
            }
            if state.dedup.get(&key).is_some_and(|d| d.outcome.is_none()) {
                break;
            }
            state.expiry.pop_front();
            if let Some(d) = state.dedup.remove(&key) {
                state.dedup_bytes -= d.fingerprint.len();
                state.retired_set.insert(key.clone());
                state.retired.push_back(key);
            }
        }
        while state.retired.len() > self.config.dedup_cap {
            let k = state.retired.pop_front().unwrap();
            state.retired_set.remove(&k);
        }
        if state.committed_bytes
            + state.dedup_bytes
            + state.reserved_bytes
            + m.bytes.len()
            + fp.len()
            > self.config.capacity
        {
            return Outcome::error(7);
        }
        if state.persisted_bytes + state.pending_disk + disk_bytes > self.config.state_limit_bytes {
            return Outcome::error(7);
        }
        if state.reserved_bytes + queue_charge > self.config.queue_bytes
            || state.stripe_reserved[log_index] + queue_charge > self.config.queue_bytes / 4
            || state.stripe_items[log_index] >= 1024
        {
            return Outcome::error(6);
        }
        if state.dedup.len() >= self.config.dedup_cap {
            return Outcome::error(18);
        }
        // The clock check immediately precedes insertion: this is the admission point.
        if now_ms() >= m.deadline {
            return Outcome::error(5);
        }
        let version = state.next;
        state.next += 1;
        let value = Value {
            version,
            ack: m.ack,
            bytes: m.bytes.clone(),
            tombstone: m.remove,
        };
        state.pending.insert(
            k.clone(),
            Meta {
                version,
                tombstone: m.remove,
                bytes: m.bytes.len(),
            },
        );
        if new {
            state.keys += 1;
            *state.namespace_counts.entry(m.ns.clone()).or_default() += 1;
        }
        state.reserved_bytes += queue_charge;
        state.stripe_reserved[log_index] += queue_charge;
        state.stripe_items[log_index] += 1;
        state.pending_disk += disk_bytes;
        state.dedup_bytes += fp.len();
        state.expiry.push_back((id.clone(), at));
        state.dedup.insert(
            id.clone(),
            Dedup {
                fingerprint: fp,
                at,
                outcome: None,
            },
        );
        drop(state);
        let result = if m.ack == 0 {
            Ok(())
        } else {
            let b = mutation_bytes(&m, version, at);
            let mut record = b"MBK1".to_vec();
            record.extend((b.len() as u32).to_le_bytes());
            record.extend((!(b.len() as u32)).to_le_bytes());
            record.extend(crc32(&b).to_le_bytes());
            record.extend(b);
            let (done, completion) = mpsc::channel();
            match self.appenders[log_index]
                .sender
                .as_ref()
                .unwrap()
                .send(Append {
                    record,
                    ack: m.ack,
                    done,
                }) {
                Ok(()) => completion
                    .recv()
                    .unwrap_or_else(|_| Err(io::Error::other("append owner stopped"))),
                Err(_) => Err(io::Error::other("append owner stopped")),
            }
        };
        // Publication precedes completion on every connection; readers never await log I/O.
        if result.is_ok() && m.ack != 0 {
            let value = Arc::new(value);
            let mut committed = self.committed.write().unwrap();
            let previous = committed.insert(k.clone(), value.clone());
            self.signals.publish(&m.ns, &m.key, value);
            drop(committed);
            drop(previous);
        }
        let outcome = if result.is_ok() {
            Outcome {
                code: 0,
                version,
                ack: m.ack,
            }
        } else {
            Outcome::error(16)
        };
        let mut state = self.state.lock().unwrap();
        state.reserved_bytes -= queue_charge;
        state.stripe_reserved[log_index] -= queue_charge;
        state.stripe_items[log_index] -= 1;
        state.pending_disk -= disk_bytes;
        // Count append-complete uncertain sync results too; a conservative
        // reservation after any failure avoids exceeding the disk cap.
        if m.ack != 0 {
            state.persisted_bytes += disk_bytes;
        }
        state.pending.remove(&k);
        if result.is_ok() && m.ack != 0 {
            state.committed_bytes =
                state.committed_bytes - old.as_ref().map_or(0, |v| v.bytes) + m.bytes.len();
            state.metadata.insert(
                k,
                Meta {
                    version,
                    tombstone: m.remove,
                    bytes: m.bytes.len(),
                },
            );
        } else if new {
            state.keys -= 1;
            *state.namespace_counts.get_mut(&m.ns).unwrap() -= 1;
        }
        if result.is_err() {
            state.read_only = true
        }
        state.dedup.get_mut(&id).unwrap().outcome = Some(outcome.clone());
        self.done.notify_all();
        outcome
    }
    pub fn dispatch(&self, b: &[u8]) -> Vec<u8> {
        self.dispatch_for(b, self.config.allow_mem)
    }
    pub fn dispatch_for(&self, b: &[u8], allow_mem: bool) -> Vec<u8> {
        self.dispatch_for_limit(b, allow_mem, MAX_FRAME)
    }
    pub fn dispatch_for_limit(&self, b: &[u8], allow_mem: bool, max_frame: usize) -> Vec<u8> {
        let max_frame = max_frame.min(MAX_FRAME);
        if b.len() > max_frame {
            return reply(
                b.get(4).copied().unwrap_or(0),
                b.get(5..13)
                    .map(|v| u64::from_le_bytes(v.try_into().unwrap()))
                    .unwrap_or(0),
                4,
                &[],
            );
        }
        let mut c = Cursor::new(b);
        let header = (|| -> Result<_, u16> { Ok((c.u16()?, c.u16()?, c.u8()?, c.u64()?)) })();
        let (major, minor, op, id) = match header {
            Ok(h) => h,
            Err(code) => return reply(0, 0, code, &[]),
        };
        if major != 1 || minor != 0 {
            return reply(op, id, 1, &[]);
        }
        let result = (|| -> Result<Vec<u8>, u16> {
            if op == HELLO {
                let max = c.u32()? as usize;
                let mem = c.u8()?;
                let client = c.text()?;
                c.end()?;
                if max < 1024 {
                    return Err(4);
                }
                if mem > 1 {
                    return Err(3);
                }
                let mut p = self.epoch.to_le_bytes().to_vec();
                p.extend((MAX_FRAME.min(max) as u32).to_le_bytes());
                p.extend((MAX_VALUE as u32).to_le_bytes());
                p.extend((MAX_BATCH as u16).to_le_bytes());
                p.push(self.config.allow_mem as u8);
                p.extend(self.config.retry_ms.to_le_bytes());
                // Legacy strict clients keep the exact original refusal list.
                let subscribe = client.split(';').any(|feature| feature == "subscribe=1");
                let scans = client.split(';').any(|feature| feature == "scans=1");
                let codes = if subscribe {
                    CODES
                } else if scans {
                    &CODES[..22]
                } else {
                    &CODES[..19]
                };
                p.extend((codes.len() as u16).to_le_bytes());
                for &(code, name, transient) in codes {
                    p.extend(code.to_le_bytes());
                    p.push(transient as u8);
                    lp(&mut p, name.as_bytes());
                }
                return Ok(p);
            }
            let deadline = c.u64()?;
            let _op_id = c.text()?;
            let ns = c.text()?;
            if op == PUT || op == CONDITIONAL_PUT || op == REMOVE {
                let m = parse_mutation(&mut c, op, deadline, _op_id, ns)?;
                c.end()?;
                if m.ack == 0 && !allow_mem {
                    return Err(15);
                }
                let o = self.mutate(m);
                if o.code != 0 {
                    return Err(o.code);
                }
                return Ok(o.bytes());
            }
            if now_ms() >= deadline {
                return Err(5);
            }
            match op {
                SUBSCRIBE => {
                    let prefix = c.bound()?;
                    let endpoint = c.text()?;
                    let values = c.u8()?;
                    if values > 1 { return Err(3); }
                    let cursor = c.bytes(16)?;
                    c.end()?;
                    self.signals.subscribe(&ns, prefix, endpoint, values != 0, cursor)
                }
                UNSUBSCRIBE | SUBSCRIPTION_STATUS => {
                    let token = c.bytes(16)?;
                    c.end()?;
                    if op == UNSUBSCRIBE {
                        self.signals.unsubscribe(&ns, token)?;
                        Ok(Vec::new())
                    } else { self.signals.status(&ns, token) }
                }
                SIGNAL_USAGE => { c.end()?; Ok(self.signals.usage()) }
                CAPTURE_METRICS => {
                    c.end()?;
                    let mut p = Vec::new();
                    for metric in &self.capture_metrics { p.extend(metric.load(Ordering::Relaxed).to_le_bytes()); }
                    Ok(p)
                }
                SNAPSHOT_OPEN => {
                    c.end()?;
                    let (token, _) = self.snapshot_open(&ns)?;
                    let mut p = Vec::new();
                    lp(&mut p, &token);
                    Ok(p)
                }
                SNAPSHOT_RELEASE => {
                    let token = c.bytes(16)?;
                    c.end()?;
                    self.snapshot_release(&ns, token)?;
                    Ok(Vec::new())
                }
                SCAN_PREFIX | SCAN_RANGE | SCAN_KEYS_PREFIX | SCAN_KEYS_RANGE => {
                    self.scan_page(&ns, &mut c, op, max_frame)
                }
                GET => {
                    let key = c.text()?;
                    c.end()?;
                    let mut p = Vec::new();
                    encode_value(&mut p, self.get_handle(&ns, &key).as_deref());
                    Ok(p)
                }
                GET_MANY => {
                    let n = c.u16()? as usize;
                    if n > MAX_BATCH {
                        return Err(4);
                    }
                    let mut keys = Vec::with_capacity(n);
                    for _ in 0..n {
                        keys.push(c.text()?)
                    }
                    c.end()?;
                    let mut p = (n as u16).to_le_bytes().to_vec();
                    for key in keys {
                        let value = self.get_handle(&ns, &key);
                        if p.len() + value.as_ref().map_or(1, |v| 14 + v.bytes.len()) + 15
                            > max_frame
                        {
                            return Err(4);
                        }
                        encode_value(&mut p, value.as_deref());
                    }
                    Ok(p)
                }
                BATCH => {
                    let n = c.u16()? as usize;
                    if n > MAX_BATCH {
                        return Err(4);
                    }
                    let mut items = Vec::with_capacity(n);
                    for _ in 0..n {
                        let op = c.u8()?;
                        if !matches!(op, PUT | CONDITIONAL_PUT | REMOVE) {
                            return Err(2);
                        }
                        let id = c.text()?;
                        let item = parse_mutation(&mut c, op, deadline, id, ns.clone())?;
                        if item.ack == 0 && !allow_mem {
                            return Err(15);
                        }
                        items.push(item)
                    }
                    c.end()?;
                    let mut p = (n as u16).to_le_bytes().to_vec();
                    // Distinct keys overlap, including their flush waits. Same-key
                    // operations retain input order; replies always retain input order.
                    let mut keys: HashMap<String, Vec<(usize, Mutation)>> = HashMap::new();
                    for (i, m) in items.into_iter().enumerate() {
                        keys.entry(m.key.clone()).or_default().push((i, m));
                    }
                    // One executing batch per store, at most 32 scoped helpers.
                    // Refuse before any item admission if another batch owns it.
                    let _batch = self.batch_gate.try_lock().map_err(|_| 6u16)?;
                    let helpers = keys.len().min(32);
                    let work = Mutex::new(keys.into_values());
                    let mut results = std::thread::scope(|scope| {
                        let handles: Vec<_> = (0..helpers)
                            .map(|_| {
                                let work = &work;
                                scope.spawn(move || {
                                    let mut out = Vec::new();
                                    loop {
                                        let items = work.lock().unwrap().next();
                                        let Some(items) = items else {
                                            break;
                                        };
                                        out.extend(
                                            items.into_iter().map(|(i, m)| (i, self.mutate(m))),
                                        );
                                    }
                                    out
                                })
                            })
                            .collect();
                        handles
                            .into_iter()
                            .flat_map(|h| h.join().unwrap())
                            .collect::<Vec<_>>()
                    });
                    results.sort_by_key(|(i, _)| *i);
                    for (_, o) in results {
                        p.extend(o.code.to_le_bytes());
                        p.extend(o.bytes());
                    }
                    Ok(p)
                }
                IO_METRICS => {
                    c.end()?;
                    let (bytes, writes, flushes) = self.io_metrics();
                    let mut p = bytes.to_le_bytes().to_vec();
                    p.extend(writes.to_le_bytes());
                    p.extend(flushes.to_le_bytes());
                    Ok(p)
                }
                HEALTH => {
                    c.end()?;
                    let (b, f, k) = self.metrics();
                    let mut p = b.to_le_bytes().to_vec();
                    p.extend(f.to_le_bytes());
                    p.extend((k as u64).to_le_bytes());
                    let namespace_keys = self
                        .state
                        .lock()
                        .unwrap()
                        .namespace_counts
                        .get(&ns)
                        .copied()
                        .unwrap_or(0);
                    p.extend((namespace_keys as u64).to_le_bytes());
                    p.extend((self.config.quota as u64).to_le_bytes());
                    p.extend((self.config.index_cap as u64).to_le_bytes());
                    p.push((k * 100 >= self.config.index_cap * 70) as u8);
                    p.push((namespace_keys * 100 >= self.config.quota * 70) as u8);
                    Ok(p)
                }
                _ => Err(2),
            }
        })();
        match result {
            Ok(p) if p.len() + 15 <= max_frame => reply(op, id, 0, &p),
            Ok(_) => reply(op, id, 4, &[]),
            Err(code) => reply(op, id, code, &[]),
        }
    }
}
fn parse_mutation(
    c: &mut Cursor<'_>,
    op: u8,
    deadline: u64,
    op_id: String,
    ns: String,
) -> Result<Mutation, u16> {
    let key = c.text()?;
    let ack = c.u8()?;
    let expected = if op == CONDITIONAL_PUT || op == REMOVE {
        Some(c.u64()?)
    } else {
        None
    };
    let bytes = if op == REMOVE {
        Vec::new()
    } else {
        c.bytes(MAX_VALUE)?.to_vec()
    };
    Ok(Mutation {
        ns,
        key,
        op_id,
        deadline,
        ack,
        expected,
        bytes,
        remove: op == REMOVE,
    })
}
fn encode_value(p: &mut Vec<u8>, v: Option<&Value>) {
    match v {
        Some(v) => {
            p.push(1);
            p.extend(v.version.to_le_bytes());
            p.push(v.ack);
            lp(p, &v.bytes)
        }
        None => p.push(0),
    }
}
pub fn reply(op: u8, id: u64, code: u16, p: &[u8]) -> Vec<u8> {
    let mut b = code.to_le_bytes().to_vec();
    b.extend(p);
    frame(op | 0x80, id, &b)
}

#[cfg(test)]
mod scan_retention_tests {
    use super::*;
    fn store(label: &str, ttl: u64) -> Store {
        let path = std::path::PathBuf::from(
            std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()),
        )
        .join(format!(
            "scan-retention-{label}-{}-{}",
            std::process::id(),
            now_ms()
        ));
        Store::open(
            &path,
            Config {
                snapshot_max: 1,
                snapshot_ttl_ms: ttl,
                ..Config::default()
            },
        )
        .unwrap()
    }
    #[test]
    fn release_keeps_in_flight_snapshot_charged() {
        let s = store("pinned", 30000);
        let (token, pinned) = s.snapshot_open("ledger").unwrap();
        s.snapshot_release("ledger", &token).unwrap();
        assert_eq!(s.scan_usage.count.load(Ordering::Acquire), 1);
        assert_eq!(s.snapshot_open("ledger").err(), Some(22));
        drop(pinned);
        assert_eq!(s.scan_usage.count.load(Ordering::Acquire), 0);
        assert_eq!(s.scan_usage.bytes.load(Ordering::Acquire), 0);
        assert!(s.snapshot_open("ledger").is_ok());
    }
    #[test]
    fn idle_expiry_reclaims_without_another_request() {
        let s = store("idle", 40);
        let (_, pinned) = s.snapshot_open("ledger").unwrap();
        drop(pinned);
        let until = Instant::now() + Duration::from_secs(2);
        while s.scan_usage.count.load(Ordering::Acquire) != 0 && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(s.scan_usage.count.load(Ordering::Acquire), 0);
        assert_eq!(s.scan_usage.bytes.load(Ordering::Acquire), 0);
        println!("idle expiry reclaimed snapshot without dispatch");
    }
    #[test]
    fn capture_shares_maximum_value_payload() {
        let s = store("arc", 30000);
        let mutation = Mutation {
            ns: "ledger".into(),
            key: "large".into(),
            op_id: "max".into(),
            deadline: now_ms() + 10000,
            ack: 1,
            expected: None,
            bytes: vec![7; MAX_VALUE],
            remove: false,
        };
        assert_eq!(s.mutate(mutation).code, 0);
        let live = s.get_handle("ledger", "large").unwrap();
        let (token, snapshot) = s.snapshot_open("ledger").unwrap();
        assert!(Arc::ptr_eq(&live, &snapshot.rows[0].1));
        assert_eq!(live.bytes.as_ptr(), snapshot.rows[0].1.bytes.as_ptr());
        let mut suffix = Vec::new();
        lp(&mut suffix, &token);
        lp(&mut suffix, &[]);
        suffix.extend(1u16.to_le_bytes());
        suffix.extend((MAX_FRAME as u32).to_le_bytes());
        lp(&mut suffix, b"");
        let page = s
            .scan_page("ledger", &mut Cursor::new(&suffix), SCAN_PREFIX, MAX_FRAME)
            .unwrap();
        assert_eq!(page.len(), 27 + 4 + 5 + 14 + MAX_VALUE);
        assert_eq!(
            s.scan_page("ledger", &mut Cursor::new(&suffix), SCAN_PREFIX, 1024)
                .err(),
            Some(4)
        );
    }
}
