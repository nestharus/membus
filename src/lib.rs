//! membus.kv/1 slice 1: bounded framing, independent committed reads, durable replay.
//! Chunk retirement (docs/CHUNK-RETIREMENT.md): chunk files by class, their catalogue, replay by version.
pub mod chunks;
mod signals;
mod timers;
pub use chunks::{Class, ManualClock, POLICY_NS};
#[cfg(windows)]
pub mod pipe_io;
use std::{
    collections::{hash_map::RandomState, HashMap, HashSet, VecDeque},
    fs::{self, File, OpenOptions},
    hash::BuildHasher,
    io::{self, Read, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
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
    (25, "timer_missing", false),
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
pub const TIMER_SET: u8 = 21;
pub const TIMER_CANCEL: u8 = 22;
pub const TIMER_STATUS: u8 = 23;
/// Chunk retirement, additive under kv/1 minor 0 (CHUNK-RETIREMENT.md section 6).
pub const POLICY_SET: u8 = 24;
pub const POLICY_GET: u8 = 25;
pub const GET_STATUS: u8 = 26;
pub const HEALTH2: u8 = 27;
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
    /// Tombstones whose index entry may drop once no other retained chunk holds their key (5.3).
    tomb_candidates: HashSet<(String, String)>,
}
#[derive(Clone)]
struct Meta {
    version: u64,
    tombstone: bool,
    bytes: usize,
    /// The chunk holding this key's latest persisted record (0: the legacy logs, or none).
    chunk: u64,
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
    /// Chunk rotation (CHUNK-RETIREMENT.md 5.2): a chunk closes past `chunk_bytes`, and a finite class's chunk
    /// also past `rotation_ms`; the idle tick runs every `tick_ms`.
    pub chunk_bytes: u64,
    pub rotation_ms: u64,
    pub tick_ms: u64,
    /// The retirement pass (5.4).
    pub retire_ms: u64,
    pub catalogue_rewrite_bytes: u64,
    /// Expiry summaries are kept this long (5.3; PLAN.md section 5: 30 days).
    pub summary_ms: u64,
    /// Ordinary admissions refuse with 7 at state_limit_bytes less this reserve; catalogue, retirement and
    /// import writes may use it (5.5). The library default is 0; the server sets 1 GiB.
    pub maintenance_reserve_bytes: u64,
    pub pinned_high_water_bytes: u64,
    /// A clock a test moves by hand; None is the system clock.
    pub clock: Option<Arc<chunks::ManualClock>>,
    /// Test seam: stop a retirement after step N (1: after `retiring`, 2: after the delete).
    pub retire_stop_after: u8,
    /// Test seam: the import leaves one key out, so its check must find the difference (T11).
    pub import_inject_difference: bool,
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
            chunk_bytes: 8 * 1024 * 1024,
            rotation_ms: 3_600_000,
            tick_ms: 1000,
            retire_ms: 10_000,
            catalogue_rewrite_bytes: 1024 * 1024,
            summary_ms: 30 * 86_400_000,
            maintenance_reserve_bytes: 0,
            pinned_high_water_bytes: 64 * 1024 * 1024,
            clock: None,
            retire_stop_after: 0,
            import_inject_difference: false,
            #[cfg(test)]
            sync_test_gate: None,
            #[cfg(test)]
            sync_test_notice: None,
        }
    }
}
pub struct Store {
    timers: timers::Shared,
    signals: signals::Hub,
    capture_metrics: [AtomicU64; 7],
    committed: Arc<RwLock<HashMap<(String, String), Arc<Value>>>>,
    scans: Arc<Mutex<ScanState>>,
    scan_usage: Arc<ScanUsage>,
    scan_reaper: Option<(mpsc::Sender<()>, std::thread::JoinHandle<()>)>,
    /// The idle tick (5.2): closes finite-class chunks past their age and rewrites a large catalogue.
    ticker: Option<(mpsc::Sender<()>, std::thread::JoinHandle<()>)>,
    state: Arc<Mutex<State>>,
    done: Condvar,
    maint: Arc<Maint>,
    router: Arc<ChunkRouter>,
    chunks: Arc<Mutex<chunks::ChunkStore>>,
    policies: Arc<RwLock<chunks::Policies>>,
    /// The version high-water the catalogue records at each open and close.
    next_issued: Arc<AtomicU64>,
    flags: Arc<Flags>,
    path: PathBuf,
    report: Mutex<chunks::ReplayReport>,
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
    /// Ok carries the sink (chunk) the record went to.
    done: mpsc::Sender<io::Result<u64>>,
}
struct AppendOwner {
    sender: Option<mpsc::Sender<Append>>,
    handle: Option<std::thread::JoinHandle<()>>,
}
struct FlushWait {
    /// The sink to sync: a stripe's log, or a chunk id under the chunk route.
    stripe: usize,
    end: u64,
    bytes: usize,
    done: mpsc::Sender<io::Result<u64>>,
    gathered: bool,
}

fn finish_flush(w: FlushWait, result: io::Result<()>, outstanding: &AtomicUsize) {
    outstanding.fetch_sub(1, Ordering::SeqCst);
    let sink = w.stripe as u64;
    let _ = w.done.send(result.map(|_| sink));
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
/// One stripe log's append with the test seams; a failed append fences the log.
fn append_to(log: &mut Log, record: &[u8], config: &Config) -> io::Result<()> {
    let result = if log.poisoned {
        Err(io::Error::other("stripe fenced after failed append"))
    } else if config.fail_partial_append || std::env::var_os("MEMBUS_TEST_PAUSE_PARTIAL").is_some() {
        let half = record.len() / 2;
        let r = write_record(log, &record[..half]);
        checkpoint("MEMBUS_TEST_PAUSE_PARTIAL");
        log.poisoned = true;
        r.and(Err(io::Error::other("test injected partial append failure")))
    } else {
        write_record(log, record)
    };
    if result.is_err() {
        log.poisoned = true;
    }
    result
}
/// Where a stripe's records go. The append owner writes through it; the flush owner syncs what it names.
trait Route: Send + Sync + 'static {
    /// Append one record for `stripe`: the result, the sink written (its flush identity) and the sink's end.
    fn write(&self, stripe: usize, record: &[u8], config: &Config) -> (io::Result<()>, usize, u64);
    /// A sink's stripe, its log while open, and the extent its close synced.
    fn sink(&self, id: usize) -> Option<(usize, Option<Arc<Mutex<Log>>>, u64)>;
    fn fence(&self, stripe: usize);
}
/// One fixed log per stripe (the pre-chunk shape, kept for the owner-level tests).
#[cfg(test)]
struct FixedRoute(Vec<Arc<Mutex<Log>>>);
#[cfg(test)]
impl Route for FixedRoute {
    fn write(&self, stripe: usize, record: &[u8], config: &Config) -> (io::Result<()>, usize, u64) {
        let mut log = self.0[stripe].lock().unwrap();
        let result = append_to(&mut log, record, config);
        (result, stripe, log.bytes)
    }
    fn sink(&self, id: usize) -> Option<(usize, Option<Arc<Mutex<Log>>>, u64)> {
        self.0.get(id).map(|log| (id, Some(log.clone()), 0))
    }
    fn fence(&self, stripe: usize) {
        self.0[stripe].lock().unwrap().poisoned = true;
    }
}
#[cfg(test)]
fn start_owners(
    logs: &[Arc<Mutex<Log>>],
    config: &Config,
) -> (Vec<AppendOwner>, std::thread::JoinHandle<()>) {
    start_routed_owners(Arc::new(FixedRoute(logs.to_vec())), logs.len(), config)
}
fn start_routed_owners(
    route: Arc<dyn Route>,
    stripes: usize,
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
    let flush_route = route.clone();
    let interval = config.flush_ms;
    let fail_sync = config.fail_sync;
    #[cfg(test)]
    let sync_test_gate = config.sync_test_gate.clone();
    let flush_owner = std::thread::spawn(move || {
        let mut pending = VecDeque::new();
        let mut last: Option<std::time::Instant> = None;
        // Keyed by sink: a sync covers the earlier complete appends of its own file.
        let mut covered: HashMap<usize, u64> = HashMap::new();
        let mut sync_failed = [false; 4];
        // A sink whose close already synced it, or a stripe fenced by a failed sync, needs no sync here.
        let settle = |w: &FlushWait, covered: &HashMap<usize, u64>, sync_failed: &[bool; 4]| -> Option<io::Result<()>> {
            let Some((stripe, log, synced)) = flush_route.sink(w.stripe) else {
                return Some(Err(io::Error::other("flush sink missing")));
            };
            if sync_failed[stripe] {
                return Some(Err(io::Error::other("stripe fenced after failed sync")));
            }
            if w.end <= covered.get(&w.stripe).copied().unwrap_or(0) {
                return Some(Ok(()));
            }
            if log.is_none() {
                return Some(if w.end <= synced {
                    Ok(())
                } else {
                    Err(io::Error::other("stripe fenced after failed sync"))
                });
            }
            None
        };
        loop {
            let mut gather = true;
            if pending.is_empty() {
                match flush_recv.recv() {
                    Ok(w) => match settle(&w, &covered, &sync_failed) {
                        Some(r) => {
                            finish_flush(w, r, &flush_outstanding);
                            continue;
                        }
                        None => {
                            gather = w.gathered;
                            pending.push_back(w);
                        }
                    },
                    Err(_) => break,
                }
                // The gather window closes on elapsed time; bounds are enforced
                // when selecting each handle's covering group below.
                let until = std::time::Instant::now()
                    + Duration::from_millis(if gather { interval } else { 0 });
                while let Some(left) = until.checked_duration_since(std::time::Instant::now()) {
                    match flush_recv.recv_timeout(left) {
                        Ok(w) => match settle(&w, &covered, &sync_failed) {
                            Some(r) => finish_flush(w, r, &flush_outstanding),
                            None => pending.push_back(w),
                        },
                        Err(_) => break,
                    }
                }
            }
            while let Ok(w) = flush_recv.try_recv() {
                match settle(&w, &covered, &sync_failed) {
                    Some(r) => finish_flush(w, r, &flush_outstanding),
                    None => pending.push_back(w),
                }
            }
            // Waiters settled while queued (a close's sync, a fence) need no sync.
            let mut waiting = VecDeque::new();
            while let Some(w) = pending.pop_front() {
                match settle(&w, &covered, &sync_failed) {
                    Some(r) => finish_flush(w, r, &flush_outstanding),
                    None => waiting.push_back(w),
                }
            }
            pending = waiting;
            let Some(front) = pending.front() else { continue };
            let sink = front.stripe;
            let mut group = Vec::new();
            let mut bytes = 0;
            let mut retained = VecDeque::new();
            while let Some(w) = pending.pop_front() {
                if w.stripe == sink
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
            let (stripe, log) = match flush_route.sink(sink) {
                Some((stripe, Some(log), _)) => (stripe, log),
                _ => {
                    // Closed (and synced) between selection and here: settle each by the close's extent.
                    for w in group {
                        let r = settle(&w, &covered, &sync_failed)
                            .unwrap_or_else(|| Err(io::Error::other("flush sink closed")));
                        finish_flush(w, r, &flush_outstanding);
                    }
                    continue;
                }
            };
            let (file, through) = {
                let log = log.lock().unwrap();
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
                let c = covered.entry(sink).or_insert(0);
                *c = (*c).max(through);
            } else {
                sync_failed[stripe] = true;
            }
            {
                let mut log = log.lock().unwrap();
                log.flushes += 1;
                if result.is_err() {
                    log.poisoned = true;
                }
            }
            if result.is_err() {
                flush_route.fence(stripe);
            }
            // A sync covers every earlier complete append on this handle,
            // including queued waiters beyond the nominal selected group.
            let mut extra = VecDeque::new();
            while let Some(w) = pending.pop_front() {
                if w.stripe == sink && w.end <= through {
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
    let owners = (0..stripes)
        .map(|stripe| {
            let route = route.clone();
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
                    let (result, sink, end) = route.write(stripe, &job.record, &config);
                    let result = if result.is_ok() && config.fail_after_append {
                        Err(io::Error::other("test injected post-admission I/O failure"))
                    } else {
                        result
                    };
                    if result.is_ok() && job.ack == 2 {
                        if config.flush_notice_delay_ms > 0 {
                            std::thread::sleep(Duration::from_millis(config.flush_notice_delay_ms));
                        }
                        let wait = FlushWait {
                            stripe: sink,
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
                        let _ = job.done.send(result.map(|_| sink as u64));
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
        if let Some((stop, worker)) = self.ticker.take() {
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
/// Flags health reads beside read_only.
#[derive(Default)]
struct Flags {
    storage_error: AtomicBool,
    expiry_degraded: AtomicBool,
}
struct OpenChunk {
    id: u64,
    log: Arc<Mutex<Log>>,
    opened_mono: u64,
}
struct SinkEntry {
    stripe: usize,
    log: Option<Arc<Mutex<Log>>>,
    synced: u64,
}
/// Chunk files by stripe and class (CHUNK-RETIREMENT.md 5.2). Each stripe's open chunks are held under that
/// stripe's lock while a record is written, so a close never meets a half-written record.
struct ChunkRouter {
    sinks: Vec<Mutex<HashMap<Class, OpenChunk>>>,
    table: RwLock<HashMap<usize, SinkEntry>>,
    fenced: [AtomicBool; 4],
    closed_io: Mutex<[u64; 3]>,
    policies: Arc<RwLock<chunks::Policies>>,
    chunks: Arc<Mutex<chunks::ChunkStore>>,
    clock: chunks::Clock,
    next: Arc<AtomicU64>,
    flags: Arc<Flags>,
    chunk_bytes: u64,
    rotation_ms: u64,
    fail_sync: bool,
}
impl ChunkRouter {
    fn open_chunk(&self, stripe: usize, class: Class, policy: String) -> io::Result<OpenChunk> {
        let mut cs = self.chunks.lock().unwrap();
        let id = cs.next_id;
        let path = chunks::chunk_path(&cs.dir, class, stripe, id);
        let now = self.clock.utc_ms();
        // The open record is flushed before the file exists, so a crash leaves at most a record with no file.
        cs.log(
            now,
            chunks::Rec::Open { seq: id, stripe: stripe as u8, class, policy: policy.clone(), next: self.next.load(Ordering::SeqCst) },
        )?;
        cs.next_id += 1;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        cs.table.insert(
            id,
            chunks::ChunkInfo {
                id,
                stripe,
                class,
                policy,
                open_ms: now,
                close_ms: None,
                close_mono: None,
                bytes: 0,
                path,
                state: chunks::ChunkState::Open,
                keys: HashMap::new(),
                blocked_since: None,
                horizons: Vec::new(),
                expired: Vec::new(),
            },
        );
        drop(cs);
        let log = Arc::new(Mutex::new(Log { file, bytes: 0, flushes: 0, write_calls: 0, poisoned: false }));
        self.table
            .write()
            .unwrap()
            .insert(id as usize, SinkEntry { stripe, log: Some(log.clone()), synced: 0 });
        Ok(OpenChunk { id, log, opened_mono: self.clock.mono_ms() })
    }
    /// Sync the chunk, then record its close. A close that cannot be recorded leaves the chunk open.
    fn close_chunk(&self, stripe: usize, c: &OpenChunk) -> io::Result<()> {
        let (file, bytes, writes, flushes) = {
            let l = c.log.lock().unwrap();
            if l.poisoned {
                return Err(io::Error::other("poisoned chunk stays open for replay"));
            }
            (l.file.try_clone()?, l.bytes, l.write_calls, l.flushes)
        };
        let synced = if self.fail_sync { Err(io::Error::other("test injected sync failure")) } else { sync_file(&file) };
        if let Err(e) = synced {
            c.log.lock().unwrap().poisoned = true;
            self.fence(stripe);
            return Err(e);
        }
        let now = self.clock.utc_ms();
        {
            let mut cs = self.chunks.lock().unwrap();
            if let Err(e) = cs.log(now, chunks::Rec::Close { seq: c.id, bytes, next: self.next.load(Ordering::SeqCst) }) {
                self.flags.storage_error.store(true, Ordering::SeqCst);
                return Err(e);
            }
            if let Some(info) = cs.table.get_mut(&c.id) {
                info.state = chunks::ChunkState::Closed;
                info.close_ms = Some(now);
                info.close_mono = Some(self.clock.mono_ms());
                info.bytes = bytes;
            }
        }
        if let Some(e) = self.table.write().unwrap().get_mut(&(c.id as usize)) {
            e.log = None;
            e.synced = bytes;
        }
        let mut io = self.closed_io.lock().unwrap();
        io[0] += bytes;
        io[1] += writes;
        io[2] += flushes + 1;
        Ok(())
    }
    /// The idle tick: close finite-class chunks past their rotation age.
    fn idle_close(&self) {
        for stripe in 0..self.sinks.len() {
            let mut open = self.sinks[stripe].lock().unwrap();
            let now = self.clock.mono_ms();
            let due: Vec<Class> = open
                .iter()
                .filter(|(class, c)| class.finite() && now.saturating_sub(c.opened_mono) >= self.rotation_ms)
                .map(|(class, _)| *class)
                .collect();
            for class in due {
                if self.close_chunk(stripe, &open[&class]).is_ok() {
                    open.remove(&class);
                }
            }
        }
    }
    fn io(&self) -> (u64, u64, u64) {
        let closed = *self.closed_io.lock().unwrap();
        let (mut bytes, mut writes, mut flushes) = (closed[0], closed[1], closed[2]);
        for stripe in &self.sinks {
            for c in stripe.lock().unwrap().values() {
                let l = c.log.lock().unwrap();
                bytes += l.bytes;
                writes += l.write_calls;
                flushes += l.flushes;
            }
        }
        (bytes, writes, flushes)
    }
    fn open_chunks(&self) -> usize {
        self.sinks.iter().map(|s| s.lock().unwrap().len()).sum()
    }
}
impl Route for ChunkRouter {
    fn write(&self, stripe: usize, record: &[u8], config: &Config) -> (io::Result<()>, usize, u64) {
        let Some((ns, key, version)) = record.get(16..).and_then(chunks::record_key) else {
            return (Err(io::Error::other("unroutable record")), 0, 0);
        };
        let (class, policy) = self.policies.read().unwrap().resolve(&ns, &key);
        let mut open = self.sinks[stripe].lock().unwrap();
        if self.fenced[stripe].load(Ordering::SeqCst) {
            return (Err(io::Error::other("stripe fenced after failed append")), 0, 0);
        }
        let now = self.clock.mono_ms();
        let rotate = open.get(&class).is_some_and(|c| {
            let b = c.log.lock().unwrap().bytes;
            (b > 0 && b + record.len() as u64 > self.chunk_bytes)
                || (class.finite() && now.saturating_sub(c.opened_mono) >= self.rotation_ms)
        });
        if rotate {
            if let Err(e) = self.close_chunk(stripe, &open[&class]) {
                self.fence(stripe);
                return (Err(e), 0, 0);
            }
            open.remove(&class);
        }
        if !open.contains_key(&class) {
            match self.open_chunk(stripe, class, policy) {
                Ok(c) => {
                    open.insert(class, c);
                }
                Err(e) => {
                    self.fence(stripe);
                    return (Err(e), 0, 0);
                }
            }
        }
        let c = &open[&class];
        let mut log = c.log.lock().unwrap();
        let result = append_to(&mut log, record, config);
        let end = log.bytes;
        drop(log);
        if result.is_err() {
            self.fence(stripe);
        } else {
            let mut cs = self.chunks.lock().unwrap();
            cs.register(c.id, &(ns, key), version);
            if let Some(info) = cs.table.get_mut(&c.id) {
                info.bytes = end;
            }
        }
        (result, c.id as usize, end)
    }
    fn sink(&self, id: usize) -> Option<(usize, Option<Arc<Mutex<Log>>>, u64)> {
        self.table.read().unwrap().get(&id).map(|e| (e.stripe, e.log.clone(), e.synced))
    }
    fn fence(&self, stripe: usize) {
        self.fenced[stripe].store(true, Ordering::SeqCst);
    }
}

/// One MBK1 payload as (mutation, version, record time).
fn parse_record(b: &[u8]) -> Result<(Mutation, u64, u64), u16> {
    let mut c = Cursor::new(b);
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
            expected: if expected == u64::MAX { None } else { Some(expected) },
            bytes,
            remove,
        },
        version,
        at,
    ))
}
fn corrupt(text: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, text)
}
/// What replay rebuilt, before any file is changed.
struct Replayed {
    committed: HashMap<(String, String), Arc<Value>>,
    chunk_of: HashMap<(String, String), u64>,
    dedup: HashMap<(String, String), Dedup>,
    next: u64,
    persisted: u64,
    chunks: chunks::ChunkStore,
    truncate: Vec<(PathBuf, u64)>,
    report: chunks::ReplayReport,
    storage_errors: Vec<String>,
    cat_last_ms: u64,
}
impl Replayed {
    fn apply(&mut self, m: Mutation, v: u64, at: u64, chunk: u64, now: u64, config: &Config) {
        self.next = self.next.max(v + 1);
        let k = (m.ns.clone(), m.key.clone());
        if self.committed.get(&k).map_or(true, |old| old.version < v) {
            self.committed.insert(
                k.clone(),
                Arc::new(Value { version: v, ack: m.ack, bytes: m.bytes.clone(), tombstone: m.remove }),
            );
            self.chunk_of.insert(k, chunk);
        }
        if now.saturating_sub(at) <= config.retry_ms && !timers::is_progress(&m.ns, &m.key) {
            self.dedup.insert(
                (m.ns.clone(), m.op_id.clone()),
                Dedup {
                    fingerprint: fingerprint(&m),
                    at,
                    outcome: Some(Outcome { code: 0, version: v, ack: m.ack }),
                },
            );
        }
    }
    /// Replay one log file. `open_at_crash` allows a torn tail to be truncated; otherwise it refuses start.
    fn replay_file(
        &mut self,
        p: &Path,
        si: usize,
        chunk: u64,
        open_at_crash: bool,
        horizons: &HashMap<(String, String), u64>,
        config: &Config,
        now: u64,
    ) -> io::Result<u64> {
        let mut f = File::open(p)?;
        let len = f.metadata()?.len();
        let name = p.display().to_string();
        let mut offset = 0u64;
        while offset < len {
            match chunks::read_frame_at(&mut f, b"MBK1", offset, len, &name)? {
                chunks::Frame::Torn(reason) => {
                    if !open_at_crash {
                        return Err(corrupt(format!("interior-corrupt {name} offset {offset}: torn tail in a closed chunk")));
                    }
                    self.truncate.push((p.to_owned(), offset));
                    self.report.stripes[si].truncations.push((name.clone(), offset, len - offset, reason.into()));
                    break;
                }
                chunks::Frame::Whole(b, n) => {
                    let (m, v, at) = parse_record(&b)
                        .map_err(|_| corrupt(format!("interior-corrupt {name} offset {offset}: payload")))?;
                    if stripe(&m.ns, &m.key) != si {
                        return Err(corrupt(format!("interior-corrupt {name} offset {offset}: wrong stripe")));
                    }
                    let k = (m.ns.clone(), m.key.clone());
                    self.chunks.register(chunk, &k, v);
                    self.report.stripes[si].records += 1;
                    self.report.stripes[si].bytes += n;
                    if horizons.get(&k).is_some_and(|h| v <= *h) {
                        // Anti-resurrection: a retired newer version's horizon hides this older one.
                        self.report.stripes[si].horizons_applied += 1;
                        self.next = self.next.max(v + 1);
                    } else {
                        self.apply(m, v, at, chunk, now, config);
                    }
                    offset += n;
                }
            }
        }
        Ok(offset)
    }
}
/// The legacy stripe logs a folder holds: the state root before the import, legacy-<epoch>/ after it.
fn legacy_files(dir: &Path) -> Vec<(usize, PathBuf)> {
    (0..4).map(|i| (i, dir.join(format!("stripe-{i}.log")))).filter(|(_, p)| p.exists()).collect()
}
/// What the legacy logs hold, read without changing them: each key's latest record whole, and the dedup window.
struct Legacy {
    committed: HashMap<Key, Arc<Value>>,
    dedup: HashMap<Key, Dedup>,
    next: u64,
    /// Each key's latest record: version, stripe, payload.
    latest: HashMap<Key, (u64, usize, Vec<u8>)>,
    /// Torn tails left in place: file, offset, bytes.
    torn: Vec<(String, u64, u64)>,
    records: u64,
}
fn read_legacy(files: &[(usize, PathBuf)], config: &Config, now: u64) -> io::Result<Legacy> {
    let mut l = Legacy {
        committed: HashMap::new(),
        dedup: HashMap::new(),
        next: 1,
        latest: HashMap::new(),
        torn: Vec::new(),
        records: 0,
    };
    for (si, p) in files {
        let mut f = File::open(p)?;
        let len = f.metadata()?.len();
        let name = p.display().to_string();
        let mut offset = 0u64;
        while offset < len {
            match chunks::read_frame_at(&mut f, b"MBK1", offset, len, &name)? {
                chunks::Frame::Torn(_) => {
                    l.torn.push((name.clone(), offset, len - offset));
                    break;
                }
                chunks::Frame::Whole(b, n) => {
                    let (m, v, at) = parse_record(&b)
                        .map_err(|_| corrupt(format!("interior-corrupt {name} offset {offset}: payload")))?;
                    if stripe(&m.ns, &m.key) != *si {
                        return Err(corrupt(format!("interior-corrupt {name} offset {offset}: wrong stripe")));
                    }
                    l.next = l.next.max(v + 1);
                    let k = (m.ns.clone(), m.key.clone());
                    if l.committed.get(&k).map_or(true, |old| old.version < v) {
                        l.committed.insert(
                            k.clone(),
                            Arc::new(Value { version: v, ack: m.ack, bytes: m.bytes.clone(), tombstone: m.remove }),
                        );
                        l.latest.insert(k, (v, *si, b));
                    }
                    if now.saturating_sub(at) <= config.retry_ms && !timers::is_progress(&m.ns, &m.key) {
                        l.dedup.insert(
                            (m.ns.clone(), m.op_id.clone()),
                            Dedup {
                                fingerprint: fingerprint(&m),
                                at,
                                outcome: Some(Outcome { code: 0, version: v, ack: m.ack }),
                            },
                        );
                    }
                    l.records += 1;
                    offset += n;
                }
            }
        }
    }
    Ok(l)
}
/// The policies an import applies before it writes: `state/import-policies.txt`, one `NS<TAB>PREFIX<TAB>CLASS` a
/// line (CLASS forever, latest or expire:MS; PREFIX may be empty); `#` starts a comment line.
fn import_policies(path: &Path, config: &Config) -> io::Result<Vec<(String, String, Class)>> {
    let p = path.join("import-policies.txt");
    if !p.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    let mut check = chunks::Policies::default();
    for (n, line) in fs::read_to_string(&p)?.lines().enumerate() {
        let line = line.trim_end_matches('\r');
        if line.trim().is_empty() || line.trim_start().starts_with('#') {
            continue;
        }
        let bad = || io::Error::new(io::ErrorKind::InvalidData, format!("import-policies.txt line {}: {line}", n + 1));
        let parts: Vec<&str> = line.split('\t').collect();
        if parts.len() != 3 {
            return Err(bad());
        }
        let class = match parts[2] {
            "forever" => Class::FOREVER,
            "latest" => Class::LATEST,
            c => Class::expire(
                c.strip_prefix("expire:")
                    .and_then(|ms| ms.parse().ok())
                    .filter(|ms| *ms >= config.retry_ms)
                    .ok_or_else(bad)?,
            ),
        };
        chunks::policy_key(parts[0], parts[1]).map_err(|_| bad())?;
        if check.would_exceed(parts[0], parts[1], class) {
            return Err(bad());
        }
        check.set(parts[0], parts[1], Some(class));
        out.push((parts[0].to_owned(), parts[1].to_owned(), class));
    }
    Ok(out)
}
/// Writes the import's chunk files and catalogue records into its own folder.
struct ImportWriter {
    cs: chunks::ChunkStore,
    open: HashMap<(usize, Class), (u64, File, u64)>,
    chunk_bytes: u64,
    utc: u64,
    next: u64,
    written: u64,
}
impl ImportWriter {
    fn write(&mut self, stripe: usize, class: Class, policy: &str, payload: &[u8]) -> io::Result<()> {
        let mut record = b"MBK1".to_vec();
        record.extend((payload.len() as u32).to_le_bytes());
        record.extend((!(payload.len() as u32)).to_le_bytes());
        record.extend(crc32(payload).to_le_bytes());
        record.extend(payload);
        if self.open.get(&(stripe, class)).is_some_and(|(_, _, b)| *b > 0 && b + record.len() as u64 > self.chunk_bytes) {
            self.close(stripe, class)?;
        }
        if !self.open.contains_key(&(stripe, class)) {
            let id = self.cs.next_id;
            self.cs.next_id += 1;
            let path = chunks::chunk_path(&self.cs.dir, class, stripe, id);
            self.cs.log(
                self.utc,
                chunks::Rec::Open { seq: id, stripe: stripe as u8, class, policy: policy.to_owned(), next: self.next },
            )?;
            fs::create_dir_all(path.parent().unwrap())?;
            let file = OpenOptions::new().create_new(true).append(true).open(&path)?;
            self.open.insert((stripe, class), (id, file, 0));
        }
        let (_, file, bytes) = self.open.get_mut(&(stripe, class)).unwrap();
        file.write_all(&record)?;
        *bytes += record.len() as u64;
        self.written += record.len() as u64;
        Ok(())
    }
    fn close(&mut self, stripe: usize, class: Class) -> io::Result<()> {
        if let Some((id, file, bytes)) = self.open.remove(&(stripe, class)) {
            sync_file(&file)?;
            self.cs.log(self.utc, chunks::Rec::Close { seq: id, bytes, next: self.next })?;
        }
        Ok(())
    }
    fn close_all(&mut self) -> io::Result<()> {
        let open: Vec<(usize, Class)> = self.open.keys().copied().collect();
        for (stripe, class) in open {
            self.close(stripe, class)?;
        }
        Ok(())
    }
}
/// Move the legacy logs into legacy-<epoch>/, unmodified, and set them read-only (5.6, step 1). Run again after a
/// stop between the switch and the move.
fn move_legacy(path: &Path, import: &chunks::Import) -> io::Result<()> {
    let to = path.join(&import.legacy_dir);
    fs::create_dir_all(&to)?;
    for (name, _, _) in &import.files {
        let from = path.join(name);
        if from.exists() {
            fs::rename(&from, to.join(name))?;
        }
        let dest = to.join(name);
        if dest.exists() {
            let mut permissions = fs::metadata(&dest)?.permissions();
            permissions.set_readonly(true);
            fs::set_permissions(&dest, permissions)?;
        }
    }
    Ok(())
}
/// The one-time import (CHUNK-RETIREMENT.md 5.6): each key's latest legacy record, at its version and time, into
/// chunks by class under chunks.import/, checked against the legacy replay before the switch. None when there is
/// nothing to import; Err (legacy logs untouched, chunks.import/ removed) on any difference.
fn import_legacy(path: &Path, config: &Config, clock: &chunks::Clock, epoch: u64) -> io::Result<Option<(chunks::Import, String)>> {
    use chunks::Rec;
    let files = legacy_files(path);
    if files.is_empty() {
        return Ok(None);
    }
    let chunks_dir = path.join("chunks");
    let catalogue = chunks_dir.join("catalogue.log");
    let records = if catalogue.exists() { chunks::read_catalogue(&catalogue)?.0 } else { Vec::new() };
    // Imported already (a stop before the move is finished at replay), or chunks written beside the legacy logs
    // (they stay one forever chunk, as CR1 reads them).
    if records.iter().any(|r| matches!(r.rec, Rec::Import(_) | Rec::Open { .. })) {
        return Ok(None);
    }
    let utc = clock.utc_ms();
    let mut listed = Vec::new();
    for (_, p) in &files {
        let name = p.file_name().unwrap().to_string_lossy().into_owned();
        listed.push((name, fs::metadata(p)?.len(), chunks::sha256_file(p)?));
    }
    let legacy = read_legacy(&files, config, now_ms())?;
    let dir = path.join("chunks.import");
    if dir.exists() {
        // An import cut short starts again from the untouched legacy logs.
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    let built = (|| -> io::Result<(chunks::Import, u64)> {
        let mut cs = chunks::ChunkStore::new(dir.clone());
        cs.cat = Some(chunks::Catalogue::open(&dir.join("catalogue.log"))?);
        let mut w = ImportWriter { cs, open: HashMap::new(), chunk_bytes: config.chunk_bytes, utc, next: legacy.next, written: 0 };
        let mut policies = chunks::Policies::default();
        // The policies first, as policy records at new versions, so each imported key takes its class.
        for (n, (ns, prefix, class)) in import_policies(path, config)?.into_iter().enumerate() {
            policies.set(&ns, &prefix, Some(class));
            let key = chunks::policy_key(&ns, &prefix).map_err(|_| io::Error::other("policy key"))?;
            let m = Mutation {
                ns: POLICY_NS.into(),
                key: key.clone(),
                op_id: format!("import-policy/{epoch}/{n}"),
                deadline: 0,
                ack: 2,
                expected: None,
                bytes: chunks::policy_value(class),
                remove: false,
            };
            let version = w.next;
            w.next += 1;
            let (c, label) = policies.resolve(POLICY_NS, &key);
            w.write(stripe(POLICY_NS, &key), c, &label, &mutation_bytes(&m, version, utc))?;
        }
        let mut latest: Vec<_> = legacy.latest.iter().collect();
        latest.sort_by_key(|(_, (v, _, _))| *v);
        let mut leave_out = config.import_inject_difference;
        for ((ns, key), (_, si, payload)) in latest {
            if std::mem::take(&mut leave_out) {
                continue;
            }
            let (class, label) = policies.resolve(ns, key);
            w.write(*si, class, &label, payload)?;
        }
        w.close_all()?;
        // The check: the import replayed alone against the legacy replay held in memory.
        let mut rp = Replayed::empty(dir.clone());
        replay_into(&mut rp, path, &dir, false, config, clock)?;
        let mut first: Option<String> = None;
        for (k, v) in &legacy.committed {
            match rp.committed.get(k) {
                Some(x) if x.version == v.version && x.bytes == v.bytes && x.tombstone == v.tombstone && x.ack == v.ack => {}
                Some(x) => {
                    first.get_or_insert(format!(
                        "{}/{}: imported version {} tombstone {} {} B, legacy version {} tombstone {} {} B",
                        k.0, k.1, x.version, x.tombstone, x.bytes.len(), v.version, v.tombstone, v.bytes.len()
                    ));
                }
                None => {
                    first.get_or_insert(format!("{}/{}: missing from the import (legacy version {})", k.0, k.1, v.version));
                }
            }
        }
        for k in rp.committed.keys() {
            if k.0 != POLICY_NS && !legacy.committed.contains_key(k) {
                first.get_or_insert(format!("{}/{}: in the import only", k.0, k.1));
            }
        }
        let live = |c: &HashMap<Key, Arc<Value>>| {
            let mut counts = std::collections::BTreeMap::new();
            for (k, v) in c {
                if !v.tombstone && k.0 != POLICY_NS {
                    *counts.entry(k.0.clone()).or_insert(0usize) += 1;
                }
            }
            counts
        };
        let (a, b) = (live(&legacy.committed), live(&rp.committed));
        if a != b {
            first.get_or_insert(format!("namespace live counts: legacy {a:?}, imported {b:?}"));
        }
        if rp.next != w.next || w.next < legacy.next {
            first.get_or_insert(format!("next: imported {}, legacy {}, written {}", rp.next, legacy.next, w.next));
        }
        for (k, d) in &rp.dedup {
            if k.0 == POLICY_NS {
                continue;
            }
            match legacy.dedup.get(k) {
                Some(l) if l.fingerprint == d.fingerprint && l.outcome == d.outcome => {}
                _ => {
                    first.get_or_insert(format!("dedup {}/{}: differs from the legacy replay", k.0, k.1));
                }
            }
        }
        if let Some(e) = rp.storage_errors.first() {
            first.get_or_insert(format!("storage error in the import: {e}"));
        }
        if let Some(f) = first {
            return Err(io::Error::new(io::ErrorKind::InvalidData, format!("import refused, legacy logs left in place: {f}")));
        }
        let import = chunks::Import {
            files: listed,
            keys: legacy.latest.len() as u64,
            bytes: w.written,
            epoch,
            legacy_dir: format!("legacy-{epoch}"),
        };
        w.cs.log(utc, Rec::Import(import.clone()))?;
        Ok((import, w.written))
    })();
    let (import, written) = match built {
        Ok(x) => x,
        Err(e) => {
            let _ = fs::remove_dir_all(&dir);
            return Err(e);
        }
    };
    // The switch: chunks.import/ becomes chunks/; then the legacy logs move, unmodified and read-only.
    if chunks_dir.exists() {
        if records.is_empty() {
            fs::remove_dir_all(&chunks_dir)?;
        } else {
            fs::rename(&chunks_dir, path.join(format!("chunks.pre-import-{epoch}")))?;
        }
    }
    fs::rename(&dir, &chunks_dir)?;
    move_legacy(path, &import)?;
    let torn: Vec<String> = legacy.torn.iter().map(|(f, o, b)| format!("{f} offset {o} ({b} B torn tail, left in place)")).collect();
    let line = format!(
        "imported {} keys ({written} B) of {} legacy records from {} legacy logs into chunks/; legacy logs in {}{}",
        import.keys,
        legacy.records,
        import.files.len(),
        import.legacy_dir,
        if torn.is_empty() { String::new() } else { format!("; {}", torn.join("; ")) }
    );
    Ok(Some((import, line)))
}
impl Replayed {
    fn empty(dir: PathBuf) -> Self {
        Replayed {
            committed: HashMap::new(),
            chunk_of: HashMap::new(),
            dedup: HashMap::new(),
            next: 1,
            persisted: 0,
            chunks: chunks::ChunkStore::new(dir),
            truncate: Vec::new(),
            report: chunks::ReplayReport::default(),
            storage_errors: Vec::new(),
            cat_last_ms: 0,
        }
    }
}
fn replay(path: &Path, config: &Config, clock: &chunks::Clock) -> Result<Replayed, (io::Error, chunks::ReplayReport)> {
    let dir = path.join("chunks");
    let mut rp = Replayed::empty(dir.clone());
    match replay_into(&mut rp, path, &dir, true, config, clock) {
        Ok(()) => Ok(rp),
        Err(e) => Err((e, rp.report)),
    }
}
/// Replay the chunks in `dir`; with `legacy`, also the state's legacy logs (before an import, as one forever chunk;
/// after it, for dedup alone within 24 h).
fn replay_into(rp: &mut Replayed, path: &Path, dir: &Path, legacy: bool, config: &Config, clock: &chunks::Clock) -> io::Result<()> {
    use chunks::{ChunkState, Rec};
    let now = now_ms();
    let dir = dir.to_owned();
    fs::create_dir_all(&dir)?;
    let (records, torn) = chunks::read_catalogue(&dir.join("catalogue.log"))?;
    rp.report.catalogue_truncation = torn.map(|(o, b, r)| (o, b, r.to_string()));
    let mut next_hw = 0u64;
    let mut horizons: Vec<(String, String, u64)> = Vec::new();
    for r in &records {
        rp.cat_last_ms = rp.cat_last_ms.max(r.at_ms);
        match &r.rec {
            Rec::Open { seq, stripe, class, policy, next } => {
                next_hw = next_hw.max(*next);
                rp.chunks.next_id = rp.chunks.next_id.max(seq + 1);
                rp.chunks.table.insert(
                    *seq,
                    chunks::ChunkInfo {
                        id: *seq,
                        stripe: *stripe as usize,
                        class: *class,
                        policy: policy.clone(),
                        open_ms: r.at_ms,
                        close_ms: None,
                        close_mono: None,
                        bytes: 0,
                        path: chunks::chunk_path(&dir, *class, *stripe as usize, *seq),
                        state: ChunkState::Open,
                        keys: HashMap::new(),
                        blocked_since: None,
                        horizons: Vec::new(),
                        expired: Vec::new(),
                    },
                );
            }
            Rec::Close { seq, bytes, next } => {
                next_hw = next_hw.max(*next);
                if let Some(c) = rp.chunks.table.get_mut(seq) {
                    c.state = ChunkState::Closed;
                    c.close_ms = Some(r.at_ms);
                    c.bytes = *bytes;
                }
            }
            Rec::Retiring { seq, next, horizons: h, expired } => {
                next_hw = next_hw.max(*next);
                horizons.extend(h.iter().cloned());
                if let Some(c) = rp.chunks.table.get_mut(seq) {
                    c.state = ChunkState::Retiring;
                    c.horizons = h.clone();
                    c.expired = expired.clone();
                }
                if !expired.is_empty() {
                    rp.chunks.summaries.add(r.at_ms, expired.clone());
                }
            }
            Rec::Retired { seq } => {
                if let Some(c) = rp.chunks.table.get_mut(seq) {
                    c.state = ChunkState::Retired;
                }
            }
            Rec::Horizons(h) => horizons.extend(h.iter().cloned()),
            Rec::Summary { retired_ms, keys } => {
                rp.chunks.summaries.add(*retired_ms, keys.clone());
            }
            Rec::Next(n) => next_hw = next_hw.max(*n),
            Rec::Resume => {}
            Rec::Import(i) => {
                rp.chunks.import = Some(i.clone());
                rp.chunks.import_at = r.at_ms;
            }
        }
    }
    let horizon_map = chunks::horizon_set(&horizons);
    rp.chunks.horizons = horizon_map.clone();
    rp.report.horizons = horizon_map.len();
    // Chunk files on disk must each have an open record that is not retired.
    for (_, _, seq, p) in chunks::list_chunk_files(&dir)? {
        let known = rp.chunks.table.get(&seq).is_some_and(|c| c.state != ChunkState::Retired && c.path == p);
        if !known {
            rp.storage_errors.push(format!("chunk file with no open record: {}", p.display()));
        }
    }
    // Legacy stripe logs (before the import): read as one forever chunk that never retires.
    let legacy_logs: Vec<PathBuf> = (0..4).map(|i| path.join(format!("stripe-{i}.log"))).collect();
    if legacy && legacy_logs.iter().any(|p| p.exists()) && rp.chunks.import.is_none() {
        rp.chunks.table.insert(
            chunks::LEGACY_CHUNK,
            chunks::ChunkInfo {
                id: chunks::LEGACY_CHUNK,
                stripe: 0,
                class: Class::FOREVER,
                policy: "(legacy stripe logs)".into(),
                open_ms: 0,
                close_ms: Some(0),
                close_mono: None,
                bytes: 0,
                path: path.to_owned(),
                state: ChunkState::Closed,
                keys: HashMap::new(),
                blocked_since: None,
                horizons: Vec::new(),
                expired: Vec::new(),
            },
        );
        let mut total = 0;
        for (i, p) in legacy_logs.iter().enumerate() {
            if !p.exists() {
                continue;
            }
            // The legacy logs were open at any crash, so a torn tail is truncated as before.
            let n = rp.replay_file(p, i, chunks::LEGACY_CHUNK, true, &horizon_map, config, now)?;
            rp.report.stripes[i].legacy_logs += 1;
            rp.report.legacy.push(p.display().to_string());
            total += n;
        }
        rp.persisted += total;
        if let Some(c) = rp.chunks.table.get_mut(&chunks::LEGACY_CHUNK) {
            c.bytes = total;
        }
    }
    let ids: Vec<u64> = rp.chunks.table.keys().copied().filter(|id| *id != chunks::LEGACY_CHUNK).collect();
    for id in ids {
        let c = rp.chunks.table[&id].clone();
        match c.state {
            ChunkState::Retired => rp.report.stripes[c.stripe].retired_skipped += 1,
            ChunkState::Retiring => {
                // Finished at start; its records were already out of the index when it began retiring.
                let len = fs::metadata(&c.path).map(|m| m.len()).unwrap_or(0);
                rp.persisted += len;
                if let Some(info) = rp.chunks.table.get_mut(&id) {
                    info.bytes = len;
                }
            }
            ChunkState::Open | ChunkState::Closed => {
                if !c.path.exists() {
                    if c.state == ChunkState::Open {
                        // The open record was flushed and the crash came before the file: an empty chunk.
                        rp.report.empty_closed.push(id);
                        continue;
                    }
                    rp.storage_errors.push(format!("chunk file missing with no retiring record: {}", c.path.display()));
                    continue;
                }
                let open_at_crash = c.state == ChunkState::Open;
                let n = rp.replay_file(&c.path, c.stripe, id, open_at_crash, &horizon_map, config, now)?;
                rp.report.stripes[c.stripe].chunks += 1;
                if !open_at_crash && n != c.bytes {
                    return Err(corrupt(format!(
                        "interior-corrupt {}: closed chunk holds {n} bytes, its close recorded {}",
                        c.path.display(),
                        c.bytes
                    )));
                }
                rp.persisted += n;
                if let Some(info) = rp.chunks.table.get_mut(&id) {
                    info.bytes = n;
                }
            }
        }
    }
    if let (true, Some(import)) = (legacy, rp.chunks.import.clone()) {
        // A stop between the switch and the move: finish the move.
        if !legacy_files(path).is_empty() {
            move_legacy(path, &import)?;
        }
        let ldir = path.join(&import.legacy_dir);
        let mut changed = Vec::new();
        for (name, size, sha) in &import.files {
            let p = ldir.join(name);
            let same = fs::metadata(&p).is_ok_and(|m| m.len() == *size) && chunks::sha256_file(&p).is_ok_and(|s| s == *sha);
            if !same {
                changed.push(format!("legacy file changed or missing since the import: {}", p.display()));
            }
        }
        let window = clock.utc_ms().saturating_sub(rp.chunks.import_at) < config.retry_ms;
        if !changed.is_empty() {
            rp.storage_errors.extend(changed);
        } else if window {
            // For 24 h after the import the legacy logs give their dedup entries alone, never values (5.6).
            let files = legacy_files(&ldir);
            let l = read_legacy(&files, config, now)?;
            for (k, d) in l.dedup {
                rp.dedup.entry(k).or_insert(d);
            }
            rp.report.legacy = files.iter().map(|(_, p)| format!("{} (dedup only)", p.display())).collect();
        }
        rp.report.import = Some(format!(
            "import of epoch {} ({} keys, {} B); legacy dedup replay {}",
            import.epoch,
            import.keys,
            import.bytes,
            if window { "on (within 24 h of the import)" } else { "off (24 h passed)" }
        ));
    }
    rp.next = rp.next.max(next_hw);
    Ok(())
}
/// HEALTH2 (opcode 27; CHUNK-RETIREMENT.md section 6): the store's state, limits and chunk retirement in one read.
#[derive(Clone, Debug, Default)]
pub struct Health2 {
    pub state_bytes: u64,
    pub state_limit_bytes: u64,
    pub state_high_water: bool,
    pub read_only: bool,
    pub storage_error: bool,
    pub expiry_degraded: bool,
    pub dedup_entries: usize,
    pub dedup_cap: usize,
    pub dedup_high_water: bool,
    pub keys: usize,
    pub index_cap: usize,
    /// Live keys by namespace, against `quota`.
    pub namespaces: Vec<(String, usize)>,
    pub quota: usize,
    pub namespace_high_water: bool,
    pub chunks_open: usize,
    pub chunks_closed: usize,
    /// Retained bytes by class label (the legacy logs as `legacy`).
    pub retained_bytes: Vec<(String, u64)>,
    pub retained_total: u64,
    /// Latest-class bytes past their window, held because they still carry a key's latest record.
    pub pinned_bytes: u64,
    pub pinned_high_water: bool,
    pub retired_chunks: u64,
    pub retired_bytes: u64,
    pub retire_blocked: usize,
    pub retire_blocked_oldest_ms: u64,
    pub retire_blocked_high_water: bool,
}
impl Health2 {
    /// The key=value list HEALTH2 returns.
    pub fn fields(&self) -> Vec<(String, String)> {
        let mut f: Vec<(String, String)> = vec![
            ("state_bytes".into(), self.state_bytes.to_string()),
            ("state_limit_bytes".into(), self.state_limit_bytes.to_string()),
            ("state_high_water".into(), self.state_high_water.to_string()),
            ("read_only".into(), self.read_only.to_string()),
            ("storage_error".into(), self.storage_error.to_string()),
            ("expiry_degraded".into(), self.expiry_degraded.to_string()),
            ("dedup_entries".into(), self.dedup_entries.to_string()),
            ("dedup_cap".into(), self.dedup_cap.to_string()),
            ("dedup_high_water".into(), self.dedup_high_water.to_string()),
            ("keys".into(), self.keys.to_string()),
            ("index_cap".into(), self.index_cap.to_string()),
            ("quota".into(), self.quota.to_string()),
            ("namespace_high_water".into(), self.namespace_high_water.to_string()),
            ("chunks_open".into(), self.chunks_open.to_string()),
            ("chunks_closed".into(), self.chunks_closed.to_string()),
            ("retained_bytes".into(), self.retained_total.to_string()),
            ("pinned_bytes".into(), self.pinned_bytes.to_string()),
            ("pinned_high_water".into(), self.pinned_high_water.to_string()),
            ("retired_chunks".into(), self.retired_chunks.to_string()),
            ("retired_bytes".into(), self.retired_bytes.to_string()),
            ("retire_blocked".into(), self.retire_blocked.to_string()),
            ("retire_blocked_oldest_ms".into(), self.retire_blocked_oldest_ms.to_string()),
            ("retire_blocked_high_water".into(), self.retire_blocked_high_water.to_string()),
        ];
        for (ns, n) in &self.namespaces {
            f.push((format!("namespace.{ns}"), n.to_string()));
        }
        for (class, b) in &self.retained_bytes {
            f.push((format!("retained_bytes.{class}"), b.to_string()));
        }
        f
    }
}
/// The resident heartbeat line (src/main.rs writes it every beat): the fields fleet's check reads, then the chunk
/// retirement flags `heartbeat_warn` may name and three counts (CHUNK-RETIREMENT.md section 6). The new flags are
/// written 0 or 1, which fleet reads as a count (above 0 flags), so a beat stays under 512 B.
#[allow(clippy::too_many_arguments)]
pub fn heartbeat_json(
    pid: u32,
    epoch: u64,
    unix_ms: u128,
    writes: u64,
    bytes: u64,
    attempts: u64,
    attempted: u64,
    failures: u64,
    h: &Health2,
) -> String {
    format!(
        "{{\"pid\":{pid},\"epoch\":{epoch},\"unix_ms\":{unix_ms},\"heartbeat_writes\":{writes},\"heartbeat_bytes\":{bytes},\"heartbeat_attempts\":{attempts},\"heartbeat_bytes_attempted\":{attempted},\"heartbeat_failures\":{failures},\"state_bytes\":{},\"state_limit_bytes\":{},\"state_high_water\":{},\"read_only\":{},\"storage_error\":{},\"expiry_degraded\":{},\"dedup_high_water\":{},\"namespace_high_water\":{},\"pinned_high_water\":{},\"retire_blocked_high_water\":{},\"dedup_entries\":{},\"retained_bytes\":{},\"retired_bytes\":{}}}\n",
        h.state_bytes,
        h.state_limit_bytes,
        h.state_high_water,
        h.read_only as u8,
        h.storage_error as u8,
        h.expiry_degraded as u8,
        h.dedup_high_water as u8,
        h.namespace_high_water as u8,
        h.pinned_high_water as u8,
        h.retire_blocked_high_water as u8,
        h.dedup_entries,
        h.retained_total,
        h.retired_bytes
    )
}
/// Retirement, expiry summaries, tombstone drops and the clock check (5.3, 5.4), shared with the tick thread.
struct Maint {
    state: Arc<Mutex<State>>,
    committed: Arc<RwLock<HashMap<(String, String), Arc<Value>>>>,
    chunks: Arc<Mutex<chunks::ChunkStore>>,
    router: Arc<ChunkRouter>,
    flags: Arc<Flags>,
    clock: chunks::Clock,
    next_issued: Arc<AtomicU64>,
    config: Config,
    path: PathBuf,
}
type Key = (String, String);
impl Maint {
    /// Retire closed chunks due by their class, or (forced) one chunk that holds no key's latest record.
    /// Returns the chunks retired whole; a stopped, kept or held chunk is left out.
    fn retire(&self, forced: Option<u64>) -> io::Result<Vec<u64>> {
        use chunks::ChunkState;
        let utc = self.clock.utc_ms();
        let mono = self.clock.mono_ms();
        let mut gone: Vec<(Key, u64)> = Vec::new();
        let mut started: Vec<(u64, PathBuf, u64, Vec<Key>)> = Vec::new();
        {
            let mut state = self.state.lock().unwrap();
            let mut cs = self.chunks.lock().unwrap();
            if forced.is_none() && cs.degraded.is_some() {
                return Ok(Vec::new());
            }
            let ids: Vec<u64> = match forced {
                Some(id) => vec![id],
                None => cs.table.keys().copied().collect(),
            };
            for id in ids {
                let Some(c) = cs.table.get(&id) else { continue };
                if id == chunks::LEGACY_CHUNK || c.state != ChunkState::Closed {
                    continue;
                }
                let expire = forced.is_none() && c.class.kind == 2;
                if forced.is_none() {
                    let window = match c.class.kind {
                        1 => self.config.retry_ms,
                        2 => c.class.retention_ms,
                        _ => continue,
                    };
                    let Some(close) = c.close_ms else { continue };
                    // Never earlier than both clocks allow: UTC close time, and monotonic time within this run.
                    if utc < close.saturating_add(window) || c.close_mono.is_some_and(|m| mono.saturating_sub(m) < window) {
                        continue;
                    }
                }
                if c.keys.keys().any(|k| state.pending.contains_key(k)) {
                    continue;
                }
                let latest: Vec<Key> =
                    c.keys.keys().filter(|k| state.metadata.get(*k).is_some_and(|m| m.chunk == id)).cloned().collect();
                if !latest.is_empty() && !expire {
                    continue;
                }
                let c = c.clone();
                let horizons: Vec<(String, String, u64)> = c
                    .keys
                    .iter()
                    .filter(|(k, _)| cs.holders.others(k, id).next().is_some())
                    .map(|((n, k), (_, max))| (n.clone(), k.clone(), *max))
                    .collect();
                let next = self.next_issued.load(Ordering::SeqCst);
                // Step 1: `retiring` with its horizons and the version high-water, flushed before anything changes.
                if let Err(e) = cs.log(
                    utc,
                    chunks::Rec::Retiring { seq: id, next, horizons: horizons.clone(), expired: latest.clone() },
                ) {
                    self.flags.storage_error.store(true, Ordering::SeqCst);
                    return Err(e);
                }
                for (n, k, v) in &horizons {
                    let h = cs.horizons.entry((n.clone(), k.clone())).or_insert(0);
                    *h = (*h).max(*v);
                }
                // Step 2: an expired key leaves the index and frees its slots; a summary keeps it for 30 days.
                for k in &latest {
                    if let Some(m) = state.metadata.remove(k) {
                        state.keys -= 1;
                        state.committed_bytes -= m.bytes;
                        if !m.tombstone {
                            if let Some(n) = state.namespace_counts.get_mut(&k.0) {
                                *n -= 1;
                            }
                        }
                        gone.push((k.clone(), m.version));
                    }
                }
                if !latest.is_empty() {
                    state.keys += cs.summaries.add(utc, latest.clone());
                }
                let info = cs.table.get_mut(&id).unwrap();
                info.state = ChunkState::Retiring;
                info.horizons = horizons;
                info.expired = latest;
                if self.config.retire_stop_after == 1 {
                    continue;
                }
                for k in c.keys.keys() {
                    cs.holders.remove(k, id);
                    state.tomb_candidates.insert(k.clone());
                }
                started.push((id, c.path.clone(), c.bytes, c.keys.keys().cloned().collect()));
            }
        }
        if !gone.is_empty() {
            let mut committed = self.committed.write().unwrap();
            for (k, v) in gone {
                if committed.get(&k).is_some_and(|x| x.version == v) {
                    committed.remove(&k);
                }
            }
        }
        // Steps 3 and 4: delete the file, then `retired`. A held delete stays retiring and is retried.
        let mut done = Vec::new();
        let mut freed = 0;
        {
            let mut cs = self.chunks.lock().unwrap();
            for (id, p, bytes, keys) in started {
                match fs::remove_file(&p) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(_) => {
                        cs.table.get_mut(&id).unwrap().blocked_since.get_or_insert(utc);
                        continue;
                    }
                }
                if self.config.retire_stop_after == 2 {
                    continue;
                }
                if let Err(e) = cs.log(utc, chunks::Rec::Retired { seq: id }) {
                    self.flags.storage_error.store(true, Ordering::SeqCst);
                    return Err(e);
                }
                let info = cs.table.get_mut(&id).unwrap();
                info.state = ChunkState::Retired;
                info.keys.clear();
                info.blocked_since = None;
                cs.retired_chunks += 1;
                cs.retired_bytes += bytes;
                freed += bytes;
                done.push(id);
                // A horizon whose key no retained chunk holds has nothing left to hide.
                for k in keys {
                    if !cs.holders.map.contains_key(&k) {
                        cs.horizons.remove(&k);
                    }
                }
            }
        }
        if freed > 0 {
            let mut state = self.state.lock().unwrap();
            state.persisted_bytes = state.persisted_bytes.saturating_sub(freed);
        }
        Ok(done)
    }
    /// Retry the held deletes; a retiring chunk's index work and horizons are already done.
    fn retry_retiring(&self) -> io::Result<usize> {
        let mut cs = self.chunks.lock().unwrap();
        let ids: Vec<u64> =
            cs.table.values().filter(|c| c.state == chunks::ChunkState::Retiring).map(|c| c.id).collect();
        let mut done = 0;
        let mut freed = 0;
        for id in ids {
            let (p, bytes) = (cs.table[&id].path.clone(), cs.table[&id].bytes);
            match fs::remove_file(&p) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(_) => continue,
            }
            let now = self.clock.utc_ms();
            if let Err(e) = cs.log(now, chunks::Rec::Retired { seq: id }) {
                self.flags.storage_error.store(true, Ordering::SeqCst);
                return Err(e);
            }
            let info = cs.table.get_mut(&id).unwrap();
            info.state = chunks::ChunkState::Retired;
            info.keys.clear();
            info.blocked_since = None;
            cs.retired_chunks += 1;
            cs.retired_bytes += bytes;
            freed += bytes;
            done += 1;
        }
        drop(cs);
        let mut state = self.state.lock().unwrap();
        state.persisted_bytes = state.persisted_bytes.saturating_sub(freed);
        Ok(done)
    }
    /// Drop tombstone index entries no other retained chunk can contradict (5.3), and expired summaries.
    fn drop_tombstones(&self) {
        let utc = self.clock.utc_ms();
        let mut gone = Vec::new();
        {
            let mut state = self.state.lock().unwrap();
            let mut cs = self.chunks.lock().unwrap();
            let freed = cs.summaries.expire(utc, self.config.summary_ms);
            state.keys -= freed;
            let candidates: Vec<Key> = state.tomb_candidates.drain().collect();
            for k in candidates {
                if state.pending.contains_key(&k) {
                    state.tomb_candidates.insert(k);
                    continue;
                }
                let Some(m) = state.metadata.get(&k) else { continue };
                if !m.tombstone {
                    continue;
                }
                let others: Vec<u64> = cs.holders.others(&k, m.chunk).collect();
                if others.is_empty() {
                    let m = state.metadata.remove(&k).unwrap();
                    state.keys -= 1;
                    state.committed_bytes -= m.bytes;
                    gone.push((k, m.version));
                } else if others
                    .iter()
                    .all(|c| *c != chunks::LEGACY_CHUNK && cs.table.get(c).is_some_and(|i| i.class.finite()))
                {
                    // Older records in finite chunks retire in time; one in a forever chunk never does.
                    state.tomb_candidates.insert(k);
                }
            }
        }
        if !gone.is_empty() {
            let mut committed = self.committed.write().unwrap();
            for (k, v) in gone {
                if committed.get(&k).is_some_and(|x| x.version == v) {
                    committed.remove(&k);
                }
            }
        }
    }
    /// One tick's clock check (5.4): a stable run of a rotation age, or `expiry.resume`, ends a pause.
    fn clock_tick(&self) {
        let utc = self.clock.utc_ms();
        let mono = self.clock.mono_ms();
        let mut cs = self.chunks.lock().unwrap();
        match cs.last_tick {
            Some((u0, m0)) => {
                let drift = (utc as i128 - u0 as i128) - (mono as i128 - m0 as i128);
                if drift.abs() <= 2000 {
                    cs.stable_since.get_or_insert((utc, mono));
                } else {
                    cs.stable_since = None;
                }
            }
            None => cs.stable_since = Some((utc, mono)),
        }
        cs.last_tick = Some((utc, mono));
        let resume_file = self.path.join("expiry.resume");
        let resume = resume_file.exists();
        let stable = cs.stable_since.is_some_and(|(_, m)| mono.saturating_sub(m) >= self.config.rotation_ms);
        if cs.degraded.is_some() && (resume || stable) && cs.log(utc, chunks::Rec::Resume).is_ok() {
            cs.degraded = None;
            self.flags.expiry_degraded.store(false, Ordering::SeqCst);
        }
        if resume {
            let _ = fs::remove_file(resume_file);
        }
    }
    /// The 10 s pass: held deletes, chunks due, tombstones and summaries.
    fn pass(&self) {
        let _ = self.retry_retiring();
        let _ = self.retire(None);
        self.drop_tombstones();
    }
    /// A catalogue past its size is rewritten to the live list (5.2).
    fn rewrite_catalogue(&self) {
        let mut cs = self.chunks.lock().unwrap();
        let at = self.config.catalogue_rewrite_bytes;
        if at > 0 && cs.cat_bytes() > at {
            let live = cs.live_list(self.clock.utc_ms(), self.next_issued.load(Ordering::SeqCst));
            if let Some(cat) = cs.cat.as_mut() {
                if cat.rewrite(&live).is_err() {
                    self.flags.storage_error.store(true, Ordering::SeqCst);
                }
            }
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
        let clock = chunks::Clock::new(config.clock.clone());
        let start_ms = now_ms();
        let refuse = |e: io::Error, mut report: chunks::ReplayReport| -> io::Error {
            report.start_ms = start_ms;
            report.ready_ms = now_ms();
            report.refusal = Some(e.to_string());
            // The replay record is written before exit; its own failure does not hide the refusal.
            let _ = report.write(path);
            e
        };
        let peek = fs::read(path.join("epoch")).ok().filter(|b| b.len() == 8).map_or(0, |b| u64::from_le_bytes(b.try_into().unwrap()));
        let imported = match import_legacy(path, &config, &clock, peek + 1) {
            Ok(x) => x,
            Err(e) => {
                let report = chunks::ReplayReport { import: Some(format!("refused: {e}")), ..Default::default() };
                return Err(refuse(e, report));
            }
        };
        let mut rp = match replay(path, &config, &clock) {
            Ok(rp) => rp,
            Err((e, report)) => return Err(refuse(e, report)),
        };
        if let Some((_, line)) = imported {
            rp.report.import = Some(format!("{line}; {}", rp.report.import.take().unwrap_or_default()));
        }
        if rp.committed.len() > config.index_cap
            || rp.committed.values().map(|v| v.bytes.len()).sum::<usize>()
                + rp.dedup
                    .values()
                    .map(|d: &Dedup| d.fingerprint.len())
                    .sum::<usize>()
                > config.capacity
            || rp.dedup.len() > config.dedup_cap
        {
            let report = rp.report.clone();
            return Err(refuse(io::Error::other("replayed state exceeds configured limits"), report));
        }
        for (p, offset) in &rp.truncate {
            OpenOptions::new().write(true).open(p)?.set_len(*offset)?;
        }
        let cat_path = path.join("chunks").join("catalogue.log");
        if let Some((offset, _, _)) = &rp.report.catalogue_truncation {
            OpenOptions::new().write(true).open(&cat_path)?.set_len(*offset)?;
        }
        let mut cs = std::mem::replace(&mut rp.chunks, chunks::ChunkStore::new(path.join("chunks")));
        let mut catalogue = chunks::Catalogue::open(&cat_path)?;
        catalogue.last_ms = rp.cat_last_ms;
        cs.cat = Some(catalogue);
        let utc = clock.utc_ms();
        let mono = clock.mono_ms();
        let next = rp.next;
        let mut persisted_bytes = rp.persisted;
        // A crash between `retiring` and `retired` is finished here; the delete is idempotent.
        let retiring: Vec<u64> = cs
            .table
            .values()
            .filter(|c| c.state == chunks::ChunkState::Retiring)
            .map(|c| c.id)
            .collect();
        for id in retiring {
            let (p, bytes) = (cs.table[&id].path.clone(), cs.table[&id].bytes);
            match fs::remove_file(&p) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(_) => {
                    cs.table.get_mut(&id).unwrap().blocked_since = Some(utc);
                    continue;
                }
            }
            cs.log(utc, chunks::Rec::Retired { seq: id })?;
            let c = cs.table.get_mut(&id).unwrap();
            c.state = chunks::ChunkState::Retired;
            c.keys.clear();
            cs.retired_chunks += 1;
            cs.retired_bytes += bytes;
            persisted_bytes -= bytes;
            rp.report.retirements_finished.push(id);
        }
        // An open record whose file was never made retires at once: it holds nothing.
        for id in rp.report.empty_closed.clone() {
            cs.log(utc, chunks::Rec::Retired { seq: id })?;
            if let Some(c) = cs.table.get_mut(&id) {
                c.state = chunks::ChunkState::Retired;
            }
        }
        // Chunks open at the crash close now, at their replayed (and truncated) length.
        let open: Vec<u64> = cs
            .table
            .values()
            .filter(|c| c.state == chunks::ChunkState::Open)
            .map(|c| c.id)
            .collect();
        for id in open {
            let bytes = cs.table[&id].bytes;
            if bytes == 0 {
                // Nothing whole was written (or its torn tail went): the empty file retires, so no empty chunk stays.
                fs::remove_file(&cs.table[&id].path).or_else(|e| if e.kind() == io::ErrorKind::NotFound { Ok(()) } else { Err(e) })?;
                cs.log(utc, chunks::Rec::Retired { seq: id })?;
                cs.table.get_mut(&id).unwrap().state = chunks::ChunkState::Retired;
                rp.report.empty_closed.push(id);
                continue;
            }
            cs.log(utc, chunks::Rec::Close { seq: id, bytes, next })?;
            let c = cs.table.get_mut(&id).unwrap();
            c.state = chunks::ChunkState::Closed;
            c.close_ms = Some(utc);
            c.close_mono = Some(mono);
            rp.report.closed_at_start.push((id, bytes));
        }
        // Retirement pauses when the clock is in doubt (5.4).
        let beat = fs::read_to_string(path.join("heartbeat.json"))
            .ok()
            .and_then(|t| t.split_once("\"unix_ms\":").map(|(_, r)| r.chars().take_while(char::is_ascii_digit).collect::<String>()))
            .and_then(|d| d.parse::<u64>().ok());
        if utc < rp.cat_last_ms {
            cs.degraded = Some(format!("UTC {utc} is earlier than the catalogue's last record {}", rp.cat_last_ms));
        } else if beat.is_some_and(|b| utc > b + 86_400_000) {
            cs.degraded = Some(format!("UTC {utc} is more than 24 h past the heartbeat's last beat {}", beat.unwrap()));
        }
        rp.report.expiry_degraded = cs.degraded.clone();
        let flags = Arc::new(Flags::default());
        flags.expiry_degraded.store(cs.degraded.is_some(), Ordering::SeqCst);
        if !rp.storage_errors.is_empty() {
            flags.storage_error.store(true, Ordering::SeqCst);
        }
        rp.report.storage_errors = rp.storage_errors.clone();
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
        let mut committed = rp.committed;
        cs.summaries.expire(utc, config.summary_ms);
        // A tombstone no other retained chunk holds a record of leaves the index at start (5.3).
        let mut tomb_candidates = HashSet::new();
        let tombs: Vec<Key> = committed.iter().filter(|(_, v)| v.tombstone).map(|(k, _)| k.clone()).collect();
        for k in tombs {
            let chunk = rp.chunk_of.get(&k).copied().unwrap_or(0);
            let others: Vec<u64> = cs.holders.others(&k, chunk).collect();
            if others.is_empty() {
                committed.remove(&k);
            } else if others
                .iter()
                .all(|c| *c != chunks::LEGACY_CHUNK && cs.table.get(c).is_some_and(|i| i.class.finite()))
            {
                tomb_candidates.insert(k);
            }
        }
        let metadata: HashMap<_, _> = committed
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    Meta {
                        version: v.version,
                        tombstone: v.tombstone,
                        bytes: v.bytes.len(),
                        chunk: rp.chunk_of.get(k).copied().unwrap_or(0),
                    },
                )
            })
            .collect();
        // The namespace count is live keys; the index count adds tombstones and expiry summaries (5.3).
        let mut namespace_counts = HashMap::new();
        for ((ns, _), m) in metadata.iter() {
            if !m.tombstone {
                *namespace_counts.entry(ns.clone()).or_insert(0) += 1;
            }
        }
        let keys = metadata.len() + cs.summaries.charged;
        let committed_bytes = metadata.values().map(|m| m.bytes).sum();
        let dedup = rp.dedup;
        let dedup_entries = dedup.len();
        let dedup_bytes = dedup.values().map(|d| d.fingerprint.len()).sum();
        let mut expiry: Vec<_> = dedup.iter().map(|(k, d)| (k.clone(), d.at)).collect();
        expiry.sort_by_key(|(_, at)| *at);
        let timers = timers::State::restore(&committed)?;
        let mut policies = chunks::Policies::default();
        for ((ns, key), v) in committed.iter() {
            if ns == chunks::POLICY_NS && !v.tombstone {
                if let Some((pns, prefix, class)) = chunks::parse_policy(key, &v.bytes) {
                    policies.set(&pns, &prefix, Some(class));
                }
            }
        }
        let policies = Arc::new(RwLock::new(policies));
        let next_issued = Arc::new(AtomicU64::new(next));
        let chunks = Arc::new(Mutex::new(cs));
        let router = Arc::new(ChunkRouter {
            sinks: (0..4).map(|_| Mutex::new(HashMap::new())).collect(),
            table: RwLock::new(HashMap::new()),
            fenced: std::array::from_fn(|_| AtomicBool::new(false)),
            closed_io: Mutex::new([0; 3]),
            policies: policies.clone(),
            chunks: chunks.clone(),
            clock: clock.clone(),
            next: next_issued.clone(),
            flags: flags.clone(),
            chunk_bytes: config.chunk_bytes,
            rotation_ms: config.rotation_ms,
            fail_sync: config.fail_sync,
        });
        let (appenders, flush_owner) = start_routed_owners(router.clone(), 4, &config);
        let committed = Arc::new(RwLock::new(committed));
        let state = Arc::new(Mutex::new(State {
            pending: HashMap::new(),
            metadata,
            namespace_counts: namespace_counts.clone(),
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
            read_only: flags.storage_error.load(Ordering::SeqCst),
            warming: false,
            tomb_candidates,
        }));
        let maint = Arc::new(Maint {
            state: state.clone(),
            committed: committed.clone(),
            chunks: chunks.clone(),
            router: router.clone(),
            flags: flags.clone(),
            clock: clock.clone(),
            next_issued: next_issued.clone(),
            config: config.clone(),
            path: path.to_owned(),
        });
        let (tick_stop, tick_receiver) = mpsc::channel();
        let tick_maint = maint.clone();
        let tick_every = Duration::from_millis(config.tick_ms.max(1));
        let retire_every = Duration::from_millis(config.retire_ms);
        let ticker = std::thread::spawn(move || {
            let mut last_pass = Instant::now();
            while matches!(
                tick_receiver.recv_timeout(tick_every),
                Err(mpsc::RecvTimeoutError::Timeout)
            ) {
                tick_maint.router.idle_close();
                tick_maint.clock_tick();
                if last_pass.elapsed() >= retire_every {
                    last_pass = Instant::now();
                    tick_maint.pass();
                }
                tick_maint.rewrite_catalogue();
            }
        });
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
        let mut report = rp.report;
        report.epoch = epoch;
        report.start_ms = start_ms;
        report.dedup_entries = dedup_entries;
        report.keys = keys;
        report.namespace_counts = namespace_counts.iter().map(|(k, v)| (k.clone(), *v)).collect();
        report.ready_ms = now_ms();
        report.write(path)?;
        Ok(Self {
            timers,
            signals: signals::Hub::new(epoch, &config),
            capture_metrics: std::array::from_fn(|_| AtomicU64::new(0)),
            committed,
            scans,
            scan_usage: Arc::new(ScanUsage::default()),
            scan_reaper: Some((stop, reaper)),
            ticker: Some((tick_stop, ticker)),
            state,
            maint,
            done: Condvar::new(),
            router,
            chunks,
            policies,
            next_issued,
            flags,
            path: path.to_owned(),
            report: Mutex::new(report),
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
        let (b, _, f) = self.router.io();
        (b, f, self.committed.read().unwrap().len())
    }
    pub fn state_bytes(&self) -> u64 {
        self.state.lock().unwrap().persisted_bytes
    }
    pub fn io_metrics(&self) -> (u64, u64, u64) {
        self.router.io()
    }
    /// The last replay record (state/replay.json holds the same).
    pub fn replay_report(&self) -> chunks::ReplayReport {
        self.report.lock().unwrap().clone()
    }
    /// Chunk table rows as (id, stripe, class label, state, bytes), for tests and status.
    pub fn chunk_rows(&self) -> Vec<(u64, usize, String, String, u64)> {
        self.chunks
            .lock()
            .unwrap()
            .table
            .values()
            .map(|c| (c.id, c.stripe, c.class.label(), format!("{:?}", c.state), c.bytes))
            .collect()
    }
    pub fn open_chunks(&self) -> usize {
        self.router.open_chunks()
    }
    pub fn storage_error(&self) -> bool {
        self.flags.storage_error.load(Ordering::SeqCst)
    }
    pub fn expiry_degraded(&self) -> bool {
        self.flags.expiry_degraded.load(Ordering::SeqCst)
    }
    /// Retire one closed chunk by the steps of 5.4 (`retiring` with horizons, holders dropped, delete,
    /// `retired`) whatever its class's window; a chunk holding a key's latest record, or a pending key, stays.
    /// Ok(true) when retired; Ok(false) when kept, held or stopped at the test seam.
    pub fn retire_chunk(&self, id: u64) -> io::Result<bool> {
        Ok(self.maint.retire(Some(id))?.contains(&id))
    }
    /// The 10 s maintenance pass, run now: held deletes, chunks due by class, tombstones, summaries.
    pub fn retirement_pass(&self) {
        self.maint.pass();
    }
    /// Chunks whose delete is held (retiring, retried by the next pass).
    pub fn retire_blocked(&self) -> usize {
        self.chunks.lock().unwrap().table.values().filter(|c| c.blocked_since.is_some()).count()
    }
    pub fn retry_retiring(&self) -> io::Result<usize> {
        self.maint.retry_retiring()
    }
    /// (index entries with summaries, live keys in `ns`).
    pub fn slots(&self, ns: &str) -> (usize, usize) {
        let state = self.state.lock().unwrap();
        (state.keys, state.namespace_counts.get(ns).copied().unwrap_or(0))
    }
    /// HEALTH2's reading. Lock order: a stripe's sink before the chunk table (the write path), and the state before
    /// the chunk table (retirement); so the open chunks are counted before either lock is taken here.
    pub fn health2(&self) -> Health2 {
        let chunks_open = self.router.open_chunks();
        let state = self.state.lock().unwrap();
        let cs = self.chunks.lock().unwrap();
        let utc = self.maint.clock.utc_ms();
        let c = &self.config;
        let mut h = Health2 {
            state_bytes: state.persisted_bytes,
            state_limit_bytes: c.state_limit_bytes,
            state_high_water: state.persisted_bytes >= c.state_limit_bytes - c.state_limit_bytes / 4,
            read_only: state.read_only,
            storage_error: self.flags.storage_error.load(Ordering::SeqCst),
            expiry_degraded: self.flags.expiry_degraded.load(Ordering::SeqCst),
            dedup_entries: state.dedup.len(),
            dedup_cap: c.dedup_cap,
            dedup_high_water: state.dedup.len() * 100 >= c.dedup_cap * 70,
            keys: state.keys,
            index_cap: c.index_cap,
            quota: c.quota,
            chunks_open,
            retired_chunks: cs.retired_chunks,
            retired_bytes: cs.retired_bytes,
            ..Health2::default()
        };
        let mut namespaces: Vec<(String, usize)> =
            state.namespace_counts.iter().filter(|(_, n)| **n > 0).map(|(k, n)| (k.clone(), *n)).collect();
        namespaces.sort();
        h.namespace_high_water = namespaces.iter().any(|(_, n)| n * 100 >= c.quota * 70);
        h.namespaces = namespaces;
        let mut by_class: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
        let mut oldest_blocked = None::<u64>;
        for chunk in cs.table.values() {
            if chunk.state == chunks::ChunkState::Retired {
                continue;
            }
            let label = if chunk.id == chunks::LEGACY_CHUNK { "legacy".to_owned() } else { chunk.class.label() };
            *by_class.entry(label).or_insert(0) += chunk.bytes;
            h.retained_total += chunk.bytes;
            if chunk.state == chunks::ChunkState::Closed && chunk.id != chunks::LEGACY_CHUNK {
                h.chunks_closed += 1;
                let due = chunk.close_ms.is_some_and(|close| utc >= close.saturating_add(c.retry_ms));
                if chunk.class.kind == 1
                    && due
                    && chunk.keys.keys().any(|k| state.metadata.get(k).is_some_and(|m| m.chunk == chunk.id))
                {
                    h.pinned_bytes += chunk.bytes;
                }
            }
            if let Some(since) = chunk.blocked_since {
                h.retire_blocked += 1;
                oldest_blocked = Some(oldest_blocked.map_or(since, |o| o.min(since)));
            }
        }
        h.retained_bytes = by_class.into_iter().collect();
        h.pinned_high_water = h.pinned_bytes >= c.pinned_high_water_bytes;
        h.retire_blocked_oldest_ms = oldest_blocked.map_or(0, |since| utc.saturating_sub(since));
        h.retire_blocked_high_water = h.retire_blocked_oldest_ms > 3_600_000;
        h
    }
    /// GET_STATUS (5.3): 0 live with its version, 1 expired (in a summary), 2 missing.
    pub fn get_status(&self, ns: &str, key: &str) -> (u8, u64) {
        if let Some(v) = self.get_handle(ns, key) {
            return (0, v.version);
        }
        let k = (ns.to_owned(), key.to_owned());
        if self.chunks.lock().unwrap().summaries.keys.contains_key(&k) {
            (1, 0)
        } else {
            (2, 0)
        }
    }
    /// Set (Some) or clear (None) a namespace prefix's class: written durably to POLICY_NS, then used by
    /// writes that follow (5.1). Records already written keep their chunk's class.
    pub fn set_policy(&self, ns: &str, prefix: &str, class: Option<Class>, deadline: u64) -> Outcome {
        let key = match chunks::policy_key(ns, prefix) {
            Ok(k) => k,
            Err(code) => return Outcome::error(code),
        };
        if let Some(c) = class {
            // An expire class shorter than the retry window would retire records whose dedup a restart still needs.
            if c.kind == 2 && c.retention_ms < self.config.retry_ms {
                return Outcome::error(3);
            }
            if self.policies.read().unwrap().would_exceed(ns, prefix, c) {
                return Outcome::error(7);
            }
        }
        let m = Mutation {
            ns: POLICY_NS.into(),
            key: key.clone(),
            op_id: {
                static SEQ: AtomicU64 = AtomicU64::new(0);
                format!("policy/{}/{}/{}", self.epoch, now_ms(), SEQ.fetch_add(1, Ordering::SeqCst))
            },
            deadline,
            ack: 2,
            expected: None,
            bytes: class.map(chunks::policy_value).unwrap_or_default(),
            remove: class.is_none(),
        };
        let outcome = self.mutate(m);
        if outcome.code == 0 {
            self.policies.write().unwrap().set(ns, prefix, class);
        }
        outcome
    }
    /// Policy rows as (namespace, prefix, class).
    pub fn policies(&self) -> Vec<(String, String, Class)> {
        self.policies.read().unwrap().rows()
    }
    /// The idle tick's work, run now (tests use it with a manual clock).
    pub fn tick(&self) {
        self.router.idle_close();
        self.maint.clock_tick();
    }
    #[allow(dead_code)]
    fn state_path(&self) -> &Path {
        &self.path
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
        self.mutate_mode(m,true)
    }
    fn mutate_progress(&self, m: Mutation) -> Outcome {
        debug_assert!(timers::is_progress(&m.ns,&m.key));
        self.mutate_mode(m,false)
    }
    fn mutate_mode(&self, m: Mutation, deduplicate: bool) -> Outcome {
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
        if let Some(d) = state.dedup.get(&id).filter(|_|deduplicate) {
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
        if deduplicate && state.retired_set.contains(&id) {
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
        // An index entry is new when the key has none; a live key is new when it has none or a tombstone (5.3).
        let new = old.is_none();
        let live_new = !m.remove && old.as_ref().map_or(true, |o| o.tombstone);
        if new && state.keys >= self.config.index_cap {
            return Outcome::error(9);
        }
        if live_new && state.namespace_counts.get(&m.ns).copied().unwrap_or(0) >= self.config.quota {
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
            + if deduplicate {fp.len()} else {0}
            > self.config.capacity
        {
            return Outcome::error(7);
        }
        // Ordinary admissions stop at the limit less the maintenance reserve (5.5).
        if state.persisted_bytes + state.pending_disk + disk_bytes
            > self.config.state_limit_bytes.saturating_sub(self.config.maintenance_reserve_bytes)
        {
            return Outcome::error(7);
        }
        if state.reserved_bytes + queue_charge > self.config.queue_bytes
            || state.stripe_reserved[log_index] + queue_charge > self.config.queue_bytes / 4
            || state.stripe_items[log_index] >= 1024
        {
            return Outcome::error(6);
        }
        if deduplicate && state.dedup.len() >= self.config.dedup_cap {
            return Outcome::error(18);
        }
        // The clock check immediately precedes insertion: this is the admission point.
        if now_ms() >= m.deadline {
            return Outcome::error(5);
        }
        let version = state.next;
        state.next += 1;
        self.next_issued.fetch_max(state.next, Ordering::SeqCst);
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
                chunk: 0,
            },
        );
        if new {
            state.keys += 1;
        }
        if live_new {
            *state.namespace_counts.entry(m.ns.clone()).or_default() += 1;
        }
        state.reserved_bytes += queue_charge;
        state.stripe_reserved[log_index] += queue_charge;
        state.stripe_items[log_index] += 1;
        state.pending_disk += disk_bytes;
        if deduplicate {
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
        }
        drop(state);
        let result = if m.ack == 0 {
            Ok(0)
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
            if m.ns != timers::NS { self.signals.publish(&m.ns, &m.key, value); }
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
        if let (Ok(chunk), true) = (&result, m.ack != 0) {
            state.committed_bytes =
                state.committed_bytes - old.as_ref().map_or(0, |v| v.bytes) + m.bytes.len();
            state.metadata.insert(
                k.clone(),
                Meta {
                    version,
                    tombstone: m.remove,
                    bytes: m.bytes.len(),
                    chunk: *chunk,
                },
            );
            if m.remove {
                // A remove of a live key frees its namespace slot at commit; the tombstone keeps an index entry.
                if old.as_ref().is_some_and(|o| !o.tombstone) {
                    *state.namespace_counts.get_mut(&m.ns).unwrap() -= 1;
                }
                state.tomb_candidates.insert(k.clone());
            }
        } else {
            if new {
                state.keys -= 1;
            }
            if live_new {
                *state.namespace_counts.get_mut(&m.ns).unwrap() -= 1;
            }
        }
        if result.is_err() {
            state.read_only = true
        }
        if deduplicate { state.dedup.get_mut(&id).unwrap().outcome = Some(outcome.clone()); }
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
                let subscribe = client.split(';').skip(1).any(|feature| feature == "subscribe=1");
                let scans = client.ends_with(";scans=1");
                let timers = client.split(';').skip(1).any(|feature| feature == "timers=1");
                let mut timer_only_codes=CODES[..22].to_vec();
                timer_only_codes.push(CODES[24]);
                let codes = if timers && !subscribe {
                    timer_only_codes.as_slice()
                } else if timers {
                    CODES
                } else if subscribe {
                    &CODES[..24]
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
                if subscribe { p.extend(self.signals.hello_limits()); }
                if timers { p.extend(timers::hello_limits()); }
                return Ok(p);
            }
            let deadline = c.u64()?;
            let _op_id = c.text()?;
            let ns = c.text()?;
            // Internal schedule/progress records are not a public KV namespace.
            if ns == timers::NS || ns == chunks::POLICY_NS { return Err(3); }
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
                TIMER_SET | TIMER_CANCEL | TIMER_STATUS => self.timer_call(op,ns,_op_id,deadline,&mut c),
                POLICY_SET => {
                    let prefix = c.bound()?;
                    let kind = c.u8()?;
                    let retention = c.u64()?;
                    c.end()?;
                    let class = if kind == 255 { None } else { Some(Class::from_wire(kind, retention)?) };
                    let o = self.set_policy(&ns, &prefix, class, deadline);
                    if o.code != 0 {
                        return Err(o.code);
                    }
                    Ok(o.bytes())
                }
                POLICY_GET => {
                    c.end()?;
                    let rows: Vec<_> = self.policies().into_iter().filter(|r| r.0 == ns).collect();
                    let mut p = (rows.len() as u16).to_le_bytes().to_vec();
                    for (_, prefix, class) in rows {
                        lp(&mut p, prefix.as_bytes());
                        p.push(class.kind);
                        p.extend(class.retention_ms.to_le_bytes());
                    }
                    Ok(p)
                }
                HEALTH2 => {
                    c.end()?;
                    let fields = self.health2().fields();
                    let mut p = (fields.len() as u16).to_le_bytes().to_vec();
                    for (k, v) in fields {
                        lp(&mut p, format!("{k}={v}").as_bytes());
                    }
                    Ok(p)
                }
                GET_STATUS => {
                    let key = c.text()?;
                    c.end()?;
                    let (status, version) = self.get_status(&ns, &key);
                    let mut p = vec![status];
                    p.extend(version.to_le_bytes());
                    Ok(p)
                }
                SUBSCRIBE => {
                    let prefix = c.bound()?;
                    let endpoint = c.text()?;
                    let values = c.u8()?;
                    if values > 1 { return Err(3); }
                    let cursor = c.bytes(16)?;
                    let protocol = c.text()?;
                    let frame_max = c.u32()? as usize;
                    c.end()?;
                    self.signals.subscribe(&ns, prefix, endpoint, values != 0, cursor, &protocol, frame_max)
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
