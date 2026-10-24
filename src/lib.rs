//! membus.kv/1 slice 1: bounded framing, independent committed reads, durable replay.
//! Chunk retirement (docs/CHUNK-RETIREMENT.md): chunk files by class, their catalogue, replay by version.
pub mod chunks;
#[cfg(test)]
extern crate self as membus;
mod signals;
mod timers;
pub use chunks::{Class, ManualClock, POLICY_NS};
pub mod client;
pub mod indexed;
#[cfg(windows)]
pub mod pipe_io;
use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64},
        mpsc, Arc, Condvar, Mutex, RwLock,
    },
    time::{SystemTime, UNIX_EPOCH},
};
#[cfg(test)]
use std::{
    collections::HashSet,
    io::Write,
    sync::atomic::{AtomicUsize, Ordering},
    time::{Duration, Instant},
};
mod append_owner;
mod cache;
mod value_disk;
pub use cache::{CachePolicy, CacheRule, CacheStats};
mod codec;
mod flush_owner;
mod heartbeat;
mod log_io;
mod maintenance;
mod replay;
mod routing;
mod snapshots;
mod store;
mod test_seam;
#[cfg(test)]
use append_owner::Append;
use append_owner::{AppendHandle, AppendOwner};
#[cfg(test)]
use codec::mutation_bytes;
pub use codec::{
    crc32, frame, lp, read_frame, read_frame_limit, reply, Cursor, BATCH, CAPTURE_METRICS, CODES,
    CONDITIONAL_PUT, GET, GET_MANY, GET_STATUS, HEALTH, HEALTH2, HELLO, IO_METRICS, MAX_BATCH,
    MAX_FRAME, MAX_ID, MAX_VALUE, POLICY_GET, POLICY_SET, PUT, REMOVE, SCAN_KEYS_PREFIX,
    SCAN_KEYS_RANGE, SCAN_PREFIX, SCAN_RANGE, SIGNAL_USAGE, SNAPSHOT_OPEN, SNAPSHOT_RELEASE,
    SUBSCRIBE, SUBSCRIPTION_STATUS, TIMER_CANCEL, TIMER_SET, TIMER_STATUS, UNSUBSCRIBE,
};
#[cfg(test)]
use flush_owner::{enqueue_flush, FlushWait};
use flush_owner::{FlushHandle, FlushOwner};
pub use heartbeat::heartbeat_json;
use log_io::sync_file;
use maintenance::Maint;
pub use maintenance::CatalogueMaintenance;
#[cfg(test)]
use routing::{stripe, FixedRoute};
use routing::{ChunkRouter, Route};
use snapshots::{ScanState, ScanUsage};
#[cfg(test)]
use store::Key;
use store::{Flags, State};
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
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
pub struct Log {
    file: File,
    pub bytes: u64,
    pub flushes: u64,
    pub write_calls: u64,
    poisoned: bool,
}
#[derive(Clone)]
pub struct Config {
    /// Resident value bytes plus cache bookkeeping; process admission also charges the index and retries.
    pub cache_bytes: usize,
    pub cache_policy: CachePolicy,
    pub cache_rules: Vec<CacheRule>,
    pub signal_history: usize,
    pub signal_bytes: usize,
    pub snapshot_bytes: usize,
    pub snapshot_max: usize,
    pub snapshot_ttl_ms: u64,
    pub state_limit_bytes: u64,
    pub quota: usize,
    /// Global slots for live entries, retained tombstones and expiry summaries.
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
    /// Test seam: refuse the data-file barrier during startup, independently of live flushes.
    pub fail_recovery_sync: bool,
    /// Chunk rotation (CHUNK-RETIREMENT.md 5.2): a chunk closes past `chunk_bytes`, and a finite class's chunk
    /// also past `rotation_ms`; the idle tick runs every `tick_ms`.
    pub chunk_bytes: u64,
    pub rotation_ms: u64,
    pub tick_ms: u64,
    /// The retirement pass (5.4).
    pub retire_ms: u64,
    /// Catalogue growth trigger; zero disables size compaction, while necessary fence repair still runs.
    pub catalogue_rewrite_bytes: u64,
    /// Retain expiry summaries for this many milliseconds; the default is 30 days.
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
    #[cfg(test)]
    pub stop_after_replay_truncation: bool,
    #[cfg(test)]
    pub flush_covered_count: Option<Arc<AtomicUsize>>,
    /// Test seam: observe or refuse each recovery directory barrier before the OS call.
    #[cfg(test)]
    pub recovery_dir_sync: Option<Arc<dyn Fn(&Path) -> io::Result<()> + Send + Sync>>,
    /// Test seam: observe or refuse a startup report's directory barriers.
    #[cfg(test)]
    pub replay_report_dir_sync: Option<Arc<dyn Fn(&Path) -> io::Result<()> + Send + Sync>>,
    /// Test seam: observe or refuse each runtime chunk-directory barrier.
    #[cfg(test)]
    pub chunk_dir_sync: Option<Arc<dyn Fn(&Path) -> io::Result<()> + Send + Sync>>,
    /// Test seam: observe or refuse state-directory name barriers before ownership.
    #[cfg(test)]
    pub state_dir_sync: Option<Arc<dyn Fn(&Path) -> io::Result<()> + Send + Sync>>,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            cache_bytes: 128 * 1024 * 1024,
            cache_policy: CachePolicy::default(),
            cache_rules: cache::defaults(),
            signal_history: 4096,
            signal_bytes: 16 * 1024 * 1024,
            snapshot_bytes: 256 * 1024 * 1024,
            snapshot_max: 8,
            snapshot_ttl_ms: 30_000,
            state_limit_bytes: 8 * 1024 * 1024 * 1024,
            quota: 500_000,
            index_cap: 917_000,
            capacity: 512 * 1024 * 1024,
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
            fail_recovery_sync: false,
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
            #[cfg(test)]
            stop_after_replay_truncation: false,
            #[cfg(test)]
            flush_covered_count: None,
            #[cfg(test)]
            recovery_dir_sync: None,
            #[cfg(test)]
            replay_report_dir_sync: None,
            #[cfg(test)]
            chunk_dir_sync: None,
            #[cfg(test)]
            state_dir_sync: None,
        }
    }
}
impl Config {
    /// Admission margin for protocol buffers, timer schedules, threads and allocator bookkeeping.
    pub(crate) fn memory_margin(&self) -> usize { (self.capacity / 8).min(32 * 1024 * 1024) }
}
pub struct Store {
    clock: chunks::Clock,
    timers: timers::Shared,
    signals: signals::Hub,
    capture_metrics: [AtomicU64; 7],
    #[cfg(test)]
    capture_visits: AtomicUsize,
    #[cfg(test)]
    capture_waiters: AtomicUsize,
    committed: Arc<RwLock<store::Committed>>,
    cache: Arc<cache::Cache>,
    cold_readers: (Mutex<usize>, Condvar),
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
    appenders: Vec<AppendHandle>,
    flush_owner: Option<FlushHandle>,
    batch_gate: Mutex<()>,
    // Administrative ordering never holds the routing map lock across append/flush work.
    policy_updates: Mutex<()>,
    class_reservations: Arc<Mutex<HashMap<Class, usize>>>,
    #[cfg(test)]
    policy_after_commit: Mutex<Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>>,
    #[cfg(test)]
    policy_join_observed: Mutex<Option<mpsc::Sender<()>>>,
    pub config: Config,
    pub epoch: u64,
    _owner: File,
}
impl Drop for Store {
    fn drop(&mut self) {
        self.stop_workers();
    }
}
impl Store {
    fn stop_workers(&mut self) {
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
/// HEALTH2 (opcode 27; CHUNK-RETIREMENT.md section 6): the store's state, limits and chunk retirement in one read.
#[derive(Clone, Debug, Default)]
pub struct Health2 {
    pub cache: CacheStats,
    pub memory_bytes: usize,
    pub memory_budget: usize,
    pub state_bytes: u64,
    pub state_limit_bytes: u64,
    pub state_high_water: bool,
    pub read_only: bool,
    pub storage_error: bool,
    /// Cause of the last failed catalogue rewrite, until a rewrite succeeds.
    pub catalogue_rewrite_error: Option<String>,
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
fn start_routed_owners(
    route: Arc<dyn Route>,
    stripes: usize,
    config: &Config,
    clock: chunks::Clock,
) -> (Vec<AppendHandle>, FlushHandle) {
    let (flush_owner, ingress) = FlushOwner::start(route.clone(), config, clock);
    let owners = (0..stripes)
        .map(|stripe| AppendOwner::start(stripe, route.clone(), config.clone(), ingress.clone()))
        .collect();
    (owners, flush_owner)
}
#[cfg(test)]
fn start_owners(logs: &[Arc<Mutex<Log>>], config: &Config) -> (Vec<AppendHandle>, FlushHandle) {
    start_routed_owners(Arc::new(FixedRoute(logs.to_vec())), logs.len(), config, chunks::Clock::new(config.clock.clone()))
}
impl Store {
    pub fn open(path: &Path, mut config: Config) -> io::Result<Self> {
        // Change history is optional bounded retention, so it yields to a smaller process budget.
        config.signal_bytes = config.signal_bytes.min(config.capacity / 8);
        if config.cache_rules.len() > 64 || config.cache_rules.iter().any(|r| r.label.is_empty()
            || r.label.len() > MAX_ID || r.label.contains(['=', '\n', '\r'])
            || r.prefix.len() > MAX_ID || r.namespace.as_ref().is_some_and(|ns| ns.len() > MAX_ID)) {
            return Err(io::Error::other("invalid cache rules or more than 64 rules"));
        }
        if config.snapshot_ttl_ms == 0 {
            return Err(io::Error::other("snapshot lifetime must be positive"));
        }
        log_io::prepare_state_directory(path, &config)?;
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
        let replay::Recovered {
            clock,
            start_mono_ms,
            flags,
            chunks: cs,
            next,
            committed,
            state,
            timers,
            policies,
            epoch,
            mut report,
        } = replay::recover(path, &config)?;
        let policies = Arc::new(RwLock::new(policies));
        let class_reservations = Arc::new(Mutex::new(HashMap::new()));
        let next_issued = Arc::new(AtomicU64::new(next));
        let chunks = Arc::new(Mutex::new(cs));
        let router = Arc::new(ChunkRouter {
            locations: Mutex::new(HashMap::new()),
            #[cfg(test)]
            selection_hook: Mutex::new(None),
            sinks: (0..4).map(|_| Mutex::new(HashMap::new())).collect(),
            table: RwLock::new(HashMap::new()),
            fenced: std::array::from_fn(|_| AtomicBool::new(false)),
            closed_io: Mutex::new([0; 3]),
            policies: policies.clone(),
            class_reservations: class_reservations.clone(),
            chunks: chunks.clone(),
            clock: clock.clone(),
            next: next_issued.clone(),
            flags: flags.clone(),
            chunk_bytes: config.chunk_bytes,
            rotation_ms: config.rotation_ms,
            fail_sync: config.fail_sync,
        });
        let (appenders, flush_owner) = start_routed_owners(router.clone(), 4, &config, clock.clone());
        let committed = Arc::new(RwLock::new(committed.into_iter().collect::<store::Committed>()));
        let cache = Arc::new(cache::Cache::new(&config, clock.clone()));
        cache.trim(config.capacity.saturating_sub(state.committed_bytes + state.dedup_bytes
            + chunks.lock().unwrap().index_bytes + config.memory_margin()));
        let state = Arc::new(Mutex::new(state));
        let maint = Arc::new(Maint {
            cache: cache.clone(),
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
        let (tick_stop, ticker) = maint.start();
        let scans = Arc::new(Mutex::new(ScanState {
            next: 1,
            snapshots: HashMap::new(),
        }));
        let (stop, reaper) = snapshots::start_reaper(scans.clone(), &config, clock.clone());
        report.ready_ms = clock.utc_ms();
        report.elapsed_ms = Some(clock.mono_ms().saturating_sub(start_mono_ms));
        report.stage = "ready".into();
        // Own every worker before the last fallible publication. On error,
        // Store::drop stops and joins them before releasing the directory owner.
        let mut store = Self {
            clock: clock.clone(),
            timers,
            signals: signals::Hub::new(epoch, &config, clock.clone()),
            capture_metrics: std::array::from_fn(|_| AtomicU64::new(0)),
            #[cfg(test)]
            capture_visits: AtomicUsize::new(0),
            #[cfg(test)]
            capture_waiters: AtomicUsize::new(0),
            committed,
            cache,
            cold_readers: (Mutex::new(0), Condvar::new()),
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
            policy_updates: Mutex::new(()),
            class_reservations,
            #[cfg(test)]
            policy_after_commit: Mutex::new(None),
            #[cfg(test)]
            policy_join_observed: Mutex::new(None),
            config,
            epoch,
            _owner: owner,
        };
        let publication = store.report.lock().unwrap().write_startup(path, &store.config);
        if let Err(error) = publication {
            // Keep ownership until workers have joined and the best-effort
            // refusal replaces any ready snapshot visible from the failed write.
            store.stop_workers();
            let mut report = store.report.lock().unwrap();
            report.ready_ms = 0;
            report.elapsed_ms = Some(store.clock.mono_ms().saturating_sub(start_mono_ms));
            report.stage = "refused".into();
            report.refusal = Some(error.to_string());
            let _ = report.write_startup(path, &store.config);
            return Err(error);
        }
        Ok(store)
    }
}

#[cfg(test)]
#[path = "flush_owner/tests.rs"]
mod flush_policy_tests;

#[cfg(test)]
#[path = "store/namespace_release_tests.rs"]
mod namespace_release_tests;

#[cfg(test)]
#[path = "snapshots/tests.rs"]
mod scan_retention_tests;

#[cfg(test)]
#[path = "maintenance/tests.rs"]
mod retirement_failure_tests;

#[cfg(test)]
#[path = "store/receiving_correction_tests.rs"]
mod receiving_correction_tests;

#[cfg(test)]
mod policy_recovery_tests;

#[cfg(test)]
#[path = "store/cold_load_tests.rs"]
mod cold_load_tests;

#[cfg(test)]
#[path = "value_disk/tests.rs"]
mod cold_identity_tests;
