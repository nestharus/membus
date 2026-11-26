//! Core persistent KV store, chunk lifecycle, replay, maintenance, cache, and snapshots.
pub(crate) mod append_owner;
mod borrowed_keys;
pub(crate) mod cache;
pub mod chunks;
pub mod clock;
pub(crate) mod flush_owner;
pub mod hash;
pub mod indexed;
pub(crate) mod log_io;
pub(crate) mod maintenance;
pub mod namespace_reserve;
pub(crate) mod read_cohort;
pub(crate) mod receiving;
pub mod recovery_image;
pub(crate) mod replay;
pub(crate) mod routing;
pub mod selected_view;
pub mod serving_authority;
pub(crate) mod snapshots;
pub(crate) mod state;
pub(crate) mod test_seam;
pub(crate) mod value_disk;

pub(crate) use state::{
    committed_get, dedup_charge, dedup_identity, fingerprint, index_charge, Committed, Dedup,
    Flags, Key, Meta, State,
};

use crate::{
    signals, timers, CachePolicy, CacheRule, CacheStats, Class, MAX_ID,
};
use append_owner::{AppendHandle, AppendOwner};
use flush_owner::{FlushHandle, FlushOwner};
use maintenance::Maint;
use routing::{ChunkRouter, Route};
use snapshots::{ScanState, ScanUsage};
use std::{
    collections::HashMap,
    fs::File,
    io,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, AtomicU64},
        mpsc, Arc, Condvar, Mutex, RwLock,
    },
};
#[cfg(test)]
use routing::FixedRoute;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;

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
    pub(crate) file: Arc<File>,
    pub(crate) flush_handle: Arc<File>,
    pub bytes: u64,
    pub flushes: u64,
    pub write_calls: u64,
    pub(crate) poisoned: bool,
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
    #[allow(clippy::type_complexity)]
    pub sync_test_gate: Option<Arc<(Mutex<(bool, bool)>, Condvar)>>,
    #[cfg(test)]
    pub sync_test_notice: Option<mpsc::Sender<()>>,
    #[cfg(test)]
    pub stop_after_replay_truncation: bool,
    #[cfg(test)]
    pub flush_covered_count: Option<Arc<AtomicUsize>>,
    /// Test seam: observe or refuse each recovery directory barrier before the OS call.
    #[cfg(test)]
    #[allow(clippy::type_complexity)]
    pub recovery_dir_sync: Option<Arc<dyn Fn(&Path) -> io::Result<()> + Send + Sync>>,
    /// Test seam: observe or refuse a startup report's directory barriers.
    #[cfg(test)]
    #[allow(clippy::type_complexity)]
    pub replay_report_dir_sync: Option<Arc<dyn Fn(&Path) -> io::Result<()> + Send + Sync>>,
    /// Test seam: observe or refuse each runtime chunk-directory barrier.
    #[cfg(test)]
    #[allow(clippy::type_complexity)]
    pub chunk_dir_sync: Option<Arc<dyn Fn(&Path) -> io::Result<()> + Send + Sync>>,
    /// Test seam: observe or refuse state-directory name barriers before ownership.
    #[cfg(test)]
    #[allow(clippy::type_complexity)]
    pub state_dir_sync: Option<Arc<dyn Fn(&Path) -> io::Result<()> + Send + Sync>>,
    /// Test seam: pause after retirement preparation releases State and chunk locks.
    #[cfg(test)]
    pub retirement_started: Option<Arc<dyn Fn() + Send + Sync>>,
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
            #[cfg(test)]
            retirement_started: None,
        }
    }
}
impl Config {
    /// Admission margin for protocol buffers, timer schedules, threads and allocator bookkeeping.
    pub(crate) fn memory_margin(&self) -> usize { (self.capacity / 8).min(32 * 1024 * 1024) }
}
pub struct Store {
    pub(crate) clock: chunks::Clock,
    pub(crate) timers: timers::Shared,
    pub(crate) signals: signals::Hub,
    pub(crate) capture_metrics: [AtomicU64; 7],
    #[cfg(test)]
    pub(crate) capture_visits: AtomicUsize,
    #[cfg(test)]
    pub(crate) capture_waiters: AtomicUsize,
    pub(crate) committed: Arc<RwLock<state::Committed>>,
    pub(crate) cache: Arc<cache::Cache>,
    pub(crate) cold_readers: state::ColdDecodeLimiter,
    pub(crate) read_usage: Arc<read_cohort::ReadUsage>,
    pub(crate) scans: Arc<Mutex<ScanState>>,
    pub(crate) scan_usage: Arc<ScanUsage>,
    pub(crate) subscription_released: Arc<AtomicBool>,
    pub(crate) scan_reaper: Option<(mpsc::Sender<()>, std::thread::JoinHandle<()>)>,
    /// The idle tick (5.2): closes finite-class chunks past their age and rewrites a large catalogue.
    pub(crate) ticker: Option<(mpsc::Sender<()>, std::thread::JoinHandle<()>)>,
    pub(crate) state: Arc<Mutex<State>>,
    pub(crate) done: Condvar,
    pub(crate) maint: Arc<Maint>,
    pub(crate) router: Arc<ChunkRouter>,
    pub(crate) chunks: Arc<Mutex<chunks::ChunkStore>>,
    pub(crate) policies: Arc<RwLock<chunks::Policies>>,
    /// The version high-water the catalogue records at each open and close.
    pub(crate) next_issued: Arc<AtomicU64>,
    pub(crate) flags: Arc<Flags>,
    pub(crate) path: PathBuf,
    pub(crate) report: Mutex<chunks::ReplayReport>,
    pub(crate) appenders: Vec<AppendHandle>,
    pub(crate) flush_owner: Option<FlushHandle>,
    pub(crate) batch_gate: Mutex<()>,
    pub(crate) admission_gate: Arc<RwLock<()>>,
    // Administrative ordering never holds the routing map lock across append/flush work.
    pub(crate) policy_updates: Mutex<()>,
    pub(crate) class_reservations: Arc<Mutex<HashMap<Class, usize>>>,
    #[cfg(test)]
    pub(crate) policy_after_commit: Mutex<Option<(mpsc::Sender<()>, mpsc::Receiver<()>)>>,
    #[cfg(test)]
    pub(crate) policy_join_observed: Mutex<Option<mpsc::Sender<()>>>,
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
    pub(crate) fn stop_workers(&mut self) {
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
pub(crate) fn start_owners(logs: &[Arc<Mutex<Log>>], config: &Config) -> (Vec<AppendHandle>, FlushHandle) {
    start_routed_owners(Arc::new(FixedRoute(logs.to_vec())), logs.len(), config, chunks::Clock::new(config.clock.clone()))
}
impl Store {
    pub fn open(path: &Path, config: Config) -> io::Result<Self> {
        if path.join(".membus-quarantine").try_exists()? {
            return Err(io::Error::other("quarantined recovery has no serving authority"));
        }
        if path.join("capture.started").try_exists()? || path.parent().is_some_and(|p|p.join("capture.started").exists()) {
            return Err(io::Error::other("retained capture image cannot be opened for serving"));
        }
        Self::open_impl(path, config, false)
    }
    pub(crate) fn open_quarantined(path: &Path, config: Config) -> io::Result<Self> {
        if !path.join(".membus-quarantine").is_file() {return Err(io::Error::other("missing quarantine fence"));}
        Self::open_impl(path, config, true)
    }
    fn open_impl(path: &Path, mut config: Config, quarantine: bool) -> io::Result<Self> {
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
        let owner = crate::platform::open_exclusive_owner(&path.join("owner.lock"))?;
        Self::open_owned_mode(path, config, owner, quarantine)
    }
    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) fn open_owned(path: &Path, config: Config, owner: File) -> io::Result<Self> {
        Self::open_owned_mode(path, config, owner, false)
    }
    fn open_owned_mode(path: &Path, config: Config, owner: File, quarantine: bool) -> io::Result<Self> {
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
        let mut committed_vec: Vec<_> = committed.into_iter().collect();
        committed_vec.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let committed = Arc::new(RwLock::new(committed_vec.into_iter().collect::<state::Committed>()));
        let cache = Arc::new(cache::Cache::new(&config, clock.clone()));
        cache.trim(state.available_memory(config.capacity, 0, config.memory_margin(),
            chunks.lock().unwrap().managed_index_bytes(),
            0));
        let state = Arc::new(Mutex::new(state));
        let admission_gate = Arc::new(RwLock::new(()));
        let maint = Arc::new(Maint {
            admission_gate: admission_gate.clone(),
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
        let ticker = if quarantine {None} else {let (stop, worker)=maint.start();Some((stop,worker))};
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
            cold_readers: state::ColdDecodeLimiter::new(config.memory_margin()),
            read_usage: Arc::new(read_cohort::ReadUsage::default()),
            scans,
            scan_usage: Arc::new(ScanUsage::default()),
            subscription_released: Arc::new(AtomicBool::new(false)),
            scan_reaper: Some((stop, reaper)),
            ticker,
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
            admission_gate,
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
