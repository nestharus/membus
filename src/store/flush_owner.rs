//! Shared flush owner: arrival protocol, gathering, coverage, synchronization and settlement.
use crate::routing::Route;
use crate::test_seam::TestSeam;
use crate::Config;
use std::collections::HashMap;
use std::collections::VecDeque;
use std::io;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

pub(crate) struct FlushWait {
    /// The sink to sync: a stripe's log, or a chunk id under the chunk route.
    pub(crate) stripe: usize,
    pub(crate) end: u64,
    pub(crate) bytes: usize,
    pub(crate) done: mpsc::Sender<io::Result<u64>>,
    pub(crate) gathered: bool,
}

pub(crate) fn finish_flush(w: FlushWait, result: io::Result<()>, outstanding: &AtomicUsize) {
    outstanding.fetch_sub(1, Ordering::SeqCst);
    let sink = w.stripe as u64;
    let _ = w.done.send(result.map(|_| sink));
}

pub(crate) fn enqueue_flush(
    mut wait: FlushWait,
    sender: &mpsc::Sender<FlushWait>,
    outstanding: &AtomicUsize,
    notice_gate: &Mutex<()>,
    after_classification: impl FnOnce(),
) {
    let _notice = notice_gate.lock().unwrap();
    wait.gathered = outstanding.fetch_add(1, Ordering::SeqCst) != 0;
    TestSeam::after_classification(after_classification);
    if let Err(e) = sender.send(wait) {
        finish_flush(
            e.0,
            Err(io::Error::other("flush owner stopped")),
            outstanding,
        );
    }
}

#[derive(Clone)]
pub(crate) struct FlushIngress {
    sender: mpsc::Sender<FlushWait>,
    outstanding: Arc<AtomicUsize>,
    notice_gate: Arc<Mutex<()>>,
}
impl FlushIngress {
    pub(crate) fn enqueue(&self, wait: FlushWait) {
        enqueue_flush(
            wait,
            &self.sender,
            &self.outstanding,
            &self.notice_gate,
            || {},
        );
    }
}
pub(crate) struct FlushHandle {
    handle: std::thread::JoinHandle<()>,
}
impl FlushOwner {
    pub(crate) fn start(route: Arc<dyn Route>, config: &Config, clock: crate::chunks::Clock) -> (FlushHandle, FlushIngress) {
        let (sender, receiver) = mpsc::channel();
        // Includes queued and currently syncing waiters. Classify at arrival,
        // not receipt: a notice queued during a sync must retain its gather policy.
        let outstanding = Arc::new(AtomicUsize::new(0));
        // Keep arrival classification and channel order together across stripes.
        // Without this, a preempted idle sender could be overtaken by a gathered
        // sender and inherit that later sender's window.
        let notice_gate = Arc::new(Mutex::new(()));
        let ingress = FlushIngress {
            sender,
            outstanding: outstanding.clone(),
            notice_gate,
        };
        let worker = Self {
            clock,
            receiver,
            route,
            outstanding,
            interval: config.flush_ms,
            seams: TestSeam::for_flush(config),
            pending: VecDeque::new(),
            last_by_sink: HashMap::new(),
            covered: HashMap::new(),
            sync_failed: [false; 4],
        };
        (
            FlushHandle {
                handle: std::thread::spawn(move || worker.run()),
            },
            ingress,
        )
    }
}
impl FlushHandle {
    pub(crate) fn join(self) -> std::thread::Result<()> {
        self.handle.join()
    }
}
/// The receiver, operating context and continuing state live on the flush thread.
pub(crate) struct FlushOwner {
    clock: crate::chunks::Clock,
    receiver: mpsc::Receiver<FlushWait>,
    route: Arc<dyn Route>,
    outstanding: Arc<AtomicUsize>,
    interval: u64,
    seams: TestSeam,
    pending: VecDeque<FlushWait>,
    // Per-sink last sync monotonic timestamp so independent stripes flush without cross-sink sleep.
    last_by_sink: HashMap<usize, u64>,
    // Keyed by sink: a sync covers the earlier complete appends of its own file.
    covered: HashMap<usize, u64>,
    sync_failed: [bool; 4],
}
impl FlushOwner {
    fn run(self) {
        let Self {
            clock,
            receiver: flush_recv,
            route: flush_route,
            outstanding: flush_outstanding,
            interval,
            seams,
            mut pending,
            mut last_by_sink,
            mut covered,
            mut sync_failed,
        } = self;
        // A sink whose close already synced it, or a stripe fenced by a failed sync, needs no sync here.
        let settle = |w: &FlushWait,
                      covered: &HashMap<usize, u64>,
                      sync_failed: &[bool; 4]|
         -> Option<io::Result<()>> {
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
            // A retired sink is removed only after Store pending operations have settled. A late
            // notice for a merely closed sink still needs its synced extent and remains present.
            covered.retain(|sink, _| flush_route.sink(*sink).is_some());
            last_by_sink.retain(|sink, _| flush_route.sink(*sink).is_some());
            seams.coverage_retained(covered.len());
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
            if pending.is_empty() {
                continue;
            }
            // Pick the first ready sink (delay == 0) in FIFO order; if every queued sink is
            // still inside its own per-sink gather spacing, wait once until the earliest sink
            // is ready (or an arrival on an unthrottled/newly-opened sink wakes us sooner).
            let now = clock.mono_ms();
            let sink_delay = |sink: usize, allow_spacing: bool, last: &HashMap<usize, u64>| -> u64 {
                if !allow_spacing {
                    return 0;
                }
                last.get(&sink).map_or(0, |&previous| {
                    interval.saturating_sub(now.saturating_sub(previous))
                })
            };
            let mut selected_sink = pending.front().unwrap().stripe;
            let mut min_delay = sink_delay(selected_sink, gather, &last_by_sink);
            if min_delay > 0 {
                for w in pending.iter().skip(1) {
                    let d = sink_delay(w.stripe, gather, &last_by_sink);
                    if d < min_delay {
                        min_delay = d;
                        selected_sink = w.stripe;
                        if d == 0 {
                            break;
                        }
                    }
                }
            }
            if min_delay > 0 {
                let mut deadline = std::time::Instant::now() + Duration::from_millis(min_delay);
                let mut until_mono = now.saturating_add(min_delay);
                loop {
                    if clock.mono_ms() >= until_mono {
                        break;
                    }
                    let left = deadline.saturating_duration_since(std::time::Instant::now());
                    if left.is_zero() {
                        break;
                    }
                    match flush_recv.recv_timeout(left) {
                        Ok(w) => match settle(&w, &covered, &sync_failed) {
                            Some(r) => finish_flush(w, r, &flush_outstanding),
                            None => {
                                let arrived_sink = w.stripe;
                                pending.push_back(w);
                                let arrived_now = clock.mono_ms();
                                let arrived_delay =
                                    last_by_sink.get(&arrived_sink).map_or(0, |&prev| {
                                        interval.saturating_sub(arrived_now.saturating_sub(prev))
                                    });
                                if arrived_delay == 0 {
                                    selected_sink = arrived_sink;
                                    break;
                                }
                                let arrived_deadline = std::time::Instant::now()
                                    + Duration::from_millis(arrived_delay);
                                if arrived_deadline < deadline {
                                    deadline = arrived_deadline;
                                    until_mono = arrived_now.saturating_add(arrived_delay);
                                    selected_sink = arrived_sink;
                                }
                            }
                        },
                        Err(mpsc::RecvTimeoutError::Timeout)
                        | Err(mpsc::RecvTimeoutError::Disconnected) => break,
                    }
                }
                while let Ok(w) = flush_recv.try_recv() {
                    match settle(&w, &covered, &sync_failed) {
                        Some(r) => finish_flush(w, r, &flush_outstanding),
                        None => pending.push_back(w),
                    }
                }
            }
            let sink = selected_sink;
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
            if group.is_empty() {
                continue;
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
                (log.flush_handle.clone(), log.bytes)
            };
            let result = seams.sync_selected_file(&file);
            last_by_sink.insert(sink, clock.mono_ms());
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
    }
}
