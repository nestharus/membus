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
            last: None,
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
    last: Option<u64>,
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
            mut last,
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
                // The gather window closes on elapsed time; bounds are enforced
                // when selecting each handle's covering group below.
                let until = clock.mono_ms().saturating_add(if gather { interval } else { 0 });
                while clock.mono_ms() < until {
                    let left = until.saturating_sub(clock.mono_ms());
                    match flush_recv.recv_timeout(Duration::from_millis(left)) {
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
            let Some(front) = pending.front() else {
                continue;
            };
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
                let delay = interval.saturating_sub(clock.mono_ms().saturating_sub(previous));
                if delay > 0 {
                    std::thread::sleep(Duration::from_millis(delay));
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
            let result = file.and_then(|f| seams.sync_selected_file(&f));
            last = Some(clock.mono_ms());
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
