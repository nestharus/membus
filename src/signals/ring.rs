use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Weak};

use parking_lot::RwLock;
use tokio::sync::Notify;

use crate::signals::live_coalesced::CoalescingInbox;
use crate::storage::SignalPublisher;
use crate::types::SignalEvent;

const SHARD_COUNT: usize = 64;

/// Wildcard namespace token subscribing across all namespaces.
pub const WILDCARD_NAMESPACE: &str = "*";

/// Configuration for the in-memory sharded signal ring and subscriptions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SignalConfig {
    pub ring_capacity_per_namespace: usize,
    pub default_live_max_pending: usize,
    pub max_live_pending: usize,
    pub max_namespaces: usize,
    pub log_catchup_batch_size: usize,
}

impl Default for SignalConfig {
    fn default() -> Self {
        Self {
            ring_capacity_per_namespace: 4096,
            default_live_max_pending: 4096,
            max_live_pending: 65_536,
            max_namespaces: 16_384,
            log_catchup_batch_size: 256,
        }
    }
}

/// Outcome of reading events strictly after `after_seq` from a namespace ring.
pub enum RingRead {
    Events(Vec<Arc<SignalEvent>>),
    Empty,
    NeedLogCatchup { evicted_through_seq: u64 },
}

struct RingState {
    capacity: usize,
    entries: VecDeque<Arc<SignalEvent>>,
    evicted_through_seq: u64,
    latest_seq: u64,
    live_subscribers: Vec<Weak<CoalescingInbox>>,
}

impl RingState {
    fn new(capacity: usize, initial_evicted_through_seq: u64) -> Self {
        Self {
            capacity: capacity.max(16),
            entries: VecDeque::with_capacity(capacity.clamp(16, 4096)),
            evicted_through_seq: initial_evicted_through_seq,
            latest_seq: initial_evicted_through_seq,
            live_subscribers: Vec::new(),
        }
    }

    fn push_batch(&mut self, batch: &[Arc<SignalEvent>]) {
        if batch.is_empty() {
            return;
        }
        let batch_max_seq = batch.iter().map(|e| e.seq).max().unwrap_or(0);
        if batch_max_seq > self.latest_seq {
            self.latest_seq = batch_max_seq;
        }

        // Fan out to live coalesced subscribers first, pruning expired Weak handles.
        if !self.live_subscribers.is_empty() {
            self.live_subscribers.retain(|weak| {
                if let Some(inbox) = weak.upgrade() {
                    for ev in batch {
                        inbox.push(Arc::clone(ev));
                    }
                    true
                } else {
                    false
                }
            });
        }

        // If a single atomic commit batch exceeds ring capacity, evict cleanly at the
        // whole-seq boundary so no partial transaction tail ever enters the RAM ring.
        if batch.len() > self.capacity {
            self.entries.clear();
            if batch_max_seq > self.evicted_through_seq {
                self.evicted_through_seq = batch_max_seq;
            }
            return;
        }

        // Evict whole-seq groups from the front until the entire batch fits atomically.
        while self.entries.len().saturating_add(batch.len()) > self.capacity {
            let Some(evicted) = self.entries.pop_front() else {
                break;
            };
            let evicted_seq = evicted.seq;
            if evicted_seq > self.evicted_through_seq {
                self.evicted_through_seq = evicted_seq;
            }
            while self
                .entries
                .front()
                .is_some_and(|front| front.seq == evicted_seq)
            {
                let _ = self.entries.pop_front();
            }
        }

        for ev in batch {
            self.entries.push_back(Arc::clone(ev));
        }
    }

    fn has_live_subscribers(&mut self) -> bool {
        self.live_subscribers.retain(|w| w.strong_count() > 0);
        !self.live_subscribers.is_empty()
    }
}

/// Per-namespace in-memory ring buffer and non-blocking wakeup fan-out.
pub struct NamespaceChannel {
    state: RwLock<RingState>,
    notify: Arc<Notify>,
}

impl NamespaceChannel {
    fn new(capacity: usize, initial_evicted_through_seq: u64) -> Self {
        Self {
            state: RwLock::new(RingState::new(capacity, initial_evicted_through_seq)),
            notify: Arc::new(Notify::new()),
        }
    }

    pub(crate) fn notify(&self) -> &Arc<Notify> {
        &self.notify
    }

    pub(crate) fn push_batch(&self, batch: &[Arc<SignalEvent>]) {
        if batch.is_empty() {
            return;
        }
        {
            let mut guard = self.state.write();
            guard.push_batch(batch);
        }
        self.notify.notify_waiters();
    }

    pub(crate) fn register_live(&self, inbox: &Arc<CoalescingInbox>) {
        let mut guard = self.state.write();
        guard.live_subscribers.retain(|w| w.strong_count() > 0);
        guard.live_subscribers.push(Arc::downgrade(inbox));
    }

    pub(crate) fn evicted_through_seq(&self) -> u64 {
        self.state.read().evicted_through_seq
    }

    fn high_water_seq(&self) -> u64 {
        let guard = self.state.read();
        guard.latest_seq.max(guard.evicted_through_seq)
    }

    fn is_evictable_idle(&self) -> bool {
        let mut guard = self.state.write();
        !guard.has_live_subscribers()
    }

    /// Read events strictly greater than `after_seq` directly from RAM when available.
    pub(crate) fn read_after(
        &self,
        after_seq: u64,
        startup_floor_seq: u64,
        limit: usize,
    ) -> RingRead {
        let guard = self.state.read();
        if after_seq < startup_floor_seq || after_seq < guard.evicted_through_seq {
            return RingRead::NeedLogCatchup {
                evicted_through_seq: guard.evicted_through_seq.max(startup_floor_seq),
            };
        }
        if guard.entries.is_empty() || after_seq >= guard.latest_seq {
            return RingRead::Empty;
        }

        let pos = guard.entries.partition_point(|ev| ev.seq <= after_seq);
        if pos >= guard.entries.len() {
            return RingRead::Empty;
        }

        let min_count = (guard.entries.len() - pos).min(limit.max(1));
        let mut out = Vec::with_capacity(min_count);
        let mut iter = guard.entries.iter().skip(pos);
        let mut last_seq = 0u64;
        for ev in iter.by_ref().take(min_count) {
            last_seq = ev.seq;
            out.push(Arc::clone(ev));
        }
        // Include any trailing events belonging to the same atomic commit `seq`.
        for ev in iter {
            if ev.seq == last_seq {
                out.push(Arc::clone(ev));
            } else {
                break;
            }
        }
        RingRead::Events(out)
    }
}

struct RingShard {
    channels: HashMap<String, Arc<NamespaceChannel>>,
    evicted_through_seq: u64,
}

impl RingShard {
    fn new() -> Self {
        Self {
            channels: HashMap::new(),
            evicted_through_seq: 0,
        }
    }
}

/// Sharded in-memory signal ring shared between storage writers and async subscribers.
pub struct SignalRing {
    capacity: usize,
    max_per_shard: usize,
    shards: Vec<RwLock<RingShard>>,
    global_channel: Arc<NamespaceChannel>,
    active_subscriptions: Arc<AtomicUsize>,
}

impl SignalRing {
    pub fn new(config: SignalConfig) -> Self {
        let capacity = config.ring_capacity_per_namespace.max(16);
        let max_per_shard = config.max_namespaces.max(SHARD_COUNT).div_ceil(SHARD_COUNT);
        let mut shards = Vec::with_capacity(SHARD_COUNT);
        for _ in 0..SHARD_COUNT {
            shards.push(RwLock::new(RingShard::new()));
        }
        Self {
            capacity,
            max_per_shard,
            shards,
            global_channel: Arc::new(NamespaceChannel::new(capacity, 0)),
            active_subscriptions: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn channel_for(&self, namespace: &str) -> Arc<NamespaceChannel> {
        if namespace == WILDCARD_NAMESPACE {
            return Arc::clone(&self.global_channel);
        }
        let shard = &self.shards[shard_index(namespace)];
        if let Some(existing) = shard.read().channels.get(namespace) {
            return Arc::clone(existing);
        }
        let mut write_guard = shard.write();
        if let Some(existing) = write_guard.channels.get(namespace) {
            return Arc::clone(existing);
        }
        if write_guard.channels.len() >= self.max_per_shard {
            // Reclaim an unreferenced idle namespace ring and preserve its high-water
            // sequence in the shard so recreated channels never skip pre-eviction commits.
            let idle_key = write_guard
                .channels
                .iter()
                .find(|(_k, ch)| Arc::strong_count(ch) == 1 && ch.is_evictable_idle())
                .map(|(k, _)| k.clone());
            if let Some(key) = idle_key {
                if let Some(evicted) = write_guard.channels.remove(&key) {
                    let watermark = evicted.high_water_seq();
                    if watermark > write_guard.evicted_through_seq {
                        write_guard.evicted_through_seq = watermark;
                    }
                }
            }
        }
        let initial_evicted = write_guard.evicted_through_seq;
        let ch = Arc::new(NamespaceChannel::new(self.capacity, initial_evicted));
        write_guard
            .channels
            .insert(namespace.to_owned(), Arc::clone(&ch));
        ch
    }

    pub fn active_subscriptions(&self) -> usize {
        self.active_subscriptions.load(Ordering::Acquire)
    }

    pub(crate) fn subscription_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.active_subscriptions)
    }

    pub fn namespace_count(&self) -> usize {
        self.shards.iter().map(|s| s.read().channels.len()).sum()
    }
}

impl SignalPublisher for SignalRing {
    fn publish_ordered(&self, events: &[SignalEvent]) {
        if events.is_empty() {
            return;
        }
        let shared: Vec<Arc<SignalEvent>> = events.iter().cloned().map(Arc::new).collect();

        // Fast path: all events in this commit belong to a single namespace.
        let first_ns = shared[0].namespace.as_str();
        if shared.iter().all(|e| e.namespace == first_ns) {
            let channel = self.channel_for(first_ns);
            channel.push_batch(&shared);
            self.global_channel.push_batch(&shared);
            return;
        }

        // Cross-namespace transaction: publish each namespace's subset atomically
        // under one write lock per channel, then publish the full batch to global.
        let mut by_ns: HashMap<&str, Vec<Arc<SignalEvent>>> = HashMap::new();
        for ev in &shared {
            by_ns
                .entry(ev.namespace.as_str())
                .or_default()
                .push(Arc::clone(ev));
        }
        for (ns, ns_batch) in by_ns {
            let channel = self.channel_for(ns);
            channel.push_batch(&ns_batch);
        }
        self.global_channel.push_batch(&shared);
    }
}

fn shard_index(namespace: &str) -> usize {
    // 64-bit FNV-1a hash for deterministic sharding with zero external dependencies.
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in namespace.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    (hash as usize) % SHARD_COUNT
}
