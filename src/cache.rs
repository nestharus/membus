//! Sharded weighted LRU. The wheel owns one expiry link per resident version.
use crate::{chunks::Clock, Class, Config, Value};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard,
    },
};
const SHARDS: usize = 16;
const SLOTS: usize = 256;
const TICK: u64 = 10;
const LOAD_GATES: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CachePolicy {
    /// Milliseconds from durable publication; no read means expiry at this boundary.
    pub initial_ttl: u64,
    /// Milliseconds from the first initial-window read, or from a cold load.
    pub active_ttl: u64,
    pub refresh_on_read: bool,
    pub never_cache: bool,
}
impl Default for CachePolicy {
    fn default() -> Self {
        Self {
            initial_ttl: 1_000,
            active_ttl: 60_000,
            refresh_on_read: true,
            never_cache: false,
        }
    }
}
/// Last matching rule wins; namespace and retention class selectors can be combined.
#[derive(Clone, Debug)]
pub struct CacheRule {
    pub namespace: Option<String>,
    pub prefix: String,
    pub class: Option<Class>,
    pub label: String,
    pub policy: CachePolicy,
}
impl CacheRule {
    pub fn namespace(ns: &str, policy: CachePolicy) -> Self {
        Self {
            namespace: Some(ns.into()),
            prefix: String::new(),
            class: None,
            label: ns.into(),
            policy,
        }
    }
}
impl std::str::FromStr for CacheRule {
    type Err = std::io::Error;
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let error = || {
            std::io::Error::other(
                "cache-policy needs NS,PREFIX,CLASS,LABEL,INITIAL_MS,ACTIVE_MS,REFRESH,NEVER",
            )
        };
        let fields: Vec<_> = text.split(',').collect();
        if fields.len() != 8 {
            return Err(error());
        }
        let class = match fields[2] {
            "*" => None,
            "forever" => Some(Class::FOREVER),
            "latest" => Some(Class::LATEST),
            value => {
                let ms = value
                    .strip_prefix("expire:")
                    .ok_or_else(error)?
                    .parse::<u64>()
                    .map_err(|_| error())?;
                Some(Class::from_wire(2, ms).map_err(|_| error())?)
            }
        };
        Ok(Self {
            namespace: (fields[0] != "*").then(|| fields[0].to_owned()),
            prefix: fields[1].into(),
            class,
            label: fields[3].into(),
            policy: CachePolicy {
                initial_ttl: fields[4].parse().map_err(|_| error())?,
                active_ttl: fields[5].parse().map_err(|_| error())?,
                refresh_on_read: fields[6].parse().map_err(|_| error())?,
                never_cache: fields[7].parse().map_err(|_| error())?,
            },
        })
    }
}
pub(crate) fn defaults() -> Vec<CacheRule> {
    [
        ("ledger", 1_000, 3_600_000),
        ("metrics", 500, 300_000),
        ("logs", 50, 10_000),
        ("evals", 50, 10_000),
    ]
    .into_iter()
    .map(|(ns, initial_ttl, active_ttl)| {
        CacheRule::namespace(
            ns,
            CachePolicy {
                initial_ttl,
                active_ttl,
                ..CachePolicy::default()
            },
        )
    })
    .collect()
}
#[derive(Clone, Debug, Default)]
pub struct CacheStats {
    pub budget: usize,
    pub limit: usize,
    pub bytes: usize,
    pub entries: usize,
    pub hits: u64,
    pub misses: u64,
    pub cold_reads: u64,
    /// Initial misses satisfied by another loader's admitted immutable value.
    pub coalesced_reads: u64,
    pub evicted_budget: u64,
    pub evicted_expired: u64,
    pub evicted_replaced: u64,
    pub evicted_retired: u64,
    pub residency: Vec<(String, usize, usize)>,
}
struct Resident {
    value: Arc<Value>,
    label: Arc<str>,
    policy: CachePolicy,
    initial: bool,
    deadline: u64,
    touch: u64,
    charge: usize,
}
struct Shard {
    entries: HashMap<u64, Resident>,
    // Updated with admission/removal under this shard's lock. Residents share
    // their label allocation with this map; zero-count rows are removed.
    residency: BTreeMap<Arc<str>, (usize, usize)>,
    lru: BTreeSet<(u64, u64)>,
    wheel: Vec<HashSet<u64>>,
    // A conservative lower bound per bucket. Removal may leave an earlier
    // bound until the next scan; it must never hide an earlier inserted lease.
    earliest: [u64; SLOTS],
    tick: u64,
    sequence: u64,
    bytes: usize,
    total_bytes: Arc<AtomicUsize>,
    evictions: [u64; 4],
    #[cfg(test)]
    expiry_visits: usize,
}
fn slot(deadline: u64) -> usize {
    (deadline / TICK % SLOTS as u64) as usize
}
impl Shard {
    fn link(&mut self, id: u64, deadline: u64) {
        let bucket = slot(deadline);
        self.wheel[bucket].insert(id);
        self.earliest[bucket] = self.earliest[bucket].min(deadline);
    }
    fn unlink(&mut self, id: u64, deadline: u64) {
        let bucket = slot(deadline);
        self.wheel[bucket].remove(&id);
        if self.wheel[bucket].is_empty() {
            self.earliest[bucket] = u64::MAX;
        }
    }
    fn remove(&mut self, id: u64, reason: usize) {
        if let Some(r) = self.entries.remove(&id) {
            self.bytes -= r.charge;
            let class = self.residency.get_mut(r.label.as_ref()).unwrap();
            class.0 -= r.charge;
            class.1 -= 1;
            if class.1 == 0 {
                self.residency.remove(r.label.as_ref());
            }
            self.total_bytes.fetch_sub(r.charge, Ordering::AcqRel);
            self.lru.remove(&(r.touch, id));
            self.unlink(id, r.deadline);
            self.evictions[reason] += 1;
        }
    }
    fn expire(&mut self, now: u64) {
        let current = now / TICK;
        let steps = current.saturating_sub(self.tick).min(SLOTS as u64);
        for step in 0..=steps {
            let bucket = ((self.tick + step) % SLOTS as u64) as usize;
            if self.earliest[bucket] > now {
                continue;
            }
            #[cfg(test)]
            { self.expiry_visits += self.wheel[bucket].len(); }
            // Long leases may circle the wheel; each link remains in its bucket.
            let mut earliest = u64::MAX;
            let due: Vec<_> = self.wheel[bucket]
                .iter()
                .copied()
                .filter(|id| self.entries.get(id).is_some_and(|r| {
                    if r.deadline <= now {
                        true
                    } else {
                        earliest = earliest.min(r.deadline);
                        false
                    }
                }))
                .collect();
            self.earliest[bucket] = earliest;
            for id in due {
                self.remove(id, 1);
            }
        }
        self.tick = current;
    }
    fn trim(&mut self, limit: usize) {
        while self.bytes > limit {
            let Some((_, id)) = self.lru.first().copied() else {
                break;
            };
            self.remove(id, 0);
        }
        // A formerly large cache must also relinquish its empty hash-table allocation.
        if self.entries.capacity() > self.entries.len().saturating_mul(4).max(16) {
            self.entries.shrink_to_fit();
        }
        for bucket in &mut self.wheel {
            if bucket.capacity() > bucket.len().saturating_mul(4).max(16) {
                bucket.shrink_to_fit();
            }
        }
    }
}
pub(crate) struct Cache {
    shards: Vec<Mutex<Shard>>,
    // Fixed stripes avoid a per-key coordination map that grows with traffic.
    // Hot hits never acquire a load gate, and no cache shard lock spans I/O.
    load_gates: [Mutex<()>; LOAD_GATES],
    clock: Clock,
    max: usize,
    // Only admissions, allowance changes and statistics take this lock. Hits
    // and invalidations retain their independent shard locking.
    admission: Mutex<()>,
    resident_bytes: Arc<AtomicUsize>,
    limit: AtomicUsize,
    hits: AtomicU64,
    misses: AtomicU64,
    cold_reads: AtomicU64,
    coalesced_reads: AtomicU64,
}
impl Cache {
    pub fn new(config: &Config, clock: Clock) -> Self {
        let resident_bytes = Arc::new(AtomicUsize::new(0));
        Self {
            shards: (0..SHARDS)
                .map(|_| {
                    Mutex::new(Shard {
                        entries: HashMap::new(),
                        residency: BTreeMap::new(),
                        lru: BTreeSet::new(),
                        wheel: (0..SLOTS).map(|_| HashSet::new()).collect(),
                        earliest: [u64::MAX; SLOTS],
                        tick: clock.mono_ms() / TICK,
                        sequence: 0,
                        bytes: 0,
                        total_bytes: resident_bytes.clone(),
                        evictions: [0; 4],
                        #[cfg(test)]
                        expiry_visits: 0,
                    })
                })
                .collect(),
            load_gates: std::array::from_fn(|_| Mutex::new(())),
            clock,
            max: config.cache_bytes,
            admission: Mutex::new(()),
            resident_bytes,
            limit: AtomicUsize::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            cold_reads: AtomicU64::new(0),
            coalesced_reads: AtomicU64::new(0),
        }
    }
    pub fn policy(config: &Config, ns: &str, key: &str, class: Class) -> (String, CachePolicy) {
        config
            .cache_rules
            .iter()
            .rev()
            .find(|r| {
                r.namespace.as_ref().is_none_or(|n| n == ns)
                    && key.starts_with(&r.prefix)
                    && r.class.is_none_or(|c| c == class)
            })
            .map_or_else(
                || (class.label(), config.cache_policy),
                |r| (r.label.clone(), r.policy),
            )
    }
    pub(super) fn value_charge(capacity: usize, label: &str) -> usize {
        capacity + label.len() + 384
    }
    pub fn get(&self, id: u64) -> Option<Arc<Value>> {
        self.lookup(id, false)
    }
    pub fn load_guard(&self, id: u64) -> MutexGuard<'_, ()> {
        self.load_gates[id as usize % LOAD_GATES].lock().unwrap()
    }
    /// A recheck preserves one initial hit/miss observation per read attempt.
    pub fn after_miss(&self, id: u64) -> Option<Arc<Value>> {
        self.lookup(id, true)
    }
    fn lookup(&self, id: u64, after_miss: bool) -> Option<Arc<Value>> {
        let now = self.clock.mono_ms();
        let mut s = self.shards[id as usize % SHARDS].lock().unwrap();
        s.expire(now);
        if s.entries.get(&id).is_some_and(|r| r.deadline <= now) {
            s.remove(id, 1);
        }
        if let Some(mut r) = s.entries.remove(&id) {
            s.lru.remove(&(r.touch, id));
            if r.initial && r.policy.refresh_on_read && !r.policy.never_cache {
                s.unlink(id, r.deadline);
                r.deadline = now.saturating_add(r.policy.active_ttl);
                r.initial = false;
                s.link(id, r.deadline);
            }
            s.sequence += 1;
            r.touch = s.sequence;
            s.lru.insert((r.touch, id));
            let value = r.value.clone();
            s.entries.insert(id, r);
            if after_miss {
                self.coalesced_reads.fetch_add(1, Ordering::Relaxed);
            } else {
                self.hits.fetch_add(1, Ordering::Relaxed);
            }
            Some(value)
        } else {
            if !after_miss {
                self.misses.fetch_add(1, Ordering::Relaxed);
            }
            None
        }
    }
    pub fn cold(&self) {
        self.cold_reads.fetch_add(1, Ordering::Relaxed);
    }
    pub fn insert(
        &self,
        value: Arc<Value>,
        label: String,
        policy: CachePolicy,
        initial: bool,
        limit: usize,
        eligible: &AtomicBool,
    ) {
        let ttl = if initial {
            policy.initial_ttl
        } else {
            policy.active_ttl
        };
        if ttl == 0 || (!initial && policy.never_cache) || value.tombstone {
            return;
        }
        let charge = Self::value_charge(value.bytes.capacity(), &label);
        // These fixed ceilings cannot be raised by concurrent maintenance.
        // A value that cannot fit need not wait for unrelated admission work.
        let limit = limit.min(self.max);
        if charge > limit {
            return;
        }
        let _admission = self.admission.lock().unwrap();
        let limit = limit.min(self.limit.load(Ordering::Acquire));
        if charge > limit {
            return;
        }
        let now = self.clock.mono_ms();
        let id = value.version;
        let shard = id as usize % SHARDS;
        {
            let mut s = self.shards[shard].lock().unwrap();
            s.expire(now);
            if !eligible.load(Ordering::Acquire) || s.entries.contains_key(&id) {
                return;
            }
        }
        let available = limit - charge;
        if self.resident_bytes.load(Ordering::Acquire) > available {
            // Expired capacity in another shard takes precedence over live LRU
            // eviction. No shard lock is held while another shard is acquired.
            self.sweep();
            self.evict_to(available, shard);
        }
        let mut s = self.shards[shard].lock().unwrap();
        // Retirement/replacement can run during pressure reclamation. Recheck
        // under the same shard lock used by invalidation before publishing.
        if !eligible.load(Ordering::Acquire) {
            return;
        }
        let label = s.residency.get_key_value(label.as_str())
            .map(|(existing, _)| existing.clone())
            .unwrap_or_else(|| Arc::<str>::from(label.as_str()));
        let class = s.residency.entry(label.clone()).or_default();
        class.0 += charge;
        class.1 += 1;
        s.sequence += 1;
        let r = Resident {
            value,
            label,
            policy,
            initial,
            deadline: now.saturating_add(ttl),
            touch: s.sequence,
            charge,
        };
        s.bytes += charge;
        self.resident_bytes.fetch_add(charge, Ordering::AcqRel);
        s.link(id, r.deadline);
        s.lru.insert((r.touch, id));
        s.entries.insert(id, r);
    }
    pub fn remove(&self, id: u64, retired: bool) {
        self.shards[id as usize % SHARDS]
            .lock()
            .unwrap()
            .remove(id, if retired { 3 } else { 2 });
    }
    pub fn trim(&self, limit: usize) {
        let _admission = self.admission.lock().unwrap();
        let limit = limit.min(self.max);
        self.limit.store(limit, Ordering::Release);
        for shard in &self.shards {
            let mut s = shard.lock().unwrap();
            s.expire(self.clock.mono_ms());
            let bytes = s.bytes;
            s.trim(bytes);
        }
        self.evict_to(limit, 0);
    }
    /// Called with admission held; independent removals can only reduce usage.
    fn evict_to(&self, limit: usize, first: usize) {
        for offset in 0..SHARDS {
            if self.resident_bytes.load(Ordering::Acquire) <= limit {
                break;
            }
            let mut s = self.shards[(first + offset) % SHARDS].lock().unwrap();
            let excess = self
                .resident_bytes
                .load(Ordering::Acquire)
                .saturating_sub(limit);
            let retained = s.bytes.saturating_sub(excess);
            s.trim(retained);
        }
        debug_assert!(self.resident_bytes.load(Ordering::Acquire) <= limit);
    }
    pub fn sweep(&self) {
        for shard in &self.shards {
            shard.lock().unwrap().expire(self.clock.mono_ms());
        }
    }
    pub fn stats(&self) -> CacheStats {
        // Keep allowance and summed shard charges coherent across admissions.
        let _admission = self.admission.lock().unwrap();
        let mut stats = CacheStats {
            budget: self.max,
            limit: self.limit.load(Ordering::Acquire),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            cold_reads: self.cold_reads.load(Ordering::Relaxed),
            coalesced_reads: self.coalesced_reads.load(Ordering::Relaxed),
            ..CacheStats::default()
        };
        let mut classes = std::collections::BTreeMap::<String, (usize, usize)>::new();
        for shard in &self.shards {
            let mut s = shard.lock().unwrap();
            s.expire(self.clock.mono_ms());
            stats.bytes += s.bytes;
            stats.entries += s.entries.len();
            stats.evicted_budget += s.evictions[0];
            stats.evicted_expired += s.evictions[1];
            stats.evicted_replaced += s.evictions[2];
            stats.evicted_retired += s.evictions[3];
            for (label, (bytes, count)) in &s.residency {
                let c = classes.entry(label.as_ref().to_owned()).or_default();
                c.0 += bytes;
                c.1 += count;
            }
        }
        stats.residency = classes.into_iter().map(|(k, (b, n))| (k, b, n)).collect();
        stats
    }
}

#[cfg(test)]
mod expiry_bucket_tests;
#[cfg(test)]
mod residency_tests;
#[cfg(test)]
mod admission_tests;
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lru_evicts_oldest_in_its_shard_and_keeps_one_wheel_link_per_entry() {
        let c = Config {
            cache_bytes: 3 * (1024 + 384 + 1),
            ..Config::default()
        };
        let clock = crate::ManualClock::new(crate::now_ms());
        let cache = Cache::new(&c, Clock::new(Some(clock.clone())));
        cache.trim(c.cache_bytes);
        let add = |version| {
            cache.insert(
                Arc::new(Value {
                    version,
                    ack: 1,
                    tombstone: false,
                    bytes: vec![0; 1024],
                }),
                "c".into(),
                CachePolicy {
                    initial_ttl: 1000,
                    active_ttl: 2000,
                    ..CachePolicy::default()
                },
                true,
                c.cache_bytes,
                &AtomicBool::new(true),
            )
        };
        for id in [16, 32, 48] {
            add(id);
        }
        assert!(cache.get(16).is_some());
        add(64);
        assert!(cache.get(32).is_none());
        for _ in 0..1000 {
            assert!(cache.get(16).is_some());
        }
        let s = cache.shards[0].lock().unwrap();
        assert_eq!(
            s.wheel.iter().map(HashSet::len).sum::<usize>(),
            s.entries.len()
        );
        assert_eq!(s.lru.len(), s.entries.len());
        drop(s);
        clock.advance(2000);
        cache.sweep();
        assert_eq!(cache.stats().entries, 0);
    }
    #[test]
    fn resident_policy_text_preserves_all_selectors_and_rejects_invalid_values() {
        let r: CacheRule = "logs,event/,expire:86400000,rapid,7,19,false,true"
            .parse()
            .unwrap();
        assert_eq!(r.namespace.as_deref(), Some("logs"));
        assert_eq!(r.prefix, "event/");
        assert_eq!(r.class, Some(Class::expire(86400000)));
        assert_eq!(r.label, "rapid");
        assert_eq!(
            r.policy,
            CachePolicy {
                initial_ttl: 7,
                active_ttl: 19,
                refresh_on_read: false,
                never_cache: true
            }
        );
        for bad in [
            "logs,*,bad,c,1,2,true,false",
            "logs,,*,c,x,2,true,false",
            "logs,,*,c,1,2,once,false",
            "short",
        ] {
            assert!(bad.parse::<CacheRule>().is_err());
        }
    }

    #[test]
    fn shared_allowance_shrink_and_parallel_admission_release_every_charge() {
        let c = Config {
            cache_bytes: 64 * 1024,
            ..Config::default()
        };
        let cache = Cache::new(
            &c,
            Clock::new(Some(crate::ManualClock::new(crate::now_ms()))),
        );
        cache.trim(c.cache_bytes);
        let add = |version| {
            cache.insert(
                Arc::new(Value {
                    version,
                    ack: 1,
                    tombstone: false,
                    bytes: vec![7; 8192],
                }),
                "x".into(),
                CachePolicy::default(),
                true,
                c.cache_bytes,
                &AtomicBool::new(true),
            )
        };
        std::thread::scope(|scope| {
            for thread in 0..4 {
                let (cache, add) = (&cache, &add);
                scope.spawn(move || {
                    for n in 0..64 {
                        add(1 + thread * 64 + n);
                        let stats = cache.stats();
                        assert!(stats.bytes <= stats.limit);
                    }
                });
            }
            for _ in 0..16 {
                cache.trim(12 * 1024);
                assert!(cache.stats().bytes <= 12 * 1024);
                cache.trim(0);
                assert_eq!(cache.stats().bytes, 0);
                cache.trim(c.cache_bytes);
            }
        });
        cache.trim(0);
        assert_eq!(cache.resident_bytes.load(Ordering::Acquire), 0);
        assert_eq!(cache.stats().entries, 0);
        cache.trim(c.cache_bytes);
        add(1000);
        assert_eq!(cache.stats().entries, 1);
        cache.remove(1000, true);
        assert_eq!(cache.resident_bytes.load(Ordering::Acquire), 0);
    }

    #[test]
    fn shared_admission_does_not_block_a_hot_hit() {
        let c = Config {
            cache_bytes: 64 * 1024,
            ..Config::default()
        };
        let cache = Cache::new(
            &c,
            Clock::new(Some(crate::ManualClock::new(crate::now_ms()))),
        );
        cache.trim(c.cache_bytes);
        cache.insert(
            Arc::new(Value {
                version: 1,
                ack: 1,
                tombstone: false,
                bytes: vec![7; 8192],
            }),
            "x".into(),
            CachePolicy::default(),
            true,
            c.cache_bytes,
            &AtomicBool::new(true),
        );
        let admission = cache.admission.lock().unwrap();
        std::thread::scope(|scope| {
            let cache = &cache;
            let (sent, received) = std::sync::mpsc::channel();
            scope.spawn(move || {
                sent.send(cache.get(1).unwrap().bytes.len()).unwrap();
            });
            let result = received.recv_timeout(std::time::Duration::from_secs(5));
            drop(admission);
            assert_eq!(result.unwrap(), 8192);
        });
    }
}
