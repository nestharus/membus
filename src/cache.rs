//! Sharded weighted LRU. The wheel owns one expiry link per resident version.
use crate::{chunks::Clock, Class, Config, Value};
use std::{
    collections::{BTreeSet, HashMap, HashSet},
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
const SHARDS: usize = 16;
const SLOTS: usize = 256;
const TICK: u64 = 10;

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
    pub evicted_budget: u64,
    pub evicted_expired: u64,
    pub evicted_replaced: u64,
    pub evicted_retired: u64,
    pub residency: Vec<(String, usize, usize)>,
}
struct Resident {
    value: Arc<Value>,
    label: String,
    policy: CachePolicy,
    initial: bool,
    deadline: u64,
    touch: u64,
    charge: usize,
}
struct Shard {
    entries: HashMap<u64, Resident>,
    lru: BTreeSet<(u64, u64)>,
    wheel: Vec<HashSet<u64>>,
    tick: u64,
    sequence: u64,
    bytes: usize,
    evictions: [u64; 4],
}
fn slot(deadline: u64) -> usize {
    (deadline / TICK % SLOTS as u64) as usize
}
impl Shard {
    fn remove(&mut self, id: u64, reason: usize) {
        if let Some(r) = self.entries.remove(&id) {
            self.bytes -= r.charge;
            self.lru.remove(&(r.touch, id));
            self.wheel[slot(r.deadline)].remove(&id);
            self.evictions[reason] += 1;
        }
    }
    fn expire(&mut self, now: u64) {
        let current = now / TICK;
        let steps = current.saturating_sub(self.tick).min(SLOTS as u64);
        for step in 0..=steps {
            let bucket = ((self.tick + step) % SLOTS as u64) as usize;
            // Long leases may circle the wheel; each link remains in its bucket.
            let due: Vec<_> = self.wheel[bucket]
                .iter()
                .copied()
                .filter(|id| self.entries.get(id).is_some_and(|r| r.deadline <= now))
                .collect();
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
    clock: Clock,
    max: usize,
    limit: AtomicUsize,
    hits: AtomicU64,
    misses: AtomicU64,
    cold_reads: AtomicU64,
}
impl Cache {
    pub fn new(config: &Config, clock: Clock) -> Self {
        Self {
            shards: (0..SHARDS)
                .map(|_| {
                    Mutex::new(Shard {
                        entries: HashMap::new(),
                        lru: BTreeSet::new(),
                        wheel: (0..SLOTS).map(|_| HashSet::new()).collect(),
                        tick: clock.mono_ms() / TICK,
                        sequence: 0,
                        bytes: 0,
                        evictions: [0; 4],
                    })
                })
                .collect(),
            clock,
            max: config.cache_bytes,
            limit: AtomicUsize::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            cold_reads: AtomicU64::new(0),
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
    pub fn get(&self, id: u64) -> Option<Arc<Value>> {
        let now = self.clock.mono_ms();
        let mut s = self.shards[id as usize % SHARDS].lock().unwrap();
        s.expire(now);
        if s.entries.get(&id).is_some_and(|r| r.deadline <= now) {
            s.remove(id, 1);
        }
        if let Some(mut r) = s.entries.remove(&id) {
            s.lru.remove(&(r.touch, id));
            if r.initial && r.policy.refresh_on_read && !r.policy.never_cache {
                s.wheel[slot(r.deadline)].remove(&id);
                r.deadline = now.saturating_add(r.policy.active_ttl);
                r.initial = false;
                s.wheel[slot(r.deadline)].insert(id);
            }
            s.sequence += 1;
            r.touch = s.sequence;
            s.lru.insert((r.touch, id));
            let value = r.value.clone();
            s.entries.insert(id, r);
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(value)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
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
        let now = self.clock.mono_ms();
        let id = value.version;
        let mut s = self.shards[id as usize % SHARDS].lock().unwrap();
        s.expire(now);
        // Retirement/replacement clears eligibility before removing under this same shard lock.
        if !eligible.load(Ordering::Acquire) {
            return;
        }
        // A racing cold reader must not reset an existing lease.
        if s.entries.contains_key(&id) {
            return;
        }
        let charge = value.bytes.capacity() + label.len() + 384;
        let limit = limit.min(self.limit.load(Ordering::Acquire)).min(self.max) / SHARDS;
        if charge > limit {
            return;
        }
        s.trim(limit - charge);
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
        s.wheel[slot(r.deadline)].insert(id);
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
        self.limit.store(limit.min(self.max), Ordering::Release);
        for shard in &self.shards {
            let mut s = shard.lock().unwrap();
            s.expire(self.clock.mono_ms());
            s.trim(limit.min(self.max) / SHARDS);
        }
    }
    pub fn sweep(&self) {
        for shard in &self.shards {
            shard.lock().unwrap().expire(self.clock.mono_ms());
        }
    }
    pub fn stats(&self) -> CacheStats {
        let mut stats = CacheStats {
            budget: self.max,
            limit: self.limit.load(Ordering::Acquire),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            cold_reads: self.cold_reads.load(Ordering::Relaxed),
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
            for r in s.entries.values() {
                let c = classes.entry(r.label.clone()).or_default();
                c.0 += r.charge;
                c.1 += 1;
            }
        }
        stats.residency = classes.into_iter().map(|(k, (b, n))| (k, b, n)).collect();
        stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn lru_evicts_oldest_in_its_shard_and_keeps_one_wheel_link_per_entry() {
        let c = Config {
            cache_bytes: 16 * 3 * (1024 + 384 + 1),
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
}
