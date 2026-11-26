//! Store admission, policy publication, committed reads and mutation completion.
use crate::append_owner::Append;
use crate::chunks;
use crate::codec::crc32;
use crate::codec::mutation_bytes;
use crate::codec::MAX_ID;
use crate::codec::MAX_VALUE;
use crate::routing::stripe;
use crate::routing::ClassReservation;
use crate::timers;
use crate::CatalogueMaintenance;
use crate::Class;
use crate::Health2;
use crate::Mutation;
use crate::Outcome;
use crate::Store;
use crate::Value;
use crate::POLICY_NS;
use std::collections::HashMap;
use std::collections::BTreeMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::io;
use std::path::Path;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::time::{Duration, Instant};

pub(crate) use super::borrowed_keys::get as committed_get;

#[derive(Clone)]
pub(crate) struct Dedup {
    pub(crate) fingerprint: [u8; 32],
    pub(crate) at: u64,
    /// Local retry cutoff. Recovery derives remaining time from the recorded UTC age once.
    pub(crate) until_mono: u64,
    /// Source chunk during replay; zero for local admission or legacy-only outcomes.
    pub(crate) replayed_chunk: u64,
    pub(crate) outcome: Option<Outcome>,
}
#[cfg_attr(test, derive(Default))]
pub(crate) struct State {
    pub(crate) subscription_usage: Arc<crate::subscription_resources::SubscriptionUsage>,
    pub(crate) pending: HashMap<(String, String), Meta>,
    pub(crate) metadata: HashMap<(String, String), Meta>,
    pub(crate) namespace_counts: HashMap<String, usize>,
    pub(crate) keys: usize,
    /// Resident latest-key index charges; payloads belong to the independently bounded cache.
    pub(crate) committed_bytes: usize,
    pub(crate) dedup_bytes: usize,
    pub(crate) dedup: HashMap<(String, String), Dedup>,
    /// POLICY_SET still owes its routing projection after the durable outcome is known.
    pub(crate) policy_publishing: HashSet<Key>,
    pub(crate) retired: VecDeque<(String, String)>,
    pub(crate) retired_set: HashSet<(String, String)>,
    pub(crate) expiry: VecDeque<((String, String), u64)>,
    pub(crate) next: u64,
    pub(crate) reserved_bytes: usize,
    pub(crate) memory_reserved: usize,
    pub(crate) stripe_reserved: [usize; 4],
    pub(crate) stripe_items: [usize; 4],
    pub(crate) persisted_bytes: u64,
    pub(crate) pending_disk: u64,
    pub(crate) namespace_reserves: crate::namespace_reserve::Book,
    pub(crate) read_only: bool,
    pub(crate) warming: bool,
    /// Tombstones whose index entry may drop once no other retained chunk holds their key (5.3).
    pub(crate) tomb_candidates: HashSet<(String, String)>,
}
impl State {
    pub(crate) fn total_reserved(&self)->usize {self.memory_reserved.saturating_add(self.subscription_usage.bytes.load(Ordering::Acquire))}
    /// Current non-cache ownership, including actually granted subscription storage.
    /// The Store captures external owners under its existing admission guard.
    pub(crate) fn available_memory(&self, capacity: usize, snapshots: usize,
        margin: usize, chunk_index: usize, read_cohorts: usize) -> usize {
        [self.committed_bytes, self.dedup_bytes, self.total_reserved(),
            snapshots, margin, chunk_index, read_cohorts].into_iter()
            .try_fold(0usize, |n, v| n.checked_add(v))
            .map_or(0, |owned| capacity.saturating_sub(owned))
    }
    pub(crate) fn release_namespace(&mut self, ns: &str) {
        if let Some(count) = self.namespace_counts.get_mut(ns) {
            *count -= 1;
            if *count == 0 {
                self.namespace_counts.remove(ns);
            }
        }
    }
}
#[derive(Clone)]
pub(crate) struct Meta {
    pub(crate) version: u64,
    pub(crate) tombstone: bool,
    pub(crate) bytes: usize,
    /// The chunk holding this key's latest persisted record (0: the legacy logs, or none).
    pub(crate) chunk: u64,
}
/// Release before notifying under state coordination, including early refusals.
pub(crate) struct PolicyUpdate<'a> {
    pub(crate) guard: Option<std::sync::MutexGuard<'a, ()>>,
    pub(crate) store: &'a Store,
}
impl Drop for PolicyUpdate<'_> {
    fn drop(&mut self) {
        self.guard.take();
        let _state = self.store.state.lock().unwrap();
        self.store.done.notify_all();
    }
}
/// Flags health reads beside read_only.
#[derive(Default)]
pub(crate) struct Flags {
    pub(crate) storage_error: AtomicBool,
    pub(crate) expiry_degraded: AtomicBool,
}
pub(crate) type Key = (String, String);
// One latest-key index, ordered so capture can seek directly to a namespace.
pub(crate) type Committed = BTreeMap<Key, Arc<crate::value_disk::Entry>>;
pub(crate) fn index_charge(k: &Key) -> usize { 512 + 4 * (k.0.len() + k.1.len()) }
pub(crate) fn dedup_charge(k: &Key) -> usize { 264 + 3 * (k.0.len() + k.1.len()) }
pub(crate) struct ColdReader<'a>(&'a (std::sync::Mutex<usize>, std::sync::Condvar));
impl Drop for ColdReader<'_> {
    fn drop(&mut self) { *self.0.0.lock().unwrap() -= 1; self.0.1.notify_one(); }
}
pub(crate) fn fingerprint(m: &Mutation) -> [u8; 32] {
    // The durable representation remains complete. Only the in-memory retry
    // comparator is compact; replay derives the same digest from retained records.
    let mut hash = chunks::Sha256::new();
    hash.update(&mutation_bytes(m, 0, 0));
    hash.finish()
}
pub(crate) fn dedup_identity(m: &Mutation) -> Key {
    // The existing policy target key carries its caller namespace; retain the literal operation ID.
    let ns = if m.ns == POLICY_NS {
        m.key.split_once('\0').map_or(m.ns.as_str(), |(ns, _)| ns)
    } else {
        m.ns.as_str()
    };
    (ns.to_owned(), m.op_id.clone())
}
impl Store {
    pub(crate) fn cold_reader(&self) -> ColdReader<'_> {
        let mut readers = self.cold_readers.0.lock().unwrap();
        while *readers >= 2 { readers = self.cold_readers.1.wait(readers).unwrap(); }
        *readers += 1;
        ColdReader(&self.cold_readers)
    }
    /// The instance clock, shared by recovery and server runtime owners.
    pub fn clock(&self) -> &chunks::Clock { &self.clock }
    pub fn get(&self, ns: &str, key: &str) -> Option<Value> {
        self.get_handle(ns, key).map(|v| (*v).clone())
    }
    /// Fallible committed read; storage errors stay distinct from absent keys.
    pub fn try_get(&self, ns: &str, key: &str) -> io::Result<Option<Value>> {
        Ok(self.get_handle_result(ns,key)?.map(|v| (*v).clone()))
    }
    pub(crate) fn get_handle(&self, ns: &str, key: &str) -> Option<Arc<Value>> {
        self.get_handle_result(ns,key).ok().flatten()
    }
    pub(crate) fn get_handle_result(&self, ns: &str, key: &str) -> io::Result<Option<Arc<Value>>> {
        let entry = committed_get(&self.committed.read().unwrap(), ns, key)
            .filter(|v| !v.tombstone)
            .cloned();
        entry.map(|entry| self.read_entry(ns, key, &entry)).transpose()
    }
    pub(crate) fn read_entry(&self, ns: &str, key: &str, entry: &crate::value_disk::Entry) -> io::Result<Arc<Value>> {
        if let Some(v) = self.cache.get(entry.version) { return Ok(v); }
        let (label, policy) = crate::cache::Cache::policy(&self.config, ns,key,entry.class);
        // Coordinate only cache-eligible cold loads. Recheck after the preceding
        // loader has published, before taking either of the global decode slots.
        // The gate retains no payload or error and is independent of key writers.
        // Indexed length is a lower bound on payload capacity. If even that
        // charge exceeds the configured maximum, no loader can admit it.
        let can_fit = crate::cache::Cache::value_charge(entry.len, &label) <= self.config.cache_bytes;
        let _load = (can_fit && self.config.cache_bytes != 0 && !policy.never_cache && policy.active_ttl != 0
            && entry.cacheable.load(Ordering::Acquire)).then(|| self.cache.load_guard(entry.version));
        if _load.is_some() {
            if let Some(v) = self.cache.after_miss(entry.version) { return Ok(v); }
        }
        let _reader = self.cold_reader();
        self.cache.cold();
        let value = self.read_entry_uncached(ns, key, entry)?;
        if self.scan_usage.released.swap(false, Ordering::AcqRel)
            | self.read_usage.released.swap(false, Ordering::AcqRel)
            | self.subscription_released.swap(false, Ordering::AcqRel) {
            // Recompute from current charges, rather than adding released bytes
            // to a stale limit. New captures and writes serialize on state.
            // Hot hits never take this path or the state lock.
            self.trim_cache(&self.state.lock().unwrap());
        }
        self.cache.insert(value.clone(), label, policy, false, self.config.cache_bytes, &entry.cacheable);
        Ok(value)
    }
    pub(crate) fn read_entry_uncached(
        &self,
        ns: &str,
        key: &str,
        entry: &crate::value_disk::Entry,
    ) -> io::Result<Arc<Value>> {
        entry.read(ns, key).inspect_err(|_| {
            self.flags.storage_error.store(true, Ordering::SeqCst);
        })
    }
    pub fn cache_stats(&self) -> crate::CacheStats { self.cache.stats() }
    pub(crate) fn signal_budget<'a>(&'a self, state: &'a State) -> crate::subscription_resources::Budget<'a> {
        crate::subscription_resources::Budget::new(
            &self.state, state, self.config.capacity,
            self.scan_usage.bytes.load(Ordering::Acquire), self.config.memory_margin(),
            self.chunks.lock().unwrap().managed_index_bytes(), self.read_cohort_bytes(),
            Some(&self.cache), &self.subscription_released,
        )
    }
    pub(crate) fn cache_limit(&self, state: &State) -> usize {
        self.signal_budget(state).available()
    }
    pub(crate) fn trim_cache(&self, state: &State) {
        self.cache.trim(self.cache_limit(state));
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
            .map(|c| {
                (
                    c.id,
                    c.stripe,
                    c.class.label(),
                    format!("{:?}", c.state),
                    c.bytes,
                )
            })
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
    /// `retired`), overriding the class's clock checks. Chunks with a latest record, pending key,
    /// or a retained retry guard (including recovered ordinary and live or recovered atomic outcomes) stay.
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
        self.chunks
            .lock()
            .unwrap()
            .table
            .values()
            .filter(|c| c.blocked_since.is_some())
            .count()
    }
    pub fn retry_retiring(&self) -> io::Result<usize> {
        self.maint.retry_retiring()
    }
    /// Recover a fenced catalogue from this owner's retained chunk state, or compact when due.
    /// This is unavailable after refused startup; it accepts no caller-supplied cold reconstruction.
    pub fn maintain_catalogue(&self) -> io::Result<CatalogueMaintenance> {
        self.maint.maintain_catalogue()
    }
    /// Latest retained resume evidence, including historical basis-free records.
    pub fn expiry_resume_record(&self) -> Option<chunks::CatRecord> {
        self.chunks.lock().unwrap().last_resume.clone()
    }
    /// (index entries with summaries, live keys in `ns`).
    pub fn slots(&self, ns: &str) -> (usize, usize) {
        let state = self.state.lock().unwrap();
        (
            state.keys,
            state.namespace_counts.get(ns).copied().unwrap_or(0),
        )
    }
    /// HEALTH2's reading. Lock order: a stripe's sink before the chunk table (the write path), and the state before
    /// the chunk table (retirement); so the open chunks are counted before either lock is taken here.
    pub fn health2(&self) -> Health2 {
        let chunks_open = self.router.open_chunks();
        let state = self.state.lock().unwrap();
        let cs = self.chunks.lock().unwrap();
        let utc = self.maint.clock.utc_ms();
        let mono = self.maint.clock.mono_ms();
        let c = &self.config;
        let mut h = Health2 {
            cache: self.cache.stats(),
            memory_budget: c.capacity,
            state_bytes: state.persisted_bytes,
            state_limit_bytes: c.state_limit_bytes,
            state_high_water: state.persisted_bytes
                >= c.state_limit_bytes - c.state_limit_bytes / 4,
            read_only: state.read_only,
            storage_error: self.flags.storage_error.load(Ordering::SeqCst),
            catalogue_rewrite_error: cs.rewrite_error.clone(),
            expiry_degraded: self.flags.expiry_degraded.load(Ordering::SeqCst),
            dedup_entries: state.dedup.len(),
            dedup_cap: c.dedup_cap,
            dedup_high_water: (state.dedup.len() as u128) * 100 >= (c.dedup_cap as u128) * 70,
            keys: state.keys,
            index_cap: c.index_cap,
            quota: c.quota,
            chunks_open,
            retired_chunks: cs.retired_chunks,
            retired_bytes: cs.retired_bytes,
            ..Health2::default()
        };
        h.memory_bytes = h.cache.bytes + state.committed_bytes + state.dedup_bytes + state.total_reserved()
            + self.scan_usage.bytes.load(Ordering::Acquire) + self.read_cohort_bytes()
            + c.memory_margin() + cs.managed_index_bytes();
        let mut namespaces: Vec<(String, usize)> = state
            .namespace_counts
            .iter()
            .filter(|(_, n)| **n > 0)
            .map(|(k, n)| (k.clone(), *n))
            .collect();
        namespaces.sort();
        h.namespace_high_water = namespaces
            .iter()
            .any(|(_, n)| (*n as u128) * 100 >= (c.quota as u128) * 70);
        h.namespaces = namespaces;
        let mut by_class: std::collections::BTreeMap<String, u64> =
            std::collections::BTreeMap::new();
        let mut oldest_blocked = None::<u64>;
        for chunk in cs.table.values() {
            if chunk.state == chunks::ChunkState::Retired {
                continue;
            }
            let label = if chunk.id == chunks::LEGACY_CHUNK {
                "legacy".to_owned()
            } else {
                chunk.class.label()
            };
            *by_class.entry(label).or_insert(0) += chunk.bytes;
            h.retained_total += chunk.bytes;
            if chunk.state == chunks::ChunkState::Closed && chunk.id != chunks::LEGACY_CHUNK {
                h.chunks_closed += 1;
                let due = chunk
                    .close_ms
                    .and_then(|close| utc.checked_sub(close))
                    .is_some_and(|age| age > c.retry_ms)
                    && chunk.close_mono.is_none_or(|close| mono.checked_sub(close)
                        .is_some_and(|age| age > c.retry_ms))
                    && chunk.recovered_retry_until_mono.is_none_or(|until| mono > until);
                if chunk.class.kind == 1
                    && due
                    && chunk
                        .keys
                        .keys()
                        .any(|k| state.metadata.get(k).is_some_and(|m| m.chunk == chunk.id))
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
        h.retire_blocked_oldest_ms = oldest_blocked.map_or(0, |since| mono.saturating_sub(since));
        h.retire_blocked_high_water = h.retire_blocked_oldest_ms > 3_600_000;
        h
    }
    /// GET_STATUS (5.3): 0 live with its version, 1 expired (in a summary), 2 missing.
    pub fn get_status(&self, ns: &str, key: &str) -> (u8, u64) {
        if let Some(v) = committed_get(&self.committed.read().unwrap(), ns, key).filter(|v|!v.tombstone) {
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
    pub fn set_policy(
        &self,
        ns: &str,
        prefix: &str,
        class: Option<Class>,
        deadline: u64,
    ) -> Outcome {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let id = format!(
            "policy/{}/{}/{}",
            self.epoch,
            self.clock.utc_ms(),
            SEQ.fetch_add(1, Ordering::SeqCst)
        );
        self.set_policy_with_id(ns, prefix, class, &id, deadline)
    }
    /// Caller-scoped POLICY_SET retry identity. A duplicate never reapplies an older projection.
    /// Built-in classes are canonicalized; unsupported kinds and zero Expire refuse before mutation.
    pub fn set_policy_with_id(
        &self,
        ns: &str,
        prefix: &str,
        class: Option<Class>,
        op_id: &str,
        deadline: u64,
    ) -> Outcome {
        let _publication = self.admission_gate.read().unwrap();
        let key = match chunks::policy_key(ns, prefix) {
            Ok(k) => k,
            Err(code) => return Outcome::error(code),
        };
        let class = match class {
            Some(c) => match Class::from_wire(c.kind, c.retention_ms) {
                Ok(canonical) => Some(canonical),
                Err(code) => return Outcome::error(code),
            },
            None => None,
        };
        let m = Mutation {
            ns: POLICY_NS.into(),
            key: key.clone(),
            op_id: op_id.into(),
            deadline,
            ack: 2,
            expected: None,
            bytes: class.map(chunks::policy_value).unwrap_or_default(),
            remove: class.is_none(),
        };
        let id = dedup_identity(&m);
        let fp = fingerprint(&m);
        // A duplicate joins the actual completion boundary, including projection, without
        // waiting for the administrative mutex. A missed identity is rechecked on every wake.
        let _update = loop {
            let mut state = self.state.lock().unwrap();
            if let Some(d) = state.dedup.get(&id) {
                if d.fingerprint != fp {
                    return Outcome::error(14);
                }
                if self.clock.mono_ms() > d.until_mono {
                    return Outcome::error(13);
                }
                if !state.policy_publishing.contains(&id) {
                    if let Some(outcome) = &d.outcome {
                        return outcome.clone();
                    }
                }
            } else {
                if state.retired_set.contains(&id) {
                    return Outcome::error(13);
                }
                match self.policy_updates.try_lock() {
                    Ok(guard) => {
                        drop(state);
                        break PolicyUpdate {
                            guard: Some(guard),
                            store: self,
                        };
                    }
                    Err(std::sync::TryLockError::WouldBlock) => {}
                    Err(std::sync::TryLockError::Poisoned(_)) => return Outcome::error(10),
                }
            }
            let wait = deadline.saturating_sub(self.clock.utc_ms());
            if wait == 0 {
                return Outcome::error(if state.dedup.contains_key(&id) { 17 } else { 5 });
            }
            #[cfg(test)]
            if let Some(entered) = self.policy_join_observed.lock().unwrap().take() {
                let _ = entered.send(());
            }
            state = self
                .done
                .wait_timeout(state, Duration::from_millis(wait))
                .unwrap()
                .0;
            drop(state);
        };
        let retry = {
            let state = self.state.lock().unwrap();
            let id = dedup_identity(&m);
            state.dedup.contains_key(&id) || state.retired_set.contains(&id)
        };
        if !retry && self.clock.utc_ms() >= deadline {
            return Outcome::error(5);
        }
        let _prospective = if !retry {
            if let Some(c) = class {
                // An expire class shorter than the retry window would retire records whose dedup a restart still needs.
                if c.kind == 2 && c.retention_ms < self.config.retry_ms {
                    return Outcome::error(3);
                }
                let mut counts = self.class_reservations.lock().unwrap();
                let current = self.policies.read().unwrap();
                let mut proposed = current.clone();
                proposed.set(ns, prefix, class);
                let mut classes = proposed.classes();
                // Old policies remain routable throughout the admitted policy's durable wait.
                classes.extend(current.classes());
                classes.extend(counts.keys().copied());
                classes.extend(
                    self.chunks
                        .lock()
                        .unwrap()
                        .table
                        .values()
                        .filter(|chunk| chunk.state != chunks::ChunkState::Retired)
                        .map(|chunk| chunk.class),
                );
                if classes.len() > chunks::MAX_CLASSES {
                    return Outcome::error(7);
                }
                Some(ClassReservation::add(
                    &self.class_reservations,
                    &mut counts,
                    c,
                ))
            } else {
                None
            }
        } else {
            None
        };
        self.state
            .lock()
            .unwrap()
            .policy_publishing
            .insert(id.clone());
        let outcome = self.mutate_mode_ungated(m, true, None);
        if outcome.code == 0 {
            #[cfg(test)]
            {
                let hook = self.policy_after_commit.lock().unwrap().take();
                if let Some((reached, resume)) = hook {
                    let _ = reached.send(());
                    let _ = resume.recv();
                }
            }
            let latest = self
                .state
                .lock()
                .unwrap()
                .metadata
                .get(&(POLICY_NS.into(), key))
                .is_some_and(|latest| latest.version == outcome.version);
            if latest {
                let _coordination = self.class_reservations.lock().unwrap();
                self.policies.write().unwrap().set(ns, prefix, class);
            }
        }
        self.state.lock().unwrap().policy_publishing.remove(&id);
        self.done.notify_all();
        outcome
    }
    /// Policy rows as (namespace, prefix, class).
    pub fn policies(&self) -> Vec<(String, String, Class)> {
        self.policies.read().unwrap().rows()
    }
    /// The idle tick's work, run now (tests use it with a manual clock).
    pub fn tick(&self) {
        self.trim_cache(&self.state.lock().unwrap());
        self.router.idle_close();
        self.maint.clock_tick();
    }
    #[allow(dead_code)]
    pub(crate) fn state_path(&self) -> &Path {
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
        self.mutate_mode(m, true)
    }
    pub(crate) fn mutate_progress(&self, m: Mutation) -> Outcome {
        debug_assert!(timers::is_progress(&m.ns, &m.key));
        self.mutate_mode(m, false)
    }
    pub(crate) fn mutate_mode(&self, m: Mutation, deduplicate: bool) -> Outcome {
        let _publication = self.admission_gate.read().unwrap();
        self.mutate_mode_ungated(m, deduplicate, None)
    }

    /// Bound new admission with the caller's local monotonic deadline.
    /// The Store still completes an already admitted write and reports its outcome.
    pub fn mutate_before(&self, m: Mutation, cutoff: Instant) -> Outcome {
        let _publication = self.admission_gate.read().unwrap();
        self.mutate_mode_ungated(m, true, Some(cutoff))
    }

    fn mutate_mode_ungated(&self, m: Mutation, deduplicate: bool, cutoff: Option<Instant>) -> Outcome {
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
        let id = dedup_identity(&m);
        let fp = fingerprint(&m);
        let log_index = stripe(&m.ns, &m.key);
        let representation_bytes = mutation_bytes(&m, 0, 0).len();
        let disk_bytes = if m.ack == 0 {
            0
        } else {
            (16 + representation_bytes) as u64
        };
        // Charge complete queued representation, not just value payloads. Empty
        // values still consume queue slots and framing/identifier memory.
        let queue_charge = representation_bytes + m.bytes.len() + 128;
        let memory_charge = 2 * queue_charge + 2 * index_charge(&k) + 512;
        let chunk_index_bytes = chunks::chunk_index_charge(&k);
        let mut state = self.state.lock().unwrap();
        let mono = self.clock.mono_ms();
        crate::namespace_reserve::expire(&mut state, mono);
        if let Some(d) = state.dedup.get(&id).filter(|_| deduplicate) {
            if d.fingerprint != fp {
                return Outcome::error(14);
            }
            if mono > d.until_mono {
                return Outcome::error(13);
            }
            loop {
                if let Some(o) = state.dedup.get(&id).and_then(|d| d.outcome.clone()) {
                    return o;
                }
                let wall = Duration::from_millis(m.deadline.saturating_sub(self.clock.utc_ms()));
                let wait = cutoff.map_or(wall, |end| wall.min(end.saturating_duration_since(Instant::now())));
                if wait.is_zero() {
                    return Outcome::error(17);
                }
                let (s, _) = self.done.wait_timeout(state, wait)
                    .unwrap();
                state = s;
            }
        }
        if deduplicate && state.retired_set.contains(&id) {
            return Outcome::error(13);
        }
        if self.clock.utc_ms() >= m.deadline || cutoff.is_some_and(|end| Instant::now() >= end) {
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
                pending.is_none() && old.as_ref().is_none_or(|v| v.tombstone)
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
        let live_new = !m.remove && old.as_ref().is_none_or(|o| o.tombstone);
        if new && state.keys >= self.config.index_cap {
            return Outcome::error(9);
        }
        if live_new && state.namespace_counts.get(&m.ns).copied().unwrap_or(0) >= self.config.quota
        {
            return Outcome::error(8);
        }
        while let Some((key, time)) = state.expiry.front().cloned() {
            if mono <= time {
                break;
            }
            if state.policy_publishing.contains(&key)
                || state.dedup.get(&key).is_some_and(|d| d.outcome.is_none())
            {
                break;
            }
            state.expiry.pop_front();
            if state.dedup.remove(&key).is_some() {
                // Keep the identity's charge while the bounded expired-ID ring owns its strings.
                state.retired_set.insert(key.clone());
                state.retired.push_back(key);
            }
        }
        while state.retired.len() > self.config.dedup_cap {
            let k = state.retired.pop_front().unwrap();
            state.retired_set.remove(&k);
            state.dedup_bytes -= dedup_charge(&k);
        }
        let admission_bytes = memory_charge
            + if new { index_charge(&k) } else { 0 }
            + chunk_index_bytes
            + if deduplicate { dedup_charge(&id) } else { 0 };
        if admission_bytes > self.cache_limit(&state) {
            return Outcome::error(7);
        }
        // Non-spending check at the original data-limit refusal boundary.
        if !crate::namespace_reserve::can_admit(&state, &m.ns, disk_bytes,
            self.config.state_limit_bytes.saturating_sub(self.config.maintenance_reserve_bytes)) {
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
        let at = self.clock.utc_ms();
        if at >= m.deadline || cutoff.is_some_and(|end| Instant::now() >= end) {
            return Outcome::error(5);
        }
        let version = state.next;
        // Each assigned revision must leave a representable persisted successor.
        let Some(next) = version.checked_add(1) else { return Outcome::error(7) };
        // Reserve/charge once, after every other pre-admission refusal. All
        // scalar, batch-item, policy and timer-control paths enter here.
        if !crate::namespace_reserve::admit(&mut state, &m.ns, disk_bytes,
            self.clock.mono_ms(), self.config.state_limit_bytes.saturating_sub(self.config.maintenance_reserve_bytes)) {
            return Outcome::error(7);
        }
        state.next = next;
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
                bytes: index_charge(&k),
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
        state.memory_reserved += memory_charge;
        state.stripe_reserved[log_index] += queue_charge;
        state.stripe_items[log_index] += 1;
        state.pending_disk += disk_bytes;
        if new { state.committed_bytes += index_charge(&k); }
        if deduplicate {
            state.dedup_bytes += dedup_charge(&id);
            let until_mono = self.clock.mono_ms().saturating_add(self.config.retry_ms);
            state.expiry.push_back((id.clone(), until_mono));
            state.dedup.insert(
                id.clone(),
                Dedup {
                    fingerprint: fp,
                    at,
                    until_mono,
                    replayed_chunk: chunks::LEGACY_CHUNK,
                    outcome: None,
                },
            );
            self.done.notify_all();
        }
        let chunk_index = (m.ack != 0)
            .then(|| chunks::ChunkIndexReservation::reserve(&self.chunks, chunk_index_bytes));
        self.trim_cache(&state);
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
                    chunk_index: chunk_index.clone(),
                    done,
                }) {
                Ok(()) => completion
                    .recv()
                    .unwrap_or_else(|_| Err(io::Error::other("append owner stopped"))),
                Err(_) => Err(io::Error::other("append owner stopped")),
            }
        };
        // Publication precedes completion on every connection; readers never await log I/O.
        let location = self.router.locations.lock().unwrap().remove(&version);
        let mut committed = (result.is_ok() && m.ack != 0).then(|| self.committed.write().unwrap());
        let mut state = self.state.lock().unwrap();
        let signal = committed.as_mut().and_then(|committed| {
            let value = Arc::new(value);
            let entry = Arc::new(crate::value_disk::Entry::new(&m, version, location.expect("append-complete location")));
            let class = entry.class;
            if let Some(old) = committed.insert(k.clone(), entry.clone()) {
                old.cacheable.store(false, Ordering::Release);
                self.cache.remove(old.version, false);
            }
            let (label, policy) = crate::cache::Cache::policy(&self.config, &m.ns, &m.key, class);
            self.cache.insert(value.clone(), label, policy, true, self.config.cache_bytes, &entry.cacheable);
            (m.ns != timers::NS).then_some(value)
        });
        let outcome = if result.is_ok() {
            Outcome {
                code: 0,
                version,
                ack: m.ack,
            }
        } else {
            Outcome::error(16)
        };
        state.reserved_bytes -= queue_charge;
        state.memory_reserved -= memory_charge;
        drop(chunk_index);
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
            state.metadata.insert(
                k.clone(),
                Meta {
                    version,
                    tombstone: m.remove,
                    bytes: index_charge(&k),
                    chunk: *chunk,
                },
            );
            if m.remove {
                // A remove of a live key frees its namespace slot at commit; the tombstone keeps an index entry.
                if old.as_ref().is_some_and(|o| !o.tombstone) {
                    state.release_namespace(&m.ns);
                }
                state.tomb_candidates.insert(k.clone());
            }
        } else {
            if new {
                state.keys -= 1;
                state.committed_bytes -= index_charge(&k);
            }
            if live_new {
                state.release_namespace(&m.ns);
            }
        }
        if result.is_err() {
            state.read_only = true
        }
        if deduplicate {
            state.dedup.get_mut(&id).unwrap().outcome = Some(outcome.clone());
        }
        // Return the write's working charge before funding history and live
        // delivery, and keep committed publication ordered through the handoff.
        if let Some(value) = signal {
            self.signals.publish_locked(&m.ns, &m.key, value, &self.signal_budget(&state));
        }
        self.trim_cache(&state);
        self.done.notify_all();
        outcome
    }
}
