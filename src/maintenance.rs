//! Retention, retirement, expiry summaries and the periodic maintenance worker.
use crate::chunks;
use crate::log_io::sync_chunk_directory;
use crate::routing::ChunkRouter;
use crate::store::Flags;
use crate::store::Key;
use crate::store::State;
use crate::Config;
use std::collections::HashSet;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::time::Duration;

#[cfg(test)]
mod directory_tests;
#[cfg(test)]
mod clock_tests;
#[cfg(test)]
#[path = "maintenance_correction_tests.rs"]
mod correction_tests;
#[cfg(test)]
mod overlap_tests;

/// Retirement, expiry summaries, tombstone drops and the clock check (5.3, 5.4), shared with the tick thread.
pub(super) struct Maint {
    pub(super) admission_gate: Arc<RwLock<()>>,
    pub(super) cache: Arc<crate::cache::Cache>,
    pub(super) state: Arc<Mutex<State>>,
    pub(super) committed: Arc<RwLock<crate::store::Committed>>,
    pub(super) chunks: Arc<Mutex<chunks::ChunkStore>>,
    pub(super) router: Arc<ChunkRouter>,
    pub(super) flags: Arc<Flags>,
    pub(super) clock: chunks::Clock,
    pub(super) next_issued: Arc<AtomicU64>,
    pub(super) config: Config,
    pub(super) path: PathBuf,
}
impl Maint {
    /// Retire closed chunks due by their class, or (forced) one chunk that holds no key's latest record.
    /// Returns the chunks retired whole; a stopped, kept or held chunk is left out.
    pub(super) fn retire(&self, forced: Option<u64>) -> io::Result<Vec<u64>> {
        let _publication = self.admission_gate.read().unwrap();
        use chunks::ChunkState;
        let utc = self.clock.utc_ms();
        let mono = self.clock.mono_ms();
        let mut gone: Vec<(Key, u64)> = Vec::new();
        let mut started: Vec<(u64, PathBuf, u64, Vec<Key>)> = Vec::new();
        let mut failure = None;
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
            // One pass per invocation, including forced requests. The same locks
            // keep this snapshot valid through all candidate decisions.
            let retry_chunks: HashSet<u64> = state.dedup.values().filter_map(|d| {
                #[cfg(test)] correction_tests::visit();
                (d.replayed_chunk != chunks::LEGACY_CHUNK
                    && (d.outcome.is_none() || mono <= d.until_mono)).then_some(d.replayed_chunk)
            }).collect();
            for id in ids {
                let Some(c) = cs.table.get(&id) else { continue };
                if id == chunks::LEGACY_CHUNK || c.state != ChunkState::Closed {
                    continue;
                }
                // A multi-entry outcome owns the whole chunk through its retry horizon,
                // including forced retirement. Never retain only a prefix's retry evidence.
                if retry_chunks.contains(&id) { continue; }
                let expire = forced.is_none() && c.class.kind == 2;
                if forced.is_none() {
                    // Recorded close UTC can precede an admission after a wall
                    // correction. Preserve every still-retryable recovered outcome.
                    if c.recovered_retry_until_mono.is_some_and(|until| mono <= until) {
                        continue;
                    }
                    let window = match c.class.kind {
                        1 => self.config.retry_ms,
                        2 => c.class.retention_ms,
                        _ => continue,
                    };
                    let Some(close) = c.close_ms else { continue };
                    // Retention may be due at equality, but retry outcomes remain
                    // valid through their inclusive horizon. Require both clocks
                    // to pass it; subtract ages so a large UTC cannot saturate an
                    // unelapsed window into eligibility after recovery.
                    let allows = |now: u64, closed: u64| now.checked_sub(closed)
                        .is_some_and(|age| age >= window && age > self.config.retry_ms);
                    if !allows(utc, close)
                        || c.close_mono.is_some_and(|m| !allows(mono, m))
                    {
                        continue;
                    }
                }
                if c.keys.keys().any(|k| state.pending.contains_key(k)) {
                    continue;
                }
                let latest: Vec<Key> = c
                    .keys
                    .keys()
                    .filter(|k| state.metadata.get(*k).is_some_and(|m| m.chunk == id))
                    .cloned()
                    .collect();
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
                if let Err(e) = cs.log_retiring(utc, id, next, &horizons, &latest) {
                    self.flags.storage_error.store(true, Ordering::SeqCst);
                    failure = Some(e);
                    break;
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
                            state.release_namespace(&k.0);
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
                info.retired_ms = Some(utc);
                if self.config.retire_stop_after == 1 {
                    continue;
                }
                for k in c.keys.keys() {
                    cs.holders.remove(k, id);
                    state.tomb_candidates.insert(k.clone());
                }
                started.push((
                    id,
                    c.path.clone(),
                    c.bytes,
                    c.keys.keys().cloned().collect(),
                ));
            }
        }
        if !gone.is_empty() {
            let mut committed = self.committed.write().unwrap();
            for (k, v) in gone {
                if committed.get(&k).is_some_and(|x| x.version == v) {
                    if let Some(entry) = committed.remove(&k) { entry.cacheable.store(false, Ordering::Release); }
                    self.cache.remove(v, true);
                }
            }
        }
        // Steps 3 and 4: delete the file, then `retired`. A held delete stays retiring and is retried.
        #[cfg(test)]
        if !started.is_empty() {
            if let Some(hook) = &self.config.retirement_started { hook(); }
        }
        let mut done = Vec::new();
        let mut freed = 0;
        {
            let mut cs = self.chunks.lock().unwrap();
            for (id, p, bytes, keys) in started {
                if failure.is_some() {
                    break;
                }
                // Preparation released this lock. A retry may already have
                // completed (and maintenance may have pruned) this obligation.
                // Only its still-Retiring state owns deletion/publication and
                // discharge; hold the chunk lock through that transition.
                if !cs.table.get(&id).is_some_and(|c| c.state == ChunkState::Retiring) {
                    continue;
                }
                match fs::remove_file(&p) {
                    Ok(()) => {}
                    Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                    Err(_) => {
                        cs.table
                            .get_mut(&id)
                            .unwrap()
                            .blocked_since
                            .get_or_insert(self.clock.mono_ms());
                        continue;
                    }
                }
                if self.config.retire_stop_after == 2 {
                    continue;
                }
                if let Err(e) = sync_chunk_directory(p.parent().unwrap(), &self.config) {
                    self.flags.storage_error.store(true, Ordering::SeqCst);
                    failure = Some(e);
                    break;
                }
                if let Err(e) = cs.log(utc, chunks::Rec::Retired { seq: id }) {
                    self.flags.storage_error.store(true, Ordering::SeqCst);
                    failure = Some(e);
                    break;
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
        match failure {
            Some(error) => Err(error),
            None => Ok(done),
        }
    }
    /// Retry the held deletes; a retiring chunk's index work and horizons are already done.
    pub(super) fn retry_retiring(&self) -> io::Result<usize> {
        let mut cs = self.chunks.lock().unwrap();
        let ids: Vec<u64> = cs
            .table
            .values()
            .filter(|c| c.state == chunks::ChunkState::Retiring)
            .map(|c| c.id)
            .collect();
        let mut done = 0;
        let mut freed = 0;
        let mut failure = None;
        for id in ids {
            let (p, bytes) = (cs.table[&id].path.clone(), cs.table[&id].bytes);
            match fs::remove_file(&p) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(_) => {
                    cs.table.get_mut(&id).unwrap().blocked_since
                        .get_or_insert(self.clock.mono_ms());
                    continue;
                }
            }
            if let Err(e) = sync_chunk_directory(p.parent().unwrap(), &self.config) {
                self.flags.storage_error.store(true, Ordering::SeqCst);
                failure = Some(e);
                break;
            }
            let now = self.clock.utc_ms();
            if let Err(e) = cs.log(now, chunks::Rec::Retired { seq: id }) {
                self.flags.storage_error.store(true, Ordering::SeqCst);
                failure = Some(e);
                break;
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
        match failure {
            Some(error) => Err(error),
            None => Ok(done),
        }
    }
    /// Drop tombstone index entries no other retained chunk can contradict (5.3), and expired summaries.
    pub(super) fn drop_tombstones(&self) {
        let _publication = self.admission_gate.read().unwrap();
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
                let Some(m) = state.metadata.get(&k) else {
                    continue;
                };
                if !m.tombstone {
                    continue;
                }
                let others: Vec<u64> = cs.holders.others(&k, m.chunk).collect();
                if others.is_empty() {
                    let m = state.metadata.remove(&k).unwrap();
                    state.keys -= 1;
                    state.committed_bytes -= m.bytes;
                    gone.push((k, m.version));
                } else if others.iter().all(|c| {
                    *c != chunks::LEGACY_CHUNK && cs.table.get(c).is_some_and(|i| i.class.finite())
                }) {
                    // Older records in finite chunks retire in time; one in a forever chunk never does.
                    state.tomb_candidates.insert(k);
                }
            }
            cs.recount_index_bytes();
        }
        if !gone.is_empty() {
            let mut committed = self.committed.write().unwrap();
            for (k, v) in gone {
                if committed.get(&k).is_some_and(|x| x.version == v) {
                    if let Some(entry) = committed.remove(&k) { entry.cacheable.store(false, Ordering::Release); }
                    self.cache.remove(v, true);
                }
            }
        }
    }
    /// One tick's clock check (5.4): a stable run of a rotation age, or `expiry.resume`, ends a pause.
    pub(super) fn clock_tick(&self) {
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
        let stable = cs
            .stable_since
            .is_some_and(|(_, m)| mono.saturating_sub(m) >= self.config.rotation_ms);
        if cs.degraded.is_some() && (resume || stable) {
            let basis = if resume { chunks::ResumeBasis::Operator } else {
                let (since_utc_ms, since_mono_ms) = cs.stable_since.unwrap();
                chunks::ResumeBasis::StableClock { since_utc_ms, since_mono_ms,
                    observed_mono_ms: mono, required_ms: self.config.rotation_ms, tolerance_ms: 2000 }
            };
            let rec = chunks::Rec::ResumeWithBasis(basis);
            if cs.log(utc, rec.clone()).is_err() {
                self.flags.storage_error.store(true, Ordering::SeqCst);
                // Preserve an explicit request until Resume is published. A
                // later tick can retry after the catalogue becomes writable.
                return;
            }
            cs.last_resume = Some(chunks::CatRecord { at_ms: utc, rec });
            cs.degraded = None;
            self.flags.expiry_degraded.store(false, Ordering::SeqCst);
        }
        if resume {
            let _ = fs::remove_file(resume_file);
        }
    }
    /// The 10 s pass: held deletes, chunks due, tombstones and summaries.
    pub(super) fn pass(&self) {
        let _ = self.retry_retiring();
        let _ = self.retire(None);
        self.drop_tombstones();
    }
    pub(super) fn prune_discharged(&self) {
        let state = self.state.lock().unwrap();
        let mut cs = self.chunks.lock().unwrap();
        let referenced: HashSet<u64> = state.metadata.values().map(|m| m.chunk).collect();
        let ids: HashSet<u64> = cs
            .table
            .values()
            .filter(|c| c.state == chunks::ChunkState::Retired && !referenced.contains(&c.id))
            .map(|c| c.id)
            .collect();
        if ids.is_empty() {
            return;
        }
        // Retirement requires no pending key belonging to a closed chunk. Mutation keeps that
        // reservation through append completion AND any delayed flush notice/result. Therefore a
        // durably Retired sink has no outstanding Store waiter; Retiring/closed sinks are retained.
        self.router
            .table
            .write()
            .unwrap()
            .retain(|id, _| !ids.contains(&(*id as u64)));
        cs.table.retain(|id, _| !ids.contains(id));
        cs.holders.map.retain(|_, holders| {
            holders.retain(|id| !ids.contains(id));
            !holders.is_empty()
        });
        let held: HashSet<_> = cs.holders.map.keys().cloned().collect();
        cs.horizons.retain(|key, _| held.contains(key));
        cs.recount_index_bytes();
    }
    /// Repair from the owned live list independently of ordinary growth compaction.
    pub(super) fn rewrite_catalogue(&self) {
        let _ = self.maintain_catalogue();
    }
    pub(super) fn maintain_catalogue(&self) -> io::Result<CatalogueMaintenance> {
        let mut cs = self.chunks.lock().unwrap();
        let result = (|| {
            let cat = cs.cat.as_ref().ok_or_else(|| io::Error::other("catalogue not open"))?;
            let recovering = cat.recovery_required()?;
            if !recovering && !cs.incomplete_retiring && !cat.needs_rewrite(self.config.catalogue_rewrite_bytes) {
                return Ok(CatalogueMaintenance::Unchanged);
            }
            cs.rewrite_live(self.clock.utc_ms(), self.next_issued.load(Ordering::SeqCst))?;
            Ok(if recovering {
                CatalogueMaintenance::Recovered { evidence: cs.cat.as_ref().unwrap().recovery_evidence().unwrap().to_owned() }
            } else { CatalogueMaintenance::Compacted })
        })();
        if result.is_err() { self.flags.storage_error.store(true, Ordering::SeqCst); }
        result
    }
}

impl Maint {
    pub(super) fn start(self: &Arc<Self>) -> (mpsc::Sender<()>, std::thread::JoinHandle<()>) {
        let (tick_stop, tick_receiver) = mpsc::channel();
        let tick_maint = self.clone();
        let tick_every = Duration::from_millis(self.config.tick_ms.max(1));
        let retire_every = self.config.retire_ms;
        let ticker = std::thread::spawn(move || {
            let mut last_pass = tick_maint.clock.mono_ms();
            while matches!(
                tick_receiver.recv_timeout(tick_every),
                Err(mpsc::RecvTimeoutError::Timeout)
            ) {
                tick_maint.router.idle_close();
                tick_maint.clock_tick();
                tick_maint.cache.sweep();
                if tick_maint.clock.mono_ms().saturating_sub(last_pass) >= retire_every {
                    last_pass = tick_maint.clock.mono_ms();
                    tick_maint.pass();
                }
                tick_maint.rewrite_catalogue();
                tick_maint.prune_discharged();
            }
        });
        (tick_stop, ticker)
    }
}

/// Outcome of owned live-list maintenance; recovery does not clear sticky health flags.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CatalogueMaintenance {
    Unchanged,
    Compacted,
    Recovered { evidence: PathBuf },
}
