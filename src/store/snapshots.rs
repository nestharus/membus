//! Snapshot registry, retained-resource charges, query-bound cursors, paging and TTL reaping.
use crate::codec::encode_value;
use crate::codec::lp;
use crate::codec::reply_header;
use crate::codec::Cursor;
use crate::codec::MAX_BATCH;
use crate::codec::MAX_ID;
use crate::codec::SCAN_KEYS_PREFIX;
use crate::codec::SCAN_KEYS_RANGE;
use crate::codec::SCAN_RANGE;
use crate::value_disk::Entry;
use crate::Config;
use crate::Store;
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::Duration;

#[derive(Default)]
pub(crate) struct ScanUsage {
    pub(crate) bytes: AtomicUsize,
    pub(crate) count: AtomicUsize,
    pub(crate) released: AtomicBool,
}
pub(crate) struct ScanCharge {
    pub(crate) usage: Arc<ScanUsage>,
    pub(crate) bytes: usize,
}
impl Drop for ScanCharge {
    fn drop(&mut self) {
        self.usage.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        self.usage.count.fetch_sub(1, Ordering::AcqRel);
        // Registry removal alone does not free a view held by a dispatcher.
        // Notify cold admission only when the last owner returns its charge.
        self.usage.released.store(true, Ordering::Release);
    }
}
pub(crate) struct Snapshot {
    pub(crate) ns: String,
    pub(crate) created: u64,
    pub(crate) rows: Vec<(String, SnapshotValue)>,
    pub(crate) selection: Option<crate::selected_view::CapturedSelection>,
    pub(crate) cursor_hash: RandomState,
    pub(crate) complete: bool,
    pub(crate) bounded_load: Option<Mutex<()>>,
    // Charges survive release/expiry while a dispatcher still holds this Arc.
    pub(crate) _charge: Arc<ScanCharge>,
}
pub(crate) struct SnapshotValue {
    pub(crate) entry: Arc<Entry>,
    // The snapshot's full-payload reservation covers values pinned on first use.
    // Never resolve through the current key: replacement and retirement may have
    // removed this exact revision from the live index and resident cache.
    loaded: OnceLock<crate::ReadValue>,
    workspace: Arc<crate::read_cohort::SnapshotWorkspace>,
    // A captured revision may be ineligible for the live cache. Coordinate
    // its first load within this view without retaining failures.
    load_gate: Mutex<()>,
    #[cfg(test)]
    load_attempts: AtomicUsize,
}
impl SnapshotValue {
    pub(crate) fn new(
        entry: Arc<Entry>,
        workspace: Arc<crate::read_cohort::SnapshotWorkspace>,
    ) -> Self {
        Self {
            entry,
            loaded: OnceLock::new(),
            workspace,
            load_gate: Mutex::new(()),
            #[cfg(test)]
            load_attempts: AtomicUsize::new(0),
        }
    }
    pub(crate) fn load(&self, store: &Store, ns: &str, key: &str) -> Result<crate::ReadValue, u16> {
        if let Some(value) = self.loaded.get() {
            return Ok(value.clone());
        }
        #[cfg(test)]
        self.load_attempts.fetch_add(1, Ordering::SeqCst);
        let _load = self.load_gate.lock().unwrap();
        if let Some(value) = self.loaded.get() {
            return Ok(value.clone());
        }
        let value = if self.entry.location.subrecord.is_some() {
            self.workspace.read(store, ns, key, &self.entry)?
        } else {
            crate::ReadValue::snapshot(store.read_entry(ns, key, &self.entry).map_err(|_| 16u16)?,
                self.workspace.owner.clone())
        };
        // Publish only a successful immutable payload. Errors leave the slot
        // empty and release the gate so a later request can retry storage.
        let _ = self.loaded.set(value);
        Ok(self.loaded.get().unwrap().clone())
    }
}
pub(crate) struct ScanState {
    pub(crate) next: u64,
    pub(crate) snapshots: HashMap<u64, Arc<Snapshot>>,
}
impl Snapshot {
    pub(crate) fn load_at(&self, store: &Store, at: usize) -> Result<crate::ReadValue, u16> {
        let (key, value) = &self.rows[at];
        let Some(gate) = self.bounded_load.as_ref().filter(|_| value.entry.location.subrecord.is_some()) else {
            return value.load(store, &self.ns, key);
        };
        if let Some(loaded) = value.loaded.get() {
            return Ok(loaded.clone());
        }
        let _load = gate.lock().unwrap();
        if let Some(loaded) = value.loaded.get() {
            return Ok(loaded.clone());
        }
        let result = self.load_bounded_envelope(store, value);
        if result.is_err() {
            store.flags.storage_error.store(true, Ordering::SeqCst);
        }
        result
    }

    fn load_bounded_envelope(&self, store: &Store, target: &SnapshotValue) -> Result<crate::ReadValue, u16> {
        let location = &target.entry.location;
        let _reader = store.cold_reader(location.record_len as usize);
        store.cache.cold();
        let encoded = location.whole_payload().map_err(|_| 16u16)?;
        let envelope = crate::atomic::decode_envelope(&encoded).map_err(|_| 16u16)?;
        let request = envelope.request;
        let ack = request.requested_ack().byte();
        let members = || request.changes().enumerate().filter_map(|(index, (key, value))| {
            let row = self.rows.binary_search_by(|(name, _)| name.as_str().cmp(key)).ok()?;
            let row = &self.rows[row].1;
            let version = envelope.first + index as u64;
            (row.entry.version == version).then_some((row, index, key, version, value))
        });
        // Validate every captured member from this extent before installing any of them.
        let mut found = false;
        for (row, index, key, version, value) in members() {
            let entry = &row.entry;
            if !entry.location.same_frame(location) || entry.location.subrecord != Some(index as u16) {
                return Err(16);
            }
            entry.check_identity(&self.ns, key, (request.namespace(), key, version, ack, value.is_none(), value.map_or(0, <[u8]>::len)))
                .map_err(|_| 16u16)?;
            found |= std::ptr::eq(row, target);
        }
        if !found {
            return Err(16);
        }
        for (row, _, _, version, value) in members() {
            let value = Arc::new(crate::Value {
                version, ack, bytes: value.unwrap_or_default().to_vec(), tombstone: false,
            });
            let _ = row.loaded.set(crate::ReadValue::snapshot(value, self._charge.clone()));
        }
        Ok(target.loaded.get().unwrap().clone())
    }
}
impl Store {
    pub(crate) fn purge_scans(&self, state: &mut ScanState) {
        let now = self.clock.mono_ms();
        state.snapshots.retain(|_, s| now.saturating_sub(s.created) < self.config.snapshot_ttl_ms);
    }
    pub(crate) fn snapshot_open(&self, ns: &str) -> Result<(Vec<u8>, Arc<Snapshot>), u16> {
        self.snapshot_open_bounded(ns, None)
    }
    pub(crate) fn snapshot_open_bounded(&self, ns: &str, bounds: Option<(usize, usize)>) -> Result<(Vec<u8>, Arc<Snapshot>), u16> {
        let created = self.clock.mono_ms();
        {
            let mut scans = self.scans.lock().unwrap();
            self.purge_scans(&mut scans);
            if self.scan_usage.count.load(Ordering::Acquire) >= self.config.snapshot_max {
                return Err(22);
            }
        }
        let lock_start = self.clock.mono_ns();
        #[cfg(test)]
        self.capture_waiters.fetch_add(1, Ordering::SeqCst);
        let committed = self.committed.read().unwrap();
        #[cfg(test)]
        self.capture_waiters.fetch_sub(1, Ordering::SeqCst);
        let lock_acquired = self.clock.mono_ns();
        let mut charge = 256usize.checked_add(ns.len()).ok_or(22u16)?;
        if let Some((_, max_bytes)) = bounds {
            charge = charge.checked_add(crate::atomic_population::CAPTURE_WORKSPACE_BYTES).ok_or(22u16)?;
            if charge > max_bytes {
                return Err(22);
            }
        }
        let mut row_count = 0usize;
        for ((_, key), value) in committed
            .range((ns.to_owned(), String::new())..)
            .inspect(|_| {
                #[cfg(test)]
                self.capture_visits.fetch_add(1, Ordering::Relaxed);
            })
            .take_while(|((namespace, _), _)| namespace == ns)
        {
            if !value.tombstone {
                // Full payload capacity is charged even when shared with the live
                // index or another snapshot; 128 covers per-row handles/metadata.
                charge = charge
                    .checked_add(key.len())
                    .and_then(|n| n.checked_add(128))
                    .and_then(|n| n.checked_add(value.len))
                    .ok_or(22u16)?;
                row_count += 1;
                if let Some((max_keys, max_bytes)) = bounds {
                    charge = charge.checked_add(512 + key.len() + value.len).ok_or(22u16)?;
                    if row_count > max_keys || charge > max_bytes {
                        return Err(22);
                    }
                }
            }
        }
        let charge_end = self.clock.mono_ns();
        // Index waits and namespace enumeration must not stall admitted views.
        // Another capture may have reserved while this one read the index.
        let mut scans = self.scans.lock().unwrap();
        self.purge_scans(&mut scans);
        if self.scan_usage.count.load(Ordering::Acquire) >= self.config.snapshot_max {
            return Err(22);
        }
        let total = self
            .scan_usage
            .bytes
            .load(Ordering::Acquire)
            .checked_add(charge)
            .ok_or(22u16)?;
        if total > self.config.snapshot_bytes {
            return Err(22);
        }
        let state = self.state.lock().unwrap();
        let complete = !state.read_only && !state.warming
            && !self.flags.storage_error.load(Ordering::SeqCst);
        // Ordinary and atomic captures take scans before State. A snapshot
        // workspace can grow under State alone, so recheck after acquiring it.
        if self.scan_usage.count.load(Ordering::Acquire) >= self.config.snapshot_max
            || self.scan_usage.bytes.load(Ordering::Acquire).checked_add(charge)
                .is_none_or(|total| total > self.config.snapshot_bytes)
        {
            return Err(22);
        }
        if charge > self.cache_limit(&state) {
            return Err(22);
        }
        let id = scans.next;
        scans.next = id.checked_add(1).ok_or(22u16)?;
        // Reservation is serialized before any snapshot key/handle allocation.
        self.scan_usage.bytes.fetch_add(charge, Ordering::AcqRel);
        self.scan_usage.count.fetch_add(1, Ordering::AcqRel);
        let reservation = Arc::new(ScanCharge {
            usage: self.scan_usage.clone(),
            bytes: charge,
        });
        drop(scans);
        self.trim_cache(&state);
        drop(state);
        let workspace = Arc::new(crate::read_cohort::SnapshotWorkspace::new(reservation.clone()));
        let selection_start = self.clock.mono_ns();
        // Seek the same namespace interval again under the same read lock.
        // Pin immutable locations and their shared file handles. Opening a view
        // or reading keys must not load unrelated values and churn the cache.
        // Admission precedes the exact live-row reservation under the same read lock.
        let mut rows = Vec::with_capacity(row_count);
        rows.extend(
            committed
                .range((ns.to_owned(), String::new())..)
                .inspect(|_| {
                    #[cfg(test)]
                    self.capture_visits.fetch_add(1, Ordering::Relaxed);
                })
                .take_while(|((namespace, _), _)| namespace == ns)
                .filter(|(_, value)| !value.tombstone)
                .map(|((_, key), entry)| {
                    (
                        key.clone(),
                        SnapshotValue {
                            entry: entry.clone(),
                            loaded: OnceLock::new(),
                            workspace: workspace.clone(),
                            load_gate: Mutex::new(()),
                            #[cfg(test)]
                            load_attempts: AtomicUsize::new(0),
                        },
                    )
                }),
        );
        let selection_end = self.clock.mono_ns();
        let copy_end = self.clock.mono_ns();
        drop(committed);
        // The committed index already yields keys in namespace/key order.
        let times = [
            1,
            lock_acquired.saturating_sub(lock_start),
            charge_end.saturating_sub(lock_acquired),
            selection_end.saturating_sub(selection_start),
            copy_end.saturating_sub(selection_end),
            copy_end.saturating_sub(lock_acquired),
            0, // Ordered capture needs no separate sorting pass.
        ];
        for (metric, n) in self.capture_metrics.iter().zip(times) {
            metric.fetch_add(n, Ordering::Relaxed);
        }
        let snapshot = Arc::new(Snapshot {
            ns: ns.to_owned(),
            created,
            rows,
            selection: None,
            cursor_hash: RandomState::new(),
            complete,
            bounded_load: bounds.map(|_| Mutex::new(())),
            _charge: reservation,
        });
        if self.clock.mono_ms().saturating_sub(snapshot.created) >= self.config.snapshot_ttl_ms {
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
    pub(crate) fn snapshot_require_complete(&self, ns: &str, token: &[u8]) -> Result<(), u16> {
        let snapshot = self.snapshot_get(ns, token)?;
        if snapshot.complete && snapshot.selection.is_none() {
            Ok(())
        } else {
            Err(16)
        }
    }
    pub(crate) fn snapshot_get(&self, ns: &str, token: &[u8]) -> Result<Arc<Snapshot>, u16> {
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
    /// Release a snapshot token. In-flight readers retain their existing charge.
    pub fn snapshot_release(&self, ns: &str, token: &[u8]) -> Result<(), u16> {
        let snapshot = self.snapshot_get(ns, token)?;
        let id = u64::from_le_bytes(token[8..].try_into().unwrap());
        self.scans.lock().unwrap().snapshots.remove(&id);
        drop(snapshot);
        Ok(())
    }
    #[cfg(test)]
    pub(crate) fn scan_page(
        &self,
        ns: &str,
        c: &mut Cursor<'_>,
        op: u8,
        max_frame: usize,
    ) -> Result<Vec<u8>, u16> {
        self.scan_output(ns, c, op, max_frame, None)
    }
    pub(crate) fn scan_reply(
        &self,
        ns: &str,
        c: &mut Cursor<'_>,
        op: u8,
        id: u64,
        max_frame: usize,
    ) -> Result<Vec<u8>, u16> {
        self.scan_output(ns, c, op, max_frame, Some(id))
    }
    fn scan_output(
        &self,
        ns: &str,
        c: &mut Cursor<'_>,
        op: u8,
        max_frame: usize,
        response_id: Option<u64>,
    ) -> Result<Vec<u8>, u16> {
        let keys_only = matches!(op, SCAN_KEYS_PREFIX | SCAN_KEYS_RANGE);
        let is_range = matches!(op, SCAN_RANGE | SCAN_KEYS_RANGE);
        let supplied = c.bytes(16)?;
        let cursor = c.bytes(2 * MAX_ID + 48)?;
        let limit = c.u16()? as usize;
        let budget = (c.u32()? as usize).min(max_frame);
        // Keep the original length-prefixed bounds for cursor binding. Neither
        // selection nor authentication needs an owned copy of this request.
        let bounds = c.remainder();
        let lower = c.bound_ref()?;
        let upper = if is_range { c.bound_ref()? } else { "" };
        c.end()?;
        if limit == 0
            || limit > MAX_BATCH
            || budget < 42
            || (is_range && !upper.is_empty() && lower > upper)
        {
            return Err(3);
        }
        let query_len = 1 + bounds.len();
        // A cursor must be accompanied by its snapshot, and binds the full
        // query plus an absolute index. Page size/budget can change on retry.
        let position = if cursor.is_empty() {
            None
        } else {
            if supplied.len() != 16
                || cursor.len() != 32 + query_len
                || &cursor[..16] != supplied
                || cursor[24] != op
                || &cursor[25..cursor.len() - 8] != bounds
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
            (supplied.to_vec(), self.snapshot_get(ns, supplied)?)
        };
        let result = (|| {
            if snapshot.selection.as_ref().is_some_and(|s| !s.matches(is_range, lower, upper)) {
                return Err(21);
            }
            if !cursor.is_empty() {
                let split = cursor.len() - 8;
                let tag = u64::from_le_bytes(cursor[split..].try_into().unwrap());
                if snapshot.cursor_hash.hash_one(&cursor[..split]) != tag {
                    return Err(21);
                }
            }
            let rows = &snapshot.rows;
            let start = rows.partition_point(|(k, _)| k.as_str() < lower);
            let end = if !is_range {
                // The matching block is contiguous under UTF-8 byte order.
                start + rows[start..].partition_point(|(k, _)| k.starts_with(lower))
            } else if upper.is_empty() {
                rows.len()
            } else {
                rows.partition_point(|(k, _)| k.as_str() < upper)
            };
            let at = position.unwrap_or(start);
            if at < start || at > end {
                return Err(21);
            }
            let mut next = at;
            let mut item_bytes = 0;
            while next < end && next - at < limit {
                let (key, value) = &rows[next];
                let bytes = 4 + key.len() + if keys_only { 0 } else { 14 + value.entry.len };
                let continuation = if next + 1 < end { 32 + query_len } else { 0 };
                if 42 + item_bytes + bytes + continuation > budget {
                    if next == at {
                        return Err(4);
                    }
                    break;
                }
                item_bytes += bytes;
                next += 1;
            }
            let continuation_len = if next < end { 32 + query_len } else { 0 };
            let payload_len = 27 + item_bytes + continuation_len;
            let mut p = match response_id {
                Some(id) => reply_header(op, id, 0, payload_len),
                None => Vec::with_capacity(payload_len),
            };
            lp(&mut p, &token);
            p.extend((continuation_len as u32).to_le_bytes());
            if continuation_len != 0 {
                let start = p.len();
                p.extend(&token);
                p.extend((next as u64).to_le_bytes());
                p.push(op);
                p.extend(bounds);
                let tag = snapshot.cursor_hash.hash_one(&p[start..]);
                p.extend(tag.to_le_bytes());
            }
            p.push((next == end) as u8);
            p.extend(((next - at) as u16).to_le_bytes());
            for (offset, (key, _)) in rows[at..next].iter().enumerate() {
                lp(&mut p, key.as_bytes());
                if !keys_only {
                    let loaded = snapshot.load_at(self, at + offset)?;
                    encode_value(&mut p, Some(&loaded));
                }
            }
            if self.clock.mono_ms().saturating_sub(snapshot.created) >= self.config.snapshot_ttl_ms {
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
}

pub(crate) fn start_reaper(
    scans: Arc<Mutex<ScanState>>,
    config: &Config,
    clock: crate::chunks::Clock,
) -> (mpsc::Sender<()>, std::thread::JoinHandle<()>) {
    let (stop, receiver) = mpsc::channel();
    let reaper_scans = scans;
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
                .retain(|_, s| clock.mono_ms().saturating_sub(s.created) < ttl.as_millis() as u64);
        }
    });
    (stop, reaper)
}

#[cfg(test)]
#[path = "../unit_tests/store/snapshots_load_tests.rs"]
mod load_tests;

#[cfg(test)]
#[path = "../unit_tests/store/snapshots_allowance_tests.rs"]
mod allowance_tests;
