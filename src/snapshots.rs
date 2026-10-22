//! Snapshot registry, retained-resource charges, query-bound cursors, paging and TTL reaping.
use crate::codec::encode_value;
use crate::codec::lp;
use crate::codec::Cursor;
use crate::codec::MAX_BATCH;
use crate::codec::MAX_ID;
use crate::codec::SCAN_KEYS_PREFIX;
use crate::codec::SCAN_KEYS_RANGE;
use crate::codec::SCAN_RANGE;
use crate::Config;
use crate::Store;
use crate::Value;
use std::collections::hash_map::RandomState;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use std::time::Instant;

#[derive(Default)]
pub(super) struct ScanUsage {
    pub(super) bytes: AtomicUsize,
    pub(super) count: AtomicUsize,
}
pub(super) struct ScanCharge {
    pub(super) usage: Arc<ScanUsage>,
    pub(super) bytes: usize,
}
impl Drop for ScanCharge {
    fn drop(&mut self) {
        self.usage.bytes.fetch_sub(self.bytes, Ordering::AcqRel);
        self.usage.count.fetch_sub(1, Ordering::AcqRel);
    }
}
pub(super) struct Snapshot {
    pub(super) ns: String,
    pub(super) created: Instant,
    pub(super) rows: Vec<(String, Arc<Value>)>,
    pub(super) cursor_hash: RandomState,
    // Charges survive release/expiry while a dispatcher still holds this Arc.
    pub(super) _charge: ScanCharge,
}
pub(super) struct ScanState {
    pub(super) next: u64,
    pub(super) snapshots: HashMap<u64, Arc<Snapshot>>,
}
impl Store {
    pub(super) fn purge_scans(&self, state: &mut ScanState) {
        let ttl = Duration::from_millis(self.config.snapshot_ttl_ms);
        state.snapshots.retain(|_, s| s.created.elapsed() < ttl);
    }
    pub(super) fn snapshot_open(&self, ns: &str) -> Result<(Vec<u8>, Arc<Snapshot>), u16> {
        let created = Instant::now();
        let mut scans = self.scans.lock().unwrap();
        self.purge_scans(&mut scans);
        if self.scan_usage.count.load(Ordering::Acquire) >= self.config.snapshot_max {
            return Err(22);
        }
        let lock_start = Instant::now();
        let committed = self.committed.read().unwrap();
        let lock_acquired = Instant::now();
        let mut charge = 256usize.checked_add(ns.len()).ok_or(22u16)?;
        let mut count = 0;
        for ((namespace, key), value) in committed.iter() {
            if namespace == ns && !value.tombstone {
                // Full payload capacity is charged even when shared with the live
                // index or another snapshot; 128 covers per-row handles/metadata.
                charge = charge
                    .checked_add(key.len())
                    .and_then(|n| n.checked_add(128))
                    .and_then(|n| n.checked_add(value.len))
                    .ok_or(22u16)?;
                count += 1;
            }
        }
        let charge_end = Instant::now();
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
        if charge > self.cache_limit(&state) { return Err(22); }
        let id = scans.next;
        scans.next = id.checked_add(1).ok_or(22u16)?;
        // Reservation is serialized before any snapshot key/handle allocation.
        self.scan_usage.bytes.fetch_add(charge, Ordering::AcqRel);
        self.scan_usage.count.fetch_add(1, Ordering::AcqRel);
        self.trim_cache(&state);
        drop(state);
        let reservation = ScanCharge {
            usage: self.scan_usage.clone(),
            bytes: charge,
        };
        let selection_start = Instant::now();
        // Separate the second global index walk from namespace-owned copying.
        // Copy immutable locations under the committed read lock, then release it
        // before loading any missing payload. The row charge covers both vectors.
        let selected: Vec<_> = committed
            .iter()
            .filter(|((namespace, _), value)| namespace == ns && !value.tombstone)
            .map(|((_, key), value)| (key.clone(), value.clone()))
            .collect();
        let selection_end = Instant::now();
        let copy_end = Instant::now();
        drop(committed);
        drop(scans);
        let mut rows = Vec::with_capacity(count);
        for (key, value) in selected {
            let payload = self.read_entry(ns, &key, &value).map_err(|_| 16u16)?;
            rows.push((key, payload));
        }
        let sort_start = Instant::now();
        rows.sort_unstable_by(|a, b| a.0.cmp(&b.0));
        let sort_end = Instant::now();
        let times = [
            1,
            lock_acquired.duration_since(lock_start).as_nanos() as u64,
            charge_end.duration_since(lock_acquired).as_nanos() as u64,
            selection_end.duration_since(selection_start).as_nanos() as u64,
            copy_end.duration_since(selection_end).as_nanos() as u64,
            copy_end.duration_since(lock_acquired).as_nanos() as u64,
            sort_end.duration_since(sort_start).as_nanos() as u64,
        ];
        for (metric, n) in self.capture_metrics.iter().zip(times) {
            metric.fetch_add(n, Ordering::Relaxed);
        }
        let snapshot = Arc::new(Snapshot {
            ns: ns.to_owned(),
            created,
            rows,
            cursor_hash: RandomState::new(),
            _charge: reservation,
        });
        if snapshot.created.elapsed() >= Duration::from_millis(self.config.snapshot_ttl_ms) {
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
    pub(super) fn snapshot_get(&self, ns: &str, token: &[u8]) -> Result<Arc<Snapshot>, u16> {
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
    pub(super) fn snapshot_release(&self, ns: &str, token: &[u8]) -> Result<(), u16> {
        let snapshot = self.snapshot_get(ns, token)?;
        let id = u64::from_le_bytes(token[8..].try_into().unwrap());
        self.scans.lock().unwrap().snapshots.remove(&id);
        drop(snapshot);
        Ok(())
    }
    pub(super) fn scan_page(
        &self,
        ns: &str,
        c: &mut Cursor<'_>,
        op: u8,
        max_frame: usize,
    ) -> Result<Vec<u8>, u16> {
        let keys_only = matches!(op, SCAN_KEYS_PREFIX | SCAN_KEYS_RANGE);
        let is_range = matches!(op, SCAN_RANGE | SCAN_KEYS_RANGE);
        let supplied = c.bytes(16)?.to_vec();
        let cursor = c.bytes(2 * MAX_ID + 48)?;
        let limit = c.u16()? as usize;
        let budget = (c.u32()? as usize).min(max_frame);
        let lower = c.bound()?;
        let upper = if is_range { c.bound()? } else { String::new() };
        c.end()?;
        if limit == 0
            || limit > MAX_BATCH
            || budget < 42
            || (is_range && !upper.is_empty() && lower > upper)
        {
            return Err(3);
        }
        let mut query = vec![op];
        lp(&mut query, lower.as_bytes());
        if is_range {
            lp(&mut query, upper.as_bytes());
        }
        // A cursor must be accompanied by its snapshot, and binds the full
        // query plus an absolute index. Page size/budget can change on retry.
        let position = if cursor.is_empty() {
            None
        } else {
            if supplied.len() != 16
                || cursor.len() != 32 + query.len()
                || cursor[..16] != supplied
                || cursor[24..cursor.len() - 8] != query
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
            (supplied.clone(), self.snapshot_get(ns, &supplied)?)
        };
        let result = (|| {
            if !cursor.is_empty() {
                let split = cursor.len() - 8;
                let tag = u64::from_le_bytes(cursor[split..].try_into().unwrap());
                if snapshot.cursor_hash.hash_one(&cursor[..split]) != tag {
                    return Err(21);
                }
            }
            let rows = &snapshot.rows;
            let start = rows.partition_point(|(k, _)| k < &lower);
            let end = if !is_range {
                // The matching block is contiguous under UTF-8 byte order.
                start + rows[start..].partition_point(|(k, _)| k.starts_with(&lower))
            } else if upper.is_empty() {
                rows.len()
            } else {
                rows.partition_point(|(k, _)| k < &upper)
            };
            let at = position.unwrap_or(start);
            if at < start || at > end {
                return Err(21);
            }
            let mut next = at;
            let mut item_bytes = 0;
            while next < end && next - at < limit {
                let (key, value) = &rows[next];
                let bytes = 4 + key.len() + if keys_only { 0 } else { 14 + value.bytes.len() };
                let continuation = if next + 1 < end { 32 + query.len() } else { 0 };
                if 42 + item_bytes + bytes + continuation > budget {
                    if next == at {
                        return Err(4);
                    }
                    break;
                }
                item_bytes += bytes;
                next += 1;
            }
            let mut continuation = Vec::new();
            if next < end {
                continuation.extend(&token);
                continuation.extend((next as u64).to_le_bytes());
                continuation.extend(&query);
                let tag = snapshot.cursor_hash.hash_one(&continuation[..]);
                continuation.extend(tag.to_le_bytes());
            }
            let mut p = Vec::with_capacity(27 + item_bytes + continuation.len());
            lp(&mut p, &token);
            lp(&mut p, &continuation);
            p.push((next == end) as u8);
            p.extend(((next - at) as u16).to_le_bytes());
            for (key, value) in &rows[at..next] {
                lp(&mut p, key.as_bytes());
                if !keys_only {
                    encode_value(&mut p, Some(value));
                }
            }
            if snapshot.created.elapsed() >= Duration::from_millis(self.config.snapshot_ttl_ms) {
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

pub(super) fn start_reaper(
    scans: Arc<Mutex<ScanState>>,
    config: &Config,
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
                .retain(|_, s| s.created.elapsed() < ttl);
        }
    });
    (stop, reaper)
}
