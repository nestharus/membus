//! Explicit, bounded ownership of validated record interpretations.
use crate::{codec, snapshots::ScanCharge, value_disk::{Entry, Location}, Store, Value};
use std::{io, ops::Deref, sync::{Arc, Mutex, atomic::{AtomicBool, AtomicUsize, Ordering}}};

const OWNER_BYTES: usize = 512;

#[derive(Default)]
pub(crate) struct ReadUsage {
    pub(crate) bytes: AtomicUsize,
    pub(crate) released: AtomicBool,
}
struct Budget {
    usage: Arc<ReadUsage>,
    limit: usize,
    used: AtomicUsize,
}
impl Drop for Budget {
    fn drop(&mut self) {
        self.usage.bytes.fetch_sub(self.limit, Ordering::AcqRel);
        self.usage.released.store(true, Ordering::Release);
    }
}
enum FrameCharge {
    Cohort { budget: Arc<Budget>, bytes: usize },
    Snapshot { owner: Arc<ScanCharge>, bytes: usize },
}
impl Drop for FrameCharge {
    fn drop(&mut self) {
        match self {
            Self::Cohort { budget, bytes } => { budget.used.fetch_sub(*bytes, Ordering::AcqRel); }
            Self::Snapshot { owner, bytes } => {
                owner.usage.bytes.fetch_sub(*bytes, Ordering::AcqRel);
                owner.usage.released.store(true, Ordering::Release);
            }
        }
    }
}
struct Member {
    namespace: Arc<str>,
    key: String,
    value: Arc<Value>,
}
struct InterpretedFrame {
    location: Location,
    _encoded: Vec<u8>,
    members: Vec<Member>,
    // Drop payloads before returning their resource allowance.
    _charge: FrameCharge,
}
impl InterpretedFrame {
    fn matches(&self, entry: &Entry) -> bool {
        self.location.same_frame(&entry.location)
            && self.members.get(entry.location.subrecord.map_or(0, usize::from))
                .is_some_and(|m| m.value.version == entry.version)
    }
    fn select(self: &Arc<Self>, ns: &str, key: &str, entry: &Entry) -> io::Result<ReadValue> {
        if !self.location.same_frame(&entry.location) {
            return Err(io::Error::other("frame source changed"));
        }
        let index = entry.location.subrecord.map_or(0, usize::from);
        let member = self.members.get(index).ok_or_else(|| io::Error::other("missing subrecord"))?;
        let v = &member.value;
        entry.check_identity(ns, key, (&member.namespace, &member.key, v.version, v.ack, v.tombstone, v.bytes.len()))?;
        Ok(ReadValue { value: v.clone(), _owner: ValueOwner::Frame { _frame: self.clone() } })
    }
}

#[derive(Clone)]
enum ValueOwner {
    Frame { _frame: Arc<InterpretedFrame> },
    Snapshot { _snapshot: Arc<ScanCharge> },
}
/// An immutable value and its retained-resource ownership. Cloning preserves the charge.
/// Copying Value explicitly creates a separate caller-owned allocation.
#[derive(Clone)]
pub struct ReadValue {
    pub(crate) value: Arc<Value>,
    _owner: ValueOwner,
}
impl Deref for ReadValue {
    type Target = Value;
    fn deref(&self) -> &Value { &self.value }
}
impl ReadValue {
    pub(crate) fn snapshot(value: Arc<Value>, owner: Arc<ScanCharge>) -> Self {
        Self { value, _owner: ValueOwner::Snapshot { _snapshot: owner } }
    }
}

/// An opt-in point-read lifetime. Each call selects the current committed Entry.
/// One frame is retained for reuse; outstanding values may keep evicted frames alive.
/// This object does not capture a namespace or a whole-store generation.
pub struct ReadCohort<'a> {
    store: &'a Store,
    frame: Mutex<Option<Arc<InterpretedFrame>>>,
    budget: Arc<Budget>,
}
impl Store {
    /// Reserve a finite managed-memory allowance before creating the cohort.
    /// Capacity refusal is WouldBlock. Zero and undersized allowances are invalid.
    pub fn read_cohort(&self, byte_budget: usize) -> io::Result<ReadCohort<'_>> {
        if byte_budget <= OWNER_BYTES {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "read cohort budget too small"));
        }
        let state = self.state.lock().unwrap();
        if byte_budget > self.cache_limit(&state) { return Err(capacity()); }
        self.read_usage.bytes.fetch_add(byte_budget, Ordering::AcqRel);
        self.trim_cache(&state);
        Ok(ReadCohort { store: self, frame: Mutex::new(None), budget: Arc::new(Budget {
            usage: self.read_usage.clone(), limit: byte_budget, used: AtomicUsize::new(OWNER_BYTES),
        }) })
    }
    /// Reserved cohort bytes, including values still alive after cohort release.
    pub fn read_cohort_bytes(&self) -> usize { self.read_usage.bytes.load(Ordering::Acquire) }
}
impl ReadCohort<'_> {
    /// Evict the retained frame. Returned values keep their ownership until dropped.
    pub fn clear(&self) { self.frame.lock().unwrap().take(); }
    pub fn try_get(&self, namespace: &str, key: &str) -> io::Result<Option<ReadValue>> {
        let entry = crate::store::committed_get(&self.store.committed.read().unwrap(), namespace, key)
            .filter(|e| !e.tombstone).cloned();
        entry.map(|entry| self.read(namespace, key, &entry)).transpose()
    }
    fn read(&self, ns: &str, key: &str, entry: &Entry) -> io::Result<ReadValue> {
        let mut retained = self.frame.lock().unwrap();
        let result = (|| {
            if let Some(frame) = retained.as_ref().filter(|f| f.matches(entry)) {
                return frame.select(ns, key, entry);
            }
            retained.take();
            let bytes = frame_charge(&entry.location)?;
            self.budget.used.try_update(Ordering::AcqRel, Ordering::Acquire, |used| {
                used.checked_add(bytes).filter(|total| *total <= self.budget.limit)
            }).map_err(|_| capacity())?;
            let charge = FrameCharge::Cohort { budget: self.budget.clone(), bytes };
            let frame = interpret(self.store, &entry.location, charge)?;
            let value = frame.select(ns, key, entry)?;
            *retained = Some(frame);
            Ok(value)
        })();
        storage_observation(self.store, &result);
        result
    }
}

fn capacity() -> io::Error { io::Error::new(io::ErrorKind::WouldBlock, "read workspace capacity") }
pub(crate) fn frame_charge(location: &Location) -> io::Result<usize> {
    let n = usize::try_from(location.record_len).map_err(|_| capacity())?;
    if n < 16 || n > crate::MAX_FRAME + 16 { return Err(io::Error::other("invalid indexed extent")); }
    // Encoded bytes, owned payloads/keys, growth/validation temporaries, and
    // conservative member/Arc/index bookkeeping for the existing MAR1 bound.
    n.checked_mul(4).and_then(|n| n.checked_add(crate::atomic::MAX_CHANGES * 256 + 4096))
        .ok_or_else(capacity)
}
fn storage_observation<T>(store: &Store, result: &io::Result<T>) {
    if result.as_ref().is_err_and(|e| e.kind() != io::ErrorKind::WouldBlock) {
        store.flags.storage_error.store(true, Ordering::SeqCst);
    }
}
fn interpret(store: &Store, location: &Location, charge: FrameCharge) -> io::Result<Arc<InterpretedFrame>> {
    let _reader = store.cold_reader();
    store.cache.cold();
    let encoded = location.whole_payload()?;
    let mut members = Vec::new();
    let mut namespace: Option<Arc<str>> = None;
    let mut append = |record: codec::RecordView<'_>| {
        let ns = namespace.get_or_insert_with(|| Arc::from(record.ns)).clone();
        members.push(Member { namespace: ns, key: record.key.into(), value: Arc::new(Value {
            version: record.version, ack: record.ack, tombstone: record.remove, bytes: record.bytes.to_vec(),
        }) });
    };
    if location.subrecord.is_some() {
        crate::atomic::visit_subrecords(&encoded, |_, record| append(record))
            .map_err(|_| io::Error::other("invalid envelope subrecord"))?;
    } else {
        append(codec::parse_record_view(&encoded).map_err(|_| io::Error::other("invalid indexed record"))?);
    }
    Ok(Arc::new(InterpretedFrame { location: location.clone(), _encoded: encoded, members, _charge: charge }))
}

/// Workspace ownership belongs to one ordinary snapshot, independently of live cache policy.
pub(crate) struct SnapshotWorkspace {
    frames: Mutex<Vec<Arc<InterpretedFrame>>>,
    pub(crate) owner: Arc<ScanCharge>,
}
impl SnapshotWorkspace {
    pub(crate) fn new(owner: Arc<ScanCharge>) -> Self { Self { frames: Mutex::new(Vec::new()), owner } }
    pub(crate) fn read(&self, store: &Store, ns: &str, key: &str, entry: &Entry) -> Result<ReadValue, u16> {
        let mut retained = self.frames.lock().unwrap();
        let result = (|| {
            if let Some(frame) = retained.iter().find(|f| f.matches(entry)) {
                return frame.select(ns, key, entry);
            }
            let bytes = frame_charge(&entry.location)?;
            let state = store.state.lock().unwrap();
            if self.owner.usage.bytes.load(Ordering::Acquire).checked_add(bytes)
                .is_none_or(|total| total > store.config.snapshot_bytes) || bytes > store.cache_limit(&state) {
                return Err(capacity());
            }
            self.owner.usage.bytes.fetch_add(bytes, Ordering::AcqRel);
            let charge = FrameCharge::Snapshot { owner: self.owner.clone(), bytes };
            store.trim_cache(&state);
            drop(state);
            let frame = interpret(store, &entry.location, charge)?;
            let value = frame.select(ns, key, entry)?;
            retained.push(frame);
            Ok(value)
        })();
        storage_observation(store, &result);
        result.map_err(|e| if e.kind() == io::ErrorKind::WouldBlock { 22 } else { 16 })
    }
}

#[cfg(test)]
pub(crate) mod tests;

