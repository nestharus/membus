//! Conditional publication of one complete namespace population. BATCH is independent.
use crate::{
    chunks::Sha256,
    codec::{self, Cursor},
    store::{self, Dedup, Meta},
    Mutation, Outcome, Store, Value,
};
use std::{
    fs::OpenOptions,
    io::{self, Write},
    path::Path,
    sync::{atomic::Ordering, mpsc, Arc},
};

/// Conservative legacy capture profile. Funded capture has no row-count cutoff.
pub const MAX_POPULATION: usize = 4096;
pub const MAX_CHANGES: usize = 128;
include!("atomic_resources.rs");
mod captured_backing;
pub use captured_backing::{BackingCharge, BackingLayout};
#[cfg(test)]
#[path = "atomic_funding_tests.rs"]
mod funding_tests;
#[path = "atomic_result.rs"]
mod result_inspection;
pub use result_inspection::{
    AtomicResultEvidence, AtomicResultInspection, InspectionError, OriginalRef, RetainedAtomicResult,
    RetryRetention, RESULT_INSPECTION_SCRATCH_BYTES,
};
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AtomicAck {
    Os,
    Flush,
}
impl AtomicAck {
    pub(crate) fn byte(self) -> u8 {
        match self {
            Self::Os => 1,
            Self::Flush => 2,
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PublicationError {
    Conflict,
    Expired,
    Unsupported,
    Resource(u16),
    Invalid,
    IdentityMismatch,
    /// The envelope was admitted; absence of a reply or barrier is not absence of effects.
    Uncertain,
    /// No retained original outcome exists in this instance. Never re-decide using a fresh view.
    RestartUncertain,
    RetryExpired,
    Unavailable,
}
#[derive(Clone, Debug)]
pub struct FinalChange {
    pub key: String,
    pub value: Option<Vec<u8>>,
}
impl FinalChange {
    pub fn put(key: &str, value: Vec<u8>) -> Self {
        Self {
            key: key.into(),
            value: Some(value),
        }
    }
    pub fn remove(key: &str) -> Self {
        Self {
            key: key.into(),
            value: None,
        }
    }
}
/// Values and revisions from the same committed population; no paging omissions.
pub struct NamespaceView {
    namespace: String,
    epoch: u64,
    expires_mono: u64,
    rows: Vec<(String, Value)>,
    population: Arc<Population>,
    budget: OriginalBudget,
    _charge: Reservation,
}
impl NamespaceView {
    pub fn rows(&self) -> &[(String, Value)] {
        &self.rows
    }
    pub(crate) fn wire_identity(&self) -> (&str, u64) {
        (&self.namespace, self.epoch)
    }

    fn wire_bytes(&self, size: usize) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(size);
        bytes.extend(b"MAV1");
        crate::lp(&mut bytes, self.namespace.as_bytes());
        bytes.extend(self.epoch.to_le_bytes());
        bytes.extend(self.expires_mono.to_le_bytes());
        bytes.extend((self.rows.len() as u32).to_le_bytes());
        for (key, value) in &self.rows {
            crate::lp(&mut bytes, key.as_bytes());
            bytes.extend(value.version.to_le_bytes());
            bytes.push(value.ack);
            crate::lp(&mut bytes, &value.bytes);
        }
        bytes
    }

    pub(crate) fn from_wire(bytes: &[u8]) -> Result<Self, PublicationError> {
        fn row<'a>(c: &mut Cursor<'a>) -> Result<(&'a str, u64, u8, &'a [u8]), u16> {
            let key = c.bound_ref()?;
            let version = c.u64()?;
            let ack = c.u8()?;
            let value = c.bytes(crate::MAX_VALUE)?;
            if !valid_id(key) || version == 0 || !matches!(ack, 1 | 2) {
                return Err(3);
            }
            Ok((key, version, ack, value))
        }
        fn parse(bytes: &[u8]) -> Result<NamespaceView, u16> {
            if bytes.get(..4) != Some(b"MAV1") {
                return Err(3);
            }
            let mut c = Cursor::new(&bytes[4..]);
            let namespace = c.text_ref()?;
            if !valid_id(namespace) {
                return Err(3);
            }
            let epoch = c.u64()?;
            let expires = c.u64()?;
            let count = c.u32()? as usize;
            if epoch == 0 || expires == 0 || count > MAX_POPULATION || count > c.remainder().len() / 18 {
                return Err(3);
            }
            let encoded_rows = c.remainder();
            let mut previous = None;
            for _ in 0..count {
                let (key, _, _, _) = row(&mut c)?;
                if previous.is_some_and(|prev| prev >= key) {
                    return Err(3);
                }
                previous = Some(key);
            }
            c.end()?;
            let mut c = Cursor::new(encoded_rows);
            let entries = (0..count).map(move |_| {
                let (key, version, ack, value) = row(&mut c).expect("validated wire row");
                (key, value.len(), (version, ack, value))
            });
            let (_, rows, guards) = capture_measure(namespace, entries.clone().map(|(key, len, _)| (key, len)))
                .map_err(|_| 3u16)?;
            capture_complete(namespace, epoch, expires, OriginalBudget::new(rows + guards + crate::MAX_FRAME), entries,
                |_, (version, ack, bytes)| Ok(Value { version, ack, bytes: bytes.to_vec(), tombstone: false }))
                .map_err(|_| 3u16)
        }
        parse(bytes).map_err(|_| PublicationError::Invalid)
    }
    pub fn prepare(
        &self,
        id: &str,
        ack: AtomicAck,
        changes: Vec<FinalChange>,
    ) -> Result<AtomicRequest, PublicationError> {
        if !valid_id(id) || changes.is_empty() || changes.len() > MAX_CHANGES {
            return Err(PublicationError::Invalid);
        }
        let mut encoded = add(64, add(self.namespace.len(), id.len())?)?;
        for (key, _) in &self.population.rows {
            encoded = add(encoded, add(12, key.len())?)?;
        }
        for (i, c) in changes.iter().enumerate() {
            if !valid_id(&c.key)
                || changes[..i].iter().any(|p| p.key == c.key)
                || c.value.as_ref().is_some_and(|v| v.len() > crate::MAX_VALUE)
            {
                return Err(PublicationError::Invalid);
            }
            encoded = add(
                encoded,
                add(9, add(c.key.len(), c.value.as_ref().map_or(0, Vec::len))?)?,
            )?;
        }
        if encoded > crate::MAX_FRAME / 3 {
            return Err(PublicationError::Resource(4));
        }
        let charge = self.budget.reserve(request_charge(
            &self.namespace,
            id,
            changes.iter().map(|c| (c.key.as_str(), c.value.as_deref())),
        )?)?;
        let mut retained = Vec::new();
        retained
            .try_reserve_exact(changes.len())
            .map_err(|_| PublicationError::Resource(7))?;
        retained.extend(changes.iter().map(|c| FinalChange {
            key: c.key.as_str().into(),
            value: c.value.as_deref().map(<[u8]>::to_vec),
        }));
        Ok(AtomicRequest {
            data: Arc::new(OriginalData {
                namespace: self.namespace.clone(),
                operation_id: id.into(),
                epoch: self.epoch,
                expires_mono: self.expires_mono,
                population: self.population.clone(),
                ack,
                changes: retained,
                _charge: charge,
            }),
        })
    }
}
/// Immutable original decision. Persist these exact bytes BEFORE the first call if caller restart matters.
#[derive(Clone, Debug)]
pub struct AtomicRequest {
    data: Arc<OriginalData>,
}

struct OriginalEncoding<'a> {
    namespace: &'a str,
    id: &'a str,
    epoch: u64,
    expires: u64,
    ack: AtomicAck,
    population: &'a [(String, u64)],
    guard: Option<&'a crate::atomic_population::PopulationGuard>,
    changes: &'a [FinalChange],
}
impl OriginalEncoding<'_> {
    fn write(&self, mut write: impl FnMut(&[u8])) {
        fn lp(write: &mut impl FnMut(&[u8]), bytes: &[u8]) {
            write(&(bytes.len() as u32).to_le_bytes());
            write(bytes);
        }
        write(if self.guard.is_some() { b"MAR2" } else { b"MAR1" });
        lp(&mut write, self.namespace.as_bytes());
        lp(&mut write, self.id.as_bytes());
        write(&self.epoch.to_le_bytes());
        write(&self.expires.to_le_bytes());
        write(&[self.ack.byte()]);
        if let Some(guard) = self.guard {
            write(&guard.wire_bytes());
        } else {
            write(&(self.population.len() as u32).to_le_bytes());
            for (key, revision) in self.population {
                lp(&mut write, key.as_bytes());
                write(&revision.to_le_bytes());
            }
        }
        write(&(self.changes.len() as u16).to_le_bytes());
        for change in self.changes {
            lp(&mut write, change.key.as_bytes());
            write(&[change.value.is_none() as u8]);
            lp(&mut write, change.value.as_deref().unwrap_or_default());
        }
    }
}

impl AtomicRequest {
    pub(crate) fn prepare_bounded(
        namespace: &str,
        id: &str,
        epoch: u64,
        expires: u64,
        guard: &crate::atomic_population::PopulationGuard,
        ack: AtomicAck,
        changes: Vec<FinalChange>,
    ) -> Result<Self, PublicationError> {
        if !valid_id(id) || changes.is_empty() || changes.len() > usize::from(guard.limits.max_changes) {
            return Err(PublicationError::Invalid);
        }
        let mut len = 85 + namespace.len() + id.len();
        for change in &changes {
            if !valid_id(&change.key) || change.value.as_ref().is_some_and(|v| v.len() > crate::MAX_VALUE) {
                return Err(PublicationError::Invalid);
            }
            len = add(len, 9 + change.key.len() + change.value.as_ref().map_or(0, Vec::len))?;
        }
        if len > guard.limits.max_original_bytes as usize {
            return Err(PublicationError::Resource(4));
        }
        let mut bytes = Vec::new();
        bytes.try_reserve_exact(len).map_err(|_| PublicationError::Resource(7))?;
        OriginalEncoding {
            namespace, id, epoch, expires, ack, population: &[], guard: Some(guard), changes: &changes,
        }.write(|part| bytes.extend_from_slice(part));
        drop(changes);
        Self::from_bytes(&bytes)
    }
    pub fn operation_id(&self) -> &str {
        &self.data.operation_id
    }
    pub fn namespace(&self) -> &str {
        &self.data.namespace
    }
    /// Exact whole-population comparison, including insertion/removal phantoms.
    /// The caller must obtain the complete ordered interval under its own gate.
    pub fn matches_population<'a>(&'a self, current: impl Iterator<Item = (&'a str, u64)>) -> bool {
        if let Some(guard) = &self.data.population.guard {
            return crate::atomic_population::population_digest(current) == (guard.count, guard.digest);
        }
        current.eq(self
            .data
            .population
            .rows
            .iter()
            .map(|(k, v)| (k.as_str(), *v)))
    }
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut b = Vec::with_capacity(self.encoded_len());
        self.encoding().write(|bytes| b.extend_from_slice(bytes));
        b
    }
    pub(crate) fn encoded_len(&self) -> usize {
        35 + self.data.namespace.len()
            + self.data.operation_id.len()
            + if self.data.population.guard.is_some() { 50 } else { self
                .data
                .population
                .rows
                .iter()
                .map(|(k, _)| 12 + k.len())
                .sum::<usize>() }
            + self
                .data
                .changes
                .iter()
                .map(|c| 9 + c.key.len() + c.value.as_ref().map_or(0, Vec::len))
                .sum::<usize>()
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, PublicationError> {
        let borrowed = BorrowedOriginal::from_canonical_bytes(bytes)?;
        // Compatibility convenience: a bounded per-original account whose
        // ownership is retained by the decoded original. Aggregate callers use
        // from_bytes_funded with their shared account instead.
        let limit = borrowed.allocation_bytes()?;
        borrowed.to_owned(&OriginalBudget::new(limit))
    }
    pub fn from_bytes_funded(
        bytes: &[u8],
        budget: &OriginalBudget,
    ) -> Result<Self, PublicationError> {
        BorrowedOriginal::from_canonical_bytes(bytes)?.to_owned(budget)
    }
    /// New-only caller-owned custody file. Never replaces a possibly sent original.
    pub fn persist_new(&self, path: &Path) -> io::Result<()> {
        let mut f = OpenOptions::new().write(true).create_new(true).open(path)?;
        f.write_all(&self.to_bytes())?;
        crate::log_io::sync_file(&f)?;
        crate::log_io::sync_dir(
            path.parent()
                .ok_or_else(|| io::Error::other("custody requires parent"))?,
        )
    }
    pub(crate) fn fingerprint(&self) -> [u8; 32] {
        let mut h = Sha256::new();
        self.encoding().write(|bytes| h.update(bytes));
        h.finish()
    }
    fn encoding(&self) -> OriginalEncoding<'_> {
        OriginalEncoding {
            namespace: &self.data.namespace,
            id: &self.data.operation_id,
            epoch: self.data.epoch,
            expires: self.data.expires_mono,
            ack: self.data.ack,
            population: &self.data.population.rows,
            guard: self.data.population.guard.as_ref(),
            changes: &self.data.changes,
        }
    }
    pub(crate) fn requested_ack(&self) -> u8 { self.data.ack.byte() }
    fn mutations(&self) -> Vec<Mutation> {
        self.data
            .changes
            .iter()
            .map(|c| Mutation {
                ns: self.data.namespace.clone(),
                key: c.key.clone(),
                op_id: self.data.operation_id.clone(),
                deadline: 0,
                ack: self.data.ack.byte(),
                expected: None,
                bytes: c.value.clone().unwrap_or_default(),
                remove: c.value.is_none(),
            })
            .collect()
    }
}

/// MBK1 framing, versioned multi-entry payload (remove discriminator 2 is reserved).
/// One checksum and one covering barrier bind the original request and ALL subrecords.
pub(crate) struct Envelope<'a> {
    pub request: BorrowedOriginal<'a>,
    pub first: u64,
    pub at: u64,
}
pub(crate) fn is_envelope(b: &[u8]) -> bool {
    b.get(17) == Some(&2)
}
pub(crate) fn envelope_bytes(r: &AtomicRequest, first: u64, at: u64) -> Vec<u8> {
    let mut b = at.to_le_bytes().to_vec();
    b.extend(first.to_le_bytes());
    b.push(r.data.ack.byte());
    b.push(2);
    b.extend(1u16.to_le_bytes());
    crate::lp(&mut b, &r.to_bytes());
    b
}
pub(crate) fn decode_envelope(b: &[u8]) -> Result<Envelope<'_>, u16> {
    let mut c = Cursor::new(b);
    let at = c.u64()?;
    let first = c.u64()?;
    let ack = c.u8()?;
    if first == 0 || c.u8()? != 2 || c.u16()? != 1 {
        return Err(3);
    }
    let request = BorrowedOriginal::from_canonical_bytes(c.bytes(crate::MAX_FRAME / 3)?).map_err(|_| 3u16)?;
    c.end()?;
    if ack != request.ack.byte() || first.checked_add(request.change_count as u64).is_none() {
        return Err(3);
    }
    Ok(Envelope { request, first, at })
}
pub(crate) fn subrecord(b: &[u8], index: u16) -> Result<Vec<u8>, u16> {
    let e = decode_envelope(b)?;
    #[cfg(test)]
    {
        crate::read_work_tests::work(2, 1);
        crate::read_work_tests::work(3, e.request.change_count);
    }
    let (key, value) = e.request.changes().nth(index as usize).ok_or(3u16)?;
    let m = e.request.mutation(key, value);
    Ok(codec::mutation_bytes(&m, e.first + index as u64, e.at))
}

impl<'a> Envelope<'a> {
    /// Visit a frame's records after the same full validation used by a point read.
    pub(crate) fn visit(&self, mut visit: impl FnMut(u16, codec::RecordView<'a>)) {
        let request = &self.request;
        #[cfg(test)]
        {
            crate::read_work_tests::work(2, 1);
            crate::read_work_tests::work(3, request.change_count);
        }
        for (i, (key, value)) in request.changes().enumerate() {
            visit(i as u16, codec::RecordView {
                at: self.at,
                version: self.first + i as u64,
                ack: request.ack.byte(),
                remove: value.is_none(),
                ns: request.namespace,
                key,
                op_id: request.operation_id,
                expected: None,
                bytes: value.unwrap_or_default(),
            });
        }
    }
}

pub(crate) fn route_records(b: &[u8]) -> Result<Vec<(String, String, u64)>, u16> {
    if is_envelope(b) {
        let e = decode_envelope(b)?;
        Ok(e.request
            .changes()
            .enumerate()
            .map(|(i, (k, _))| (e.request.namespace.into(), k.into(), e.first + i as u64))
            .collect())
    } else {
        let (m, v, _) = codec::parse_record(b)?;
        Ok(vec![(m.ns, m.key, v)])
    }
}
pub(crate) fn envelope_stripe(ns: &str) -> usize {
    crate::routing::stripe(ns, "")
}

impl Store {
    pub fn capture_namespace(&self, namespace: &str) -> Result<NamespaceView, PublicationError> {
        self.capture_atomic(namespace, None, None)
    }
    pub(crate) fn capture_namespace_for_wire(&self, namespace: &str, max: usize) -> Result<Vec<u8>, PublicationError> {
        self.capture_atomic(namespace, None, Some(max)).map(|view| view.wire_bytes(
            28 + namespace.len() + view.rows.iter().map(|(key, value)| 17 + key.len() + value.bytes.len()).sum::<usize>()
        ))
    }
    /// Reserve the entire finite account against actual snapshot and memory
    /// headroom before materializing the complete population. Requests prepared
    /// from this view keep the same reservation alive after the view is dropped.
    pub fn capture_namespace_funded(
        &self,
        namespace: &str,
        bytes: usize,
    ) -> Result<NamespaceView, PublicationError> {
        self.capture_atomic(namespace, Some(bytes), None)
    }
    fn capture_atomic(
        &self,
        namespace: &str,
        requested: Option<usize>,
        wire_max: Option<usize>,
    ) -> Result<NamespaceView, PublicationError> {
        valid_namespace(namespace)?;
        if let Some(max) = wire_max {
            Self::atomic_wire_size(namespace, &self.committed.read().unwrap(), max)?;
        }
        let _gate = self.admission_gate.write().unwrap();
        let committed = self.committed.read().unwrap();
        if let Some(max) = wire_max {
            Self::atomic_wire_size(namespace, &committed, max)?;
        }
        if !self.atomic_compatible(namespace, &committed) {
            return Err(PublicationError::Unsupported);
        }
        let entries = committed
            .range((namespace.to_owned(), String::new())..)
            .take_while(|(k, _)| k.0 == namespace)
            .filter(|(_, e)| !e.tombstone)
            .map(|(k, e)| (k.1.as_str(), e.len, e));
        let (count, rows, guards) =
            capture_measure(namespace, entries.clone().map(|(k, n, _)| (k, n)))?;
        if requested.is_none() && count > MAX_POPULATION {
            return Err(PublicationError::Resource(4));
        }
        let minimum = add(rows, guards)?;
        let bytes = match requested {
            Some(n) => n,
            None => add(minimum, crate::MAX_FRAME)?,
        };
        if bytes < minimum {
            return Err(PublicationError::Resource(7));
        }
        // Same coordination as ordinary snapshot reservation: committed->scans->state.
        let scans = self.scans.lock().unwrap();
        let state = self.state.lock().unwrap();
        if state.read_only || state.warming || (wire_max.is_some() && self.flags.storage_error.load(Ordering::SeqCst)) {
            return Err(PublicationError::Unavailable);
        }
        let total = self
            .scan_usage
            .bytes
            .load(Ordering::Acquire)
            .checked_add(bytes)
            .ok_or(PublicationError::Resource(7))?;
        if self.scan_usage.count.load(Ordering::Acquire) >= self.config.snapshot_max
            || total > self.config.snapshot_bytes
            || self.cache_limit(&state) < bytes
        {
            return Err(PublicationError::Resource(7));
        }
        self.scan_usage.bytes.fetch_add(bytes, Ordering::AcqRel);
        self.scan_usage.count.fetch_add(1, Ordering::AcqRel);
        let budget = OriginalBudget::backed(
            bytes,
            Some(crate::snapshots::ScanCharge {
                usage: self.scan_usage.clone(),
                bytes,
            }),
        );
        self.trim_cache(&state);
        drop(state);
        drop(scans);
        capture_complete(
            namespace,
            self.epoch,
            self.clock
                .mono_ms()
                .saturating_add(self.config.snapshot_ttl_ms.min(self.config.retry_ms)),
            budget,
            entries,
            |key, entry| {
                let value = if wire_max.is_some() {
                    self.read_entry(namespace, key, entry)
                } else {
                    entry.read(namespace, key)
                };
                value
                    .map(|v| (*v).clone())
                    .map_err(|_| PublicationError::Uncertain)
            },
        )
    }
    fn atomic_wire_size(namespace: &str, committed: &store::Committed, max: usize) -> Result<(), PublicationError> {
        let mut size = 28 + namespace.len();
        for (i, ((_, key), entry)) in committed.range((namespace.to_owned(), String::new())..)
            .take_while(|((ns, _), _)| ns == namespace).filter(|(_, e)| !e.tombstone).enumerate()
        {
            if !valid_id(key) || entry.version == 0 || !matches!(entry.ack, 1 | 2) || entry.len > crate::MAX_VALUE {
                return Err(PublicationError::Uncertain);
            }
            size = size.checked_add(17 + key.len() + entry.len).ok_or(PublicationError::Resource(4))?;
            if i >= MAX_POPULATION || size > max {
                return Err(PublicationError::Resource(4));
            }
        }
        if size > max {
            return Err(PublicationError::Resource(4));
        }
        Ok(())
    }
    pub(crate) fn atomic_compatible(&self, ns: &str, committed: &store::Committed) -> bool {
        // Initial additive capability supports FOREVER only, including all prefix policies.
        self.policies
            .read()
            .unwrap()
            .rows()
            .iter()
            .all(|(n, _, c)| n != ns || *c == crate::Class::FOREVER)
            && committed
                .iter()
                .filter(|(k, _)| k.0 == ns)
                .all(|(_, e)| e.class == crate::Class::FOREVER)
    }
    pub(crate) fn publish_atomic_for_wire(
        &self, ns: &str, op: &str, bytes: &[u8], deadline: u64,
    ) -> Result<Outcome, PublicationError> {
        let original = BorrowedOriginal::from_canonical_bytes(bytes)?;
        if original.namespace() != ns || original.operation_id() != op {
            return Err(PublicationError::IdentityMismatch);
        }
        let id = (ns.to_owned(), op.to_owned());
        let owned_bytes = original.allocation_bytes()?;
        // Include the pending admission key vector until publication has its own grants.
        let ingress_bytes = original.changes().try_fold(owned_bytes, |total, (key, _)| {
            add(total, 128 + ns.len() + key.len())
        })?;
        let fingerprint = original.fingerprint();
        let _gate = self.admission_gate.write().unwrap();
        let committed = self.committed.read().unwrap();
        let state = self.state.lock().unwrap();
        if let Some(outcome) = self.original_status(
            &state, &id, &fingerprint, original.epoch(), original.expires_mono(), deadline,
        )? {
            return Ok(outcome);
        }
        if !self.atomic_compatible(ns, &committed) {
            return Err(PublicationError::Unsupported);
        }
        if !original.matches_current(live_population(ns, &committed)) {
            return Err(PublicationError::Conflict);
        }
        let _backing = self.reserve_read_backing(&state, ingress_bytes, ingress_bytes)
            .map_err(|_| PublicationError::Resource(7))?;
        drop(state);
        drop(committed);
        let request = original.to_owned(&OriginalBudget::new(owned_bytes))?;
        self.publish_atomic_under_gate(&request, deadline, fingerprint)
    }

    fn original_status(
        &self, state: &store::State, id: &store::Key, fingerprint: &[u8; 32],
        epoch: u64, expires: u64, deadline: u64,
    ) -> Result<Option<Outcome>, PublicationError> {
        let mono = self.clock.mono_ms();
        if let Some(outcome) = retained_outcome(fingerprint, state.dedup.get(id), state.retired_set.contains(id), mono)? {
            return Ok(Some(outcome));
        }
        if epoch != self.epoch {
            return Err(PublicationError::RestartUncertain);
        }
        if self.clock.utc_ms() >= deadline || mono >= expires {
            return Err(PublicationError::Expired);
        }
        if state.read_only || state.warming || self.flags.storage_error.load(Ordering::SeqCst) {
            return Err(PublicationError::Unavailable);
        }
        Ok(None)
    }
    pub fn publish_atomic(
        &self,
        r: &AtomicRequest,
        deadline: u64,
    ) -> Result<Outcome, PublicationError> {
        // Outermost gate: raw writes, whole policy projection, and expiry finish before comparison.
        // No state/index lock is retained while acquiring this gate.
        let _gate = self.admission_gate.write().unwrap();
        self.publish_atomic_under_gate(r, deadline, r.fingerprint())
    }
    fn publish_atomic_under_gate(
        &self, r: &AtomicRequest, deadline: u64, fp: [u8; 32],
    ) -> Result<Outcome, PublicationError> {
        let id = (r.data.namespace.clone(), r.data.operation_id.clone());
        let committed = self.committed.read().unwrap();
        let mut state = self.state.lock().unwrap();
        if let Some(outcome) = self.original_status(
            &state, &id, &fp, r.data.epoch, r.data.expires_mono, deadline,
        )? {
            return Ok(outcome);
        }
        if !self.atomic_compatible(&r.data.namespace, &committed) {
            return Err(PublicationError::Unsupported);
        }
        if !r.matches_population(live_population(&r.data.namespace, &committed)) {
            return Err(PublicationError::Conflict);
        }
        let keys: Vec<_> = r
            .data
            .changes
            .iter()
            .map(|m| (r.data.namespace.clone(), m.key.clone()))
            .collect();
        let new_count = keys
            .iter()
            .filter(|k| !state.metadata.contains_key(*k))
            .count();
        let mut final_live = state
            .namespace_counts
            .get(&r.data.namespace)
            .copied()
            .unwrap_or(0);
        for (k, m) in keys.iter().zip(&r.data.changes) {
            let live = state.metadata.get(k).is_some_and(|v| !v.tombstone);
            if m.value.is_none() && live {
                final_live -= 1;
            } else if m.value.is_some() && !live {
                final_live += 1;
            }
        }
        if state.keys.saturating_add(new_count) > self.config.index_cap {
            return Err(PublicationError::Resource(9));
        }
        if final_live > self.config.quota {
            return Err(PublicationError::Resource(8));
        }
        // Use the same bounded expired-identity ring as ordinary admission.
        let mono = self.clock.mono_ms();
        while let Some((key, until)) = state.expiry.front().cloned() {
            if mono <= until
                || state.policy_publishing.contains(&key)
                || state.dedup.get(&key).is_some_and(|d| d.outcome.is_none())
            {
                break;
            }
            state.expiry.pop_front();
            if state.dedup.remove(&key).is_some() {
                state.retired_set.insert(key.clone());
                state.retired.push_back(key);
            }
        }
        while state.retired.len() > self.config.dedup_cap {
            let key = state.retired.pop_front().unwrap();
            state.retired_set.remove(&key);
            state.dedup_bytes -= store::dedup_charge(&key);
        }
        if state.dedup.len() >= self.config.dedup_cap {
            return Err(PublicationError::Resource(18));
        }
        let first = state.next;
        let next = first
            .checked_add(r.data.changes.len() as u64)
            .ok_or(PublicationError::Resource(7))?;
        let at = self.clock.utc_ms();
        let payload_len = 24 + r.encoded_len();
        if payload_len > crate::MAX_FRAME {
            return Err(PublicationError::Resource(4));
        }
        let disk = (16 + payload_len) as u64;
        let queue = payload_len
            + r.data
                .changes
                .iter()
                .map(|m| m.value.as_ref().map_or(0, Vec::len))
                .sum::<usize>()
            + 128 * r.data.changes.len();
        // Split chunk registration out of the envelope/decode/publication,
        // latest-index and queue reservation so it can transfer at append time.
        let index: usize = keys.iter().map(store::index_charge).sum();
        let chunk_index_bytes: usize = keys.iter().map(crate::chunks::chunk_index_charge).sum();
        let memory = 4 * queue + 2 * index + 4096;
        let log_index = envelope_stripe(&r.data.namespace);
        if state.reserved_bytes.saturating_add(queue) > self.config.queue_bytes
            || state.stripe_reserved[log_index].saturating_add(queue) > self.config.queue_bytes / 4
            || state.stripe_items[log_index] >= 1024
        {
            return Err(PublicationError::Resource(6));
        }
        if self.cache_limit(&state) < memory + chunk_index_bytes + store::dedup_charge(&id) {
            return Err(PublicationError::Resource(7));
        }
        if state
            .persisted_bytes
            .saturating_add(state.pending_disk)
            .saturating_add(disk)
            > self
                .config
                .state_limit_bytes
                .saturating_sub(self.config.maintenance_reserve_bytes)
        {
            return Err(PublicationError::Resource(7));
        }
        if self.clock.utc_ms() >= deadline || self.clock.mono_ms() >= r.data.expires_mono {
            return Err(PublicationError::Expired);
        }
        // Admission: every remaining failure is uncertainty, with original identity retained.
        let until = self.clock.mono_ms().saturating_add(self.config.retry_ms);
        state.next = next;
        self.next_issued.fetch_max(next, Ordering::SeqCst);
        state.reserved_bytes += queue;
        state.memory_reserved += memory;
        state.pending_disk += disk;
        state.stripe_reserved[log_index] += queue;
        state.stripe_items[log_index] += 1;
        state.dedup_bytes += store::dedup_charge(&id);
        state.dedup.insert(
            id.clone(),
            Dedup {
                fingerprint: fp,
                at,
                until_mono: until,
                replayed_chunk: 0,
                outcome: None,
            },
        );
        state.expiry.push_back((id.clone(), until));
        for (i, (k, m)) in keys.iter().zip(&r.data.changes).enumerate() {
            state.pending.insert(
                k.clone(),
                Meta {
                    version: first + i as u64,
                    tombstone: m.value.is_none(),
                    bytes: store::index_charge(k),
                    chunk: 0,
                },
            );
        }
        let chunk_index = crate::chunks::ChunkIndexReservation::reserve(&self.chunks, chunk_index_bytes);
        self.trim_cache(&state);
        drop(state);
        drop(committed);
        // Payload/peer-value allocations follow whole-request memory reservation.
        let payload = envelope_bytes(r, first, at);
        debug_assert_eq!(payload.len(), payload_len);
        let mutations = r.mutations();
        let mut record = b"MBK1".to_vec();
        record.extend((payload.len() as u32).to_le_bytes());
        record.extend((!(payload.len() as u32)).to_le_bytes());
        record.extend(crate::crc32(&payload).to_le_bytes());
        record.extend(payload);
        let (done, completion) = mpsc::channel();
        let result = self.appenders[log_index]
            .sender
            .as_ref()
            .ok_or_else(|| io::Error::other("stopped append owner"))
            .and_then(|sender| {
                sender
                    .send(crate::append_owner::Append {
                        record,
                        ack: r.data.ack.byte(),
                        chunk_index: Some(chunk_index.clone()),
                        done,
                    })
                    .map_err(|_| io::Error::other("stopped append owner"))
            })
            .and_then(|_| {
                completion
                    .recv()
                    .unwrap_or_else(|_| Err(io::Error::other("append completion lost")))
            });
        let locations: Vec<_> = {
            let mut locations = self.router.locations.lock().unwrap();
            (0..mutations.len())
                .map(|i| locations.remove(&(first + i as u64)))
                .collect()
        };
        let chunk = result.as_ref().copied().ok();
        let mut committed = self.committed.write().unwrap();
        let mut state = self.state.lock().unwrap();
        let mut remaining_memory = memory;
        let success = result.is_ok() && locations.iter().all(Option::is_some);
        if success {
            // One index publication. Readers observe the previous or the complete final population.
            for (i, ((k, m), location)) in keys.iter().zip(&mutations).zip(locations).enumerate() {
                let v = first + i as u64;
                let entry = Arc::new(crate::value_disk::Entry::new(m, v, location.unwrap()));
                if let Some(old) = committed.insert(k.clone(), entry) {
                    old.cacheable.store(false, Ordering::Release);
                    self.cache.remove(old.version, false);
                }
                if !state.metadata.contains_key(k) {
                    let charge = store::index_charge(k);
                    state.keys += 1;
                    // Move the latest-key charge out of the pending allocation.
                    state.memory_reserved -= charge;
                    remaining_memory -= charge;
                    state.committed_bytes += charge;
                }
                state.metadata.insert(
                    k.clone(),
                    Meta {
                        version: v,
                        tombstone: m.remove,
                        bytes: store::index_charge(k),
                        chunk: chunk.unwrap(),
                    },
                );
                if m.remove {
                    state.tomb_candidates.insert(k.clone());
                }
            }
            if final_live == 0 {
                state.namespace_counts.remove(&r.data.namespace);
            } else {
                state
                    .namespace_counts
                    .insert(r.data.namespace.clone(), final_live);
            }
        } else {
            state.read_only = true;
            self.flags.storage_error.store(true, Ordering::SeqCst);
        }
        for k in &keys {
            state.pending.remove(k);
        }
        state.reserved_bytes -= queue;
        state.memory_reserved -= remaining_memory;
        drop(chunk_index);
        state.pending_disk -= disk;
        state.stripe_reserved[log_index] -= queue;
        state.stripe_items[log_index] -= 1;
        state.persisted_bytes += disk;
        let outcome = if success {
            Outcome {
                code: 0,
                version: first,
                ack: r.data.ack.byte(),
            }
        } else {
            Outcome::error(16)
        };
        let d = state.dedup.get_mut(&id).unwrap();
        d.outcome = Some(outcome.clone());
        d.replayed_chunk = chunk.unwrap_or(0);
        if success {
            let budget = self.signal_budget(&state);
            for (i, m) in mutations.iter().enumerate() {
                self.signals.publish(
                    &m.ns,
                    &m.key,
                    Arc::new(Value {
                        version: first + i as u64,
                        ack: m.ack,
                        bytes: m.bytes.clone(),
                        tombstone: m.remove,
                    }),
                    &budget,
                );
            }
        }
        self.trim_cache(&state);
        self.done.notify_all();
        drop(state);
        drop(committed);
        if success {
            Ok(outcome)
        } else {
            Err(PublicationError::Uncertain)
        }
    }
}
