//! Budgeted acquisition of one immutable selected committed population.
use crate::snapshots::{ScanCharge, Snapshot, SnapshotValue};
use crate::{Store, MAX_ID};
use std::collections::hash_map::RandomState;
use std::sync::{atomic::Ordering, Arc};

/// UTF-8 lexical selection. Range is half-open; an empty prefix selects all keys.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Selection<'a> {
    Prefix(&'a str),
    Range { lower: &'a str, upper: &'a str },
}
impl Selection<'_> {
    pub(crate) fn valid(self) -> bool {
        match self {
            Self::Prefix(p) => p.len() <= MAX_ID,
            Self::Range { lower, upper } => {
                lower.len() <= MAX_ID && upper.len() <= MAX_ID && lower < upper
            }
        }
    }
    fn bytes(self) -> usize {
        match self {
            Self::Prefix(p) => p.len(),
            Self::Range { lower, upper } => lower.len() + upper.len(),
        }
    }
    // None ends the ordered walk; false is examined but outside the selection.
    fn includes(self, ns: &str, namespace: &str, key: &str) -> Option<bool> {
        if namespace < ns {
            return Some(false);
        }
        if namespace > ns {
            return None;
        }
        match self {
            Self::Prefix(p) if key < p => Some(false),
            Self::Prefix(p) if key.starts_with(p) => Some(true),
            Self::Prefix(_) => None,
            Self::Range { upper, .. } if key >= upper => None,
            Self::Range { lower, .. } => Some(key >= lower),
        }
    }
}

/// Independent caller ceilings for BOTH acquisition passes, not output paging.
/// See docs/SELECTED-VIEW.md for the exact sequential traversal and byte units.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AcquisitionBudget {
    pub max_candidates: u64,
    pub max_metadata_bytes: u64,
    pub max_retained_bytes: u64,
}

/// A successful metadata capture, not payload validation or atomic COMPLETE authority.
/// The token owns resident capacity until explicit snapshot_release or TTL expiry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SelectedView {
    pub snapshot: [u8; 16],
    pub population: u64,
    pub candidates: u64,
    pub metadata_bytes: u64,
    pub retained_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcquisitionError {
    Invalid,
    WorkBudget,
    RetainedBudget,
    SnapshotFull,
    Expired,
    ReadOnly,
    Warming,
    StorageUnavailable,
}
impl AcquisitionError {
    pub fn code(self) -> u16 {
        match self {
            Self::Invalid => 3,
            Self::WorkBudget | Self::RetainedBudget => 4,
            Self::SnapshotFull => 22,
            Self::Expired => 20,
            Self::ReadOnly => 10,
            Self::Warming => 11,
            Self::StorageUnavailable => 16,
        }
    }
}
impl std::fmt::Display for AcquisitionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for AcquisitionError {}

pub(crate) enum CapturedSelection {
    Prefix(String),
    Range(String, String),
}
impl CapturedSelection {
    pub(crate) fn matches(&self, range: bool, lower: &str, upper: &str) -> bool {
        match self {
            Self::Prefix(p) => !range && p == lower,
            Self::Range(lo, hi) => range && lo == lower && hi == upper,
        }
    }
}
impl From<Selection<'_>> for CapturedSelection {
    fn from(s: Selection<'_>) -> Self {
        match s {
            Selection::Prefix(p) => Self::Prefix(p.to_owned()),
            Selection::Range { lower, upper } => Self::Range(lower.to_owned(), upper.to_owned()),
        }
    }
}

impl Store {
    /// Acquire selected keys/revisions under one committed-index lock. No values
    /// are read here. Use the existing scans with these exact selection bounds.
    /// Failure returns no token or partial population. This call does not retry.
    pub fn snapshot_open_selected(
        &self,
        ns: &str,
        selection: Selection<'_>,
        budget: AcquisitionBudget,
    ) -> Result<SelectedView, AcquisitionError> {
        use AcquisitionError::*;
        if ns.is_empty()
            || ns.len() > MAX_ID
            || ns == crate::timers::NS
            || ns == crate::chunks::POLICY_NS
            || !selection.valid()
        {
            return Err(Invalid);
        }
        let created = self.clock.mono_ms();
        #[cfg(test)]
        self.capture_waiters.fetch_add(1, Ordering::SeqCst);
        let committed = self.committed.read().unwrap();
        #[cfg(test)]
        self.capture_waiters.fetch_sub(1, Ordering::SeqCst);
        let expired =
            || self.clock.mono_ms().saturating_sub(created) >= self.config.snapshot_ttl_ms;
        if expired() {
            return Err(Expired);
        }
        if self.flags.storage_error.load(Ordering::Acquire) {
            return Err(StorageUnavailable);
        }

        let mut charge = 256 + ns.len() + selection.bytes();
        if charge as u64 > budget.max_retained_bytes {
            return Err(RetainedBudget);
        }
        let (mut visits, mut metadata, mut population) = (0u64, 0u64, 0usize);
        // No unmetered BTree range seek/comparisons. Walk from the first entry,
        // charging preceding namespaces/keys, tombstones and the stop witness.
        // Reserve each visit's second-pass work BEFORE inspecting the first.
        let mut entries = committed.iter();
        while entries.len() != 0 {
            visits = visits
                .checked_add(2)
                .filter(|n| *n <= budget.max_candidates)
                .ok_or(WorkBudget)?;
            let fixed = metadata
                .checked_add(256)
                .filter(|n| *n <= budget.max_metadata_bytes)
                .ok_or(WorkBudget)?;
            let ((namespace, key), entry) = entries.next().unwrap();
            #[cfg(test)]
            self.capture_visits.fetch_add(1, Ordering::Relaxed);
            metadata = fixed
                .checked_add(2 * (namespace.len() + key.len()) as u64)
                .filter(|n| *n <= budget.max_metadata_bytes)
                .ok_or(WorkBudget)?;
            let Some(included) = selection.includes(ns, namespace, key) else {
                break;
            };
            if included && !entry.tombstone {
                charge = charge
                    .checked_add(128)
                    .and_then(|n| n.checked_add(key.len()))
                    .and_then(|n| n.checked_add(entry.len))
                    .ok_or(RetainedBudget)?;
                if charge as u64 > budget.max_retained_bytes {
                    return Err(RetainedBudget);
                }
                population += 1;
            }
        }
        let mut scans = self.scans.lock().unwrap();
        self.purge_scans(&mut scans);
        let state = self.state.lock().unwrap();
        if state.read_only {
            return Err(ReadOnly);
        }
        if state.warming {
            return Err(Warming);
        }
        if self.flags.storage_error.load(Ordering::Acquire) {
            return Err(StorageUnavailable);
        }
        if expired() {
            return Err(Expired);
        }
        let total = self
            .scan_usage
            .bytes
            .load(Ordering::Acquire)
            .checked_add(charge)
            .ok_or(SnapshotFull)?;
        if self.scan_usage.count.load(Ordering::Acquire) >= self.config.snapshot_max
            || total > self.config.snapshot_bytes
            || charge > self.cache_limit(&state)
        {
            return Err(SnapshotFull);
        }
        let id = scans.next;
        scans.next = id.checked_add(1).ok_or(SnapshotFull)?;
        self.scan_usage.bytes.fetch_add(charge, Ordering::AcqRel);
        self.scan_usage.count.fetch_add(1, Ordering::AcqRel);
        let reservation = Arc::new(ScanCharge {
            usage: self.scan_usage.clone(),
            bytes: charge,
        });
        self.trim_cache(&state);
        drop(state);
        drop(scans);
        let workspace = Arc::new(crate::read_cohort::SnapshotWorkspace::new(
            reservation.clone(),
        ));

        // Everything retained, including the selector, is allocated after Store
        // admission. The same read lock makes the reserved second pass exact.
        #[cfg(test)]
        tests::before_materialization(self);
        let mut rows = Vec::new();
        rows.try_reserve_exact(population)
            .map_err(|_| SnapshotFull)?;
        #[cfg(test)]
        tests::materialized(self, &rows, rows.capacity(), None);
        for ((namespace, key), entry) in committed.iter().take((visits / 2) as usize) {
            #[cfg(test)]
            self.capture_visits.fetch_add(1, Ordering::Relaxed);
            if selection.includes(ns, namespace, key) == Some(true) && !entry.tombstone {
                rows.push((
                    key.clone(),
                    SnapshotValue::new(entry.clone(), workspace.clone()),
                ));
                #[cfg(test)]
                tests::materialized(self, &rows, rows.capacity(), None);
            }
        }
        drop(committed);
        let snapshot = Arc::new(Snapshot {
            ns: ns.to_owned(),
            created,
            rows,
            selection: Some(selection.into()),
            cursor_hash: RandomState::new(),
            complete: true,
            bounded_load: None,
            _charge: reservation,
        });
        #[cfg(test)]
        tests::materialized(
            self,
            &snapshot.rows,
            snapshot.rows.capacity(),
            Some(&snapshot),
        );
        #[cfg(test)]
        tests::before_publish();
        let mut scans = self.scans.lock().unwrap();
        if expired() {
            return Err(Expired);
        }
        if self.flags.storage_error.load(Ordering::Acquire) {
            return Err(StorageUnavailable);
        }
        scans.snapshots.insert(id, snapshot);
        let mut token = [0; 16];
        token[..8].copy_from_slice(&self.epoch.to_le_bytes());
        token[8..].copy_from_slice(&id.to_le_bytes());
        Ok(SelectedView {
            snapshot: token,
            population: population as u64,
            candidates: visits,
            metadata_bytes: metadata,
            retained_bytes: charge as u64,
        })
    }
}

#[cfg(test)]
mod tests;
