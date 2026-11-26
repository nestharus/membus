//! Read-only observation of the existing original retry evidence.
use super::{AtomicAck, AtomicRequest, BorrowedOriginal, OriginalBudget};
use crate::{store::Key, Store};
use std::{
    borrow::Borrow,
    hash::{Hash, Hasher},
};

/// Conservative managed allowance for the existing SHA partial-block/padding buffers.
pub const RESULT_INSPECTION_SCRATCH_BYTES: usize = 256;

/// An existing validated provider original, without serializing or owning a copy.
/// Neither variant carries domain provenance or permission to repeat an effect.
#[derive(Clone, Copy)]
pub enum OriginalRef<'a> {
    Request(&'a AtomicRequest),
    Borrowed(&'a BorrowedOriginal<'a>),
}
impl<'a> OriginalRef<'a> {
    fn namespace(self) -> &'a str {
        match self {
            Self::Request(r) => r.namespace(),
            Self::Borrowed(r) => r.namespace(),
        }
    }
    fn operation_id(self) -> &'a str {
        match self {
            Self::Request(r) => r.operation_id(),
            Self::Borrowed(r) => r.operation_id(),
        }
    }
    fn epoch(self) -> u64 {
        match self {
            Self::Request(r) => r.data.epoch,
            Self::Borrowed(r) => r.epoch(),
        }
    }
    fn expires_mono(self) -> u64 {
        match self {
            Self::Request(r) => r.data.expires_mono,
            Self::Borrowed(r) => r.expires_mono(),
        }
    }
    fn requested_ack(self) -> AtomicAck {
        match self {
            Self::Request(r) => r.data.ack,
            Self::Borrowed(r) => r.requested_ack(),
        }
    }
    fn change_count(self) -> usize {
        match self {
            Self::Request(r) => r.data.changes.len(),
            Self::Borrowed(r) => r.change_count(),
        }
    }
    fn fingerprint(self) -> [u8; 32] {
        match self {
            Self::Request(r) => r.fingerprint(),
            Self::Borrowed(r) => r.fingerprint(),
        }
    }
}

/// Local retry retention is inclusive at its exact cutoff, as for publication.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetryRetention {
    WithinWindow,
    Expired,
}

/// Recorded evidence, including historical evidence outside the retry window.
/// None of these variants is a present serving or external-effect grant.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetainedAtomicResult {
    Committed {
        version: u64,
        achieved: AtomicAck,
    },
    /// An admitted identity has no settled outcome. Work may already have effects.
    Pending,
    /// A retained non-success code; it does not prove absence of effects.
    Uncertain {
        recorded_code: u16,
    },
    /// A success-shaped outcome cannot be associated with this original's Ack/revisions.
    InvalidOutcome,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AtomicResultEvidence {
    Matching {
        recorded_at_utc_ms: u64,
        retry_until_mono_ms: u64,
        retention: RetryRetention,
        result: RetainedAtomicResult,
    },
    IdentityMismatch,
    /// The bounded anti-resurrection ring names this namespace/operation ID.
    /// It no longer retains a fingerprint, result or exact cutoff for comparison.
    RetiredIdentity,
    /// No matching identity record or retirement marker is currently retained.
    /// Never sent, refused, evicted and expired history cannot be distinguished.
    NotRetained,
}

/// One read of retry state, not an admission/reconciliation decision.
/// The original's monotonic horizon is comparable only in its original epoch.
/// Epoch equality cannot authenticate a restored clone or endpoint incarnation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AtomicResultInspection {
    pub store_epoch: u64,
    pub original_epoch: u64,
    pub original_expires_mono_ms: u64,
    pub observed_mono_ms: u64,
    pub fingerprint: [u8; 32],
    pub evidence: AtomicResultEvidence,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InspectionError {
    /// The caller's existing allocation account cannot fund bounded hash scratch.
    ScratchUnavailable,
    /// The state lock is poisoned. The query does not clear or repair it.
    StateUnavailable,
}
impl std::fmt::Display for InspectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for InspectionError {}

// Borrow a tuple key for the existing HashMap/HashSet without allocating two
// Strings or walking the tables. Tuple<str,str> hashing and equality agree with
// the stored tuple<String,String>, including each string's hash boundary.
trait Identity {
    fn parts(&self) -> (&str, &str);
}
impl Identity for Key {
    fn parts(&self) -> (&str, &str) {
        (&self.0, &self.1)
    }
}
impl Identity for (&str, &str) {
    fn parts(&self) -> (&str, &str) {
        (self.0, self.1)
    }
}
impl<'a> Borrow<dyn Identity + 'a> for Key {
    fn borrow(&self) -> &(dyn Identity + 'a) {
        self
    }
}
impl PartialEq for dyn Identity + '_ {
    fn eq(&self, other: &Self) -> bool {
        self.parts() == other.parts()
    }
}
impl Eq for dyn Identity + '_ {}
impl Hash for dyn Identity + '_ {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.parts().hash(h);
    }
}

impl Store {
    /// Inspect only currently retained evidence for this exact original.
    /// Streams its existing canonical fingerprint, then takes only the state
    /// lock. Does not wait for pending publication, admit on a miss, perform I/O,
    /// prune/expire retries, change flags, or acquire a new original/view grant.
    /// The supplied caller account must fund RESULT_INSPECTION_SCRATCH_BYTES;
    /// that transient reservation is returned even on a query error.
    /// Missing/expired/mismatched/unavailable evidence never permits a new effect.
    pub fn inspect_atomic_result(
        &self,
        original: OriginalRef<'_>,
        scratch: &OriginalBudget,
    ) -> Result<AtomicResultInspection, InspectionError> {
        // Caller-owned transient accounting only: no Store reservation or retry
        // admission. Reserve before hashing and release on every return path.
        let _scratch = scratch
            .reserve(RESULT_INSPECTION_SCRATCH_BYTES)
            .map_err(|_| InspectionError::ScratchUnavailable)?;
        let fingerprint = original.fingerprint();
        let key = (original.namespace(), original.operation_id());
        let identity = &key as &dyn Identity;
        let state = self
            .state
            .lock()
            .map_err(|_| InspectionError::StateUnavailable)?;
        let observed_mono_ms = self.clock.mono_ms();
        let evidence = if let Some(record) = state.dedup.get(identity) {
            if record.fingerprint != fingerprint {
                AtomicResultEvidence::IdentityMismatch
            } else {
                let result = match &record.outcome {
                    None => RetainedAtomicResult::Pending,
                    Some(o) if o.code != 0 => RetainedAtomicResult::Uncertain {
                        recorded_code: o.code,
                    },
                    Some(o)
                        if o.version != 0
                            && o.ack == original.requested_ack().byte()
                            && o.version
                                .checked_add(original.change_count() as u64)
                                .is_some() =>
                    {
                        RetainedAtomicResult::Committed {
                            version: o.version,
                            achieved: original.requested_ack(),
                        }
                    }
                    Some(_) => RetainedAtomicResult::InvalidOutcome,
                };
                AtomicResultEvidence::Matching {
                    recorded_at_utc_ms: record.at,
                    retry_until_mono_ms: record.until_mono,
                    retention: if observed_mono_ms > record.until_mono {
                        RetryRetention::Expired
                    } else {
                        RetryRetention::WithinWindow
                    },
                    result,
                }
            }
        } else if state.retired_set.contains(identity) {
            AtomicResultEvidence::RetiredIdentity
        } else {
            AtomicResultEvidence::NotRetained
        };
        Ok(AtomicResultInspection {
            store_epoch: self.epoch,
            original_epoch: original.epoch(),
            original_expires_mono_ms: original.expires_mono(),
            observed_mono_ms,
            fingerprint,
            evidence,
        })
    }
}

#[cfg(test)]
#[path = "../unit_tests/atomic/result_tests.rs"]
mod tests;
