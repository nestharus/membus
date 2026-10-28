//! Pure recovery boundary for an independently authenticated domain custodian.
//!
//! No default custodian, storage implementation, authentication protocol, or
//! permission to publish is supplied. See docs/ATOMIC-ORIGINAL-CUSTODY.md.
use super::AtomicRequest;

/// Expected identity comes from the domain owner, never from offered bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OriginalScope<'a> {
    pub domain: &'a str,
    pub generation: u64,
    pub kind: &'a str,
}

/// An authenticated custodian's record of the actual domain-produced original.
///
/// Merely constructing this data does not authenticate it. Only the selected
/// DomainOriginalCustody implementation supplies its provenance. In particular,
/// a public decoder followed by this constructor is not a conforming adapter.
#[derive(Clone, Debug)]
pub struct CustodyRecord {
    pub domain: String,
    pub generation: u64,
    pub kind: String,
    pub original: AtomicRequest,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CustodyError {
    /// No retained original is known. This never permits a replacement decision.
    UnknownOriginal,
    /// Bytes may exist, but authentic domain-original provenance is unavailable.
    ProvenanceUnavailable,
    Unavailable,
    Invalid,
    ScopeMismatch,
    OriginalMismatch,
}

/// Existing domain authority's authenticated, read-only original lookup port.
///
/// An implementation must authenticate the actual domain producer and retain
/// the exact original at that domain's decision boundary, before any send. It
/// must bind dataset/domain, generation, operation kind and identity to that
/// original, including all original domain preconditions. It must not attest an
/// arbitrary syntactically valid request supplied by a recovery caller. Neither
/// a checksum, a copied epoch nor a matching current state supplies this proof.
///
/// Caller-process-loss support requires this custodian and its authenticated
/// original custody to survive independently of the lost caller. An in-memory
/// implementation in that caller cannot meet that boundary. Missing evidence
/// returns ProvenanceUnavailable or UnknownOriginal; no empty record or fresh
/// capture is substituted. This crate provides no production adapter.
pub trait DomainOriginalCustody {
    fn load_original(
        &self,
        scope: OriginalScope<'_>,
        operation_id: &str,
    ) -> Result<CustodyRecord, CustodyError>;
}

/// An exact original recovered through the explicitly selected authority port.
/// This says nothing about publication outcome, current authority, or retry
/// eligibility. Keep every prior observation of uncertainty independently.
#[derive(Debug)]
pub struct RecoveredOriginal {
    record: CustodyRecord,
}
impl RecoveredOriginal {
    pub fn original(&self) -> &AtomicRequest {
        &self.record.original
    }
    pub fn scope(&self) -> OriginalScope<'_> {
        OriginalScope {
            domain: &self.record.domain,
            generation: self.record.generation,
            kind: &self.record.kind,
        }
    }
}

/// Compare an offered request with the authenticated retained domain original.
/// No Store, recapture, re-ID, clock renewal, send or inferred outcome occurs.
pub fn recover_original(
    custody: &(impl DomainOriginalCustody + ?Sized),
    expected: OriginalScope<'_>,
    offered: &AtomicRequest,
) -> Result<RecoveredOriginal, CustodyError> {
    let valid = |s: &str| !s.is_empty() && s.len() <= crate::MAX_ID && !s.contains('\0');
    if !valid(expected.domain) || !valid(expected.kind) || expected.generation == 0 {
        return Err(CustodyError::Invalid);
    }
    offered.validate().map_err(|_| CustodyError::Invalid)?;
    let record = custody.load_original(expected, offered.operation_id())?;
    if record.domain != expected.domain
        || record.generation != expected.generation
        || record.kind != expected.kind
    {
        return Err(CustodyError::ScopeMismatch);
    }
    record
        .original
        .validate()
        .map_err(|_| CustodyError::Invalid)?;
    // The canonical codec remains the sole complete identity definition.
    if record.original.to_bytes() != offered.to_bytes() {
        return Err(CustodyError::OriginalMismatch);
    }
    Ok(RecoveredOriginal { record })
}
