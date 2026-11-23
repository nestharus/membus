//! Declared complete caller backing, charged to the actual captured account.
use super::{NamespaceView, PublicationError, Reservation};
use std::alloc::Layout;

/// Checked sum of the caller's complete managed allocation layouts.
///
/// Include object/control-block metadata, collection capacity rather than length,
/// native buffers, padding and every overlapping temporary/retained allocation.
/// A Layout describes a declared backing requirement, not observed allocation or
/// allocator/RSS availability. Opaque implementation overhead needs a justified
/// bound from the caller's actual representation; otherwise refuse before copy.
/// No method here measures a Command, certifies a payload count or allocates it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BackingLayout {
    bytes: usize,
}
impl BackingLayout {
    pub const fn empty() -> Self {
        Self { bytes: 0 }
    }

    /// Add one complete allocation, conservatively padded to its alignment.
    /// For a header plus tail, first compose their actual Layout with extend;
    /// summing their unpadded payload lengths is not equivalent.
    pub fn allocation(self, layout: Layout) -> Result<Self, PublicationError> {
        self.allocations(layout, 1)
    }

    /// Add separate equal allocations. This is not a single packed array.
    pub fn allocations(self, layout: Layout, count: usize) -> Result<Self, PublicationError> {
        let bytes = layout
            .pad_to_align()
            .size()
            .checked_mul(count)
            .and_then(|n| self.bytes.checked_add(n))
            .ok_or(PublicationError::Resource(7))?;
        Ok(Self { bytes })
    }

    /// Add one array allocation for the full declared element capacity.
    /// The containing Vec/object header and any element-owned backing are
    /// separate allocations and must also be included. Zero-sized elements do
    /// not eliminate their separately owned backing or containing metadata.
    pub fn array<T>(self, capacity: usize) -> Result<Self, PublicationError> {
        self.allocation(Layout::array::<T>(capacity).map_err(|_| PublicationError::Resource(7))?)
    }

    pub fn bytes(self) -> usize {
        self.bytes
    }
}

/// One affine debit on the SAME account as the issuing NamespaceView.
///
/// Acquire before backing grows, then move into its lifetime owner. Aliases
/// must retain that owner, not clone this charge or create a replacement budget.
/// Drop returns only this debit; it never alters originals, outcomes or deadlines.
/// The underlying captured account/Store scan reservation stays owned until its
/// last genuine owner is gone, even after the view has dropped.
///
/// This guard does not discover or free caller allocations. The caller must keep
/// it until ALL covered backing has been destroyed, including Arc/Weak control
/// blocks and error/unwind paths. A field in an ordinary Arc payload alone does
/// not establish that ordering. See the captured-backing ownership contract.
/// Deliberately leaking/forgetting the guard retains the debit conservatively.
///
/// ```compile_fail
/// use membus::atomic::BackingCharge;
/// fn cannot_duplicate(charge: BackingCharge) {
///     let duplicate = charge.clone();
/// }
/// ```
#[must_use = "retain the charge until all covered managed backing is destroyed"]
#[derive(Debug)]
pub struct BackingCharge {
    reservation: Reservation,
}
impl BackingCharge {
    pub fn bytes(&self) -> usize {
        self.reservation.bytes
    }

    /// Acquire a separate, temporary debit on this charge's captured account.
    ///
    /// A later operation can fund its own backing after the original view
    /// drops. Both debits compete with the existing account's finite allowance
    /// and return independently when their owners drop.
    pub fn reserve_additional(&self, layout: BackingLayout) -> Result<Self, PublicationError> {
        Ok(Self {
            reservation: self.reservation.budget.reserve(layout.bytes())?,
        })
    }
}

impl NamespaceView {
    /// Reserve complete declared side backing before caller copies/allocates it.
    ///
    /// Uses exactly this capture's private OriginalBudget and its existing CAS
    /// admission, competing with population/original preparation on that pool.
    /// Exhaustion/checked overflow refuses Resource(7) without a new debit or
    /// caller effect. No allocation callback or alternate account is accepted.
    /// An empty plan charges zero but still retains the actual account lifetime.
    /// This issues no publication, deadline renewal, memory/RSS certification,
    /// native admission, principal authority or durable restart entitlement.
    pub fn reserve_backing(
        &self,
        layout: BackingLayout,
    ) -> Result<BackingCharge, PublicationError> {
        Ok(BackingCharge {
            reservation: self.budget.reserve(layout.bytes())?,
        })
    }
}

#[cfg(test)]
mod tests;
