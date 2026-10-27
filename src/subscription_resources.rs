//! Typed ownership of subscription managed-memory charges in the Store account.
//!
//! Charges describe retained representations and prospective allocations, not
//! measured RSS or kernel memory. Only Store admission can create new ownership.
//! Grant, growth and return (including token destruction) acquire State. Do not
//! hold a Hub lock across them: existing health reads acquire State then Hub.
use crate::{store::State, Store};
use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}};

/// A representation's managed-byte demand. These numbers confer no capacity.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SubscriptionResources {
    pub metadata: usize,
    pub retained: usize,
    pub in_flight: usize,
    pub encoding: usize,
    pub publication: usize,
}

/// One separately transferable part of a subscription's ownership.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscriptionResource { Metadata, Retained, InFlight, Encoding, Publication }

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReservationError { InvalidDemand, Capacity, ForeignOwner, Returned, InsufficientPart }

impl SubscriptionResources {
    /// Charge the existing registration representation and specified backing
    /// capacities before allocating them. Capacities are demand dimensions,
    /// not a provider budget; later consumers must allocate within them.
    pub fn registration(namespace_capacity: usize, prefix_capacity: usize,
        endpoint_capacity: usize, queue_capacity: usize) -> Result<Self, ReservationError> {
        let metadata = crate::signals::registration_allocation_bytes()
            .checked_add(std::mem::size_of::<SubscriptionReservation>())
            .and_then(|n| n.checked_add(namespace_capacity))
            .and_then(|n| n.checked_add(prefix_capacity))
            .and_then(|n| n.checked_add(endpoint_capacity)).ok_or(ReservationError::InvalidDemand)?;
        let retained = queue_capacity.checked_mul(std::mem::size_of::<Arc<()>>())
            .ok_or(ReservationError::InvalidDemand)?;
        // Worker-owned endpoint copy. Native kernel storage is not measured.
        let result = Self { metadata, retained, in_flight: endpoint_capacity, ..Self::default() };
        result.total()?; Ok(result)
    }
    /// One prospective retained Change/Value allocation. Shared payloads are
    /// conservatively charged in full; backing queue slots are separate.
    pub fn change(namespace_capacity: usize, key_capacity: usize, value_capacity: usize)
        -> Result<Self, ReservationError> {
        let retained = crate::signals::change_allocation_bytes()
            .checked_add(namespace_capacity).and_then(|n| n.checked_add(key_capacity))
            .and_then(|n| n.checked_add(value_capacity)).ok_or(ReservationError::InvalidDemand)?;
        Ok(Self { retained, ..Self::default() })
    }
    /// Both actual encode Vec backing capacities, including growth slack chosen
    /// by the consumer. Capacity growth must be granted before allocation.
    pub fn encoding(body_capacity: usize, frame_capacity: usize) -> Result<Self, ReservationError> {
        let encoding = body_capacity.checked_add(frame_capacity).ok_or(ReservationError::InvalidDemand)?;
        Ok(Self { encoding, ..Self::default() })
    }
    /// Reserve a future history/publication allocation before publication.
    pub fn publication(namespace_capacity: usize, key_capacity: usize, value_capacity: usize)
        -> Result<Self, ReservationError> {
        let change = Self::change(namespace_capacity,key_capacity,value_capacity)?;
        Ok(Self { publication: change.retained, ..Self::default() })
    }

    pub fn total(self) -> Result<usize, ReservationError> {
        [self.metadata, self.retained, self.in_flight, self.encoding, self.publication]
            .into_iter().try_fold(0usize, |n, v| n.checked_add(v).ok_or(ReservationError::InvalidDemand))
    }
    fn part_mut(&mut self, part: SubscriptionResource) -> &mut usize {
        match part {
            SubscriptionResource::Metadata => &mut self.metadata,
            SubscriptionResource::Retained => &mut self.retained,
            SubscriptionResource::InFlight => &mut self.in_flight,
            SubscriptionResource::Encoding => &mut self.encoding,
            SubscriptionResource::Publication => &mut self.publication,
        }
    }
    fn checked_add(self, other: Self) -> Result<Self, ReservationError> {
        let mut result = self;
        for part in PARTS {
            *result.part_mut(part) = result.part_mut(part).checked_add(*other.clone().part_mut(part))
                .ok_or(ReservationError::InvalidDemand)?;
        }
        result.total()?;
        Ok(result)
    }
    fn checked_sub(self, other: Self) -> Result<Self, ReservationError> {
        let mut result = self;
        for part in PARTS {
            *result.part_mut(part) = result.part_mut(part).checked_sub(*other.clone().part_mut(part))
                .ok_or(ReservationError::InsufficientPart)?;
        }
        Ok(result)
    }
}
const PARTS: [SubscriptionResource; 5] = [SubscriptionResource::Metadata,
    SubscriptionResource::Retained, SubscriptionResource::InFlight,
    SubscriptionResource::Encoding, SubscriptionResource::Publication];

/// Advisory current account observation. Copying it never reserves resources.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscriptionResourceProfile {
    pub capacity: usize,
    pub committed_index: usize,
    pub retained_retries: usize,
    /// Includes pending ordinary/atomic work and all issued subscription grants.
    pub reserved: usize,
    pub snapshots: usize,
    pub margin: usize,
    pub chunk_index: usize,
    pub legacy_signals: usize,
    /// Cache can yield this allowance; this is not a physical free-byte sample.
    pub available: usize,
}

/// Non-Clone ownership. Move or split it into the actual retained owner.
/// Return only after the covered representation is released. Cancellation must
/// retain an in-flight child until its exact old callback finishes.
#[must_use = "retain the reservation for the lifetime of its charged representation"]
pub struct SubscriptionReservation {
    owner: Option<Arc<Mutex<State>>>,
    released: Arc<AtomicBool>,
    resources: SubscriptionResources,
}
impl SubscriptionReservation {
    pub fn resources(&self) -> SubscriptionResources { self.resources }
    pub fn is_returned(&self) -> bool { self.owner.is_none() }
    /// Transfer ownership without increasing or returning the account charge.
    pub fn split(&mut self, resources: SubscriptionResources) -> Result<Self, ReservationError> {
        let owner = self.owner.as_ref().ok_or(ReservationError::Returned)?;
        if resources.total()? == 0 { return Err(ReservationError::InvalidDemand); }
        let remaining = self.resources.checked_sub(resources)?;
        let child = Self { owner: Some(owner.clone()), released: self.released.clone(), resources };
        self.resources = remaining;
        Ok(child)
    }
    /// Reclassify owned bytes, for example queued -> in-flight. No new grant.
    pub fn transfer(&mut self, from: SubscriptionResource, to: SubscriptionResource, bytes: usize)
        -> Result<(), ReservationError>
    {
        if self.is_returned() { return Err(ReservationError::Returned); }
        let mut next = self.resources;
        let left = next.part_mut(from).checked_sub(bytes).ok_or(ReservationError::InsufficientPart)?;
        *next.part_mut(from) = left;
        *next.part_mut(to) = next.part_mut(to).checked_add(bytes).ok_or(ReservationError::InvalidDemand)?;
        self.resources = next;
        Ok(())
    }
    /// Idempotent exact-once return of this token's remaining ownership.
    /// Release Hub/consumer locks first; this method acquires the Store State.
    pub fn return_to_owner(&mut self) {
        if let Some(owner) = self.owner.take() {
            let bytes = self.resources.total().expect("issued charge is representable");
            let mut state = owner.lock().unwrap();
            state.memory_reserved = state.memory_reserved.checked_sub(bytes).expect("owned charge retained");
            self.resources = SubscriptionResources::default();
            self.released.store(true, Ordering::Release);
        }
    }
}
impl Drop for SubscriptionReservation {
    fn drop(&mut self) { self.return_to_owner(); }
}

// Actual State debit used by both public issuing methods. The allowance is
// computed by the Store while this same State guard is held, never by a caller.
fn debit(state: &mut State, available: usize, resources: SubscriptionResources) -> Result<(), ReservationError> {
    let bytes = resources.total()?;
    if bytes == 0 { return Err(ReservationError::InvalidDemand); }
    if bytes > available { return Err(ReservationError::Capacity); }
    state.memory_reserved = state.memory_reserved.checked_add(bytes).ok_or(ReservationError::Capacity)?;
    Ok(())
}

fn issue(owner: &Arc<Mutex<State>>, state: &mut State, available: usize,
    resources: SubscriptionResources, released: &Arc<AtomicBool>) -> Result<SubscriptionReservation, ReservationError>
{
    debit(state, available, resources)?;
    Ok(SubscriptionReservation { owner: Some(owner.clone()), released: released.clone(), resources })
}
fn check_owner(original: &Arc<Mutex<State>>, owner: &Arc<Mutex<State>>) -> Result<(), ReservationError> {
    if !Arc::ptr_eq(original, owner) { return Err(ReservationError::ForeignOwner); }
    Ok(())
}
fn extend(owner: &Arc<Mutex<State>>, state: &mut State, available: usize,
    reservation: &mut SubscriptionReservation, additional: SubscriptionResources) -> Result<(), ReservationError>
{
    let original = reservation.owner.as_ref().ok_or(ReservationError::Returned)?;
    check_owner(original, owner)?;
    let next = reservation.resources.checked_add(additional)?;
    debit(state, available, additional)?;
    reservation.resources = next;
    Ok(())
}
impl Store {
    /// Current managed capacity after every existing owner; this observation is
    /// not an authorization for later allocation. Admission recomputes it.
    pub fn subscription_resource_profile(&self) -> SubscriptionResourceProfile {
        let state = self.state.lock().unwrap();
        self.subscription_profile_locked(&state)
    }
    fn subscription_profile_locked(&self, state: &State) -> SubscriptionResourceProfile {
        let chunk_index = self.chunks.lock().unwrap().index_bytes;
        let snapshots = self.scan_usage.bytes.load(Ordering::Acquire);
        let margin = self.config.memory_margin();
        let legacy_signals = self.signals.memory_reservation_bytes();
        SubscriptionResourceProfile { capacity: self.config.capacity,
            committed_index: state.committed_bytes, retained_retries: state.dedup_bytes,
            reserved: state.memory_reserved, snapshots, margin, chunk_index, legacy_signals,
            available: state.available_memory(self.config.capacity, snapshots, margin, chunk_index, legacy_signals) }
    }
    /// Reserve before allocating registration metadata, queue backing, retained
    /// data, encoding or future publication. No fixed subscription policy cap.
    pub fn reserve_subscription_resources(&self, resources: SubscriptionResources)
        -> Result<SubscriptionReservation, ReservationError>
    {
        let mut state = self.state.lock().unwrap();
        let available = self.cache_limit(&state);
        let reservation = issue(&self.state, &mut state, available, resources, &self.subscription_released)?;
        self.trim_cache(&state);
        Ok(reservation)
    }
    /// Additional demand is admitted against the same current account. A token
    /// from another Store, even with equal numbers or epoch, never spends here.
    pub fn extend_subscription_resources(&self, reservation: &mut SubscriptionReservation,
        additional: SubscriptionResources) -> Result<(), ReservationError>
    {
        let mut state = self.state.lock().unwrap();
        let available = self.cache_limit(&state);
        extend(&self.state, &mut state, available, reservation, additional)?;
        self.trim_cache(&state);
        Ok(())
    }
}

#[cfg(test)]
#[path = "subscription_resources/tests.rs"]
mod tests;
