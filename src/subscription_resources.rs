//! Typed ownership of subscription managed-memory charges in the Store account.
//!
//! Charges describe retained representations and prospective allocations, not
//! measured RSS or kernel memory. Only Store admission can create new ownership.
//! Grant and growth acquire State. Return atomically releases the live subscription
//! portion after allocation release, without acquiring State under the Hub lock.
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
            .checked_add(namespace_capacity)
            .and_then(|n| n.checked_add(prefix_capacity)).ok_or(ReservationError::InvalidDemand)?;
        let retained = queue_capacity.checked_mul(crate::signals::queue_link_allocation_bytes())
            .ok_or(ReservationError::InvalidDemand)?;
        // The shared slot owns endpoint, Connection and its grant through I/O.
        let in_flight=crate::signals::connection_allocation_bytes().checked_add(endpoint_capacity).ok_or(ReservationError::InvalidDemand)?;
        let result = Self { metadata, retained, in_flight, ..Self::default() };
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
    usage: Arc<SubscriptionUsage>,
    legacy: bool,
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
        let child = Self { owner: Some(owner.clone()), released: self.released.clone(), usage:self.usage.clone(),legacy:self.legacy,resources };
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
    /// Returns only the live subscription portion; never acquires State or changes ordinary debits.
    pub fn return_to_owner(&mut self) {
        if let Some(owner) = self.owner.take() {
            let bytes = self.resources.total().expect("issued charge is representable");
            // The original State identity stays attached, but release does not
            // acquire it. Ordinary/atomic debits are never overwritten.
            self.usage.bytes.try_update(Ordering::AcqRel,Ordering::Acquire,|n|n.checked_sub(bytes)).expect("owned charge retained");
            if self.legacy {self.usage.legacy.try_update(Ordering::AcqRel,Ordering::Acquire,|n|n.checked_sub(bytes)).expect("legacy charge retained");}
            drop(owner);
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
    state.subscription_usage.bytes.try_update(Ordering::AcqRel,Ordering::Acquire,|n|n.checked_add(bytes)).map_err(|_|ReservationError::Capacity)?;
    Ok(())
}

pub(crate) fn issue(owner: &Arc<Mutex<State>>, state: &mut State, available: usize,
    resources: SubscriptionResources, released: &Arc<AtomicBool>) -> Result<SubscriptionReservation, ReservationError>
{
    debit(state, available, resources)?;
    Ok(SubscriptionReservation { owner: Some(owner.clone()), released: released.clone(),usage:state.subscription_usage.clone(),legacy:false,resources })
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
    if reservation.legacy {reservation.usage.legacy.fetch_add(additional.total()?,Ordering::AcqRel);}
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
        let legacy_signals = state.subscription_usage.legacy.load(Ordering::Acquire);
        SubscriptionResourceProfile { capacity: self.config.capacity,
            committed_index: state.committed_bytes, retained_retries: state.dedup_bytes,
            reserved: state.total_reserved(), snapshots, margin, chunk_index, legacy_signals,
            available: state.available_memory(self.config.capacity, snapshots, margin, chunk_index, 0) }
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


/// Live subscription portion of the existing State admission account. Grants
/// serialize on State; release is atomic after the covered allocation is freed.
#[derive(Default)]
pub(crate) struct SubscriptionUsage {
    pub(crate) bytes: std::sync::atomic::AtomicUsize,
    pub(crate) legacy: std::sync::atomic::AtomicUsize,
}
/// No Store/Hub/worker strong reference: publisher admission cannot keep its
/// service owner alive or cause a worker to destroy/join itself.
#[derive(Clone)]
pub(crate) struct SubscriptionAccount {
    owner: std::sync::Weak<Mutex<State>>,
    pub(crate) usage: Arc<SubscriptionUsage>,
    capacity: usize,
    margin: usize,
    released: Arc<AtomicBool>,
    context: AccountContext,
}
#[derive(Clone)]
enum AccountContext {
    Store {
        scans: Arc<crate::snapshots::ScanUsage>,
        chunks: std::sync::Weak<Mutex<crate::chunks::ChunkStore>>,
        cache: std::sync::Weak<crate::cache::Cache>,
    },
    #[cfg(test)]
    Capture { scans: Arc<crate::snapshots::ScanUsage>, chunk_index: usize },
}
impl SubscriptionAccount {
    pub(crate) fn from_store(store:&Store)->Self {
        Self{owner:Arc::downgrade(&store.state),usage:store.state.lock().unwrap().subscription_usage.clone(),
            capacity:store.config.capacity,margin:store.config.memory_margin(),released:store.subscription_released.clone(),
            context:AccountContext::Store{scans:store.scan_usage.clone(),chunks:Arc::downgrade(&store.chunks),cache:Arc::downgrade(&store.cache)}}
    }
    #[cfg(test)]
    pub(crate) fn for_capture(owner:&Arc<Mutex<State>>,config:&crate::Config,scans:Arc<crate::snapshots::ScanUsage>,chunk_index:usize)->Self {
        Self{owner:Arc::downgrade(owner),usage:owner.lock().unwrap().subscription_usage.clone(),capacity:config.capacity,
            margin:config.memory_margin(),released:Arc::new(AtomicBool::new(false)),context:AccountContext::Capture{scans,chunk_index}}
    }
    pub(crate) fn reserve(&self,resources:SubscriptionResources,legacy:bool)->Result<SubscriptionReservation,ReservationError> {
        let owner=self.owner.upgrade().ok_or(ReservationError::Returned)?;
        let mut state=owner.lock().unwrap();
        let (snapshots,index,cache)=match &self.context {
            AccountContext::Store{scans,chunks,cache}=>{
                let chunks=chunks.upgrade().ok_or(ReservationError::Returned)?;
                let index=chunks.lock().unwrap().index_bytes;
                (scans.bytes.load(Ordering::Acquire),index,Some(cache.upgrade().ok_or(ReservationError::Returned)?))
            },
            #[cfg(test)]
            AccountContext::Capture{scans,chunk_index}=>(scans.bytes.load(Ordering::Acquire),*chunk_index,None),
        };
        let available=state.available_memory(self.capacity,snapshots,self.margin,index,0);
        let mut grant=issue(&owner,&mut state,available,resources,&self.released)?;
        if legacy {
            self.usage.legacy.fetch_add(resources.total()?,Ordering::AcqRel);grant.legacy=true;
        }
        if let Some(cache)=cache {cache.trim(state.available_memory(self.capacity,snapshots,self.margin,index,0));}
        Ok(grant)
    }
}

#[cfg(test)]
mod catchup_account_tests {
    use super::*;
    use crate::{chunks::{Clock,ManualClock,ChunkStore},cache::{Cache,CachePolicy},Config,Value};
    use std::path::PathBuf;

    #[test]
    fn current_store_account_context_trims_real_cache_and_returns_only_its_charge() {
        let config=Config{capacity:4*1024*1024,cache_bytes:2*1024*1024,..Config::default()};
        let clock=Clock::new(Some(ManualClock::new(2_000_000)));
        let cache=Arc::new(Cache::new(&config,clock));cache.trim(config.cache_bytes);
        for version in 1..=4 {
            cache.insert(Arc::new(Value{version,ack:1,bytes:vec![2;384*1024],tombstone:false}),
                "authored".into(),CachePolicy::default(),true,config.cache_bytes,&AtomicBool::new(true));
        }
        let before_cache=cache.stats().bytes;assert!(before_cache>1024*1024);
        let owner=Arc::new(Mutex::new(State::default()));owner.lock().unwrap().memory_reserved=512*1024;
        let scans=Arc::new(crate::snapshots::ScanUsage::default());
        let pressure_bytes=512*1024;
        {let state=owner.lock().unwrap();assert!(pressure_bytes<=state.available_memory(config.capacity,0,config.memory_margin(),64*1024,0));
            scans.bytes.store(pressure_bytes,Ordering::Release);scans.count.store(1,Ordering::Release);}
        let selected_charge=crate::snapshots::ScanCharge{usage:scans.clone(),bytes:pressure_bytes};
        let selected_payload=vec![7u8;pressure_bytes];
        // This is the real metadata type's I/O-free constructor. No Store,
        // directory, persistence backend or journal is created or substituted.
        let chunks=Arc::new(Mutex::new(ChunkStore::new(PathBuf::new())));chunks.lock().unwrap().index_bytes=64*1024;
        let released=Arc::new(AtomicBool::new(false));
        let account=SubscriptionAccount{owner:Arc::downgrade(&owner),usage:owner.lock().unwrap().subscription_usage.clone(),capacity:config.capacity,
            margin:config.memory_margin(),released:released.clone(),context:AccountContext::Store{scans:scans.clone(),chunks:Arc::downgrade(&chunks),cache:Arc::downgrade(&cache)}};
        let mut grant=account.reserve(SubscriptionResources::encoding(896*1024,896*1024).unwrap(),false).unwrap();
        let available=owner.lock().unwrap().available_memory(config.capacity,scans.bytes.load(Ordering::Acquire),config.memory_margin(),64*1024,0);
        let after=cache.stats();assert!(after.bytes<=available);assert!(after.bytes<before_cache);
        let reserved=owner.lock().unwrap().total_reserved();assert_eq!(reserved,512*1024+grant.resources().total().unwrap());
        grant.return_to_owner();grant.return_to_owner();
        assert_eq!(owner.lock().unwrap().total_reserved(),512*1024);assert!(released.load(Ordering::Acquire));
        assert_eq!(scans.bytes.load(Ordering::Acquire),selected_payload.len());
        drop(selected_payload);drop(selected_charge);assert_eq!(scans.bytes.load(Ordering::Acquire),0);
    }

    #[test]
    fn foreign_returned_and_over_capacity_growth_preserve_actual_owner_and_parts() {
        let config=Config{capacity:4*1024*1024,..Config::default()};
        let owner=Arc::new(Mutex::new(State::default()));let foreign=Arc::new(Mutex::new(State::default()));
        let account=SubscriptionAccount::for_capture(&owner,&config,Arc::new(crate::snapshots::ScanUsage::default()),0);
        let mut grant=account.reserve(SubscriptionResources::encoding(512,512).unwrap(),false).unwrap();
        let original=grant.resources();let before=owner.lock().unwrap().total_reserved();
        let growth=SubscriptionResources::encoding(256,256).unwrap();
        {
            let mut state=foreign.lock().unwrap();let available=state.available_memory(config.capacity,0,config.memory_margin(),0,0);
            assert_eq!(extend(&foreign,&mut state,available,&mut grant,growth),Err(ReservationError::ForeignOwner));
            assert_eq!(state.total_reserved(),0);
        }
        assert_eq!(grant.resources(),original);assert_eq!(owner.lock().unwrap().total_reserved(),before);
        {
            let mut state=owner.lock().unwrap();let available=state.available_memory(config.capacity,0,config.memory_margin(),0,0);
            let too_much=SubscriptionResources::encoding(available+1,0).unwrap();
            assert_eq!(extend(&owner,&mut state,available,&mut grant,too_much),Err(ReservationError::Capacity));
            assert_eq!(state.total_reserved(),before);assert_eq!(grant.resources(),original);
            extend(&owner,&mut state,available,&mut grant,growth).unwrap();
        }
        // Actual ownership extension succeeds before the corresponding new
        // representation is allocated; a rejected extension allocated nothing.
        let new_buffer=vec![0u8;growth.total().unwrap()];assert_eq!(new_buffer.len(),512);
        drop(new_buffer);grant.return_to_owner();grant.return_to_owner();
        let mut state=owner.lock().unwrap();let available=state.available_memory(config.capacity,0,config.memory_margin(),0,0);
        assert_eq!(extend(&owner,&mut state,available,&mut grant,growth),Err(ReservationError::Returned));
        assert_eq!(state.total_reserved(),0);
    }
}
