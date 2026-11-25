//! Finite data-record headroom owned by one live Store, not a durable tenant grant.
use crate::{store::State, Store, MAX_ID};
use std::{collections::HashMap, fmt, sync::Arc};

/// New data-record bytes and admissions permitted during a live Store lease.
/// Bytes include framing and all retained history, not just current values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReserveRequest {
    pub bytes: u64,
    pub appends: u64,
    pub lifetime_ms: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReserveError {
    Invalid,
    UnsupportedNamespace,
    AlreadyReserved,
    Resource,
    Unavailable,
    Expired,
    Released,
}

/// The boundary that refused this issuance, not a promise of future capacity.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReserveResource {
    DataBytes,
    ManagedMemory,
    Allocation,
    Identity,
}

/// A coordinated point observation in bytes (or IDs for Identity).
/// None means that this boundary does not expose a truthful numeric operand.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReservePressure {
    pub resource: ReserveResource,
    pub limit: Option<u64>,
    pub required: Option<u64>,
    pub available: Option<u64>,
    pub active_accounts: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReserveRefusal {
    pub error: ReserveError,
    pub pressure: Option<ReservePressure>,
}
impl From<ReserveError> for ReserveRefusal {
    fn from(error: ReserveError) -> Self {
        Self {
            error,
            pressure: None,
        }
    }
}

/// A cumulative account. Retirement, key removal and retries do not refund spend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReserveStatus {
    pub remaining_bytes: u64,
    pub remaining_appends: u64,
    pub admitted_bytes: u64,
    pub admitted_appends: u64,
    pub expires_mono_ms: u64,
}

struct Account {
    id: u64,
    status: ReserveStatus,
    memory: usize,
}

#[derive(Default)]
pub(crate) struct Book {
    next: u64,
    accounts: HashMap<String, Account>,
    #[cfg(test)]
    allocation_additional: Option<usize>,
}

struct Token<'a> {
    store: &'a Store,
    namespace: String,
    id: u64,
    expires: u64,
}

impl Token<'_> {
    fn remove(&self) -> Result<ReserveStatus, ReserveError> {
        let mut state = self.store.state.lock().unwrap();
        let now = self.store.clock.mono_ms();
        expire(&mut state, now);
        if now >= self.expires {
            return Err(ReserveError::Expired);
        }
        let account = state
            .namespace_reserves
            .accounts
            .get(&self.namespace)
            .filter(|a| a.id == self.id)
            .ok_or(ReserveError::Released)?;
        let status = account.status;
        let memory = account.memory;
        state.namespace_reserves.accounts.remove(&self.namespace);
        state.namespace_reserves.accounts.shrink_to_fit();
        state.memory_reserved -= memory;
        Ok(status)
    }
}

impl Drop for Token<'_> {
    fn drop(&mut self) {
        // Arc invokes this exactly once, even when aliases drop concurrently.
        let _ = self.remove();
    }
}

/// Shared ownership of one namespace's live-Store data admission account.
/// All writes to that namespace spend it, including writes without this handle.
/// No serialization, transfer to another Store, renewal or restart import exists.
/// The borrow prevents closing/capturing the Store while this token is retained.
///
/// ```compile_fail
/// use membus::{Store, namespace_reserve::ReserveRequest};
/// fn cannot_outlive_store(store: Store) {
///     let grant = store.reserve_namespace("n", ReserveRequest {
///         bytes: 100, appends: 1, lifetime_ms: 1000,
///     }).unwrap();
///     drop(store);
///     let _ = grant.status();
/// }
/// ```
#[derive(Clone)]
pub struct NamespaceReservation<'a> {
    token: Arc<Token<'a>>,
}

impl fmt::Debug for NamespaceReservation<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NamespaceReservation")
            .field("namespace", &self.token.namespace)
            .field("expires_mono_ms", &self.token.expires)
            .finish()
    }
}

impl NamespaceReservation<'_> {
    pub fn namespace(&self) -> &str {
        &self.token.namespace
    }

    /// Identity within this issuing Store only; not a durable grant credential.
    pub fn id(&self) -> u64 {
        self.token.id
    }

    pub fn status(&self) -> Result<ReserveStatus, ReserveError> {
        let mut state = self.token.store.state.lock().unwrap();
        let now = self.token.store.clock.mono_ms();
        expire(&mut state, now);
        if now >= self.token.expires {
            return Err(ReserveError::Expired);
        }
        state
            .namespace_reserves
            .accounts
            .get(&self.token.namespace)
            .filter(|a| a.id == self.token.id)
            .map(|a| a.status)
            .ok_or(ReserveError::Released)
    }

    /// Revoke all aliases and return the final account observation. Only unused
    /// headroom is released; admitted, pending and uncertain bytes stay charged.
    pub fn release(self) -> Result<ReserveStatus, ReserveError> {
        self.token.remove()
    }
}

impl Store {
    /// Issue a process-local namespace data-record lease from actual admitted
    /// capacity. Other writers cannot spend its unused bytes. Every covered
    /// namespace admission shares its finite byte/append allowance.
    ///
    /// This reserves neither queue concurrency, keys, retry slots, RAM for future
    /// requests, filesystem free space, catalogue/sidecars nor serving authority.
    /// Existing limits/refusals still apply. Grants do not survive Store restart.
    pub fn reserve_namespace(
        &self,
        namespace: &str,
        request: ReserveRequest,
    ) -> Result<NamespaceReservation<'_>, ReserveError> {
        self.reserve_namespace_detailed(namespace, request)
            .map_err(|r| r.error)
    }

    /// Issue through the same admission as reserve_namespace, with a coordinated
    /// refusal observation. Successful issuance grows the live book as needed;
    /// there is no compiled active-population ceiling or separate resize step.
    ///
    /// The Store owner can release unused leases and retry this exact request.
    /// Legitimate completed DATA retirement can replenish global data headroom;
    /// it cannot replenish spent lease allowance. This method neither retries
    /// automatically nor changes Config. Allocation availability is unknown,
    /// even when configured managed headroom is sufficient. Rust's infallible
    /// Arc allocation may still abort on process OOM; this is not an RSS bound.
    pub fn reserve_namespace_detailed(
        &self,
        namespace: &str,
        request: ReserveRequest,
    ) -> Result<NamespaceReservation<'_>, ReserveRefusal> {
        if namespace.is_empty()
            || namespace.len() > MAX_ID
            || namespace.contains('\0')
            || request.bytes == 0
            || request.appends == 0
            || request.lifetime_ms == 0
        {
            return Err(ReserveError::Invalid.into());
        }
        // Internal records protect other accounts as competitors, but cannot be
        // assigned a tenant allowance by this deliberately narrower operation.
        if namespace.starts_with("__membus_") {
            return Err(ReserveError::UnsupportedNamespace.into());
        }
        // State serializes issuance with every DATA preflight/admission and
        // completion. Do not wait for the publication gate: an admitted append
        // may be awaiting settlement while its pending bytes remain protected.
        let mut state = self.state.lock().unwrap();
        let now = self.clock.mono_ms();
        expire(&mut state, now);
        let expires = now
            .checked_add(request.lifetime_ms)
            .ok_or(ReserveError::Invalid)?;
        if state.read_only
            || state.warming
            || self
                .flags
                .storage_error
                .load(std::sync::atomic::Ordering::Acquire)
        {
            return Err(ReserveError::Unavailable.into());
        }
        if state.namespace_reserves.accounts.contains_key(namespace) {
            return Err(ReserveError::AlreadyReserved.into());
        }
        let memory = 512 + 4 * namespace.len();
        let limit = self
            .config
            .state_limit_bytes
            .saturating_sub(self.config.maintenance_reserve_bytes);
        let pressure = |resource, limit, required, available| ReserveRefusal {
            error: ReserveError::Resource,
            pressure: Some(ReservePressure {
                resource,
                limit,
                required,
                available,
                active_accounts: state.namespace_reserves.accounts.len(),
            }),
        };
        let used_data = state
            .persisted_bytes
            .checked_add(state.pending_disk)
            .and_then(|n| n.checked_add(reserved(&state)));
        if !fits(&state, request.bytes, reserved(&state), limit) {
            return Err(pressure(
                ReserveResource::DataBytes,
                Some(limit),
                Some(request.bytes),
                used_data.map(|n| limit.saturating_sub(n)),
            ));
        }
        let available_memory = self.cache_limit(&state);
        if available_memory < memory {
            return Err(pressure(
                ReserveResource::ManagedMemory,
                Some(self.config.capacity as u64),
                Some(memory as u64),
                Some(available_memory as u64),
            ));
        }
        let id = state
            .namespace_reserves
            .next
            .checked_add(1)
            .ok_or_else(|| pressure(ReserveResource::Identity, Some(u64::MAX), None, Some(0)))?;
        let allocation_refusal = pressure(ReserveResource::Allocation, None, None, None);
        // Reserve configured managed backing before the fallible allocations.
        // No identity, entitlement or existing account changes on refusal.
        state.memory_reserved += memory;
        self.trim_cache(&state);
        let allocated = (|| {
            let mut key = String::new();
            key.try_reserve_exact(namespace.len())?;
            key.push_str(namespace);
            let mut token_name = String::new();
            token_name.try_reserve_exact(namespace.len())?;
            token_name.push_str(namespace);
            let additional = 1;
            #[cfg(test)]
            let additional = state
                .namespace_reserves
                .allocation_additional
                .unwrap_or(additional);
            state.namespace_reserves.accounts.try_reserve(additional)?;
            Ok::<_, std::collections::TryReserveError>((key, token_name))
        })();
        let (key, token_name) = match allocated {
            Ok(names) => names,
            Err(_) => {
                state.memory_reserved -= memory;
                return Err(allocation_refusal);
            }
        };
        state.namespace_reserves.next = id;
        state.namespace_reserves.accounts.insert(
            key,
            Account {
                id,
                memory,
                status: ReserveStatus {
                    remaining_bytes: request.bytes,
                    remaining_appends: request.appends,
                    admitted_bytes: 0,
                    admitted_appends: 0,
                    expires_mono_ms: expires,
                },
            },
        );
        Ok(NamespaceReservation {
            token: Arc::new(Token {
                store: self,
                namespace: token_name,
                id,
                expires,
            }),
        })
    }
}

/// Must hold State. Expiry returns unused entitlement, never admitted charges.
pub(crate) fn expire(state: &mut State, now: u64) {
    let mut memory = 0;
    state.namespace_reserves.accounts.retain(|_, account| {
        if now >= account.status.expires_mono_ms {
            memory += account.memory;
            false
        } else {
            true
        }
    });
    if memory != 0 {
        state.namespace_reserves.accounts.shrink_to_fit();
    }
    state.memory_reserved -= memory;
}

fn reserved(state: &State) -> u64 {
    // Issuance only admits a checked sum within the u64 Store data limit.
    state
        .namespace_reserves
        .accounts
        .values()
        .map(|a| a.status.remaining_bytes)
        .sum()
}

fn fits(state: &State, disk: u64, protected: u64, limit: u64) -> bool {
    state
        .persisted_bytes
        .checked_add(state.pending_disk)
        .and_then(|n| n.checked_add(disk))
        .and_then(|n| n.checked_add(protected))
        .is_some_and(|n| n <= limit)
}

/// Non-spending preflight preserves the existing resource-refusal ordering.
pub(crate) fn can_admit(state: &State, ns: &str, disk: u64, limit: u64) -> bool {
    let protected = reserved(state);
    let own = state.namespace_reserves.accounts.get(ns);
    if own.is_some_and(|a| a.status.remaining_appends == 0 || disk > a.status.remaining_bytes) {
        return false;
    }
    // Spending this account moves bytes from unused entitlement to pending
    // disk, so it is counted once. Other accounts remain fully protected.
    let protected = if own.is_some() {
        protected - disk
    } else {
        protected
    };
    fits(state, disk, protected, limit)
}

/// The common scalar/control and atomic data-admission point, under State and
/// the admission gate. No later pre-admission refusal is allowed after success.
pub(crate) fn admit(state: &mut State, ns: &str, disk: u64, now: u64, limit: u64) -> bool {
    expire(state, now);
    if !can_admit(state, ns, disk, limit) {
        return false;
    }
    if let Some(account) = state.namespace_reserves.accounts.get_mut(ns) {
        account.status.remaining_bytes -= disk;
        account.status.remaining_appends -= 1;
        account.status.admitted_bytes += disk;
        account.status.admitted_appends += 1;
    }
    true
}

#[cfg(test)]
#[path = "namespace_reserve_growth_tests.rs"]
mod growth_tests;
#[cfg(test)]
#[path = "namespace_reserve_tests.rs"]
mod tests;
