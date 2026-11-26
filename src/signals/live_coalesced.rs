use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::storage::codec::encode_kv_key;
use crate::types::{MembusError, SignalEvent};

struct CoalescingState {
    next_ticket: u64,
    pending: HashMap<Vec<u8>, Arc<SignalEvent>>,
    fifo_order: BTreeMap<u64, Vec<u8>>,
    max_pending: usize,
    overflowed: bool,
    closed: bool,
}

/// Bounded per-subscriber coalescing inbox updated without blocking writers.
///
/// Repeated updates for the same `(namespace, key)` coalesce in place while
/// preserving the oldest FIFO arrival ticket so hot keys cannot starve quiet keys.
pub(crate) struct CoalescingInbox {
    key_prefix: Vec<u8>,
    state: Mutex<CoalescingState>,
    notify: Notify,
}

impl CoalescingInbox {
    pub(crate) fn new(key_prefix: Vec<u8>, max_pending: usize) -> Self {
        Self {
            key_prefix,
            state: Mutex::new(CoalescingState {
                next_ticket: 1,
                pending: HashMap::new(),
                fifo_order: BTreeMap::new(),
                max_pending: max_pending.max(1),
                overflowed: false,
                closed: false,
            }),
            notify: Notify::new(),
        }
    }

    pub(crate) fn push(&self, event: Arc<SignalEvent>) {
        if !self.key_prefix.is_empty() && !event.key.starts_with(&self.key_prefix) {
            return;
        }
        let composite_key = encode_kv_key(&event.namespace, &event.key);
        let should_notify = {
            let mut guard = self.state.lock();
            if guard.closed {
                return;
            }
            if let Some(existing) = guard.pending.get_mut(&composite_key) {
                // Coalesce in place, preserving the existing FIFO arrival ticket in `fifo_order`.
                *existing = event;
                true
            } else if guard.pending.len() >= guard.max_pending {
                guard.overflowed = true;
                true
            } else {
                let ticket = guard.next_ticket;
                guard.next_ticket = guard.next_ticket.saturating_add(1);
                guard.fifo_order.insert(ticket, composite_key.clone());
                guard.pending.insert(composite_key, event);
                true
            }
        };
        if should_notify {
            self.notify.notify_one();
        }
    }

    pub(crate) fn close(&self) {
        {
            let mut guard = self.state.lock();
            guard.closed = true;
        }
        self.notify.notify_waiters();
    }
}

/// Live coalesced subscription stream preserving oldest-ticket FIFO fairness.
pub struct LiveCoalescedSubscription {
    inbox: Arc<CoalescingInbox>,
    epoch: u64,
    latest_seq_fn: Arc<dyn Fn() -> u64 + Send + Sync>,
    active_counter: Arc<AtomicUsize>,
}

impl LiveCoalescedSubscription {
    pub(crate) fn new(
        inbox: Arc<CoalescingInbox>,
        epoch: u64,
        latest_seq_fn: Arc<dyn Fn() -> u64 + Send + Sync>,
        active_counter: Arc<AtomicUsize>,
    ) -> Self {
        active_counter.fetch_add(1, Ordering::AcqRel);
        Self {
            inbox,
            epoch,
            latest_seq_fn,
            active_counter,
        }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Receive the next coalesced event in oldest-arrival FIFO order.
    ///
    /// Returns `Err(MembusError::Gap { .. })` if distinct pending keys exceeded `max_pending`.
    pub async fn next_event(&mut self) -> Result<Option<SignalEvent>, MembusError> {
        loop {
            let notified = self.inbox.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let next_item = {
                let mut guard = self.inbox.state.lock();
                if guard.overflowed {
                    // Surface overflow immediately so sustained distinct-key bursts cannot
                    // hide dropped keys behind a continuously refilled FIFO queue.
                    guard.overflowed = false;
                    guard.fifo_order.clear();
                    guard.pending.clear();
                    Some(Err(MembusError::Gap {
                        earliest_available_seq: (self.latest_seq_fn)().saturating_add(1),
                        current_epoch: self.epoch,
                    }))
                } else if let Some((_ticket, composite_key)) = guard.fifo_order.pop_first() {
                    guard
                        .pending
                        .remove(&composite_key)
                        .map(|ev| Ok(Some((*ev).clone())))
                } else if guard.closed {
                    Some(Ok(None))
                } else {
                    None
                }
            };

            if let Some(outcome) = next_item {
                return outcome;
            }
            notified.await;
        }
    }

    pub fn pending_count(&self) -> usize {
        self.inbox.state.lock().pending.len()
    }
}

impl Drop for LiveCoalescedSubscription {
    fn drop(&mut self) {
        self.inbox.close();
        self.active_counter.fetch_sub(1, Ordering::AcqRel);
    }
}
