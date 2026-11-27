use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::signals::ring::{NamespaceChannel, RingRead, WILDCARD_NAMESPACE};
use crate::storage::contract::StorageEngine;
use crate::types::{MembusError, SignalEvent};

pub(crate) struct EachChangeInit {
    pub(crate) storage: Arc<dyn StorageEngine>,
    pub(crate) channel: Arc<NamespaceChannel>,
    pub(crate) namespace: String,
    pub(crate) key_prefix: Vec<u8>,
    pub(crate) epoch: u64,
    pub(crate) since_seq: u64,
    pub(crate) startup_floor_seq: u64,
    pub(crate) batch_size: usize,
    pub(crate) active_counter: Arc<AtomicUsize>,
}

/// Lossless ordered subscription stream reading fast-path events from the RAM ring
/// and seamlessly catching up lagging cursors from the durable `log` column family.
pub struct EachChangeSubscription {
    storage: Arc<dyn StorageEngine>,
    channel: Arc<NamespaceChannel>,
    namespace: String,
    key_prefix: Vec<u8>,
    epoch: u64,
    delivered_seq: u64,
    fetched_seq: u64,
    startup_floor_seq: u64,
    batch_size: usize,
    buffered: VecDeque<SignalEvent>,
    active_counter: Arc<AtomicUsize>,
}

impl EachChangeSubscription {
    pub(crate) fn new(init: EachChangeInit) -> Self {
        init.active_counter.fetch_add(1, Ordering::AcqRel);
        Self {
            storage: init.storage,
            channel: init.channel,
            namespace: init.namespace,
            key_prefix: init.key_prefix,
            epoch: init.epoch,
            delivered_seq: init.since_seq,
            fetched_seq: init.since_seq,
            startup_floor_seq: init.startup_floor_seq,
            batch_size: init.batch_size.max(1),
            buffered: VecDeque::new(),
            active_counter: init.active_counter,
        }
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Sequence number of the last event actually delivered to the caller
    /// (or the initial `since_seq` before the first event is delivered).
    pub fn cursor_seq(&self) -> u64 {
        self.delivered_seq
    }

    /// Yield the next committed matching event strictly after `cursor_seq()`.
    ///
    /// Never silently drops events: returns `Err(MembusError::Gap { .. })` if the
    /// durable commit log has been trimmed past the subscriber's unread position.
    pub async fn next_event(&mut self) -> Result<Option<SignalEvent>, MembusError> {
        if let Some(ev) = self.buffered.pop_front() {
            self.delivered_seq = ev.seq;
            return Ok(Some(ev));
        }

        loop {
            let notified = self.channel.notify().notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let horizon = self.storage.log_horizon_seq();
            if self.fetched_seq < horizon {
                return Err(MembusError::Gap {
                    earliest_available_seq: horizon.saturating_add(1),
                    current_epoch: self.epoch,
                });
            }

            let effective_floor = if self.fetched_seq >= self.startup_floor_seq {
                0
            } else {
                self.startup_floor_seq
            };

            match self
                .channel
                .read_after(self.fetched_seq, effective_floor, self.batch_size)
            {
                RingRead::Events(events) => {
                    for ev in events {
                        if ev.seq > self.fetched_seq {
                            self.fetched_seq = ev.seq;
                        }
                        if self.matches(&ev) {
                            self.buffered.push_back((*ev).clone());
                        }
                    }
                    if let Some(first) = self.buffered.pop_front() {
                        self.delivered_seq = first.seq;
                        return Ok(Some(first));
                    }
                }
                RingRead::NeedLogCatchup { .. } => {
                    let storage = Arc::clone(&self.storage);
                    let after = self.fetched_seq;
                    let limit = self.batch_size;
                    let page = tokio::task::spawn_blocking(move || {
                        storage.scan_log_after(after, limit)
                    })
                    .await
                    .map_err(|err| {
                        MembusError::Storage(format!("log catch-up task failed: {err}"))
                    })??;

                    if page.scanned_through_seq > self.fetched_seq {
                        self.fetched_seq = page.scanned_through_seq;
                    }
                    if self.fetched_seq >= self.startup_floor_seq {
                        self.startup_floor_seq = 0;
                    }

                    for ev in page.events {
                        if ev.seq > self.fetched_seq {
                            self.fetched_seq = ev.seq;
                        }
                        if self.matches(&ev) {
                            self.buffered.push_back(ev);
                        }
                    }

                    if let Some(first) = self.buffered.pop_front() {
                        self.delivered_seq = first.seq;
                        return Ok(Some(first));
                    }

                    // If we advanced past `after`, re-check the RAM ring immediately on
                    // the next iteration instead of parking before draining already-published
                    // in-memory events.
                    if self.fetched_seq > after {
                        continue;
                    }

                    if self.fetched_seq >= self.channel.evicted_through_seq()
                        && self.fetched_seq >= self.startup_floor_seq
                    {
                        notified.await;
                    }
                }
                RingRead::Empty => {
                    notified.await;
                }
            }
        }
    }

    fn matches(&self, ev: &SignalEvent) -> bool {
        let ns_ok = self.namespace == WILDCARD_NAMESPACE || ev.namespace == self.namespace;
        let prefix_ok = self.key_prefix.is_empty() || ev.key.starts_with(&self.key_prefix);
        ns_ok && prefix_ok
    }
}

impl Drop for EachChangeSubscription {
    fn drop(&mut self) {
        self.active_counter.fetch_sub(1, Ordering::AcqRel);
    }
}
