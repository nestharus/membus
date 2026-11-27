use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex as FastMutex;

use crate::storage::publisher::SignalPublisher;
use crate::types::SignalEvent;

struct SequencerState {
    next_publish_seq: u64,
    ready: BTreeMap<u64, Option<Vec<SignalEvent>>>,
}

/// Guarantees strict contiguous sequence-order publication to [`SignalPublisher`] and
/// monotonic advancement of `published_seq` across concurrent RocksDB transactions.
pub(crate) struct PublishSequencer {
    next_seq: AtomicU64,
    published_seq: AtomicU64,
    state: FastMutex<SequencerState>,
    publisher: Arc<dyn SignalPublisher>,
}

impl PublishSequencer {
    pub(crate) fn new(initial_seq: u64, publisher: Arc<dyn SignalPublisher>) -> Self {
        Self {
            next_seq: AtomicU64::new(initial_seq.saturating_add(1)),
            published_seq: AtomicU64::new(initial_seq),
            state: FastMutex::new(SequencerState {
                next_publish_seq: initial_seq.saturating_add(1),
                ready: BTreeMap::new(),
            }),
            publisher,
        }
    }

    pub(crate) fn reserve(self: &Arc<Self>) -> SeqTicket {
        let seq = self.next_seq.fetch_add(1, Ordering::AcqRel);
        SeqTicket {
            seq,
            sequencer: Arc::clone(self),
            settled: false,
        }
    }

    pub(crate) fn published_seq(&self) -> u64 {
        self.published_seq.load(Ordering::Acquire)
    }

    fn settle(&self, seq: u64, events: Option<Vec<SignalEvent>>) {
        let mut guard = self.state.lock();
        guard.ready.insert(seq, events);
        loop {
            let current = guard.next_publish_seq;
            let Some(entry) = guard.ready.remove(&current) else {
                break;
            };
            guard.next_publish_seq = current.saturating_add(1);
            if let Some(committed_events) = entry {
                self.published_seq.store(current, Ordering::Release);
                self.publisher.publish_ordered(&committed_events);
            } else {
                self.published_seq.store(current, Ordering::Release);
            }
        }
    }
}

/// RAII reservation for one commit sequence number. Dropping without calling
/// [`SeqTicket::commit`] settles the slot as aborted so later sequence numbers are never blocked.
pub(crate) struct SeqTicket {
    seq: u64,
    sequencer: Arc<PublishSequencer>,
    settled: bool,
}

impl SeqTicket {
    pub(crate) fn seq(&self) -> u64 {
        self.seq
    }

    pub(crate) fn commit(mut self, events: Vec<SignalEvent>) {
        self.settled = true;
        self.sequencer.settle(self.seq, Some(events));
    }
}

impl Drop for SeqTicket {
    fn drop(&mut self) {
        if !self.settled {
            self.sequencer.settle(self.seq, None);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::PublishSequencer;
    use crate::storage::publisher::SignalPublisher;
    use crate::types::{SignalEvent, SignalKind};

    struct VecPublisher(Mutex<Vec<u64>>);

    impl SignalPublisher for VecPublisher {
        fn publish_ordered(&self, events: &[SignalEvent]) {
            let mut guard = self.0.lock().unwrap();
            for ev in events {
                guard.push(ev.seq);
            }
        }
    }

    fn dummy_event(seq: u64) -> SignalEvent {
        SignalEvent {
            seq,
            epoch: 1,
            namespace: "ns".to_owned(),
            key: b"k".to_vec(),
            version: seq,
            kind: SignalKind::Put,
            value_hint: None,
        }
    }

    #[test]
    fn out_of_order_and_aborted_tickets_publish_strictly_in_sequence() {
        let publisher = Arc::new(VecPublisher(Mutex::new(Vec::new())));
        let seq = Arc::new(PublishSequencer::new(0, publisher.clone()));

        let t1 = seq.reserve();
        let t2 = seq.reserve();
        let t3 = seq.reserve();
        assert_eq!((t1.seq(), t2.seq(), t3.seq()), (1, 2, 3));

        // Commit 3 first, abort 2, commit 1 last.
        t3.commit(vec![dummy_event(3)]);
        assert_eq!(seq.published_seq(), 0);
        drop(t2);
        assert_eq!(seq.published_seq(), 0);
        t1.commit(vec![dummy_event(1)]);

        assert_eq!(seq.published_seq(), 3);
        assert_eq!(*publisher.0.lock().unwrap(), vec![1, 3]);
    }
}
