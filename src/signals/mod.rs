pub mod each_change;
pub mod live_coalesced;
pub mod ring;

use std::sync::Arc;

use crate::signals::live_coalesced::CoalescingInbox;
use crate::storage::StorageEngine;
use crate::types::{validate_namespace, MembusError};

pub use each_change::EachChangeSubscription;
pub use live_coalesced::LiveCoalescedSubscription;
pub use ring::{SignalConfig, SignalRing, WILDCARD_NAMESPACE};

/// Subscription coordinator joining the lock-free RAM signal ring with durable `log` CF catch-up.
pub struct SignalBroker {
    storage: Arc<dyn StorageEngine>,
    ring: Arc<SignalRing>,
    startup_seq: u64,
    config: SignalConfig,
}

impl SignalBroker {
    pub fn new(
        storage: Arc<dyn StorageEngine>,
        ring: Arc<SignalRing>,
        config: SignalConfig,
    ) -> Self {
        let startup_seq = storage.latest_seq();
        Self {
            storage,
            ring,
            startup_seq,
            config,
        }
    }

    pub fn epoch(&self) -> u64 {
        self.storage.epoch()
    }

    pub fn latest_seq(&self) -> u64 {
        self.storage.latest_seq()
    }

    pub fn active_subscriptions(&self) -> usize {
        self.ring.active_subscriptions()
    }

    /// Open a lossless ordered `EachChange` subscription starting strictly after `(epoch, since_seq)`.
    ///
    /// Pass `epoch = 0` to bind to the current instance epoch from `since_seq`.
    pub fn subscribe_each_change(
        &self,
        namespace: &str,
        key_prefix: Vec<u8>,
        requested_epoch: u64,
        since_seq: u64,
    ) -> Result<EachChangeSubscription, MembusError> {
        validate_subscription_namespace(namespace)?;

        let current_epoch = self.storage.epoch();
        let horizon = self.storage.log_horizon_seq();
        let latest = self.storage.latest_seq();
        if requested_epoch != 0 && requested_epoch != current_epoch {
            return Err(MembusError::Gap {
                earliest_available_seq: horizon.saturating_add(1),
                current_epoch,
            });
        }
        if since_seq < horizon || since_seq > latest {
            return Err(MembusError::Gap {
                earliest_available_seq: horizon.saturating_add(1),
                current_epoch,
            });
        }

        let channel = self.ring.channel_for(namespace);
        Ok(EachChangeSubscription::new(
            Arc::clone(&self.storage),
            channel,
            namespace.to_owned(),
            key_prefix,
            current_epoch,
            since_seq,
            self.startup_seq,
            self.config.log_catchup_batch_size,
            self.ring.subscription_counter(),
        ))
    }

    /// Open a `LiveCoalesced` subscription coalescing bursts per `(namespace, key)`
    /// while preserving oldest-arrival FIFO order.
    pub fn subscribe_live(
        &self,
        namespace: &str,
        key_prefix: Vec<u8>,
        max_pending: Option<usize>,
    ) -> Result<LiveCoalescedSubscription, MembusError> {
        validate_subscription_namespace(namespace)?;

        let limit = max_pending
            .unwrap_or(self.config.default_live_max_pending)
            .clamp(1, self.config.max_live_pending.max(1));
        let inbox = Arc::new(CoalescingInbox::new(key_prefix, limit));
        let channel = self.ring.channel_for(namespace);
        channel.register_live(&inbox);

        let storage_for_seq = Arc::clone(&self.storage);
        let latest_seq_fn: Arc<dyn Fn() -> u64 + Send + Sync> =
            Arc::new(move || storage_for_seq.latest_seq());

        Ok(LiveCoalescedSubscription::new(
            inbox,
            self.storage.epoch(),
            latest_seq_fn,
            self.ring.subscription_counter(),
        ))
    }
}

fn validate_subscription_namespace(namespace: &str) -> Result<(), MembusError> {
    if namespace == WILDCARD_NAMESPACE {
        return Ok(());
    }
    validate_namespace(namespace)
}
