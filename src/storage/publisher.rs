use crate::types::SignalEvent;

/// Callback boundary invoked in strict commit-sequence order after each durable commit.
pub trait SignalPublisher: Send + Sync {
    fn publish_ordered(&self, events: &[SignalEvent]);
}

/// No-op publisher for standalone storage callers that do not attach a signal ring.
pub struct NoopPublisher;

impl SignalPublisher for NoopPublisher {
    fn publish_ordered(&self, _events: &[SignalEvent]) {}
}
