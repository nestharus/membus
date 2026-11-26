//! Resident server workers: wire dispatch gate, heartbeat writer, and scheduled timers.
pub(crate) mod heartbeat;
pub(crate) mod resident_dispatch;
pub(crate) mod timers;
