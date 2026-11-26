//! membus.kv/1: bounded framing, independent committed reads, durable replay,
//! chunk retirement, atomic namespace publication, signals, and timers.

#[cfg(test)]
extern crate self as membus;

pub mod atomic;
pub mod client;
pub mod platform;
mod protocol;
mod server;
mod signals;
mod store;
pub mod transport;

pub use atomic::population as atomic_population;
pub use protocol::codec::{
    crc32, frame, lp, read_frame, read_frame_limit, reply, Cursor, ATOMIC_CAPTURE,
    ATOMIC_POPULATION, ATOMIC_PUBLISH, BATCH, CAPTURE_METRICS, CODES, CONDITIONAL_PUT, GET,
    GET_MANY, GET_STATUS, HEALTH, HEALTH2, HELLO, IO_METRICS, MAX_BATCH, MAX_FRAME, MAX_ID,
    MAX_VALUE, POLICY_GET, POLICY_SET, PUT, REMOVE, SCAN_KEYS_PREFIX, SCAN_KEYS_RANGE,
    SCAN_PREFIX, SCAN_RANGE, SIGNAL_USAGE, SNAPSHOT_OPEN, SNAPSHOT_OPEN_SELECTED,
    SNAPSHOT_RELEASE, SNAPSHOT_REQUIRE_COMPLETE, SUBSCRIBE, SUBSCRIPTION_STATUS, TIMER_CANCEL,
    TIMER_SET, TIMER_STATUS, UNSUBSCRIBE,
};
pub use protocol::dispatch::ConnectionDispatch;
pub use server::heartbeat::heartbeat_json;
pub use server::resident_dispatch::ResidentDispatch;
pub use server::timers::Owner as ResidentWorkers;
pub use signals::contract as subscription_contract;
pub use signals::resources as subscription_resources;
pub use store::cache::{CachePolicy, CacheRule, CacheStats};
pub use store::chunks::{self, Class, ManualClock, POLICY_NS};
pub use store::indexed;
pub use store::maintenance::CatalogueMaintenance;
pub use store::namespace_reserve;
pub use store::read_cohort::{ReadCohort, ReadValue};
pub use store::receiving::DurablePlacement;
pub use store::recovery_image;
pub use store::selected_view;
pub use store::serving_authority;
pub use store::{Config, Health2, Log, Mutation, Outcome, Store, Value};
#[cfg(windows)]
pub use transport::pipe_io;

// Internal crate-level module aliases for cohesive cross-subsystem access.
pub(crate) use atomic::wire as atomic_wire;
pub(crate) use protocol::codec;
pub(crate) use server::{heartbeat, resident_dispatch, timers};
pub(crate) use store::log_io::sync_file;
pub(crate) use store::{
    append_owner, cache, flush_owner, log_io, read_cohort, replay, routing, snapshots, test_seam,
    value_disk,
};
#[cfg(test)]
pub(crate) use protocol::codec::mutation_bytes;
#[cfg(test)]
pub(crate) use store::maintenance;
#[cfg(test)]
pub(crate) use store::read_cohort::tests as read_work_tests;

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod unit_tests;
