use std::sync::atomic::{AtomicU64, Ordering};

use crate::types::now_unix_ms;

/// Monotonic in-process Hybrid Logical Clock (millisecond precision) ensuring causal
/// local writes always advance past observed remote timestamps even under negative clock skew.
pub(crate) struct HlcClock {
    last_ms: AtomicU64,
}

impl HlcClock {
    pub(crate) fn new(initial_ms: u64) -> Self {
        Self {
            last_ms: AtomicU64::new(initial_ms.max(now_unix_ms())),
        }
    }

    pub(crate) fn current(&self) -> u64 {
        self.last_ms.load(Ordering::Acquire).max(now_unix_ms())
    }

    /// Advance the clock for a new local write: `max(wall_ms, prev + 1)`.
    pub(crate) fn tick(&self) -> u64 {
        let wall = now_unix_ms();
        let mut prev = self.last_ms.load(Ordering::Acquire);
        loop {
            let next = wall.max(prev.saturating_add(1));
            match self.last_ms.compare_exchange_weak(
                prev,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return next,
                Err(actual) => prev = actual,
            }
        }
    }

    /// Observe a remote peer's timestamp so any subsequent local `tick()` is strictly greater.
    pub(crate) fn observe(&self, remote_ts_ms: u64) {
        let floor = remote_ts_ms.max(now_unix_ms());
        let mut prev = self.last_ms.load(Ordering::Acquire);
        while floor > prev {
            match self.last_ms.compare_exchange_weak(
                prev,
                floor,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => prev = actual,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::HlcClock;

    #[test]
    fn hlc_ticks_strictly_monotonically_past_future_remote_observation() {
        let clock = HlcClock::new(0);
        let t1 = clock.tick();
        let t2 = clock.tick();
        assert!(t2 > t1);

        let future_remote = t2.saturating_add(60_000);
        clock.observe(future_remote);
        assert!(clock.current() >= future_remote);

        let t3 = clock.tick();
        assert!(t3 > future_remote);
        let t4 = clock.tick();
        assert!(t4 > t3);
    }
}
