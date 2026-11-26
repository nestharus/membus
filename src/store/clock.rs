//! Monotonic and UTC store clock with explicit manual test control.
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

/// A clock a test can move by hand. UTC and monotonic time advance together unless a test jumps UTC alone.
pub struct ManualClock {
    utc: AtomicU64,
    mono: AtomicU64,
}
impl ManualClock {
    pub fn new(utc_ms: u64) -> Arc<Self> {
        Arc::new(Self { utc: AtomicU64::new(utc_ms), mono: AtomicU64::new(0) })
    }
    fn update(field: &AtomicU64, f: impl Fn(u64) -> u64) {
        let mut current = field.load(Ordering::SeqCst);
        loop {
            match field.compare_exchange_weak(current, f(current), Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }
    /// Advance both counters, saturating each at the representable limit.
    pub fn advance(&self, ms: u64) {
        Self::update(&self.utc, |now| now.saturating_add(ms));
        Self::update(&self.mono, |now| now.saturating_add(ms));
    }
    /// A wall-clock correction with no elapsed time.
    pub fn jump_utc(&self, delta_ms: i64) {
        Self::update(&self.utc, |now| now.saturating_add_signed(delta_ms));
    }
    pub fn utc_ms(&self) -> u64 {
        self.utc.load(Ordering::SeqCst)
    }
}
#[derive(Clone)]
pub struct Clock {
    manual: Option<Arc<ManualClock>>,
    start: Instant,
}
impl Clock {
    pub fn new(manual: Option<Arc<ManualClock>>) -> Self {
        Self { manual, start: Instant::now() }
    }
    pub fn utc_ms(&self) -> u64 {
        match &self.manual {
            Some(m) => m.utc.load(Ordering::SeqCst),
            None => crate::now_ms(),
        }
    }
    pub fn mono_ms(&self) -> u64 {
        match &self.manual {
            Some(m) => m.mono.load(Ordering::SeqCst),
            None => self.start.elapsed().as_millis() as u64,
        }
    }
    /// High-resolution elapsed time for capture diagnostics, in the same clock domain.
    pub(crate) fn mono_ns(&self) -> u64 {
        match &self.manual {
            Some(m) => m.mono.load(Ordering::SeqCst).saturating_mul(1_000_000),
            None => self.start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        }
    }
}
