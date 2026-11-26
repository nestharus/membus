use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::types::MembusError;

/// Configuration for bounded in-flight write admission backpressure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdmissionConfig {
    pub max_in_flight_ops: usize,
    pub max_in_flight_bytes: usize,
    pub retry_after_ms: u32,
}

impl Default for AdmissionConfig {
    fn default() -> Self {
        Self {
            max_in_flight_ops: 16_384,
            max_in_flight_bytes: 128 * 1024 * 1024,
            retry_after_ms: 5,
        }
    }
}

struct AdmissionInner {
    config: AdmissionConfig,
    in_flight_ops: AtomicUsize,
    in_flight_bytes: AtomicUsize,
}

/// Lock-free atomic admission gate bounding in-flight operation count and byte volume.
///
/// Callers receive an RAII [`AdmissionPermit`] that releases both budgets on drop.
#[derive(Clone)]
pub struct AdmissionGate {
    inner: Arc<AdmissionInner>,
}

impl AdmissionGate {
    pub fn new(config: AdmissionConfig) -> Self {
        Self {
            inner: Arc::new(AdmissionInner {
                config,
                in_flight_ops: AtomicUsize::new(0),
                in_flight_bytes: AtomicUsize::new(0),
            }),
        }
    }

    pub fn try_acquire(&self, bytes: usize) -> Result<AdmissionPermit, MembusError> {
        let mut ops = self.inner.in_flight_ops.load(Ordering::Relaxed);
        loop {
            if ops >= self.inner.config.max_in_flight_ops {
                return Err(MembusError::Busy {
                    retry_after_ms: self.inner.config.retry_after_ms,
                    reason: format!(
                        "in-flight op limit reached ({}/{})",
                        ops, self.inner.config.max_in_flight_ops
                    ),
                });
            }
            match self.inner.in_flight_ops.compare_exchange_weak(
                ops,
                ops + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => ops = actual,
            }
        }

        let mut cur_bytes = self.inner.in_flight_bytes.load(Ordering::Relaxed);
        loop {
            let next_bytes = cur_bytes.saturating_add(bytes);
            if next_bytes > self.inner.config.max_in_flight_bytes {
                self.inner.in_flight_ops.fetch_sub(1, Ordering::AcqRel);
                return Err(MembusError::Busy {
                    retry_after_ms: self.inner.config.retry_after_ms,
                    reason: format!(
                        "in-flight byte budget reached ({}+{bytes} > {})",
                        cur_bytes, self.inner.config.max_in_flight_bytes
                    ),
                });
            }
            match self.inner.in_flight_bytes.compare_exchange_weak(
                cur_bytes,
                next_bytes,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => cur_bytes = actual,
            }
        }

        Ok(AdmissionPermit {
            inner: Arc::clone(&self.inner),
            bytes,
        })
    }

    pub fn in_flight_ops(&self) -> usize {
        self.inner.in_flight_ops.load(Ordering::Acquire)
    }

    pub fn in_flight_bytes(&self) -> usize {
        self.inner.in_flight_bytes.load(Ordering::Acquire)
    }
}

/// RAII permit releasing reserved op and byte capacity on drop.
pub struct AdmissionPermit {
    inner: Arc<AdmissionInner>,
    bytes: usize,
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        self.inner
            .in_flight_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
        self.inner.in_flight_ops.fetch_sub(1, Ordering::AcqRel);
    }
}
