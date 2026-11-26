use crate::{ConnectionDispatch, Store};
use std::{
    io::{self, Write},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

/// Shared only by the worker owner and the transports of this generation.
#[derive(Default)]
pub(crate) struct RequestGate {
    state: AtomicUsize,
}

// Admission and close must have a single ordering. Closing leaves the count
// of already admitted dispatches intact. Further requests cannot increase it.
const CLOSED: usize = 1 << (usize::BITS - 1);

struct Request<'a>(&'a RequestGate);

impl Drop for Request<'_> {
    fn drop(&mut self) {
        self.0.state.fetch_sub(1, Ordering::AcqRel);
    }
}

impl RequestGate {
    fn admit(&self) -> io::Result<Request<'_>> {
        match self
            .state
            .try_update(Ordering::AcqRel, Ordering::Acquire, |state| {
                (state < CLOSED - 1).then(|| state + 1)
            }) {
            Ok(_) => Ok(Request(self)),
            Err(state) if state & CLOSED != 0 => Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "resident request admission closed",
            )),
            Err(_) => Err(io::Error::other("resident request count exhausted")),
        }
    }

    pub(crate) fn close(&self) {
        self.state.fetch_or(CLOSED, Ordering::AcqRel);
    }

    pub(crate) fn drained(&self) -> bool {
        self.state.load(Ordering::Acquire) == CLOSED
    }
}

/// The selected resident Store and its original worker owner's wire admission.
/// Clones held by listeners or connections continue to hold the Store.
#[derive(Clone)]
pub struct ResidentDispatch {
    pub(crate) store: Arc<Store>,
    pub(crate) gate: Arc<RequestGate>,
}

impl ResidentDispatch {
    /// Dispatch one complete body and write its framed reply on the same connection.
    /// Returns whether that connection may read another frame. A failed reply
    /// write can follow a committed mutation and is returned to the transport.
    pub fn dispatch_reply(
        &self,
        connection: &mut ConnectionDispatch,
        body: &[u8],
        stream: &mut impl Write,
    ) -> io::Result<bool> {
        let _admission = self.gate.admit()?;
        let was_ready = connection.is_ready();
        let response = connection.dispatch(&self.store, body);
        let code = u16::from_le_bytes(response[17..19].try_into().unwrap());
        stream.write_all(&response)?;
        Ok(code != 1 && (was_ready || connection.is_ready()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn closed_admission_drains_only_after_every_original_returns() {
        let gate = RequestGate::default();
        let first = gate.admit().unwrap();
        let last = gate.admit().unwrap();
        gate.close();
        assert_eq!(
            gate.admit().err().unwrap().kind(),
            io::ErrorKind::ConnectionAborted
        );
        drop(first);
        assert!(!gate.drained());
        gate.close();
        drop(last);
        assert!(gate.drained());
        assert!(gate.admit().is_err());
    }

    #[test]
    fn premature_consuming_join_keeps_capture_and_restart_closed() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join(".test-output")
            .join(format!(
                "resident-unsettled-{}-{}",
                std::process::id(),
                crate::now_ms()
            ));
        let store = Arc::new(Store::open(&root, crate::Config::default()).unwrap());
        let workers = store.start_resident_workers(0).unwrap();
        let wire = workers.wire_dispatch(&store).unwrap();
        let request = wire.gate.admit().unwrap();
        assert!(workers
            .join()
            .unwrap_err()
            .to_string()
            .contains("requests pending=true"));
        drop(request);
        assert!(crate::timers::require_joined(&store.timers).is_err());
        assert!(store.start_resident_workers(0).is_err());
    }
}
