//! One additive wire envelope; legacy refusal negotiation and BATCH are unchanged.
use crate::{
    atomic::{AtomicRequest, NamespaceView, PublicationError as P},
    client::{Ack, Client, Deadline, Error, MutationReply, Transport},
    Cursor,
};
pub(crate) fn encode(r: Result<Vec<u8>, P>) -> Vec<u8> {
    let mut b = b"MAW1".to_vec();
    match r {
        Ok(v) => {
            b.push(0);
            b.extend(v);
        }
        Err(e) => {
            b.push(match e {
                P::Conflict => 1,
                P::Expired => 2,
                P::Unsupported => 3,
                P::Resource(_) => 4,
                P::Invalid => 5,
                P::IdentityMismatch => 6,
                P::Uncertain => 7,
                P::RestartUncertain => 8,
                P::RetryExpired => 9,
                P::Unavailable => 10,
            });
            if let P::Resource(c) = e {
                b.extend(c.to_le_bytes());
            }
        }
    }
    b
}
pub(crate) fn decode(b: &[u8]) -> Result<&[u8], Error> {
    if b.get(..4) != Some(b"MAW1") {
        return Err(Error::Protocol("atomic reply version"));
    }
    let mut c = Cursor::new(&b[4..]);
    let status = c.u8().map_err(|_| Error::Protocol("atomic status"))?;
    if status == 0 {
        return Ok(c.remainder());
    }
    let error = match status {
        1 => P::Conflict,
        2 => P::Expired,
        3 => P::Unsupported,
        4 => {
            let n = c.u16().map_err(|_| Error::Protocol("atomic resource"))?;
            if !matches!(n, 4 | 6 | 7 | 8 | 9 | 18) {
                return Err(Error::Protocol("atomic resource code"));
            }
            P::Resource(n)
        }
        5 => P::Invalid,
        6 => P::IdentityMismatch,
        7 => P::Uncertain,
        8 => P::RestartUncertain,
        9 => P::RetryExpired,
        10 => P::Unavailable,
        _ => return Err(Error::Protocol("unknown atomic status")),
    };
    c.end()
        .map_err(|_| Error::Protocol("atomic refusal tail"))?;
    Err(Error::Atomic(error))
}
impl<T: Transport> Client<T> {
    /// A complete bounded FOREVER population, never a page. This call has no mutation retry.
    pub fn capture_atomic_namespace(
        &mut self,
        namespace: &str,
        operation: &str,
        d: Deadline,
    ) -> Result<NamespaceView, Error> {
        self.require_atomic_wire()?;
        let p = self.prefix(operation, namespace, d)?;
        let result = self
            .exchange(crate::ATOMIC_CAPTURE, &p, d.local)
            .and_then(|b| {
                let v = NamespaceView::from_wire(decode(&b)?)
                    .map_err(|_| Error::Protocol("incomplete atomic population"))?;
                if v.wire_identity() != (namespace, self.negotiated().epoch) {
                    return Err(Error::Protocol("atomic capture identity"));
                }
                Ok(v)
            });
        if matches!(result, Err(Error::Protocol(_))) {
            self.usable = false;
        }
        result
    }
    /// Send the exact original, even after reconnect/restart, solely to reconcile it.
    /// Server retained outcomes precede epoch/horizon checks; absent changed-epoch outcomes
    /// remain uncertain. No new view, original ID, horizon, Ack, or automatic retry is created.
    pub fn publish_atomic(
        &mut self,
        r: &AtomicRequest,
        d: Deadline,
    ) -> Result<MutationReply, Error> {
        self.require_atomic_wire()?;
        let n = 8 + 8 + r.namespace().len() + r.operation_id().len() + 4 + r.encoded_len();
        // Local bounds and connection/deadline admission precede mutation entry.
        if n + 13 > self.negotiated().max_frame {
            return Err(Error::Invalid("atomic frame limit"));
        }
        if !r.fits_value_limit(self.negotiated().max_value) {
            return Err(Error::Invalid("value limit"));
        }
        let mut p = self.prefix(r.operation_id(), r.namespace(), d)?;
        crate::lp(&mut p, &r.to_bytes());
        let id = self.begin_exchange(p.len(), d.local)?;
        let bytes = crate::frame(crate::ATOMIC_PUBLISH, id, &p);
        let result = self
            .exchange_bytes(crate::ATOMIC_PUBLISH, id, &bytes, d.local)
            .and_then(|b| {
                let mut c = Cursor::new(decode(&b)?);
                let version = c.u64().map_err(|_| Error::Protocol("atomic version"))?;
                let ack = c.u8().map_err(|_| Error::Protocol("atomic ack"))?;
                c.end()
                    .map_err(|_| Error::Protocol("atomic success tail"))?;
                if version == 0 || ack != r.requested_ack() {
                    return Err(Error::Protocol("atomic achieved acknowledgement"));
                }
                Ok(MutationReply {
                    epoch: self.negotiated().epoch,
                    version,
                    achieved: if ack == 1 { Ack::Os } else { Ack::Flush },
                })
            });
        match result {
            Ok(o) => Ok(o),
            Err(Error::Atomic(e)) if !matches!(e, P::Uncertain | P::RestartUncertain) => {
                Err(Error::Atomic(e))
            }
            Err(Error::Refused(e)) if !matches!(e.code, 16 | 17) => Err(Error::Refused(e)),
            Err(e) => {
                self.usable = false;
                Err(Error::Uncertain {
                    epoch: self.negotiated().epoch,
                    operation_id: r.operation_id().into(),
                    cause: Box::new(e),
                })
            }
        }
    }
}
