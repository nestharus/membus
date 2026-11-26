//! Exact typed projection of the existing policy wire. No Store or implicit retry.
use super::*;
use crate::Class;
use std::collections::HashSet;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    pub prefix: String,
    pub class: Class,
}
/// Request-associated namespace and connection epoch; neither is echoed by POLICY_GET.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolicyTable {
    namespace: String,
    epoch: u64,
    rows: Vec<Policy>,
}
impl PolicyTable {
    pub fn namespace(&self) -> &str { &self.namespace }
    pub fn epoch(&self) -> u64 { self.epoch }
    pub fn rows(&self) -> &[Policy] { &self.rows }
    /// Exact configured prefix, with no implicit FOREVER or longest-prefix substitution.
    pub fn exact(&self, prefix: &str) -> Option<Class> {
        self.rows.iter().find(|p| p.prefix == prefix).map(|p| p.class)
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PolicySetRequest {
    pub operation_id: String,
    pub namespace: String,
    pub prefix: String,
    pub class: Class,
}
/// The same immutable original/epoch/first-send/horizon container as ordinary Attempt.
/// Existing unparameterized Attempt remains Attempt<MutationRequest>.
pub type PolicySetAttempt = Attempt<PolicySetRequest>;

fn namespace_valid(namespace: &str) -> Result<()> {
    identifier(namespace)?;
    if namespace.contains('\0') || namespace == crate::POLICY_NS
        || namespace == crate::timers::NS {
        return Err(Error::Invalid("policy namespace"));
    }
    Ok(())
}
fn request_valid(r: &PolicySetRequest, retry_window_ms: u64) -> Result<()> {
    identifier(&r.operation_id)?;
    namespace_valid(&r.namespace)?;
    if r.prefix.len() > MAX_ID { return Err(Error::Invalid("policy prefix bound")); }
    // Use the defining provider's namespace/prefix scalar bound, including empty prefix.
    crate::chunks::policy_key(&r.namespace, &r.prefix)
        .map_err(|_| Error::Invalid("policy prefix bound"))?;
    if Class::from_wire(r.class.kind, r.class.retention_ms).ok() != Some(r.class)
        || (r.class.kind == 2 && r.class.retention_ms < retry_window_ms) {
        return Err(Error::Invalid("policy class or retention"));
    }
    Ok(())
}
fn decode_rows(payload: &[u8], namespace: &str) -> Result<Vec<Policy>> {
    let mut check = Cursor::new(payload);
    let count = parse(check.u16())? as usize;
    // Every row needs a length, class and u64 retention, even for an empty prefix.
    if count > payload.len().saturating_sub(2) / 13 {
        return Err(Error::Protocol("policy count bound"));
    }
    let mut seen = HashSet::with_capacity(count);
    for _ in 0..count {
        let prefix = std::str::from_utf8(parse(check.bytes(MAX_ID))?)
            .map_err(|_| Error::Protocol("policy prefix UTF8"))?;
        if namespace.len() + 1 + prefix.len() > MAX_ID || !seen.insert(prefix) {
            return Err(Error::Protocol("policy prefix bound or duplicate"));
        }
        let kind = parse(check.u8())?;
        let retention = parse(check.u64())?;
        let class = Class::from_wire(kind, retention)
            .map_err(|_| Error::Protocol("policy class"))?;
        if class.kind != kind || class.retention_ms != retention {
            return Err(Error::Protocol("noncanonical policy class"));
        }
    }
    parse(check.end())?;
    // Only a complete bounded validated table owns prefix strings.
    let mut c = Cursor::new(payload);
    parse(c.u16())?;
    let mut rows = Vec::with_capacity(count);
    for _ in 0..count {
        let prefix = parse(c.bound())?;
        let kind = parse(c.u8())?;
        let retention_ms = parse(c.u64())?;
        rows.push(Policy { prefix, class: Class { kind, retention_ms } });
    }
    parse(c.end())?;
    Ok(rows)
}
impl<T: Transport> Client<T> {
    /// Read exactly the selected namespace's configured policies. No defaults are inferred.
    pub fn policy_get(&mut self, namespace: &str, operation_id: &str,
                      d: Deadline) -> Result<PolicyTable> {
        namespace_valid(namespace)?;
        let p = self.prefix(operation_id, namespace, d)?;
        let b = self.exchange(crate::POLICY_GET, &p, d.local)?;
        let rows = match decode_rows(&b, namespace) {
            Ok(rows) => rows,
            Err(error) => { self.usable = false; return Err(error); }
        };
        Ok(PolicyTable { namespace: namespace.into(), epoch: self.negotiated.epoch, rows })
    }
    /// Prepare one NEW setter. Retain this exact token after every uncertain result.
    pub fn prepare_policy_set(&self, request: PolicySetRequest) -> Result<PolicySetAttempt> {
        request_valid(&request, self.negotiated.retry_window_ms)?;
        let size = 13 + 8 + 3 * 4 + request.operation_id.len()
            + request.namespace.len() + request.prefix.len() + 1 + 8;
        if size > self.negotiated.max_frame { return Err(Error::Invalid("policy frame limit")); }
        Ok(Attempt { request, epoch: self.negotiated.epoch, first_send: None,
                     retry_window_ms: self.negotiated.retry_window_ms })
    }
    /// One existing POLICY_SET, fixed Flush, with original Attempt horizon and exact receipt.
    /// The caller supplies the same original Deadline across an explicit reconnect/revisit.
    pub fn policy_set(&mut self, attempt: &mut PolicySetAttempt,
                      d: Deadline) -> Result<MutationReply> {
        attempt.check_horizon(self.negotiated.epoch)?;
        let r = &attempt.request;
        request_valid(r, attempt.retry_window_ms)?;
        let mut p = self.prefix(&r.operation_id, &r.namespace, d)?;
        lp(&mut p, r.prefix.as_bytes());
        p.push(r.class.kind);
        p.extend(r.class.retention_ms.to_le_bytes());
        // Recheck the actual current connection's frame/deadline before first-send custody.
        let id = self.begin_exchange(p.len(), d.local)?;
        let bytes = frame(crate::POLICY_SET, id, &p);
        attempt.first_send.get_or_insert_with(Instant::now);
        let result = self.exchange_bytes(crate::POLICY_SET, id, &bytes, d.local)
            .and_then(|b| {
                let mut c = Cursor::new(&b);
                let version = parse(c.u64())?;
                let ack = parse(c.u8())?;
                parse(c.end())?;
                if version == 0 || ack != Ack::Flush as u8 {
                    return Err(Error::Protocol("policy achieved acknowledgement"));
                }
                Ok(MutationReply { epoch: self.negotiated.epoch, version, achieved: Ack::Flush })
            });
        match result {
            Ok(v) => Ok(v),
            Err(e @ Error::Invalid(_)) => Err(e),
            Err(Error::Refused(r)) if !matches!(r.code, 16 | 17) => Err(Error::Refused(r)),
            Err(error) => {
                self.usable = false;
                Err(Error::Uncertain { epoch: attempt.epoch,
                    operation_id: r.operation_id.clone(), cause: Box::new(error) })
            }
        }
    }
}
