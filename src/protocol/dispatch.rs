//! Wire negotiation, connection state, and decomposed per-family opcode dispatch.
use super::codec::{
    encode_value, lp, parse_mutation, reply, reply_header, Cursor, ATOMIC_CAPTURE,
    ATOMIC_POPULATION, ATOMIC_PUBLISH, BATCH, CAPTURE_METRICS, CODES, CONDITIONAL_PUT, GET,
    GET_MANY, GET_STATUS, HEALTH, HEALTH2, HELLO, IO_METRICS, MAX_BATCH, MAX_FRAME, MAX_VALUE,
    POLICY_GET, POLICY_SET, PUT, REMOVE, SCAN_KEYS_PREFIX, SCAN_KEYS_RANGE, SCAN_PREFIX,
    SCAN_RANGE, SIGNAL_USAGE, SNAPSHOT_OPEN, SNAPSHOT_OPEN_SELECTED, SNAPSHOT_RELEASE,
    SNAPSHOT_REQUIRE_COMPLETE, SUBSCRIBE, SUBSCRIPTION_STATUS, TIMER_CANCEL, TIMER_SET,
    TIMER_STATUS, UNSUBSCRIBE,
};
use crate::{chunks, timers, Class, Mutation, Store};
use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Mutex},
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Extension {
    Atomic,
    FiniteHints,
}

fn subscription_profile(
    identity: &str,
    subscribe: bool,
    connected: bool,
) -> Result<(Extension, &'static [u8]), u16> {
    use crate::subscription_contract::{DISCOVERY, FINITE_DISCOVERY};
    let mut owned = false;
    let mut finite = false;
    for feature in identity.split(';').skip(1) {
        owned |= feature == "subscription-contract=1";
        finite |= feature == "subscription-contract=2";
    }
    match (owned, connected && subscribe && finite) {
        (true, true) => Err(2),
        (true, false) => Ok((Extension::Atomic, DISCOVERY)),
        (false, true) => Ok((Extension::FiniteHints, FINITE_DISCOVERY)),
        (false, false) => Ok((Extension::Atomic, &[])),
    }
}

#[cfg(test)]
pub(crate) fn append_subscription_contract_discovery(identity: &str, payload: &mut Vec<u8>) {
    let subscribe = identity
        .split(';')
        .skip(1)
        .any(|feature| feature == "subscribe=1");
    payload.extend_from_slice(subscription_profile(identity, subscribe, true).unwrap().1);
}

#[derive(Clone, Copy)]
struct NegotiatedConnection {
    max_frame: usize,
    allow_mem: bool,
    subscribe: bool,
    timers: bool,
    extension: Extension,
}

/// Wire negotiation and opcode selection for exactly one transport connection.
#[derive(Default)]
pub struct ConnectionDispatch {
    negotiated: Option<NegotiatedConnection>,
}

impl ConnectionDispatch {
    pub fn is_ready(&self) -> bool {
        self.negotiated.is_some()
    }

    pub fn max_frame(&self) -> usize {
        self.negotiated.map_or(MAX_FRAME, |hello| hello.max_frame)
    }

    pub fn dispatch(&mut self, store: &Store, body: &[u8]) -> Vec<u8> {
        store.dispatch_frame(body, false, self.max_frame(), Some(self))
    }
}

enum DispatchReply {
    Hello(Vec<u8>, NegotiatedConnection),
    Payload(Vec<u8>),
    Framed(Vec<u8>),
}

impl Store {
    pub fn dispatch(&self, b: &[u8]) -> Vec<u8> {
        self.dispatch_for(b, self.config.allow_mem)
    }

    pub fn dispatch_for(&self, b: &[u8], allow_mem: bool) -> Vec<u8> {
        self.dispatch_for_limit(b, allow_mem, MAX_FRAME)
    }

    pub fn dispatch_for_limit(&self, b: &[u8], allow_mem: bool, max_frame: usize) -> Vec<u8> {
        self.dispatch_frame(b, allow_mem, max_frame, None)
    }

    fn dispatch_frame(
        &self,
        b: &[u8],
        allow_mem: bool,
        max_frame: usize,
        connection: Option<&mut ConnectionDispatch>,
    ) -> Vec<u8> {
        let max_frame = max_frame.min(MAX_FRAME);
        if b.len() > max_frame {
            return reply(
                b.get(4).copied().unwrap_or(0),
                b.get(5..13)
                    .map(|v| u64::from_le_bytes(v.try_into().unwrap()))
                    .unwrap_or(0),
                4,
                &[],
            );
        }
        let mut c = Cursor::new(b);
        let header = (|| -> Result<_, u16> { Ok((c.u16()?, c.u16()?, c.u8()?, c.u64()?)) })();
        let (major, minor, op, id) = match header {
            Ok(h) => h,
            Err(code) => return reply(0, 0, code, &[]),
        };
        let negotiated = connection.as_ref().and_then(|session| session.negotiated);
        if connection.is_some() && (op == HELLO) == negotiated.is_some() {
            return reply(op, id, 3, &[]);
        }
        let extension = negotiated.map_or(Extension::Atomic, |hello| hello.extension);
        let allow_mem = negotiated.map_or(allow_mem, |hello| hello.allow_mem);
        if let Some(hello) = negotiated {
            if (!hello.subscribe && (SUBSCRIBE..=SIGNAL_USAGE).contains(&op))
                || (!hello.timers && (TIMER_SET..=TIMER_STATUS).contains(&op))
                || (extension == Extension::FiniteHints && op == SNAPSHOT_REQUIRE_COMPLETE)
            {
                return reply(op, id, 2, &[]);
            }
        }
        if major != 1 || minor != 0 {
            return reply(op, id, 1, &[]);
        }
        let result = self.route_operation(
            &mut c,
            op,
            id,
            allow_mem,
            max_frame,
            extension,
            connection.is_some(),
        );
        match result {
            Ok(DispatchReply::Hello(p, hello)) if p.len() + 15 <= max_frame.min(hello.max_frame) => {
                if let Some(connection) = connection {
                    connection.negotiated = Some(hello);
                }
                reply(op, id, 0, &p)
            }
            Ok(DispatchReply::Payload(p)) if p.len() + 15 <= max_frame => reply(op, id, 0, &p),
            Ok(DispatchReply::Framed(p)) if p.len() <= max_frame + 4 => p,
            Ok(_) => reply(op, id, 4, &[]),
            Err(code) => reply(op, id, code, &[]),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn route_operation(
        &self,
        c: &mut Cursor<'_>,
        op: u8,
        id: u64,
        allow_mem: bool,
        max_frame: usize,
        extension: Extension,
        connected: bool,
    ) -> Result<DispatchReply, u16> {
        if op == HELLO {
            return self.dispatch_hello(c, connected);
        }
        let deadline = c.u64()?;
        // These identifiers belong to the request for this dispatch only.
        // Operations that retain work take their own copies below.
        let op_id = c.text_ref()?;
        let ns = c.text_ref()?;
        // Internal schedule/progress records are not a public KV namespace.
        if ns == timers::NS || ns == chunks::POLICY_NS {
            return Err(3);
        }
        if matches!(op, PUT | CONDITIONAL_PUT | REMOVE) {
            return self.dispatch_single_mutation(c, op, deadline, op_id, ns, allow_mem);
        }
        if extension == Extension::FiniteHints && (ATOMIC_CAPTURE..=ATOMIC_POPULATION).contains(&op)
        {
            if self.clock.utc_ms() >= deadline {
                return Err(5);
            }
            return self
                .finite_control(op, ns, c)
                .map(DispatchReply::Payload);
        }
        if matches!(op, ATOMIC_CAPTURE | ATOMIC_PUBLISH | ATOMIC_POPULATION) {
            return self.dispatch_atomic(c, op, id, ns, op_id, deadline, max_frame);
        }
        if self.clock.utc_ms() >= deadline && op != POLICY_SET {
            return Err(5);
        }
        if matches!(
            op,
            SCAN_PREFIX | SCAN_RANGE | SCAN_KEYS_PREFIX | SCAN_KEYS_RANGE
        ) {
            return self
                .scan_reply(ns, c, op, id, max_frame)
                .map(DispatchReply::Framed);
        }
        match op {
            GET | GET_MANY => self.dispatch_committed_read(c, op, id, ns, max_frame),
            TIMER_SET | TIMER_CANCEL | TIMER_STATUS => self
                .timer_call(op, ns.to_owned(), op_id.to_owned(), deadline, c)
                .map(DispatchReply::Payload),
            POLICY_SET | POLICY_GET => self
                .dispatch_policy(c, op, ns, op_id, deadline)
                .map(DispatchReply::Payload),
            SUBSCRIBE | UNSUBSCRIBE | SUBSCRIPTION_STATUS | SIGNAL_USAGE => self
                .dispatch_signals(c, op, ns)
                .map(DispatchReply::Payload),
            SNAPSHOT_OPEN | SNAPSHOT_OPEN_SELECTED | SNAPSHOT_RELEASE
            | SNAPSHOT_REQUIRE_COMPLETE => self
                .dispatch_snapshots(c, op, ns, max_frame)
                .map(DispatchReply::Payload),
            BATCH => self
                .dispatch_batch(c, ns, deadline, allow_mem)
                .map(DispatchReply::Payload),
            GET_STATUS | HEALTH | HEALTH2 | IO_METRICS | CAPTURE_METRICS => self
                .dispatch_status_and_metrics(c, op, ns)
                .map(DispatchReply::Payload),
            _ => Err(2),
        }
    }

    fn dispatch_hello(
        &self,
        c: &mut Cursor<'_>,
        connected: bool,
    ) -> Result<DispatchReply, u16> {
        let max = c.u32()? as usize;
        let mem = c.u8()?;
        let client = c.text()?;
        c.end()?;
        if max < 1024 {
            return Err(4);
        }
        if mem > 1 {
            return Err(3);
        }
        let selected_max = MAX_FRAME.min(max);
        let mut p = self.epoch.to_le_bytes().to_vec();
        p.extend((selected_max as u32).to_le_bytes());
        p.extend((MAX_VALUE as u32).to_le_bytes());
        p.extend((MAX_BATCH as u16).to_le_bytes());
        p.push(self.config.allow_mem as u8);
        p.extend(self.config.retry_ms.to_le_bytes());
        // Legacy strict clients keep the exact original refusal list.
        let subscribe = client
            .split(';')
            .skip(1)
            .any(|feature| feature == "subscribe=1");
        let scans = client.ends_with(";scans=1");
        let timers = client
            .split(';')
            .skip(1)
            .any(|feature| feature == "timers=1");
        let mut timer_only_codes = CODES[..22].to_vec();
        timer_only_codes.push(CODES[24]);
        let codes = if timers && !subscribe {
            timer_only_codes.as_slice()
        } else if timers {
            CODES
        } else if subscribe {
            &CODES[..24]
        } else if scans {
            &CODES[..22]
        } else {
            &CODES[..19]
        };
        p.extend((codes.len() as u16).to_le_bytes());
        for &(code, name, transient) in codes {
            p.extend(code.to_le_bytes());
            p.push(transient as u8);
            lp(&mut p, name.as_bytes());
        }
        if subscribe {
            p.extend(self.signals.hello_limits());
        }
        if timers {
            p.extend(timers::hello_limits());
        }
        let (extension, discovery) = subscription_profile(&client, subscribe, connected)?;
        p.extend_from_slice(discovery);
        Ok(DispatchReply::Hello(
            p,
            NegotiatedConnection {
                max_frame: selected_max,
                allow_mem: mem != 0,
                subscribe,
                timers,
                extension,
            },
        ))
    }

    fn dispatch_single_mutation(
        &self,
        c: &mut Cursor<'_>,
        op: u8,
        deadline: u64,
        op_id: &str,
        ns: &str,
        allow_mem: bool,
    ) -> Result<DispatchReply, u16> {
        let m = parse_mutation(c, op, deadline, op_id.to_owned(), ns.to_owned())?;
        c.end()?;
        if m.ack == 0 && !allow_mem {
            return Err(15);
        }
        let o = self.mutate(m);
        if o.code != 0 {
            return Err(o.code);
        }
        Ok(DispatchReply::Payload(o.bytes()))
    }

    #[allow(clippy::too_many_arguments)]
    fn dispatch_atomic(
        &self,
        c: &mut Cursor<'_>,
        op: u8,
        id: u64,
        ns: &str,
        op_id: &str,
        deadline: u64,
        max_frame: usize,
    ) -> Result<DispatchReply, u16> {
        match op {
            ATOMIC_CAPTURE => {
                c.end()?;
                let result = if self.clock.utc_ms() >= deadline {
                    Err(crate::atomic::PublicationError::Expired)
                } else {
                    self.capture_namespace_for_wire(ns, max_frame.saturating_sub(20), id)
                };
                Ok(match result {
                    Ok(frame) => DispatchReply::Framed(frame),
                    Err(error) => DispatchReply::Payload(crate::atomic_wire::encode(Err(error))),
                })
            }
            ATOMIC_PUBLISH => {
                let bytes = c.bytes(MAX_FRAME / 3)?;
                c.end()?;
                let result = self
                    .publish_atomic_for_wire(ns, op_id, bytes, deadline)
                    .map(|o| o.bytes());
                Ok(DispatchReply::Payload(crate::atomic_wire::encode(result)))
            }
            ATOMIC_POPULATION => {
                let limits = crate::atomic_population::PopulationLimits::decode(c)?;
                c.end()?;
                let result = if self.clock.utc_ms() >= deadline {
                    Err(crate::atomic::PublicationError::Expired)
                } else {
                    self.atomic_population_header(ns, limits, max_frame.saturating_sub(20))
                };
                Ok(DispatchReply::Payload(crate::atomic_wire::encode(result)))
            }
            _ => Err(2),
        }
    }

    fn dispatch_committed_read(
        &self,
        c: &mut Cursor<'_>,
        op: u8,
        id: u64,
        ns: &str,
        max_frame: usize,
    ) -> Result<DispatchReply, u16> {
        match op {
            GET => {
                let key = c.text_ref()?;
                c.end()?;
                self.committed_read_reply(ns, std::slice::from_ref(&key), op, id, max_frame)
                    .map(DispatchReply::Framed)
            }
            GET_MANY => {
                let n = c.u16()? as usize;
                if n > MAX_BATCH {
                    return Err(4);
                }
                let mut keys = Vec::with_capacity(n);
                for _ in 0..n {
                    keys.push(c.text_ref()?);
                }
                c.end()?;
                self.committed_read_reply(ns, &keys, op, id, max_frame)
                    .map(DispatchReply::Framed)
            }
            _ => Err(2),
        }
    }

    fn dispatch_policy(
        &self,
        c: &mut Cursor<'_>,
        op: u8,
        ns: &str,
        op_id: &str,
        deadline: u64,
    ) -> Result<Vec<u8>, u16> {
        match op {
            POLICY_SET => {
                let prefix = c.bound()?;
                let kind = c.u8()?;
                let retention = c.u64()?;
                c.end()?;
                let class = if kind == 255 {
                    None
                } else {
                    Some(Class::from_wire(kind, retention)?)
                };
                let o = self.set_policy_with_id(ns, &prefix, class, op_id, deadline);
                if o.code != 0 {
                    return Err(o.code);
                }
                Ok(o.bytes())
            }
            POLICY_GET => {
                c.end()?;
                let rows: Vec<_> = self.policies().into_iter().filter(|r| r.0 == ns).collect();
                let count = u16::try_from(rows.len()).map_err(|_| 4u16)?;
                let mut p = count.to_le_bytes().to_vec();
                for (_, prefix, class) in rows {
                    lp(&mut p, prefix.as_bytes());
                    p.push(class.kind);
                    p.extend(class.retention_ms.to_le_bytes());
                }
                Ok(p)
            }
            _ => Err(2),
        }
    }

    fn dispatch_signals(&self, c: &mut Cursor<'_>, op: u8, ns: &str) -> Result<Vec<u8>, u16> {
        match op {
            SUBSCRIBE => {
                let prefix = c.bound()?;
                let endpoint = c.text()?;
                let values = c.u8()?;
                if values > 1 {
                    return Err(3);
                }
                let cursor = c.bytes(16)?;
                let protocol = c.text()?;
                let frame_max = c.u32()? as usize;
                c.end()?;
                let state = self.state.lock().unwrap();
                self.signals.subscribe_locked(
                    ns,
                    prefix,
                    endpoint,
                    values != 0,
                    cursor,
                    &protocol,
                    frame_max,
                    &self.signal_budget(&state),
                )
            }
            UNSUBSCRIBE | SUBSCRIPTION_STATUS => {
                let token = c.bytes(16)?;
                c.end()?;
                if op == UNSUBSCRIBE {
                    self.signals.unsubscribe(ns, token)?;
                    Ok(Vec::new())
                } else {
                    self.signals.status(ns, token)
                }
            }
            SIGNAL_USAGE => {
                c.end()?;
                Ok(self.signals.usage())
            }
            _ => Err(2),
        }
    }

    fn dispatch_snapshots(
        &self,
        c: &mut Cursor<'_>,
        op: u8,
        ns: &str,
        max_frame: usize,
    ) -> Result<Vec<u8>, u16> {
        match op {
            SNAPSHOT_OPEN_SELECTED => {
                use crate::selected_view::{AcquisitionBudget, Selection};
                let kind = c.u8()?;
                let lower = c.bound_ref()?;
                let selection = match kind {
                    0 => Selection::Prefix(lower),
                    1 => Selection::Range {
                        lower,
                        upper: c.bound_ref()?,
                    },
                    _ => return Err(3),
                };
                let budget = AcquisitionBudget {
                    max_candidates: c.u64()?,
                    max_metadata_bytes: c.u64()?,
                    max_retained_bytes: c.u64()?,
                };
                c.end()?;
                // Admit the entire fixed reply BEFORE creating a resident token.
                if max_frame < 67 {
                    return Err(4);
                }
                let view = self
                    .snapshot_open_selected(ns, selection, budget)
                    .map_err(|e| e.code())?;
                let mut p = Vec::with_capacity(52);
                lp(&mut p, &view.snapshot);
                p.extend(view.population.to_le_bytes());
                p.extend(view.candidates.to_le_bytes());
                p.extend(view.metadata_bytes.to_le_bytes());
                p.extend(view.retained_bytes.to_le_bytes());
                Ok(p)
            }
            SNAPSHOT_OPEN => {
                c.end()?;
                let (token, _) = self.snapshot_open(ns)?;
                let mut p = Vec::new();
                lp(&mut p, &token);
                Ok(p)
            }
            SNAPSHOT_RELEASE => {
                let token = c.bytes(16)?;
                c.end()?;
                self.snapshot_release(ns, token)?;
                Ok(Vec::new())
            }
            SNAPSHOT_REQUIRE_COMPLETE => {
                let token = c.bytes(16)?;
                c.end()?;
                self.snapshot_require_complete(ns, token)?;
                Ok(Vec::new())
            }
            _ => Err(2),
        }
    }

    fn dispatch_batch(
        &self,
        c: &mut Cursor<'_>,
        ns: &str,
        deadline: u64,
        allow_mem: bool,
    ) -> Result<Vec<u8>, u16> {
        let n = c.u16()? as usize;
        if n > MAX_BATCH {
            return Err(4);
        }
        let mut items = Vec::with_capacity(n);
        for _ in 0..n {
            let op = c.u8()?;
            if !matches!(op, PUT | CONDITIONAL_PUT | REMOVE) {
                return Err(2);
            }
            let id = c.text()?;
            let item = parse_mutation(c, op, deadline, id, ns.to_owned())?;
            if item.ack == 0 && !allow_mem {
                return Err(15);
            }
            items.push(item);
        }
        c.end()?;
        let mut p = (n as u16).to_le_bytes().to_vec();
        // Distinct keys overlap, including their flush waits. Same-key
        // operations retain input order; replies always retain input order.
        let mut keys: HashMap<String, Vec<(usize, Mutation)>> = HashMap::new();
        for (i, m) in items.into_iter().enumerate() {
            keys.entry(m.key.clone()).or_default().push((i, m));
        }
        // One executing batch per store, at most 32 scoped helpers.
        // Refuse before any item admission if another batch owns it.
        let _batch = self.batch_gate.try_lock().map_err(|_| 6u16)?;
        let helpers = keys.len().min(32);
        let work = Mutex::new(keys.into_values());
        let mut results = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..helpers)
                .map(|_| {
                    let work = &work;
                    scope.spawn(move || {
                        let mut out = Vec::new();
                        loop {
                            let items = work.lock().unwrap().next();
                            let Some(items) = items else {
                                break;
                            };
                            out.extend(items.into_iter().map(|(i, m)| (i, self.mutate(m))));
                        }
                        out
                    })
                })
                .collect();
            handles
                .into_iter()
                .flat_map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        });
        results.sort_by_key(|(i, _)| *i);
        for (_, o) in results {
            p.extend(o.code.to_le_bytes());
            p.extend(o.bytes());
        }
        Ok(p)
    }

    fn dispatch_status_and_metrics(
        &self,
        c: &mut Cursor<'_>,
        op: u8,
        ns: &str,
    ) -> Result<Vec<u8>, u16> {
        match op {
            GET_STATUS => {
                let key = c.text()?;
                c.end()?;
                let (status, version) = self.get_status(ns, &key);
                let mut p = vec![status];
                p.extend(version.to_le_bytes());
                Ok(p)
            }
            HEALTH2 => {
                c.end()?;
                let fields = self.health2().fields();
                let count = u16::try_from(fields.len()).map_err(|_| 4u16)?;
                let mut p = count.to_le_bytes().to_vec();
                for (k, v) in fields {
                    lp(&mut p, format!("{k}={v}").as_bytes());
                }
                Ok(p)
            }
            CAPTURE_METRICS => {
                c.end()?;
                let mut p = Vec::new();
                for metric in &self.capture_metrics {
                    p.extend(metric.load(Ordering::Relaxed).to_le_bytes());
                }
                Ok(p)
            }
            IO_METRICS => {
                c.end()?;
                let (bytes, writes, flushes) = self.io_metrics();
                let mut p = bytes.to_le_bytes().to_vec();
                p.extend(writes.to_le_bytes());
                p.extend(flushes.to_le_bytes());
                Ok(p)
            }
            HEALTH => {
                c.end()?;
                let (b, f, k) = self.metrics();
                let mut p = b.to_le_bytes().to_vec();
                p.extend(f.to_le_bytes());
                p.extend((k as u64).to_le_bytes());
                let namespace_keys = self
                    .state
                    .lock()
                    .unwrap()
                    .namespace_counts
                    .get(ns)
                    .copied()
                    .unwrap_or(0);
                p.extend((namespace_keys as u64).to_le_bytes());
                p.extend((self.config.quota as u64).to_le_bytes());
                p.extend((self.config.index_cap as u64).to_le_bytes());
                p.push(((k as u128) * 100 >= (self.config.index_cap as u128) * 70) as u8);
                p.push(((namespace_keys as u128) * 100 >= (self.config.quota as u128) * 70) as u8);
                Ok(p)
            }
            _ => Err(2),
        }
    }

    fn committed_read_reply(
        &self,
        ns: &str,
        keys: &[&str],
        op: u8,
        id: u64,
        max_frame: usize,
    ) -> Result<Vec<u8>, u16> {
        let available = max_frame.checked_sub(15).ok_or(4u16)?;
        let counted = op == GET_MANY;
        let mut payload_len = if counted { 2 } else { 0 };
        if payload_len > available {
            return Err(4);
        }
        let mut selected = Vec::with_capacity(keys.len());
        for &key in keys {
            let entry = crate::store::committed_get(&self.committed.read().unwrap(), ns, key)
                .filter(|entry| !entry.tombstone)
                .cloned();
            payload_len = payload_len
                .checked_add(entry.as_ref().map_or(1, |entry| 14 + entry.len))
                .filter(|size| *size <= available)
                .ok_or(4u16)?;
            selected.push(entry);
        }
        let mut response = reply_header(op, id, 0, payload_len);
        if counted {
            response.extend((keys.len() as u16).to_le_bytes());
        }
        for (&key, entry) in keys.iter().zip(selected) {
            let value = entry
                .map(|entry| self.read_entry(ns, key, &entry))
                .transpose()
                .map_err(|_| 16u16)?;
            encode_value(&mut response, value.as_deref());
        }
        debug_assert_eq!(response.len(), payload_len + 19);
        Ok(response)
    }

    fn finite_control(&self, op: u8, ns: &str, c: &mut Cursor<'_>) -> Result<Vec<u8>, u16> {
        use crate::subscription_contract::*;
        if op == FINITE_CREATE {
            let options = HintOptions {
                profile: c.u16()?,
                lifetime_ms: c.u64()?,
                write_ms: c.u32()?,
                incarnation: c.bytes(16)?.try_into().map_err(|_| 3u16)?,
                recipient: c.bytes(16)?.try_into().map_err(|_| 3u16)?,
            };
            let prefix = c.bound_ref()?;
            let endpoint = c.text_ref()?;
            c.end()?;
            return Ok(self
                .subscribe_finite_hints(ns, prefix, endpoint, options)?
                .bytes()
                .to_vec());
        }
        let receipt = HintReceipt::decode(c.bytes(64)?)?;
        c.end()?;
        if op == FINITE_CANCEL {
            self.cancel_finite_hints(ns, receipt)?;
            Ok(Vec::new())
        } else {
            Ok(self.renew_finite_hints(ns, receipt)?.bytes().to_vec())
        }
    }
}
