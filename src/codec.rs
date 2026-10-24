//! Wire framing, bounded decoding, durable payload encoding and opcode dispatch.
use crate::chunks;
use crate::timers;
use crate::Class;
use crate::Mutation;
use crate::Outcome;
use crate::Store;
use crate::Value;
use std::collections::HashMap;
use std::io;
use std::io::Read;
use std::sync::atomic::Ordering;
use std::sync::Mutex;

pub const MAX_FRAME: usize = 16 * 1024 * 1024;
pub const MAX_VALUE: usize = 4 * 1024 * 1024;
pub const MAX_ID: usize = 1024;
pub const MAX_BATCH: usize = 128;
pub const CODES: &[(u16, &str, bool)] = &[
    (1, "incompatible_version", false),
    (2, "unsupported", false),
    (3, "malformed", false),
    (4, "too_large", false),
    (5, "deadline", true),
    (6, "busy", true),
    (7, "capacity", false),
    (8, "quota", false),
    (9, "index_cap", false),
    (10, "read_only", true),
    (11, "warming", true),
    (12, "conflict", false),
    (13, "retry_window_expired", false),
    (14, "op_id_mismatch", false),
    (15, "mem_not_enabled", false),
    (16, "io_failed_uncertain", true),
    (17, "pending_timeout_uncertain", true),
    (18, "dedup_full", true),
    (19, "pending_conflict", true),
    (20, "cursor_expired", false),
    (21, "invalid_cursor", false),
    (22, "snapshot_full", true),
    (23, "resume_gap", false),
    (24, "subscription_missing", false),
    (25, "timer_missing", false),
];
pub const HELLO: u8 = 1;
pub const GET: u8 = 2;
pub const GET_MANY: u8 = 3;
pub const PUT: u8 = 4;
pub const CONDITIONAL_PUT: u8 = 5;
pub const REMOVE: u8 = 6;
pub const BATCH: u8 = 7;
pub const HEALTH: u8 = 8;
/// Additive kv/1 minor0 diagnostic operation; existing HEALTH layout is unchanged.
pub const IO_METRICS: u8 = 9;
pub const SNAPSHOT_OPEN: u8 = 10;
pub const SCAN_PREFIX: u8 = 11;
pub const SCAN_RANGE: u8 = 12;
pub const SNAPSHOT_RELEASE: u8 = 13;
pub const SCAN_KEYS_PREFIX: u8 = 14;
pub const SCAN_KEYS_RANGE: u8 = 15;
pub const SUBSCRIBE: u8 = 16;
pub const UNSUBSCRIBE: u8 = 17;
pub const SUBSCRIPTION_STATUS: u8 = 18;
pub const SIGNAL_USAGE: u8 = 19;
pub const CAPTURE_METRICS: u8 = 20;
pub const TIMER_SET: u8 = 21;
pub const TIMER_CANCEL: u8 = 22;
pub const TIMER_STATUS: u8 = 23;
/// Chunk retirement, additive under kv/1 minor 0 (CHUNK-RETIREMENT.md section 6).
pub const POLICY_SET: u8 = 24;
pub const POLICY_GET: u8 = 25;
pub const GET_STATUS: u8 = 26;
pub const HEALTH2: u8 = 27;
const fn crc32_table() -> [u32; 256] {
    let mut table = [0; 256];
    let mut index = 0;
    while index < table.len() {
        let mut remainder = index as u32;
        let mut round = 0;
        while round < 8 {
            remainder = (remainder >> 1) ^ if remainder & 1 != 0 { 0xedb8_8320 } else { 0 };
            round += 1;
        }
        table[index] = remainder;
        index += 1;
    }
    table
}
// One immutable table, evaluated at compile time and shared by every caller.
static CRC32_TABLE: [u32; 256] = crc32_table();
pub fn crc32(bytes: &[u8]) -> u32 {
    let mut c = !0u32;
    for &b in bytes {
        c = (c >> 8) ^ CRC32_TABLE[((c as u8) ^ b) as usize];
    }
    !c
}
pub fn lp(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend((bytes.len() as u32).to_le_bytes());
    out.extend(bytes);
}
pub struct Cursor<'a> {
    b: &'a [u8],
    p: usize,
}
impl<'a> Cursor<'a> {
    pub fn new(b: &'a [u8]) -> Self {
        Self { b, p: 0 }
    }
    fn take(&mut self, n: usize) -> Result<&'a [u8], u16> {
        let e = self.p.checked_add(n).ok_or(3u16)?;
        if e > self.b.len() {
            return Err(3);
        }
        let b = &self.b[self.p..e];
        self.p = e;
        Ok(b)
    }
    pub fn u8(&mut self) -> Result<u8, u16> {
        Ok(self.take(1)?[0])
    }
    pub fn u16(&mut self) -> Result<u16, u16> {
        Ok(u16::from_le_bytes(self.take(2)?.try_into().unwrap()))
    }
    pub fn u32(&mut self) -> Result<u32, u16> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().unwrap()))
    }
    pub fn u64(&mut self) -> Result<u64, u16> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().unwrap()))
    }
    pub fn bytes(&mut self, max: usize) -> Result<&'a [u8], u16> {
        let n = self.u32()? as usize;
        if n > max {
            return Err(4);
        }
        self.take(n)
    }
    pub fn text(&mut self) -> Result<String, u16> {
        Ok(self.text_ref()?.to_owned())
    }
    fn text_ref(&mut self) -> Result<&'a str, u16> {
        let s = std::str::from_utf8(self.bytes(MAX_ID)?).map_err(|_| 3u16)?;
        if s.is_empty() {
            return Err(3);
        }
        Ok(s)
    }
    /// Scan bounds may be empty; identifiers still use text().
    pub fn bound(&mut self) -> Result<String, u16> {
        Ok(self.bound_ref()?.to_owned())
    }
    pub(super) fn bound_ref(&mut self) -> Result<&'a str, u16> {
        std::str::from_utf8(self.bytes(MAX_ID)?).map_err(|_| 3u16)
    }
    pub(super) fn remainder(&self) -> &'a [u8] {
        &self.b[self.p..]
    }
    pub fn end(&self) -> Result<(), u16> {
        if self.p == self.b.len() {
            Ok(())
        } else {
            Err(3)
        }
    }
}
pub fn frame(op: u8, id: u64, payload: &[u8]) -> Vec<u8> {
    let mut out = frame_header(op, id, payload.len());
    out.extend_from_slice(payload);
    out
}
fn frame_header(op: u8, id: u64, payload_len: usize) -> Vec<u8> {
    let body_len = 13 + payload_len;
    let mut out = Vec::with_capacity(4 + body_len);
    out.extend_from_slice(&(body_len as u32).to_le_bytes());
    out.extend_from_slice(&1u16.to_le_bytes());
    out.extend_from_slice(&0u16.to_le_bytes());
    out.push(op);
    out.extend_from_slice(&id.to_le_bytes());
    out
}
pub fn read_frame<R: Read>(r: &mut R) -> io::Result<Vec<u8>> {
    read_frame_limit(r, MAX_FRAME)
}
pub fn read_frame_limit<R: Read>(r: &mut R, max: usize) -> io::Result<Vec<u8>> {
    let mut n = [0; 4];
    r.read_exact(&mut n)?;
    let n = u32::from_le_bytes(n) as usize;
    if n < 13 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "malformed frame",
        ));
    }
    if n > max.min(MAX_FRAME) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "too_large frame; refused before body allocation",
        ));
    }
    let mut b = vec![0; n];
    r.read_exact(&mut b)?;
    Ok(b)
}
impl Outcome {
    pub(super) fn error(code: u16) -> Self {
        Self {
            code,
            version: 0,
            ack: 0,
        }
    }
    pub fn bytes(&self) -> Vec<u8> {
        let mut b = self.version.to_le_bytes().to_vec();
        b.push(self.ack);
        b
    }
}
/// One MBK1 payload as (mutation, version, record time).
pub(super) fn parse_record(b: &[u8]) -> Result<(Mutation, u64, u64), u16> {
    let record = parse_record_view(b)?;
    Ok((
        Mutation {
            ns: record.ns.to_owned(),
            key: record.key.to_owned(),
            op_id: record.op_id.to_owned(),
            deadline: 0,
            ack: record.ack,
            expected: record.expected,
            bytes: record.bytes.to_vec(),
            remove: record.remove,
        },
        record.version,
        record.at,
    ))
}
/// Validated borrowed fields share the exact durable decoding rules with replay.
pub(super) struct RecordView<'a> {
    pub(super) at: u64,
    pub(super) version: u64,
    pub(super) ack: u8,
    pub(super) remove: bool,
    pub(super) ns: &'a str,
    pub(super) key: &'a str,
    pub(super) op_id: &'a str,
    pub(super) expected: Option<u64>,
    pub(super) bytes: &'a [u8],
}
pub(super) fn parse_record_view(b: &[u8]) -> Result<RecordView<'_>, u16> {
    let mut c = Cursor::new(b);
    let at = c.u64()?;
    let version = c.u64()?;
    let ack = c.u8()?;
    let remove = c.u8()? != 0;
    let ns = c.text_ref()?;
    let key = c.text_ref()?;
    let op_id = c.text_ref()?;
    let expected = c.u64()?;
    let bytes = c.bytes(MAX_VALUE)?;
    c.end()?;
    if !matches!(ack, 1 | 2) {
        return Err(3);
    }
    Ok(RecordView {
        ns,
        key,
        op_id,
        ack,
        expected: if expected == u64::MAX {
            None
        } else {
            Some(expected)
        },
        bytes,
        remove,
        version,
        at,
    })
}
pub(super) fn mutation_bytes(m: &Mutation, version: u64, at: u64) -> Vec<u8> {
    let mut b = Vec::new();
    b.extend(at.to_le_bytes());
    b.extend(version.to_le_bytes());
    b.push(m.ack);
    b.push(m.remove as u8);
    lp(&mut b, m.ns.as_bytes());
    lp(&mut b, m.key.as_bytes());
    lp(&mut b, m.op_id.as_bytes());
    b.extend(m.expected.unwrap_or(u64::MAX).to_le_bytes());
    lp(&mut b, &m.bytes);
    b
}
enum DispatchReply {
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
        if major != 1 || minor != 0 {
            return reply(op, id, 1, &[]);
        }
        let result = (|| -> Result<DispatchReply, u16> {
            if op == HELLO {
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
                let mut p = self.epoch.to_le_bytes().to_vec();
                p.extend((MAX_FRAME.min(max) as u32).to_le_bytes());
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
                return Ok(DispatchReply::Payload(p));
            }
            let deadline = c.u64()?;
            // These identifiers belong to the request for this dispatch only.
            // Operations that retain work take their own copies below.
            let _op_id = c.text_ref()?;
            let ns = c.text_ref()?;
            // Internal schedule/progress records are not a public KV namespace.
            if ns == timers::NS || ns == chunks::POLICY_NS {
                return Err(3);
            }
            if op == PUT || op == CONDITIONAL_PUT || op == REMOVE {
                let m = parse_mutation(&mut c, op, deadline, _op_id.to_owned(), ns.to_owned())?;
                c.end()?;
                if m.ack == 0 && !allow_mem {
                    return Err(15);
                }
                let o = self.mutate(m);
                if o.code != 0 {
                    return Err(o.code);
                }
                return Ok(DispatchReply::Payload(o.bytes()));
            }
            if self.clock.utc_ms() >= deadline && op != POLICY_SET {
                return Err(5);
            }
            if matches!(
                op,
                SCAN_PREFIX | SCAN_RANGE | SCAN_KEYS_PREFIX | SCAN_KEYS_RANGE
            ) {
                return self
                    .scan_reply(ns, &mut c, op, id, max_frame)
                    .map(DispatchReply::Framed);
            }
            match op {
                TIMER_SET | TIMER_CANCEL | TIMER_STATUS => {
                    self.timer_call(op, ns.to_owned(), _op_id.to_owned(), deadline, &mut c)
                }
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
                    let o = self.set_policy_with_id(ns, &prefix, class, _op_id, deadline);
                    if o.code != 0 {
                        return Err(o.code);
                    }
                    Ok(o.bytes())
                }
                POLICY_GET => {
                    c.end()?;
                    let rows: Vec<_> = self.policies().into_iter().filter(|r| r.0 == ns).collect();
                    let mut p = (rows.len() as u16).to_le_bytes().to_vec();
                    for (_, prefix, class) in rows {
                        lp(&mut p, prefix.as_bytes());
                        p.push(class.kind);
                        p.extend(class.retention_ms.to_le_bytes());
                    }
                    Ok(p)
                }
                HEALTH2 => {
                    c.end()?;
                    let fields = self.health2().fields();
                    let mut p = (fields.len() as u16).to_le_bytes().to_vec();
                    for (k, v) in fields {
                        lp(&mut p, format!("{k}={v}").as_bytes());
                    }
                    Ok(p)
                }
                GET_STATUS => {
                    let key = c.text()?;
                    c.end()?;
                    let (status, version) = self.get_status(ns, &key);
                    let mut p = vec![status];
                    p.extend(version.to_le_bytes());
                    Ok(p)
                }
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
                    self.signals.subscribe(
                        ns,
                        prefix,
                        endpoint,
                        values != 0,
                        cursor,
                        &protocol,
                        frame_max,
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
                CAPTURE_METRICS => {
                    c.end()?;
                    let mut p = Vec::new();
                    for metric in &self.capture_metrics {
                        p.extend(metric.load(Ordering::Relaxed).to_le_bytes());
                    }
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
                GET => {
                    let key = c.text_ref()?;
                    c.end()?;
                    return self
                        .committed_read_reply(ns, std::slice::from_ref(&key), op, id, max_frame)
                        .map(DispatchReply::Framed);
                }
                GET_MANY => {
                    let n = c.u16()? as usize;
                    if n > MAX_BATCH {
                        return Err(4);
                    }
                    let mut keys = Vec::with_capacity(n);
                    for _ in 0..n {
                        keys.push(c.text_ref()?)
                    }
                    c.end()?;
                    return self
                        .committed_read_reply(ns, &keys, op, id, max_frame)
                        .map(DispatchReply::Framed);
                }
                BATCH => {
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
                        let item = parse_mutation(&mut c, op, deadline, id, ns.to_owned())?;
                        if item.ack == 0 && !allow_mem {
                            return Err(15);
                        }
                        items.push(item)
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
                                        out.extend(
                                            items.into_iter().map(|(i, m)| (i, self.mutate(m))),
                                        );
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
                    p.push((k * 100 >= self.config.index_cap * 70) as u8);
                    p.push((namespace_keys * 100 >= self.config.quota * 70) as u8);
                    Ok(p)
                }
                _ => Err(2),
            }
            .map(DispatchReply::Payload)
        })();
        match result {
            Ok(DispatchReply::Payload(p)) if p.len() + 15 <= max_frame => reply(op, id, 0, &p),
            Ok(DispatchReply::Framed(p)) if p.len() <= max_frame + 4 => p,
            Ok(_) => reply(op, id, 4, &[]),
            Err(code) => reply(op, id, code, &[]),
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
            // Each key retains its own committed observation, as before. No
            // index lock spans another key or disk I/O: this is not a snapshot.
            let entry = crate::store::committed_get(&self.committed.read().unwrap(), ns, key)
                .filter(|entry| !entry.tombstone)
                .cloned();
            payload_len = payload_len
                .checked_add(entry.as_ref().map_or(1, |entry| 14 + entry.len))
                .filter(|size| *size <= available)
                .ok_or(4u16)?;
            selected.push(entry);
        }
        // Refuse the whole response before touching the payload cache or disk.
        // Immutable entries bind both the size check and subsequent load, so a
        // concurrent replacement cannot substitute a larger value afterward.
        // Write directly into the final response; a failed load drops this
        // unfinished success buffer and dispatch returns an ordinary refusal.
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
}
fn parse_mutation(
    c: &mut Cursor<'_>,
    op: u8,
    deadline: u64,
    op_id: String,
    ns: String,
) -> Result<Mutation, u16> {
    let key = c.text()?;
    let ack = c.u8()?;
    if ack > 2 {
        return Err(3);
    }
    let expected = if op == CONDITIONAL_PUT || op == REMOVE {
        Some(c.u64()?)
    } else {
        None
    };
    let bytes = if op == REMOVE {
        Vec::new()
    } else {
        c.bytes(MAX_VALUE)?.to_vec()
    };
    Ok(Mutation {
        ns,
        key,
        op_id,
        deadline,
        ack,
        expected,
        bytes,
        remove: op == REMOVE,
    })
}
pub(super) fn encode_value(p: &mut Vec<u8>, v: Option<&Value>) {
    match v {
        Some(v) => {
            p.push(1);
            p.extend(v.version.to_le_bytes());
            p.push(v.ack);
            lp(p, &v.bytes)
        }
        None => p.push(0),
    }
}
pub fn reply(op: u8, id: u64, code: u16, p: &[u8]) -> Vec<u8> {
    let mut out = reply_header(op, id, code, p.len());
    out.extend_from_slice(p);
    out
}
pub(super) fn reply_header(op: u8, id: u64, code: u16, payload_len: usize) -> Vec<u8> {
    let mut out = frame_header(op | 0x80, id, 2 + payload_len);
    out.extend_from_slice(&code.to_le_bytes());
    out
}

#[cfg(test)]
#[path = "codec/crc_tests.rs"]
mod crc_tests;

#[cfg(test)]
mod read_tests;

#[cfg(test)]
#[path = "codec/record_tests.rs"]
mod record_tests;
