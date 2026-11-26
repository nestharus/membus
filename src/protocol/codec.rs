//! Wire framing, bounded decoding and durable payload encoding.
use crate::{Mutation, Outcome, Value};
use std::io::{self, Read};

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
pub const ATOMIC_CAPTURE: u8 = 28;
pub const ATOMIC_PUBLISH: u8 = 29;
pub const ATOMIC_POPULATION: u8 = 30;
pub const SNAPSHOT_REQUIRE_COMPLETE: u8 = 31;
/// Additive selected-view acquisition; HELLO and existing operations are unchanged.
pub const SNAPSHOT_OPEN_SELECTED: u8 = 32;
const fn make_crc32_slice8() -> [[u32; 256]; 8] {
    let mut table = [[0u32; 256]; 8];
    let mut i = 0;
    while i < 256 {
        let mut c = i as u32;
        let mut r = 0;
        while r < 8 {
            c = (c >> 1) ^ if c & 1 != 0 { 0xedb8_8320 } else { 0 };
            r += 1;
        }
        table[0][i] = c;
        i += 1;
    }
    let mut s = 1;
    while s < 8 {
        let mut i = 0;
        while i < 256 {
            let prev = table[s - 1][i];
            table[s][i] = (prev >> 8) ^ table[0][(prev & 0xff) as usize];
            i += 1;
        }
        s += 1;
    }
    table
}
// One immutable slicing-by-8 table, evaluated at compile time and shared by every caller.
static CRC32_SLICE8: [[u32; 256]; 8] = make_crc32_slice8();

/// Streaming IEEE CRC-32 update (pass `!0u32` initially and invert the final state with `!c`).
pub(crate) fn crc32_update(mut c: u32, mut bytes: &[u8]) -> u32 {
    while bytes.len() >= 8 {
        let lo = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) ^ c;
        let hi = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        c = CRC32_SLICE8[7][(lo & 0xff) as usize]
            ^ CRC32_SLICE8[6][((lo >> 8) & 0xff) as usize]
            ^ CRC32_SLICE8[5][((lo >> 16) & 0xff) as usize]
            ^ CRC32_SLICE8[4][((lo >> 24) & 0xff) as usize]
            ^ CRC32_SLICE8[3][(hi & 0xff) as usize]
            ^ CRC32_SLICE8[2][((hi >> 8) & 0xff) as usize]
            ^ CRC32_SLICE8[1][((hi >> 16) & 0xff) as usize]
            ^ CRC32_SLICE8[0][((hi >> 24) & 0xff) as usize];
        bytes = &bytes[8..];
    }
    for &b in bytes {
        c = (c >> 8) ^ CRC32_SLICE8[0][((c as u8) ^ b) as usize];
    }
    c
}

pub fn crc32(bytes: &[u8]) -> u32 {
    !crc32_update(!0u32, bytes)
}
pub fn lp(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend((bytes.len() as u32).to_le_bytes());
    out.extend(bytes);
}
#[derive(Clone)]
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
    pub(crate) fn text_ref(&mut self) -> Result<&'a str, u16> {
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
    pub(crate) fn bound_ref(&mut self) -> Result<&'a str, u16> {
        std::str::from_utf8(self.bytes(MAX_ID)?).map_err(|_| 3u16)
    }
    pub(crate) fn remainder(&self) -> &'a [u8] {
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
pub(crate) fn frame_header(op: u8, id: u64, payload_len: usize) -> Vec<u8> {
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
    pub(crate) fn error(code: u16) -> Self {
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
pub(crate) fn parse_record(b: &[u8]) -> Result<(Mutation, u64, u64), u16> {
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
pub(crate) struct RecordView<'a> {
    pub(crate) at: u64,
    pub(crate) version: u64,
    pub(crate) ack: u8,
    pub(crate) remove: bool,
    pub(crate) ns: &'a str,
    pub(crate) key: &'a str,
    pub(crate) op_id: &'a str,
    pub(crate) expected: Option<u64>,
    pub(crate) bytes: &'a [u8],
}
pub(crate) fn parse_record_view(b: &[u8]) -> Result<RecordView<'_>, u16> {
    let mut c = Cursor::new(b);
    let at = c.u64()?;
    let version = c.u64()?;
    let ack = c.u8()?;
    let remove_byte = c.u8()?;
    if remove_byte > 1 {return Err(3)}
    let remove = remove_byte != 0;
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
/// Exact encoded payload length of `mutation_bytes(m, ..)` without heap allocation.
pub(crate) fn mutation_encoded_len(m: &Mutation) -> usize {
    42 + m.ns.len() + m.key.len() + m.op_id.len() + m.bytes.len()
}
fn write_mutation_body(out: &mut Vec<u8>, m: &Mutation, version: u64, at: u64) {
    out.extend_from_slice(&at.to_le_bytes());
    out.extend_from_slice(&version.to_le_bytes());
    out.push(m.ack);
    out.push(m.remove as u8);
    lp(out, m.ns.as_bytes());
    lp(out, m.key.as_bytes());
    lp(out, m.op_id.as_bytes());
    out.extend_from_slice(&m.expected.unwrap_or(u64::MAX).to_le_bytes());
    lp(out, &m.bytes);
}
pub(crate) fn mutation_bytes(m: &Mutation, version: u64, at: u64) -> Vec<u8> {
    let mut b = Vec::with_capacity(mutation_encoded_len(m));
    write_mutation_body(&mut b, m, version, at);
    b
}
/// Streaming SHA-256 over canonical `mutation_bytes(m, 0, 0)` with zero heap allocations.
pub(crate) fn fingerprint_mutation(m: &Mutation) -> [u8; 32] {
    let mut hash = crate::store::hash::Sha256::new();
    hash.update(&[0u8; 16]);
    hash.update(&[m.ack, m.remove as u8]);
    hash.update(&(m.ns.len() as u32).to_le_bytes());
    hash.update(m.ns.as_bytes());
    hash.update(&(m.key.len() as u32).to_le_bytes());
    hash.update(m.key.as_bytes());
    hash.update(&(m.op_id.len() as u32).to_le_bytes());
    hash.update(m.op_id.as_bytes());
    hash.update(&m.expected.unwrap_or(u64::MAX).to_le_bytes());
    hash.update(&(m.bytes.len() as u32).to_le_bytes());
    hash.update(&m.bytes);
    hash.finish()
}
/// Encode a complete `MBK1` mutation frame in a single allocation.
pub(crate) fn encode_mbk1_mutation(m: &Mutation, version: u64, at: u64) -> Vec<u8> {
    let payload_len = mutation_encoded_len(m);
    let len_u32 = payload_len as u32;
    let mut out = Vec::with_capacity(16 + payload_len);
    out.extend_from_slice(b"MBK1");
    out.extend_from_slice(&len_u32.to_le_bytes());
    out.extend_from_slice(&(!len_u32).to_le_bytes());
    out.extend_from_slice(&0u32.to_le_bytes());
    write_mutation_body(&mut out, m, version, at);
    debug_assert_eq!(out.len(), 16 + payload_len);
    let sum = crc32(&out[16..]);
    out[12..16].copy_from_slice(&sum.to_le_bytes());
    out
}
pub(crate) fn parse_mutation(
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
pub(crate) fn encode_value(p: &mut Vec<u8>, v: Option<&Value>) {
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
pub(crate) fn reply_header(op: u8, id: u64, code: u16, payload_len: usize) -> Vec<u8> {
    let mut out = frame_header(op | 0x80, id, 2 + payload_len);
    out.extend_from_slice(&code.to_le_bytes());
    out
}


#[cfg(test)]
#[path = "../unit_tests/protocol/crc_tests.rs"]
mod crc_tests;
#[cfg(test)]
#[path = "../unit_tests/protocol/read_tests.rs"]
mod read_tests;
#[cfg(test)]
#[path = "../unit_tests/protocol/record_tests.rs"]
mod record_tests;

#[cfg(test)]
mod counted_reply_tests {
    use super::*;
    use crate::{Class, Store};
    use crate::Config;
    use std::{collections::BTreeMap, path::PathBuf};

    fn fixture(label: &str) -> (PathBuf, Store) {
        let root = PathBuf::from(
            std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()),
        )
        .join(format!("counted-reply-{label}-{}-{}", std::process::id(), crate::now_ms()));
        let store = Store::open(&root, Config {
            tick_ms: 3_600_000,
            retire_ms: 3_600_000,
            ..Config::default()
        }).unwrap();
        (root, store)
    }

    fn request(op: u8) -> Vec<u8> {
        let mut payload = u64::MAX.to_le_bytes().to_vec();
        lp(&mut payload, b"counted-reply");
        lp(&mut payload, b"ledger");
        frame(op, 37, &payload)
    }

    fn response(store: &Store, root: &std::path::Path, op: u8, limit: usize) -> Vec<u8> {
        let reply = store.dispatch_for_limit(&request(op)[4..], false, limit);
        // Retain exact wire evidence beside this test's isolated state.
        std::fs::write(root.join(format!("reply-{limit}.bin")), &reply).unwrap();
        let mut wire = std::io::Cursor::new(&reply);
        let body = read_frame_limit(&mut wire, limit).unwrap();
        assert_eq!(wire.position() as usize, reply.len());
        let mut decoder = Cursor::new(&body);
        assert_eq!(decoder.u16(), Ok(1));
        assert_eq!(decoder.u16(), Ok(0));
        assert_eq!(decoder.u8(), Ok(op | 0x80));
        assert_eq!(decoder.u64(), Ok(37));
        body[13..].to_vec()
    }

    fn policy(count: usize) {
        let (root, store) = fixture(&format!("policy-{count}"));
        // Seed the projection directly: this tests framing, not durable admission or load.
        {
            let mut policies = store.policies.write().unwrap();
            for i in 0..count {
                policies.set("ledger", &format!("p{i:05}"), Some(Class::LATEST));
            }
            // The count applies only to the requested namespace.
            policies.set("other", "unrelated", Some(Class::FOREVER));
        }
        assert_eq!(store.policies().len(), count + 1);
        let body = response(&store, &root, POLICY_GET, MAX_FRAME);
        let mut c = Cursor::new(&body);
        if count > u16::MAX as usize {
            assert_eq!(c.u16(), Ok(4), "overflow must refuse the whole reply");
            assert_eq!(c.end(), Ok(()), "refusal has no partial payload");
        } else {
            assert_eq!(c.u16(), Ok(0));
            assert_eq!(c.u16(), Ok(u16::MAX));
            // The policy CLI uses this counted Cursor/bound/u8/u64/end contract.
            for i in 0..count {
                assert_eq!(c.bound(), Ok(format!("p{i:05}")));
                assert_eq!(c.u8(), Ok(Class::LATEST.kind));
                assert_eq!(c.u64(), Ok(Class::LATEST.retention_ms));
            }
            assert_eq!(c.end(), Ok(()));
        }
        assert_eq!(response(&store, &root, POLICY_GET, 1024), 4u16.to_le_bytes());
    }

    fn health(count: usize) {
        let (root, store) = fixture(&format!("health-{count}"));
        let fixed = store.health2().fields().len();
        {
            let mut state = store.state.lock().unwrap();
            for i in 0..count - fixed {
                state.namespace_counts.insert(format!("n{i:05}"), 1);
            }
            state.keys = count - fixed;
        }
        let expected: BTreeMap<_, _> = store.health2().fields().into_iter().collect();
        assert_eq!(expected.len(), count);
        let body = response(&store, &root, HEALTH2, MAX_FRAME);
        let mut c = Cursor::new(&body);
        if count > u16::MAX as usize {
            assert_eq!(c.u16(), Ok(4), "overflow must refuse the whole reply");
            assert_eq!(c.end(), Ok(()), "refusal has no partial payload");
        } else {
            assert_eq!(c.u16(), Ok(0));
            assert_eq!(c.u16(), Ok(u16::MAX));
            let mut decoded = BTreeMap::new();
            for _ in 0..count {
                let field = c.bound().unwrap();
                let (key, value) = field.split_once('=').unwrap();
                assert!(decoded.insert(key.to_owned(), value.to_owned()).is_none());
            }
            assert_eq!(c.end(), Ok(()));
            assert_eq!(decoded, expected);
        }
        assert_eq!(response(&store, &root, HEALTH2, 1024), 4u16.to_le_bytes());
    }

    #[test]
    fn policy_65535() { policy(65_535); }
    #[test]
    fn policy_65536() { policy(65_536); }
    #[test]
    fn health_65535() { health(65_535); }
    #[test]
    fn health_65536() { health(65_536); }
}
