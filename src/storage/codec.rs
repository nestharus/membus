use crate::types::{MembusError, RecordMeta, SignalEvent, SignalKind, StoredRecord};

const RECORD_MAGIC: u8 = 0x4D;
const RECORD_FORMAT_V2: u8 = 2;
pub const RECORD_HEADER_LEN: usize = 24;

const LOG_MAGIC: u8 = 0x4C;
const LOG_FORMAT_V2: u8 = 2;
const LOG_HEADER_LEN: usize = 26;

/// Encode `(namespace, key)` as `[u32_be ns_len][ns_bytes][key_bytes]`.
///
/// Within a single namespace the prefix `[u32_be ns_len][ns_bytes]` is constant,
/// so keys retain exact lexicographic ordering while distinct namespaces never
/// alias each other as prefixes.
pub fn encode_kv_key(namespace: &str, key: &[u8]) -> Vec<u8> {
    let ns_bytes = namespace.as_bytes();
    let mut out = Vec::with_capacity(4 + ns_bytes.len() + key.len());
    out.extend_from_slice(&(ns_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(ns_bytes);
    out.extend_from_slice(key);
    out
}

/// Encode the namespace prefix `[u32_be ns_len][ns_bytes]` used for prefix seeks.
pub fn encode_ns_prefix(namespace: &str) -> Vec<u8> {
    let ns_bytes = namespace.as_bytes();
    let mut out = Vec::with_capacity(4 + ns_bytes.len());
    out.extend_from_slice(&(ns_bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(ns_bytes);
    out
}

/// Decode a composite `kv` key into `(namespace, key)`.
pub fn decode_kv_key(raw: &[u8]) -> Result<(&str, &[u8]), MembusError> {
    if raw.len() < 4 {
        return Err(MembusError::Corruption(
            "kv key shorter than 4-byte namespace length prefix".to_owned(),
        ));
    }
    let ns_len = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    let key_start = 4usize
        .checked_add(ns_len)
        .ok_or_else(|| MembusError::Corruption("kv key namespace length overflow".to_owned()))?;
    if key_start > raw.len() {
        return Err(MembusError::Corruption(
            "kv key truncated inside namespace bytes".to_owned(),
        ));
    }
    let namespace = std::str::from_utf8(&raw[4..key_start])
        .map_err(|err| MembusError::Corruption(format!("invalid utf8 namespace: {err}")))?;
    Ok((namespace, &raw[key_start..]))
}

/// RocksDB prefix-extractor `in_domain` predicate for `kv` composite keys.
pub fn kv_prefix_in_domain(raw: &[u8]) -> bool {
    if raw.len() < 4 {
        return false;
    }
    let ns_len = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    4usize.checked_add(ns_len).is_some_and(|end| end <= raw.len())
}

/// RocksDB prefix-extractor `transform` returning `[u32_be ns_len][ns_bytes]`.
pub fn kv_prefix_transform(raw: &[u8]) -> &[u8] {
    if raw.len() < 4 {
        return raw;
    }
    let ns_len = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    match 4usize.checked_add(ns_len) {
        Some(end) if end <= raw.len() => &raw[..end],
        _ => raw,
    }
}

/// Smallest byte string strictly greater than `key` under lexicographic order.
pub fn next_key_after(key: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() + 1);
    out.extend_from_slice(key);
    out.push(0);
    out
}

/// Encode a `kv` value with a compact 24-byte checksummed record header.
///
/// Layout:
/// `[0] magic (0x4D)`
/// `[1] format_version (2)`
/// `[2] flags`
/// `[3] reserved (0)`
/// `[4..8] crc32_be`
/// `[8..16] version_be`
/// `[16..24] ts_ms_be`
/// `[24..] value bytes`
pub fn encode_record(version: u64, flags: u8, ts_ms: u64, value: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(RECORD_HEADER_LEN + value.len());
    let prefix = [RECORD_MAGIC, RECORD_FORMAT_V2, flags, 0u8];
    let version_bytes = version.to_be_bytes();
    let ts_bytes = ts_ms.to_be_bytes();

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&prefix);
    hasher.update(&version_bytes);
    hasher.update(&ts_bytes);
    hasher.update(value);
    let crc = hasher.finalize();

    out.extend_from_slice(&prefix);
    out.extend_from_slice(&crc.to_be_bytes());
    out.extend_from_slice(&version_bytes);
    out.extend_from_slice(&ts_bytes);
    out.extend_from_slice(value);
    out
}

/// Validate header & CRC32 and return record metadata plus value slice.
pub fn decode_record_parts(raw: &[u8]) -> Result<(RecordMeta, &[u8]), MembusError> {
    if raw.len() < RECORD_HEADER_LEN {
        return Err(MembusError::Corruption(format!(
            "record shorter than {RECORD_HEADER_LEN}-byte header: {} bytes",
            raw.len()
        )));
    }
    if raw[0] != RECORD_MAGIC || raw[1] != RECORD_FORMAT_V2 {
        return Err(MembusError::Corruption(format!(
            "unexpected record magic/version: 0x{:02x}/{}",
            raw[0], raw[1]
        )));
    }
    let flags = raw[2];
    let stored_crc = u32::from_be_bytes([raw[4], raw[5], raw[6], raw[7]]);
    let version = u64::from_be_bytes([
        raw[8], raw[9], raw[10], raw[11], raw[12], raw[13], raw[14], raw[15],
    ]);
    let ts_ms = u64::from_be_bytes([
        raw[16], raw[17], raw[18], raw[19], raw[20], raw[21], raw[22], raw[23],
    ]);
    let value = &raw[RECORD_HEADER_LEN..];

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&raw[0..4]);
    hasher.update(&raw[8..RECORD_HEADER_LEN]);
    hasher.update(value);
    let computed_crc = hasher.finalize();
    if computed_crc != stored_crc {
        return Err(MembusError::Corruption(format!(
            "record CRC32 mismatch: stored 0x{stored_crc:08x}, computed 0x{computed_crc:08x}"
        )));
    }

    Ok((
        RecordMeta {
            version,
            flags,
            ts_ms,
            crc32: stored_crc,
        },
        value,
    ))
}

/// Decode a validated `StoredRecord` from a key and raw `kv` CF value.
pub fn decode_record(key: Vec<u8>, raw: &[u8]) -> Result<StoredRecord, MembusError> {
    let (meta, value) = decode_record_parts(raw)?;
    Ok(StoredRecord {
        key,
        value: value.to_vec(),
        meta,
    })
}

/// Encode a `log` CF sequence key as big-endian `u64`.
pub fn encode_log_seq(seq: u64) -> [u8; 8] {
    seq.to_be_bytes()
}

/// Decode a `log` CF sequence key from big-endian `u64`.
pub fn decode_log_seq(raw: &[u8]) -> Result<u64, MembusError> {
    if raw.len() != 8 {
        return Err(MembusError::Corruption(format!(
            "expected 8-byte log seq key, got {} bytes",
            raw.len()
        )));
    }
    Ok(u64::from_be_bytes([
        raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
    ]))
}

/// Encode a commit log entry storing all signal events for one atomic commit.
pub fn encode_log_entry(epoch: u64, seq: u64, events: &[SignalEvent]) -> Vec<u8> {
    let body_capacity: usize = events
        .iter()
        .map(|ev| {
            1 + 8
                + 2
                + ev.namespace.len()
                + 4
                + ev.key.len()
                + 1
                + ev.value_hint.as_ref().map_or(0, |h| 4 + h.len())
        })
        .sum();
    let mut body = Vec::with_capacity(20 + body_capacity);
    body.extend_from_slice(&epoch.to_be_bytes());
    body.extend_from_slice(&seq.to_be_bytes());
    body.extend_from_slice(&(events.len() as u32).to_be_bytes());
    for ev in events {
        body.push(ev.kind as u8);
        body.extend_from_slice(&ev.version.to_be_bytes());
        body.extend_from_slice(&(ev.namespace.len() as u16).to_be_bytes());
        body.extend_from_slice(ev.namespace.as_bytes());
        body.extend_from_slice(&(ev.key.len() as u32).to_be_bytes());
        body.extend_from_slice(&ev.key);
        match &ev.value_hint {
            Some(hint) => {
                body.push(1);
                body.extend_from_slice(&(hint.len() as u32).to_be_bytes());
                body.extend_from_slice(hint);
            }
            None => body.push(0),
        }
    }

    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&[LOG_MAGIC, LOG_FORMAT_V2]);
    hasher.update(&body);
    let crc = hasher.finalize();

    let mut out = Vec::with_capacity(6 + body.len());
    out.push(LOG_MAGIC);
    out.push(LOG_FORMAT_V2);
    out.extend_from_slice(&crc.to_be_bytes());
    out.extend_from_slice(&body);
    out
}

/// Decode and CRC-verify a commit log entry from the `log` CF.
pub fn decode_log_entry(raw: &[u8]) -> Result<Vec<SignalEvent>, MembusError> {
    if raw.len() < LOG_HEADER_LEN {
        return Err(MembusError::Corruption(format!(
            "log entry shorter than {LOG_HEADER_LEN} bytes: {}",
            raw.len()
        )));
    }
    if raw[0] != LOG_MAGIC || raw[1] != LOG_FORMAT_V2 {
        return Err(MembusError::Corruption(format!(
            "unexpected log entry magic/version: 0x{:02x}/{}",
            raw[0], raw[1]
        )));
    }
    let stored_crc = u32::from_be_bytes([raw[2], raw[3], raw[4], raw[5]]);
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&raw[0..2]);
    hasher.update(&raw[6..]);
    let computed_crc = hasher.finalize();
    if computed_crc != stored_crc {
        return Err(MembusError::Corruption(format!(
            "log entry CRC32 mismatch: stored 0x{stored_crc:08x}, computed 0x{computed_crc:08x}"
        )));
    }

    let mut cur = &raw[6..];
    let epoch = read_u64(&mut cur)?;
    let seq = read_u64(&mut cur)?;
    let count = read_u32(&mut cur)? as usize;

    let mut events = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        let kind = SignalKind::from_u8(read_u8(&mut cur)?)?;
        let version = read_u64(&mut cur)?;
        let ns_len = read_u16(&mut cur)? as usize;
        let ns_bytes = read_slice(&mut cur, ns_len)?;
        let namespace = std::str::from_utf8(ns_bytes)
            .map_err(|err| MembusError::Corruption(format!("log entry utf8 error: {err}")))?
            .to_owned();
        let key_len = read_u32(&mut cur)? as usize;
        let key = read_slice(&mut cur, key_len)?.to_vec();
        let has_hint = read_u8(&mut cur)?;
        let value_hint = match has_hint {
            0 => None,
            1 => {
                let hint_len = read_u32(&mut cur)? as usize;
                Some(read_slice(&mut cur, hint_len)?.to_vec())
            }
            other => {
                return Err(MembusError::Corruption(format!(
                    "invalid value_hint tag: {other}"
                )));
            }
        };
        events.push(SignalEvent {
            seq,
            epoch,
            namespace,
            key,
            version,
            kind,
            value_hint,
        });
    }
    if !cur.is_empty() {
        return Err(MembusError::Corruption(format!(
            "trailing {} bytes in log entry",
            cur.len()
        )));
    }
    Ok(events)
}

fn read_u8(cur: &mut &[u8]) -> Result<u8, MembusError> {
    if cur.is_empty() {
        return Err(MembusError::Corruption("unexpected eof reading u8".to_owned()));
    }
    let v = cur[0];
    *cur = &cur[1..];
    Ok(v)
}

fn read_u16(cur: &mut &[u8]) -> Result<u16, MembusError> {
    let bytes = read_slice(cur, 2)?;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn read_u32(cur: &mut &[u8]) -> Result<u32, MembusError> {
    let bytes = read_slice(cur, 4)?;
    Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_u64(cur: &mut &[u8]) -> Result<u64, MembusError> {
    let bytes = read_slice(cur, 8)?;
    Ok(u64::from_be_bytes([
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
    ]))
}

fn read_slice<'a>(cur: &mut &'a [u8], len: usize) -> Result<&'a [u8], MembusError> {
    if cur.len() < len {
        return Err(MembusError::Corruption(format!(
            "unexpected eof reading {len} bytes (remaining {})",
            cur.len()
        )));
    }
    let (head, tail) = cur.split_at(len);
    *cur = tail;
    Ok(head)
}
