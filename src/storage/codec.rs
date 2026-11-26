use crate::types::{
    validate_namespace, IndexMutations, LogMutationDelta, MembusError, MutationOp, RecordMeta,
    ReplicationStamp, SearchDocument, SearchLogOp, SearchVisibility, SignalEvent, SignalKind,
    StoredRecord,
};

const RECORD_MAGIC: u8 = 0x4D;
const RECORD_FORMAT_V2: u8 = 2;
pub const RECORD_HEADER_LEN: usize = 24;

/// Record header flag indicating the `CF_KV` value bytes encode a compact [`crate::types::ColdRef`]
/// lineage tombstone rather than raw hot user bytes.
pub const RECORD_FLAG_COLD_REF: u8 = 0x01;

const LOG_MAGIC: u8 = 0x4C;
const LOG_FORMAT_V2: u8 = 2;
const LOG_FORMAT_V3: u8 = 3;
const LOG_FORMAT_V4: u8 = 4;
const LOG_HEADER_LEN: usize = 26;

/// Domain prefix separating legacy unscoped raw blobs from namespace-scoped CAS blobs in `CF_BLOB`.
pub const BLOB_DOMAIN_RAW: u8 = 0x01;
pub const BLOB_DOMAIN_CAS: u8 = 0x02;

/// Encode a legacy unscoped raw blob key `[0x01][digest]`.
pub fn encode_raw_blob_key(digest: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(1 + digest.len());
    out.push(BLOB_DOMAIN_RAW);
    out.extend_from_slice(digest);
    out
}

/// Encode a namespace-scoped CAS blob key `[0x02][u32_be ns_len][ns][sha256: 32B]`.
pub fn encode_cas_blob_key(namespace: &str, sha256: &[u8; 32]) -> Result<Vec<u8>, MembusError> {
    validate_namespace(namespace)?;
    let ns_b = namespace.as_bytes();
    let mut out = Vec::with_capacity(1 + 4 + ns_b.len() + 32);
    out.push(BLOB_DOMAIN_CAS);
    out.extend_from_slice(&(ns_b.len() as u32).to_be_bytes());
    out.extend_from_slice(ns_b);
    out.extend_from_slice(sha256);
    Ok(out)
}

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

/// Compute the canonical CRC32 checksum for a `kv` record header + value payload.
pub fn compute_record_crc32(version: u64, flags: u8, ts_ms: u64, value: &[u8]) -> u32 {
    let prefix = [RECORD_MAGIC, RECORD_FORMAT_V2, flags, 0u8];
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&prefix);
    hasher.update(&version.to_be_bytes());
    hasher.update(&ts_ms.to_be_bytes());
    hasher.update(value);
    hasher.finalize()
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
    let crc = compute_record_crc32(version, flags, ts_ms, value);

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

/// Encode a commit log entry storing signal events (v2 compatibility wrapper).
pub fn encode_log_entry(epoch: u64, seq: u64, events: &[SignalEvent]) -> Vec<u8> {
    encode_log_entry_with_search(epoch, seq, events, &[])
}

/// Encode a commit log entry storing signal events, standalone search ops, and full
/// replication/backup deltas (`LOG_FORMAT_V4`).
#[allow(clippy::too_many_arguments)]
pub fn encode_log_entry_v4(
    epoch: u64,
    seq: u64,
    ts_ms: u64,
    replicated: bool,
    events: &[SignalEvent],
    standalone_search_ops: &[SearchLogOp],
    mutations: &[LogMutationDelta],
    standalone_origin: Option<&ReplicationStamp>,
) -> Vec<u8> {
    let mut body = Vec::with_capacity(64 + events.len() * 64 + mutations.len() * 64);
    encode_log_body_events(epoch, seq, events, &mut body);
    body.extend_from_slice(&(standalone_search_ops.len() as u32).to_be_bytes());
    for op in standalone_search_ops {
        encode_search_log_op(op, &mut body);
    }
    body.push(u8::from(replicated));
    body.extend_from_slice(&ts_ms.to_be_bytes());
    body.extend_from_slice(&(mutations.len() as u32).to_be_bytes());
    for m in mutations {
        encode_log_mutation_delta(m, &mut body);
    }
    encode_opt_replication_stamp(standalone_origin, &mut body);
    finalize_log_entry(LOG_FORMAT_V4, &body)
}

/// Encode a commit log entry storing both signal events and optional search mutations.
pub fn encode_log_entry_with_search(
    epoch: u64,
    seq: u64,
    events: &[SignalEvent],
    search_ops: &[SearchLogOp],
) -> Vec<u8> {
    let format_ver = if search_ops.is_empty() {
        LOG_FORMAT_V2
    } else {
        LOG_FORMAT_V3
    };
    let mut body = Vec::with_capacity(32 + events.len() * 64);
    encode_log_body_events(epoch, seq, events, &mut body);
    if format_ver == LOG_FORMAT_V3 {
        body.extend_from_slice(&(search_ops.len() as u32).to_be_bytes());
        for op in search_ops {
            encode_search_log_op(op, &mut body);
        }
    }
    finalize_log_entry(format_ver, &body)
}

fn encode_log_body_events(epoch: u64, seq: u64, events: &[SignalEvent], body: &mut Vec<u8>) {
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
}

fn finalize_log_entry(format_ver: u8, body: &[u8]) -> Vec<u8> {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(&[LOG_MAGIC, format_ver]);
    hasher.update(body);
    let crc = hasher.finalize();

    let mut out = Vec::with_capacity(6 + body.len());
    out.push(LOG_MAGIC);
    out.push(format_ver);
    out.extend_from_slice(&crc.to_be_bytes());
    out.extend_from_slice(body);
    out
}

fn encode_opt_replication_stamp(stamp: Option<&ReplicationStamp>, out: &mut Vec<u8>) {
    match stamp {
        Some(s) => {
            out.push(1);
            out.extend_from_slice(&s.timestamp_ms.to_be_bytes());
            out.extend_from_slice(&(s.origin_node_id.len() as u16).to_be_bytes());
            out.extend_from_slice(s.origin_node_id.as_bytes());
            out.extend_from_slice(&s.origin_seq.to_be_bytes());
            out.extend_from_slice(&s.value_sha256);
        }
        None => out.push(0),
    }
}

fn decode_opt_replication_stamp(cur: &mut &[u8]) -> Result<Option<ReplicationStamp>, MembusError> {
    match read_u8(cur)? {
        0 => Ok(None),
        1 => {
            let timestamp_ms = read_u64(cur)?;
            let nid_len = read_u16(cur)? as usize;
            let nid_bytes = read_slice(cur, nid_len)?;
            let origin_node_id = std::str::from_utf8(nid_bytes)
                .map_err(|err| MembusError::Corruption(format!("stamp utf8 error: {err}")))?
                .to_owned();
            let origin_seq = read_u64(cur)?;
            let sha_slice = read_slice(cur, 32)?;
            let mut value_sha256 = [0u8; 32];
            value_sha256.copy_from_slice(sha_slice);
            Ok(Some(ReplicationStamp {
                timestamp_ms,
                origin_node_id,
                origin_seq,
                value_sha256,
            }))
        }
        other => Err(MembusError::Corruption(format!(
            "invalid replication stamp tag: {other}"
        ))),
    }
}

fn encode_log_mutation_delta(delta: &LogMutationDelta, out: &mut Vec<u8>) {
    match &delta.op {
        MutationOp::Put { value } => {
            out.push(1);
            out.push(delta.flags);
            let meta_bits =
                u8::from(delta.preserve_indexes) | (u8::from(delta.is_tier_swap) << 1);
            out.push(meta_bits);
            out.extend_from_slice(&(delta.namespace.len() as u16).to_be_bytes());
            out.extend_from_slice(delta.namespace.as_bytes());
            out.extend_from_slice(&(delta.key.len() as u32).to_be_bytes());
            out.extend_from_slice(&delta.key);
            out.extend_from_slice(&(value.len() as u32).to_be_bytes());
            out.extend_from_slice(value);
        }
        MutationOp::Remove => {
            out.push(2);
            out.push(delta.flags);
            let meta_bits =
                u8::from(delta.preserve_indexes) | (u8::from(delta.is_tier_swap) << 1);
            out.push(meta_bits);
            out.extend_from_slice(&(delta.namespace.len() as u16).to_be_bytes());
            out.extend_from_slice(delta.namespace.as_bytes());
            out.extend_from_slice(&(delta.key.len() as u32).to_be_bytes());
            out.extend_from_slice(&delta.key);
        }
    }
    match &delta.indexes {
        Some(idx) if !idx.is_empty() => {
            let encoded = serde_json::to_vec(idx).unwrap_or_default();
            out.push(1);
            out.extend_from_slice(&(encoded.len() as u32).to_be_bytes());
            out.extend_from_slice(&encoded);
        }
        _ => out.push(0),
    }
    match &delta.search_op {
        Some(op) => {
            out.push(1);
            encode_search_log_op(op, out);
        }
        None => out.push(0),
    }
    encode_opt_replication_stamp(delta.origin_stamp.as_ref(), out);
}

fn decode_log_mutation_delta(cur: &mut &[u8]) -> Result<LogMutationDelta, MembusError> {
    let tag = read_u8(cur)?;
    let flags = read_u8(cur)?;
    let meta_bits = read_u8(cur)?;
    if (meta_bits & !0x03) != 0 {
        return Err(MembusError::Corruption(format!(
            "invalid log delta meta_bits: 0x{meta_bits:02x}"
        )));
    }
    let preserve_indexes = (meta_bits & 0x01) != 0;
    let is_tier_swap = (meta_bits & 0x02) != 0;
    let ns_len = read_u16(cur)? as usize;
    let ns_bytes = read_slice(cur, ns_len)?;
    let namespace = std::str::from_utf8(ns_bytes)
        .map_err(|err| MembusError::Corruption(format!("log delta utf8 error: {err}")))?
        .to_owned();
    let key_len = read_u32(cur)? as usize;
    let key = read_slice(cur, key_len)?.to_vec();
    let op = match tag {
        1 => {
            let val_len = read_u32(cur)? as usize;
            let value = read_slice(cur, val_len)?.to_vec();
            MutationOp::Put { value }
        }
        2 => MutationOp::Remove,
        other => {
            return Err(MembusError::Corruption(format!(
                "unknown log delta op tag: {other}"
            )));
        }
    };
    let indexes = match read_u8(cur)? {
        0 => None,
        1 => {
            let idx_len = read_u32(cur)? as usize;
            let idx_bytes = read_slice(cur, idx_len)?;
            let parsed: IndexMutations = serde_json::from_slice(idx_bytes).map_err(|err| {
                MembusError::Corruption(format!("corrupt index mutations in log delta: {err}"))
            })?;
            Some(parsed)
        }
        other => {
            return Err(MembusError::Corruption(format!(
                "invalid log delta indexes tag: {other}"
            )));
        }
    };
    let search_op = match read_u8(cur)? {
        0 => None,
        1 => Some(decode_search_log_op(cur)?),
        other => {
            return Err(MembusError::Corruption(format!(
                "invalid log delta search_op tag: {other}"
            )));
        }
    };
    let origin_stamp = decode_opt_replication_stamp(cur)?;
    Ok(LogMutationDelta {
        namespace,
        key,
        op,
        flags,
        preserve_indexes,
        is_tier_swap,
        indexes,
        search_op,
        origin_stamp,
    })
}

/// Decode and CRC-verify signal events from a `CF_LOG` commit entry.
pub fn decode_log_entry(raw: &[u8]) -> Result<Vec<SignalEvent>, MembusError> {
    let (_epoch, _seq, events, _search_ops) = decode_log_entry_full(raw)?;
    Ok(events)
}

/// Decode and CRC-verify `(epoch, seq, events, all_search_ops)` from a `CF_LOG` entry.
///
/// Combines per-mutation `search_op`s (in exact mutation order) followed by any standalone
/// search ops so `SearchEngine` restart replay sees every committed search op in order.
pub fn decode_log_entry_full(
    raw: &[u8],
) -> Result<(u64, u64, Vec<SignalEvent>, Vec<SearchLogOp>), MembusError> {
    let decoded = decode_log_entry_all(raw)?;
    let mut all_search_ops = Vec::with_capacity(
        decoded.search_ops.len()
            + decoded
                .mutations
                .iter()
                .filter(|m| m.search_op.is_some())
                .count(),
    );
    for m in &decoded.mutations {
        if let Some(op) = &m.search_op {
            all_search_ops.push(op.clone());
        }
    }
    all_search_ops.extend(decoded.search_ops);
    Ok((decoded.epoch, decoded.seq, decoded.events, all_search_ops))
}

/// Decode and CRC-verify a full [`crate::types::LogDeltaEntry`] from a `CF_LOG` entry.
pub fn decode_log_entry_delta(raw: &[u8]) -> Result<crate::types::LogDeltaEntry, MembusError> {
    let decoded = decode_log_entry_all(raw)?;
    Ok(crate::types::LogDeltaEntry {
        seq: decoded.seq,
        epoch: decoded.epoch,
        ts_ms: decoded.ts_ms,
        replicated: decoded.replicated,
        mutations: decoded.mutations,
        search_ops: decoded.search_ops,
        standalone_origin: decoded.standalone_origin,
    })
}

struct DecodedLogEntry {
    epoch: u64,
    seq: u64,
    ts_ms: u64,
    replicated: bool,
    events: Vec<SignalEvent>,
    search_ops: Vec<SearchLogOp>,
    mutations: Vec<LogMutationDelta>,
    standalone_origin: Option<ReplicationStamp>,
}

fn decode_log_entry_all(raw: &[u8]) -> Result<DecodedLogEntry, MembusError> {
    if raw.len() < LOG_HEADER_LEN {
        return Err(MembusError::Corruption(format!(
            "log entry shorter than {LOG_HEADER_LEN} bytes: {}",
            raw.len()
        )));
    }
    let format_ver = raw[1];
    if raw[0] != LOG_MAGIC
        || (format_ver != LOG_FORMAT_V2
            && format_ver != LOG_FORMAT_V3
            && format_ver != LOG_FORMAT_V4)
    {
        return Err(MembusError::Corruption(format!(
            "unexpected log entry magic/version: 0x{:02x}/{format_ver}",
            raw[0]
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

    let mut events = Vec::with_capacity(count.min(cur.len()).min(256));
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

    let mut search_ops = Vec::new();
    if format_ver == LOG_FORMAT_V3 || format_ver == LOG_FORMAT_V4 {
        let op_count = read_u32(&mut cur)? as usize;
        search_ops.reserve(op_count.min(cur.len()).min(256));
        for _ in 0..op_count {
            search_ops.push(decode_search_log_op(&mut cur)?);
        }
    }

    let mut ts_ms = 0u64;
    let mut replicated = false;
    let mut mutations = Vec::new();
    let mut standalone_origin = None;
    if format_ver == LOG_FORMAT_V4 {
        let rep_flag = read_u8(&mut cur)?;
        if rep_flag > 1 {
            return Err(MembusError::Corruption(format!(
                "invalid log delta replicated flag: {rep_flag}"
            )));
        }
        replicated = rep_flag == 1;
        ts_ms = read_u64(&mut cur)?;
        let mut_count = read_u32(&mut cur)? as usize;
        mutations.reserve(mut_count.min(cur.len()).min(256));
        for _ in 0..mut_count {
            mutations.push(decode_log_mutation_delta(&mut cur)?);
        }
        standalone_origin = decode_opt_replication_stamp(&mut cur)?;
    }

    if !cur.is_empty() {
        return Err(MembusError::Corruption(format!(
            "trailing {} bytes in log entry",
            cur.len()
        )));
    }
    Ok(DecodedLogEntry {
        epoch,
        seq,
        ts_ms,
        replicated,
        events,
        search_ops,
        mutations,
        standalone_origin,
    })
}

fn encode_search_log_op(op: &SearchLogOp, out: &mut Vec<u8>) {
    match op {
        SearchLogOp::Index(doc) => {
            out.push(1);
            push_str32(out, &doc.namespace);
            push_str32(out, &doc.doc_id);
            push_str32(out, &doc.kind);
            push_str32(out, &doc.title);
            out.extend_from_slice(&(doc.tags.len() as u32).to_be_bytes());
            for tag in &doc.tags {
                push_str32(out, tag);
            }
            push_str32(out, &doc.body);
            out.push(doc.visibility as u8);
        }
        SearchLogOp::Delete { namespace, doc_id } => {
            out.push(2);
            push_str32(out, namespace);
            push_str32(out, doc_id);
        }
        SearchLogOp::RevokePrefix { namespace, prefix } => {
            out.push(3);
            push_str32(out, namespace);
            push_str32(out, prefix);
        }
    }
}

fn decode_search_log_op(cur: &mut &[u8]) -> Result<SearchLogOp, MembusError> {
    match read_u8(cur)? {
        1 => {
            let namespace = read_str32(cur)?;
            let doc_id = read_str32(cur)?;
            let kind = read_str32(cur)?;
            let title = read_str32(cur)?;
            let tag_count = read_u32(cur)? as usize;
            let mut tags = Vec::with_capacity(tag_count.min(cur.len() / 4).min(256));
            for _ in 0..tag_count {
                tags.push(read_str32(cur)?);
            }
            let body = read_str32(cur)?;
            let visibility = SearchVisibility::from_u8(read_u8(cur)?)
                .map_err(|e| MembusError::Corruption(e.to_string()))?;
            Ok(SearchLogOp::Index(SearchDocument {
                namespace,
                doc_id,
                kind,
                title,
                tags,
                body,
                visibility,
            }))
        }
        2 => Ok(SearchLogOp::Delete {
            namespace: read_str32(cur)?,
            doc_id: read_str32(cur)?,
        }),
        3 => Ok(SearchLogOp::RevokePrefix {
            namespace: read_str32(cur)?,
            prefix: read_str32(cur)?,
        }),
        other => Err(MembusError::Corruption(format!(
            "unknown search log op tag: {other}"
        ))),
    }
}

fn push_str32(out: &mut Vec<u8>, s: &str) {
    out.extend_from_slice(&(s.len() as u32).to_be_bytes());
    out.extend_from_slice(s.as_bytes());
}

fn read_str32(cur: &mut &[u8]) -> Result<String, MembusError> {
    let len = read_u32(cur)? as usize;
    let bytes = read_slice(cur, len)?;
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|err| MembusError::Corruption(format!("log entry utf8 error: {err}")))
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
