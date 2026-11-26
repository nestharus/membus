use bytes::{BufMut, Bytes, BytesMut};

use crate::types::{
    Ack, CommitResult, ExpectedVersion, HealthStatus, MembusError, MutationOp, RecordMeta,
    ScanOptions, ScanPage, SignalEvent, SignalKind, StoredRecord, VersionGuard, WriteMutation,
};

const OP_GET: u8 = 1;
const OP_GET_MANY: u8 = 2;
const OP_PUT: u8 = 3;
const OP_REMOVE: u8 = 4;
const OP_COMMIT_TX: u8 = 5;
const OP_OPEN_SNAPSHOT: u8 = 6;
const OP_CLOSE_SNAPSHOT: u8 = 7;
const OP_SCAN_PREFIX: u8 = 8;
const OP_SCAN_RANGE: u8 = 9;
const OP_SUBSCRIBE_EACH: u8 = 10;
const OP_SUBSCRIBE_LIVE: u8 = 11;
const OP_CHECKPOINT: u8 = 12;
const OP_HEALTH: u8 = 13;
const OP_PUT_BLOB: u8 = 14;
const OP_GET_BLOB: u8 = 15;

const RESP_GET: u8 = 1;
const RESP_GET_MANY: u8 = 2;
const RESP_COMMIT: u8 = 3;
const RESP_SNAPSHOT_OPENED: u8 = 4;
const RESP_UNIT: u8 = 5;
const RESP_SCAN: u8 = 6;
const RESP_SUBSCRIBED: u8 = 7;
const RESP_CHECKPOINT: u8 = 8;
const RESP_HEALTH: u8 = 9;
const RESP_BLOB: u8 = 10;
const RESP_ERROR: u8 = 255;

const ERR_INVALID_ARG: u8 = 1;
const ERR_CONFLICT: u8 = 2;
const ERR_TX_CONFLICT: u8 = 3;
const ERR_BUSY: u8 = 4;
const ERR_GAP: u8 = 5;
const ERR_NOT_FOUND: u8 = 6;
const ERR_CORRUPTION: u8 = 7;
const ERR_STORAGE: u8 = 8;
const ERR_PROTOCOL: u8 = 9;
const ERR_IO: u8 = 10;
const ERR_CLOSED: u8 = 11;
const ERR_FRAME_TOO_LARGE: u8 = 12;

const MAX_BATCH_ITEMS: usize = 65_536;

/// Strongly-typed RPC request carried in `FrameKind::Request`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Request {
    Get {
        namespace: String,
        key: Vec<u8>,
        snapshot_id: Option<u64>,
    },
    GetMany {
        namespace: String,
        keys: Vec<Vec<u8>>,
        snapshot_id: Option<u64>,
    },
    Put {
        namespace: String,
        key: Vec<u8>,
        value: Vec<u8>,
        ack: Ack,
        expected: ExpectedVersion,
    },
    Remove {
        namespace: String,
        key: Vec<u8>,
        ack: Ack,
        expected: ExpectedVersion,
    },
    CommitTx {
        ack: Ack,
        guards: Vec<VersionGuard>,
        mutations: Vec<WriteMutation>,
    },
    OpenSnapshot,
    CloseSnapshot {
        snapshot_id: u64,
    },
    ScanPrefix {
        namespace: String,
        prefix: Vec<u8>,
        options: ScanOptions,
        snapshot_id: Option<u64>,
    },
    ScanRange {
        namespace: String,
        start_key: Vec<u8>,
        end_key: Vec<u8>,
        options: ScanOptions,
        snapshot_id: Option<u64>,
    },
    SubscribeEachChange {
        namespace: String,
        key_prefix: Vec<u8>,
        epoch: u64,
        since_seq: u64,
    },
    SubscribeLive {
        namespace: String,
        key_prefix: Vec<u8>,
        max_pending: Option<u32>,
    },
    Checkpoint {
        dest_dir: String,
    },
    Health,
    PutBlob {
        digest: Vec<u8>,
        data: Vec<u8>,
        ack: Ack,
    },
    GetBlob {
        digest: Vec<u8>,
    },
}

/// Strongly-typed RPC response carried in `FrameKind::Response`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Response {
    Get(Option<StoredRecord>),
    GetMany(Vec<Option<StoredRecord>>),
    Commit(CommitResult),
    SnapshotOpened { snapshot_id: u64, snapshot_seq: u64 },
    Unit,
    Scan(ScanPage),
    Subscribed { epoch: u64, start_seq: u64 },
    Checkpoint { seq: u64 },
    Health(HealthStatus),
    Blob(Option<Vec<u8>>),
    Error(MembusError),
}

pub fn encode_request(req: &Request) -> Result<Bytes, MembusError> {
    let mut buf = BytesMut::with_capacity(128);
    match req {
        Request::Get {
            namespace,
            key,
            snapshot_id,
        } => {
            buf.put_u8(OP_GET);
            put_str(&mut buf, namespace);
            put_bytes(&mut buf, key);
            put_opt_u64(&mut buf, *snapshot_id);
        }
        Request::GetMany {
            namespace,
            keys,
            snapshot_id,
        } => {
            buf.put_u8(OP_GET_MANY);
            put_str(&mut buf, namespace);
            let key_count = u32::try_from(keys.len()).map_err(|_| {
                MembusError::InvalidArgument("get_many key count exceeds u32".to_owned())
            })?;
            buf.put_u32(key_count);
            for key in keys {
                put_bytes(&mut buf, key);
            }
            put_opt_u64(&mut buf, *snapshot_id);
        }
        Request::Put {
            namespace,
            key,
            value,
            ack,
            expected,
        } => {
            buf.put_u8(OP_PUT);
            put_str(&mut buf, namespace);
            put_bytes(&mut buf, key);
            put_bytes(&mut buf, value);
            buf.put_u8(*ack as u8);
            put_expected(&mut buf, *expected);
        }
        Request::Remove {
            namespace,
            key,
            ack,
            expected,
        } => {
            buf.put_u8(OP_REMOVE);
            put_str(&mut buf, namespace);
            put_bytes(&mut buf, key);
            buf.put_u8(*ack as u8);
            put_expected(&mut buf, *expected);
        }
        Request::CommitTx {
            ack,
            guards,
            mutations,
        } => {
            buf.put_u8(OP_COMMIT_TX);
            buf.put_u8(*ack as u8);
            let guard_count = u32::try_from(guards.len()).map_err(|_| {
                MembusError::InvalidArgument("commit_tx guard count exceeds u32".to_owned())
            })?;
            buf.put_u32(guard_count);
            for guard in guards {
                put_str(&mut buf, &guard.namespace);
                put_bytes(&mut buf, &guard.key);
                put_expected(&mut buf, guard.expected);
            }
            let mut_count = u32::try_from(mutations.len()).map_err(|_| {
                MembusError::InvalidArgument("commit_tx mutation count exceeds u32".to_owned())
            })?;
            buf.put_u32(mut_count);
            for m in mutations {
                put_str(&mut buf, &m.namespace);
                put_bytes(&mut buf, &m.key);
                match &m.op {
                    MutationOp::Put { value } => {
                        buf.put_u8(1);
                        put_bytes(&mut buf, value);
                    }
                    MutationOp::Remove => {
                        buf.put_u8(2);
                    }
                }
            }
        }
        Request::OpenSnapshot => {
            buf.put_u8(OP_OPEN_SNAPSHOT);
        }
        Request::CloseSnapshot { snapshot_id } => {
            buf.put_u8(OP_CLOSE_SNAPSHOT);
            buf.put_u64(*snapshot_id);
        }
        Request::ScanPrefix {
            namespace,
            prefix,
            options,
            snapshot_id,
        } => {
            buf.put_u8(OP_SCAN_PREFIX);
            put_str(&mut buf, namespace);
            put_bytes(&mut buf, prefix);
            put_scan_options(&mut buf, options)?;
            put_opt_u64(&mut buf, *snapshot_id);
        }
        Request::ScanRange {
            namespace,
            start_key,
            end_key,
            options,
            snapshot_id,
        } => {
            buf.put_u8(OP_SCAN_RANGE);
            put_str(&mut buf, namespace);
            put_bytes(&mut buf, start_key);
            put_bytes(&mut buf, end_key);
            put_scan_options(&mut buf, options)?;
            put_opt_u64(&mut buf, *snapshot_id);
        }
        Request::SubscribeEachChange {
            namespace,
            key_prefix,
            epoch,
            since_seq,
        } => {
            buf.put_u8(OP_SUBSCRIBE_EACH);
            put_str(&mut buf, namespace);
            put_bytes(&mut buf, key_prefix);
            buf.put_u64(*epoch);
            buf.put_u64(*since_seq);
        }
        Request::SubscribeLive {
            namespace,
            key_prefix,
            max_pending,
        } => {
            buf.put_u8(OP_SUBSCRIBE_LIVE);
            put_str(&mut buf, namespace);
            put_bytes(&mut buf, key_prefix);
            match max_pending {
                Some(v) => {
                    buf.put_u8(1);
                    buf.put_u32(*v);
                }
                None => buf.put_u8(0),
            }
        }
        Request::Checkpoint { dest_dir } => {
            buf.put_u8(OP_CHECKPOINT);
            put_str(&mut buf, dest_dir);
        }
        Request::Health => {
            buf.put_u8(OP_HEALTH);
        }
        Request::PutBlob { digest, data, ack } => {
            buf.put_u8(OP_PUT_BLOB);
            put_bytes(&mut buf, digest);
            put_bytes(&mut buf, data);
            buf.put_u8(*ack as u8);
        }
        Request::GetBlob { digest } => {
            buf.put_u8(OP_GET_BLOB);
            put_bytes(&mut buf, digest);
        }
    }
    Ok(buf.freeze())
}

pub fn decode_request(raw: &[u8]) -> Result<Request, MembusError> {
    let mut cur = raw;
    let op = take_u8(&mut cur)?;
    let req = match op {
        OP_GET => {
            let namespace = take_string(&mut cur)?;
            let key = take_bytes(&mut cur)?;
            let snapshot_id = take_opt_u64(&mut cur)?;
            Request::Get {
                namespace,
                key,
                snapshot_id,
            }
        }
        OP_GET_MANY => {
            let namespace = take_string(&mut cur)?;
            let count = take_count(&mut cur)?;
            let mut keys = Vec::with_capacity(bounded_vec_cap(count, cur.len()));
            for _ in 0..count {
                keys.push(take_bytes(&mut cur)?);
            }
            let snapshot_id = take_opt_u64(&mut cur)?;
            Request::GetMany {
                namespace,
                keys,
                snapshot_id,
            }
        }
        OP_PUT => {
            let namespace = take_string(&mut cur)?;
            let key = take_bytes(&mut cur)?;
            let value = take_bytes(&mut cur)?;
            let ack = Ack::from_u8(take_u8(&mut cur)?)?;
            let expected = take_expected(&mut cur)?;
            Request::Put {
                namespace,
                key,
                value,
                ack,
                expected,
            }
        }
        OP_REMOVE => {
            let namespace = take_string(&mut cur)?;
            let key = take_bytes(&mut cur)?;
            let ack = Ack::from_u8(take_u8(&mut cur)?)?;
            let expected = take_expected(&mut cur)?;
            Request::Remove {
                namespace,
                key,
                ack,
                expected,
            }
        }
        OP_COMMIT_TX => {
            let ack = Ack::from_u8(take_u8(&mut cur)?)?;
            let guard_count = take_count(&mut cur)?;
            let mut guards = Vec::with_capacity(bounded_vec_cap(guard_count, cur.len()));
            for _ in 0..guard_count {
                let namespace = take_string(&mut cur)?;
                let key = take_bytes(&mut cur)?;
                let expected = take_expected(&mut cur)?;
                guards.push(VersionGuard {
                    namespace,
                    key,
                    expected,
                });
            }
            let mut_count = take_count(&mut cur)?;
            let mut mutations = Vec::with_capacity(bounded_vec_cap(mut_count, cur.len()));
            for _ in 0..mut_count {
                let namespace = take_string(&mut cur)?;
                let key = take_bytes(&mut cur)?;
                let op_tag = take_u8(&mut cur)?;
                let op = match op_tag {
                    1 => MutationOp::Put {
                        value: take_bytes(&mut cur)?,
                    },
                    2 => MutationOp::Remove,
                    other => {
                        return Err(MembusError::Protocol(format!(
                            "unknown mutation op tag: {other}"
                        )));
                    }
                };
                mutations.push(WriteMutation { namespace, key, op });
            }
            Request::CommitTx {
                ack,
                guards,
                mutations,
            }
        }
        OP_OPEN_SNAPSHOT => Request::OpenSnapshot,
        OP_CLOSE_SNAPSHOT => Request::CloseSnapshot {
            snapshot_id: take_u64(&mut cur)?,
        },
        OP_SCAN_PREFIX => {
            let namespace = take_string(&mut cur)?;
            let prefix = take_bytes(&mut cur)?;
            let options = take_scan_options(&mut cur)?;
            let snapshot_id = take_opt_u64(&mut cur)?;
            Request::ScanPrefix {
                namespace,
                prefix,
                options,
                snapshot_id,
            }
        }
        OP_SCAN_RANGE => {
            let namespace = take_string(&mut cur)?;
            let start_key = take_bytes(&mut cur)?;
            let end_key = take_bytes(&mut cur)?;
            let options = take_scan_options(&mut cur)?;
            let snapshot_id = take_opt_u64(&mut cur)?;
            Request::ScanRange {
                namespace,
                start_key,
                end_key,
                options,
                snapshot_id,
            }
        }
        OP_SUBSCRIBE_EACH => {
            let namespace = take_string(&mut cur)?;
            let key_prefix = take_bytes(&mut cur)?;
            let epoch = take_u64(&mut cur)?;
            let since_seq = take_u64(&mut cur)?;
            Request::SubscribeEachChange {
                namespace,
                key_prefix,
                epoch,
                since_seq,
            }
        }
        OP_SUBSCRIBE_LIVE => {
            let namespace = take_string(&mut cur)?;
            let key_prefix = take_bytes(&mut cur)?;
            let max_pending = match take_u8(&mut cur)? {
                0 => None,
                1 => Some(take_u32(&mut cur)?),
                other => {
                    return Err(MembusError::Protocol(format!(
                        "invalid optional u32 tag: {other}"
                    )));
                }
            };
            Request::SubscribeLive {
                namespace,
                key_prefix,
                max_pending,
            }
        }
        OP_CHECKPOINT => Request::Checkpoint {
            dest_dir: take_string(&mut cur)?,
        },
        OP_HEALTH => Request::Health,
        OP_PUT_BLOB => {
            let digest = take_bytes(&mut cur)?;
            let data = take_bytes(&mut cur)?;
            let ack = Ack::from_u8(take_u8(&mut cur)?)?;
            Request::PutBlob { digest, data, ack }
        }
        OP_GET_BLOB => Request::GetBlob {
            digest: take_bytes(&mut cur)?,
        },
        other => {
            return Err(MembusError::Protocol(format!(
                "unknown request opcode: {other}"
            )));
        }
    };
    ensure_empty(cur)?;
    Ok(req)
}

pub fn encode_response(resp: &Response) -> Bytes {
    let mut buf = BytesMut::with_capacity(128);
    match resp {
        Response::Get(rec) => {
            buf.put_u8(RESP_GET);
            put_opt_record(&mut buf, rec.as_ref());
        }
        Response::GetMany(recs) => {
            buf.put_u8(RESP_GET_MANY);
            buf.put_u32(recs.len() as u32);
            for rec in recs {
                put_opt_record(&mut buf, rec.as_ref());
            }
        }
        Response::Commit(commit) => {
            buf.put_u8(RESP_COMMIT);
            buf.put_u64(commit.seq);
            buf.put_u64(commit.epoch);
            buf.put_u32(commit.versions.len() as u32);
            for ver in &commit.versions {
                buf.put_u64(*ver);
            }
        }
        Response::SnapshotOpened {
            snapshot_id,
            snapshot_seq,
        } => {
            buf.put_u8(RESP_SNAPSHOT_OPENED);
            buf.put_u64(*snapshot_id);
            buf.put_u64(*snapshot_seq);
        }
        Response::Unit => {
            buf.put_u8(RESP_UNIT);
        }
        Response::Scan(page) => {
            buf.put_u8(RESP_SCAN);
            buf.put_u64(page.snapshot_seq);
            buf.put_u8(u8::from(page.truncated));
            match &page.next_cursor {
                Some(cursor) => {
                    buf.put_u8(1);
                    put_bytes(&mut buf, cursor);
                }
                None => buf.put_u8(0),
            }
            buf.put_u32(page.records.len() as u32);
            for rec in &page.records {
                put_record(&mut buf, rec);
            }
        }
        Response::Subscribed { epoch, start_seq } => {
            buf.put_u8(RESP_SUBSCRIBED);
            buf.put_u64(*epoch);
            buf.put_u64(*start_seq);
        }
        Response::Checkpoint { seq } => {
            buf.put_u8(RESP_CHECKPOINT);
            buf.put_u64(*seq);
        }
        Response::Health(h) => {
            buf.put_u8(RESP_HEALTH);
            buf.put_u64(h.epoch);
            buf.put_u64(h.latest_seq);
            buf.put_u64(h.log_horizon_seq);
            buf.put_u64(h.in_flight_ops as u64);
            buf.put_u64(h.in_flight_bytes as u64);
            buf.put_u64(h.active_connections as u64);
            buf.put_u64(h.active_subscriptions as u64);
        }
        Response::Blob(data) => {
            buf.put_u8(RESP_BLOB);
            match data {
                Some(bytes) => {
                    buf.put_u8(1);
                    put_bytes(&mut buf, bytes);
                }
                None => buf.put_u8(0),
            }
        }
        Response::Error(err) => {
            buf.put_u8(RESP_ERROR);
            put_error(&mut buf, err);
        }
    }
    buf.freeze()
}

pub fn decode_response(raw: &[u8]) -> Result<Response, MembusError> {
    let mut cur = raw;
    let tag = take_u8(&mut cur)?;
    let resp = match tag {
        RESP_GET => Response::Get(take_opt_record(&mut cur)?),
        RESP_GET_MANY => {
            let count = take_count(&mut cur)?;
            let mut recs = Vec::with_capacity(bounded_vec_cap(count, cur.len()));
            for _ in 0..count {
                recs.push(take_opt_record(&mut cur)?);
            }
            Response::GetMany(recs)
        }
        RESP_COMMIT => {
            let seq = take_u64(&mut cur)?;
            let epoch = take_u64(&mut cur)?;
            let count = take_count(&mut cur)?;
            let mut versions = Vec::with_capacity(bounded_vec_cap(count, cur.len()));
            for _ in 0..count {
                versions.push(take_u64(&mut cur)?);
            }
            Response::Commit(CommitResult {
                seq,
                epoch,
                versions,
            })
        }
        RESP_SNAPSHOT_OPENED => Response::SnapshotOpened {
            snapshot_id: take_u64(&mut cur)?,
            snapshot_seq: take_u64(&mut cur)?,
        },
        RESP_UNIT => Response::Unit,
        RESP_SCAN => {
            let snapshot_seq = take_u64(&mut cur)?;
            let truncated = take_u8(&mut cur)? != 0;
            let next_cursor = match take_u8(&mut cur)? {
                0 => None,
                1 => Some(take_bytes(&mut cur)?),
                other => {
                    return Err(MembusError::Protocol(format!(
                        "invalid cursor presence tag: {other}"
                    )));
                }
            };
            let count = take_count(&mut cur)?;
            let mut records = Vec::with_capacity(bounded_vec_cap(count, cur.len()));
            for _ in 0..count {
                records.push(take_record(&mut cur)?);
            }
            Response::Scan(ScanPage {
                records,
                next_cursor,
                truncated,
                snapshot_seq,
            })
        }
        RESP_SUBSCRIBED => Response::Subscribed {
            epoch: take_u64(&mut cur)?,
            start_seq: take_u64(&mut cur)?,
        },
        RESP_CHECKPOINT => Response::Checkpoint {
            seq: take_u64(&mut cur)?,
        },
        RESP_HEALTH => Response::Health(HealthStatus {
            epoch: take_u64(&mut cur)?,
            latest_seq: take_u64(&mut cur)?,
            log_horizon_seq: take_u64(&mut cur)?,
            in_flight_ops: take_u64(&mut cur)? as usize,
            in_flight_bytes: take_u64(&mut cur)? as usize,
            active_connections: take_u64(&mut cur)? as usize,
            active_subscriptions: take_u64(&mut cur)? as usize,
        }),
        RESP_BLOB => {
            let blob = match take_u8(&mut cur)? {
                0 => None,
                1 => Some(take_bytes(&mut cur)?),
                other => {
                    return Err(MembusError::Protocol(format!(
                        "invalid blob presence tag: {other}"
                    )));
                }
            };
            Response::Blob(blob)
        }
        RESP_ERROR => Response::Error(take_error(&mut cur)?),
        other => {
            return Err(MembusError::Protocol(format!(
                "unknown response tag: {other}"
            )));
        }
    };
    ensure_empty(cur)?;
    Ok(resp)
}

pub fn encode_signal_event(ev: &SignalEvent) -> Bytes {
    let mut buf = BytesMut::with_capacity(64 + ev.namespace.len() + ev.key.len());
    buf.put_u64(ev.seq);
    buf.put_u64(ev.epoch);
    buf.put_u64(ev.version);
    buf.put_u8(ev.kind as u8);
    put_str(&mut buf, &ev.namespace);
    put_bytes(&mut buf, &ev.key);
    match &ev.value_hint {
        Some(hint) => {
            buf.put_u8(1);
            put_bytes(&mut buf, hint);
        }
        None => buf.put_u8(0),
    }
    buf.freeze()
}

pub fn decode_signal_event(raw: &[u8]) -> Result<SignalEvent, MembusError> {
    let mut cur = raw;
    let seq = take_u64(&mut cur)?;
    let epoch = take_u64(&mut cur)?;
    let version = take_u64(&mut cur)?;
    let kind = SignalKind::from_u8(take_u8(&mut cur)?)?;
    let namespace = take_string(&mut cur)?;
    let key = take_bytes(&mut cur)?;
    let value_hint = match take_u8(&mut cur)? {
        0 => None,
        1 => Some(take_bytes(&mut cur)?),
        other => {
            return Err(MembusError::Protocol(format!(
                "invalid value_hint tag: {other}"
            )));
        }
    };
    ensure_empty(cur)?;
    Ok(SignalEvent {
        seq,
        epoch,
        namespace,
        key,
        version,
        kind,
        value_hint,
    })
}

pub fn encode_stream_end(err: Option<&MembusError>) -> Bytes {
    let mut buf = BytesMut::with_capacity(32);
    match err {
        None => buf.put_u8(0),
        Some(e) => {
            buf.put_u8(1);
            put_error(&mut buf, e);
        }
    }
    buf.freeze()
}

pub fn decode_stream_end(raw: &[u8]) -> Result<Option<MembusError>, MembusError> {
    let mut cur = raw;
    let out = match take_u8(&mut cur)? {
        0 => None,
        1 => Some(take_error(&mut cur)?),
        other => {
            return Err(MembusError::Protocol(format!(
                "invalid stream_end tag: {other}"
            )));
        }
    };
    ensure_empty(cur)?;
    Ok(out)
}

fn put_str(buf: &mut BytesMut, s: &str) {
    put_bytes(buf, s.as_bytes());
}

fn put_bytes(buf: &mut BytesMut, b: &[u8]) {
    buf.put_u32(b.len() as u32);
    buf.put_slice(b);
}

fn put_opt_u64(buf: &mut BytesMut, val: Option<u64>) {
    match val {
        Some(v) => {
            buf.put_u8(1);
            buf.put_u64(v);
        }
        None => buf.put_u8(0),
    }
}

fn put_expected(buf: &mut BytesMut, expected: ExpectedVersion) {
    match expected {
        ExpectedVersion::Any => buf.put_u8(0),
        ExpectedVersion::Absent => buf.put_u8(1),
        ExpectedVersion::Version(v) => {
            buf.put_u8(2);
            buf.put_u64(v);
        }
    }
}

fn put_scan_options(buf: &mut BytesMut, opts: &ScanOptions) -> Result<(), MembusError> {
    let limit = u32::try_from(opts.limit).map_err(|_| {
        MembusError::InvalidArgument(format!(
            "scan limit {} exceeds u32 maximum {}",
            opts.limit,
            u32::MAX
        ))
    })?;
    match &opts.after_key {
        Some(k) => {
            buf.put_u8(1);
            put_bytes(buf, k);
        }
        None => buf.put_u8(0),
    }
    buf.put_u32(limit);
    buf.put_u64(opts.max_bytes as u64);
    Ok(())
}

fn put_record(buf: &mut BytesMut, rec: &StoredRecord) {
    put_bytes(buf, &rec.key);
    put_bytes(buf, &rec.value);
    buf.put_u64(rec.meta.version);
    buf.put_u8(rec.meta.flags);
    buf.put_u64(rec.meta.ts_ms);
    buf.put_u32(rec.meta.crc32);
}

fn put_opt_record(buf: &mut BytesMut, rec: Option<&StoredRecord>) {
    match rec {
        Some(r) => {
            buf.put_u8(1);
            put_record(buf, r);
        }
        None => buf.put_u8(0),
    }
}

fn put_error(buf: &mut BytesMut, err: &MembusError) {
    match err {
        MembusError::InvalidArgument(msg) => {
            buf.put_u8(ERR_INVALID_ARG);
            put_str(buf, msg);
        }
        MembusError::Conflict {
            namespace,
            key,
            expected,
            actual,
        } => {
            buf.put_u8(ERR_CONFLICT);
            put_str(buf, namespace);
            put_bytes(buf, key);
            put_expected(buf, *expected);
            put_opt_u64(buf, *actual);
        }
        MembusError::TxConflict(msg) => {
            buf.put_u8(ERR_TX_CONFLICT);
            put_str(buf, msg);
        }
        MembusError::Busy {
            retry_after_ms,
            reason,
        } => {
            buf.put_u8(ERR_BUSY);
            buf.put_u32(*retry_after_ms);
            put_str(buf, reason);
        }
        MembusError::Gap {
            earliest_available_seq,
            current_epoch,
        } => {
            buf.put_u8(ERR_GAP);
            buf.put_u64(*earliest_available_seq);
            buf.put_u64(*current_epoch);
        }
        MembusError::NotFound(msg) => {
            buf.put_u8(ERR_NOT_FOUND);
            put_str(buf, msg);
        }
        MembusError::Corruption(msg) => {
            buf.put_u8(ERR_CORRUPTION);
            put_str(buf, msg);
        }
        MembusError::Storage(msg) => {
            buf.put_u8(ERR_STORAGE);
            put_str(buf, msg);
        }
        MembusError::Protocol(msg) => {
            buf.put_u8(ERR_PROTOCOL);
            put_str(buf, msg);
        }
        MembusError::FrameTooLarge { len, max_len } => {
            buf.put_u8(ERR_FRAME_TOO_LARGE);
            buf.put_u64(*len as u64);
            buf.put_u64(*max_len as u64);
        }
        MembusError::Io(msg) => {
            buf.put_u8(ERR_IO);
            put_str(buf, msg);
        }
        MembusError::Closed => {
            buf.put_u8(ERR_CLOSED);
        }
    }
}

fn take_u8(cur: &mut &[u8]) -> Result<u8, MembusError> {
    if cur.is_empty() {
        return Err(MembusError::Protocol("unexpected eof decoding u8".to_owned()));
    }
    let v = cur[0];
    *cur = &cur[1..];
    Ok(v)
}

fn take_u32(cur: &mut &[u8]) -> Result<u32, MembusError> {
    let s = take_slice(cur, 4)?;
    Ok(u32::from_be_bytes([s[0], s[1], s[2], s[3]]))
}

fn take_u64(cur: &mut &[u8]) -> Result<u64, MembusError> {
    let s = take_slice(cur, 8)?;
    Ok(u64::from_be_bytes([
        s[0], s[1], s[2], s[3], s[4], s[5], s[6], s[7],
    ]))
}

fn take_slice<'a>(cur: &mut &'a [u8], len: usize) -> Result<&'a [u8], MembusError> {
    if cur.len() < len {
        return Err(MembusError::Protocol(format!(
            "unexpected eof decoding {len} bytes (remaining {})",
            cur.len()
        )));
    }
    let (head, tail) = cur.split_at(len);
    *cur = tail;
    Ok(head)
}

fn take_bytes(cur: &mut &[u8]) -> Result<Vec<u8>, MembusError> {
    let len = take_u32(cur)? as usize;
    Ok(take_slice(cur, len)?.to_vec())
}

fn take_string(cur: &mut &[u8]) -> Result<String, MembusError> {
    let bytes = take_bytes(cur)?;
    String::from_utf8(bytes)
        .map_err(|err| MembusError::Protocol(format!("invalid utf8 string: {err}")))
}

fn bounded_vec_cap(count: usize, remaining_bytes: usize) -> usize {
    count.min(remaining_bytes).min(256)
}

fn take_count(cur: &mut &[u8]) -> Result<usize, MembusError> {
    let count = take_u32(cur)? as usize;
    if count > MAX_BATCH_ITEMS {
        return Err(MembusError::Protocol(format!(
            "batch count {count} exceeds limit {MAX_BATCH_ITEMS}"
        )));
    }
    Ok(count)
}

fn take_opt_u64(cur: &mut &[u8]) -> Result<Option<u64>, MembusError> {
    match take_u8(cur)? {
        0 => Ok(None),
        1 => Ok(Some(take_u64(cur)?)),
        other => Err(MembusError::Protocol(format!(
            "invalid optional u64 tag: {other}"
        ))),
    }
}

fn take_expected(cur: &mut &[u8]) -> Result<ExpectedVersion, MembusError> {
    match take_u8(cur)? {
        0 => Ok(ExpectedVersion::Any),
        1 => Ok(ExpectedVersion::Absent),
        2 => Ok(ExpectedVersion::Version(take_u64(cur)?)),
        other => Err(MembusError::Protocol(format!(
            "invalid expected_version tag: {other}"
        ))),
    }
}

fn take_scan_options(cur: &mut &[u8]) -> Result<ScanOptions, MembusError> {
    let after_key = match take_u8(cur)? {
        0 => None,
        1 => Some(take_bytes(cur)?),
        other => {
            return Err(MembusError::Protocol(format!(
                "invalid after_key tag: {other}"
            )));
        }
    };
    let limit = take_u32(cur)? as usize;
    let max_bytes = take_u64(cur)? as usize;
    Ok(ScanOptions {
        after_key,
        limit,
        max_bytes,
    })
}

fn take_record(cur: &mut &[u8]) -> Result<StoredRecord, MembusError> {
    let key = take_bytes(cur)?;
    let value = take_bytes(cur)?;
    let version = take_u64(cur)?;
    let flags = take_u8(cur)?;
    let ts_ms = take_u64(cur)?;
    let crc32 = take_u32(cur)?;
    Ok(StoredRecord {
        key,
        value,
        meta: RecordMeta {
            version,
            flags,
            ts_ms,
            crc32,
        },
    })
}

fn take_opt_record(cur: &mut &[u8]) -> Result<Option<StoredRecord>, MembusError> {
    match take_u8(cur)? {
        0 => Ok(None),
        1 => Ok(Some(take_record(cur)?)),
        other => Err(MembusError::Protocol(format!(
            "invalid optional record tag: {other}"
        ))),
    }
}

fn take_error(cur: &mut &[u8]) -> Result<MembusError, MembusError> {
    match take_u8(cur)? {
        ERR_INVALID_ARG => Ok(MembusError::InvalidArgument(take_string(cur)?)),
        ERR_CONFLICT => Ok(MembusError::Conflict {
            namespace: take_string(cur)?,
            key: take_bytes(cur)?,
            expected: take_expected(cur)?,
            actual: take_opt_u64(cur)?,
        }),
        ERR_TX_CONFLICT => Ok(MembusError::TxConflict(take_string(cur)?)),
        ERR_BUSY => Ok(MembusError::Busy {
            retry_after_ms: take_u32(cur)?,
            reason: take_string(cur)?,
        }),
        ERR_GAP => Ok(MembusError::Gap {
            earliest_available_seq: take_u64(cur)?,
            current_epoch: take_u64(cur)?,
        }),
        ERR_NOT_FOUND => Ok(MembusError::NotFound(take_string(cur)?)),
        ERR_CORRUPTION => Ok(MembusError::Corruption(take_string(cur)?)),
        ERR_STORAGE => Ok(MembusError::Storage(take_string(cur)?)),
        ERR_PROTOCOL => Ok(MembusError::Protocol(take_string(cur)?)),
        ERR_FRAME_TOO_LARGE => Ok(MembusError::FrameTooLarge {
            len: take_u64(cur)? as usize,
            max_len: take_u64(cur)? as usize,
        }),
        ERR_IO => Ok(MembusError::Io(take_string(cur)?)),
        ERR_CLOSED => Ok(MembusError::Closed),
        other => Err(MembusError::Protocol(format!(
            "unknown wire error tag: {other}"
        ))),
    }
}

fn ensure_empty(cur: &[u8]) -> Result<(), MembusError> {
    if cur.is_empty() {
        Ok(())
    } else {
        Err(MembusError::Protocol(format!(
            "trailing {} bytes in wire message",
            cur.len()
        )))
    }
}
