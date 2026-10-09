//! membus.kv/1 slice 1: bounded framing, independent committed reads, durable replay.
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::Path,
    sync::{Condvar, Mutex, RwLock},
    time::{Duration, SystemTime, UNIX_EPOCH},
};
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
];
pub const HELLO: u8 = 1;
pub const GET: u8 = 2;
pub const GET_MANY: u8 = 3;
pub const PUT: u8 = 4;
pub const CONDITIONAL_PUT: u8 = 5;
pub const REMOVE: u8 = 6;
pub const BATCH: u8 = 7;
pub const HEALTH: u8 = 8;
pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
pub fn crc32(bytes: &[u8]) -> u32 {
    let mut c = !0u32;
    for &b in bytes {
        c ^= b as u32;
        for _ in 0..8 {
            c = (c >> 1) ^ if c & 1 != 0 { 0xedb88320 } else { 0 };
        }
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
        let b = self.bytes(MAX_ID)?;
        let s = std::str::from_utf8(b).map_err(|_| 3u16)?;
        if s.is_empty() {
            return Err(3);
        }
        Ok(s.to_owned())
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
    let mut b = Vec::new();
    b.extend(1u16.to_le_bytes());
    b.extend(0u16.to_le_bytes());
    b.push(op);
    b.extend(id.to_le_bytes());
    b.extend(payload);
    let mut out = (b.len() as u32).to_le_bytes().to_vec();
    out.extend(b);
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
#[derive(Clone, Debug, PartialEq)]
pub struct Value {
    pub version: u64,
    pub ack: u8,
    pub bytes: Vec<u8>,
    pub tombstone: bool,
}
#[derive(Clone, Debug)]
pub struct Mutation {
    pub ns: String,
    pub key: String,
    pub op_id: String,
    pub deadline: u64,
    pub ack: u8,
    pub expected: Option<u64>,
    pub bytes: Vec<u8>,
    pub remove: bool,
}
#[derive(Clone, Debug, PartialEq)]
pub struct Outcome {
    pub code: u16,
    pub version: u64,
    pub ack: u8,
}
impl Outcome {
    fn error(code: u16) -> Self {
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
#[derive(Clone)]
struct Dedup {
    fingerprint: Vec<u8>,
    at: u64,
    outcome: Option<Outcome>,
}
struct State {
    pending: HashMap<(String, String), Meta>,
    metadata: HashMap<(String, String), Meta>,
    namespace_counts: HashMap<String, usize>,
    keys: usize,
    committed_bytes: usize,
    dedup_bytes: usize,
    dedup: HashMap<(String, String), Dedup>,
    retired: VecDeque<(String, String)>,
    retired_set: HashSet<(String, String)>,
    expiry: VecDeque<((String, String), u64)>,
    next: u64,
    reserved_bytes: usize,
    read_only: bool,
    warming: bool,
}
#[derive(Clone)]
struct Meta {
    version: u64,
    tombstone: bool,
    bytes: usize,
}
pub struct Log {
    file: File,
    pub bytes: u64,
    pub flushes: u64,
    last_flush: u64,
    poisoned: bool,
}
#[derive(Clone)]
pub struct Config {
    pub quota: usize,
    pub index_cap: usize,
    pub capacity: usize,
    pub queue_bytes: usize,
    pub retry_ms: u64,
    pub dedup_cap: usize,
    pub flush_ms: u64,
    pub allow_mem: bool,
    pub write_delay_ms: u64,
    pub fail_after_append: bool,
    pub fail_partial_append: bool,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            quota: 500_000,
            index_cap: 917_000,
            capacity: 256 * 1024 * 1024,
            queue_bytes: 64 * 1024 * 1024,
            retry_ms: 24 * 3600 * 1000,
            dedup_cap: 262144,
            flush_ms: 50,
            allow_mem: false,
            write_delay_ms: 0,
            fail_after_append: false,
            fail_partial_append: false,
        }
    }
}
pub struct Store {
    committed: RwLock<HashMap<(String, String), Value>>,
    state: Mutex<State>,
    done: Condvar,
    logs: Vec<Mutex<Log>>,
    flush_gate: Mutex<u64>,
    pub config: Config,
    pub epoch: u64,
    _owner: File,
}
fn mutation_bytes(m: &Mutation, version: u64, at: u64) -> Vec<u8> {
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
fn fingerprint(m: &Mutation) -> Vec<u8> {
    mutation_bytes(m, 0, 0)
}
fn stripe(ns: &str, key: &str) -> usize {
    let mut b = ns.as_bytes().to_vec();
    b.push(0);
    b.extend(key.as_bytes());
    crc32(&b) as usize % 4
}
impl Store {
    pub fn open(path: &Path, config: Config) -> io::Result<Self> {
        fs::create_dir_all(path)?;
        let mut options = OpenOptions::new();
        options.write(true).create(true).truncate(false);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options.share_mode(0);
        }
        #[cfg(not(windows))]
        {
            options.create_new(true);
        }
        let owner = options.open(path.join("owner.lock"))?;
        Self::open_owned(path, config, owner)
    }
    fn open_owned(path: &Path, config: Config, owner: File) -> io::Result<Self> {
        let mut committed = HashMap::new();
        let mut dedup = HashMap::new();
        let mut next = 1u64;
        let mut logs = Vec::new();
        let mut torn = Vec::new();
        let now = now_ms();
        for i in 0..4 {
            let p = path.join(format!("stripe-{i}.log"));
            let mut f = OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(&p)?;
            let mut offset = 0u64;
            let len = f.metadata()?.len();
            while offset < len {
                let mut head = [0; 16];
                if len - offset < 16 {
                    torn.push((p.clone(), offset));
                    break;
                }
                f.read_exact(&mut head)?;
                if &head[0..4] != b"MBK1" {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("interior-corrupt {} offset {}: magic", p.display(), offset),
                    ));
                }
                let n = u32::from_le_bytes(head[4..8].try_into().unwrap()) as usize;
                if u32::from_le_bytes(head[8..12].try_into().unwrap()) != !(n as u32) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "interior-corrupt: length complement",
                    ));
                }
                if n > MAX_FRAME {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "interior-corrupt: invalid chunk length",
                    ));
                }
                if len - offset - 16 < n as u64 {
                    torn.push((p.clone(), offset));
                    break;
                }
                let mut b = vec![0; n];
                f.read_exact(&mut b)?;
                if crc32(&b) != u32::from_le_bytes(head[12..16].try_into().unwrap()) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("interior-corrupt {} offset {}: CRC", p.display(), offset),
                    ));
                }
                let parsed = (|| -> Result<_, u16> {
                    let mut c = Cursor::new(&b);
                    let at = c.u64()?;
                    let version = c.u64()?;
                    let ack = c.u8()?;
                    let remove = c.u8()? != 0;
                    let ns = c.text()?;
                    let key = c.text()?;
                    let op_id = c.text()?;
                    let expected = c.u64()?;
                    let bytes = c.bytes(MAX_VALUE)?.to_vec();
                    c.end()?;
                    if !matches!(ack, 1 | 2) {
                        return Err(3);
                    }
                    Ok((
                        Mutation {
                            ns,
                            key,
                            op_id,
                            deadline: 0,
                            ack,
                            expected: if expected == u64::MAX {
                                None
                            } else {
                                Some(expected)
                            },
                            bytes,
                            remove,
                        },
                        version,
                        at,
                    ))
                })();
                let (m, v, at) = parsed.map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "interior-corrupt: payload")
                })?;
                if stripe(&m.ns, &m.key) != i {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "interior-corrupt: wrong stripe",
                    ));
                }
                next = next.max(v + 1);
                let k = (m.ns.clone(), m.key.clone());
                let value = Value {
                    version: v,
                    ack: m.ack,
                    bytes: m.bytes.clone(),
                    tombstone: m.remove,
                };
                if committed
                    .get(&k)
                    .map_or(true, |old: &Value| old.version < v)
                {
                    committed.insert(k, value);
                }
                if now.saturating_sub(at) <= config.retry_ms {
                    dedup.insert(
                        (m.ns.clone(), m.op_id.clone()),
                        Dedup {
                            fingerprint: fingerprint(&m),
                            at,
                            outcome: Some(Outcome {
                                code: 0,
                                version: v,
                                ack: m.ack,
                            }),
                        },
                    );
                }
                offset += 16 + n as u64;
            }
            drop(f);
            let file = OpenOptions::new().append(true).open(&p)?;
            logs.push(Mutex::new(Log {
                file,
                bytes: 0,
                flushes: 0,
                last_flush: 0,
                poisoned: false,
            }));
        }
        if committed.len() > config.index_cap
            || committed.values().map(|v| v.bytes.len()).sum::<usize>()
                + dedup
                    .values()
                    .map(|d: &Dedup| d.fingerprint.len())
                    .sum::<usize>()
                > config.capacity
            || dedup.len() > config.dedup_cap
        {
            return Err(io::Error::other("replayed state exceeds configured limits"));
        }
        for (p, offset) in torn {
            OpenOptions::new().write(true).open(p)?.set_len(offset)?;
        }
        let epoch_path = path.join("epoch");
        let previous = if epoch_path.exists() {
            let b = fs::read(&epoch_path)?;
            if b.len() != 8 {
                return Err(io::Error::new(io::ErrorKind::InvalidData, "invalid epoch"));
            }
            u64::from_le_bytes(b.try_into().unwrap())
        } else {
            0
        };
        let epoch = previous
            .checked_add(1)
            .ok_or_else(|| io::Error::other("epoch exhausted"))?;
        let epoch_tmp = path.join("epoch.tmp");
        {
            let mut f = File::create(&epoch_tmp)?;
            f.write_all(&epoch.to_le_bytes())?;
            f.sync_all()?;
        }
        fs::rename(epoch_tmp, epoch_path)?;
        let metadata: HashMap<_, _> = committed
            .iter()
            .map(|(k, v)| {
                (
                    k.clone(),
                    Meta {
                        version: v.version,
                        tombstone: v.tombstone,
                        bytes: v.bytes.len(),
                    },
                )
            })
            .collect();
        let mut namespace_counts = HashMap::new();
        for (ns, _) in metadata.keys() {
            *namespace_counts.entry(ns.clone()).or_insert(0) += 1;
        }
        let keys = metadata.len();
        let committed_bytes = metadata.values().map(|m| m.bytes).sum();
        let dedup_bytes = dedup.values().map(|d| d.fingerprint.len()).sum();
        let mut expiry: Vec<_> = dedup.iter().map(|(k, d)| (k.clone(), d.at)).collect();
        expiry.sort_by_key(|(_, at)| *at);
        Ok(Self {
            committed: RwLock::new(committed),
            state: Mutex::new(State {
                pending: HashMap::new(),
                metadata,
                namespace_counts,
                keys,
                committed_bytes,
                dedup_bytes,
                dedup,
                retired: VecDeque::new(),
                retired_set: HashSet::new(),
                expiry: expiry.into(),
                next,
                reserved_bytes: 0,
                read_only: false,
                warming: false,
            }),
            done: Condvar::new(),
            logs,
            flush_gate: Mutex::new(0),
            config,
            epoch,
            _owner: owner,
        })
    }
    pub fn get(&self, ns: &str, key: &str) -> Option<Value> {
        self.committed
            .read()
            .unwrap()
            .get(&(ns.to_owned(), key.to_owned()))
            .filter(|v| !v.tombstone)
            .cloned()
    }
    pub fn metrics(&self) -> (u64, u64, usize) {
        let mut b = 0;
        let mut f = 0;
        for l in &self.logs {
            let l = l.lock().unwrap();
            b += l.bytes;
            f += l.flushes;
        }
        (b, f, self.committed.read().unwrap().len())
    }
    pub fn set_read_only(&self, b: bool) {
        self.state.lock().unwrap().read_only = b
    }
    pub fn set_warming(&self, b: bool) {
        self.state.lock().unwrap().warming = b
    }
    pub fn pending(&self, ns: &str, key: &str) -> bool {
        self.state
            .lock()
            .unwrap()
            .pending
            .contains_key(&(ns.into(), key.into()))
    }
    pub fn mutate(&self, m: Mutation) -> Outcome {
        if m.ns.is_empty() || m.key.is_empty() || m.op_id.is_empty() {
            return Outcome::error(3);
        }
        if m.ns.len() > MAX_ID
            || m.key.len() > MAX_ID
            || m.op_id.len() > MAX_ID
            || m.bytes.len() > MAX_VALUE
        {
            return Outcome::error(4);
        }
        if m.ack > 2 {
            return Outcome::error(3);
        }
        if m.ack == 0 && !self.config.allow_mem {
            return Outcome::error(15);
        }
        let k = (m.ns.clone(), m.key.clone());
        let id = (m.ns.clone(), m.op_id.clone());
        let fp = fingerprint(&m);
        let at = now_ms();
        let mut state = self.state.lock().unwrap();
        if let Some(d) = state.dedup.get(&id) {
            if d.fingerprint != fp {
                return Outcome::error(14);
            }
            if at.saturating_sub(d.at) > self.config.retry_ms {
                return Outcome::error(13);
            }
            loop {
                if let Some(o) = state.dedup.get(&id).and_then(|d| d.outcome.clone()) {
                    return o;
                }
                let wait = m.deadline.saturating_sub(now_ms());
                if wait == 0 {
                    return Outcome::error(17);
                }
                let (s, _) = self
                    .done
                    .wait_timeout(state, Duration::from_millis(wait))
                    .unwrap();
                state = s;
            }
        }
        if state.retired_set.contains(&id) {
            return Outcome::error(13);
        }
        if now_ms() >= m.deadline {
            return Outcome::error(5);
        }
        if state.warming {
            return Outcome::error(11);
        }
        if state.read_only {
            return Outcome::error(10);
        }
        let old = state.metadata.get(&k).cloned();
        if let Some(expected) = m.expected {
            let pending = state.pending.get(&k);
            if pending.is_some() {
                return Outcome::error(19);
            }
            let visible = pending.or(old.as_ref());
            let matches = if expected == 0 {
                pending.is_none() && old.as_ref().map_or(true, |v| v.tombstone)
            } else {
                visible.is_some_and(|v| !v.tombstone && v.version == expected)
            };
            if !matches {
                return Outcome::error(12);
            }
        }
        if state.pending.contains_key(&k) {
            return Outcome::error(6);
        }
        let new = old.is_none();
        if new && state.keys >= self.config.index_cap {
            return Outcome::error(9);
        }
        if new && state.namespace_counts.get(&m.ns).copied().unwrap_or(0) >= self.config.quota {
            return Outcome::error(8);
        }
        while let Some((key, time)) = state.expiry.front().cloned() {
            if at.saturating_sub(time) <= self.config.retry_ms {
                break;
            }
            if state.dedup.get(&key).is_some_and(|d| d.outcome.is_none()) {
                break;
            }
            state.expiry.pop_front();
            if let Some(d) = state.dedup.remove(&key) {
                state.dedup_bytes -= d.fingerprint.len();
                state.retired_set.insert(key.clone());
                state.retired.push_back(key);
            }
        }
        while state.retired.len() > self.config.dedup_cap {
            let k = state.retired.pop_front().unwrap();
            state.retired_set.remove(&k);
        }
        if state.committed_bytes
            + state.dedup_bytes
            + state.reserved_bytes
            + m.bytes.len()
            + fp.len()
            > self.config.capacity
        {
            return Outcome::error(7);
        }
        if state.reserved_bytes + m.bytes.len() > self.config.queue_bytes {
            return Outcome::error(6);
        }
        if state.dedup.len() >= self.config.dedup_cap {
            return Outcome::error(18);
        }
        // The clock check immediately precedes insertion: this is the admission point.
        if now_ms() >= m.deadline {
            return Outcome::error(5);
        }
        let version = state.next;
        state.next += 1;
        let value = Value {
            version,
            ack: m.ack,
            bytes: m.bytes.clone(),
            tombstone: m.remove,
        };
        state.pending.insert(
            k.clone(),
            Meta {
                version,
                tombstone: m.remove,
                bytes: m.bytes.len(),
            },
        );
        if new {
            state.keys += 1;
            *state.namespace_counts.entry(m.ns.clone()).or_default() += 1;
        }
        state.reserved_bytes += m.bytes.len();
        state.dedup_bytes += fp.len();
        state.expiry.push_back((id.clone(), at));
        state.dedup.insert(
            id.clone(),
            Dedup {
                fingerprint: fp,
                at,
                outcome: None,
            },
        );
        drop(state);
        if self.config.write_delay_ms > 0 {
            std::thread::sleep(Duration::from_millis(self.config.write_delay_ms))
        }
        let result = if m.ack == 0 {
            Ok(())
        } else {
            let b = mutation_bytes(&m, version, at);
            let mut record = b"MBK1".to_vec();
            record.extend((b.len() as u32).to_le_bytes());
            record.extend((!(b.len() as u32)).to_le_bytes());
            record.extend(crc32(&b).to_le_bytes());
            record.extend(b);
            let log_index = stripe(&m.ns, &m.key);
            let mut log = self.logs[log_index].lock().unwrap();
            // A failed append may leave a torn tail. Fence this file under its owner
            // lock so an already-admitted writer cannot append a valid frame behind it.
            let r = if log.poisoned {
                Err(io::Error::other("stripe fenced after failed append"))
            } else if self.config.fail_partial_append {
                let n = record.len() / 2;
                let r = log.file.write_all(&record[..n]);
                if r.is_ok() {
                    log.bytes += n as u64;
                }
                log.poisoned = true;
                Err(io::Error::other("test injected partial append failure"))
            } else {
                let r = log.file.write_all(&record);
                if r.is_ok() {
                    log.bytes += record.len() as u64;
                } else {
                    log.poisoned = true;
                }
                r
            };
            let r = if r.is_ok() && self.config.fail_after_append {
                Err(io::Error::other("test injected post-admission I/O failure"))
            } else {
                r
            };
            let sync_file = if r.is_ok() && m.ack == 2 {
                Some(log.file.try_clone())
            } else {
                None
            };
            drop(log);
            if r.is_ok() && m.ack == 2 {
                let mut gate = self.flush_gate.lock().unwrap();
                let delay = (*gate + self.config.flush_ms).saturating_sub(now_ms());
                if delay > 0 {
                    std::thread::sleep(Duration::from_millis(delay))
                }
                let r = sync_file.unwrap().and_then(|f| f.sync_all());
                if r.is_ok() {
                    let mut log = self.logs[log_index].lock().unwrap();
                    log.flushes += 1;
                    log.last_flush = now_ms();
                    *gate = log.last_flush;
                }
                r
            } else {
                r
            }
        };
        // Publication precedes completion on every connection; readers never await log I/O.
        if result.is_ok() && m.ack != 0 {
            self.committed.write().unwrap().insert(k.clone(), value);
        }
        let outcome = if result.is_ok() {
            Outcome {
                code: 0,
                version,
                ack: m.ack,
            }
        } else {
            Outcome::error(16)
        };
        let mut state = self.state.lock().unwrap();
        state.reserved_bytes -= m.bytes.len();
        state.pending.remove(&k);
        if result.is_ok() && m.ack != 0 {
            state.committed_bytes =
                state.committed_bytes - old.as_ref().map_or(0, |v| v.bytes) + m.bytes.len();
            state.metadata.insert(
                k,
                Meta {
                    version,
                    tombstone: m.remove,
                    bytes: m.bytes.len(),
                },
            );
        } else if new {
            state.keys -= 1;
            *state.namespace_counts.get_mut(&m.ns).unwrap() -= 1;
        }
        if result.is_err() {
            state.read_only = true
        }
        state.dedup.get_mut(&id).unwrap().outcome = Some(outcome.clone());
        self.done.notify_all();
        outcome
    }
    pub fn dispatch(&self, b: &[u8]) -> Vec<u8> {
        self.dispatch_for(b, self.config.allow_mem)
    }
    pub fn dispatch_for(&self, b: &[u8], allow_mem: bool) -> Vec<u8> {
        let mut c = Cursor::new(b);
        let header = (|| -> Result<_, u16> { Ok((c.u16()?, c.u16()?, c.u8()?, c.u64()?)) })();
        let (major, minor, op, id) = match header {
            Ok(h) => h,
            Err(code) => return reply(0, 0, code, &[]),
        };
        if major != 1 || minor != 0 {
            return reply(op, id, 1, &[]);
        }
        let result = (|| -> Result<Vec<u8>, u16> {
            if op == HELLO {
                let max = c.u32()? as usize;
                let mem = c.u8()?;
                let _client = c.text()?;
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
                p.extend((CODES.len() as u16).to_le_bytes());
                for &(code, name, transient) in CODES {
                    p.extend(code.to_le_bytes());
                    p.push(transient as u8);
                    lp(&mut p, name.as_bytes());
                }
                return Ok(p);
            }
            let deadline = c.u64()?;
            let _op_id = c.text()?;
            let ns = c.text()?;
            if op == PUT || op == CONDITIONAL_PUT || op == REMOVE {
                let m = parse_mutation(&mut c, op, deadline, _op_id, ns)?;
                c.end()?;
                if m.ack == 0 && !allow_mem {
                    return Err(15);
                }
                let o = self.mutate(m);
                if o.code != 0 {
                    return Err(o.code);
                }
                return Ok(o.bytes());
            }
            if now_ms() >= deadline {
                return Err(5);
            }
            match op {
                GET => {
                    let key = c.text()?;
                    c.end()?;
                    let mut p = Vec::new();
                    encode_value(&mut p, self.get(&ns, &key));
                    Ok(p)
                }
                GET_MANY => {
                    let n = c.u16()? as usize;
                    if n > MAX_BATCH {
                        return Err(4);
                    }
                    let mut keys = Vec::with_capacity(n);
                    for _ in 0..n {
                        keys.push(c.text()?)
                    }
                    c.end()?;
                    let mut p = (n as u16).to_le_bytes().to_vec();
                    for key in keys {
                        let value = self.get(&ns, &key);
                        if p.len() + value.as_ref().map_or(1, |v| 14 + v.bytes.len()) + 15
                            > MAX_FRAME
                        {
                            return Err(4);
                        }
                        encode_value(&mut p, value);
                    }
                    Ok(p)
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
                        let item = parse_mutation(&mut c, op, deadline, id, ns.clone())?;
                        if item.ack == 0 && !allow_mem {
                            return Err(15);
                        }
                        items.push(item)
                    }
                    c.end()?;
                    let mut p = (n as u16).to_le_bytes().to_vec();
                    for m in items {
                        let o = self.mutate(m);
                        p.extend(o.code.to_le_bytes());
                        p.extend(o.bytes());
                    }
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
                        .get(&ns)
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
        })();
        match result {
            Ok(p) => reply(op, id, 0, &p),
            Err(code) => reply(op, id, code, &[]),
        }
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
fn encode_value(p: &mut Vec<u8>, v: Option<Value>) {
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
    let mut b = code.to_le_bytes().to_vec();
    b.extend(p);
    frame(op | 0x80, id, &b)
}
