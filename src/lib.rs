//! membus.kv/1 slice 1: bounded framing, independent committed reads, durable replay.
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::Path,
    sync::{mpsc, Arc, Condvar, Mutex, RwLock},
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
/// Additive kv/1 minor0 diagnostic operation; existing HEALTH layout is unchanged.
pub const IO_METRICS: u8 = 9;
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
    stripe_reserved: [usize; 4],
    stripe_items: [usize; 4],
    persisted_bytes: u64,
    pending_disk: u64,
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
    pub write_calls: u64,
    poisoned: bool,
}
#[derive(Clone)]
pub struct Config {
    pub state_limit_bytes: u64,
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
            state_limit_bytes: 8 * 1024 * 1024 * 1024,
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
    logs: Vec<Arc<Mutex<Log>>>,
    appenders: Vec<AppendOwner>,
    flush_owner: Option<std::thread::JoinHandle<()>>,
    batch_gate: Mutex<()>,
    pub config: Config,
    pub epoch: u64,
    _owner: File,
}
struct Append {
    record: Vec<u8>,
    ack: u8,
    done: mpsc::Sender<io::Result<()>>,
}
struct AppendOwner {
    sender: Option<mpsc::Sender<Append>>,
    handle: Option<std::thread::JoinHandle<()>>,
}
struct FlushWait {
    stripe: usize,
    end: u64,
    bytes: usize,
    done: mpsc::Sender<io::Result<()>>,
}

// Test-only kill checkpoints, enabled explicitly by a path-valued environment
// variable. The marker proves the process reached the injection point.
fn checkpoint(name: &str) {
    if let Some(path) = std::env::var_os(name) {
        let _ = fs::write(path, b"reached");
        std::thread::sleep(Duration::from_secs(40));
    }
}
fn write_record(log: &mut Log, mut bytes: &[u8]) -> io::Result<()> {
    while !bytes.is_empty() {
        log.write_calls += 1;
        match log.file.write(bytes) {
            Ok(0) => return Err(io::Error::new(io::ErrorKind::WriteZero, "append")),
            Ok(n) => {
                log.bytes += n as u64;
                bytes = &bytes[n..];
            }
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(())
}
fn start_owners(
    logs: &[Arc<Mutex<Log>>],
    config: &Config,
) -> (Vec<AppendOwner>, std::thread::JoinHandle<()>) {
    let (flush_send, flush_recv) = mpsc::channel::<FlushWait>();
    let flush_logs = logs.to_vec();
    let interval = config.flush_ms;
    let flush_owner = std::thread::spawn(move || {
        let mut pending = VecDeque::new();
        let mut last = 0u64;
        loop {
            if pending.is_empty() {
                match flush_recv.recv() {
                    Ok(w) => pending.push_back(w),
                    Err(_) => break,
                }
                // The gather window closes on elapsed time; bounds are enforced
                // when selecting each handle's covering group below.
                let until = std::time::Instant::now() + Duration::from_millis(interval);
                while let Some(left) = until.checked_duration_since(std::time::Instant::now()) {
                    match flush_recv.recv_timeout(left) {
                        Ok(w) => pending.push_back(w),
                        Err(_) => break,
                    }
                }
            }
            while let Ok(w) = flush_recv.try_recv() {
                pending.push_back(w);
            }
            let stripe = pending.front().unwrap().stripe;
            let mut group = Vec::new();
            let mut bytes = 0;
            let mut retained = VecDeque::new();
            while let Some(w) = pending.pop_front() {
                if w.stripe == stripe
                    && group.len() < 64
                    && (group.is_empty() || bytes + w.bytes <= 144_192)
                {
                    bytes += w.bytes;
                    group.push(w);
                } else {
                    retained.push_back(w);
                }
            }
            pending = retained;
            let delay = (last + interval).saturating_sub(now_ms());
            if delay > 0 {
                std::thread::sleep(Duration::from_millis(delay));
            }
            let (file, through) = {
                let log = flush_logs[stripe].lock().unwrap();
                (log.file.try_clone(), log.bytes)
            };
            let result = file.and_then(|f| {
                checkpoint("MEMBUS_TEST_PAUSE_SYNC");
                f.sync_all()
            });
            last = now_ms();
            {
                let mut log = flush_logs[stripe].lock().unwrap();
                log.flushes += 1;
                if result.is_err() {
                    log.poisoned = true;
                }
            }
            // A sync covers every earlier complete append on this handle,
            // including queued waiters beyond the nominal selected group.
            let mut extra = VecDeque::new();
            while let Some(w) = pending.pop_front() {
                if w.stripe == stripe && w.end <= through {
                    group.push(w);
                } else {
                    extra.push_back(w);
                }
            }
            pending = extra;
            for w in group {
                let r = match &result {
                    Ok(()) => Ok(()),
                    Err(e) => Err(io::Error::new(e.kind(), e.to_string())),
                };
                let _ = w.done.send(r);
            }
        }
    });
    let owners = logs
        .iter()
        .enumerate()
        .map(|(stripe, log)| {
            let log = log.clone();
            let config = config.clone();
            let flush = flush_send.clone();
            let (sender, receiver) = mpsc::channel::<Append>();
            let handle = std::thread::spawn(move || {
                while let Ok(job) = receiver.recv() {
                    if config.write_delay_ms > 0 {
                        std::thread::sleep(Duration::from_millis(config.write_delay_ms));
                    }
                    let mut log = log.lock().unwrap();
                    let result = if log.poisoned {
                        Err(io::Error::other("stripe fenced after failed append"))
                    } else if config.fail_partial_append
                        || std::env::var_os("MEMBUS_TEST_PAUSE_PARTIAL").is_some()
                    {
                        let half = job.record.len() / 2;
                        let r = write_record(&mut log, &job.record[..half]);
                        checkpoint("MEMBUS_TEST_PAUSE_PARTIAL");
                        log.poisoned = true;
                        r.and(Err(io::Error::other(
                            "test injected partial append failure",
                        )))
                    } else {
                        write_record(&mut log, &job.record)
                    };
                    if result.is_err() {
                        log.poisoned = true;
                    }
                    let end = log.bytes;
                    let result = if result.is_ok() && config.fail_after_append {
                        Err(io::Error::other("test injected post-admission I/O failure"))
                    } else {
                        result
                    };
                    drop(log);
                    if result.is_ok() && job.ack == 2 {
                        let wait = FlushWait {
                            stripe,
                            end,
                            bytes: job.record.len(),
                            done: job.done,
                        };
                        if let Err(e) = flush.send(wait) {
                            let _ = e.0.done.send(Err(io::Error::other("flush owner stopped")));
                        }
                    } else {
                        let _ = job.done.send(result);
                    }
                }
            });
            AppendOwner {
                sender: Some(sender),
                handle: Some(handle),
            }
        })
        .collect();
    (owners, flush_owner)
}
impl Drop for Store {
    fn drop(&mut self) {
        for owner in &mut self.appenders {
            owner.sender.take();
        }
        for owner in &mut self.appenders {
            if let Some(h) = owner.handle.take() {
                let _ = h.join();
            }
        }
        if let Some(h) = self.flush_owner.take() {
            let _ = h.join();
        }
    }
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
        let mut persisted_bytes = 0;
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
            persisted_bytes += offset;
            drop(f);
            let file = OpenOptions::new().append(true).open(&p)?;
            logs.push(Arc::new(Mutex::new(Log {
                file,
                bytes: 0,
                flushes: 0,
                write_calls: 0,
                poisoned: false,
            })));
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
        let (appenders, flush_owner) = start_owners(&logs, &config);
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
                stripe_reserved: [0; 4],
                stripe_items: [0; 4],
                persisted_bytes,
                pending_disk: 0,
                read_only: false,
                warming: false,
            }),
            done: Condvar::new(),
            logs,
            appenders,
            flush_owner: Some(flush_owner),
            batch_gate: Mutex::new(()),
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
    pub fn state_bytes(&self) -> u64 {
        self.state.lock().unwrap().persisted_bytes
    }
    pub fn io_metrics(&self) -> (u64, u64, u64) {
        let (mut bytes, mut writes, mut flushes) = (0, 0, 0);
        for log in &self.logs {
            let log = log.lock().unwrap();
            bytes += log.bytes;
            writes += log.write_calls;
            flushes += log.flushes;
        }
        (bytes, writes, flushes)
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
        let log_index = stripe(&m.ns, &m.key);
        let disk_bytes = if m.ack == 0 {
            0
        } else {
            (16 + mutation_bytes(&m, 0, 0).len()) as u64
        };
        // Charge complete queued representation, not just value payloads. Empty
        // values still consume queue slots and framing/identifier memory.
        let queue_charge = fp.len() + m.bytes.len() + 128;
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
        if state.persisted_bytes + state.pending_disk + disk_bytes > self.config.state_limit_bytes {
            return Outcome::error(7);
        }
        if state.reserved_bytes + queue_charge > self.config.queue_bytes
            || state.stripe_reserved[log_index] + queue_charge > self.config.queue_bytes / 4
            || state.stripe_items[log_index] >= 1024
        {
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
        state.reserved_bytes += queue_charge;
        state.stripe_reserved[log_index] += queue_charge;
        state.stripe_items[log_index] += 1;
        state.pending_disk += disk_bytes;
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
        let result = if m.ack == 0 {
            Ok(())
        } else {
            let b = mutation_bytes(&m, version, at);
            let mut record = b"MBK1".to_vec();
            record.extend((b.len() as u32).to_le_bytes());
            record.extend((!(b.len() as u32)).to_le_bytes());
            record.extend(crc32(&b).to_le_bytes());
            record.extend(b);
            let (done, completion) = mpsc::channel();
            match self.appenders[log_index]
                .sender
                .as_ref()
                .unwrap()
                .send(Append {
                    record,
                    ack: m.ack,
                    done,
                }) {
                Ok(()) => completion
                    .recv()
                    .unwrap_or_else(|_| Err(io::Error::other("append owner stopped"))),
                Err(_) => Err(io::Error::other("append owner stopped")),
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
        state.reserved_bytes -= queue_charge;
        state.stripe_reserved[log_index] -= queue_charge;
        state.stripe_items[log_index] -= 1;
        state.pending_disk -= disk_bytes;
        // Count append-complete uncertain sync results too; a conservative
        // reservation after any failure avoids exceeding the disk cap.
        if m.ack != 0 {
            state.persisted_bytes += disk_bytes;
        }
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
                    // Distinct keys overlap, including their flush waits. Same-key
                    // operations retain input order; replies always retain input order.
                    let mut keys: HashMap<String, Vec<(usize, Mutation)>> = HashMap::new();
                    for (i, m) in items.into_iter().enumerate() {
                        keys.entry(m.key.clone()).or_default().push((i, m));
                    }
                    // One executing batch per store, at most 32 scoped helpers.
                    // Refuse before any item admission if another batch owns it.
                    let _batch = self.batch_gate.try_lock().map_err(|_| 6u16)?;
                    let work = Mutex::new(keys.into_values());
                    let mut results = std::thread::scope(|scope| {
                        let handles: Vec<_> = (0..n.min(32))
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
