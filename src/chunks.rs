//! Chunk files, their catalogue, retention classes and policies (CHUNK-RETIREMENT.md sections 5.1-5.5).
//!
//! Records keep the MBK1 frame. Each stripe has one open chunk per class; a chunk closes by size, by age and
//! by the idle tick, and a closed chunk retires as a whole file when its class allows. The catalogue is an
//! append-only log of `open`, `close`, `retiring` and `retired` records, each flushed before it takes effect.
//! A split retirement waits for every part.
use crate::{crc32, lp, Cursor, MAX_FRAME, MAX_ID};
use std::borrow::Cow;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

/// Policies are records in this gated namespace; client puts and removes there are refused.
pub const POLICY_NS: &str = "__membus_policy_v1";
pub const MAX_CLASSES: usize = 64;
pub const LEGACY_CHUNK: u64 = 0;

pub type Key = (String, String);

/// A retention class: its kind and retention. Each distinct class has its own folder and open chunks.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, PartialOrd, Ord)]
pub struct Class {
    pub kind: u8,
    pub retention_ms: u64,
}
impl Class {
    pub const FOREVER: Class = Class { kind: 0, retention_ms: 0 };
    pub const LATEST: Class = Class { kind: 1, retention_ms: 0 };
    pub fn expire(retention_ms: u64) -> Class {
        Class { kind: 2, retention_ms }
    }
    pub fn from_wire(kind: u8, retention_ms: u64) -> Result<Class, u16> {
        match kind {
            0 => Ok(Class::FOREVER),
            1 => Ok(Class::LATEST),
            2 if retention_ms > 0 => Ok(Class::expire(retention_ms)),
            _ => Err(3),
        }
    }
    pub fn dir(&self) -> String {
        match self.kind {
            0 => "forever".into(),
            1 => "latest".into(),
            _ => format!("expire-{}ms", self.retention_ms),
        }
    }
    pub fn label(&self) -> String {
        self.label_text().into_owned()
    }
    // Policy selection can borrow common names; public callers still own label().
    pub(crate) fn label_text(&self) -> Cow<'static, str> {
        match self.kind {
            0 => Cow::Borrowed("forever"),
            1 => Cow::Borrowed("latest"),
            _ => Cow::Owned(format!("expire({}ms)", self.retention_ms)),
        }
    }
    /// Finite classes rotate by age as well as size; forever rotates by size only.
    pub fn finite(&self) -> bool {
        self.kind != 0
    }
    fn encode(&self) -> String {
        match self.kind {
            0 => "forever".into(),
            1 => "latest".into(),
            _ => format!("expire:{}", self.retention_ms),
        }
    }
    fn decode(text: &str) -> Option<Class> {
        match text {
            "forever" => Some(Class::FOREVER),
            "latest" => Some(Class::LATEST),
            _ => text
                .strip_prefix("expire:")
                .and_then(|n| n.parse().ok())
                .filter(|n: &u64| *n > 0)
                .map(Class::expire),
        }
    }
}

/// A clock a test can move by hand. UTC and monotonic time advance together unless a test jumps UTC alone.
pub struct ManualClock {
    utc: AtomicU64,
    mono: AtomicU64,
}
impl ManualClock {
    pub fn new(utc_ms: u64) -> Arc<Self> {
        Arc::new(Self { utc: AtomicU64::new(utc_ms), mono: AtomicU64::new(0) })
    }
    fn update(field: &AtomicU64, f: impl Fn(u64) -> u64) {
        let mut current = field.load(Ordering::SeqCst);
        loop {
            match field.compare_exchange_weak(current, f(current), Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }
    /// Advance both counters, saturating each at the representable limit.
    pub fn advance(&self, ms: u64) {
        Self::update(&self.utc, |now| now.saturating_add(ms));
        Self::update(&self.mono, |now| now.saturating_add(ms));
    }
    /// A wall-clock correction with no elapsed time.
    pub fn jump_utc(&self, delta_ms: i64) {
        Self::update(&self.utc, |now| now.saturating_add_signed(delta_ms));
    }
    pub fn utc_ms(&self) -> u64 {
        self.utc.load(Ordering::SeqCst)
    }
}
#[derive(Clone)]
pub struct Clock {
    manual: Option<Arc<ManualClock>>,
    start: Instant,
}
impl Clock {
    pub fn new(manual: Option<Arc<ManualClock>>) -> Self {
        Self { manual, start: Instant::now() }
    }
    pub fn utc_ms(&self) -> u64 {
        match &self.manual {
            Some(m) => m.utc.load(Ordering::SeqCst),
            None => crate::now_ms(),
        }
    }
    pub fn mono_ms(&self) -> u64 {
        match &self.manual {
            Some(m) => m.mono.load(Ordering::SeqCst),
            None => self.start.elapsed().as_millis() as u64,
        }
    }
    /// High-resolution elapsed time for capture diagnostics, in the same clock domain.
    pub(crate) fn mono_ns(&self) -> u64 {
        match &self.manual {
            Some(m) => m.mono.load(Ordering::SeqCst).saturating_mul(1_000_000),
            None => self.start.elapsed().as_nanos().min(u64::MAX as u128) as u64,
        }
    }
}

/// Policies by namespace: (prefix, class) pairs; the longest matching prefix wins; no match is forever.
#[derive(Default, Clone)]
pub struct Policies {
    by_ns: HashMap<String, BTreeMap<String, Class>>,
}
impl Policies {
    pub fn resolve(&self, ns: &str, key: &str) -> (Class, String) {
        if ns == crate::timers::NS {
            return (Class::LATEST, "(built in: membus timers)".into());
        }
        if ns == POLICY_NS {
            return (Class::FOREVER, "(built in: policies)".into());
        }
        if let Some(prefixes) = self.by_ns.get(ns) {
            // Longest prefix: walk candidates from the longest down.
            let mut best: Option<(&String, &Class)> = None;
            for (prefix, class) in prefixes {
                if key.starts_with(prefix.as_str()) && best.is_none_or(|(b, _)| prefix.len() > b.len()) {
                    best = Some((prefix, class));
                }
            }
            if let Some((prefix, class)) = best {
                return (*class, format!("{ns}/{prefix}"));
            }
        }
        (Class::FOREVER, "(default)".into())
    }
    pub fn classes(&self) -> BTreeSet<Class> {
        let mut out: BTreeSet<Class> = [Class::FOREVER, Class::LATEST].into_iter().collect();
        for prefixes in self.by_ns.values() {
            out.extend(prefixes.values().copied());
        }
        out
    }
    pub fn set(&mut self, ns: &str, prefix: &str, class: Option<Class>) {
        match class {
            Some(c) => {
                self.by_ns.entry(ns.to_owned()).or_default().insert(prefix.to_owned(), c);
            }
            None => {
                if let Some(p) = self.by_ns.get_mut(ns) {
                    p.remove(prefix);
                }
            }
        }
    }
    pub fn would_exceed(&self, ns: &str, prefix: &str, class: Class) -> bool {
        let mut next = self.clone();
        next.set(ns, prefix, Some(class));
        next.classes().len() > MAX_CLASSES
    }
    pub fn rows(&self) -> Vec<(String, String, Class)> {
        let mut out = Vec::new();
        for (ns, prefixes) in &self.by_ns {
            for (prefix, class) in prefixes {
                out.push((ns.clone(), prefix.clone(), *class));
            }
        }
        out.sort();
        out
    }
}
/// Policy record key and value in POLICY_NS.
pub fn policy_key(ns: &str, prefix: &str) -> Result<String, u16> {
    if ns.contains('\0') || ns.is_empty() || ns == POLICY_NS || ns == crate::timers::NS {
        return Err(3);
    }
    let key = format!("{ns}\0{prefix}");
    if key.len() > MAX_ID {
        return Err(4);
    }
    Ok(key)
}
pub fn policy_value(class: Class) -> Vec<u8> {
    class.encode().into_bytes()
}
pub fn parse_policy(key: &str, value: &[u8]) -> Option<(String, String, Class)> {
    let (ns, prefix) = key.split_once('\0')?;
    let class = Class::decode(std::str::from_utf8(value).ok()?)?;
    Some((ns.to_owned(), prefix.to_owned(), class))
}

// ---- catalogue ----

#[derive(Clone, Debug, PartialEq)]
pub enum Rec {
    Open { seq: u64, stripe: u8, class: Class, policy: String, next: u64 },
    Close { seq: u64, bytes: u64, next: u64 },
    Retiring { seq: u64, next: u64, horizons: Vec<(String, String, u64)>, expired: Vec<Key> },
    /// Indexed parts of one retirement; only a complete consecutive set takes effect.
    RetiringPart { seq: u64, next: u64, index: u32, count: u32, horizons: Vec<(String, String, u64)>, expired: Vec<Key> },
    Retired { seq: u64 },
    /// Carried by a rewrite: horizons still needed and expiry summaries still kept.
    Horizons(Vec<(String, String, u64)>),
    Summary { retired_ms: u64, keys: Vec<Key> },
    Next(u64),
    /// Historical resume with no retained initiating basis.
    Resume,
    ResumeWithBasis(ResumeBasis),
    Import(Import),
}
/// Initiating evidence, not authentication of the operator or a portable monotonic epoch.
#[derive(Clone, Debug, PartialEq)]
pub enum ResumeBasis {
    Operator,
    StableClock { since_utc_ms: u64, since_mono_ms: u64, observed_mono_ms: u64,
        required_ms: u64, tolerance_ms: u64 },
}
#[derive(Clone, Debug, PartialEq, Default)]
pub struct Import {
    /// Per retained import input: (file name, size, sha256 hex). Older imports
    /// list only stripe logs; new imports also name an optional policy file.
    pub files: Vec<(String, u64, String)>,
    pub keys: u64,
    pub bytes: u64,
    pub epoch: u64,
    pub legacy_dir: String,
}
#[derive(Clone, Debug, PartialEq)]
pub struct CatRecord {
    pub at_ms: u64,
    pub rec: Rec,
}
fn put_key(b: &mut Vec<u8>, k: &Key) {
    lp(b, k.0.as_bytes());
    lp(b, k.1.as_bytes());
}
fn take_text(c: &mut Cursor<'_>) -> Result<String, u16> {
    let b = c.bytes(MAX_FRAME)?;
    Ok(std::str::from_utf8(b).map_err(|_| 3u16)?.to_owned())
}
fn take_key(c: &mut Cursor<'_>) -> Result<Key, u16> {
    Ok((take_text(c)?, take_text(c)?))
}
pub fn encode(r: &CatRecord) -> Vec<u8> {
    let mut b = Vec::new();
    let kind = match &r.rec {
        Rec::Open { .. } => 1u8,
        Rec::Close { .. } => 2,
        Rec::Retiring { .. } => 3,
        Rec::Retired { .. } => 4,
        Rec::Horizons(_) => 5,
        Rec::Summary { .. } => 6,
        Rec::Next(_) => 7,
        Rec::Resume => 8,
        Rec::Import(_) => 9,
        Rec::RetiringPart { .. } => 10,
        Rec::ResumeWithBasis(_) => 11,
    };
    b.push(kind);
    b.extend(r.at_ms.to_le_bytes());
    match &r.rec {
        Rec::Open { seq, stripe, class, policy, next } => {
            b.extend(seq.to_le_bytes());
            b.push(*stripe);
            b.push(class.kind);
            b.extend(class.retention_ms.to_le_bytes());
            lp(&mut b, policy.as_bytes());
            b.extend(next.to_le_bytes());
        }
        Rec::Close { seq, bytes, next } => {
            b.extend(seq.to_le_bytes());
            b.extend(bytes.to_le_bytes());
            b.extend(next.to_le_bytes());
        }
        Rec::Retiring { seq, next, horizons, expired }
        | Rec::RetiringPart { seq, next, horizons, expired, .. } => {
            b.extend(seq.to_le_bytes());
            b.extend(next.to_le_bytes());
            if let Rec::RetiringPart { index, count, .. } = &r.rec {
                b.extend(index.to_le_bytes());
                b.extend(count.to_le_bytes());
            }
            b.extend((horizons.len() as u32).to_le_bytes());
            for (ns, key, v) in horizons {
                put_key(&mut b, &(ns.clone(), key.clone()));
                b.extend(v.to_le_bytes());
            }
            b.extend((expired.len() as u32).to_le_bytes());
            for k in expired {
                put_key(&mut b, k);
            }
        }
        Rec::Retired { seq } => b.extend(seq.to_le_bytes()),
        Rec::Horizons(list) => {
            b.extend((list.len() as u32).to_le_bytes());
            for (ns, key, v) in list {
                put_key(&mut b, &(ns.clone(), key.clone()));
                b.extend(v.to_le_bytes());
            }
        }
        Rec::Summary { retired_ms, keys } => {
            b.extend(retired_ms.to_le_bytes());
            b.extend((keys.len() as u32).to_le_bytes());
            for k in keys {
                put_key(&mut b, k);
            }
        }
        Rec::Next(n) => b.extend(n.to_le_bytes()),
        Rec::Resume => {}
        Rec::ResumeWithBasis(basis) => match basis {
            ResumeBasis::Operator => b.push(1),
            ResumeBasis::StableClock { since_utc_ms, since_mono_ms, observed_mono_ms, required_ms, tolerance_ms } => {
                b.push(2);
                for n in [since_utc_ms, since_mono_ms, observed_mono_ms, required_ms, tolerance_ms] {
                    b.extend(n.to_le_bytes());
                }
            }
        },
        Rec::Import(i) => {
            b.extend((i.files.len() as u32).to_le_bytes());
            for (name, size, sha) in &i.files {
                lp(&mut b, name.as_bytes());
                b.extend(size.to_le_bytes());
                lp(&mut b, sha.as_bytes());
            }
            b.extend(i.keys.to_le_bytes());
            b.extend(i.bytes.to_le_bytes());
            b.extend(i.epoch.to_le_bytes());
            lp(&mut b, i.legacy_dir.as_bytes());
        }
    }
    frame(b"MBC1", &b)
}
fn encode_bounded(r: &CatRecord) -> io::Result<Vec<u8>> {
    let bytes = encode(r);
    if bytes.len().saturating_sub(16) > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "catalogue record exceeds replay frame bound"));
    }
    Ok(bytes)
}
pub fn frame(magic: &[u8; 4], b: &[u8]) -> Vec<u8> {
    let mut out = magic.to_vec();
    out.extend((b.len() as u32).to_le_bytes());
    out.extend((!(b.len() as u32)).to_le_bytes());
    out.extend(crc32(b).to_le_bytes());
    out.extend(b);
    out
}
fn decode_payload(b: &[u8]) -> Result<CatRecord, u16> {
    let mut c = Cursor::new(b);
    let kind = c.u8()?;
    let at_ms = c.u64()?;
    let rec = match kind {
        1 => {
            let seq = c.u64()?;
            let stripe = c.u8()?;
            let class = Class::from_wire(c.u8()?, c.u64()?)?;
            let policy = take_text(&mut c)?;
            let next = c.u64()?;
            Rec::Open { seq, stripe, class, policy, next }
        }
        2 => Rec::Close { seq: c.u64()?, bytes: c.u64()?, next: c.u64()? },
        3 | 10 => {
            let seq = c.u64()?;
            let next = c.u64()?;
            let part = if kind == 10 {
                let index = c.u32()?;
                let count = c.u32()?;
                if count == 0 || index >= count { return Err(3); }
                Some((index, count))
            } else { None };
            let n = c.u32()? as usize;
            let mut horizons = Vec::new();
            for _ in 0..n {
                let (ns, key) = take_key(&mut c)?;
                horizons.push((ns, key, c.u64()?));
            }
            let m = c.u32()? as usize;
            let mut expired = Vec::new();
            for _ in 0..m {
                expired.push(take_key(&mut c)?);
            }
            match part {
                Some((index, count)) => Rec::RetiringPart { seq, next, index, count, horizons, expired },
                None => Rec::Retiring { seq, next, horizons, expired },
            }
        }
        4 => Rec::Retired { seq: c.u64()? },
        5 => {
            let n = c.u32()? as usize;
            let mut list = Vec::new();
            for _ in 0..n {
                let (ns, key) = take_key(&mut c)?;
                list.push((ns, key, c.u64()?));
            }
            Rec::Horizons(list)
        }
        6 => {
            let retired_ms = c.u64()?;
            let n = c.u32()? as usize;
            let mut keys = Vec::new();
            for _ in 0..n {
                keys.push(take_key(&mut c)?);
            }
            Rec::Summary { retired_ms, keys }
        }
        7 => Rec::Next(c.u64()?),
        8 => Rec::Resume,
        11 => Rec::ResumeWithBasis(match c.u8()? {
            1 => ResumeBasis::Operator,
            2 => ResumeBasis::StableClock { since_utc_ms: c.u64()?, since_mono_ms: c.u64()?,
                observed_mono_ms: c.u64()?, required_ms: c.u64()?, tolerance_ms: c.u64()? },
            _ => return Err(3),
        }),
        9 => {
            let n = c.u32()? as usize;
            let mut files = Vec::new();
            for _ in 0..n {
                let name = take_text(&mut c)?;
                let size = c.u64()?;
                let sha = take_text(&mut c)?;
                files.push((name, size, sha));
            }
            Rec::Import(Import {
                files,
                keys: c.u64()?,
                bytes: c.u64()?,
                epoch: c.u64()?,
                legacy_dir: take_text(&mut c)?,
            })
        }
        _ => return Err(3),
    };
    c.end()?;
    Ok(CatRecord { at_ms, rec })
}

/// Encode a retirement using the legacy form when it fits, otherwise indexed bounded parts.
/// Entry sizes include UTF-8 byte lengths and all length prefixes, not just key counts.
fn retiring_records(
    at_ms: u64,
    seq: u64,
    next: u64,
    horizons: &[(String, String, u64)],
    expired: &[Key],
) -> Vec<CatRecord> {
    let horizon_size = |(ns, key, _): &(String, String, u64)| 16 + ns.len() + key.len();
    let key_size = |(ns, key): &Key| 8 + ns.len() + key.len();
    let size = 33
        + horizons.iter().map(horizon_size).sum::<usize>()
        + expired.iter().map(key_size).sum::<usize>();
    if size <= MAX_FRAME {
        return vec![CatRecord {
            at_ms,
            rec: Rec::Retiring {
                seq,
                next,
                horizons: horizons.to_vec(),
                expired: expired.to_vec(),
            },
        }];
    }
    // Valid store keys are bounded by MAX_ID, so each entry fits on its own.
    let mut lists = Vec::new();
    let mut h = Vec::new();
    let mut e = Vec::new();
    let mut bytes = 41;
    for item in horizons {
        let size = horizon_size(item);
        if bytes + size > MAX_FRAME && !h.is_empty() {
            lists.push((std::mem::take(&mut h), std::mem::take(&mut e)));
            bytes = 41;
        }
        h.push(item.clone());
        bytes += size;
    }
    for item in expired {
        let size = key_size(item);
        if bytes + size > MAX_FRAME && (!h.is_empty() || !e.is_empty()) {
            lists.push((std::mem::take(&mut h), std::mem::take(&mut e)));
            bytes = 41;
        }
        e.push(item.clone());
        bytes += size;
    }
    lists.push((h, e));
    let count = u32::try_from(lists.len()).expect("retirement part count exceeds u32");
    lists
        .into_iter()
        .enumerate()
        .map(|(index, (horizons, expired))| CatRecord {
            at_ms,
            rec: Rec::RetiringPart {
                seq,
                next,
                index: index as u32,
                count,
                horizons,
                expired,
            },
        })
        .collect()
}

/// Fold complete consecutive retirement sets into the legacy logical transition.
/// Abandoned prefixes have no effect, including on the version and clock high-water marks.
/// A new index zero always starts a new attempt, even if its other fields are identical.
pub fn committed_records(records: Vec<CatRecord>) -> (Vec<CatRecord>, bool) {
    let mut out = Vec::new();
    let mut pending: Option<CatRecord> = None;
    let mut incomplete = false;
    for record in records {
        if let Rec::RetiringPart {
            seq,
            next,
            index,
            count,
            horizons,
            expired,
        } = record.rec
        {
            if index == 0 && count > 0 {
                incomplete |= pending.take().is_some();
                pending = Some(CatRecord {
                    at_ms: record.at_ms,
                    rec: Rec::RetiringPart {
                        seq,
                        next,
                        index,
                        count,
                        horizons,
                        expired,
                    },
                });
            } else if let Some(CatRecord {
                at_ms,
                rec:
                    Rec::RetiringPart {
                        seq: p_seq,
                        next: p_next,
                        index: p_index,
                        count: p_count,
                        horizons: h,
                        expired: e,
                    },
            }) = pending.as_mut()
            {
                if *at_ms == record.at_ms
                    && *p_seq == seq
                    && *p_next == next
                    && *p_count == count
                    && p_index.checked_add(1) == Some(index)
                    && index < count
                {
                    *p_index = index;
                    h.extend(horizons);
                    e.extend(expired);
                } else {
                    pending = None;
                    incomplete = true;
                }
            } else {
                incomplete = true;
            }
            let complete = pending.as_ref().is_some_and(|r| {
                matches!(&r.rec,
                Rec::RetiringPart { index, count, .. } if index.checked_add(1) == Some(*count))
            });
            if complete {
                let r = pending.take().unwrap();
                if let Rec::RetiringPart {
                    seq,
                    next,
                    horizons,
                    expired,
                    ..
                } = r.rec
                {
                    out.push(CatRecord {
                        at_ms: r.at_ms,
                        rec: Rec::Retiring {
                            seq,
                            next,
                            horizons,
                            expired,
                        },
                    });
                }
            }
        } else {
            incomplete |= pending.take().is_some();
            out.push(record);
        }
    }
    incomplete |= pending.is_some();
    (out, incomplete)
}

/// One frame read from a log: Ok(Some(payload, frame_len)), Ok(None) for a torn tail (short header or short
/// body), Err for interior corruption (magic, length complement, length bound or CRC of a complete frame).
pub enum Frame {
    Whole(Vec<u8>, u64),
    Torn(&'static str),
}
pub fn read_frame_at(f: &mut File, magic: &[u8; 4], offset: u64, len: u64, name: &str) -> io::Result<Frame> {
    capture_frame_at(f, magic, offset, len, name)?.validate(offset, name)
}

// Captured bytes remain opaque until their checksum is validated. Separating
// cursor work from buffer work lets independent offsets share a read handle
// without holding its cursor mutex across the checksum calculation.
pub(crate) struct CapturedFrame(CapturedBody);
enum CapturedBody {
    Whole(Vec<u8>, u32),
    Torn(&'static str),
}
impl CapturedFrame {
    pub(crate) fn validate(self, offset: u64, name: &str) -> io::Result<Frame> {
        match self.0 {
            CapturedBody::Torn(reason) => Ok(Frame::Torn(reason)),
            CapturedBody::Whole(bytes, expected) => {
                #[cfg(test)]
                cold_validation_tests::before_validation();
                if crc32(&bytes) != expected {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("interior-corrupt {name} offset {offset}: CRC"),
                    ));
                }
                let frame_len = 16 + bytes.len() as u64;
                Ok(Frame::Whole(bytes, frame_len))
            }
        }
    }
}
pub(crate) fn capture_frame_at(f: &mut File, magic: &[u8; 4], offset: u64, len: u64, name: &str) -> io::Result<CapturedFrame> {
    if len - offset < 16 {
        return Ok(CapturedFrame(CapturedBody::Torn("short header")));
    }
    let mut head = [0; 16];
    f.read_exact(&mut head)?;
    if &head[0..4] != magic {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("interior-corrupt {name} offset {offset}: magic"),
        ));
    }
    let n = u32::from_le_bytes(head[4..8].try_into().unwrap()) as usize;
    if u32::from_le_bytes(head[8..12].try_into().unwrap()) != !(n as u32) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("interior-corrupt {name} offset {offset}: length complement"),
        ));
    }
    if n > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("interior-corrupt {name} offset {offset}: invalid chunk length"),
        ));
    }
    if len - offset - 16 < n as u64 {
        return Ok(CapturedFrame(CapturedBody::Torn("short body")));
    }
    let mut b = vec![0; n];
    f.read_exact(&mut b)?;
    Ok(CapturedFrame(CapturedBody::Whole(
        b, u32::from_le_bytes(head[12..16].try_into().unwrap()),
    )))
}

#[cfg(test)]
mod cold_validation_tests;

/// The catalogue as read at start: its records, and a torn tail's offset when one was found.
pub type CatalogueRead = (Vec<CatRecord>, Option<(u64, u64, &'static str)>);
pub fn read_catalogue(path: &Path) -> io::Result<CatalogueRead> {
    require_resolved_append(path)?;
    let mut out = Vec::new();
    if !path.exists() {
        return Ok((out, None));
    }
    let mut f = File::open(path)?;
    let len = f.metadata()?.len();
    let mut offset = 0;
    let name = path.display().to_string();
    while offset < len {
        match read_frame_at(&mut f, b"MBC1", offset, len, &name)? {
            Frame::Torn(reason) => return Ok((out, Some((offset, len - offset, reason)))),
            Frame::Whole(b, n) => {
                let r = decode_payload(&b).map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("interior-corrupt {name} offset {offset}: catalogue payload"),
                    )
                })?;
                out.push(r);
                offset += n;
            }
        }
    }
    Ok((out, None))
}

fn append_pending_path(path: &Path) -> PathBuf {
    path.with_extension("log.append-pending")
}

// A complete frame is not evidence that an append returned successfully. Persist the unresolved
// boundary before writing, and retain it when neither publication nor rollback can be established.
// Replay refuses even a malformed marker; it never guesses that its absence of usable detail means
// there was no outstanding append. Recovery by a known live-list rewrite is separately explicit.
fn require_resolved_append(path: &Path) -> io::Result<()> {
    let pending = append_pending_path(path);
    if pending.try_exists()? {
        return Err(io::Error::other(format!(
            "catalogue append unresolved: {}; replay refused, catalogue and chunks retained",
            pending.display()
        )));
    }
    Ok(())
}

fn begin_append(path: &Path, boundary: u64) -> io::Result<()> {
    let mut marker = OpenOptions::new().write(true).create_new(true).open(append_pending_path(path))?;
    let mut bytes = b"MBCP1\0\0\0".to_vec();
    bytes.extend(boundary.to_le_bytes());
    bytes.extend(crc32(&bytes).to_le_bytes());
    marker.write_all(&bytes)?;
    crate::sync_file(&marker)
}

/// One cooperative writer per catalogue directory, retained across log replacement.
pub struct Catalogue {
    path: PathBuf,
    _owner: File,
    file: File,
    pub bytes: u64,
    pub last_ms: u64,
    /// Bytes in the last successful compact image; only subsequent growth triggers compaction.
    pub rewrite_base_bytes: u64,
    #[cfg(test)]
    pub(crate) rewrites: u64,
    pub fail_writes: bool,
    fenced: bool,
    recovery_evidence: Option<PathBuf>,
    #[cfg(test)]
    pub(crate) fail_after_once: Option<usize>,
    #[cfg(test)]
    fail_partial_once: Option<usize>,
    #[cfg(test)]
    pub(crate) fail_sync_once: bool,
    #[cfg(test)]
    fail_reopen_once: bool,
    #[cfg(test)]
    pub(crate) fail_recovery_once: bool,
    #[cfg(test)]
    directory_sync: Option<Arc<dyn Fn(&Path) -> io::Result<()> + Send + Sync>>,
}
impl Catalogue {
    pub fn open(path: &Path) -> io::Result<Self> {
        let owner = Self::acquire_owner(path)?;
        Self::open_owned(path, owner)
    }
    /// The sibling lock stays at a stable name when legacy import replaces the directory.
    pub(crate) fn acquire_owner(path: &Path) -> io::Result<File> {
        let parent = path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."))
            .canonicalize()?;
        let mut name = parent.file_name().ok_or_else(|| io::Error::other("catalogue directory name required"))?.to_os_string();
        name.push(".catalogue-owner");
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true).truncate(false);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            options.share_mode(0);
        }
        let owner = options.open(parent.with_file_name(name))
            .map_err(|e| io::Error::other(format!("catalogue writer ownership refused: {e}")))?;
        #[cfg(not(windows))]
        owner.try_lock().map_err(|e| io::Error::other(format!("catalogue writer ownership refused: {e}")))?;
        Ok(owner)
    }
    pub(crate) fn open_owned(path: &Path, owner: File) -> io::Result<Self> {
        let parent = path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
        let path = parent.canonicalize()?.join(path.file_name().ok_or_else(|| io::Error::other("catalogue file name required"))?);
        let path = path.as_path();
        require_resolved_append(path)?;
        let file = Self::writer_file(path, true)?;
        let bytes = file.metadata()?.len();
        Ok(Self {
            path: path.to_owned(), _owner: owner, file, bytes, last_ms: 0, rewrite_base_bytes: 0, fail_writes: false, fenced: false,
            recovery_evidence: None,
            #[cfg(test)]
            rewrites: 0,
            #[cfg(test)]
            fail_after_once: None,
            #[cfg(test)]
            fail_partial_once: None,
            #[cfg(test)]
            fail_sync_once: false,
            #[cfg(test)]
            fail_reopen_once: false,
            #[cfg(test)]
            fail_recovery_once: false,
            #[cfg(test)]
            directory_sync: None,
        })
    }
    pub(crate) fn writer_file(path: &Path, create: bool) -> io::Result<File> {
        let mut options = OpenOptions::new();
        options.read(true).write(true).create(create).truncate(false);
        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            // Permit snapshots and our atomic rename; deny a second writable alias, including hardlinks.
            options.share_mode(1 | 4);
        }
        let file = options.open(path)?;
        #[cfg(not(windows))]
        file.try_lock().map_err(|e| io::Error::other(format!("catalogue file ownership refused: {e}")))?;
        Ok(file)
    }
    pub fn path(&self) -> &Path { &self.path }
    pub fn recovery_required(&self) -> io::Result<bool> {
        Ok(self.fenced || append_pending_path(&self.path).try_exists()?)
    }
    pub fn recovery_evidence(&self) -> Option<&Path> { self.recovery_evidence.as_deref() }
    fn preserve_uncertain(&self) -> io::Result<PathBuf> {
        for n in 0..32 {
            let dir = self.path.parent().unwrap().join(format!("catalogue-recovery-{}-{n}", crate::now_ms()));
            match fs::create_dir(&dir) {
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e),
                Ok(()) => {}
            }
            for (source, name) in [(self.path.clone(), "catalogue.log"), (append_pending_path(&self.path), "append-pending")] {
                if source.try_exists()? {
                    let mut input = File::open(source)?;
                    let mut output = OpenOptions::new().write(true).create_new(true).open(dir.join(name))?;
                    io::copy(&mut input, &mut output)?;
                    crate::sync_file(&output)?;
                }
            }
            crate::log_io::sync_dir(&dir)?;
            crate::log_io::sync_dir(self.path.parent().unwrap())?;
            return Ok(dir);
        }
        Err(io::Error::other("cannot reserve catalogue recovery evidence directory"))
    }
    pub(crate) fn truncate(&mut self, bytes: u64) -> io::Result<()> {
        self.file.set_len(bytes)?;
        self.bytes = bytes;
        Ok(())
    }
    fn sync_parent(&self) -> io::Result<()> {
        let parent = self.path.parent().filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        #[cfg(test)]
        if let Some(hook) = &self.directory_sync { hook(parent)?; }
        crate::log_io::sync_dir(parent)
    }
    fn resolve_append(&self) -> io::Result<()> {
        let pending = append_pending_path(&self.path);
        // Retain the exact recovery evidence before unlinking it. A directory
        // error must not leave a complete, unaccepted frame looking resolved.
        let marker_bytes = fs::read(&pending)?;
        fs::remove_file(&pending)?;
        if let Err(error) = self.sync_parent() {
            let restored = (|| -> io::Result<()> {
                let mut marker = OpenOptions::new().write(true).create_new(true).open(&pending)?;
                marker.write_all(&marker_bytes)?;
                crate::sync_file(&marker)?;
                self.sync_parent()
            })();
            return match restored {
                Ok(()) => Err(error),
                Err(restoration) => Err(io::Error::other(format!(
                    "{error}; catalogue marker restoration failed: {restoration}"
                ))),
            };
        }
        Ok(())
    }
    /// Append and flush; the record takes effect only after this returns Ok.
    pub fn append(&mut self, r: &CatRecord) -> io::Result<()> {
        if self.fenced {
            return Err(io::Error::other("catalogue append fenced after boundary recovery failure"));
        }
        #[cfg(test)]
        if let Some(left) = self.fail_after_once.as_mut() {
            if *left == 0 {
                self.fail_after_once = None;
                return Err(io::Error::other("test injected catalogue append failure"));
            }
            *left -= 1;
        }
        if self.fail_writes {
            return Err(io::Error::other("test injected catalogue write failure"));
        }
        let b = encode_bounded(r)?;
        let boundary = self.file.metadata()?.len();
        if let Err(error) = begin_append(&self.path, boundary).and_then(|_| self.sync_parent()) {
            // Marker creation/file/directory sync can fail after creating bytes. No append was attempted;
            // retain all evidence and require an explicit rewrite rather than overwriting the marker.
            self.fenced = true;
            return Err(error);
        }
        let written = (|| -> io::Result<()> {
            self.file.seek(SeekFrom::End(0))?;
            #[cfg(test)]
            if let Some(n) = self.fail_partial_once.take() {
                self.file.write_all(&b[..n.min(b.len())])?;
                return Err(io::Error::other("test injected partial catalogue append"));
            }
            self.file.write_all(&b)?;
            #[cfg(test)]
            if std::mem::take(&mut self.fail_sync_once) {
                return Err(io::Error::other("test injected catalogue sync failure"));
            }
            crate::sync_file(&self.file)
        })();
        if let Err(error) = written {
            // No caller applied this record. Restore and flush its old boundary before another append.
            let recovered = (|| -> io::Result<()> {
                #[cfg(test)]
                if std::mem::take(&mut self.fail_recovery_once) {
                    return Err(io::Error::other("test injected boundary recovery failure"));
                }
                self.file.set_len(boundary)?;
                crate::sync_file(&self.file)?;
                self.resolve_append()
            })();
            if let Err(recovery) = recovered {
                self.fenced = true;
                return Err(io::Error::other(format!("{error}; catalogue boundary recovery failed: {recovery}")));
            }
            return Err(error);
        }
        if let Err(error) = self.resolve_append() {
            self.fenced = true;
            return Err(io::Error::other(format!("catalogue publication unresolved: {error}")));
        }
        self.bytes += b.len() as u64;
        self.last_ms = self.last_ms.max(r.at_ms);
        Ok(())
    }
    /// Rewrite atomically to the live list (tmp, flush, rename). Metadata only; no record is copied.
    pub fn rewrite(&mut self, live: &[CatRecord]) -> io::Result<()> {
        let evidence = if self.recovery_required()? { Some(self.preserve_uncertain()?) } else { None };
        let tmp = self.path.with_extension("log.tmp");
        {
            let mut f = File::create(&tmp)?;
            let mut all = Vec::new();
            for r in live {
                all.extend(encode_bounded(r)?);
            }
            f.write_all(&all)?;
            crate::sync_file(&f)?;
        }
        #[cfg(test)]
        if std::mem::take(&mut self.fail_reopen_once) {
            return Err(io::Error::other("test injected replacement open failure"));
        }
        // Prepare every fallible handle operation before publishing the replacement. On error the
        // current handle still names the current path, so a later append cannot go to an unlinked log.
        let replacement = Self::writer_file(&tmp, false)?;
        let bytes = replacement.metadata()?.len();
        fs::rename(&tmp, &self.path)?;
        self.file = replacement;
        self.bytes = bytes;
        if evidence.is_some() { self.recovery_evidence = evidence; }
        // A post-rename failure must keep the handle on the published image,
        // fence appends and retain any unresolved marker until a rewrite retries.
        self.fenced = true;
        self.sync_parent()?;
        // A retained marker is cleared only after the caller's known live list has replaced the
        // uncertain catalogue. Its removal barrier must complete before clearing the live fence.
        let pending = append_pending_path(&self.path);
        if pending.try_exists()? {
            self.resolve_append()?;
        }
        self.fenced = false;
        self.rewrite_base_bytes = self.bytes;
        #[cfg(test)]
        { self.rewrites += 1; }
        Ok(())
    }
    /// Repair a fence regardless of the size trigger; zero disables ordinary compaction only.
    pub fn needs_rewrite(&self, threshold: u64) -> bool {
        self.fenced || (threshold > 0 && self.bytes.saturating_sub(self.rewrite_base_bytes) > threshold)
    }
}

// ---- chunk table ----

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ChunkState {
    Open,
    Closed,
    Retiring,
    Retired,
}
#[derive(Clone, Debug)]
pub struct ChunkInfo {
    pub id: u64,
    pub stripe: usize,
    pub class: Class,
    pub policy: String,
    pub open_ms: u64,
    pub close_ms: Option<u64>,
    /// Monotonic close time when the chunk closed in this run.
    pub close_mono: Option<u64>,
    /// Latest inclusive local cutoff of outcomes recovered from this chunk; not persisted.
    pub recovered_retry_until_mono: Option<u64>,
    pub bytes: u64,
    pub path: PathBuf,
    pub state: ChunkState,
    /// Each key with records here: (lowest, highest) version held.
    pub keys: HashMap<Key, (u64, u64)>,
    /// Monotonic start of this process's observed delete blockage; not persisted.
    pub blocked_since: Option<u64>,
    pub horizons: Vec<(String, String, u64)>,
    pub expired: Vec<Key>,
    /// Original logical retirement time, preserved by live-list rewrites.
    pub retired_ms: Option<u64>,
}
pub fn chunk_path(dir: &Path, class: Class, stripe: usize, seq: u64) -> PathBuf {
    dir.join(class.dir()).join(format!("{stripe}-{seq}.log"))
}

/// What a start found and did, written as state/replay.json and a line of state/replay.log.
#[derive(Default, Debug, Clone)]
pub struct ReplayReport {
    /// Validated attempted epoch; zero when validation failed. Non-ready stages do not prove publication.
    pub epoch: u64,
    pub start_ms: u64,
    pub ready_ms: u64,
    /// Captured local elapsed time for startup reports. None preserves standalone UTC-only reports.
    pub elapsed_ms: Option<u64>,
    /// Empty historically; current stages are ready, refused and truncation_pending.
    /// Pending records describe a plan, not proof that truncation completed.
    pub stage: String,
    pub stripes: [StripeReport; 4],
    pub catalogue_truncation: Option<(u64, u64, String)>,
    pub retirements_finished: Vec<u64>,
    pub closed_at_start: Vec<(u64, u64)>,
    pub empty_closed: Vec<u64>,
    pub dedup_entries: usize,
    pub keys: usize,
    pub namespace_counts: BTreeMap<String, usize>,
    pub horizons: usize,
    pub storage_errors: Vec<String>,
    pub refusal: Option<String>,
    pub legacy: Vec<String>,
    pub import: Option<String>,
    pub expiry_degraded: Option<String>,
}
#[derive(Default, Debug, Clone)]
pub struct StripeReport {
    pub chunks: usize,
    pub legacy_logs: usize,
    pub records: u64,
    pub bytes: u64,
    pub truncations: Vec<(String, u64, u64, String)>,
    pub retired_skipped: usize,
    pub horizons_applied: u64,
}
fn esc(s: &str) -> String {
    let mut o = String::new();
    for ch in s.chars() {
        match ch {
            '"' => o.push_str("\\\""),
            '\\' => o.push_str("\\\\"),
            c if (c as u32) < 0x20 => o.push_str(&format!("\\u{:04x}", c as u32)),
            c => o.push(c),
        }
    }
    o
}
impl ReplayReport {
    fn duration_ms(&self) -> u64 {
        self.elapsed_ms.unwrap_or_else(|| self.ready_ms.saturating_sub(self.start_ms))
    }
    pub fn json(&self) -> String {
        let stripes: Vec<String> = self
            .stripes
            .iter()
            .enumerate()
            .map(|(i, s)| {
                let t: Vec<String> = s
                    .truncations
                    .iter()
                    .map(|(c, o, b, r)| {
                        format!("{{\"chunk\":\"{}\",\"offset\":{o},\"bytes\":{b},\"reason\":\"{}\"}}", esc(c), esc(r))
                    })
                    .collect();
                format!(
                    "{{\"stripe\":{i},\"chunks\":{},\"legacy_logs\":{},\"records\":{},\"bytes\":{},\"truncations\":[{}],\"retired_skipped\":{},\"horizons_applied\":{}}}",
                    s.chunks, s.legacy_logs, s.records, s.bytes, t.join(","), s.retired_skipped, s.horizons_applied
                )
            })
            .collect();
        let ns: Vec<String> =
            self.namespace_counts.iter().map(|(k, v)| format!("\"{}\":{v}", esc(k))).collect();
        let list = |v: &Vec<String>| v.iter().map(|s| format!("\"{}\"", esc(s))).collect::<Vec<_>>().join(",");
        let opt = |v: &Option<String>| v.as_ref().map_or("null".into(), |s| format!("\"{}\"", esc(s)));
        format!(
            "{{\"epoch\":{},\"start_ms\":{},\"ready_ms\":{},\"stage\":\"{}\",\"duration_ms\":{},\"stripes\":[{}],\"catalogue_truncation\":{},\"retirements_finished\":{:?},\"closed_at_start\":[{}],\"empty_closed\":{:?},\"dedup_entries\":{},\"keys\":{},\"namespace_counts\":{{{}}},\"horizons\":{},\"legacy\":[{}],\"import\":{},\"expiry_degraded\":{},\"storage_errors\":[{}],\"refusal\":{}}}",
            self.epoch,
            self.start_ms,
            self.ready_ms,
            esc(&self.stage),
            self.duration_ms(),
            stripes.join(","),
            self.catalogue_truncation
                .as_ref()
                .map_or("null".into(), |(o, b, r)| format!("{{\"offset\":{o},\"bytes\":{b},\"reason\":\"{}\"}}", esc(r))),
            self.retirements_finished,
            self.closed_at_start.iter().map(|(s, b)| format!("{{\"chunk\":{s},\"bytes\":{b}}}")).collect::<Vec<_>>().join(","),
            self.empty_closed,
            self.dedup_entries,
            self.keys,
            ns.join(","),
            self.horizons,
            list(&self.legacy),
            opt(&self.import),
            opt(&self.expiry_degraded),
            list(&self.storage_errors),
            opt(&self.refusal),
        )
    }
    /// Publish replay.json and append bounded replay.log history, including their directory barriers.
    pub fn write(&self, state: &Path) -> io::Result<()> {
        self.write_with_sync(state, crate::log_io::sync_dir)
    }
    pub(crate) fn write_startup(&self, state: &Path, _config: &crate::Config) -> io::Result<()> {
        #[cfg(test)]
        if let Some(hook) = &_config.replay_report_dir_sync {
            return self.write_with_sync(state, |dir| {
                hook(dir)?;
                crate::log_io::sync_dir(dir)
            });
        }
        self.write(state)
    }
    fn write_with_sync(&self, state: &Path, mut sync: impl FnMut(&Path) -> io::Result<()>) -> io::Result<()> {
        let line = self.json();
        let tmp = state.join("replay.json.tmp");
        let mut pending = File::create(&tmp)?;
        pending.write_all(format!("{line}\n").as_bytes())?;
        crate::sync_file(&pending)?;
        drop(pending);
        fs::rename(&tmp, state.join("replay.json"))?;
        sync(state)?;
        let log = state.join("replay.log");
        if fs::metadata(&log).map(|m| m.len()).unwrap_or(0) > 1024 * 1024 {
            let text = fs::read_to_string(&log)?;
            let lines: Vec<&str> = text.lines().collect();
            let keep = lines[lines.len().saturating_sub(256)..].join("\n");
            let tmp = state.join("replay.log.tmp");
            let mut retained = File::create(&tmp)?;
            retained.write_all(format!("{keep}\n").as_bytes())?;
            crate::sync_file(&retained)?;
            drop(retained);
            fs::rename(&tmp, &log)?;
            sync(state)?;
        }
        let mut f = OpenOptions::new().create(true).append(true).open(&log)?;
        f.write_all(format!("{line}\n").as_bytes())?;
        crate::sync_file(&f)?;
        // Opening the history may have created its name. Persist it before a
        // pending report permits repair or a ready report permits Store return.
        sync(state)?;
        Ok(())
    }
    pub fn summary(&self) -> String {
        let records: u64 = self.stripes.iter().map(|s| s.records).sum();
        let chunks: usize = self.stripes.iter().map(|s| s.chunks).sum();
        let truncations: usize = self.stripes.iter().map(|s| s.truncations.len()).sum();
        format!(
            "replay epoch={} records={records} chunks={chunks} legacy={} truncations={truncations} retirements_finished={} keys={} dedup={} horizons={} storage_errors={} duration_ms={}",
            self.epoch,
            self.legacy.len(),
            self.retirements_finished.len(),
            self.keys,
            self.dedup_entries,
            self.horizons,
            self.storage_errors.len(),
            self.duration_ms()
        )
    }
}

#[cfg(test)]
#[path = "replay_report_tests.rs"]
mod replay_report_tests;

/// The record fields replay and the retirement bookkeeping need: (ns, key, version) from an MBK1 payload.
pub fn record_key(payload: &[u8]) -> Option<(String, String, u64)> {
    let mut c = Cursor::new(payload);
    let _at = c.u64().ok()?;
    let version = c.u64().ok()?;
    let _ack = c.u8().ok()?;
    let _remove = c.u8().ok()?;
    let ns = c.text().ok()?;
    let key = c.text().ok()?;
    Some((ns, key, version))
}

/// Chunk files under `dir` as (class dir, stripe, seq, path).
pub fn list_chunk_files(dir: &Path) -> io::Result<Vec<(String, usize, u64, PathBuf)>> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let class_dir = entry.file_name().to_string_lossy().into_owned();
        for f in fs::read_dir(entry.path())? {
            let f = f?;
            let name = f.file_name().to_string_lossy().into_owned();
            let Some(stem) = name.strip_suffix(".log") else { continue };
            let Some((s, q)) = stem.split_once('-') else { continue };
            if let (Ok(s), Ok(q)) = (s.parse::<usize>(), q.parse::<u64>()) {
                out.push((class_dir.clone(), s, q, f.path()));
            }
        }
    }
    out.sort_by_key(|x| x.2);
    Ok(out)
}

/// Holders: for each key, the retained chunks holding any of its records.
#[derive(Default)]
pub struct Holders {
    pub map: HashMap<Key, BTreeSet<u64>>,
}
impl Holders {
    pub fn add(&mut self, k: &Key, chunk: u64) {
        self.map.entry(k.clone()).or_default().insert(chunk);
    }
    pub fn remove(&mut self, k: &Key, chunk: u64) {
        if let Some(s) = self.map.get_mut(k) {
            s.remove(&chunk);
            if s.is_empty() {
                self.map.remove(k);
            }
        }
    }
    pub fn others(&self, k: &Key, chunk: u64) -> impl Iterator<Item = u64> + '_ {
        self.map.get(k).into_iter().flat_map(move |s| s.iter().copied().filter(move |c| *c != chunk))
    }
}

/// Expiry summaries: the key sets of retired expire-class chunks, kept for 30 days.
#[derive(Default)]
pub struct Summaries {
    pub list: BTreeMap<u64, Vec<Key>>,
    pub keys: HashMap<Key, u64>,
    pub charged: usize,
}
impl Summaries {
    pub fn add(&mut self, retired_ms: u64, keys: Vec<Key>) -> usize {
        let mut added = 0;
        for k in keys {
            match self.keys.get(&k) {
                Some(previous) if *previous >= retired_ms => continue,
                None => added += 1,
                _ => {}
            }
            self.keys.insert(k.clone(), retired_ms);
            self.list.entry(retired_ms).or_default().push(k);
        }
        self.charged += added;
        added
    }
    /// Drop summaries past `keep_ms`; returns the index slots freed.
    pub fn expire(&mut self, now: u64, keep_ms: u64) -> usize {
        let mut freed = 0;
        while let Some((at, _)) = self.list.first_key_value() {
            if now.saturating_sub(*at) < keep_ms {
                break;
            }
            let (at, keys) = self.list.pop_first().unwrap();
            for k in keys {
                if self.keys.get(&k) == Some(&at) {
                    self.keys.remove(&k);
                    freed += 1;
                }
            }
        }
        self.charged -= freed;
        freed
    }
}

/// The chunk table, its catalogue and what retirement keeps: holders, horizons and expiry summaries.
pub struct ChunkStore {
    /// Conservative resident charge for retained chunk key references, separate from the latest-key index.
    pub index_bytes: usize,
    pub dir: PathBuf,
    pub cat: Option<Catalogue>,
    pub table: BTreeMap<u64, ChunkInfo>,
    pub holders: Holders,
    pub horizons: HashMap<Key, u64>,
    pub summaries: Summaries,
    pub next_id: u64,
    pub retired_chunks: u64,
    pub retired_bytes: u64,
    pub import: Option<Import>,
    /// When the import record was written; a catalogue rewrite keeps it, so the 24 h dedup window does not move.
    pub import_at: u64,
    /// Retirement pauses while the clock is in doubt (5.4); the reason, and the stable run since.
    pub degraded: Option<String>,
    pub stable_since: Option<(u64, u64)>,
    pub last_tick: Option<(u64, u64)>,
    /// Latest resume evidence retained through replay and compaction.
    pub last_resume: Option<CatRecord>,
    /// Last failed rewrite's cause, cleared by a successful rewrite.
    pub rewrite_error: Option<String>,
    /// A partial retirement prefix needs discharge by a known-live rewrite before retry.
    pub incomplete_retiring: bool,
}
impl ChunkStore {
    pub fn new(dir: PathBuf) -> Self {
        Self {
            index_bytes: 0,
            dir,
            cat: None,
            table: BTreeMap::new(),
            holders: Holders::default(),
            horizons: HashMap::new(),
            summaries: Summaries::default(),
            next_id: 1,
            retired_chunks: 0,
            retired_bytes: 0,
            import: None,
            import_at: 0,
            degraded: None,
            stable_since: None,
            last_tick: None,
            last_resume: None,
            rewrite_error: None,
            incomplete_retiring: false,
        }
    }
    pub fn register(&mut self, id: u64, k: &Key, v: u64) {
        if let Some(c) = self.table.get_mut(&id) {
            if !c.keys.contains_key(k) { self.index_bytes += 2 * crate::store::index_charge(k); }
            let e = c.keys.entry(k.clone()).or_insert((v, v));
            e.0 = e.0.min(v);
            e.1 = e.1.max(v);
        }
        self.holders.add(k, id);
    }
    pub(crate) fn recount_index_bytes(&mut self) {
        self.index_bytes = self.table.values().flat_map(|c| c.keys.keys()).map(crate::store::index_charge).sum::<usize>()
            + self.holders.map.keys().map(crate::store::index_charge).sum::<usize>()
            + self.summaries.keys.keys().map(crate::store::index_charge).sum::<usize>()
            + self.horizons.keys().map(crate::store::index_charge).sum::<usize>();
    }
    /// Append and flush one catalogue record.
    pub fn log(&mut self, at_ms: u64, rec: Rec) -> io::Result<()> {
        match self.cat.as_mut() {
            Some(c) => c.append(&CatRecord { at_ms, rec }),
            None => Err(io::Error::other("catalogue not open")),
        }
    }
    /// Publish the entire transition before the caller applies any of its effects.
    pub fn log_retiring(
        &mut self,
        at_ms: u64,
        seq: u64,
        next: u64,
        horizons: &[(String, String, u64)],
        expired: &[Key],
    ) -> io::Result<()> {
        if self.incomplete_retiring {
            self.rewrite_live(at_ms, next)?;
        }
        let records = retiring_records(at_ms, seq, next, horizons, expired);
        self.incomplete_retiring = records.len() > 1;
        for record in records {
            self.log(record.at_ms, record.rec)?;
        }
        self.incomplete_retiring = false;
        Ok(())
    }
    /// The live list omits unapplied prefixes; a failed rewrite preserves their retry fence.
    pub fn rewrite_live(&mut self, now: u64, next: u64) -> io::Result<()> {
        let live = self.live_list(now, next);
        let result = match self.cat.as_mut() {
            Some(cat) => cat.rewrite(&live),
            None => Err(io::Error::other("catalogue not open")),
        };
        match &result {
            Ok(()) => {
                self.incomplete_retiring = false;
                self.rewrite_error = None;
            }
            Err(error) => self.rewrite_error = Some(error.to_string()),
        }
        result
    }
    pub fn cat_bytes(&self) -> u64 {
        self.cat.as_ref().map_or(0, |c| c.bytes)
    }
    /// The live list a rewrite keeps: every unretired chunk's records, horizons, summaries and the import.
    pub fn live_list(&self, now: u64, next: u64) -> Vec<CatRecord> {
        let mut out = vec![CatRecord { at_ms: now, rec: Rec::Next(next) }];
        if let Some(resume) = &self.last_resume { out.push(resume.clone()); }
        if let Some(i) = &self.import {
            out.push(CatRecord { at_ms: self.import_at, rec: Rec::Import(i.clone()) });
        }
        for c in self.table.values() {
            if c.id == LEGACY_CHUNK || c.state == ChunkState::Retired {
                continue;
            }
            out.push(CatRecord {
                at_ms: c.open_ms,
                rec: Rec::Open { seq: c.id, stripe: c.stripe as u8, class: c.class, policy: c.policy.clone(), next },
            });
            if let Some(close) = c.close_ms {
                out.push(CatRecord { at_ms: close, rec: Rec::Close { seq: c.id, bytes: c.bytes, next } });
            }
            if c.state == ChunkState::Retiring {
                out.extend(retiring_records(c.retired_ms.unwrap_or(now), c.id, next, &c.horizons, &c.expired));
            }
        }
        // Payload: kind + at_ms + count, then length-prefixed UTF-8 pairs and versions.
        let mut part = Vec::new();
        let mut bytes = 13usize;
        for ((ns, key), version) in &self.horizons {
            let size = 16usize.saturating_add(ns.len()).saturating_add(key.len());
            if !part.is_empty() && bytes.checked_add(size).is_none_or(|n| n > MAX_FRAME) {
                out.push(CatRecord { at_ms: now, rec: Rec::Horizons(std::mem::take(&mut part)) });
                bytes = 13;
            }
            bytes = bytes.saturating_add(size);
            part.push((ns.clone(), key.clone(), *version));
        }
        if !part.is_empty() {
            out.push(CatRecord { at_ms: now, rec: Rec::Horizons(part) });
        }
        for (at, keys) in &self.summaries.list {
            // Payload: kind + at_ms + retired_ms + count, then length-prefixed UTF-8 pairs.
            let mut part = Vec::new();
            let mut bytes = 21usize;
            for key in keys {
                let size = 8usize.saturating_add(key.0.len()).saturating_add(key.1.len());
                if !part.is_empty() && bytes.checked_add(size).is_none_or(|n| n > MAX_FRAME) {
                    out.push(CatRecord { at_ms: *at, rec: Rec::Summary { retired_ms: *at, keys: std::mem::take(&mut part) } });
                    bytes = 21;
                }
                bytes = bytes.saturating_add(size);
                part.push(key.clone());
            }
            if !part.is_empty() {
                out.push(CatRecord { at_ms: *at, rec: Rec::Summary { retired_ms: *at, keys: part } });
            }
        }
        out
    }
}

pub fn horizon_set(list: &[(String, String, u64)]) -> HashMap<Key, u64> {
    let mut out: HashMap<Key, u64> = HashMap::new();
    for (ns, key, v) in list {
        let e = out.entry((ns.clone(), key.clone())).or_insert(0);
        *e = (*e).max(*v);
    }
    out
}

pub fn unique<T: Clone + Eq + std::hash::Hash>(v: &[T]) -> Vec<T> {
    let mut seen = HashSet::new();
    v.iter().filter(|x| seen.insert((*x).clone())).cloned().collect()
}

/// sha256 of a file, hex (std only; FIPS 180-4).
pub fn sha256_file(path: &Path) -> io::Result<String> {
    let mut f = File::open(path)?;
    let mut h = Sha256::new();
    let mut buf = vec![0u8; 1 << 16];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(h.hex())
}
pub struct Sha256 {
    state: [u32; 8],
    buf: Vec<u8>,
    len: u64,
}
const K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5, 0xd807aa98,
    0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
    0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da, 0x983e5152, 0xa831c66d, 0xb00327c8,
    0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
    0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819,
    0xd6990624, 0xf40e3585, 0x106aa070, 0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
    0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7,
    0xc67178f2,
];
impl Default for Sha256 {
    fn default() -> Self { Self::new() }
}
impl Sha256 {
    pub fn new() -> Self {
        Self {
            state: [
                0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
            ],
            buf: Vec::new(),
            len: 0,
        }
    }
    fn block(&mut self, b: &[u8]) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes(b[i * 4..i * 4 + 4].try_into().unwrap());
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16].wrapping_add(s0).wrapping_add(w[i - 7]).wrapping_add(s1);
        }
        let mut s = self.state;
        for i in 0..64 {
            let s1 = s[4].rotate_right(6) ^ s[4].rotate_right(11) ^ s[4].rotate_right(25);
            let ch = (s[4] & s[5]) ^ (!s[4] & s[6]);
            let t1 = s[7].wrapping_add(s1).wrapping_add(ch).wrapping_add(K[i]).wrapping_add(w[i]);
            let s0 = s[0].rotate_right(2) ^ s[0].rotate_right(13) ^ s[0].rotate_right(22);
            let maj = (s[0] & s[1]) ^ (s[0] & s[2]) ^ (s[1] & s[2]);
            let t2 = s0.wrapping_add(maj);
            s = [t1.wrapping_add(t2), s[0], s[1], s[2], s[3].wrapping_add(t1), s[4], s[5], s[6]];
        }
        for (state, value) in self.state.iter_mut().zip(s) {
            *state = state.wrapping_add(value);
        }
    }
    pub fn update(&mut self, mut d: &[u8]) {
        self.len += d.len() as u64;
        if !self.buf.is_empty() {
            let need = 64 - self.buf.len();
            let take = need.min(d.len());
            self.buf.extend(&d[..take]);
            d = &d[take..];
            if self.buf.len() == 64 {
                let b = std::mem::take(&mut self.buf);
                self.block(&b);
            }
        }
        while d.len() >= 64 {
            self.block(&d[..64]);
            d = &d[64..];
        }
        self.buf.extend(d);
    }
    pub fn finish(mut self) -> [u8; 32] {
        let bits = self.len * 8;
        let mut pad = vec![0x80u8];
        while (self.buf.len() + pad.len()) % 64 != 56 {
            pad.push(0);
        }
        pad.extend(bits.to_be_bytes());
        let len = self.len;
        self.update(&pad);
        self.len = len;
        let mut bytes = [0; 32];
        for (word, out) in self.state.iter().zip(bytes.as_chunks_mut::<4>().0) {
            out.copy_from_slice(&word.to_be_bytes());
        }
        bytes
    }
    pub fn hex(self) -> String {
        self.finish().iter().map(|x| format!("{x:02x}")).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn sha256_published_vectors() {
        let h = |b: &[u8]| {
            let mut s = Sha256::new();
            s.update(b);
            s.hex()
        };
        assert_eq!(h(b""), "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
        assert_eq!(h(b"abc"), "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad");
        assert_eq!(
            h(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
        assert_eq!(h(&vec![b'a'; 1_000_000]), "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0");
    }
    #[test]
    fn catalogue_records_round_trip() {
        let recs = vec![
            Rec::Open { seq: 3, stripe: 2, class: Class::expire(5), policy: "q/event/".into(), next: 9 },
            Rec::Close { seq: 3, bytes: 77, next: 10 },
            Rec::Retiring { seq: 3, next: 11, horizons: vec![("a".into(), "b".into(), 4)], expired: vec![("a".into(), "c".into())] },
            Rec::Retired { seq: 3 },
            Rec::Horizons(vec![("x".into(), "y".into(), 2)]),
            Rec::Summary { retired_ms: 5, keys: vec![("n".into(), "k".into())] },
            Rec::Next(12),
            Rec::Resume,
            Rec::Import(Import { files: vec![("stripe-0.log".into(), 9, "ab".into())], keys: 1, bytes: 2, epoch: 3, legacy_dir: "legacy-3".into() }),
        ];
        for (i, rec) in recs.into_iter().enumerate() {
            let r = CatRecord { at_ms: i as u64, rec };
            let b = encode(&r);
            let payload = &b[16..];
            assert_eq!(decode_payload(payload).unwrap(), r);
        }
    }
    #[test]
    fn policies_longest_prefix_and_class_cap() {
        let mut p = Policies::default();
        p.set("slots", "", Some(Class::LATEST));
        p.set("slots", "event/", Some(Class::expire(3 * 86_400_000)));
        assert_eq!(p.resolve("slots", "inference").0, Class::LATEST);
        assert_eq!(p.resolve("slots", "event/1").0, Class::expire(3 * 86_400_000));
        assert_eq!(p.resolve("other", "k").0, Class::FOREVER);
        assert_eq!(p.resolve(crate::timers::NS, "p/x").0, Class::LATEST);
        for i in 1..=61u64 {
            p.set("many", &format!("{i}/"), Some(Class::expire(i * 1000)));
        }
        assert_eq!(p.classes().len(), 64);
        assert!(p.would_exceed("many", "x/", Class::expire(999_999)));
        assert!(!p.would_exceed("many", "y/", Class::LATEST));
    }
}


#[cfg(test)]
mod catalogue_failure_tests {
    use super::*;
    use std::sync::{atomic::AtomicUsize, Mutex};
    fn catalogue(label: &str) -> Catalogue {
        let p = PathBuf::from(std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()))
            .join(format!("catalogue-{label}-{}-{}", std::process::id(), crate::now_ms()));
        fs::create_dir_all(&p).unwrap();
        Catalogue::open(&p.join("catalogue.log")).unwrap()
    }
    fn record(n: u64) -> CatRecord { CatRecord { at_ms: n, rec: Rec::Next(n) } }
    #[test]
    fn second_writer_is_refused_before_stale_rollback_and_after_replacement() {
        let mut b = catalogue("writer-exclusion");
        b.append(&record(1)).unwrap();
        let path = b.path.clone();
        assert!(Catalogue::open(&path).is_err(), "A must not capture a rollback boundary while B owns publication");
        b.append(&record(2)).unwrap();
        let acknowledged = fs::read(&path).unwrap();
        b.fail_sync_once = true;
        b.fail_recovery_once = true;
        assert!(b.append(&record(3)).is_err());
        assert!(fs::read(&path).unwrap().starts_with(&acknowledged));
        let uncertain = fs::read(&path).unwrap();
        let marker = fs::read(append_pending_path(&path)).unwrap();
        b.rewrite(&[record(1), record(2)]).unwrap();
        let evidence = b.recovery_evidence().unwrap();
        assert_eq!(fs::read(evidence.join("catalogue.log")).unwrap(), uncertain);
        assert_eq!(fs::read(evidence.join("append-pending")).unwrap(), marker);
        assert!(Catalogue::open(&path).is_err(), "replacement must retain lifetime exclusion");
        drop(b);
        let mut a = Catalogue::open(&path).unwrap();
        a.append(&record(4)).unwrap();
        assert_eq!(read_catalogue(&path).unwrap(), (vec![record(1), record(2), record(4)], None));
    }
    #[test]
    fn resume_encoding_evolution_and_malformed_controls() {
        let mut legacy = vec![8];
        legacy.extend(42u64.to_le_bytes());
        assert_eq!(decode_payload(&legacy).unwrap(), CatRecord { at_ms: 42, rec: Rec::Resume });
        for basis in [ResumeBasis::Operator, ResumeBasis::StableClock { since_utc_ms: 9,
            since_mono_ms: 10, observed_mono_ms: 20, required_ms: 10, tolerance_ms: 2000 }] {
            let record = CatRecord { at_ms: 42, rec: Rec::ResumeWithBasis(basis) };
            let bytes = encode(&record);
            assert_eq!(decode_payload(&bytes[16..]).unwrap(), record);
            assert!(decode_payload(&bytes[16..bytes.len()-1]).is_err());
            let mut extra = bytes[16..].to_vec();
            extra.push(0);
            assert!(decode_payload(&extra).is_err(), "unknown extension must refuse rather than invent basis");
        }
        let mut unknown = vec![11];
        unknown.extend(42u64.to_le_bytes());
        unknown.push(3);
        assert!(decode_payload(&unknown).is_err());
        legacy.push(1);
        assert!(decode_payload(&legacy).is_err());
    }
    fn check_append_failure(n: usize) {
        let mut c = catalogue(&format!("partial-{n}"));
        c.append(&record(1)).unwrap();
        let before = fs::read(&c.path).unwrap();
        c.fail_partial_once = Some(n);
        assert!(c.append(&record(2)).is_err());
        assert_eq!(fs::read(&c.path).unwrap(), before, "failed append must restore the durable frame boundary");
        c.append(&record(3)).unwrap();
        let (rows, torn) = read_catalogue(&c.path).unwrap();
        assert_eq!(rows, vec![record(1), record(3)]);
        assert_eq!(torn, None);
    }
    #[test]
    fn partial_catalogue_header_does_not_poison_later_appends() { check_append_failure(7); }
    #[test]
    fn partial_catalogue_body_does_not_hide_later_appends() { check_append_failure(20); }
    #[test]
    fn failed_catalogue_sync_does_not_apply_an_uncommitted_record() {
        let mut c = catalogue("sync");
        c.append(&record(1)).unwrap();
        let before = fs::read(&c.path).unwrap();
        c.fail_sync_once = true;
        assert!(c.append(&record(2)).is_err());
        assert_eq!(fs::read(&c.path).unwrap(), before);
        c.append(&record(3)).unwrap();
        assert_eq!(read_catalogue(&c.path).unwrap(), (vec![record(1), record(3)], None));
    }
    #[test]
    fn failed_replacement_open_keeps_the_published_catalogue_and_append_handle() {
        let mut c = catalogue("replace");
        c.append(&record(1)).unwrap();
        let before = fs::read(&c.path).unwrap();
        c.fail_reopen_once = true;
        assert!(c.rewrite(&[record(2)]).is_err());
        assert_eq!(fs::read(&c.path).unwrap(), before);
        c.append(&record(3)).unwrap();
        assert_eq!(read_catalogue(&c.path).unwrap(), (vec![record(1), record(3)], None));
        c.rewrite(&[record(3)]).unwrap();
        c.append(&record(4)).unwrap();
        assert_eq!(read_catalogue(&c.path).unwrap(), (vec![record(3), record(4)], None));
    }
    #[test]
    fn failed_boundary_recovery_fences_appends_until_a_successful_rewrite() {
        let mut c = catalogue("fence");
        c.append(&record(1)).unwrap();
        c.fail_partial_once = Some(7);
        c.fail_recovery_once = true;
        assert!(c.append(&record(2)).unwrap_err().to_string().contains("boundary recovery failed"));
        let failed = fs::read(&c.path).unwrap();
        assert!(c.append(&record(3)).unwrap_err().to_string().contains("fenced"));
        assert_eq!(fs::read(&c.path).unwrap(), failed);
        c.rewrite(&[record(1)]).unwrap();
        c.append(&record(3)).unwrap();
        assert_eq!(read_catalogue(&c.path).unwrap(), (vec![record(1), record(3)], None));
    }
    #[test]
    fn complete_failed_append_is_not_admitted_on_reopen() {
        let mut c = catalogue("complete-unresolved");
        c.append(&record(1)).unwrap();
        c.fail_sync_once = true;
        c.fail_recovery_once = true;
        assert!(c.append(&record(2)).unwrap_err().to_string().contains("boundary recovery failed"));
        let path = c.path.clone();
        let failed_bytes = fs::read(&path).unwrap();
        assert!(failed_bytes.ends_with(&encode(&record(2))), "witness must leave the complete failed frame");
        drop(c);
        assert!(read_catalogue(&path).unwrap_err().to_string().contains("append unresolved"));
        assert!(Catalogue::open(&path).err().unwrap().to_string().contains("ownership refused"));
        assert_eq!(fs::read(&path).unwrap(), failed_bytes, "refusal preserves recovery evidence");
    }
    #[test]
    fn unresolved_complete_append_can_be_replaced_only_with_known_live_state() {
        let mut c = catalogue("complete-rewrite");
        c.append(&record(1)).unwrap();
        c.fail_sync_once = true;
        c.fail_recovery_once = true;
        assert!(c.append(&record(2)).is_err());
        assert!(read_catalogue(&c.path).is_err());
        c.rewrite(&[record(1)]).unwrap();
        assert_eq!(read_catalogue(&c.path).unwrap(), (vec![record(1)], None));
        let path = c.path.clone();
        drop(c);
        let mut reopened = Catalogue::open(&path).unwrap();
        reopened.append(&record(3)).unwrap();
        assert_eq!(read_catalogue(&path).unwrap(), (vec![record(1), record(3)], None));
    }
    #[test]
    fn zero_threshold_still_requests_repair_of_an_unresolved_append() {
        let mut c = catalogue("zero-threshold-fence");
        c.append(&record(1)).unwrap();
        c.fail_sync_once = true;
        c.fail_recovery_once = true;
        assert!(c.append(&record(2)).is_err());
        assert!(c.needs_rewrite(0), "disabling size compaction cannot disable fence repair");
        c.rewrite(&[record(1)]).unwrap();
        assert!(!c.needs_rewrite(0));
        c.append(&record(3)).unwrap();
        assert!(!c.needs_rewrite(0));
        assert_eq!(read_catalogue(&c.path).unwrap(), (vec![record(1), record(3)], None));
    }
    #[test]
    fn zero_threshold_keeps_unfenced_catalogue_growth_uncompacted() {
        let mut c = catalogue("zero-threshold-growth");
        c.append(&record(1)).unwrap();
        c.append(&record(2)).unwrap();
        assert!(!c.needs_rewrite(0));
        assert!(!c.needs_rewrite(c.bytes));
        assert!(c.needs_rewrite(c.bytes - 1));
    }
    #[test]
    fn incomplete_marker_never_authorizes_replay_or_overwrite() {
        let mut c = catalogue("incomplete-marker");
        c.append(&record(1)).unwrap();
        let before = fs::read(&c.path).unwrap();
        fs::write(append_pending_path(&c.path), b"MBC").unwrap();
        assert!(read_catalogue(&c.path).is_err());
        assert!(c.append(&record(2)).is_err());
        assert_eq!(fs::read(&c.path).unwrap(), before);
        assert_eq!(fs::read(append_pending_path(&c.path)).unwrap(), b"MBC");
    }

    fn unresolved_catalogue(label: &str) -> Catalogue {
        let mut c = catalogue(label);
        c.append(&record(1)).unwrap();
        c.fail_sync_once = true;
        c.fail_recovery_once = true;
        assert!(c.append(&record(2)).is_err());
        assert!(append_pending_path(&c.path).exists());
        c
    }

    #[test]
    fn rewrite_directory_sync_follows_rename_before_marker_resolution() {
        let mut c = unresolved_catalogue("rewrite-directory-order");
        let path = c.path.clone();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = seen.clone();
        c.directory_sync = Some(Arc::new(move |dir| {
            assert_eq!(dir, path.parent().unwrap());
            assert_eq!(fs::read(&path)?, encode(&record(1)), "replacement must already name the published path");
            assert!(!path.with_extension("log.tmp").exists());
            observed.lock().unwrap().push(append_pending_path(&path).exists());
            Ok(())
        }));
        c.rewrite(&[record(1)]).unwrap();
        assert_eq!(seen.lock().unwrap().first(), Some(&true),
                   "the replacement directory barrier must precede clearing the recovery marker");
        assert!(!append_pending_path(&c.path).exists());
        assert_eq!(read_catalogue(&c.path).unwrap(), (vec![record(1)], None));
    }

    #[test]
    fn rewrite_directory_sync_failure_keeps_the_marker_and_append_fence_until_retry() {
        let mut c = unresolved_catalogue("rewrite-directory-failure");
        c.directory_sync = Some(Arc::new(|_| Err(io::Error::other("injected replacement directory sync failure"))));
        let error = c.rewrite(&[record(1)]).err().expect("replacement directory sync must be required");
        assert!(error.to_string().contains("replacement directory sync failure"));
        assert!(append_pending_path(&c.path).exists());
        assert!(read_catalogue(&c.path).unwrap_err().to_string().contains("append unresolved"));
        assert!(Catalogue::open(&c.path).err().unwrap().to_string().contains("ownership refused"));
        assert!(c.append(&record(3)).unwrap_err().to_string().contains("fenced"));
        assert_eq!(fs::read(&c.path).unwrap(), encode(&record(1)));
        assert_eq!(c.rewrites, 0, "a failed barrier must not count as a successful rewrite");
        c.directory_sync = None;
        c.rewrite(&[record(1)]).unwrap();
        c.append(&record(3)).unwrap();
        assert_eq!(read_catalogue(&c.path).unwrap(), (vec![record(1), record(3)], None));
    }

    #[test]
    fn failed_rewrite_directory_sync_keeps_the_handle_on_the_published_image() {
        let mut c = catalogue("rewrite-directory-handle");
        c.append(&record(1)).unwrap();
        c.directory_sync = Some(Arc::new(|_| Err(io::Error::other("injected replacement directory sync failure"))));
        assert!(c.rewrite(&[record(2)]).is_err(), "the directory failure must be returned");
        let mut retained = c.file.try_clone().unwrap();
        retained.seek(SeekFrom::Start(0)).unwrap();
        let mut image = Vec::new();
        retained.read_to_end(&mut image).unwrap();
        assert_eq!(image, encode(&record(2)));
        assert_eq!(image, fs::read(&c.path).unwrap(), "the retained handle must match the renamed path");
        assert!(c.append(&record(3)).unwrap_err().to_string().contains("fenced"));
        assert!(c.needs_rewrite(u64::MAX));
        assert_eq!(c.rewrite_base_bytes, 0);
        assert_eq!(c.rewrites, 0);
        c.directory_sync = None;
        c.rewrite(&[record(2)]).unwrap();
        c.append(&record(3)).unwrap();
        assert_eq!(read_catalogue(&c.path).unwrap(), (vec![record(2), record(3)], None));
    }

    #[test]
    fn append_marker_directory_sync_precedes_each_frame() {
        let mut c = catalogue("marker-directory-order");
        let path = c.path.clone();
        let expected = Arc::new(std::sync::Mutex::new(Vec::<u8>::new()));
        let before = expected.clone();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = seen.clone();
        c.directory_sync = Some(Arc::new(move |dir| {
            assert_eq!(dir, path.parent().unwrap());
            // This control observes intent creation; resolution has separate controls.
            if !append_pending_path(&path).exists() { return Ok(()); }
            let old = before.lock().unwrap();
            assert_eq!(fs::read(&path)?, *old, "the frame must not precede its marker-name barrier");
            let marker = fs::read(append_pending_path(&path))?;
            assert_eq!(marker.len(), 20);
            assert_eq!(&marker[..8], b"MBCP1\0\0\0");
            let boundary = u64::from_le_bytes(marker[8..16].try_into().unwrap());
            assert_eq!(boundary, old.len() as u64);
            assert_eq!(u32::from_le_bytes(marker[16..].try_into().unwrap()), crc32(&marker[..16]));
            observed.lock().unwrap().push(boundary);
            Ok(())
        }));
        c.append(&record(1)).unwrap();
        *expected.lock().unwrap() = fs::read(&c.path).unwrap();
        c.append(&record(2)).unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![0, encode(&record(1)).len() as u64]);
        assert!(!append_pending_path(&c.path).exists());
        assert_eq!(read_catalogue(&c.path).unwrap(), (vec![record(1), record(2)], None));
    }

    #[test]
    fn marker_directory_sync_failure_preserves_the_boundary_and_fences_until_rewrite() {
        let mut c = catalogue("marker-directory-failure");
        c.append(&record(1)).unwrap();
        let before = fs::read(&c.path).unwrap();
        let bytes = c.bytes;
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = calls.clone();
        c.directory_sync = Some(Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Err(io::Error::other("injected marker directory sync failure"))
        }));
        let error = c.append(&record(2)).err().expect("the marker name must be synced before writing a frame");
        assert!(error.to_string().contains("marker directory sync failure"));
        assert_eq!(fs::read(&c.path).unwrap(), before);
        assert_eq!((c.bytes, c.last_ms), (bytes, 1));
        assert!(append_pending_path(&c.path).exists());
        assert!(c.append(&record(3)).unwrap_err().to_string().contains("fenced"));
        assert_eq!(calls.load(Ordering::SeqCst), 1, "the fence must stop later writes before another marker attempt");
        assert!(Catalogue::open(&c.path).err().unwrap().to_string().contains("ownership refused"));
        assert!(read_catalogue(&c.path).unwrap_err().to_string().contains("append unresolved"));
        c.directory_sync = None;
        c.rewrite(&[record(1)]).unwrap();
        c.append(&record(3)).unwrap();
        assert_eq!(read_catalogue(&c.path).unwrap(), (vec![record(1), record(3)], None));
    }

    #[test]
    fn marker_name_barrier_precedes_a_partial_write_and_boundary_recovery() {
        let mut c = catalogue("marker-directory-partial");
        c.append(&record(1)).unwrap();
        let before = fs::read(&c.path).unwrap();
        let expected = before.clone();
        let path = c.path.clone();
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = calls.clone();
        c.directory_sync = Some(Arc::new(move |_| {
            // Keep observing the pre-frame intent barrier on this partial-write control.
            if !append_pending_path(&path).exists() { return Ok(()); }
            assert_eq!(fs::read(&path)?, expected);
            assert!(append_pending_path(&path).exists());
            observed.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }));
        c.fail_partial_once = Some(7);
        assert!(c.append(&record(2)).unwrap_err().to_string().contains("partial catalogue append"));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(fs::read(&c.path).unwrap(), before);
        assert!(!append_pending_path(&c.path).exists());
        c.directory_sync = None;
        c.append(&record(3)).unwrap();
        assert_eq!(read_catalogue(&c.path).unwrap(), (vec![record(1), record(3)], None));
    }

    fn resolution_catalogue(label: &str) -> Catalogue {
        let mut c = catalogue(label);
        c.append(&record(1)).unwrap();
        c
    }

    fn assert_refused(c: &mut Catalogue) {
        assert!(c.append(&record(9)).unwrap_err().to_string().contains("fenced"));
        assert!(read_catalogue(&c.path).unwrap_err().to_string().contains("append unresolved"));
        assert!(Catalogue::open(&c.path).err().unwrap().to_string().contains("ownership refused"));
    }

    fn fail_resolution(c: &mut Catalogue) -> Arc<Mutex<Vec<bool>>> {
        let path = c.path.clone();
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed = events.clone();
        c.directory_sync = Some(Arc::new(move |dir| {
            assert_eq!(dir, path.parent().unwrap());
            let pending = append_pending_path(&path).exists();
            observed.lock().unwrap().push(pending);
            if pending { Ok(()) } else { Err(io::Error::other("injected resolution directory failure")) }
        }));
        events
    }

    fn retry_from(c: &mut Catalogue, n: u64) {
        c.directory_sync = None;
        c.rewrite(&[record(n)]).unwrap();
        c.append(&record(n + 2)).unwrap();
        assert_eq!(read_catalogue(&c.path).unwrap(), (vec![record(n), record(n + 2)], None));
    }

    #[test]
    fn append_resolution_directory_barrier_precedes_publication_and_reopen() {
        let mut c = resolution_catalogue("append-order");
        let path = c.path.clone();
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed = events.clone();
        c.directory_sync = Some(Arc::new(move |dir| {
            assert_eq!(dir, path.parent().unwrap());
            let pending = append_pending_path(&path).exists();
            let expected = if pending { encode(&record(1)) } else {
                [encode(&record(1)), encode(&record(2))].concat()
            };
            assert_eq!(fs::read(&path)?, expected, "the marker removal follows the flushed complete frame");
            observed.lock().unwrap().push(pending);
            Ok(())
        }));
        c.append(&record(2)).unwrap();
        assert_eq!(*events.lock().unwrap(), vec![true, false], "success requires both directory barriers");
        assert_eq!(c.last_ms, 2);
        assert_eq!(c.bytes, fs::metadata(&c.path).unwrap().len());
        let path = c.path.clone();
        drop(c);
        let _reopened = Catalogue::open(&path).unwrap();
        assert_eq!(read_catalogue(&path).unwrap(), (vec![record(1), record(2)], None));
    }

    #[test]
    fn rollback_resolution_directory_barrier_follows_durable_boundary_repair() {
        let mut c = resolution_catalogue("rollback-order");
        let path = c.path.clone();
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed = events.clone();
        c.directory_sync = Some(Arc::new(move |_| {
            assert_eq!(fs::read(&path)?, encode(&record(1)), "rollback must precede marker resolution");
            observed.lock().unwrap().push(append_pending_path(&path).exists());
            Ok(())
        }));
        c.fail_partial_once = Some(7);
        assert!(c.append(&record(2)).unwrap_err().to_string().contains("partial catalogue append"));
        assert_eq!(*events.lock().unwrap(), vec![true, false], "rollback must also sync the marker unlink");
        c.directory_sync = None;
        c.append(&record(3)).unwrap();
        assert_eq!(read_catalogue(&c.path).unwrap(), (vec![record(1), record(3)], None));
    }

    #[test]
    fn append_resolution_directory_failure_restores_marker_and_refuses_reopen() {
        let mut c = resolution_catalogue("append-failure");
        let bytes = c.bytes;
        let events = fail_resolution(&mut c);
        let error = c.append(&record(2)).expect_err("publication must wait for its resolution directory sync");
        assert!(error.to_string().contains("resolution directory failure"));
        assert_eq!(*events.lock().unwrap(), vec![true, false, true], "restore the marker and its name on failure");
        assert_eq!((c.bytes, c.last_ms), (bytes, 1));
        assert_eq!(fs::read(&c.path).unwrap(), [encode(&record(1)), encode(&record(2))].concat());
        let marker = fs::read(append_pending_path(&c.path)).unwrap();
        assert_eq!(u64::from_le_bytes(marker[8..16].try_into().unwrap()), bytes);
        assert_eq!(u32::from_le_bytes(marker[16..].try_into().unwrap()), crc32(&marker[..16]));
        assert_refused(&mut c);
        retry_from(&mut c, 1);
    }

    #[test]
    fn rollback_resolution_directory_failure_restores_marker_and_keeps_fence() {
        let mut c = resolution_catalogue("rollback-failure");
        let bytes = c.bytes;
        let events = fail_resolution(&mut c);
        c.fail_sync_once = true;
        let error = c.append(&record(2)).unwrap_err().to_string();
        assert!(error.contains("catalogue sync failure"));
        assert!(error.contains("resolution directory failure"), "rollback resolution must reach the directory sync");
        assert!(error.contains("boundary recovery failed"));
        assert_eq!(*events.lock().unwrap(), vec![true, false, true]);
        assert_eq!(fs::read(&c.path).unwrap(), encode(&record(1)));
        assert_eq!((c.bytes, c.last_ms), (bytes, 1));
        assert_refused(&mut c);
        retry_from(&mut c, 1);
    }

    #[test]
    fn rewrite_resolution_directory_failure_preserves_replacement_handle_and_retry() {
        let mut c = resolution_catalogue("rewrite-failure");
        c.fail_sync_once = true;
        c.fail_recovery_once = true;
        assert!(c.append(&record(2)).is_err());
        let marker = fs::read(append_pending_path(&c.path)).unwrap();
        let events = fail_resolution(&mut c);
        let error = c.rewrite(&[record(3)]).expect_err("rewrite must wait for marker resolution durability");
        assert!(error.to_string().contains("resolution directory failure"));
        assert_eq!(*events.lock().unwrap(), vec![true, false, true]);
        assert_eq!(fs::read(append_pending_path(&c.path)).unwrap(), marker, "retain the exact original boundary evidence");
        let mut handle = c.file.try_clone().unwrap();
        handle.seek(SeekFrom::Start(0)).unwrap();
        let mut retained = Vec::new();
        handle.read_to_end(&mut retained).unwrap();
        assert_eq!(retained, encode(&record(3)));
        assert_eq!(retained, fs::read(&c.path).unwrap());
        assert_eq!((c.rewrites, c.rewrite_base_bytes), (0, 0));
        assert!(c.needs_rewrite(u64::MAX));
        assert_refused(&mut c);
        retry_from(&mut c, 3);
    }

    #[test]
    fn repeated_resolution_and_restore_barrier_failures_keep_visible_refusal() {
        let mut c = resolution_catalogue("restore-sync-failure");
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        c.directory_sync = Some(Arc::new(move |_| {
            match observed.fetch_add(1, Ordering::SeqCst) {
                0 => Ok(()),
                1 => Err(io::Error::other("injected resolution sync failure")),
                _ => Err(io::Error::other("injected restoration sync failure")),
            }
        }));
        let error = c.append(&record(2)).expect_err("a resolution failure must refuse publication").to_string();
        assert!(error.contains("resolution sync failure"));
        assert!(error.contains("restoration sync failure"));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        assert_refused(&mut c);
        retry_from(&mut c, 1);
    }

    #[test]
    fn marker_recreation_failure_reports_both_errors_and_keeps_live_fence() {
        let mut c = resolution_catalogue("restore-create-failure");
        let path = append_pending_path(&c.path);
        c.directory_sync = Some(Arc::new(move |_| {
            if path.exists() { return Ok(()); }
            // Make recreation fail deterministically without removing any retained evidence.
            fs::create_dir(&path)?;
            Err(io::Error::other("injected resolution failure before denied recreation"))
        }));
        let error = c.append(&record(2)).expect_err("failed resolution cannot authorize publication").to_string();
        assert!(error.contains("resolution failure before denied recreation"));
        assert!(error.contains("marker restoration failed"), "return restoration failure alongside the original error");
        assert!(append_pending_path(&c.path).is_dir());
        assert_refused(&mut c);
    }

    #[cfg(windows)]
    #[test]
    fn held_marker_unlink_refuses_before_resolution_barrier() {
        use std::os::windows::fs::OpenOptionsExt;
        let mut c = resolution_catalogue("held-marker");
        let path = append_pending_path(&c.path);
        let held = Arc::new(Mutex::new(None));
        let slot = held.clone();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        c.directory_sync = Some(Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            *slot.lock().unwrap() = Some(OpenOptions::new().read(true).share_mode(3).open(&path)?);
            Ok(())
        }));
        assert!(c.append(&record(2)).unwrap_err().to_string().contains("publication unresolved"));
        assert_eq!(calls.load(Ordering::SeqCst), 1, "an unsuccessful unlink must not reach the resolution barrier");
        assert_refused(&mut c);
        drop(held.lock().unwrap().take());
        retry_from(&mut c, 1);
    }
}

#[cfg(test)]
mod retiring_parts_tests {
    use super::*;

    fn part(index: u32, count: u32) -> CatRecord {
        CatRecord {
            at_ms: 123,
            rec: Rec::RetiringPart {
                seq: 2,
                next: 50,
                index,
                count,
                horizons: vec![("ns".into(), format!("h{index}"), 30 + index as u64)],
                expired: vec![("ns".into(), format!("e{index}"))],
            },
        }
    }

    #[test]
    fn retiring_parts_codec_and_utf8_sizes_are_bounded() {
        let h = vec![("界".repeat(341), "é".repeat(512), 7); 9000];
        let e = vec![("界".repeat(341), "é".repeat(512)); 9000];
        let records = retiring_records(123, 2, 50, &h, &e);
        assert!(records.len() > 2);
        for row in &records {
            let frame = encode_bounded(row).unwrap();
            assert!(frame.len() - 16 <= MAX_FRAME);
            assert_eq!(decode_payload(&frame[16..]).unwrap(), *row);
        }
        assert!(encode(&records[0]).len() - 16 > MAX_FRAME - 2100);
        let (logical, incomplete) = committed_records(records);
        assert!(!incomplete);
        assert_eq!(
            logical,
            vec![CatRecord {
                at_ms: 123,
                rec: Rec::Retiring {
                    seq: 2,
                    next: 50,
                    horizons: h,
                    expired: e
                }
            }]
        );
        for invalid in [part(0, 0), part(2, 2)] {
            assert!(decode_payload(&encode(&invalid)[16..]).is_err());
        }
        let old = retiring_records(123, 2, 50, &[], &[]);
        assert!(matches!(old[0].rec, Rec::Retiring { .. }));
        assert_eq!(committed_records(old.clone()), (old, false));
    }

    #[test]
    fn retiring_parts_incomplete_sets_cannot_publish_or_mix_attempts() {
        let other = CatRecord {
            at_ms: 124,
            rec: Rec::Next(90),
        };
        for bad in [
            vec![part(0, 3)],
            vec![part(1, 3), part(2, 3)],
            vec![part(0, 3), part(2, 3)],
            vec![part(0, 3), part(1, 3), part(1, 3), part(2, 3)],
        ] {
            assert_eq!(committed_records(bad), (vec![], true));
        }
        let mut changed_time = part(1, 2);
        changed_time.at_ms += 1;
        let mut changed_next = part(1, 2);
        if let Rec::RetiringPart { next, .. } = &mut changed_next.rec {
            *next += 1;
        }
        let mut changed_seq = part(1, 2);
        if let Rec::RetiringPart { seq, .. } = &mut changed_seq.rec {
            *seq += 1;
        }
        for mismatched in [changed_time, changed_next, changed_seq, part(1, 3)] {
            assert_eq!(
                committed_records(vec![part(0, 2), mismatched]),
                (vec![], true)
            );
        }
        assert_eq!(
            committed_records(vec![part(0, 2), other.clone(), part(1, 2)]),
            (vec![other], true)
        );
        let (complete, incomplete) = committed_records(vec![part(0, 2), part(0, 2), part(1, 2)]);
        assert!(incomplete);
        assert_eq!(complete.len(), 1);
        match &complete[0].rec {
            Rec::Retiring {
                horizons, expired, ..
            } => {
                assert_eq!(
                    horizons.len(),
                    2,
                    "the abandoned first part is not combined with the retry"
                );
                assert_eq!(expired.len(), 2);
            }
            _ => panic!("a complete set must become one retirement"),
        }
    }
}
