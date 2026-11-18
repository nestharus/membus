//! Epoch-scoped change retention and one bounded, independent delivery owner.
use crate::{lp, Config, Value, MAX_ID};
use std::{collections::{HashMap, VecDeque}, io, sync::{atomic::{AtomicBool, Ordering}, Arc, Condvar, Mutex}, thread, time::Duration};

const SUB_MAX: usize = 64;
const SUB_ITEMS: usize = 256;
const SUB_BYTES: usize = 8 * 1024 * 1024;
const QUEUE_BYTES: usize = 16 * 1024 * 1024;
const WRITE_MS: u32 = 100;
pub const QUEUED: u8 = 0;
pub const WRITTEN: u8 = 1;
pub const FAILED: u8 = 2;
pub const DROPPED: u8 = 3;
pub const CANCELLED: u8 = 4;

struct Change {
    seq: u64,
    ns: String,
    key: String,
    value: Arc<Value>,
    charge: usize,
}
struct Subscription {
    id: u64,
    ns: String,
    prefix: String,
    endpoint: String,
    values: bool,
    frame_max: usize,
    active: bool,
    status: u8,
    queue: VecDeque<Arc<Change>>,
    bytes: usize, // includes delivery in flight
    written: u64,
    failed: u64,
    dropped: u64,
    last: u64,
}
struct State {
    head: u64,
    floor: u64, // a resume at floor is valid; earlier history is gone
    next: u64,
    ring: VecDeque<Arc<Change>>,
    ring_bytes: usize,
    queue_bytes: usize,
    subs: Vec<Subscription>,
    turn: usize,
    stop: bool,
}
#[cfg(windows)]
type Connection = crate::pipe_io::Pipe;
#[cfg(not(windows))]
type Connection = ();
pub struct Hub {
    epoch: u64,
    ring_max: usize,
    ring_budget: usize,
    state: Arc<(Mutex<State>, Condvar)>,
    released: Arc<AtomicBool>,
    worker: Option<thread::JoinHandle<()>>,
}
/// The share available to signals and the value cache while Store holds State.
pub(super) struct Budget<'a> {
    pub(super) bytes: usize,
    pub(super) cache: &'a crate::cache::Cache,
}
impl Budget<'_> {
    fn fund(&self, signal_bytes: usize) {
        self.cache.shrink(self.bytes - signal_bytes);
    }
}
fn token(epoch: u64, seq: u64) -> Vec<u8> {
    let mut b = epoch.to_le_bytes().to_vec();
    b.extend(seq.to_le_bytes());
    b
}
fn parse_token(epoch: u64, b: &[u8], code: u16) -> Result<u64, u16> {
    if b.len() != 16 || u64::from_le_bytes(b[..8].try_into().unwrap()) != epoch {
        return Err(code);
    }
    Ok(u64::from_le_bytes(b[8..].try_into().unwrap()))
}
fn clear(sub: &mut Subscription) -> usize {
    let bytes: usize = sub.queue.iter().map(|c| c.charge).sum();
    sub.dropped += sub.queue.len() as u64;
    sub.queue.clear();
    sub.bytes -= bytes;
    bytes
}
fn encode(epoch: u64, id: u64, change: &Change, values: bool) -> Vec<u8> {
    let mut b = b"MBC1".to_vec();
    b.extend(1u16.to_le_bytes());
    b.extend(token(epoch, id));
    b.extend(token(epoch, change.seq));
    lp(&mut b, change.ns.as_bytes());
    lp(&mut b, change.key.as_bytes());
    b.extend(change.value.version.to_le_bytes());
    b.push(change.value.ack);
    b.push(change.value.tombstone as u8);
    b.push(values as u8);
    if values { lp(&mut b, &change.value.bytes); }
    let mut frame = (b.len() as u32).to_le_bytes().to_vec();
    frame.extend(b);
    frame
}
impl Hub {
    pub(crate) fn stop_for_capture(&mut self) -> io::Result<()> {
        {
            let mut s=self.state.0.lock().unwrap();
            if s.subs.iter().any(|sub|sub.active || sub.bytes!=0) || s.queue_bytes!=0 {
                return Err(io::Error::other("signal delivery owner/subscription is not settled"));
            }
            s.stop=true;self.state.1.notify_all();
        }
        if let Some(worker)=self.worker.take() {worker.join().map_err(|_|io::Error::other("signal owner panicked"))?;}
        Ok(())
    }

    pub fn new(epoch: u64, config: &Config, clock: crate::chunks::Clock) -> Self {
        Self::with_sink(epoch, config, deliver, clock)
    }
    fn with_sink(epoch: u64, config: &Config,
        sink: impl Fn(&str, &[u8], u32, &mut Option<Connection>, &crate::chunks::Clock) -> io::Result<()> + Send + 'static,
        clock: crate::chunks::Clock,
    ) -> Self {
        let state = Arc::new((Mutex::new(State { head: 0, floor: 0, next: 1,
            ring: VecDeque::new(), ring_bytes: 0, queue_bytes: 0,
            subs: Vec::new(), turn: 0, stop: false }), Condvar::new()));
        let owner = state.clone();
        let released = Arc::new(AtomicBool::new(false));
        let worker_released = released.clone();
        let worker = thread::spawn(move || {
            let mut connections: HashMap<u64, Option<Connection>> = HashMap::new();
            loop {
                let job = {
                    let (lock, wake) = &*owner;
                    let mut s = lock.lock().unwrap();
                    loop {
                        connections.retain(|id, _| s.subs.iter().any(|sub| sub.id == *id && sub.active));
                        if s.stop { return; }
                        let mut job = None;
                        let count = s.subs.len();
                        for offset in 0..count {
                            let n = (s.turn + offset) % count;
                            let sub = &mut s.subs[n];
                            if let Some(change) = sub.queue.pop_front() {
                                job = Some((sub.id, sub.endpoint.clone(), sub.values, change));
                                s.turn = (n + 1) % count;
                                break;
                            }
                        }
                        if let Some(job) = job { break job; }
                        s = wake.wait(s).unwrap();
                    }
                };
                let (id, endpoint, values, change) = job;
                // Encoding and all endpoint I/O are outside the publication/hub lock.
                let result = sink(&endpoint, &encode(epoch, id, &change, values), WRITE_MS,
                    connections.entry(id).or_default(), &clock);
                let (charge, seq) = (change.charge, change.seq);
                drop(change);
                let mut s = owner.0.lock().unwrap();
                s.queue_bytes -= charge;
                if let Some(sub) = s.subs.iter_mut().find(|sub| sub.id == id) {
                    sub.bytes -= charge;
                    if result.is_ok() {
                        sub.written += 1;
                        sub.last = seq;
                        if sub.active { sub.status = if sub.queue.is_empty() { WRITTEN } else { QUEUED }; }
                    } else {
                        sub.failed += 1;
                        if sub.active { sub.status = FAILED; }
                        sub.active = false;
                        let freed = clear(sub);
                        s.queue_bytes -= freed;
                    }
                }
                worker_released.store(true, Ordering::Release);
            }
        });
        Self { epoch, ring_max: config.signal_history, ring_budget: config.signal_bytes, state, released, worker: Some(worker) }
    }
    /// Store holds State and, for KV changes, the committed write lock. The
    /// worker never takes State or the cache admission lock.
    pub(super) fn publish(&self, ns: &str, key: &str, value: Arc<Value>, budget: &Budget<'_>) {
        let mut s = self.state.0.lock().unwrap();
        let Some(seq) = s.head.checked_add(1) else {
            // Sequence exhaustion explicitly fences all streams, never reuses ids.
            for sub in &mut s.subs { sub.active = false; sub.status = DROPPED; }
            return;
        };
        let charge = 256 + ns.len() + key.len() + value.bytes.capacity();
        let before = s.ring_bytes + s.queue_bytes;
        // Keep existing deliveries, including the worker's in-flight charge.
        // History yields oldest first. A change that cannot be retained moves
        // the floor through its sequence so resume never skips the gap.
        let ring_budget = self.ring_budget.min(budget.bytes.saturating_sub(s.queue_bytes));
        while s.ring.len() >= self.ring_max || s.ring_bytes + charge > ring_budget {
            let Some(old) = s.ring.pop_front() else { break; };
            s.ring_bytes -= old.charge;
            s.floor = old.seq;
        }
        let retain = self.ring_max != 0 && charge <= ring_budget;
        let ring_bytes = s.ring_bytes + if retain { charge } else { 0 };
        let queue_budget = QUEUE_BYTES.min(budget.bytes.saturating_sub(ring_bytes));
        let mut queue_bytes = s.queue_bytes;
        let mut enqueue = [false; SUB_MAX];
        for (sub, enqueue) in s.subs.iter_mut().zip(&mut enqueue) {
            if sub.active && sub.ns == ns && key.starts_with(&sub.prefix) {
                if wire_size(ns, key, &value, sub.values) > sub.frame_max || sub.queue.len() >= SUB_ITEMS
                    || sub.bytes + charge > SUB_BYTES || queue_bytes + charge > queue_budget {
                    sub.active = false;
                    sub.status = DROPPED;
                    queue_bytes -= clear(sub);
                    sub.dropped += 1;
                } else {
                    *enqueue = true;
                    queue_bytes += charge;
                }
            }
        }
        // Reclaim cache capacity before any new history or queued reference
        // becomes resident. Evictions and discarded queues already yielded.
        if ring_bytes + queue_bytes > before {
            budget.fund(ring_bytes + queue_bytes);
        }
        s.head = seq;
        if !retain { s.floor = seq; }
        if retain || enqueue.contains(&true) {
            let change = Arc::new(Change { seq, ns: ns.into(), key: key.into(), value, charge });
            if retain { s.ring.push_back(change.clone()); }
            for (sub, enqueue) in s.subs.iter_mut().zip(enqueue) {
                if enqueue {
                    sub.queue.push_back(change.clone());
                    sub.bytes += charge;
                    sub.status = QUEUED;
                }
            }
        }
        s.ring_bytes = ring_bytes;
        s.queue_bytes = queue_bytes;
        self.state.1.notify_one();
    }
    pub(super) fn subscribe(&self, ns: &str, prefix: String, endpoint: String, values: bool, cursor: &[u8], protocol: &str, frame_max: usize, budget: &Budget<'_>) -> Result<Vec<u8>, u16> {
        if protocol != "membus.change/1" { return Err(2); }
        if !(1024..=crate::MAX_FRAME).contains(&frame_max) { return Err(4); }
        if !valid_endpoint(&endpoint) || prefix.len() > MAX_ID { return Err(3); }
        let mut s = self.state.0.lock().unwrap();
        let start = if cursor.is_empty() { s.head } else { parse_token(self.epoch, cursor, 23)? };
        if start < s.floor || start > s.head { return Err(23); }
        let matches = |c: &&Arc<Change>| c.seq > start && c.ns == ns && c.key.starts_with(&prefix);
        let (mut items, mut bytes) = (0, 0);
        for c in s.ring.iter().filter(matches) {
            if wire_size(&c.ns, &c.key, &c.value, values) > frame_max { return Err(4); }
            items += 1;
            bytes += c.charge;
        }
        // Refuse oversize catch-up instead of returning a silently partial stream.
        if items > SUB_ITEMS || bytes > SUB_BYTES || s.queue_bytes + bytes > QUEUE_BYTES
            || (bytes != 0 && s.ring_bytes + s.queue_bytes + bytes > budget.bytes) { return Err(6); }
        if s.subs.len() == SUB_MAX {
            if let Some(n) = s.subs.iter().position(|sub| !sub.active && sub.bytes == 0) { s.subs.remove(n); }
            else { return Err(6); }
        }
        let id = s.next;
        s.next = id.checked_add(1).ok_or(7u16)?;
        if bytes != 0 { budget.fund(s.ring_bytes + s.queue_bytes + bytes); }
        let queue = s.ring.iter().filter(matches).cloned().collect();
        let status = if items == 0 { WRITTEN } else { QUEUED };
        s.subs.push(Subscription { id, ns: ns.into(), prefix, endpoint, values, frame_max,
            active: true, status, queue, bytes, written: 0, failed: 0, dropped: 0, last: start });
        s.queue_bytes += bytes;
        let mut reply = Vec::new();
        lp(&mut reply, &token(self.epoch, id));
        lp(&mut reply, &token(self.epoch, start));
        self.state.1.notify_one();
        Ok(reply)
    }
    pub fn unsubscribe(&self, ns: &str, b: &[u8]) -> Result<(), u16> {
        let id = parse_token(self.epoch, b, 24)?;
        let mut s = self.state.0.lock().unwrap();
        let sub = s.subs.iter_mut().find(|sub| sub.id == id && sub.ns == ns).ok_or(24u16)?;
        sub.active = false;
        sub.status = CANCELLED;
        let freed = clear(sub);
        s.queue_bytes -= freed;
        if freed != 0 { self.released.store(true, Ordering::Release); }
        self.state.1.notify_one();
        Ok(())
    }
    pub fn status(&self, ns: &str, b: &[u8]) -> Result<Vec<u8>, u16> {
        let id = parse_token(self.epoch, b, 24)?;
        let s = self.state.0.lock().unwrap();
        let sub = s.subs.iter().find(|sub| sub.id == id && sub.ns == ns).ok_or(24u16)?;
        let mut reply = vec![sub.active as u8, sub.status];
        reply.extend((sub.queue.len() as u32).to_le_bytes());
        for n in [sub.bytes as u64, sub.written, sub.failed, sub.dropped] { reply.extend(n.to_le_bytes()); }
        lp(&mut reply, &token(self.epoch, sub.last));
        Ok(reply)
    }
    pub fn usage(&self) -> Vec<u8> {
        let s = self.state.0.lock().unwrap();
        let mut p = Vec::new();
        for n in [s.head, s.floor, s.ring.len() as u64, s.ring_bytes as u64,
            s.queue_bytes as u64, s.subs.len() as u64, self.ring_max as u64,
            self.ring_budget as u64, QUEUE_BYTES as u64] { p.extend(n.to_le_bytes()); }
        p
    }
    pub(crate) fn memory_bytes(&self) -> usize {
        let s = self.state.0.lock().unwrap();
        s.ring_bytes + s.queue_bytes
    }
    pub(super) fn take_released(&self) -> bool {
        self.released.swap(false, Ordering::AcqRel)
    }
    pub fn hello_limits(&self) -> Vec<u8> {
        let mut p = 1u16.to_le_bytes().to_vec(); // membus.change/1
        p.extend((crate::MAX_FRAME as u32).to_le_bytes());
        p.extend((self.ring_max as u64).to_le_bytes());
        p.extend((self.ring_budget as u64).to_le_bytes());
        p.extend((SUB_MAX as u16).to_le_bytes());
        p.extend((SUB_ITEMS as u16).to_le_bytes());
        p.extend((SUB_BYTES as u64).to_le_bytes());
        p.extend((QUEUE_BYTES as u64).to_le_bytes());
        p.extend(WRITE_MS.to_le_bytes());
        p
    }
}
impl Drop for Hub {
    fn drop(&mut self) {
        self.state.0.lock().unwrap().stop = true;
        self.state.1.notify_one();
        if let Some(worker) = self.worker.take() { let _ = worker.join(); }
    }
}
fn valid_endpoint(s: &str) -> bool {
    let lower = s.to_ascii_lowercase();
    lower.starts_with(r"\\.\pipe\") && s.len() > 9 && s.len() <= MAX_ID && !s.contains('\0')
        && !s[9..].contains(['/', ':'])
        && s[9..].split('\\').all(|part| !part.is_empty() && part != "." && part != ".." && !part.ends_with(['.', ' ']))
}
fn wire_size(ns: &str, key: &str, value: &Value, values: bool) -> usize {
    57 + ns.len() + key.len() + if values { 4 + value.bytes.len() } else { 0 }
}
#[cfg(windows)]
fn deliver(endpoint: &str, payload: &[u8], timeout: u32, connection: &mut Option<Connection>, clock: &crate::chunks::Clock) -> io::Result<()> {
    use std::{fs::OpenOptions, os::windows::fs::OpenOptionsExt};
    // CreateFile never waits for a missing/busy pipe. Overlapped writes share
    // one absolute deadline, including all partial chunks and cancellation.
    let deadline = clock.mono_ms().saturating_add(timeout as u64);
    if connection.is_none() {
      let file = loop {
        match OpenOptions::new().write(true).custom_flags(0x40000000)
            .security_qos_flags(0x100000 | 0x10000).open(endpoint) {
            Ok(file) => break file,
            Err(error) if error.raw_os_error() == Some(231) && clock.mono_ms() < deadline => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(error),
        }
      };
      *connection = Some(crate::pipe_io::Pipe::new(file, timeout));
    }
    let result = connection.as_mut().unwrap().write_before(payload, deadline, clock);
    if result.is_err() { *connection = None; }
    result
}
#[cfg(not(windows))]
fn deliver(_: &str, _: &[u8], _: u32, _: &mut Option<Connection>, _: &crate::chunks::Clock) -> io::Result<()> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "signals require Windows pipes"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{cache::{Cache, CachePolicy}, chunks::Clock};

    fn ok(_: &str, _: &[u8], _: u32, _: &mut Option<Connection>, _: &crate::chunks::Clock) -> io::Result<()> { Ok(()) }
    fn value(n: u64) -> Arc<Value> { Arc::new(Value { version: n, ack: 1, bytes: vec![42], tombstone: false }) }
    #[test]
    fn retention_gap_and_filter_resume() {
        let config = Config { signal_history: 2, ..Config::default() };
        let h = Hub::with_sink(7, &config, ok, Clock::new(None));
        let cache = Cache::new(&config, Clock::new(None));
        let budget = Budget { bytes: config.capacity, cache: &cache };
        h.publish("quota", "event/1", value(1), &budget);
        h.publish("other", "event/2", value(2), &budget);
        h.publish("quota", "state/3", value(3), &budget);
        assert_eq!(h.subscribe("quota", "event/".into(), r"\\.\pipe\test".into(), false, &token(7, 0), "membus.change/1", crate::MAX_FRAME, &budget).err(), Some(23));
        assert_eq!(h.subscribe("quota", "".into(), r"\\.\pipe\test".into(), false, &token(6, 3), "membus.change/1", crate::MAX_FRAME, &budget).err(), Some(23));
        let r = h.subscribe("quota", "state/".into(), r"\\.\pipe\test".into(), true, &token(7, 1), "membus.change/1", crate::MAX_FRAME, &budget).unwrap();
        let sub = &r[4..20];
        let until = std::time::Instant::now() + Duration::from_secs(1);
        while h.status("quota", sub).unwrap()[1] != WRITTEN && std::time::Instant::now() < until { thread::yield_now(); }
        let state = h.status("quota", sub).unwrap();
        assert_eq!(u64::from_le_bytes(state[14..22].try_into().unwrap()), 1);
        assert_eq!(parse_token(7, &state[42..58], 24).unwrap(), 3);
        h.unsubscribe("quota", sub).unwrap();
        assert_eq!(h.status("quota", sub).unwrap()[1], CANCELLED);
        assert_eq!(h.status("other", sub).err(), Some(24));
    }
    #[test]
    fn oversized_replay_is_explicit_and_endpoints_are_local() {
        let config = Config::default();
        let h = Hub::with_sink(7, &config, ok, Clock::new(None));
        let cache = Cache::new(&config, Clock::new(None));
        let budget = Budget { bytes: config.capacity, cache: &cache };
        for n in 1..=257 { h.publish("quota", "event/x", value(n), &budget); }
        assert_eq!(h.subscribe("quota", "".into(), r"\\.\pipe\test".into(), false, &token(7, 0), "membus.change/1", crate::MAX_FRAME, &budget).err(), Some(6));
        assert_eq!(h.subscribe("quota", "".into(), r"\\remote\pipe\x".into(), false, &[], "membus.change/1", crate::MAX_FRAME, &budget).err(), Some(3));
        assert_eq!(h.subscribe("quota", "".into(), r"\\.\pipe\..\GLOBALROOT\x".into(), false, &[], "membus.change/1", crate::MAX_FRAME, &budget).err(), Some(3));
        assert_eq!(h.subscribe("quota", "".into(), r"\\.\pipe\test".into(), false, &[], "other/1", crate::MAX_FRAME, &budget).err(), Some(2));
    }

    #[test]
    fn history_replacement_funds_net_growth_before_cache_admission() {
        let config = Config { signal_history: 1, signal_bytes: 2048, ..Config::default() };
        let h = Hub::with_sink(7, &config, ok, Clock::new(None));
        let cache = Cache::new(&config, Clock::new(None));
        let budget = Budget { bytes: 2048, cache: &cache };
        cache.trim(budget.bytes);
        let payload = |version, len| Arc::new(Value { version, ack: 1, bytes: vec![42; len], tombstone: false });
        let first = payload(1, 300);
        let first_owner = Arc::downgrade(&first);
        h.publish("n", "k", first, &budget);
        let charge = h.memory_bytes();
        assert_eq!(charge, 558);
        cache.insert(payload(10, 800), "test".into(), CachePolicy::default(), false, budget.bytes, &AtomicBool::new(true));
        let resident = cache.stats().bytes;
        assert!(resident > budget.bytes - 2 * charge);

        h.publish("n", "k", payload(2, 300), &budget);
        assert_eq!(h.memory_bytes(), charge);
        assert_eq!(cache.stats().bytes, resident);
        assert!(first_owner.upgrade().is_none());
        assert_eq!(h.state.0.lock().unwrap().floor, 1);

        h.publish("n", "k", payload(3, 1100), &budget);
        assert_eq!(h.memory_bytes(), 1358);
        assert_eq!(cache.stats().limit, budget.bytes - 1358);
        assert_eq!(cache.stats().bytes, 0);
        h.publish("n", "k", payload(4, 2048), &budget);
        assert_eq!(h.memory_bytes(), 0);
        let sub = |start| h.subscribe("n", "".into(), r"\\.\pipe\test".into(), false,
            &token(7, start), "membus.change/1", crate::MAX_FRAME, &budget);
        assert_eq!(sub(3).err(), Some(23));
        assert!(sub(4).is_ok());
    }

    #[test]
    fn queued_and_in_flight_deliveries_keep_their_charge_through_eviction_and_drop() {
        use std::sync::mpsc;
        let config = Config { signal_history: 1, ..Config::default() };
        let (entered, delivery) = mpsc::channel();
        let (complete, proceed) = mpsc::channel();
        let timeout = Duration::from_secs(5);
        let h = Hub::with_sink(7, &config, move |_, frame, _, _, _| {
            entered.send(frame.to_vec()).unwrap();
            proceed.recv_timeout(timeout).map_err(io::Error::other)
        }, Clock::new(None));
        let cache = Cache::new(&config, Clock::new(None));
        let charge = 259;
        let budget = Budget { bytes: 3 * charge, cache: &cache };
        cache.trim(budget.bytes);
        let sub = |cursor: &[u8]| h.subscribe("n", "".into(), r"\\.\pipe\test".into(), true, cursor,
            "membus.change/1", crate::MAX_FRAME, &budget).map(|reply| reply[4..20].to_vec());
        let live = sub(&[]).unwrap();
        let first = value(1);
        let first_owner = Arc::downgrade(&first);
        h.publish("n", "k", first, &budget);
        let frame = delivery.recv_timeout(timeout).unwrap();
        assert_eq!(parse_token(7, &frame[26..42], 23).unwrap(), 1);
        assert_eq!(frame.last(), Some(&42));
        assert_eq!(h.memory_bytes(), 2 * charge);

        h.publish("n", "k", value(2), &budget);
        assert_eq!(h.memory_bytes(), budget.bytes);
        assert_eq!(cache.stats().limit, 0);
        let status = h.status("n", &live).unwrap();
        assert_eq!(u32::from_le_bytes(status[2..6].try_into().unwrap()), 1);
        assert_eq!(u64::from_le_bytes(status[6..14].try_into().unwrap()), (2 * charge) as u64);
        assert!(first_owner.upgrade().is_some());

        h.publish("n", "k", value(3), &budget);
        assert_eq!(&h.status("n", &live).unwrap()[..2], &[0, DROPPED]);
        assert_eq!(h.memory_bytes(), 2 * charge);
        let replay = sub(&token(7, 2)).unwrap();
        assert_eq!(h.memory_bytes(), budget.bytes);
        assert_eq!(sub(&token(7, 2)), Err(6));
        assert_eq!(h.state.0.lock().unwrap().subs.len(), 2);
        h.unsubscribe("n", &replay).unwrap();
        assert!(h.take_released());
        assert_eq!(h.memory_bytes(), 2 * charge);
        assert!(first_owner.upgrade().is_some());

        complete.send(()).unwrap();
        let start = std::time::Instant::now();
        while !h.take_released() && start.elapsed() < timeout { thread::yield_now(); }
        assert_eq!(h.memory_bytes(), charge);
        assert!(first_owner.upgrade().is_none());
        let status = h.status("n", &live).unwrap();
        assert_eq!(&status[..2], &[0, DROPPED]);
        assert_eq!(u64::from_le_bytes(status[14..22].try_into().unwrap()), 1);
        assert_eq!(u64::from_le_bytes(status[30..38].try_into().unwrap()), 2);
    }

    #[test]
    fn catch_up_shrinks_store_cache_until_the_last_delivery_owner_releases() {
        use crate::{codec, ManualClock, Mutation, Store};
        use std::{path::PathBuf, sync::mpsc};

        const CAPACITY: usize = 2 * 1024 * 1024;
        let path = PathBuf::from(std::env::var_os("MEMBUS_TEST_OUTPUT").unwrap_or_else(|| ".test-output".into()))
            .join(format!("signal-catch-up-{}-{}", std::process::id(), crate::now_ms()));
        let mut store = Store::open(&path, Config {
            capacity: CAPACITY,
            cache_bytes: CAPACITY,
            signal_history: 1,
            tick_ms: 3_600_000,
            retire_ms: 3_600_000,
            clock: Some(ManualClock::new(1_000_000)),
            ..Config::default()
        }).unwrap();
        let (entered, delivery) = mpsc::channel();
        let (complete, proceed) = mpsc::channel();
        let timeout = Duration::from_secs(5);
        store.signals = Hub::with_sink(store.epoch, &store.config, move |_, _, _, _, _| {
            entered.send(()).unwrap();
            proceed.recv_timeout(timeout).map_err(io::Error::other)
        }, store.clock.clone());
        for (ns, key) in [("other", "cold"), ("ledger", "row")] {
            let outcome = store.mutate(Mutation {
                ns: ns.into(), key: key.into(), op_id: key.into(), deadline: 2_000_000,
                ack: 1, expected: None, bytes: vec![42; 4096], remove: false,
            });
            assert_eq!(outcome.code, 0);
        }
        let charge = store.signals.memory_bytes();
        assert_eq!(charge, 4361);
        let leftover = 1024;
        let available = store.cache_limit(&store.state.lock().unwrap());
        let cohort = store.read_cohort(available - charge - leftover).unwrap();
        let retained = cohort.try_get("ledger", "row").unwrap().unwrap();
        drop(cohort);
        let subscribe = || {
            let mut request = 2_000_000u64.to_le_bytes().to_vec();
            lp(&mut request, b"subscribe");
            lp(&mut request, b"ledger");
            lp(&mut request, b"");
            lp(&mut request, br"\\.\pipe\test");
            request.push(1);
            lp(&mut request, &token(store.epoch, 1));
            lp(&mut request, b"membus.change/1");
            request.extend((crate::MAX_FRAME as u32).to_le_bytes());
            store.dispatch(&codec::frame(codec::SUBSCRIBE, 1, &request)[4..])
        };
        let reply_code = |b: &[u8]| u16::from_le_bytes(b[17..19].try_into().unwrap());
        let reply = subscribe();
        assert_eq!(reply_code(&reply), 0);
        let id = &reply[23..39];
        delivery.recv_timeout(timeout).unwrap();
        assert_eq!(reply_code(&subscribe()), 6);
        let blocked = store.health2();
        assert_eq!(blocked.cache.limit, leftover);
        assert!(blocked.memory_bytes <= blocked.memory_budget);
        assert_eq!(store.signals.memory_bytes(), 2 * charge);
        assert_eq!(store.try_get("other", "cold").unwrap().unwrap().bytes[0], 42);
        assert_eq!(store.cache_stats().bytes, 0);
        store.signals.unsubscribe("ledger", id).unwrap();
        assert_eq!(store.signals.memory_bytes(), 2 * charge);

        complete.send(()).unwrap();
        let start = std::time::Instant::now();
        while store.signals.memory_bytes() != charge && start.elapsed() < timeout { thread::yield_now(); }
        assert_eq!(store.signals.memory_bytes(), charge);
        assert_eq!(store.cache_stats().limit, leftover);
        assert_eq!(store.try_get("other", "cold").unwrap().unwrap().bytes[0], 42);
        let settled = store.health2();
        assert_eq!(settled.cache.limit, leftover + charge);
        assert!(settled.cache.bytes > 0);
        assert!(settled.memory_bytes <= settled.memory_budget);
        assert_eq!(retained.bytes, vec![42; 4096]);
        assert_eq!(store.signals.status("ledger", id).unwrap()[1], CANCELLED);
    }
}
