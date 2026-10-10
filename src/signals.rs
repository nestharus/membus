//! Epoch-scoped change retention and one bounded, independent delivery owner.
use crate::{lp, Config, Value, MAX_ID};
use std::{collections::VecDeque, io, sync::{Arc, Condvar, Mutex}, thread, time::Duration};

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
type Sink = fn(&str, &[u8], u32) -> io::Result<()>;
pub struct Hub {
    epoch: u64,
    ring_max: usize,
    ring_budget: usize,
    state: Arc<(Mutex<State>, Condvar)>,
    worker: Option<thread::JoinHandle<()>>,
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
    sub.bytes -= bytes;
    sub.dropped += sub.queue.len() as u64;
    sub.queue.clear();
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
    pub fn new(epoch: u64, config: &Config) -> Self {
        Self::with_sink(epoch, config, deliver)
    }
    fn with_sink(epoch: u64, config: &Config, sink: Sink) -> Self {
        let state = Arc::new((Mutex::new(State { head: 0, floor: 0, next: 1,
            ring: VecDeque::new(), ring_bytes: 0, queue_bytes: 0,
            subs: Vec::new(), turn: 0, stop: false }), Condvar::new()));
        let owner = state.clone();
        let worker = thread::spawn(move || {
            loop {
                let job = {
                    let (lock, wake) = &*owner;
                    let mut s = lock.lock().unwrap();
                    loop {
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
                let result = sink(&endpoint, &encode(epoch, id, &change, values), WRITE_MS);
                let mut s = owner.0.lock().unwrap();
                s.queue_bytes -= change.charge;
                if let Some(sub) = s.subs.iter_mut().find(|sub| sub.id == id) {
                    sub.bytes -= change.charge;
                    if result.is_ok() {
                        sub.written += 1;
                        sub.last = change.seq;
                        if sub.active { sub.status = if sub.queue.is_empty() { WRITTEN } else { QUEUED }; }
                    } else {
                        sub.failed += 1;
                        if sub.active { sub.status = FAILED; }
                        sub.active = false;
                        let freed = clear(sub);
                        s.queue_bytes -= freed;
                    }
                }
            }
        });
        Self { epoch, ring_max: config.signal_history, ring_budget: config.signal_bytes, state, worker: Some(worker) }
    }
    /// Called while the committed write lock orders successful publication.
    pub fn publish(&self, ns: &str, key: &str, value: Arc<Value>) {
        let mut s = self.state.0.lock().unwrap();
        let Some(seq) = s.head.checked_add(1) else {
            // Sequence exhaustion explicitly fences all streams, never reuses ids.
            for sub in &mut s.subs { sub.active = false; sub.status = DROPPED; }
            return;
        };
        s.head = seq;
        let charge = 256 + ns.len() + key.len() + value.bytes.capacity();
        let change = Arc::new(Change { seq, ns: ns.into(), key: key.into(), value, charge });
        s.ring_bytes += charge;
        s.ring.push_back(change.clone());
        while s.ring.len() > self.ring_max || s.ring_bytes > self.ring_budget {
            let old = s.ring.pop_front().unwrap();
            s.ring_bytes -= old.charge;
            s.floor = old.seq;
        }
        let mut total = s.queue_bytes;
        for sub in &mut s.subs {
            if sub.active && sub.ns == ns && key.starts_with(&sub.prefix) {
                if sub.queue.len() >= SUB_ITEMS || sub.bytes + charge > SUB_BYTES || total + charge > QUEUE_BYTES {
                    sub.active = false;
                    sub.status = DROPPED;
                    total -= clear(sub);
                    sub.dropped += 1;
                } else {
                    sub.queue.push_back(change.clone());
                    sub.bytes += charge;
                    total += charge;
                    sub.status = QUEUED;
                }
            }
        }
        s.queue_bytes = total;
        self.state.1.notify_one();
    }
    pub fn subscribe(&self, ns: &str, prefix: String, endpoint: String, values: bool, cursor: &[u8]) -> Result<Vec<u8>, u16> {
        if !valid_endpoint(&endpoint) || prefix.len() > MAX_ID { return Err(3); }
        let mut s = self.state.0.lock().unwrap();
        let start = if cursor.is_empty() { s.head } else { parse_token(self.epoch, cursor, 23)? };
        if start < s.floor || start > s.head { return Err(23); }
        let queue: VecDeque<_> = s.ring.iter().filter(|c| c.seq > start && c.ns == ns && c.key.starts_with(&prefix)).cloned().collect();
        let bytes: usize = queue.iter().map(|c| c.charge).sum();
        // Refuse oversize catch-up instead of returning a silently partial stream.
        if queue.len() > SUB_ITEMS || bytes > SUB_BYTES || s.queue_bytes + bytes > QUEUE_BYTES { return Err(6); }
        if s.subs.len() == SUB_MAX {
            if let Some(n) = s.subs.iter().position(|sub| !sub.active && sub.bytes == 0) { s.subs.remove(n); }
            else { return Err(6); }
        }
        let id = s.next;
        s.next = id.checked_add(1).ok_or(7u16)?;
        let status = if queue.is_empty() { WRITTEN } else { QUEUED };
        s.subs.push(Subscription { id, ns: ns.into(), prefix, endpoint, values,
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
}
#[cfg(windows)]
fn deliver(endpoint: &str, payload: &[u8], timeout: u32) -> io::Result<()> {
    use std::{fs::OpenOptions, os::windows::fs::OpenOptionsExt};
    // CreateFile never waits for a missing/busy pipe. Overlapped writes share
    // one absolute deadline, including all partial chunks and cancellation.
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout as u64);
    let file = loop {
        match OpenOptions::new().write(true).custom_flags(0x40000000).open(endpoint) {
            Ok(file) => break file,
            Err(error) if error.raw_os_error() == Some(231) && std::time::Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(1));
            }
            Err(error) => return Err(error),
        }
    };
    let mut pipe = crate::pipe_io::Pipe::new(file, timeout);
    pipe.write_until(payload, deadline)
}
#[cfg(not(windows))]
fn deliver(_: &str, _: &[u8], _: u32) -> io::Result<()> {
    Err(io::Error::new(io::ErrorKind::Unsupported, "signals require Windows pipes"))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn ok(_: &str, _: &[u8], _: u32) -> io::Result<()> { Ok(()) }
    fn value(n: u64) -> Arc<Value> { Arc::new(Value { version: n, ack: 1, bytes: vec![42], tombstone: false }) }
    #[test]
    fn retention_gap_and_filter_resume() {
        let h = Hub::with_sink(7, &Config { signal_history: 2, ..Config::default() }, ok);
        h.publish("quota", "event/1", value(1));
        h.publish("other", "event/2", value(2));
        h.publish("quota", "state/3", value(3));
        assert_eq!(h.subscribe("quota", "event/".into(), r"\\.\pipe\test".into(), false, &token(7, 0)).err(), Some(23));
        assert_eq!(h.subscribe("quota", "".into(), r"\\.\pipe\test".into(), false, &token(6, 3)).err(), Some(23));
        let r = h.subscribe("quota", "state/".into(), r"\\.\pipe\test".into(), true, &token(7, 1)).unwrap();
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
        let h = Hub::with_sink(7, &Config::default(), ok);
        for n in 1..=257 { h.publish("quota", "event/x", value(n)); }
        assert_eq!(h.subscribe("quota", "".into(), r"\\.\pipe\test".into(), false, &token(7, 0)).err(), Some(6));
        assert_eq!(h.subscribe("quota", "".into(), r"\\remote\pipe\x".into(), false, &[]).err(), Some(3));
    }
}
