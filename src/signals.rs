//! Epoch-scoped change retention and one bounded, independent delivery owner.
use crate::{lp, Config, Value, MAX_ID};
use std::{io, sync::{Arc, Condvar, Mutex}, thread, time::Duration};

#[path = "signals/finite.rs"]
mod finite;
use finite::*;
#[path="signals/legacy.rs"]
mod legacy;
use legacy::*;

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
    allocation: Option<crate::subscription_resources::SubscriptionReservation>,
}
struct Subscription {
    id: u64,
    ns: String,
    prefix: String,
    slot: Option<LegacySlotRef>,
    values: bool,
    frame_max: usize,
    active: bool,
    status: u8,
    queue: std::collections::LinkedList<ChangeRef>,
    bytes: usize, // includes delivery in flight
    written: u64,
    failed: u64,
    dropped: u64,
    last: u64,
    allocation: Option<crate::subscription_resources::SubscriptionReservation>,
}
struct State {
    head: u64,
    floor: u64, // a resume at floor is valid; earlier history is gone
    next: u64,
    ring: std::collections::LinkedList<ChangeRef>,
    ring_bytes: usize,
    queue_bytes: usize,
    subs: std::collections::LinkedList<Subscription>,
    finite: std::collections::LinkedList<FiniteSubscription>,
    stop: bool,
}
#[cfg(windows)]
type Connection = crate::pipe_io::Pipe;
#[cfg(not(windows))]
type Connection = ();
type Sink = fn(&str, &[u8], u32, &mut Option<Connection>, &crate::chunks::Clock) -> io::Result<()>;
pub struct Hub {
    epoch: u64,
    ring_max: usize,
    ring_budget: usize,
    state: Arc<(Mutex<State>, Condvar)>,
    worker: Option<thread::JoinHandle<()>>,
    clock: crate::chunks::Clock,
    account: std::sync::OnceLock<crate::subscription_resources::SubscriptionAccount>,
    admission:Mutex<()>,
}
/// Existing concrete layouts, shared with prospective resource demand builders.
pub(crate) fn registration_allocation_bytes() -> usize {
    2*(std::mem::size_of::<Subscription>()+2*std::mem::size_of::<usize>())
}
pub(crate) fn connection_allocation_bytes()->usize {legacy::slot_allocation_bytes()}
pub(crate) fn queue_link_allocation_bytes()->usize {2*std::mem::size_of::<usize>()+std::mem::size_of::<ChangeRef>()}
pub(crate) fn change_allocation_bytes() -> usize {
    // Arc strong/weak counters for Change and Value, plus a retained Arc link.
    std::mem::size_of::<Change>() + std::mem::size_of::<Value>()
        + 4 * std::mem::size_of::<usize>() + std::mem::size_of::<Arc<Change>>()
}
/// Prospective managed representation charge for the unchanged legacy Hub.
/// This is reserved even while quiet. Kernel buffers/handles and allocator RSS
/// are not measured; the Store margin remains separate. No policy is enlarged.
#[cfg(test)]
pub(crate) fn legacy_memory_reservation(history_items: usize, history_bytes: usize) -> usize {
    use std::mem::size_of;
    // A retained Change charges at least 256. Include the append-before-evict
    // slot and replay collection, with backing growth for both VecDeques.
    let history_slots = history_items.min(history_bytes / 256).saturating_add(1);
    let links = history_slots.saturating_mul(4).saturating_add(8)
        .saturating_mul(size_of::<Arc<Change>>());
    // Actual row, owned identifier strings, queue backing and connection-map
    // bookkeeping. The existing legacy queue/registration bounds still govern.
    let row = size_of::<Subscription>().saturating_add(3 * MAX_ID)
        .saturating_add(2 * (SUB_ITEMS + 1) * size_of::<Arc<Change>>())
        .saturating_add(4 * size_of::<(u64, Option<Connection>)>());
    let transient_change = 256usize.saturating_add(2 * MAX_ID).saturating_add(crate::MAX_VALUE);
    // encode holds body and framed buffers at once, each with growth slack.
    history_bytes.saturating_add(QUEUE_BYTES).saturating_add(links)
        // Shared Hub/State/clock/list metadata is present even with no rows.
        .saturating_add(size_of::<State>()).saturating_add(size_of::<Hub>())
        .saturating_add(SUB_MAX.saturating_mul(row)).saturating_add(transient_change)
        .saturating_add(4 * (crate::MAX_FRAME + 4))
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
    if let Some(grant)=sub.allocation.as_mut() {
        let retained=grant.resources().retained;
        if retained!=0 {let mut queue=grant.split(crate::subscription_resources::SubscriptionResources{retained,..Default::default()}).expect("owned queue backing");queue.return_to_owner();}
    }
    bytes
}
fn encode(epoch: u64, id: u64, change: &Change, values: bool) -> Vec<u8> {
    let mut b = Vec::with_capacity(wire_size(change,values));
    b.extend_from_slice(b"MBC1");
    b.extend(1u16.to_le_bytes());
    b.extend(epoch.to_le_bytes());b.extend(id.to_le_bytes());
    b.extend(epoch.to_le_bytes());b.extend(change.seq.to_le_bytes());
    lp(&mut b, change.ns.as_bytes());
    lp(&mut b, change.key.as_bytes());
    b.extend(change.value.version.to_le_bytes());
    b.push(change.value.ack);
    b.push(change.value.tombstone as u8);
    b.push(values as u8);
    if values { lp(&mut b, &change.value.bytes); }
    let mut frame = Vec::with_capacity(b.len()+4);
    frame.extend((b.len() as u32).to_le_bytes());
    frame.extend(b);
    frame
}
enum Job {
    Legacy(u64,LegacySlotRef,bool,ChangeRef),
    Hint(crate::subscription_contract::HintBinding,Arc<HintSlot>,u64,u32,u64,u64),
}
impl Hub {
    pub(crate) fn stop_for_capture(&mut self) -> io::Result<()> {
        {
            let mut s=self.state.0.lock().unwrap();
            if !s.finite.is_empty() || s.subs.iter().any(|sub|sub.active || sub.bytes!=0) || s.queue_bytes!=0 {
                return Err(io::Error::other("signal delivery owner/subscription is not settled"));
            }
            s.stop=true;self.state.1.notify_all();
        }
        if let Some(worker)=self.worker.take() {worker.join().map_err(|_|io::Error::other("signal owner panicked"))?;}
        {let mut s=self.state.0.lock().unwrap();while let Some(row)=s.subs.pop_front(){drop(row);}while let Some(change)=s.ring.pop_front(){drop(change);}s.ring_bytes=0;}
        Ok(())
    }

    pub fn new(epoch: u64, config: &Config, clock: crate::chunks::Clock) -> Self {
        Self::with_sink(epoch, config, deliver, clock)
    }
    fn with_sink(epoch: u64, config: &Config, sink: Sink, clock: crate::chunks::Clock) -> Self {
        let state = Arc::new((Mutex::new(State { head: 0, floor: 0, next: 1,
            ring: Default::default(), ring_bytes: 0, queue_bytes: 0,
            subs: Default::default(), finite: Default::default(), stop: false }), Condvar::new()));
        let owner = state.clone();
        let hub_clock=clock.clone();
        let worker = thread::spawn(move || {

            loop {
                let job = {
                    let (lock, wake) = &*owner;
                    let mut s = lock.lock().unwrap();
                    loop {
                        let retired=detach_expired(&mut s,clock.mono_ms());
                        if !retired.is_empty() {drop(s);settle_rows(retired);s=lock.lock().unwrap();continue;}

                        if s.stop {
                            let retired=std::mem::take(&mut s.finite);drop(s);settle_rows(retired);return;
                        }
                        let mut job = None;
                        let count=s.subs.len();
                        for _ in 0..count {
                            let mut sub=s.subs.pop_front().unwrap();
                            if let Some(change)=sub.queue.pop_front() {
                                job=Some(Job::Legacy(sub.id,sub.slot.as_ref().unwrap().clone(),sub.values,change));
                            }
                            s.subs.push_back(sub);
                            if job.is_some(){break;}
                        }
                        if let Some(job) = job { break job; }
                        let mut hint=None;
                        for _ in 0..s.finite.len() {
                            let mut row=s.finite.pop_front().unwrap();
                            if hint.is_none() {if let Some(seq)=row.pending.take() {
                                hint=Some(Job::Hint(row.binding,row.slot.clone(),seq,row.write_ms,row.until,row.generation));
                            }}
                            s.finite.push_back(row);
                        }
                        if let Some(job)=hint {break job;}
                        if let Some(until)=s.finite.iter().map(|row|row.until).min() {
                            let wait=Duration::from_millis(until.saturating_sub(clock.mono_ms()));
                            s=wake.wait_timeout(s,wait).unwrap().0;
                        }else{s = wake.wait(s).unwrap();}
                    }
                };
                let (id, slot, values, change) = match job {
                    Job::Legacy(id,slot,values,change)=>(id,slot,values,change),
                    Job::Hint(binding,slot,seq,write_ms,until,generation)=>{
                        // Local completion confers no recipient-accepted evidence. No
                        // persistent connection survives the actual in-flight owner.
                        let mut frame=[0u8;64];frame[..4].copy_from_slice(&60u32.to_le_bytes());frame[4..8].copy_from_slice(b"MBH2");
                        frame[8..56].copy_from_slice(&binding.bytes());frame[56..].copy_from_slice(&seq.to_le_bytes());
                        let now=clock.mono_ms();
                        if now<until {
                            let mut connection=None;
                            let timeout=(until-now).min(write_ms as u64) as u32;
                            let result=sink(&slot.endpoint,&frame,timeout,&mut connection,&clock);
                            drop(connection);
                            if result.is_err() {
                                // Exact old identity only. Detach while locked; return
                                // allocation ownership after releasing the Hub mutex.
                                let mut s=owner.0.lock().unwrap();let mut retired=std::collections::LinkedList::new();
                                for _ in 0..s.finite.len() {let row=s.finite.pop_front().unwrap();
                                    if row.binding==binding && row.generation==generation {retired.push_back(row);}else{s.finite.push_back(row);}}
                                drop(s);settle_rows(retired);
                            }
                        }
                        settle_slot(slot);continue;
                    }
                };
                // Encoding and all endpoint I/O are outside the publication/hub lock.
                let result = slot.send(sink,&encode(epoch,id,&change,values),&clock);
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
                        drop(sub.slot.take());
                        let freed = clear(sub);
                        s.queue_bytes -= freed;
                    }
                }
            }
        });
        Self { epoch, ring_max: config.signal_history, ring_budget: config.signal_bytes, state, worker: Some(worker), clock:hub_clock,account:Default::default(),admission:Mutex::new(()) }
    }
    pub fn unsubscribe(&self, ns: &str, b: &[u8]) -> Result<(), u16> {
        let id = parse_token(self.epoch, b, 24)?;
        let mut s = self.state.0.lock().unwrap();
        let sub = s.subs.iter_mut().find(|sub| sub.id == id && sub.ns == ns).ok_or(24u16)?;
        sub.active = false;
        sub.status = CANCELLED;
        drop(sub.slot.take());
        let freed = clear(sub);
        s.queue_bytes -= freed;
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
    pub(crate) fn memory_reservation_bytes(&self) -> usize {
        self.account.get().map_or(0,|account|account.usage.bytes.load(std::sync::atomic::Ordering::Acquire))
    }
    pub(crate) fn memory_bytes(&self) -> usize {
        let s = self.state.0.lock().unwrap();
        s.ring_bytes + s.queue_bytes
    }
    pub(crate) fn contract_discovery(&self) -> &'static [u8] { crate::subscription_contract::DISCOVERY }
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
fn wire_size(c: &Change, values: bool) -> usize {
    57 + c.ns.len() + c.key.len() + if values { 4 + c.value.bytes.len() } else { 0 }
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
    fn ok(_: &str, _: &[u8], _: u32, _: &mut Option<Connection>, _: &crate::chunks::Clock) -> io::Result<()> { Ok(()) }
    fn value(n: u64) -> Arc<Value> { Arc::new(Value { version: n, ack: 1, bytes: vec![42], tombstone: false }) }
    #[test]
    fn retention_gap_and_filter_resume() {
        let h = Hub::with_sink(7, &Config { signal_history: 2, ..Config::default() }, ok, crate::chunks::Clock::new(None));
        let _account=h.capture_account(&Config {signal_history:2,..Config::default()});
        h.publish("quota", "event/1", value(1));
        h.publish("other", "event/2", value(2));
        h.publish("quota", "state/3", value(3));
        assert_eq!(h.subscribe("quota", "event/".into(), r"\\.\pipe\test".into(), false, &token(7, 0), "membus.change/1", crate::MAX_FRAME).err(), Some(23));
        assert_eq!(h.subscribe("quota", "".into(), r"\\.\pipe\test".into(), false, &token(6, 3), "membus.change/1", crate::MAX_FRAME).err(), Some(23));
        let r = h.subscribe("quota", "state/".into(), r"\\.\pipe\test".into(), true, &token(7, 1), "membus.change/1", crate::MAX_FRAME).unwrap();
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
        let h = Hub::with_sink(7, &Config::default(), ok, crate::chunks::Clock::new(None));
        let _account=h.capture_account(&Config::default());
        for n in 1..=257 { h.publish("quota", "event/x", value(n)); }
        assert_eq!(h.subscribe("quota", "".into(), r"\\.\pipe\test".into(), false, &token(7, 0), "membus.change/1", crate::MAX_FRAME).err(), Some(6));
        assert_eq!(h.subscribe("quota", "".into(), r"\\remote\pipe\x".into(), false, &[], "membus.change/1", crate::MAX_FRAME).err(), Some(3));
        assert_eq!(h.subscribe("quota", "".into(), r"\\.\pipe\..\GLOBALROOT\x".into(), false, &[], "membus.change/1", crate::MAX_FRAME).err(), Some(3));
        assert_eq!(h.subscribe("quota", "".into(), r"\\.\pipe\test".into(), false, &[], "other/1", crate::MAX_FRAME).err(), Some(2));
    }
}

#[cfg(test)]
mod contract_seam_tests {
    use super::*;
    fn inert(_: &str, _: &[u8], _: u32, _: &mut Option<Connection>, _: &crate::chunks::Clock) -> io::Result<()> { Ok(()) }
    #[test]
    fn actual_64_quiet_legacy_registrations_do_not_expire() {
        let manual = crate::chunks::ManualClock::new(900_000);
        let hub = Hub::with_sink(91, &Config { signal_history: 0, signal_bytes: 0, ..Config::default() }, inert, crate::chunks::Clock::new(Some(manual.clone())));
        let _account=hub.capture_account(&Config{signal_history:0,signal_bytes:0,..Config::default()});
        let mut tokens = Vec::new();
        for i in 0..64 {
            let reply = hub.subscribe("authored", "".into(), format!(r"\\.\pipe\inert-{i}"), false, &[], "membus.change/1", 1024).unwrap();
            tokens.push(reply[4..20].to_vec());
        }
        manual.advance(1_000_000);
        manual.jump_utc(-700_000);
        assert_eq!(hub.subscribe("authored", "".into(), r"\\.\pipe\inert-65".into(), false, &[], "membus.change/1", 1024), Err(6));
        assert_eq!(hub.memory_bytes(), 0);
        for token in &tokens { assert_eq!(hub.status("authored", token).unwrap()[0], 1); }
        for token in tokens { hub.unsubscribe("authored", &token).unwrap(); }
        let mut hub = hub;
        hub.stop_for_capture().unwrap();
    }

}

#[cfg(test)]
#[path="signals/quiet_compat_tests.rs"]
mod quiet_compat_tests;
