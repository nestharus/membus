//! Epoch-scoped change retention and one bounded, independent delivery owner.
use crate::{lp, Config, Value, MAX_ID};
use std::{io, sync::{Arc, Condvar, Mutex}, thread, time::Duration};

mod finite;
use finite::*;
mod legacy;
use legacy::*;
pub mod contract;
pub mod resources;
mod worker;

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
    ring: std::collections::LinkedList<History>,
    ring_bytes: usize,
    queue_bytes: usize,
    subs: std::collections::LinkedList<Subscription>,
    finite: std::collections::LinkedList<FiniteSubscription>,
    // A lower bound on every retained expiry. Renewal/removal may leave it
    // early; only a due full sweep advances it. No extra registry is retained.
    finite_expiry:Option<u64>,
    stop: bool,
    #[cfg(test)]
    worker_visits:(usize,usize),
}
type Connection = crate::transport::local_pipe::SignalConnection;
#[cfg(test)]
type Sink = fn(&str, &[u8], u32, &mut Option<Connection>, &crate::chunks::Clock) -> io::Result<()>;
type DeliverySink = dyn Fn(&str, &[u8], u32, &mut Option<Connection>, &crate::chunks::Clock) -> io::Result<()> + Send;
pub struct Hub {
    epoch: u64,
    ring_max: usize,
    ring_budget: usize,
    state: Arc<(Mutex<State>, Condvar)>,
    worker: Option<thread::JoinHandle<()>>,
    clock: crate::chunks::Clock,
    #[cfg(test)]
    account: std::sync::OnceLock<crate::subscription_resources::SubscriptionAccount>,
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
    fn with_sink(epoch: u64, config: &Config, sink: impl Fn(&str, &[u8], u32, &mut Option<Connection>, &crate::chunks::Clock) -> io::Result<()> + Send + 'static, clock: crate::chunks::Clock) -> Self {
        let state = Arc::new((Mutex::new(State { head: 0, floor: 0, next: 1,
            ring: Default::default(), ring_bytes: 0, queue_bytes: 0,
            subs: Default::default(), finite: Default::default(), finite_expiry:None, #[cfg(test)] worker_visits:(0,0), stop: false }), Condvar::new()));
        let worker = worker::DeliveryWorker::start(epoch,state.clone(),Box::new(sink),clock.clone());
        Self { epoch, ring_max: config.signal_history, ring_budget: config.signal_bytes, state, worker: Some(worker), clock, #[cfg(test)] account:Default::default() }
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
    #[cfg(test)]
    pub(crate) fn memory_reservation_bytes(&self) -> usize {
        self.account.get().map_or(0,|account|account.usage.bytes.load(std::sync::atomic::Ordering::Acquire))
    }
    #[cfg(test)]
    pub(crate) fn memory_bytes(&self) -> usize {
        let s = self.state.0.lock().unwrap();
        s.ring_bytes + s.queue_bytes
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
    crate::transport::local_pipe::is_valid_local_pipe_name(s, true)
}
fn wire_size(c: &Change, values: bool) -> usize {
    57 + c.ns.len() + c.key.len() + if values { 4 + c.value.bytes.len() } else { 0 }
}
fn deliver(
    endpoint: &str,
    payload: &[u8],
    timeout: u32,
    connection: &mut Option<Connection>,
    clock: &crate::chunks::Clock,
) -> io::Result<()> {
    crate::transport::local_pipe::deliver_signal_pipe(endpoint, payload, timeout, connection, clock)
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


    struct Funding {
        owner: Arc<Mutex<crate::store::State>>,
        cache: crate::cache::Cache,
        released: Arc<std::sync::atomic::AtomicBool>,
        bytes: usize,
    }
    impl Funding {
        fn new(config: &Config, bytes: usize) -> Self {
            let cache = crate::cache::Cache::new(config, crate::chunks::Clock::new(None));
            cache.trim(bytes);
            Self { owner: Arc::new(Mutex::new(crate::store::State::default())),
                cache, released: Default::default(), bytes }
        }
        fn within<T>(&self, f: impl FnOnce(&crate::subscription_resources::Budget<'_>) -> T) -> T {
            let state = self.owner.lock().unwrap();
            f(&crate::subscription_resources::Budget::new(&self.owner, &state, self.bytes,
                0, 0, 0, 0, Some(&self.cache), &self.released))
        }
        fn owned(&self) -> usize {
            self.owner.lock().unwrap().subscription_usage.bytes.load(std::sync::atomic::Ordering::Acquire)
        }
    }

    #[test]
    fn history_replacement_funds_net_growth_before_cache_admission() {
        use crate::cache::CachePolicy;
        let config = Config { signal_history: 1, signal_bytes: 2048, ..Config::default() };
        let h = Hub::with_sink(7, &config, ok, crate::chunks::Clock::new(None));
        let funding = Funding::new(&config, 16 * 1024);
        funding.owner.lock().unwrap().memory_reserved = funding.bytes - 2048;
        funding.cache.trim(2048);
        let payload = |version, len| Arc::new(Value { version, ack: 1, bytes: vec![42; len], tombstone: false });
        let publish = |v| funding.within(|b| h.publish_locked("n", "k", v, b));
        let first = payload(1, 300);
        let first_owner = Arc::downgrade(&first);
        publish(first);
        let charge = funding.owned();
        assert_eq!(h.memory_bytes(), 558);
        assert_eq!(charge, publication_demand(1, 1, 300, true).unwrap().total().unwrap());
        funding.cache.insert(payload(10, 800), "test".into(), CachePolicy::default(), false, funding.bytes,
            &std::sync::atomic::AtomicBool::new(true));
        let resident = funding.cache.stats().bytes;
        assert!(resident > 2048 - 2 * charge);

        publish(payload(2, 300));
        assert_eq!(funding.owned(), charge);
        assert_eq!(funding.cache.stats().bytes, resident);
        assert!(first_owner.upgrade().is_none());
        assert_eq!(h.state.0.lock().unwrap().floor, 1);

        publish(payload(3, 1100));
        assert_eq!(h.memory_bytes(), 1358);
        assert_eq!(funding.cache.stats().limit, 2048 - funding.owned());
        assert_eq!(funding.cache.stats().bytes, 0);
        publish(payload(4, 2048));
        assert_eq!(funding.owned(), 0);
        assert_eq!(h.memory_bytes(), 0);
        assert_eq!(h.state.0.lock().unwrap().floor, 4);
        funding.owner.lock().unwrap().memory_reserved = 0;
        let register = |start| funding.within(|b| h.subscribe_locked("n", "".into(), r"\\.\pipe\test".into(),
            false, &token(7, start), "membus.change/1", 1024, b));
        assert_eq!(register(3), Err(23));
        assert!(register(4).is_ok());
    }

    #[test]
    fn queued_and_in_flight_deliveries_keep_their_charge_through_eviction_and_drop() {
        use std::{sync::mpsc, time::Instant};
        let config = Config { signal_history: 1, ..Config::default() };
        let (entered, delivery) = mpsc::channel();
        let (complete, proceed) = mpsc::channel();
        let timeout = Duration::from_secs(5);
        let h = Hub::with_sink(7, &config, move |_, frame, _, _, _| {
            entered.send(frame.to_vec()).unwrap();
            proceed.recv_timeout(timeout).map_err(io::Error::other)
        }, crate::chunks::Clock::new(None));
        let funding = Funding::new(&config, 16 * 1024);
        let live = funding.within(|b| h.subscribe_locked("n", "".into(), r"\\.\pipe\test".into(),
            true, &[], "membus.change/1", 1024, b)).unwrap();
        let id = &live[4..20];
        let publish = |v| funding.within(|b| h.publish_locked("n", "k", v, b));
        let initial = funding.owned();
        let charge = 259;
        let first = value(1);
        let first_owner = Arc::downgrade(&first);
        publish(first);
        let frame = delivery.recv_timeout(timeout).unwrap();
        assert_eq!(parse_token(7, &frame[26..42], 23).unwrap(), 1);
        assert_eq!(frame.last(), Some(&42));
        assert_eq!(h.memory_bytes(), 2 * charge);

        publish(value(2));
        let body = publication_demand(1, 1, 1, false).unwrap().total().unwrap();
        let ring = publication_demand(1, 1, 1, true).unwrap().total().unwrap() - body;
        assert_eq!(funding.owned(), initial + 2 * body + ring);
        assert_eq!(h.memory_bytes(), 3 * charge);
        assert!(first_owner.upgrade().is_some());
        let status = h.status("n", id).unwrap();
        assert_eq!(u32::from_le_bytes(status[2..6].try_into().unwrap()), 1);
        assert_eq!(u64::from_le_bytes(status[6..14].try_into().unwrap()), (2 * charge) as u64);

        // Take every remaining byte. The oldest queued value still owns its
        // body after the ring node yields, so the next publication must drop it.
        let available = funding.within(|b| b.available());
        let pressure = funding.within(|b| b.reserve(crate::subscription_resources::SubscriptionResources {
            metadata: available, ..Default::default()
        }, false)).unwrap();
        publish(value(3));
        assert_eq!(&h.status("n", id).unwrap()[..2], &[0, DROPPED]);
        assert_eq!(h.memory_bytes(), charge);
        assert!(first_owner.upgrade().is_some());
        let blocked = funding.owned();
        complete.send(()).unwrap();
        let deadline = Instant::now() + timeout;
        while h.memory_bytes() != 0 && Instant::now() < deadline { thread::yield_now(); }
        assert_eq!(h.memory_bytes(), 0);
        assert!(first_owner.upgrade().is_none());
        assert!(funding.owned() < blocked);
        let status = h.status("n", id).unwrap();
        assert_eq!(&status[..2], &[0, DROPPED]);
        assert_eq!(u64::from_le_bytes(status[14..22].try_into().unwrap()), 1);
        assert_eq!(u64::from_le_bytes(status[30..38].try_into().unwrap()), 2);
        h.unsubscribe("n", id).unwrap();
        drop(pressure);
    }

    #[test]
    fn catch_up_shrinks_store_cache_until_the_last_delivery_owner_releases() {
        use crate::{codec, ManualClock, Mutation, Store};
        use std::{path::PathBuf, sync::mpsc, time::Instant};

        const CAPACITY: usize = 2 * 1024 * 1024;
        let path = PathBuf::from(std::env::var_os("MEMBUS_TEST_OUTPUT").unwrap_or_else(|| ".test-output".into()))
            .join(format!("signal-catch-up-{}-{}", std::process::id(), crate::now_ms()));
        let mut store = Store::open(&path, Config {
            capacity: CAPACITY, cache_bytes: CAPACITY, signal_history: 1,
            tick_ms: 3_600_000, retire_ms: 3_600_000,
            clock: Some(ManualClock::new(1_000_000)), ..Config::default()
        }).unwrap();
        let (entered, delivery) = mpsc::channel();
        let (complete, proceed) = mpsc::channel();
        let timeout = Duration::from_secs(5);
        store.signals = Hub::with_sink(store.epoch, &store.config, move |_, _, _, _, _| {
            entered.send(()).unwrap();
            proceed.recv_timeout(timeout).map_err(io::Error::other)
        }, store.clock.clone());
        for (ns, key) in [("ledger", "row"), ("other", "cold")] {
            assert_eq!(store.mutate(Mutation {
                ns: ns.into(), key: key.into(), op_id: key.into(), deadline: 2_000_000,
                ack: 1, expected: None, bytes: vec![42; 4096], remove: false,
            }).code, 0);
        }
        // Put the catch-up row last, so it is the single retained history row.
        assert_eq!(store.mutate(Mutation {
            ns: "ledger".into(), key: "row".into(), op_id: "tail".into(), deadline: 2_000_000,
            ack: 1, expected: None, bytes: vec![42; 4096], remove: false,
        }).code, 0);
        let charge = store.signals.memory_bytes();
        assert_eq!(charge, 4361);
        let frame_max = 8192;
        let endpoint = r"\\.\pipe\test";
        let registration = row_demand(6, 0, endpoint.len(), frame_max).unwrap();
        let bytes = registration.total().unwrap();
        let leftover = 1024;
        let available = store.cache_limit(&store.state.lock().unwrap());
        let cohort = store.read_cohort(available - bytes - leftover).unwrap();
        let retained = cohort.try_get("ledger", "row").unwrap().unwrap();
        drop(cohort);
        let subscribe = || {
            let mut request = 2_000_000u64.to_le_bytes().to_vec();
            lp(&mut request, b"subscribe");
            lp(&mut request, b"ledger");
            lp(&mut request, b"");
            lp(&mut request, endpoint.as_bytes());
            request.push(1);
            lp(&mut request, &token(store.epoch, 2));
            lp(&mut request, b"membus.change/1");
            request.extend((frame_max as u32).to_le_bytes());
            store.dispatch(&codec::frame(codec::SUBSCRIBE, 1, &request)[4..])
        };
        let reply_code = |b: &[u8]| u16::from_le_bytes(b[17..19].try_into().unwrap());
        let reply = subscribe();
        assert_eq!(reply_code(&reply), 0);
        let id = &reply[23..39];
        delivery.recv_timeout(timeout).unwrap();
        assert_eq!(reply_code(&subscribe()), 7);
        let blocked = store.health2();
        assert_eq!(blocked.cache.limit, leftover);
        assert!(blocked.memory_bytes <= blocked.memory_budget);
        assert_eq!(store.signals.memory_bytes(), 2 * charge);
        assert_eq!(store.try_get("other", "cold").unwrap().unwrap().bytes[0], 42);
        assert_eq!(store.cache_stats().bytes, 0);
        store.signals.unsubscribe("ledger", id).unwrap();
        assert_eq!(store.signals.memory_bytes(), 2 * charge);

        complete.send(()).unwrap();
        let deadline = Instant::now() + timeout;
        while store.signals.memory_bytes() != charge && Instant::now() < deadline { thread::yield_now(); }
        assert_eq!(store.signals.memory_bytes(), charge);
        assert_eq!(store.cache_stats().limit, leftover);
        assert_eq!(store.try_get("other", "cold").unwrap().unwrap().bytes[0], 42);
        let settled = store.health2();
        assert_eq!(settled.cache.limit, leftover + registration.retained + registration.in_flight + registration.encoding);
        assert!(settled.cache.bytes > 0);
        assert!(settled.memory_bytes <= settled.memory_budget);
        assert_eq!(retained.bytes, vec![42; 4096]);
        assert_eq!(store.signals.status("ledger", id).unwrap()[1], CANCELLED);
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
#[path = "../unit_tests/signals/quiet_compat_tests.rs"]
mod quiet_compat_tests;

#[cfg(test)]
#[path = "../unit_tests/signals/worker_tests.rs"]
mod worker_tests;
