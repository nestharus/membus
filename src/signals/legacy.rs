//! Allocation owners for legacy change/1 using the same Store account.
use super::*;
use crate::subscription_resources::{SubscriptionReservation,SubscriptionResources};
#[cfg(test)]
use crate::subscription_resources::SubscriptionAccount;
use std::{collections::LinkedList,ops::Deref};

pub(super) struct ChangeRef(Option<Arc<Change>>);
impl ChangeRef {pub(super) fn new(change:Change)->Self {Self(Some(Arc::new(change)))}}
impl Clone for ChangeRef {fn clone(&self)->Self {Self(Some(self.0.as_ref().unwrap().clone()))}}
impl Deref for ChangeRef {type Target=Change;fn deref(&self)->&Change {self.0.as_ref().unwrap()}}
impl Drop for ChangeRef {
    fn drop(&mut self) {
        if let Some(Change{ns,key,value,mut allocation,..})=Arc::into_inner(self.0.take().unwrap()) {
            // Arc allocation is already freed; release retained representations
            // before returning their exact managed account ownership.
            drop(ns);drop(key);drop(value);
            if let Some(ref mut grant)=allocation {grant.return_to_owner();}
        }
    }
}
struct LegacySlot {
    endpoint:String,connection:Mutex<Option<Connection>>,allocation:SubscriptionReservation,
}
pub(super) struct LegacySlotRef(Option<Arc<LegacySlot>>);
impl Clone for LegacySlotRef {fn clone(&self)->Self {Self(Some(self.0.as_ref().unwrap().clone()))}}
impl LegacySlotRef {
    pub(super) fn send(&self,sink:&DeliverySink,bytes:&[u8],clock:&crate::chunks::Clock)->io::Result<()> {
        let slot=self.0.as_ref().unwrap();
        sink(&slot.endpoint,bytes,WRITE_MS,&mut slot.connection.lock().unwrap(),clock)
    }
}
impl Drop for LegacySlotRef {
    fn drop(&mut self) {
        if let Some(LegacySlot{endpoint,connection,mut allocation})=Arc::into_inner(self.0.take().unwrap()) {
            drop(connection);drop(endpoint);allocation.return_to_owner();
        }
    }
}
impl Drop for Subscription {
    fn drop(&mut self) {
        // Every normal owner removes the linked node before dropping the row.
        drop(std::mem::take(&mut self.ns));drop(std::mem::take(&mut self.prefix));
        while let Some(change)=self.queue.pop_front(){drop(change);}
        drop(self.slot.take());
        if let Some(mut grant)=self.allocation.take(){grant.return_to_owner();}
    }
}
impl Drop for State {
    fn drop(&mut self) {
        // Explicit pop frees each node before its allocation-owner destructor.
        while let Some(row)=self.subs.pop_front(){drop(row);}
        while let Some(change)=self.ring.pop_front(){drop(change);}
    }
}
pub(super) fn slot_allocation_bytes()->usize {std::mem::size_of::<LegacySlot>()+2*std::mem::size_of::<usize>()}
// Reserve both simultaneous Vec capacities for the selected wire mode.
// Key-only frames carry identifiers and metadata, never a value payload.
fn encoding_demand(frame:usize,values:bool)->Result<usize,u16> {
    let body=frame.min(57+2*MAX_ID+if values {4+crate::MAX_VALUE}else{0});
    body.checked_mul(2).and_then(|n|n.checked_add(4)).ok_or(7u16)
}
pub(super) fn row_demand(ns:usize,prefix:usize,endpoint:usize,frame:usize)->Result<SubscriptionResources,u16> {
    let mut demand=SubscriptionResources::registration(ns,prefix,endpoint,SUB_ITEMS).map_err(|_|7u16)?;
    demand.encoding=encoding_demand(frame,true)?;Ok(demand)
}
pub(super) struct History {
    change: ChangeRef,
    _allocation: SubscriptionReservation,
}
impl Deref for History {
    type Target = Change;
    fn deref(&self) -> &Change { &self.change }
}
fn history_node_demand() -> SubscriptionResources {
    SubscriptionResources {
        retained: 2 * std::mem::size_of::<usize>() + std::mem::size_of::<History>(),
        ..Default::default()
    }
}
pub(super) fn publication_demand(ns:usize,key:usize,value:usize,history:bool)->Result<SubscriptionResources,u16> {
    let mut resources=SubscriptionResources::change(ns,key,value).map_err(|_|7u16)?;
    if history {resources.retained=resources.retained.checked_add(history_node_demand().retained).ok_or(7u16)?;}
    Ok(resources)
}
impl Hub {
    #[cfg(test)]
    pub(crate) fn bind_account(&self,account:SubscriptionAccount) {assert!(self.account.set(account).is_ok(),"one Store account per Hub");}
    #[cfg(test)]
    pub(super) fn capture_account(&self,config:&Config)->Arc<Mutex<crate::store::State>> {
        let owner=Arc::new(Mutex::new(crate::store::State::default()));
        self.bind_account(SubscriptionAccount::for_capture(&owner,config,Arc::new(crate::snapshots::ScanUsage::default()),0));owner
    }
    #[cfg(test)]
    pub fn publish(&self, ns: &str, key: &str, value: Arc<Value>) {
        self.account.get().unwrap().with_budget(|budget| {
            self.publish_locked(ns, key, value, budget)
        }).unwrap();
    }

    /// The caller holds Store State. The worker only returns ownership and
    /// never takes State, so no publication path reacquires a Store lock here.
    pub(crate) fn publish_locked(&self, ns: &str, key: &str, value: Arc<Value>,
        budget: &crate::subscription_resources::Budget<'_>)
    {
        let mut s = self.state.0.lock().unwrap();
        let Some(seq) = s.head.checked_add(1) else {
            let mut freed = 0;
            for row in &mut s.subs {
                row.active = false;
                row.status = DROPPED;
                freed += clear(row);
                drop(row.slot.take());
            }
            s.queue_bytes -= freed;
            return;
        };
        s.head = seq;
        let now = self.clock.mono_ms();
        for row in &mut s.finite {
            if now < row.until && row.ns == ns && key.starts_with(&row.prefix) {
                row.pending = Some(seq);
            }
        }

        let charge = 256 + ns.len() + key.len() + value.bytes.capacity();
        let fits_history = self.ring_max != 0 && charge <= self.ring_budget;
        let live = s.subs.iter().any(|row| row.active && row.ns == ns && key.starts_with(&row.prefix));
        let history_bytes = publication_demand(ns.len(), key.len(), value.bytes.capacity(), true)
            .ok().and_then(|d| d.total().ok()).unwrap_or(usize::MAX);
        // Retire oldest history before funding replacement. A change still
        // owned by the worker or a queue keeps its grant after ring eviction.
        while let Some(old) = s.ring.front() {
            if fits_history && s.ring.len() < self.ring_max
                && s.ring_bytes + charge <= self.ring_budget && history_bytes <= budget.available()
            {
                break;
            }
            let (charge, seq) = (old.charge, old.seq);
            s.ring_bytes -= charge;
            s.floor = seq;
            drop(s.ring.pop_front());
        }
        let retain = fits_history && history_bytes <= budget.available();
        let grant = if retain || live {
            publication_demand(ns.len(), key.len(), value.bytes.capacity(), retain)
                .ok().and_then(|d| budget.reserve(d, true).ok())
        } else {
            None
        };
        // The retained ring node has its own charge, released on eviction even
        // when the shared Change still has another owner.
        let change = grant.map(|mut grant| {
            let history = retain.then(|| grant.split(history_node_demand()).expect("funded history node"));
            let change = ChangeRef::new(Change {
                seq, ns: ns.into(), key: key.into(), value, charge, allocation: Some(grant),
            });
            if let Some(history) = history {
                s.ring_bytes += charge;
                s.ring.push_back(History { change: change.clone(), _allocation: history });
            }
            change
        });
        if !retain || change.is_none() {
            s.floor = seq;
        }
        let mut total = s.queue_bytes;
        for row in &mut s.subs {
            if !row.active || row.ns != ns || !key.starts_with(&row.prefix) {
                continue;
            }
            let eligible = change.as_ref().filter(|change| wire_size(change, row.values) <= row.frame_max
                && row.queue.len() < SUB_ITEMS && row.bytes + charge <= SUB_BYTES
                && total + charge <= QUEUE_BYTES);
            if let Some(change) = eligible {
                row.queue.push_back(change.clone());
                row.bytes += charge;
                total += charge;
                row.status = QUEUED;
            } else {
                row.active = false;
                row.status = DROPPED;
                row.dropped += 1;
                total -= clear(row);
                drop(row.slot.take());
            }
        }
        s.queue_bytes = total;
        self.state.1.notify_one();
    }
    #[cfg(test)]
    pub fn subscribe(&self,ns:&str,prefix:String,endpoint:String,values:bool,cursor:&[u8],protocol:&str,frame_max:usize)->Result<Vec<u8>,u16> {
        self.account.get().unwrap().with_budget(|budget| {
            self.subscribe_locked(ns, prefix, endpoint, values, cursor, protocol, frame_max, budget)
        }).map_err(|_| 7u16)?
    }
    pub(crate) fn subscribe_locked(&self,ns:&str,prefix:String,endpoint:String,values:bool,cursor:&[u8],protocol:&str,frame_max:usize,
        budget: &crate::subscription_resources::Budget<'_>) -> Result<Vec<u8>,u16> {
        if protocol!="membus.change/1" {return Err(2);}
        if !(1024..=crate::MAX_FRAME).contains(&frame_max){return Err(4);}
        if !valid_endpoint(&endpoint) || prefix.len()>MAX_ID || ns.len()>MAX_ID {return Err(3);}
        let mut s = self.state.0.lock().unwrap();
        let start = if cursor.is_empty() { s.head } else { parse_token(self.epoch, cursor, 23)? };
        if start < s.floor || start > s.head { return Err(23); }
        let mut count = 0;
        let mut bytes = 0;
        for change in s.ring.iter().filter(|c| c.seq > start && c.ns == ns && c.key.starts_with(&prefix)) {
            count += 1;
            bytes += change.charge;
            if wire_size(change, values) > frame_max { return Err(4); }
        }
        if count > SUB_ITEMS || bytes > SUB_BYTES || s.queue_bytes + bytes > QUEUE_BYTES {
            return Err(6);
        }
        let mut demand = row_demand(ns.len(), prefix.capacity(), endpoint.capacity(), frame_max)?;
        if !values { demand.encoding = encoding_demand(frame_max, false)?; }
        if s.subs.len() == SUB_MAX {
            let mut removed = false;
            for _ in 0..s.subs.len() {
                let old = s.subs.pop_front().unwrap();
                if !removed && !old.active && old.bytes == 0 {
                    drop(old);
                    removed = true;
                } else {
                    s.subs.push_back(old);
                }
            }
            if !removed {
                return Err(6);
            }
        }
        let mut allocation = budget.reserve(demand, true).map_err(|_| 7u16)?;
        let child = allocation.split(SubscriptionResources {
            in_flight: demand.in_flight, encoding: demand.encoding, ..Default::default()
        }).expect("funded legacy slot");
        let slot = LegacySlotRef(Some(Arc::new(LegacySlot {
            endpoint, connection: Mutex::new(None), allocation: child,
        })));
        let mut row = Subscription {
            id: 0, ns: ns.into(), prefix, slot: Some(slot), values, frame_max, active: true,
            status: WRITTEN, queue: LinkedList::new(), bytes: 0, written: 0, failed: 0,
            dropped: 0, last: 0, allocation: Some(allocation),
        };
        row.id=s.next;s.next=row.id.checked_add(1).ok_or(7u16)?;
        row.last=start;row.bytes=bytes;
        row.queue.extend(s.ring.iter().filter(|c|c.seq>start && c.ns==ns && c.key.starts_with(&row.prefix)).map(|history| history.change.clone()));
        row.status=if row.queue.is_empty(){WRITTEN}else{QUEUED};
        let mut reply=Vec::new();lp(&mut reply,&token(self.epoch,row.id));lp(&mut reply,&token(self.epoch,start));
        s.subs.push_back(row);s.queue_bytes+=bytes;self.state.1.notify_all();Ok(reply)
    }
}

// Fresh authored controls for selected legacy encoding and shared admission.
// No Store directory, native endpoint, historical witness or runtime is opened.
#[cfg(test)]
mod catchup_accounting {
    use super::*;
    use crate::{chunks::{Clock,ManualClock},snapshots::{ScanUsage,ScanCharge}};
    use std::sync::atomic::Ordering;

    struct Gate { entered:bool, release:bool, fail:bool, frames:Vec<Vec<u8>> }
    static SERIAL:Mutex<()>=Mutex::new(());
    static GATE:Mutex<Gate>=Mutex::new(Gate{entered:false,release:false,fail:false,frames:Vec::new()});
    static WAKE:Condvar=Condvar::new();
    fn sink(_: &str,b:&[u8],_:u32,_:&mut Option<Connection>,_:&Clock)->io::Result<()> {
        let mut gate=GATE.lock().unwrap();gate.entered=true;gate.frames.push(b.to_vec());WAKE.notify_all();
        while !gate.release {
            let (next,timeout)=WAKE.wait_timeout(gate,Duration::from_secs(5)).unwrap();gate=next;
            if timeout.timed_out(){return Err(io::Error::other("authored sink gate timed out"));}
        }
        if gate.fail {Err(io::Error::other("authored local write failure"))}else{Ok(())}
    }
    fn reset(fail:bool) {*GATE.lock().unwrap()=Gate{entered:false,release:false,fail,frames:Vec::new()};}
    fn release() {GATE.lock().unwrap().release=true;WAKE.notify_all();}
    fn wait_for(mut f:impl FnMut()->bool)->bool {
        let until=std::time::Instant::now()+Duration::from_secs(3);
        while !f(){if std::time::Instant::now()>=until{return false;}thread::yield_now();}true
    }
    struct Fixture {hub:Hub,owner:Arc<Mutex<crate::store::State>>,scans:Arc<ScanUsage>,config:Config}
    impl Fixture {
        fn new(capacity:usize)->Self {
            let config=Config{capacity,cache_bytes:0,signal_history:8,signal_bytes:8*1024*1024,..Config::default()};
            let clock=Clock::new(Some(ManualClock::new(2_000_000)));
            let hub=Hub::with_sink(73,&config,sink,clock);
            let owner=Arc::new(Mutex::new(crate::store::State::default()));let scans=Arc::new(ScanUsage::default());
            hub.bind_account(SubscriptionAccount::for_capture(&owner,&config,scans.clone(),0));
            Self{hub,owner,scans,config}
        }
        fn free(&self)->usize {self.owner.lock().unwrap().available_memory(self.config.capacity,self.scans.bytes.load(Ordering::Acquire),self.config.memory_margin(),0,0)}
        fn finish(mut self) {
            release();
            assert!(wait_for(||self.hub.state.0.lock().unwrap().queue_bytes==0));
            self.hub.stop_for_capture().unwrap();
            assert_eq!(self.owner.lock().unwrap().subscription_usage.bytes.load(Ordering::Acquire),0);
        }
    }
    fn value(version:u64)->Arc<Value> {Arc::new(Value{version,ack:1,bytes:vec![11;384*1024],tombstone:false})}
    fn subscribe(h:&Hub,values:bool,frame:usize,cursor:&[u8])->Result<Vec<u8>,u16> {
        h.subscribe("scope","event/".into(),r"\\.\pipe\fresh-catchup".into(),values,cursor,"membus.change/1",frame)
    }
    fn token_from(reply:&[u8])->&[u8] {&reply[4..20]}
    fn usage(f:&Fixture)->usize {f.owner.lock().unwrap().subscription_usage.bytes.load(Ordering::Acquire)}

    #[test]
    fn key_only_complete_catchup_admits_under_ordinary_and_snapshot_pressure() {
        let _serial=SERIAL.lock().unwrap();
        for selected_pressure in [false,true] {
            reset(false);let f=Fixture::new(4*1024*1024);
            f.owner.lock().unwrap().memory_reserved=1024*1024;
            // A real ScanCharge retains authored selected-view payload pressure;
            // this does not run or substitute a Store snapshot constructor.
            let pressure=if selected_pressure {
                let bytes=512*1024;
                {let state=f.owner.lock().unwrap();assert!(bytes<=state.available_memory(f.config.capacity,0,f.config.memory_margin(),0,0));
                    f.scans.bytes.store(bytes,Ordering::Release);f.scans.count.store(1,Ordering::Release);}
                Some((vec![5u8;bytes],ScanCharge{usage:f.scans.clone(),bytes}))
            }else{None};
            let held=value(1);f.hub.publish("scope","event/first",held.clone());
            let before=usage(&f);let ordinary=f.owner.lock().unwrap().memory_reserved;
            let result=subscribe(&f.hub,false,crate::MAX_FRAME,&token(73,0));
            let mut admitted=false;let mut completed=false;
            if let Ok(ref reply)=result {
                admitted=wait_for(||GATE.lock().unwrap().entered);
                let status=f.hub.status("scope",token_from(reply)).unwrap();
                assert_eq!(u32::from_le_bytes(status[2..6].try_into().unwrap()),0);
                assert!(u64::from_le_bytes(status[6..14].try_into().unwrap())>0,"a popped replay is still in flight");
                assert!(usage(&f)>before);assert_eq!(f.owner.lock().unwrap().memory_reserved,ordinary);
                assert_eq!(f.scans.bytes.load(Ordering::Acquire),if selected_pressure{512*1024}else{0});
                assert!(f.free()<f.config.capacity);
                f.hub.unsubscribe("scope",token_from(reply)).unwrap();release();
                completed=wait_for(||f.hub.state.0.lock().unwrap().queue_bytes==0);
            }
            if let Some((payload,charge))=pressure {drop(payload);drop(charge);}
            f.finish();
            assert!(result.is_ok(),"key-only replay should not reserve a value encoder: {result:?}");
            assert!(admitted && completed);
            let frames=&GATE.lock().unwrap().frames;assert_eq!(frames.len(),1);
            assert_eq!(frames[0].last(),Some(&0),"key-only body ends with include_value=false after its variable-length identifiers");
            assert!(frames[0].len()<256,"384 KiB history payload was not sent in key-only mode");
        }
    }

    fn measured_registration(values:bool,frame:usize)->usize {
        reset(false);let f=Fixture::new(32*1024*1024);let before=usage(&f);
        let reply=subscribe(&f.hub,values,frame,&[]).unwrap();let delta=usage(&f)-before;
        f.hub.unsubscribe("scope",token_from(&reply)).unwrap();f.finish();delta
    }
    #[test]
    fn catchup_exact_fit_one_over_and_value_mode_preserve_refusal_without_partial_rows() {
        let _serial=SERIAL.lock().unwrap();
        for values in [false,true] {
            let frame=if values {512*1024}else{crate::MAX_FRAME};
            let demand=measured_registration(values,frame);
            for shortage in [0usize,1] {
                reset(false);let f=Fixture::new(4*1024*1024);let held=value(3);
                f.hub.publish("scope","event/first",held.clone());
                let free=f.free();assert!(free>=demand);f.owner.lock().unwrap().memory_reserved=free-demand+shortage;
                let before=usage(&f);let references=Arc::strong_count(&held);
                let result=subscribe(&f.hub,values,frame,&token(73,0));
                let rows=f.hub.state.0.lock().unwrap().subs.len();
                if let Ok(ref reply)=result {f.hub.unsubscribe("scope",token_from(reply)).unwrap();}
                release();assert!(wait_for(||f.hub.state.0.lock().unwrap().queue_bytes==0));
                if shortage==1 {
                    assert_eq!(usage(&f),before);assert_eq!(Arc::strong_count(&held),references);
                    assert!(!GATE.lock().unwrap().entered);assert_eq!(rows,0);
                }
                f.finish();
                if shortage==0 {assert!(result.is_ok());}else{assert_eq!(result,Err(7));}
            }
        }
        // Reserving a possible full value encoder remains a real demand.
        reset(false);let f=Fixture::new(4*1024*1024);
        let result=subscribe(&f.hub,true,crate::MAX_FRAME,&[]);f.finish();assert_eq!(result,Err(7));
    }

    #[test]
    fn maximum_identifier_frames_fit_the_retained_body_and_frame_grant() {
        let _serial=SERIAL.lock().unwrap();
        for values in [false,true] {
            reset(false);let f=Fixture::new(32*1024*1024);
            let ns="n".repeat(MAX_ID);let key="k".repeat(MAX_ID);let held=value(4);
            f.hub.publish(&ns,&key,held);
            let reply=f.hub.subscribe(&ns,"k".into(),r"\\.\pipe\fresh-identifier".into(),values,&token(73,0),"membus.change/1",crate::MAX_FRAME).unwrap();
            assert!(wait_for(||GATE.lock().unwrap().entered));
            let encoding={let state=f.hub.state.0.lock().unwrap();state.subs.front().unwrap().slot.as_ref().unwrap().0.as_ref().unwrap().allocation.resources().encoding};
            let actual_frame=GATE.lock().unwrap().frames[0].len();
            // Actual encoded body and simultaneously built framed vector, not
            // the demand-builder formula, supply the lower-bound oracle.
            let enough=encoding>=actual_frame+(actual_frame-4);
            f.hub.unsubscribe(&ns,token_from(&reply)).unwrap();f.finish();assert!(enough);
        }
    }

    #[test]
    fn two_replays_share_one_change_charge_and_cancel_retains_old_inflight_child() {
        let _serial=SERIAL.lock().unwrap();reset(true);let f=Fixture::new(16*1024*1024);
        let held=value(5);f.hub.publish("scope","event/first",held.clone());
        let original=usage(&f);
        let first=subscribe(&f.hub,true,512*1024,&token(73,0)).unwrap();assert!(wait_for(||GATE.lock().unwrap().entered));
        let one=usage(&f);let second=subscribe(&f.hub,true,512*1024,&token(73,0)).unwrap();let two=usage(&f);
        assert_eq!(one-original,two-one,"second replay should fund its row/slot, not another retained payload");
        f.hub.unsubscribe("scope",token_from(&first)).unwrap();f.hub.unsubscribe("scope",token_from(&second)).unwrap();
        // Evict the first history Change while its old callback still owns it.
        for version in 6..=14 {f.hub.publish("other","fresh",Arc::new(Value{version,ack:1,bytes:Vec::new(),tombstone:false}));}
        let inflight={let s=f.hub.state.0.lock().unwrap();s.queue_bytes};
        let retained=usage(&f);assert!(inflight>0);assert!(Arc::strong_count(&held)>=2);
        let frame=GATE.lock().unwrap().frames[0].len();
        assert!(retained>=frame+(frame-4)+held.bytes.capacity(),"old callback lost the real payload/body/frame funding while blocked");
        release();assert!(wait_for(||f.hub.state.0.lock().unwrap().queue_bytes==0));
        assert!(wait_for(||Arc::strong_count(&held)==1));assert!(usage(&f)<retained);
        f.finish();
    }

    #[test]
    fn full_cursor_fifo_filter_and_refusal_neighbors_remain_distinct() {
        let _serial=SERIAL.lock().unwrap();reset(false);let f=Fixture::new(16*1024*1024);
        f.hub.publish("scope","event/a",value(1));f.hub.publish("other","event/b",value(2));
        f.hub.publish("scope","skip/c",value(3));f.hub.publish("scope","event/d",value(4));
        let baseline=usage(&f);
        assert_eq!(subscribe(&f.hub,false,1024,&token(72,0)),Err(23));
        assert_eq!(subscribe(&f.hub,false,1024,&token(73,5)),Err(23));
        assert_eq!(f.hub.subscribe("scope","event/".into(),r"\\remote\pipe\bad".into(),false,&[],"membus.change/1",1024),Err(3));
        assert_eq!(f.hub.subscribe("scope","event/".into(),r"\\.\pipe\ok".into(),false,&[],"wrong/1",1024),Err(2));
        assert_eq!(subscribe(&f.hub,true,1024,&token(73,0)),Err(4));
        assert_eq!(usage(&f),baseline);assert!(f.hub.state.0.lock().unwrap().subs.is_empty());
        let reply=subscribe(&f.hub,false,1024,&token(73,0)).unwrap();
        assert_eq!(&reply[24..40],&token(73,0));release();
        assert!(wait_for(||f.hub.status("scope",token_from(&reply)).unwrap()[1]==WRITTEN));
        let frames=GATE.lock().unwrap().frames.clone();
        let sequences:Vec<_>=frames.iter().map(|b|u64::from_le_bytes(b[34..42].try_into().unwrap())).collect();
        f.hub.unsubscribe("scope",token_from(&reply)).unwrap();f.finish();assert_eq!(sequences,vec![1,4]);
    }
}
