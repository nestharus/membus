//! Allocation owners for legacy change/1 using the same Store account.
use super::*;
use crate::subscription_resources::{SubscriptionAccount,SubscriptionReservation,SubscriptionResources};
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
    pub(super) fn send(&self,sink:Sink,bytes:&[u8],clock:&crate::chunks::Clock)->io::Result<()> {
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
pub(super) fn row_demand(ns:usize,prefix:usize,endpoint:usize,frame:usize)->Result<SubscriptionResources,u16> {
    let mut demand=SubscriptionResources::registration(ns,prefix,endpoint,SUB_ITEMS).map_err(|_|7u16)?;
    let body=frame.min(57+2*MAX_ID+4+crate::MAX_VALUE);
    demand.encoding=body.checked_mul(2).and_then(|n|n.checked_add(4)).ok_or(7u16)?;Ok(demand)
}
pub(super) fn publication_demand(ns:usize,key:usize,value:usize,history:bool)->Result<SubscriptionResources,u16> {
    let mut resources=SubscriptionResources::change(ns,key,value).map_err(|_|7u16)?;
    if history {resources.retained=resources.retained.checked_add(2*std::mem::size_of::<usize>()+std::mem::size_of::<ChangeRef>()).ok_or(7u16)?;}
    Ok(resources)
}
impl Hub {
    pub(crate) fn bind_account(&self,account:SubscriptionAccount) {assert!(self.account.set(account).is_ok(),"one Store account per Hub");}
    #[cfg(test)]
    pub(super) fn capture_account(&self,config:&Config)->Arc<Mutex<crate::store::State>> {
        let owner=Arc::new(Mutex::new(crate::store::State::default()));
        self.bind_account(SubscriptionAccount::for_capture(&owner,config,Arc::new(crate::snapshots::ScanUsage::default()),0));owner
    }
    pub fn publish(&self,ns:&str,key:&str,value:Arc<Value>) {
        let _admission=self.admission.lock().unwrap();
        let history=self.ring_max!=0 && self.ring_budget!=0;
        let needed=history || self.state.0.lock().unwrap().subs.iter().any(|r|r.active && r.ns==ns && key.starts_with(&r.prefix));
        // State is never acquired while Hub is held. The existing production
        // publisher ordering is committed -> State -> chunks/cache -> Hub.
        let grant=if needed {publication_demand(ns.len(),key.len(),value.bytes.capacity(),history).and_then(|d|self.account.get().ok_or(7u16)?.reserve(d,true).map_err(|_|7u16)).ok()}else{None};
        let mut s=self.state.0.lock().unwrap();
        let Some(seq)=s.head.checked_add(1) else {
            let mut freed=0;for row in &mut s.subs {row.active=false;row.status=DROPPED;freed+=clear(row);drop(row.slot.take());}
            s.queue_bytes-=freed;return;
        };
        s.head=seq;
        let now=self.clock.mono_ms();
        for row in &mut s.finite {if now<row.until && row.ns==ns && key.starts_with(&row.prefix) {row.pending=Some(seq);}}
        let Some(grant)=grant else {
            if needed {
                while let Some(old)=s.ring.pop_front(){drop(old);}s.ring_bytes=0;s.floor=seq;
                let mut freed=0;
                for row in &mut s.subs {if row.active && row.ns==ns && key.starts_with(&row.prefix) {
                    row.active=false;row.status=DROPPED;row.dropped+=1;freed+=clear(row);drop(row.slot.take());
                }}s.queue_bytes-=freed;
            }else{s.floor=seq;}
            self.state.1.notify_all();return;
        };
        let charge=256+ns.len()+key.len()+value.bytes.capacity();
        let change=ChangeRef::new(Change{seq,ns:ns.into(),key:key.into(),value,charge,allocation:Some(grant)});
        if history {
            s.ring_bytes+=charge;s.ring.push_back(change.clone());
            while s.ring.len()>self.ring_max || s.ring_bytes>self.ring_budget {
                let old=s.ring.pop_front().unwrap();s.ring_bytes-=old.charge;s.floor=old.seq;drop(old);
            }
        }else{s.floor=seq;}
        let mut total=s.queue_bytes;
        for row in &mut s.subs {if row.active && row.ns==ns && key.starts_with(&row.prefix) {
            if wire_size(&change,row.values)>row.frame_max || row.queue.len()>=SUB_ITEMS || row.bytes+charge>SUB_BYTES || total+charge>QUEUE_BYTES {
                row.active=false;row.status=DROPPED;total-=clear(row);row.dropped+=1;drop(row.slot.take());
            }else{row.queue.push_back(change.clone());row.bytes+=charge;total+=charge;row.status=QUEUED;}
        }}s.queue_bytes=total;self.state.1.notify_all();
    }
    pub fn subscribe(&self,ns:&str,prefix:String,endpoint:String,values:bool,cursor:&[u8],protocol:&str,frame_max:usize)->Result<Vec<u8>,u16> {
        if protocol!="membus.change/1" {return Err(2);}
        if !(1024..=crate::MAX_FRAME).contains(&frame_max){return Err(4);}
        if !valid_endpoint(&endpoint) || prefix.len()>MAX_ID || ns.len()>MAX_ID {return Err(3);}
        let _admission=self.admission.lock().unwrap();
        let demand=row_demand(ns.len(),prefix.capacity(),endpoint.capacity(),frame_max)?;
        let mut allocation=self.account.get().ok_or(7u16)?.reserve(demand,true).map_err(|_|7u16)?;
        let child=allocation.split(SubscriptionResources{in_flight:demand.in_flight,encoding:demand.encoding,..Default::default()}).map_err(|_|7u16)?;
        let slot=LegacySlotRef(Some(Arc::new(LegacySlot{endpoint,connection:Mutex::new(None),allocation:child})));
        let mut row=Subscription{id:0,ns:ns.into(),prefix,slot:Some(slot),values,frame_max,active:true,status:WRITTEN,queue:LinkedList::new(),bytes:0,written:0,failed:0,dropped:0,last:0,allocation:Some(allocation)};
        let mut s=self.state.0.lock().unwrap();
        let start=if cursor.is_empty(){s.head}else{parse_token(self.epoch,cursor,23)?};
        if start<s.floor || start>s.head {return Err(23);}
        let matches=s.ring.iter().filter(|c|c.seq>start && c.ns==ns && c.key.starts_with(&row.prefix));
        let mut count=0;let mut bytes=0;
        for c in matches {count+=1;bytes+=c.charge;if wire_size(c,values)>frame_max{return Err(4);}}
        if count>SUB_ITEMS || bytes>SUB_BYTES || s.queue_bytes+bytes>QUEUE_BYTES {return Err(6);}
        if s.subs.len()==SUB_MAX {
            let mut removed=false;
            for _ in 0..s.subs.len(){let old=s.subs.pop_front().unwrap();if !removed && !old.active && old.bytes==0 {drop(old);removed=true;}else{s.subs.push_back(old);}}
            if !removed{return Err(6);}
        }
        row.id=s.next;s.next=row.id.checked_add(1).ok_or(7u16)?;
        row.last=start;row.bytes=bytes;
        row.queue.extend(s.ring.iter().filter(|c|c.seq>start && c.ns==ns && c.key.starts_with(&row.prefix)).cloned());
        row.status=if row.queue.is_empty(){WRITTEN}else{QUEUED};
        let mut reply=Vec::new();lp(&mut reply,&token(self.epoch,row.id));lp(&mut reply,&token(self.epoch,start));
        s.subs.push_back(row);s.queue_bytes+=bytes;self.state.1.notify_all();Ok(reply)
    }
}
