//! Existing Hub's coalesced finite owners. No replay queue or delivery receipt.
use super::*;
use crate::subscription_contract::{HintOptions,HintBinding,HintReceipt,FINITE_DISCOVERY};
use crate::subscription_resources::{SubscriptionReservation,SubscriptionResources};
use std::collections::LinkedList;

// Allocations precede the charge's destruction. A worker retains this Arc until
// endpoint I/O and connection release complete, even after its row is detached.
pub(super) struct HintSlot { pub(super) endpoint:String, _charge:SubscriptionReservation }
pub(super) struct FiniteSubscription {
    pub(super) ns:String, pub(super) prefix:String, pub(super) binding:HintBinding, pub(super) until:u64, pub(super) generation:u64,
    lifetime:u64, pub(super) write_ms:u32, pub(super) pending:Option<u64>, pub(super) slot:Arc<HintSlot>,
    _charge:SubscriptionReservation,
}
pub(super) fn demand(ns:usize,prefix:usize,endpoint:usize)->Result<SubscriptionResources,u16> {
    // Two list nodes cover pop/reinsert and detached reclamation. The slot's Arc
    // header, native connection value and fixed 64-byte encode are charged too.
    let metadata=std::mem::size_of::<FiniteSubscription>().checked_mul(2)
        .and_then(|n|n.checked_add(4*std::mem::size_of::<usize>()))
        .and_then(|n|n.checked_add(ns)).and_then(|n|n.checked_add(prefix)).ok_or(7u16)?;
    let in_flight=std::mem::size_of::<HintSlot>().checked_add(2*std::mem::size_of::<usize>())
        .and_then(|n|n.checked_add(endpoint)).and_then(|n|n.checked_add(std::mem::size_of::<Option<Connection>>())).ok_or(7u16)?;
    Ok(SubscriptionResources{metadata,in_flight,encoding:64,..Default::default()})
}
pub(super) fn detach_expired(s:&mut State, now:u64)->LinkedList<FiniteSubscription> {
    let mut retired=LinkedList::new();
    if s.finite_expiry.is_none_or(|until|now<until) {return retired;}
    s.finite_expiry=None;
    for _ in 0..s.finite.len() {
        #[cfg(test)] {s.worker_visits.0+=1;}
        let row=s.finite.pop_front().unwrap();
        if now>=row.until {retired.push_back(row);}else{
            s.finite_expiry=Some(s.finite_expiry.map_or(row.until,|until|until.min(row.until)));
            s.finite.push_back(row);
        }
    }
    retired
}
pub(super) fn settle_slot(slot:Arc<HintSlot>) {
    if let Some(HintSlot{endpoint,mut _charge})=Arc::into_inner(slot) {
        drop(endpoint);_charge.return_to_owner();
    }
}
pub(super) fn settle_rows(mut rows:LinkedList<FiniteSubscription>) {
    while let Some(row)=rows.pop_front() {
        let FiniteSubscription{ns,prefix,slot,mut _charge,..}=row;
        drop(ns);drop(prefix);settle_slot(slot);_charge.return_to_owner();
    }
}
impl Hub {
    pub(crate) fn finite_discovery(&self)->&'static [u8] {FINITE_DISCOVERY}
    // The caller supplies a real Store token. This internal seam neither creates
    // authority from a number nor invokes State while holding the Hub mutex.
    pub(super) fn install_hint(&self,ns:&str,prefix:&str,endpoint:&str,options:HintOptions,mut grant:SubscriptionReservation)->Result<HintReceipt,u16> {
        let checked=(|| {options.validate()?;
            if ns.is_empty() || ns.len()>MAX_ID || prefix.len()>MAX_ID || !valid_endpoint(endpoint) {return Err(3);}
            let required=demand(ns.len(),prefix.len(),endpoint.len())?;
            if grant.resources()!=required {return Err(7);}
            let until=self.clock.mono_ms().checked_add(options.lifetime_ms).ok_or(3u16)?;
            Ok((required,until))})();
        let (required,until)=match checked {Ok(x)=>x,Err(e)=>{grant.return_to_owner();return Err(e);}};
        let slot_charge=grant.split(SubscriptionResources{in_flight:required.in_flight,encoding:64,..Default::default()}).map_err(|_|7u16)?;
        let slot=Arc::new(HintSlot{endpoint:endpoint.into(),_charge:slot_charge});
        let mut row=FiniteSubscription{ns:ns.into(),prefix:prefix.into(),binding:HintBinding{epoch:self.epoch,id:0,incarnation:options.incarnation,recipient:options.recipient},until,generation:1,lifetime:options.lifetime_ms,write_ms:options.write_ms,pending:None,slot,_charge:grant};
        let mut s=self.state.0.lock().unwrap();
        let retired=detach_expired(&mut s,self.clock.mono_ms());
        let result=if self.clock.mono_ms()>=until {Err(5)}else if s.stop {Err(2)}else if let Some(next)=s.next.checked_add(1) {
            row.binding.id=s.next;s.next=next;
            let receipt=HintReceipt{binding:row.binding,authority_until_ms:row.until,generation:1};
            s.finite_expiry=Some(s.finite_expiry.map_or(row.until,|until|until.min(row.until)));
            s.finite.push_back(row);self.state.1.notify_all();drop(s);settle_rows(retired);return Ok(receipt);
        }else{Err(7)};
        drop(s);settle_rows(retired);{let mut retired=LinkedList::new();retired.push_back(row);settle_rows(retired);}result
    }
    pub(crate) fn control_hint(&self,ns:&str,receipt:HintReceipt,cancel:bool)->Result<HintReceipt,u16> {
        let mut s=self.state.0.lock().unwrap();let now=self.clock.mono_ms();
        let mut retired=detach_expired(&mut s,now);
        let mut result=Err(24u16);
        for _ in 0..s.finite.len() {
            let mut row=s.finite.pop_front().unwrap();
            if row.ns==ns && row.binding==receipt.binding && row.generation==receipt.generation && row.until==receipt.authority_until_ms {
                if cancel {result=Ok(receipt);retired.push_back(row);continue;}
                if let (Some(until),Some(generation))=(now.checked_add(row.lifetime),row.generation.checked_add(1)) {
                    row.until=until;row.generation=generation;result=Ok(HintReceipt{binding:row.binding,authority_until_ms:until,generation});
                }else{result=Err(3);}
            }
            s.finite.push_back(row);
        }
        self.state.1.notify_all();drop(s);settle_rows(retired);result
    }
}
impl crate::Store {
    pub fn subscribe_finite_hints(&self,ns:&str,prefix:&str,endpoint:&str,options:HintOptions)->Result<HintReceipt,u16> {
        options.validate()?;
        if ns.is_empty() || ns.len()>MAX_ID || prefix.len()>MAX_ID || !valid_endpoint(endpoint) {return Err(3);}
        self.clock.mono_ms().checked_add(options.lifetime_ms).ok_or(3u16)?;
        let grant=self.reserve_subscription_resources(demand(ns.len(),prefix.len(),endpoint.len())?).map_err(|_|7u16)?;
        self.signals.install_hint(ns,prefix,endpoint,options,grant)
    }
    pub fn renew_finite_hints(&self,ns:&str,receipt:HintReceipt)->Result<HintReceipt,u16> {self.signals.control_hint(ns,receipt,false)}
    pub fn cancel_finite_hints(&self,ns:&str,receipt:HintReceipt)->Result<(),u16> {self.signals.control_hint(ns,receipt,true).map(|_|())}
}

#[cfg(test)]
#[path="finite_tests.rs"]
mod tests;
