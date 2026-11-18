use super::*;
fn inert(_: &str,_:&[u8],_:u32,_:&mut Option<Connection>,_:&crate::chunks::Clock)->io::Result<()> {Ok(())}
#[test]
fn quiet_16m_ordinary_admission_precedes_any_subscription() {
    let config=Config{capacity:16*1024*1024,signal_history:0,signal_bytes:0,..Config::default()};
    let mut hub=Hub::with_sink(7,&config,inert,crate::chunks::Clock::new(None));
    let state=crate::store::State::default();
    let allowance=state.available_memory(config.capacity,0,config.memory_margin(),0,hub.memory_reservation_bytes());
    hub.stop_for_capture().unwrap();
    // Literal original 16 MiB capacity less its existing 2 MiB margin.
    assert_eq!(allowance,14*1024*1024);
    assert!(allowance>=8192,"an otherwise eligible small ordinary write is starved");
}

use crate::subscription_resources::{SubscriptionAccount,SubscriptionResources};
use crate::subscription_contract::*;
use crate::chunks::{Clock,ManualClock};
fn configured(history:usize,sink:Sink)->(Hub,Arc<Mutex<crate::store::State>>,Arc<ManualClock>) {
    let config=Config{capacity:16*1024*1024,signal_history:history,signal_bytes:if history==0{0}else{2*1024*1024},..Config::default()};
    let clock=ManualClock::new(1000);let h=Hub::with_sink(7,&config,sink,Clock::new(Some(clock.clone())));
    let owner=Arc::new(Mutex::new(crate::store::State::default()));
    h.bind_account(SubscriptionAccount::for_capture(&owner,&config,Arc::new(crate::snapshots::ScanUsage::default()),0));(h,owner,clock)
}
fn options()->HintOptions {HintOptions{profile:1,lifetime_ms:100,write_ms:20,incarnation:[3;16],recipient:[4;16]}}
fn add(h:&Hub)->Result<HintReceipt,u16> {
    let endpoint=r"\\.\pipe\authored";
    let grant=h.account.get().unwrap().reserve(finite::demand(2,1,endpoint.len())?,false).map_err(|_|7u16)?;
    h.install_hint("ns","p",endpoint,options(),grant)
}
fn allowance(owner:&Arc<Mutex<crate::store::State>>)->usize {owner.lock().unwrap().available_memory(16*1024*1024,0,2*1024*1024,0,0)}
fn wait_for(mut f:impl FnMut()->bool) {let until=std::time::Instant::now()+Duration::from_secs(3);while !f(){assert!(std::time::Instant::now()<until,"finite capture did not settle");thread::yield_now();}}
#[test]
fn small_real_account_funds_more_than_64_and_expiry_restores_ordinary_allowance() {
    let (mut h,owner,clock)=configured(0,inert);assert_eq!(allowance(&owner),14*1024*1024);
    for _ in 0..96 {add(&h).unwrap();}
    let owned=owner.lock().unwrap().subscription_usage.bytes.load(std::sync::atomic::Ordering::Acquire);
    assert!(owned>96*64);assert_eq!(allowance(&owner)+owned,14*1024*1024);
    clock.advance(100);h.state.1.notify_all();wait_for(||allowance(&owner)==14*1024*1024);
    h.stop_for_capture().unwrap();
}
#[test]
fn quiet_publication_allocates_no_legacy_payload_and_small_pressure_refuses() {
    let (mut h,owner,_)=configured(0,inert);
    h.publish("ns","p/key",Arc::new(Value{version:1,ack:1,bytes:vec![1;8192],tombstone:false}));
    assert_eq!(allowance(&owner),14*1024*1024);
    owner.lock().unwrap().memory_reserved=14*1024*1024-1;
    let result=add(&h);let remaining=allowance(&owner);h.stop_for_capture().unwrap();
    assert_eq!(result,Err(7));assert_eq!(remaining,1);
}
#[test]
fn subscription_return_preserves_retry_atomic_snapshot_and_external_read_ownership() {
    let (mut h,owner,_)=configured(0,inert);
    {let mut s=owner.lock().unwrap();s.committed_bytes=11;s.dedup_bytes=17;s.memory_reserved=23;}
    let scan=Arc::new(crate::snapshots::ScanUsage::default());scan.bytes.store(31,std::sync::atomic::Ordering::Release);
    let account=SubscriptionAccount::for_capture(&owner,&Config{capacity:16*1024*1024,..Config::default()},scan.clone(),37);
    let before=owner.lock().unwrap().available_memory(16*1024*1024,31,2*1024*1024,37,41);
    assert_eq!(before,14*1024*1024-160);
    let mut grant=account.reserve(SubscriptionResources::encoding(4096,4096).unwrap(),false).unwrap();
    let during=owner.lock().unwrap().available_memory(16*1024*1024,31,2*1024*1024,37,41);
    assert_eq!(before-during,8192);grant.return_to_owner();grant.return_to_owner();
    let s=owner.lock().unwrap();let after=s.available_memory(16*1024*1024,31,2*1024*1024,37,41);let ordinary=(s.committed_bytes,s.dedup_bytes,s.memory_reserved);drop(s);
    h.stop_for_capture().unwrap();assert_eq!(after,before);assert_eq!(ordinary,(11,17,23));assert_eq!(scan.bytes.load(std::sync::atomic::Ordering::Acquire),31);
}
#[test]
fn cancellation_returns_without_reacquiring_held_state() {
    let (h,owner,_)=configured(0,inert);let receipt=add(&h).unwrap();let h=Arc::new(h);
    let state=owner.lock().unwrap();let (tx,rx)=std::sync::mpsc::channel();
    let thread={let h=h.clone();thread::spawn(move||{let result=h.control_hint("ns",receipt,true);tx.send(result).unwrap();})};
    let before_unlock=rx.recv_timeout(Duration::from_secs(1));drop(state);thread.join().unwrap();
    let mut h=Arc::try_unwrap(h).ok().unwrap();h.stop_for_capture().unwrap();assert!(matches!(before_unlock,Ok(Ok(_))));assert_eq!(allowance(&owner),14*1024*1024);
}
static SERIAL:Mutex<()>=Mutex::new(());
static GATE:Mutex<(bool,bool)>=Mutex::new((false,false));
static WAKE:Condvar=Condvar::new();
fn blocked(_: &str,_:&[u8],_:u32,_:&mut Option<Connection>,_:&Clock)->io::Result<()> {
    let mut state=GATE.lock().unwrap();state.0=true;WAKE.notify_all();while !state.1 {state=WAKE.wait(state).unwrap();}Ok(())
}
fn entered(){let mut s=GATE.lock().unwrap();while !s.0 {s=WAKE.wait(s).unwrap();}}
fn release(){GATE.lock().unwrap().1=true;WAKE.notify_all();}
#[test]
fn finite_in_flight_expiry_retains_charge_to_actual_completion() {
    let _serial=SERIAL.lock().unwrap();*GATE.lock().unwrap()=(false,false);
    let (mut h,owner,clock)=configured(0,blocked);let receipt=add(&h).unwrap();
    h.publish("ns","p/key",Arc::new(Value{version:1,ack:1,bytes:vec![],tombstone:false}));entered();
    clock.advance(100);let expired=h.control_hint("ns",receipt,false);
    let retained=owner.lock().unwrap().subscription_usage.bytes.load(std::sync::atomic::Ordering::Acquire);
    release();wait_for(||allowance(&owner)==14*1024*1024);h.stop_for_capture().unwrap();
    assert_eq!(expired,Err(24));assert!(retained>=64,"in-flight encoding/endpoint allocation lost its funding");
}
#[test]
fn legacy_payload_queue_encoding_and_in_flight_have_real_ownership() {
    let _serial=SERIAL.lock().unwrap();*GATE.lock().unwrap()=(false,false);
    let (mut h,owner,_)=configured(0,blocked);
    let reply=h.subscribe("ns","p".into(),r"\\.\pipe\authored".into(),true,&[],"membus.change/1",4*1024*1024).unwrap();let token=&reply[4..20];
    h.publish("ns","p/first",Arc::new(Value{version:1,ack:1,bytes:vec![1],tombstone:false}));entered();
    h.publish("ns","p/second",Arc::new(Value{version:2,ack:1,bytes:vec![2;2*1024*1024],tombstone:false}));
    let retained={let s=h.state.0.lock().unwrap();s.subs.front().unwrap().queue.back().unwrap().allocation.as_ref().unwrap().resources().retained};
    h.unsubscribe("ns",token).unwrap();let flight=owner.lock().unwrap().subscription_usage.bytes.load(std::sync::atomic::Ordering::Acquire);
    release();wait_for(||h.status("ns",token).map(|b|u64::from_le_bytes(b[6..14].try_into().unwrap())==0).unwrap_or(false));
    h.stop_for_capture().unwrap();let remaining=allowance(&owner);
    assert!(retained>=2*1024*1024,"actual queued payload capacity is undercharged");assert!(flight>=8*1024*1024,"negotiated in-flight encoding grant was returned early");assert_eq!(remaining,14*1024*1024);
}
#[test]
fn legacy_history_pressure_is_explicit_gap_while_ordinary_allowance_is_not_a_fixed_blackout() {
    let (mut h,owner,_)=configured(2,inert);
    h.publish("ns","p/one",Arc::new(Value{version:1,ack:1,bytes:vec![1;1024],tombstone:false}));
    let used=owner.lock().unwrap().subscription_usage.bytes.load(std::sync::atomic::Ordering::Acquire);assert!(used>=1024);
    let available=allowance(&owner);owner.lock().unwrap().memory_reserved=available;
    h.publish("ns","p/two",Arc::new(Value{version:2,ack:1,bytes:vec![2;4096],tombstone:false}));
    let gap={let s=h.state.0.lock().unwrap();(s.floor,s.head,s.ring.len())};
    h.stop_for_capture().unwrap();assert_eq!(gap,(2,2,0));
    assert_eq!(owner.lock().unwrap().memory_reserved,available);
}
