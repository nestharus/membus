//! Actual Hub/State/reservation/worker with inert callbacks and independent clocks.
use super::*;
use crate::{chunks::{ManualClock,Clock}, subscription_contract::*,subscription_resources::*};
use std::sync::atomic::{AtomicBool};
fn inert(_: &str,_:&[u8],_:u32,_:&mut Option<Connection>,_:&Clock)->io::Result<()> {Ok(())}
fn options()->HintOptions {HintOptions{profile:1,lifetime_ms:40,write_ms:10,incarnation:[7;16],recipient:[9;16]}}
fn grant(owner:&Arc<Mutex<crate::store::State>>,ns:&str,prefix:&str,endpoint:&str,capacity:usize)->Result<SubscriptionReservation,crate::subscription_resources::ReservationError> {
    let mut state=owner.lock().unwrap();let available=state.available_memory(capacity,0,0,0,0);
    crate::subscription_resources::issue(owner,&mut state,available,demand(ns.len(),prefix.len(),endpoint.len()).unwrap(),&Arc::new(AtomicBool::new(false)))
}
fn add(h:&Hub,owner:&Arc<Mutex<crate::store::State>>,opts:HintOptions)->HintReceipt {
    let endpoint=r"\\.\pipe\authored";
    h.install_hint("ns","p",endpoint,opts,grant(owner,"ns","p",endpoint,1_000_000).unwrap()).unwrap()
}
fn wait_for(mut check:impl FnMut()->bool) {let until=std::time::Instant::now()+Duration::from_secs(3);while !check() {assert!(std::time::Instant::now()<until,"worker did not settle");thread::yield_now();}}
fn fixture(sink:Sink)->(Hub,Arc<ManualClock>,Arc<Mutex<crate::store::State>>) {
    let clock=ManualClock::new(900_000);let hub=Hub::with_sink(91,&Config{signal_history:0,signal_bytes:0,..Config::default()},sink,Clock::new(Some(clock.clone())));
    (hub,clock,Arc::new(Mutex::new(crate::store::State::default())))
}
#[test]
fn quiet_expiry_reclaims_more_than_legacy_limit_from_actual_account() {
    let (mut h,clock,owner)=fixture(inert);
    for _ in 0..80 {add(&h,&owner,options());}
    assert_eq!(h.state.0.lock().unwrap().finite.len(),80);
    assert!(owner.lock().unwrap().memory_reserved>80*64);
    clock.jump_utc(i64::MAX);h.state.1.notify_all();
    assert_eq!(h.state.0.lock().unwrap().finite.len(),80);
    clock.advance(39);h.state.1.notify_all();assert_eq!(h.state.0.lock().unwrap().finite.len(),80);
    clock.advance(1);h.state.1.notify_all();
    wait_for(||owner.lock().unwrap().memory_reserved==0);
    assert!(h.state.0.lock().unwrap().finite.is_empty());h.stop_for_capture().unwrap();
}
#[test]
fn renewal_cancel_boundary_and_old_identity_never_touch_replacement() {
    let (mut h,clock,owner)=fixture(inert);let old=add(&h,&owner,options());
    clock.advance(39);let renewed=h.control_hint("ns",old,false).unwrap();
    assert_eq!((renewed.generation,renewed.authority_until_ms),(2,79));
    assert_eq!(h.control_hint("ns",old,true),Err(24));
    let neighbor=add(&h,&owner,options());
    let before=owner.lock().unwrap().memory_reserved;
    let mut wrong=renewed;wrong.binding.recipient=[8;16];assert_eq!(h.control_hint("ns",wrong,true),Err(24));
    wrong=renewed;wrong.binding.incarnation=[8;16];assert_eq!(h.control_hint("ns",wrong,false),Err(24));
    wrong=renewed;wrong.binding.epoch=90;assert_eq!(h.control_hint("ns",wrong,false),Err(24));
    assert_eq!(h.control_hint("other",renewed,true),Err(24));
    assert_eq!(owner.lock().unwrap().memory_reserved,before);
    clock.advance(40);assert_eq!(h.control_hint("ns",renewed,false),Err(24));
    let replacement=add(&h,&owner,options());assert_ne!(replacement.binding.id,old.binding.id);
    assert_eq!(h.control_hint("ns",neighbor,true),Err(24));
    h.control_hint("ns",replacement,true).unwrap();wait_for(||owner.lock().unwrap().memory_reserved==0);h.stop_for_capture().unwrap();
}
#[test]
fn actual_pressure_and_rollback_do_not_spend_neighbor_owners() {
    let (mut h,_,owner)=fixture(inert);owner.lock().unwrap().memory_reserved=17;
    let endpoint=r"\\.\pipe\authored";let required=demand(2,1,endpoint.len()).unwrap().total().unwrap();
    assert!(matches!(grant(&owner,"ns","p",endpoint,required+16),Err(ReservationError::Capacity)));
    assert_eq!(owner.lock().unwrap().memory_reserved,17);
    let g=grant(&owner,"ns","p",endpoint,required+17).unwrap();
    let mut bad=options();bad.profile=2;assert_eq!(h.install_hint("ns","p",endpoint,bad,g),Err(2));
    assert_eq!(owner.lock().unwrap().memory_reserved,17);
    owner.lock().unwrap().memory_reserved=0;h.stop_for_capture().unwrap();
}
static BLOCK:Mutex<(bool,bool)>=Mutex::new((false,false));
static WAKE:Condvar=Condvar::new();
fn blocked(_: &str,b:&[u8],_:u32,_:&mut Option<Connection>,_:&Clock)->io::Result<()> {
    assert_eq!(&b[4..8],b"MBH2");let mut flags=BLOCK.lock().unwrap();flags.0=true;WAKE.notify_all();
    while !flags.1 {flags=WAKE.wait(flags).unwrap();}Err(io::Error::other("authored old failure"))
}
#[test]
fn in_flight_quiet_expiry_and_late_failure_keep_replacement_charge() {
    *BLOCK.lock().unwrap()=(false,false);
    let (mut h,clock,owner)=fixture(blocked);let first=add(&h,&owner,options());
    h.publish("ns","p/key",Arc::new(Value{version:1,ack:1,bytes:vec![],tombstone:false}));
    {let mut flags=BLOCK.lock().unwrap();while !flags.0 {flags=WAKE.wait(flags).unwrap();}}
    clock.advance(40);assert_eq!(h.control_hint("ns",first,false),Err(24));
    let retained=owner.lock().unwrap().memory_reserved;
    assert!(retained>=64,"in-flight owner was returned early");
    let replacement=add(&h,&owner,options());let total=owner.lock().unwrap().memory_reserved;
    {BLOCK.lock().unwrap().1=true;WAKE.notify_all();}
    wait_for(||owner.lock().unwrap().memory_reserved<total);
    assert_eq!(h.state.0.lock().unwrap().finite.len(),1);
    assert!(h.control_hint("ns",replacement,false).is_ok());
    let current={let s=h.state.0.lock().unwrap();let r=s.finite.front().unwrap();HintReceipt{binding:r.binding,authority_until_ms:r.until,generation:r.generation}};
    h.control_hint("ns",current,true).unwrap();wait_for(||owner.lock().unwrap().memory_reserved==0);h.stop_for_capture().unwrap();
}
#[test]
fn exact_wire_profile_discovery_and_receiver_binding() {
    let (mut h,_,_)=fixture(inert);let mut p=h.hello_limits();let original=p.clone();
    crate::codec::append_subscription_contract_discovery("r;subscription-contract=2",&h,&mut p);
    assert_eq!(original.len(),46);assert_eq!(&p[46..],b"MSH2\x01\x00\x01\x00");
    for profile in [0,2,3,65535] {let mut o=options();o.profile=profile;assert_eq!(o.validate(),Err(2));}
    let binding=HintBinding{epoch:91,id:1,incarnation:[7;16],recipient:[9;16]};
    let mut frame=[0;64];frame[..4].copy_from_slice(&60u32.to_le_bytes());frame[4..8].copy_from_slice(b"MBH2");frame[8..56].copy_from_slice(&binding.bytes());frame[56..].copy_from_slice(&123u64.to_le_bytes());
    assert_eq!(decode_hint(&frame,binding),Ok(123));let mut replacement=binding;replacement.recipient=[8;16];assert_eq!(decode_hint(&frame,replacement),Err(3));h.stop_for_capture().unwrap();
}
