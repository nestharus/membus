//! Pure actual-State controls. No Store/Hub construction or operating fixture.
use super::*;
use crate::snapshots::ScanUsage;
use std::sync::Barrier;
use std::thread;

fn demand(n: usize) -> SubscriptionResources { SubscriptionResources { retained: n, ..Default::default() } }
fn state() -> Arc<Mutex<State>> { Arc::new(Mutex::new(State::default())) }
fn flag() -> Arc<AtomicBool> { Arc::new(AtomicBool::new(false)) }

#[test]
fn actual_account_all_owners_boundary_and_once_only_rollback() {
    let owner=state(); let released=flag(); let snapshots=ScanUsage::default();
    snapshots.bytes.store(19,Ordering::Release);
    let mut s=owner.lock().unwrap();
    s.committed_bytes=13; s.dedup_bytes=17; s.memory_reserved=11;
    // Literal independently selected charges sum to 143, leaving exactly 57.
    let available=s.available_memory(200,snapshots.bytes.load(Ordering::Acquire),23,29,31);
    assert_eq!(available,57);
    assert!(matches!(issue(&owner,&s,available,demand(58),&released),Err(ReservationError::Capacity)));
    assert_eq!(s.total_reserved(),11);
    let mut grant=issue(&owner,&s,available,demand(57),&released).unwrap();
    assert_eq!(s.total_reserved(),68);
    assert_eq!(s.available_memory(200,19,23,29,31),0);
    drop(s);
    grant.return_to_owner(); grant.return_to_owner();
    let s=owner.lock().unwrap();
    assert_eq!((s.committed_bytes,s.dedup_bytes,s.total_reserved()),(13,17,11));
    assert_eq!(snapshots.bytes.load(Ordering::Acquire),19);
    assert!(released.load(Ordering::Acquire));
    assert_eq!(s.available_memory(200,19,23,29,31),57);
}

#[test]
fn transfer_split_cancel_old_callback_preserve_replacement_and_pending() {
    let owner=state();let released=flag();
    owner.lock().unwrap().memory_reserved=7; // unrelated pending owner
    let original=SubscriptionResources { metadata:10,retained:20,in_flight:0,encoding:30,publication:40 };
    let mut registration={let s=owner.lock().unwrap();issue(&owner,&s,193,original,&released).unwrap()};
    registration.transfer(SubscriptionResource::Retained,SubscriptionResource::InFlight,12).unwrap();
    let mut callback=registration.split(SubscriptionResources {in_flight:12,encoding:30,..Default::default()}).unwrap();
    assert_eq!(owner.lock().unwrap().total_reserved(),107);
    registration.return_to_owner();
    assert_eq!(owner.lock().unwrap().total_reserved(),49);
    let mut replacement={let s=owner.lock().unwrap();issue(&owner,&s,151,demand(100),&released).unwrap()};
    callback.return_to_owner();callback.return_to_owner();
    assert_eq!(owner.lock().unwrap().total_reserved(),107);
    assert_eq!(replacement.resources().retained,100);
    replacement.return_to_owner();
    assert_eq!(owner.lock().unwrap().total_reserved(),7);
}

#[test]
fn growth_rechecks_current_pressure_and_is_atomic_on_failure() {
    let owner=state();let released=flag();
    let mut grant={let s=owner.lock().unwrap();issue(&owner,&s,100,demand(40),&released).unwrap()};
    let mut s=owner.lock().unwrap();
    s.memory_reserved+=50; // another admitted ordinary owner, same real account
    let available=s.available_memory(100,0,0,0,0);
    assert_eq!(available,10);
    assert_eq!(extend(&owner,&s,available,&mut grant,demand(11)),Err(ReservationError::Capacity));
    assert_eq!((s.total_reserved(),grant.resources().retained),(90,40));
    extend(&owner,&s,available,&mut grant,demand(10)).unwrap();
    assert_eq!((s.total_reserved(),grant.resources().retained),(100,50));
    s.memory_reserved-=50;drop(s);
    grant.return_to_owner();assert_eq!(owner.lock().unwrap().total_reserved(),0);
}

#[test]
fn zero_overflow_foreign_and_returned_never_debit() {
    let owner=state();let foreign=state();let released=flag();
    let s=owner.lock().unwrap();
    assert!(matches!(issue(&owner,&s,100,demand(0),&released),Err(ReservationError::InvalidDemand)));
    let overflow=SubscriptionResources {metadata:usize::MAX,encoding:1,..Default::default()};
    assert!(matches!(issue(&owner,&s,usize::MAX,overflow,&released),Err(ReservationError::InvalidDemand)));
    assert_eq!(s.total_reserved(),0);
    let mut grant=issue(&owner,&s,100,demand(10),&released).unwrap();drop(s);
    {let f=foreign.lock().unwrap();
    assert_eq!(extend(&foreign,&f,100,&mut grant,demand(5)),Err(ReservationError::ForeignOwner));
    assert_eq!(f.total_reserved(),0);}
    grant.return_to_owner();
    {let s=owner.lock().unwrap();
    assert_eq!(extend(&owner,&s,100,&mut grant,demand(5)),Err(ReservationError::Returned));
    assert_eq!(s.total_reserved(),0);}
    assert!(matches!(grant.split(demand(1)),Err(ReservationError::Returned)));
}

#[test]
fn invalid_part_operations_leave_every_original_part_owned() {
    let owner=state();let released=flag();
    let mut grant={let s=owner.lock().unwrap();issue(&owner,&s,100,demand(25),&released).unwrap()};
    let original=grant.resources();
    assert!(matches!(grant.split(demand(26)),Err(ReservationError::InsufficientPart)));
    assert!(matches!(grant.split(demand(0)),Err(ReservationError::InvalidDemand)));
    assert_eq!(grant.transfer(SubscriptionResource::Retained,SubscriptionResource::InFlight,26),Err(ReservationError::InsufficientPart));
    assert_eq!(grant.resources(),original);
    grant.transfer(SubscriptionResource::Retained,SubscriptionResource::Retained,25).unwrap();
    assert_eq!(grant.resources(),original);
    grant.return_to_owner();
}

#[test]
fn simultaneous_actual_account_grants_cannot_spend_same_capacity() {
    let owner=state();let released=flag();let start=Arc::new(Barrier::new(3));
    // Both contenders ask for 60 of one 100-byte actual State account.
    let jobs:Vec<_>=(0..2).map(|_| {let owner=owner.clone();let released=released.clone();let start=start.clone();
        thread::spawn(move || {start.wait();let s=owner.lock().unwrap();
            let available=s.available_memory(100,0,0,0,0);
            issue(&owner,&s,available,demand(60),&released)
        })}).collect();
    start.wait();
    let results:Vec<_>=jobs.into_iter().map(|j|j.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r|r.is_ok()).count(),1);
    assert_eq!(results.iter().filter(|r|matches!(r,Err(ReservationError::Capacity))).count(),1);
    assert_eq!(owner.lock().unwrap().total_reserved(),60);
    for mut grant in results.into_iter().flatten() {grant.return_to_owner();}
    assert_eq!(owner.lock().unwrap().total_reserved(),0);
}

#[test]
fn cancellation_and_exact_old_completions_race_without_neighbor_loss() {
    let owner=state();let released=flag();
    let mut parent={let s=owner.lock().unwrap();issue(&owner,&s,100,demand(90),&released).unwrap()};
    let a=parent.split(demand(30)).unwrap();let b=parent.split(demand(20)).unwrap();
    let start=Arc::new(Barrier::new(4));
    let handles:Vec<_>=[parent,a,b].into_iter().map(|mut grant| {let start=start.clone();
        thread::spawn(move || {start.wait();grant.return_to_owner();grant.return_to_owner();})}).collect();
    let mut neighbor={let s=owner.lock().unwrap();issue(&owner,&s,10,demand(10),&released).unwrap()};
    start.wait();for h in handles {h.join().unwrap();}
    assert_eq!(owner.lock().unwrap().total_reserved(),10);
    assert_eq!(neighbor.resources().retained,10);neighbor.return_to_owner();
}

#[test]
fn retained_snapshot_pressure_and_overflow_are_fail_closed() {
    let s=State::default();let snapshots=ScanUsage::default();snapshots.bytes.store(99,Ordering::Release);
    // Registry removal cannot lower this retained owner count by assertion.
    assert_eq!(s.available_memory(100,snapshots.bytes.load(Ordering::Acquire),0,0,0),1);
    assert_eq!(s.available_memory(100,101,0,0,0),0);
    assert_eq!(s.available_memory(usize::MAX,usize::MAX,1,0,0),0);
}

#[test]
fn old_envelope_is_diagnostic_not_quiet_admission_ownership() {
    // Independent lower bound: 16 MiB history + 16 MiB queues + 64 MiB
    // simultaneous encoder buffers + a 4 MiB prospective publication.
    let normal=crate::signals::legacy_memory_reservation(4096,16*1024*1024);
    assert!(normal>100*1024*1024);
    let smaller=crate::signals::legacy_memory_reservation(4096,15*1024*1024);
    assert_eq!(normal-smaller,1024*1024);
    let quiet=crate::signals::legacy_memory_reservation(0,0);
    assert!(quiet>84*1024*1024);
    let s=State::default();assert_eq!(s.available_memory(quiet-1,0,0,0,quiet),0);
    assert_eq!(s.available_memory(quiet+73,0,0,0,quiet),73);
    assert_eq!(crate::signals::legacy_memory_reservation(usize::MAX,usize::MAX),usize::MAX);
}

#[test]
fn debit_reserves_in_actual_shared_pending_account() {
    let s=State { memory_reserved: 7, ..State::default() };
    debit(&s,93,demand(60)).unwrap();
    assert_eq!(s.total_reserved(),67);
}

#[test]
fn owner_provenance_is_identity_not_equal_balances() {
    let a=state();let b=state();
    assert_eq!(check_owner(&a,&a),Ok(()));
    assert_eq!(check_owner(&a,&b),Err(ReservationError::ForeignOwner));
}
