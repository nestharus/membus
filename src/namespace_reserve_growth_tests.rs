//! New owned specimens; allocation seams are absent from production.
use super::*;
use crate::{Config, Mutation};
use std::{
    fs,
    path::PathBuf,
    sync::{Condvar, Mutex},
    time::Instant,
};

fn root(label: &str) -> PathBuf {
    let base = PathBuf::from(std::env::var_os("MEMBUS_NAMESPACE_RESERVE_TEST_ROOT").unwrap());
    assert!(base.is_absolute() && base.is_dir());
    let p = base.join(format!(
        "growth-unit-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&p).unwrap();
    println!("retained growth specimen {}", p.display());
    p
}
fn config() -> Config {
    Config {
        capacity: 8 * 1024 * 1024,
        state_limit_bytes: 1024 * 1024,
        cache_bytes: 0,
        signal_bytes: 0,
        flush_ms: 0,
        tick_ms: 3_600_000,
        retire_ms: 3_600_000,
        clock: Some(crate::ManualClock::new(1_000_000)),
        ..Config::default()
    }
}
fn request(bytes: u64) -> ReserveRequest {
    ReserveRequest {
        bytes,
        appends: 4,
        lifetime_ms: 3_600_000,
    }
}
fn mutation(ns: &str, key: &str, id: &str, bytes: usize) -> Mutation {
    Mutation {
        ns: ns.into(),
        key: key.into(),
        op_id: id.into(),
        deadline: u64::MAX,
        ack: 2,
        expected: None,
        bytes: vec![65; bytes],
        remove: false,
    }
}

#[test]
fn actual_try_reserve_failure_rolls_back_backing_and_preserves_identity() {
    let s = Store::open(&root("allocation"), config()).unwrap();
    let leases: Vec<_> = (0..67)
        .map(|i| {
            s.reserve_namespace(&format!("n{i:04}"), request(64))
                .unwrap()
        })
        .collect();
    let old: Vec<_> = leases
        .iter()
        .map(|l| (l.id(), l.status().unwrap()))
        .collect();
    let memory = s.health2().memory_bytes;
    let next = s.state.lock().unwrap().namespace_reserves.next;
    s.state
        .lock()
        .unwrap()
        .namespace_reserves
        .allocation_additional = Some(usize::MAX);
    let refused = s
        .reserve_namespace_detailed("next", request(64))
        .unwrap_err();
    let p = refused.pressure.unwrap();
    assert_eq!(p.resource, ReserveResource::Allocation);
    assert_eq!((p.limit, p.required, p.available), (None, None, None));
    assert_eq!(s.health2().memory_bytes, memory);
    assert_eq!(s.state.lock().unwrap().namespace_reserves.next, next);
    for (l, o) in leases.iter().zip(&old) {
        assert_eq!((l.id(), l.status().unwrap()), *o);
    }
    // This authored operand triggers a real standard-library capacity/layout
    // refusal, not OS memory exhaustion. It is not a production grant/config.
    s.state
        .lock()
        .unwrap()
        .namespace_reserves
        .allocation_additional = None;
    let retry = s.reserve_namespace_detailed("next", request(64)).unwrap();
    assert_eq!(retry.id(), next + 1);
    println!(
        "actual std try_reserve refusal={p:?}; exact retry id={}",
        retry.id()
    );
}

#[test]
fn physical_id_and_namespace_bounds_refuse_without_changing_live_accounts() {
    let s = Store::open(&root("representation"), config()).unwrap();
    let lease = s.reserve_namespace("kept", request(64)).unwrap();
    let original = (lease.id(), lease.status().unwrap());
    assert_eq!(
        s.reserve_namespace_detailed(&"x".repeat(MAX_ID + 1), request(1))
            .unwrap_err()
            .error,
        ReserveError::Invalid
    );
    // A synthetic terminal representable ID is separate from population policy.
    s.state.lock().unwrap().namespace_reserves.next = u64::MAX;
    let p = s
        .reserve_namespace_detailed("next", request(1))
        .unwrap_err()
        .pressure
        .unwrap();
    assert_eq!(
        (p.resource, p.limit, p.required, p.available),
        (ReserveResource::Identity, Some(u64::MAX), None, Some(0))
    );
    assert_eq!((lease.id(), lease.status().unwrap()), original);
}

#[test]
fn admitted_range_has_accounted_backing_complete_scan_and_protected_write_headroom() {
    use crate::atomic::{AtomicAck, FinalChange, PublicationError};
    let s = Store::open(
        &root("range"),
        Config {
            state_limit_bytes: 256 * 512,
            ..config()
        },
    )
    .unwrap();
    let baseline = s.health2().memory_bytes;
    let begin = Instant::now();
    let mut leases = Vec::new();
    for i in 0..256 {
        let ns = format!("n{i:04}");
        leases.push(s.reserve_namespace(&ns, request(512)).unwrap());
        assert_eq!(s.mutate(mutation(&ns, "key", "original", 8)).code, 0);
        if [63, 64, 65, 128, 256].contains(&(i + 1)) {
            let st = s.state.lock().unwrap();
            let book = &st.namespace_reserves;
            let accounted: usize = book.accounts.values().map(|a| a.memory).sum();
            // A lower bound on typed table payload is measured independently;
            // allocator/control-byte overhead and process RSS remain unknown.
            let typed_table = book.accounts.capacity()
                * (std::mem::size_of::<String>() + std::mem::size_of::<Account>());
            assert!(accounted >= typed_table);
            assert_eq!(book.accounts.len(), i + 1);
            assert_eq!(
                reserved(&st),
                leases
                    .iter()
                    .map(|l| book.accounts[l.namespace()].status.remaining_bytes)
                    .sum()
            );
            println!("range={} slots={} account_charge={accounted} typed_table_lower_bound={typed_table} elapsed_us={}",i+1,book.accounts.capacity(),begin.elapsed().as_micros());
        }
    }
    let before: Vec<_> = leases
        .iter()
        .map(|l| (l.id(), l.status().unwrap()))
        .collect();
    let capture_begin = Instant::now();
    for lease in &leases {
        let view = s.capture_namespace_funded(lease.namespace(), 4096).unwrap();
        assert_eq!(view.rows().len(), 1);
        assert_eq!(view.rows()[0].0, "key");
        assert_eq!(view.rows()[0].1.bytes, vec![65; 8]);
    }
    let scan_us = capture_begin.elapsed().as_micros();
    let next = s
        .reserve_namespace_detailed("n0256", request(512))
        .unwrap_err()
        .pressure
        .unwrap();
    assert_eq!(
        (
            next.resource,
            next.required,
            next.available,
            next.active_accounts
        ),
        (ReserveResource::DataBytes, Some(512), Some(0), 256)
    );
    let data = s.state_bytes();
    assert_eq!(
        s.mutate(mutation("competitor", "key", "no-headroom", 1))
            .code,
        7
    );
    let other = s
        .capture_namespace_funded("competitor", 4096)
        .unwrap()
        .prepare(
            "refused",
            AtomicAck::Flush,
            vec![FinalChange::put("x", vec![1])],
        )
        .unwrap();
    assert_eq!(
        s.publish_atomic(&other, u64::MAX),
        Err(PublicationError::Resource(7))
    );
    assert_eq!(s.state_bytes(), data);
    assert!(s.try_get("competitor", "key").unwrap().is_none());
    assert!(s.try_get("competitor", "x").unwrap().is_none());
    for (lease, prior) in leases.iter().zip(&before) {
        assert_eq!((lease.id(), lease.status().unwrap()), *prior);
    }
    let health = s.health2();
    assert!(health.memory_bytes < s.config.capacity);
    println!("range_final=256 baseline_managed={baseline} actual_managed={} data={data} scans=256 scan_us={scan_us}; physical allocator/RSS unknown",health.memory_bytes);
}

#[test]
fn growth_and_pressure_preserve_actual_pending_and_uncertain_spend() {
    let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
    struct Release(Arc<(Mutex<(bool, bool)>, Condvar)>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0 .0.lock().unwrap().1 = true;
            self.0 .1.notify_all();
        }
    }
    let s = Store::open(
        &root("pending"),
        Config {
            sync_test_gate: Some(gate.clone()),
            fail_sync: true,
            ..config()
        },
    )
    .unwrap();
    let lease = s.reserve_namespace("owned", request(4096)).unwrap();
    let alias = lease.clone();
    let original = mutation("owned", "k", "grant", 231); // actual full frame = 300
    std::thread::scope(|scope| {
        let _release = Release(gate.clone());
        let job = scope.spawn(|| s.mutate(original.clone()));
        let entered = gate.1.wait_while(gate.0.lock().unwrap(), |v| !v.0).unwrap();
        drop(entered);
        let prior = (lease.id(), lease.status().unwrap());
        assert_eq!(prior.1.admitted_bytes, 300);
        let mut peers = Vec::new();
        for i in 0..129 {
            peers.push(
                s.reserve_namespace(&format!("n{i:04}"), request(64))
                    .unwrap(),
            );
        }
        assert_eq!((alias.id(), alias.status().unwrap()), prior);
        let refused = s
            .reserve_namespace_detailed("pressure", request(s.config.state_limit_bytes))
            .unwrap_err();
        assert_eq!(
            refused.pressure.unwrap().resource,
            ReserveResource::DataBytes
        );
        assert_eq!((alias.id(), alias.status().unwrap()), prior);
        {
            let state = s.state.lock().unwrap();
            assert_eq!((state.pending_disk, state.persisted_bytes), (300, 0));
        }
        lease.release().unwrap();
        assert_eq!(alias.status(), Err(ReserveError::Released));
        {
            let state = s.state.lock().unwrap();
            assert_eq!(state.pending_disk, 300);
        }
        gate.0.lock().unwrap().1 = true;
        gate.1.notify_all();
        assert_eq!(job.join().unwrap().code, 16);
        assert_eq!(s.mutate(original.clone()).code, 16);
        assert_eq!(s.state_bytes(), 300);
        for peer in &peers {
            assert_eq!(peer.status().unwrap().admitted_bytes, 0);
        }
        assert_eq!(
            s.reserve_namespace_detailed("after-uncertainty", request(1))
                .unwrap_err()
                .error,
            ReserveError::Unavailable
        );
        println!("growth peers=129 pending=300 uncertainty retained=300; original remains16 and unavailable fence retained");
    });
}

#[test]
fn growth_during_pending_atomic_publication_keeps_whole_uncertain_charge() {
    use crate::atomic::{AtomicAck, FinalChange, PublicationError};
    let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
    struct Release(Arc<(Mutex<(bool, bool)>, Condvar)>);
    impl Drop for Release {
        fn drop(&mut self) {
            self.0 .0.lock().unwrap().1 = true;
            self.0 .1.notify_all();
        }
    }
    let s = Store::open(
        &root("atomic-pending"),
        Config {
            sync_test_gate: Some(gate.clone()),
            fail_sync: true,
            ..config()
        },
    )
    .unwrap();
    let original = s
        .capture_namespace_funded("owned", 4096)
        .unwrap()
        .prepare(
            "whole",
            AtomicAck::Flush,
            vec![
                FinalChange::put("a", vec![65]),
                FinalChange::put("b", vec![66]),
            ],
        )
        .unwrap();
    let expected = 40 + original.to_bytes().len() as u64;
    let lease = s.reserve_namespace("owned", request(4096)).unwrap();
    std::thread::scope(|scope| {
        let _release = Release(gate.clone());
        let job = scope.spawn(|| s.publish_atomic(&original, u64::MAX));
        let entered = gate.1.wait_while(gate.0.lock().unwrap(), |v| !v.0).unwrap();
        drop(entered);
        let prior = (lease.id(), lease.status().unwrap());
        assert_eq!(
            (prior.1.admitted_bytes, prior.1.admitted_appends),
            (expected, 1)
        );
        let peers: Vec<_> = (0..129)
            .map(|i| {
                s.reserve_namespace(&format!("a{i:04}"), request(64))
                    .unwrap()
            })
            .collect();
        assert_eq!((lease.id(), lease.status().unwrap()), prior);
        {
            let st = s.state.lock().unwrap();
            assert_eq!((st.pending_disk, st.persisted_bytes), (expected, 0));
        }
        gate.0.lock().unwrap().1 = true;
        gate.1.notify_all();
        assert_eq!(job.join().unwrap(), Err(PublicationError::Uncertain));
        assert_eq!(
            s.publish_atomic(&original, u64::MAX),
            Err(PublicationError::Uncertain)
        );
        assert_eq!(s.state_bytes(), expected);
        assert_eq!((lease.id(), lease.status().unwrap()), prior);
        assert!(peers
            .iter()
            .all(|l| l.status().unwrap().admitted_bytes == 0));
        println!("atomic pending growth peers=129 charged={expected} whole outcome uncertain; original account unchanged");
    });
}
