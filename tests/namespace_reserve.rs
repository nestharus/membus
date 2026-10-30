//! Retained authored Stores: no resident endpoint or existing application data.
use membus::{
    atomic::{AtomicAck, FinalChange, PublicationError},
    namespace_reserve::{ReserveError, ReserveRequest},
    Class, Config, ManualClock, Mutation, Store,
};
use std::{
    fs,
    path::PathBuf,
    sync::Barrier,
    time::{SystemTime, UNIX_EPOCH},
};

fn root(label: &str) -> PathBuf {
    let base = PathBuf::from(
        std::env::var_os("MEMBUS_NAMESPACE_RESERVE_TEST_ROOT")
            .expect("explicit retained fixture root"),
    );
    assert!(base.is_absolute() && base.is_dir());
    let path = base.join(format!(
        "{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&path).unwrap();
    path
}
fn config() -> Config {
    Config {
        cache_bytes: 0,
        signal_bytes: 0,
        tick_ms: 3_600_000,
        retire_ms: 3_600_000,
        flush_ms: 0,
        ..Default::default()
    }
}
fn request(bytes: u64, appends: u64) -> ReserveRequest {
    ReserveRequest {
        bytes,
        appends,
        lifetime_ms: 60_000,
    }
}
fn mutation(ns: &str, key: &str, id: &str, bytes: usize) -> Mutation {
    Mutation {
        ns: ns.into(),
        key: key.into(),
        op_id: id.into(),
        deadline: membus::now_ms() + 60_000,
        ack: 2,
        expected: None,
        bytes: vec![b'v'; bytes],
        remove: false,
    }
}
// Independent MBK1 framing oracle: 16-byte header, fixed 26-byte mutation
// fields and four u32-length-prefixed fields (namespace/key/id/value).
fn disk(m: &Mutation) -> u64 {
    (58 + m.ns.len() + m.key.len() + m.op_id.len() + m.bytes.len()) as u64
}
fn batch(s: &Store, ns: &str) -> Vec<u16> {
    let mut payload = (membus::now_ms() + 60_000).to_le_bytes().to_vec();
    membus::lp(&mut payload, b"batch");
    membus::lp(&mut payload, ns.as_bytes());
    payload.extend(2u16.to_le_bytes());
    for key in ["b1", "b2"] {
        payload.push(membus::PUT);
        membus::lp(&mut payload, key.as_bytes());
        membus::lp(&mut payload, key.as_bytes());
        payload.push(2);
        membus::lp(&mut payload, b"value");
    }
    let wire = membus::frame(membus::BATCH, 1, &payload);
    let reply = s.dispatch(&wire[4..]);
    assert_eq!(u16::from_le_bytes(reply[17..19].try_into().unwrap()), 0);
    let mut c = membus::Cursor::new(&reply[19..]);
    assert_eq!(c.u16().unwrap(), 2);
    let mut codes = Vec::new();
    for _ in 0..2 {
        codes.push(c.u16().unwrap());
        c.u64().unwrap();
        c.u8().unwrap();
    }
    c.end().unwrap();
    codes
}

#[test]
fn exact_capacity_protects_unused_entitlement_and_charges_full_record() {
    let s = Store::open(
        &root("exact"),
        Config {
            state_limit_bytes: 600,
            maintenance_reserve_bytes: 100,
            ..config()
        },
    )
    .unwrap();
    let lease = s.reserve_namespace("owned", request(300, 2)).unwrap();
    let too_big = mutation("other", "k", "other", 132); // 201 bytes
    assert_eq!(disk(&too_big), 201);
    assert_eq!(s.mutate(too_big).code, 7);
    assert_eq!(s.state_bytes(), 0);
    let competitor = mutation("other", "k", "other", 131);
    assert_eq!(disk(&competitor), 200);
    assert_eq!(s.mutate(competitor).code, 0);
    let owned = mutation("owned", "k", "grant", 231);
    assert_eq!(disk(&owned), 300);
    assert_eq!(s.mutate(owned).code, 0);
    assert_eq!(s.state_bytes(), 500);
    let status = lease.release().unwrap();
    assert_eq!((status.admitted_bytes, status.remaining_bytes), (300, 0));
    assert_eq!(s.mutate(mutation("other", "x", "new", 0)).code, 7);
    assert_eq!(
        s.state_bytes(),
        500,
        "release must not refund written bytes"
    );
}

#[test]
fn issuance_is_finite_bounded_exclusive_and_charged_to_real_memory() {
    let s = Store::open(
        &root("issuance"),
        Config {
            state_limit_bytes: 500,
            ..config()
        },
    )
    .unwrap();
    assert_eq!(
        s.reserve_namespace("", request(1, 1)).unwrap_err(),
        ReserveError::Invalid
    );
    assert_eq!(
        s.reserve_namespace("x", request(0, 1)).unwrap_err(),
        ReserveError::Invalid
    );
    assert_eq!(
        s.reserve_namespace("x", request(1, 0)).unwrap_err(),
        ReserveError::Invalid
    );
    assert_eq!(
        s.reserve_namespace("__membus_timers_v1", request(1, 1))
            .unwrap_err(),
        ReserveError::UnsupportedNamespace
    );
    let before = s.health2().memory_bytes;
    let a = s.reserve_namespace("a", request(300, 1)).unwrap();
    assert!(s.health2().memory_bytes >= before + 516);
    assert_eq!(
        s.reserve_namespace("a", request(1, 1)).unwrap_err(),
        ReserveError::AlreadyReserved
    );
    assert_eq!(
        s.reserve_namespace("b", request(201, 1)).unwrap_err(),
        ReserveError::Resource
    );
    let b = s.reserve_namespace("b", request(200, 1)).unwrap();
    assert_eq!(
        s.reserve_namespace("c", request(1, 1)).unwrap_err(),
        ReserveError::Resource
    );
    drop(a);
    drop(b);
    assert_eq!(s.health2().memory_bytes, before);
    let leases: Vec<_> = (0..64)
        .map(|i| {
            s.reserve_namespace(&format!("n{i}"), request(1, 1))
                .unwrap()
        })
        .collect();
    let beyond = s.reserve_namespace("overflow", request(1, 1)).unwrap();
    assert_eq!(beyond.status().unwrap().remaining_bytes, 1);
    drop(beyond);
    drop(leases);
    s.reserve_namespace("overflow", request(500, 1)).unwrap();
    s.set_read_only(true);
    assert_eq!(
        s.reserve_namespace("refused", request(1, 1)).unwrap_err(),
        ReserveError::Unavailable
    );
}

#[test]
fn conditional_remove_and_real_batch_share_account_and_retries_do_not_spend_twice() {
    let s = Store::open(&root("entrypoints"), config()).unwrap();
    let lease = s.reserve_namespace("owned", request(4096, 4)).unwrap();
    let m = mutation("owned", "k", "one", 4);
    let first = s.mutate(m.clone());
    assert_eq!(first.code, 0);
    let before = lease.status().unwrap();
    assert_eq!(s.mutate(m.clone()), first);
    assert_eq!(lease.status().unwrap(), before);
    let mut conditional = mutation("owned", "k", "conditional", 5);
    conditional.expected = Some(first.version + 1);
    assert_eq!(s.mutate(conditional.clone()).code, 12);
    assert_eq!(lease.status().unwrap(), before);
    conditional.expected = Some(first.version);
    let updated = s.mutate(conditional);
    assert_eq!(updated.code, 0);
    let mut remove = mutation("owned", "k", "remove", 0);
    remove.expected = Some(updated.version);
    remove.remove = true;
    assert_eq!(s.mutate(remove).code, 0);
    let mut codes = batch(&s, "owned");
    codes.sort();
    assert_eq!(
        codes,
        vec![0, 7],
        "BATCH is per-item, not a separate budget"
    );
    let after = lease.status().unwrap();
    assert_eq!(after.admitted_appends, 4);
    assert_eq!(
        s.mutate(m),
        first,
        "retained success remains valid after exhaustion"
    );
    assert_eq!(lease.status().unwrap(), after);
    assert!(s.get("owned", "k").is_none());
}

#[test]
fn atomic_counts_one_envelope_and_preserves_original_retry_and_no_prefix() {
    fn population(s: &Store, first: u64) {
        let expected = vec![
            ("a".to_owned(), first, 2, b"A".to_vec()),
            ("b".to_owned(), first + 1, 2, b"B".to_vec()),
        ];
        for (key, version, ack, bytes) in &expected {
            let value = s
                .try_get("owned", key)
                .expect("successful value read must not fail")
                .expect("published value must exist under the active lease");
            assert_eq!(
                (&value.version, &value.ack, &value.bytes),
                (version, ack, bytes)
            );
        }
        let view = s
            .capture_namespace("owned")
            .expect("complete published population");
        let actual: Vec<_> = view
            .rows()
            .iter()
            .map(|(k, v)| (k.clone(), v.version, v.ack, v.bytes.clone()))
            .collect();
        assert_eq!(
            actual, expected,
            "no missing, extra or partially published member"
        );
        println!("observed complete atomic lease population {actual:?}");
    }
    let s = Store::open(&root("atomic"), config()).unwrap();
    let original = s
        .capture_namespace("owned")
        .unwrap()
        .prepare(
            "whole",
            AtomicAck::Flush,
            vec![
                FinalChange::put("a", b"A".to_vec()),
                FinalChange::put("b", b"B".to_vec()),
            ],
        )
        .unwrap();
    let identity = original.to_bytes();
    let bytes = 40 + original.to_bytes().len() as u64;
    let lease = s.reserve_namespace("owned", request(bytes, 1)).unwrap();
    let outcome = s
        .publish_atomic(&original, membus::now_ms() + 60_000)
        .unwrap();
    assert_eq!((outcome.code, outcome.ack), (0, 2));
    assert_ne!(outcome.version, 0);
    population(&s, outcome.version);
    assert_eq!(s.state_bytes(), bytes);
    let status = lease.status().unwrap();
    assert_eq!(
        (
            status.admitted_bytes,
            status.admitted_appends,
            status.remaining_bytes
        ),
        (bytes, 1, 0)
    );
    assert_eq!(
        s.publish_atomic(&original, membus::now_ms() + 60_000)
            .unwrap(),
        outcome
    );
    assert_eq!(lease.status().unwrap(), status);
    assert_eq!(s.state_bytes(), bytes);
    assert_eq!(original.to_bytes(), identity);
    population(&s, outcome.version);
    let next = s
        .capture_namespace("owned")
        .unwrap()
        .prepare(
            "next",
            AtomicAck::Flush,
            vec![FinalChange::put("c", vec![1])],
        )
        .unwrap();
    assert_eq!(
        s.publish_atomic(&next, membus::now_ms() + 60_000),
        Err(PublicationError::Resource(7))
    );
    assert!(s
        .try_get("owned", "c")
        .expect("refused successor read must not fail")
        .is_none());
    assert_eq!(lease.status().unwrap(), status);
    assert_eq!(s.state_bytes(), bytes);
    population(&s, outcome.version);
}

#[test]
fn competing_atomic_and_control_records_cannot_consume_protected_headroom() {
    let s = Store::open(
        &root("controls"),
        Config {
            state_limit_bytes: 512,
            ..config()
        },
    )
    .unwrap();
    let original = s
        .capture_namespace("other")
        .unwrap()
        .prepare(
            "whole",
            AtomicAck::Flush,
            vec![FinalChange::put("a", vec![3])],
        )
        .unwrap();
    let lease = s.reserve_namespace("owned", request(512, 2)).unwrap();
    assert_eq!(
        s.publish_atomic(&original, membus::now_ms() + 60_000),
        Err(PublicationError::Resource(7))
    );
    assert!(s.get("other", "a").is_none());
    assert_eq!(
        s.set_policy_with_id(
            "other",
            "",
            Some(Class::LATEST),
            "policy",
            membus::now_ms() + 60_000
        )
        .code,
        7
    );
    assert!(s.policies().is_empty());
    let mut timer = (membus::now_ms() + 60_000).to_le_bytes().to_vec();
    membus::lp(&mut timer, b"timer-set");
    membus::lp(&mut timer, b"other");
    membus::lp(&mut timer, b"t");
    timer.extend(u64::MAX.to_le_bytes());
    membus::lp(&mut timer, b"wake");
    timer.extend((membus::now_ms() + 1000).to_le_bytes());
    for _ in 0..3 {
        timer.extend(0u64.to_le_bytes());
    }
    membus::lp(&mut timer, b"payload");
    timer.push(2);
    let wire = membus::frame(membus::TIMER_SET, 2, &timer);
    let response = s.dispatch(&wire[4..]);
    assert_eq!(u16::from_le_bytes(response[17..19].try_into().unwrap()), 7);
    assert_eq!(
        s.mutate(mutation("__membus_timers_v1", "s/authored", "timer", 10))
            .code,
        7
    );
    assert_eq!(s.state_bytes(), 0);
    assert_eq!(lease.status().unwrap().admitted_appends, 0);
    lease.release().unwrap();
    assert_eq!(
        s.set_policy_with_id(
            "other",
            "",
            Some(Class::LATEST),
            "policy",
            membus::now_ms() + 60_000
        )
        .code,
        0
    );
}

#[test]
fn aliases_concurrency_and_replacement_cannot_double_spend_or_release() {
    let s = Store::open(&root("concurrency"), config()).unwrap();
    let lease = s.reserve_namespace("owned", request(4096, 4)).unwrap();
    let barrier = Barrier::new(16);
    let outcomes = std::thread::scope(|scope| {
        let jobs: Vec<_> = (0..16)
            .map(|i| {
                let s = &s;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    s.mutate(mutation("owned", &format!("k{i}"), &format!("op{i}"), 8))
                })
            })
            .collect();
        jobs.into_iter()
            .map(|j| j.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(outcomes.iter().filter(|o| o.code == 0).count(), 4);
    assert!(outcomes.iter().all(|o| matches!(o.code, 0 | 7)));
    assert_eq!(lease.status().unwrap().admitted_appends, 4);
    let stale = lease.clone();
    lease.release().unwrap();
    assert_eq!(stale.status(), Err(ReserveError::Released));
    let next = s.reserve_namespace("owned", request(20, 1)).unwrap();
    drop(stale);
    assert_eq!(next.status().unwrap().remaining_bytes, 20);
    let other = next.clone();
    let barrier = Barrier::new(2);
    std::thread::scope(|scope| {
        scope.spawn(|| {
            barrier.wait();
            drop(next);
        });
        scope.spawn(|| {
            barrier.wait();
            drop(other);
        });
    });
    s.reserve_namespace("owned", request(20, 1)).unwrap();
}

#[test]
fn monotonic_expiry_returns_only_unused_entitlement_and_old_alias_cannot_revoke_new() {
    let clock = ManualClock::new(membus::now_ms());
    let s = Store::open(
        &root("expiry"),
        Config {
            clock: Some(clock.clone()),
            ..config()
        },
    )
    .unwrap();
    let lease = s
        .reserve_namespace(
            "owned",
            ReserveRequest {
                lifetime_ms: 100,
                ..request(300, 2)
            },
        )
        .unwrap();
    assert_eq!(s.mutate(mutation("owned", "k", "written", 10)).code, 0);
    let written = s.state_bytes();
    clock.jump_utc(5_000_000);
    assert!(lease.status().is_ok());
    clock.advance(99);
    assert!(lease.status().is_ok());
    clock.advance(1);
    assert_eq!(lease.status(), Err(ReserveError::Expired));
    let new = s.reserve_namespace("owned", request(20, 1)).unwrap();
    drop(lease);
    assert_eq!(new.status().unwrap().remaining_bytes, 20);
    assert_eq!(s.state_bytes(), written);
}

#[test]
fn failed_and_uncertain_scalar_appends_remain_charged_and_replay_does_not_restore_a_grant() {
    for (label, partial) in [("uncertain", false), ("partial", true)] {
        let path = root(label);
        let m = mutation("owned", "k", "lost", 50);
        let charged = disk(&m);
        {
            let s = Store::open(
                &path,
                Config {
                    state_limit_bytes: 1000,
                    fail_after_append: !partial,
                    fail_partial_append: partial,
                    ..config()
                },
            )
            .unwrap();
            let lease = s.reserve_namespace("owned", request(900, 2)).unwrap();
            assert_eq!(s.mutate(m.clone()).code, 16);
            let status = lease.status().unwrap();
            assert_eq!(
                (status.admitted_bytes, status.admitted_appends),
                (charged, 1)
            );
            assert_eq!(s.mutate(m.clone()).code, 16);
            assert_eq!(lease.status().unwrap(), status);
            lease.release().unwrap();
            assert_eq!(s.state_bytes(), charged, "uncertainty is not a refund");
            assert!(s.health2().read_only);
        }
        let s = Store::open(
            &path,
            Config {
                state_limit_bytes: 1000,
                ..config()
            },
        )
        .unwrap();
        let actual = s.state_bytes();
        if !partial {
            assert_eq!(actual, charged);
            assert!(s.get("owned", "k").is_some());
        }
        // This is new issuance after completed recovery, not restoration of the
        // old grant. It must charge the actual retained recovered population.
        assert_eq!(
            s.reserve_namespace("owned", request(1001 - actual, 2))
                .unwrap_err(),
            ReserveError::Resource
        );
        let new = s
            .reserve_namespace("owned", request(1000 - actual, 2))
            .unwrap();
        assert_eq!(new.status().unwrap().admitted_appends, 0);
    }
}

#[test]
fn atomic_uncertainty_and_refusals_never_refund_or_spend_a_second_time() {
    let s = Store::open(
        &root("atomic-uncertain"),
        Config {
            fail_after_append: true,
            ..config()
        },
    )
    .unwrap();
    let original = s
        .capture_namespace("owned")
        .unwrap()
        .prepare(
            "whole",
            AtomicAck::Flush,
            vec![FinalChange::put("a", vec![1])],
        )
        .unwrap();
    let lease = s.reserve_namespace("owned", request(4096, 2)).unwrap();
    assert_eq!(
        s.publish_atomic(&original, membus::now_ms() + 60_000),
        Err(PublicationError::Uncertain)
    );
    let status = lease.status().unwrap();
    assert_eq!(status.admitted_bytes, 40 + original.to_bytes().len() as u64);
    assert_eq!(
        s.publish_atomic(&original, membus::now_ms() + 60_000),
        Err(PublicationError::Uncertain)
    );
    assert_eq!(lease.status().unwrap(), status);
    lease.release().unwrap();
    assert_eq!(s.state_bytes(), status.admitted_bytes);
}

#[test]
fn removal_and_completed_retirement_do_not_refund_the_cumulative_lease() {
    let clock = ManualClock::new(membus::now_ms());
    let s = Store::open(
        &root("retention"),
        Config {
            clock: Some(clock.clone()),
            retry_ms: 0,
            chunk_bytes: 1,
            ..config()
        },
    )
    .unwrap();
    assert_eq!(
        s.set_policy(
            "owned",
            "",
            Some(Class::expire(10)),
            membus::now_ms() + 60_000
        )
        .code,
        0
    );
    let lease = s
        .reserve_namespace(
            "owned",
            ReserveRequest {
                lifetime_ms: 10_000_000,
                ..request(4096, 3)
            },
        )
        .unwrap();
    let first = s.mutate(mutation("owned", "k", "first", 100));
    assert_eq!(first.code, 0);
    let mut remove = mutation("owned", "k", "remove", 0);
    remove.remove = true;
    remove.expected = Some(first.version);
    assert_eq!(s.mutate(remove).code, 0);
    let status = lease.status().unwrap();
    let before = s.state_bytes();
    clock.advance(3_600_001);
    s.tick();
    s.retirement_pass();
    assert!(
        s.state_bytes() < before,
        "actual completed retirement releases Store data bytes"
    );
    assert_eq!(
        lease.status().unwrap(),
        status,
        "physical retirement cannot reconstruct spent entitlement"
    );
}

#[test]
fn mem_ack_is_counted_as_an_admission_and_other_resource_refusals_do_not_spend() {
    let s = Store::open(
        &root("mem"),
        Config {
            allow_mem: true,
            ..config()
        },
    )
    .unwrap();
    let lease = s.reserve_namespace("owned", request(100, 1)).unwrap();
    let mut m = mutation("owned", "k", "mem", 10);
    m.ack = 0;
    assert_eq!(s.mutate(m.clone()).code, 0);
    assert_eq!(
        (
            lease.status().unwrap().admitted_bytes,
            lease.status().unwrap().admitted_appends
        ),
        (0, 1)
    );
    assert_eq!(s.mutate(m).code, 0);
    assert_eq!(s.mutate(mutation("owned", "k", "second", 0)).code, 7);
    let s = Store::open(
        &root("independent-refusal"),
        Config {
            queue_bytes: 1,
            ..config()
        },
    )
    .unwrap();
    let lease = s.reserve_namespace("owned", request(100, 1)).unwrap();
    assert_eq!(s.mutate(mutation("owned", "k", "queue", 0)).code, 6);
    assert_eq!(lease.status().unwrap().admitted_appends, 0);
}

#[test]
fn existing_disk_refusal_precedes_queue_refusal_without_spending_the_lease() {
    let s = Store::open(
        &root("refusal-order"),
        Config {
            state_limit_bytes: 60,
            queue_bytes: 1,
            ..config()
        },
    )
    .unwrap();
    let lease = s.reserve_namespace("owned", request(60, 1)).unwrap();
    assert_eq!(s.mutate(mutation("other", "k", "other", 0)).code, 7);
    assert_eq!(lease.status().unwrap().admitted_appends, 0);
}
