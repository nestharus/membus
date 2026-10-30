//! Resource-admitted growth in fresh, retained, private Stores.
use membus::{
    namespace_reserve::{ReserveError, ReserveRequest, ReserveResource},
    Config, Mutation, Store,
};
use std::{fs, path::PathBuf};

fn root(label: &str) -> PathBuf {
    let base = PathBuf::from(std::env::var_os("MEMBUS_NAMESPACE_RESERVE_TEST_ROOT").unwrap());
    assert!(base.is_absolute() && base.is_dir());
    let p = base.join(format!(
        "growth-{label}-{}-{}",
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

#[test]
fn managed_pressure_retains_all_accounts_and_release_allows_same_request() {
    let s = Store::open(
        &root("memory"),
        Config {
            capacity: 65536,
            ..config()
        },
    )
    .unwrap();
    let mut leases = Vec::new();
    let mut n = 0;
    let (refused_ns, pressure) = loop {
        let ns = format!("n{n:04}");
        let before = s.health2().memory_bytes;
        match s.reserve_namespace_detailed(&ns, request(1)) {
            Ok(lease) => {
                leases.push(lease);
                n += 1;
            }
            Err(r) => {
                assert_eq!(r.error, ReserveError::Resource);
                assert_eq!(s.health2().memory_bytes, before);
                break (ns, r.pressure.unwrap());
            }
        }
        assert!(
            s.health2().memory_bytes <= s.config.capacity,
            "admitted bookkeeping exceeded the configured managed limit"
        );
    };
    assert!(leases.len() > 64);
    assert_eq!(pressure.resource, ReserveResource::ManagedMemory);
    assert_eq!(pressure.limit, Some(65536));
    assert_eq!(pressure.required, Some(532));
    assert_eq!(
        pressure.available,
        Some((65536 - s.health2().memory_bytes) as u64)
    );
    assert!(pressure.available.unwrap() < pressure.required.unwrap());
    let prior: Vec<_> = leases
        .iter()
        .map(|l| (l.id(), l.status().unwrap()))
        .collect();
    assert_eq!(
        s.reserve_namespace_detailed(&refused_ns, request(1))
            .unwrap_err()
            .pressure,
        Some(pressure)
    );
    let alias = leases[0].clone();
    let released_id = leases.remove(0).id();
    alias.release().unwrap();
    let retry = s
        .reserve_namespace_detailed(&refused_ns, request(1))
        .unwrap();
    assert_eq!(
        retry.id(),
        prior.last().unwrap().0 + 1,
        "refusal must not burn an ID"
    );
    for (lease, old) in leases.iter().zip(prior.iter().skip(1)) {
        assert_eq!((lease.id(), lease.status().unwrap()), *old);
    }
    let replacement = s.reserve_namespace("n0000", request(1)).unwrap_err();
    assert_eq!(
        replacement,
        ReserveError::Resource,
        "a healthy replacement still needs real backing"
    );
    println!("managed pressure active={} required={:?} available={:?} released_id={released_id} retry_id={}",
        pressure.active_accounts, pressure.required, pressure.available, retry.id());
}

#[test]
fn data_pressure_and_stale_aliases_do_not_refund_spend_or_revoke_replacement() {
    let s = Store::open(
        &root("data"),
        Config {
            state_limit_bytes: 8192,
            ..config()
        },
    )
    .unwrap();
    let a = s.reserve_namespace("a", request(4096)).unwrap();
    let b = s.reserve_namespace("b", request(4096)).unwrap();
    let stale = b.clone();
    let current = mutation("a", "original");
    let receipt = s.mutate(current.clone());
    assert_eq!(receipt.code, 0);
    let before = a.status().unwrap();
    let old_id = b.id();
    let attempt = request(1024);
    let r = s.reserve_namespace_detailed("next", attempt).unwrap_err();
    let pressure = r.pressure.unwrap();
    assert_eq!(
        (
            pressure.resource,
            pressure.limit,
            pressure.required,
            pressure.available
        ),
        (ReserveResource::DataBytes, Some(8192), Some(1024), Some(0))
    );
    assert_eq!(a.status().unwrap(), before);
    b.release().unwrap();
    assert_eq!(stale.status(), Err(ReserveError::Released));
    let retry = s.reserve_namespace_detailed("next", attempt).unwrap();
    assert_eq!(retry.id(), old_id + 1);
    let replacement = s.reserve_namespace("b", request(3072)).unwrap();
    let replacement_status = replacement.status().unwrap();
    assert_ne!(replacement.id(), old_id);
    drop(stale);
    assert_eq!(replacement.status().unwrap(), replacement_status);
    assert_eq!(a.status().unwrap(), before);
    assert_eq!(s.mutate(current), receipt);
    assert_eq!(s.try_get("a", "key").unwrap().unwrap().bytes, b"retained");
    assert_eq!(s.state_bytes(), before.admitted_bytes);
}

#[test]
#[ignore = "negative oracle: a fresh post-reopen lease must not impersonate a lost live account"]
fn harmful_reopen_is_not_preservation_of_live_account() {
    let p = root("reopen-negative");
    let (old_status, old_epoch, old_receipt) = {
        let s = Store::open(&p, config()).unwrap();
        let lease = s.reserve_namespace("same", request(1024)).unwrap();
        let outcome = s.mutate(mutation("same", "original"));
        assert_eq!(outcome.code, 0);
        (lease.status().unwrap(), s.epoch, outcome)
    };
    let reopened = Store::open(&p, config()).unwrap();
    assert_eq!(reopened.mutate(mutation("same", "original")), old_receipt);
    assert_eq!(
        reopened.try_get("same", "key").unwrap().unwrap().bytes,
        b"retained"
    );
    assert_ne!(reopened.epoch, old_epoch);
    let fresh = reopened.reserve_namespace("same", request(1024)).unwrap();
    println!(
        "lost account={old_status:?}; fresh={:?}; original effect retained",
        fresh.status().unwrap()
    );
    assert_eq!(
        fresh.status().unwrap(),
        old_status,
        "reopen lost the live account even though the original effect survived"
    );
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
fn mutation(ns: &str, id: &str) -> Mutation {
    Mutation {
        ns: ns.into(),
        key: "key".into(),
        op_id: id.into(),
        deadline: u64::MAX,
        ack: 2,
        expected: None,
        bytes: b"retained".to_vec(),
        remove: false,
    }
}

#[test]
fn below_at_above_former_boundary_keeps_all_live_accounts() {
    let s = Store::open(&root("boundary"), config()).unwrap();
    let mut leases = Vec::new();
    let mut observations = Vec::new();
    let mut ids = Vec::new();
    let mut aliases = Vec::new();
    for n in 0..129 {
        let ns = format!("n{n:04}");
        let lease = s.reserve_namespace(&ns, request(1024)).unwrap_or_else(|e| {
            panic!(
                "account {} with sufficient configured resources: {e:?}",
                n + 1
            )
        });
        let outcome = s.mutate(mutation(&ns, "original"));
        assert_eq!((outcome.code, outcome.ack), (0, 2));
        assert_ne!(outcome.version, 0);
        observations.push((lease.status().unwrap(), outcome));
        ids.push(lease.id());
        aliases.push(lease.clone());
        leases.push(lease);
        if [63, 64, 65, 128, 129].contains(&(n + 1)) {
            for (i, old) in leases.iter().enumerate() {
                assert_eq!(old.id(), ids[i]);
                assert_eq!(old.status().unwrap(), observations[i].0);
                assert_eq!(
                    (aliases[i].id(), aliases[i].status().unwrap()),
                    (ids[i], observations[i].0)
                );
                assert_eq!(
                    s.mutate(mutation(old.namespace(), "original")),
                    observations[i].1
                );
                let v = s.try_get(old.namespace(), "key").unwrap().unwrap();
                assert_eq!(
                    (v.version, v.bytes),
                    (observations[i].1.version, b"retained".to_vec())
                );
            }
            println!(
                "population={} managed={} data={} active tokens preserved",
                n + 1,
                s.health2().memory_bytes,
                s.state_bytes()
            );
        }
    }
    assert!(s.health2().memory_bytes < s.config.capacity);
}

#[test]
fn concurrent_live_issuers_share_one_actual_data_admission() {
    let s = Store::open(
        &root("issuers"),
        Config {
            state_limit_bytes: 4096,
            ..config()
        },
    )
    .unwrap();
    let gate = std::sync::Barrier::new(64);
    let results = std::thread::scope(|scope| {
        let jobs: Vec<_> = (0..64)
            .map(|i| {
                let store = &s;
                let gate = &gate;
                scope.spawn(move || {
                    gate.wait();
                    store.reserve_namespace_detailed(&format!("n{i:04}"), request(100))
                })
            })
            .collect();
        jobs.into_iter()
            .map(|job| job.join().unwrap())
            .collect::<Vec<_>>()
    });
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 40);
    assert!(results
        .iter()
        .filter_map(|r| r.as_ref().err())
        .all(|r| r.pressure.unwrap().resource == ReserveResource::DataBytes));
    let mut ids: Vec<_> = results
        .iter()
        .filter_map(|r| r.as_ref().ok())
        .map(|l| l.id())
        .collect();
    ids.sort();
    assert_eq!(ids, (1..=40).collect::<Vec<_>>());
    assert_eq!(
        results
            .iter()
            .filter_map(|r| r.as_ref().ok())
            .map(|l| l.status().unwrap().remaining_bytes)
            .sum::<u64>(),
        4000
    );
    for lease in results.iter().filter_map(|r| r.as_ref().ok()) {
        let receipt = s.mutate(mutation(lease.namespace(), "original"));
        assert_eq!((receipt.code, receipt.ack), (0, 2));
        assert_eq!(
            s.try_get(lease.namespace(), "key").unwrap().unwrap().bytes,
            b"retained"
        );
    }
    let refused = s
        .reserve_namespace_detailed("too-large", request(97))
        .unwrap_err()
        .pressure
        .unwrap();
    assert_eq!((refused.required, refused.available), (Some(97), Some(96)));
}
