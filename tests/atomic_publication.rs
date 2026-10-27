//! Private authored stores; all files are retained under an explicit selector.
use membus::{
    atomic::{AtomicAck, AtomicRequest, FinalChange, PublicationError},
    Config, Mutation, Store,
};
use std::{
    fs,
    path::PathBuf,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
fn root(label: &str) -> PathBuf {
    let base = PathBuf::from(
        std::env::var_os("MEMBUS_PROVIDER_TEST_ROOT").expect("explicit retained fixture root"),
    );
    assert!(base.is_absolute() && base.is_dir());
    let p = base.join(format!(
        "atomic-{label}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&p).unwrap();
    println!("retained authored store {}", p.display());
    p
}
fn config() -> Config {
    Config {
        cache_bytes: 0,
        signal_bytes: 0,
        tick_ms: 60000,
        retire_ms: 60000,
        flush_ms: 0,
        ..Default::default()
    }
}
fn put(s: &Store, id: &str, key: &str) -> u64 {
    let o = s.mutate(Mutation {
        ns: "authored".into(),
        key: key.into(),
        op_id: id.into(),
        deadline: membus::now_ms() + 30000,
        ack: 2,
        expected: None,
        bytes: id.as_bytes().to_vec(),
        remove: false,
    });
    assert_eq!(o.code, 0);
    o.version
}
fn request(s: &Store, id: &str, changes: Vec<FinalChange>) -> AtomicRequest {
    s.capture_namespace("authored")
        .unwrap()
        .prepare(id, AtomicAck::Flush, changes)
        .unwrap()
}
fn publish(s: &Store, r: &AtomicRequest) -> Result<membus::Outcome, PublicationError> {
    s.publish_atomic(r, membus::now_ms() + 30000)
}
#[test]
fn whole_set_conflicts_on_phantom_and_publishes_no_prefix() {
    let p = root("phantom");
    let s = Store::open(&p, config()).unwrap();
    put(&s, "capacity", "capacity");
    let r = request(
        &s,
        "decision",
        vec![
            FinalChange::put("a", b"A".to_vec()),
            FinalChange::put("b", b"B".to_vec()),
        ],
    );
    put(&s, "phantom", "new");
    assert_eq!(publish(&s, &r), Err(PublicationError::Conflict));
    assert!(s.get("authored", "a").is_none() && s.get("authored", "b").is_none());
}
#[test]
fn full_set_replay_cold_locations_and_durable_original_duplicate() {
    let p = root("replay");
    let (r, out) = {
        let s = Store::open(&p, config()).unwrap();
        put(&s, "old", "old");
        let r = request(
            &s,
            "whole",
            vec![
                FinalChange::remove("old"),
                FinalChange::put("a", b"one".to_vec()),
                FinalChange::put("b", b"two".to_vec()),
            ],
        );
        r.persist_new(&p.join("original-operation.custody"))
            .unwrap();
        let out = publish(&s, &r).unwrap();
        assert_eq!(out.ack, 2);
        assert!(s.get("authored", "old").is_none());
        assert_eq!(s.try_get("authored", "a").unwrap().unwrap().bytes, b"one");
        (r, out)
    };
    let s = Store::open(&p, config()).unwrap();
    assert!(!s.health2().read_only && !s.health2().storage_error);
    assert_eq!(s.try_get("authored", "b").unwrap().unwrap().bytes, b"two");
    put(&s, "successor", "a");
    let restored =
        AtomicRequest::from_bytes(&fs::read(p.join("original-operation.custody")).unwrap())
            .unwrap();
    assert_eq!(r.to_bytes(), restored.to_bytes());
    assert_eq!(publish(&s, &restored).unwrap(), out);
    assert_eq!(s.get("authored", "a").unwrap().bytes, b"successor");
}
#[test]
fn quota_disk_queue_index_retry_and_memory_refuse_before_any_effect() {
    for (label, c) in [
        (
            "quota",
            Config {
                quota: 1,
                ..config()
            },
        ),
        (
            "disk",
            Config {
                state_limit_bytes: 1,
                ..config()
            },
        ),
        (
            "queue",
            Config {
                queue_bytes: 1,
                ..config()
            },
        ),
        (
            "index",
            Config {
                index_cap: 1,
                ..config()
            },
        ),
        (
            "retry",
            Config {
                dedup_cap: 0,
                ..config()
            },
        ),
        (
            "memory",
            Config {
                capacity: 32768,
                ..config()
            },
        ),
    ] {
        let p = root(label);
        let s = Store::open(&p, c).unwrap();
        let before = s.io_metrics();
        let r = request(
            &s,
            "oversize",
            vec![
                FinalChange::put("a", vec![1; 8192]),
                FinalChange::put("b", vec![2; 8192]),
            ],
        );
        assert!(
            matches!(publish(&s, &r), Err(PublicationError::Resource(_))),
            "{label}"
        );
        assert_eq!(s.io_metrics(), before, "{label}");
        assert!(s.get("authored", "a").is_none() && s.get("authored", "b").is_none());
    }
}
#[test]
fn concurrent_decisions_from_one_population_have_only_one_winner() {
    let p = root("race");
    let s = Arc::new(Store::open(&p, config()).unwrap());
    let a = request(&s, "first", vec![FinalChange::put("a", vec![1])]);
    let b = request(&s, "second", vec![FinalChange::put("b", vec![2])]);
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let jobs: Vec<_> = [a, b]
        .into_iter()
        .map(|r| {
            let s = s.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                b.wait();
                publish(&s, &r)
            })
        })
        .collect();
    barrier.wait();
    let results: Vec<_> = jobs.into_iter().map(|j| j.join().unwrap()).collect();
    assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|r| **r == Err(PublicationError::Conflict))
            .count(),
        1
    );
}
#[test]
fn admitted_sync_failure_is_uncertain_and_replay_deduplicates_whole_request() {
    let p = root("uncertain");
    let r = {
        let s = Store::open(
            &p,
            Config {
                fail_sync: true,
                ..config()
            },
        )
        .unwrap();
        let r = request(
            &s,
            "uncertain",
            vec![
                FinalChange::put("a", vec![1]),
                FinalChange::put("b", vec![2]),
            ],
        );
        assert_eq!(publish(&s, &r), Err(PublicationError::Uncertain));
        assert!(s.get("authored", "a").is_none() && s.get("authored", "b").is_none());
        r
    };
    assert!(Store::open(
        &p,
        Config {
            fail_recovery_sync: true,
            ..config()
        }
    )
    .is_err());
    let s = Store::open(&p, config()).unwrap();
    let out = publish(&s, &r).unwrap();
    assert_eq!(out.ack, 2);
    assert!(s.get("authored", "a").is_some() && s.get("authored", "b").is_some());
}
#[test]
fn unsupported_expiry_deadline_identity_and_restart_are_distinct() {
    let p = root("refusals");
    let r = {
        let s = Store::open(&p, config()).unwrap();
        let r = request(&s, "old", vec![FinalChange::put("a", vec![1])]);
        assert_eq!(s.publish_atomic(&r, 0), Err(PublicationError::Expired));
        assert_eq!(
            s.set_policy(
                "authored",
                "",
                Some(membus::Class::expire(s.config.retry_ms)),
                membus::now_ms() + 30000
            )
            .code,
            0
        );
        assert_eq!(publish(&s, &r), Err(PublicationError::Unsupported));
        r
    };
    let s = Store::open(&p, config()).unwrap();
    assert_eq!(publish(&s, &r), Err(PublicationError::RestartUncertain));
}
#[test]
fn torn_admitted_envelope_never_replays_a_prefix_or_accepts_new_restart_attempt() {
    let p = root("torn");
    let r = {
        let s = Store::open(
            &p,
            Config {
                fail_partial_append: true,
                ..config()
            },
        )
        .unwrap();
        let r = request(
            &s,
            "torn-original",
            vec![
                FinalChange::put("a", vec![1]),
                FinalChange::put("b", vec![2]),
            ],
        );
        assert_eq!(publish(&s, &r), Err(PublicationError::Uncertain));
        r
    };
    // Keep the complete torn input unchanged; replay may truncate only its private copy.
    fn copy(from: &std::path::Path, to: &std::path::Path) {
        fs::create_dir(to).unwrap();
        for e in fs::read_dir(from).unwrap() {
            let e = e.unwrap();
            assert!(!e.file_type().unwrap().is_symlink());
            if e.file_type().unwrap().is_dir() {
                copy(&e.path(), &to.join(e.file_name()));
            } else {
                fs::copy(e.path(), to.join(e.file_name())).unwrap();
            }
        }
    }
    let working = p.with_file_name(format!(
        "{}-working",
        p.file_name().unwrap().to_str().unwrap()
    ));
    copy(&p, &working);
    let s = Store::open(&working, config()).unwrap();
    assert!(s.get("authored", "a").is_none() && s.get("authored", "b").is_none());
    assert_eq!(publish(&s, &r), Err(PublicationError::RestartUncertain));
}
#[test]
fn retained_success_ignores_new_view_deadline_and_rejects_changed_original_identity() {
    let p = root("identity");
    let s = Store::open(&p, config()).unwrap();
    let r = request(
        &s,
        "original",
        vec![
            FinalChange::put("a", vec![1]),
            FinalChange::put("b", vec![2]),
        ],
    );
    let out = publish(&s, &r).unwrap();
    put(&s, "newer", "a");
    assert_eq!(s.publish_atomic(&r, 0).unwrap(), out);
    let changed = request(&s, "original", vec![FinalChange::remove("a")]);
    assert_eq!(
        publish(&s, &changed),
        Err(PublicationError::IdentityMismatch)
    );
    assert_eq!(s.get("authored", "a").unwrap().bytes, b"newer");
}
#[test]
fn population_change_removal_and_revision_replacement_both_conflict() {
    for label in ["removal", "revision"] {
        let p = root(label);
        let s = Store::open(&p, config()).unwrap();
        let old = put(&s, "before", "existing");
        let r = request(
            &s,
            "decision",
            vec![
                FinalChange::put("a", vec![1]),
                FinalChange::put("b", vec![2]),
            ],
        );
        let o = s.mutate(Mutation {
            ns: "authored".into(),
            key: "existing".into(),
            op_id: "change".into(),
            deadline: membus::now_ms() + 30000,
            ack: 2,
            expected: Some(old),
            bytes: b"changed".to_vec(),
            remove: label == "removal",
        });
        assert_eq!(o.code, 0);
        assert_eq!(publish(&s, &r), Err(PublicationError::Conflict));
        assert!(s.get("authored", "a").is_none());
    }
}
#[test]
fn snapshot_reader_observes_all_old_or_all_final_values() {
    let p = root("visibility");
    let s = Arc::new(
        Store::open(
            &p,
            Config {
                write_delay_ms: 20,
                ..config()
            },
        )
        .unwrap(),
    );
    let r = request(
        &s,
        "all-at-once",
        vec![
            FinalChange::put("a", vec![1]),
            FinalChange::put("b", vec![2]),
        ],
    );
    let writer = {
        let s = s.clone();
        std::thread::spawn(move || publish(&s, &r).unwrap())
    };
    for _ in 0..12 {
        let v = s.capture_namespace("authored").unwrap();
        assert!(v.rows().is_empty() || v.rows().len() == 2);
    }
    writer.join().unwrap();
    assert_eq!(s.capture_namespace("authored").unwrap().rows().len(), 2);
}
#[test]
fn frame_limit_and_duplicate_targets_refuse_without_append() {
    let p = root("request-bounds");
    let s = Store::open(&p, config()).unwrap();
    let v = s.capture_namespace("authored").unwrap();
    let before = s.io_metrics();
    assert!(matches!(
        v.prepare(
            "large",
            AtomicAck::Os,
            vec![
                FinalChange::put("a", vec![1; membus::MAX_VALUE]),
                FinalChange::put("b", vec![2; membus::MAX_VALUE])
            ]
        ),
        Err(PublicationError::Resource(4))
    ));
    assert!(matches!(
        v.prepare(
            "duplicate",
            AtomicAck::Os,
            vec![FinalChange::remove("a"), FinalChange::put("a", vec![1])]
        ),
        Err(PublicationError::Invalid)
    ));
    assert_eq!(s.io_metrics(), before);
}
#[test]
fn final_quota_replacement_is_admitted_as_one_set_and_expiry_is_not_readmission() {
    let p = root("net-quota");
    let clock = membus::ManualClock::new(membus::now_ms());
    let s = Store::open(
        &p,
        Config {
            quota: 1,
            chunk_bytes: 1,
            clock: Some(clock.clone()),
            ..config()
        },
    )
    .unwrap();
    put(&s, "old", "old");
    let r = request(
        &s,
        "replace",
        vec![FinalChange::remove("old"), FinalChange::put("new", vec![2])],
    );
    let out = publish(&s, &r).unwrap();
    assert_eq!(s.slots("authored").1, 1);
    put(&s, "replacement", "new");
    assert_eq!(publish(&s, &r).unwrap(), out);
    clock.advance(s.config.retry_ms + 1);
    assert_eq!(publish(&s, &r), Err(PublicationError::RetryExpired));
}
#[test]
fn raw_mutation_and_policy_projection_serialize_with_the_complete_decision() {
    for policy in [false, true] {
        let p = root(if policy { "policy-race" } else { "raw-race" });
        let s = Arc::new(Store::open(&p, config()).unwrap());
        let r = request(
            &s,
            "complete-decision",
            vec![
                FinalChange::put("a", vec![1]),
                FinalChange::put("b", vec![2]),
            ],
        );
        let barrier = Arc::new(std::sync::Barrier::new(3));
        let decision = {
            let s = s.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                b.wait();
                publish(&s, &r)
            })
        };
        let other = {
            let s = s.clone();
            let b = barrier.clone();
            std::thread::spawn(move || {
                b.wait();
                if policy {
                    assert_eq!(
                        s.set_policy(
                            "authored",
                            "",
                            Some(membus::Class::expire(s.config.retry_ms)),
                            membus::now_ms() + 30000
                        )
                        .code,
                        0
                    );
                } else {
                    put(&s, "raw-phantom", "phantom");
                }
            })
        };
        barrier.wait();
        let result = decision.join().unwrap();
        other.join().unwrap();
        assert!(
            result.is_ok()
                || result
                    == Err(if policy {
                        PublicationError::Unsupported
                    } else {
                        PublicationError::Conflict
                    })
        );
        assert_eq!(s.get("authored", "a").is_some(), result.is_ok());
        assert_eq!(s.get("authored", "b").is_some(), result.is_ok());
        if policy {
            assert!(matches!(
                s.capture_namespace("authored"),
                Err(PublicationError::Unsupported)
            ));
        }
    }
}
#[test]
fn atomic_retry_admission_reclaims_expired_capacity_without_rerunning_old_original() {
    let p = root("retry-reclaim");
    let clock = membus::ManualClock::new(membus::now_ms());
    let s = Store::open(
        &p,
        Config {
            dedup_cap: 1,
            retry_ms: 10,
            clock: Some(clock.clone()),
            ..config()
        },
    )
    .unwrap();
    let first = request(&s, "first", vec![FinalChange::remove("absent")]);
    publish(&s, &first).unwrap();
    clock.advance(11);
    let second = request(&s, "second", vec![FinalChange::put("value", vec![1])]);
    publish(&s, &second).unwrap();
    assert_eq!(publish(&s, &first), Err(PublicationError::RetryExpired));
    assert_eq!(s.get("authored", "value").unwrap().bytes, vec![1]);
}
#[test]
fn atomic_envelope_survives_complete_capture_and_only_quarantined_recovery() {
    use membus::{
        client::Ack,
        recovery_image::{recover_quarantined, validate_image, CaptureRequest},
    };
    let root = root("capture-envelope");
    let original = root.join("original");
    let image = root.join("image");
    let working = root.join("working");
    let s = Store::open(&original, config()).unwrap();
    let r = request(
        &s,
        "original-before-capture",
        vec![
            FinalChange::put("a", b"one".to_vec()),
            FinalChange::put("b", b"two".to_vec()),
        ],
    );
    r.persist_new(&original.join("operation.custody")).unwrap();
    // The caller could lose this reply; the envelope still owns the original request.
    publish(&s, &r).unwrap();
    let manifest = s
        .capture_stopped(
            &image,
            CaptureRequest {
                source_label: "authored-atomic-image".into(),
                ack: Ack::Flush,
            },
        )
        .unwrap();
    let copy = recover_quarantined(&image, &working, config()).unwrap();
    assert_eq!(
        copy.try_get("authored", "a").unwrap().unwrap().bytes,
        b"one"
    );
    assert_eq!(
        copy.try_get("authored", "b").unwrap().unwrap().bytes,
        b"two"
    );
    assert!(copy.health2().read_only);
    drop(copy);
    assert!(Store::open(&working, config()).is_err());
    assert_eq!(validate_image(&image).unwrap(), manifest);
}
#[test]
fn nonexpiring_latest_class_is_explicitly_outside_the_initial_compatible_scope() {
    let p = root("latest-unsupported");
    let s = Store::open(&p, config()).unwrap();
    let r = request(&s, "original", vec![FinalChange::put("a", vec![1])]);
    assert_eq!(
        s.set_policy(
            "authored",
            "",
            Some(membus::Class::LATEST),
            membus::now_ms() + 30000
        )
        .code,
        0
    );
    assert_eq!(publish(&s, &r), Err(PublicationError::Unsupported));
    assert!(matches!(
        s.capture_namespace("authored"),
        Err(PublicationError::Unsupported)
    ));
}
