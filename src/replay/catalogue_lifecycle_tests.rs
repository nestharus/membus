//! Catalogue lifecycle references cannot authorize unrelated suppression or revival.
use super::*;

#[path = "catalogue_lifecycle_tests/retired_admission.rs"]
mod retired_admission;

fn append(f: &Fixture, records: Vec<chunks::Rec>) {
    let mut cat = chunks::Catalogue::open(&f.0.join("chunks/catalogue.log")).unwrap();
    for rec in records {
        cat.append(&chunks::CatRecord { at_ms: UTC, rec }).unwrap();
    }
}

fn refused(f: &Fixture, data: &Path, reason: &str) {
    let cat_path = f.0.join("chunks/catalogue.log");
    let catalogue = fs::read(&cat_path).unwrap();
    let bytes = fs::read(data).unwrap();
    let result = Store::open(&f.0, f.config(&ManualClock::new(UTC)));
    let error = result.err().unwrap_or_else(|| {
        panic!(
            "invalid lifecycle must refuse; data/tail preserved={}",
            fs::read(data).is_ok_and(|b| b == bytes)
        )
    });
    assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    assert!(error.to_string().contains(reason), "{error}");
    assert_eq!(fs::read(&cat_path).unwrap(), catalogue);
    assert_eq!(fs::read(data).unwrap(), bytes);
    assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 1u64.to_le_bytes());
    let report = fs::read_to_string(f.0.join("replay.json")).unwrap();
    assert!(report.contains("\"stage\":\"refused\""), "{report}");
    assert!(report.contains(reason), "{report}");
    assert_eq!(elapsed_json_number(&report, "ready_ms"), 0);
    assert_eq!(elapsed_json_number(&report, "epoch"), 2);
    let history = fs::read_to_string(f.0.join("replay.log")).unwrap();
    assert_eq!(history.lines().last(), Some(report.trim_end()));
}

fn seeded() -> (Fixture, Arc<ManualClock>, PathBuf, Mutation, Outcome) {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let store = f.open(&manual);
    let m = Mutation {
        ack: 2,
        ..mutation("lifecycle-value", u64::MAX)
    };
    let outcome = store.mutate(m.clone());
    assert_eq!(outcome.code, 0);
    let path = store.chunks.lock().unwrap().table[&1].path.clone();
    drop(store);
    f.unlock();
    (f, manual, path, m, outcome)
}

mod references {
    use super::*;

    #[test]
    fn orphan_close_refuses_without_accepting_its_highwater_or_repairing_data() {
        for seq in [0, 99, u64::MAX] {
            let (f, _, path, _, _) = seeded();
            let mut bytes = fs::read(&path).unwrap();
            bytes.extend(b"MBK");
            fs::write(&path, bytes).unwrap();
            append(
                &f,
                vec![chunks::Rec::Close {
                    seq,
                    bytes: 0,
                    next: u64::MAX,
                }],
            );
            refused(
                &f,
                &path,
                &format!("catalogue Close without prior Open for chunk {seq}"),
            );
        }
    }

    #[test]
    fn orphan_retiring_cannot_hide_another_chunks_value_or_retry() {
        for seq in [0, 99] {
            let (f, _, path, m, outcome) = seeded();
            append(
                &f,
                vec![chunks::Rec::Retiring {
                    seq,
                    next: 900,
                    horizons: vec![(m.ns.clone(), m.key.clone(), outcome.version)],
                    expired: vec![(m.ns, m.key)],
                }],
            );
            refused(
                &f,
                &path,
                &format!("catalogue Retiring without prior Open for chunk {seq}"),
            );
        }
    }

    #[test]
    fn orphan_retired_refuses_before_other_open_tail_repairs() {
        let (f, _, path, _, _) = seeded();
        let mut bytes = fs::read(&path).unwrap();
        bytes.extend(b"MBK");
        fs::write(&path, bytes).unwrap();
        append(&f, vec![chunks::Rec::Retired { seq: 99 }]);
        refused(
            &f,
            &path,
            "catalogue Retired without prior Open for chunk 99",
        );
    }

    #[test]
    fn a_later_open_does_not_validate_an_earlier_orphan_close() {
        let (f, _, path, _, _) = seeded();
        append(
            &f,
            vec![
                chunks::Rec::Close {
                    seq: 2,
                    bytes: 0,
                    next: 2,
                },
                chunks::Rec::Open {
                    seq: 2,
                    stripe: 0,
                    class: Class::FOREVER,
                    policy: "forever".into(),
                    next: 2,
                },
            ],
        );
        refused(&f, &path, "catalogue Close without prior Open for chunk 2");
    }

}

mod close_boundaries {
    use super::*;

    fn changed_close(delta: i64) {
        let (f, _, path, _, _) = seeded();
        let len = fs::metadata(&path).unwrap().len();
        append(
            &f,
            vec![
                chunks::Rec::Close {
                    seq: 1,
                    bytes: len.checked_add_signed(delta).unwrap(),
                    next: 2,
                },
                chunks::Rec::Close {
                    seq: 1,
                    bytes: len,
                    next: 2,
                },
            ],
        );
        refused(
            &f,
            &path,
            "catalogue Close changes closed byte boundary for chunk 1",
        );
    }

    #[test]
    fn a_later_close_cannot_increase_a_known_closed_boundary() {
        changed_close(-1);
    }

    #[test]
    fn a_later_close_cannot_shrink_a_known_closed_boundary() {
        changed_close(1);
    }

    fn revival(retired: bool) {
        let (f, _, path, _, _) = seeded();
        let len = fs::metadata(&path).unwrap().len();
        let mut records = vec![
            chunks::Rec::Close {
                seq: 1,
                bytes: len,
                next: 2,
            },
            chunks::Rec::Retiring {
                seq: 1,
                next: 2,
                horizons: Vec::new(),
                expired: Vec::new(),
            },
        ];
        if retired {
            records.push(chunks::Rec::Retired { seq: 1 });
        }
        records.push(chunks::Rec::Close {
            seq: 1,
            bytes: len,
            next: 2,
        });
        append(&f, records);
        refused(&f, &path, "catalogue Close after retirement for chunk 1");
    }

    #[test]
    fn a_close_cannot_revive_a_retiring_chunks_values_and_retry() {
        revival(false);
    }

    #[test]
    fn a_close_cannot_revive_a_retired_chunks_leftover_file() {
        revival(true);
    }

    #[test]
    fn an_identical_close_boundary_remains_replayable_and_rewritable() {
        let (f, manual, path, m, outcome) = seeded();
        let len = fs::metadata(&path).unwrap().len();
        append(
            &f,
            vec![
                chunks::Rec::Close {
                    seq: 1,
                    bytes: len,
                    next: 2,
                },
                chunks::Rec::Close {
                    seq: 1,
                    bytes: len,
                    next: 3,
                },
            ],
        );
        let mut store = f.open(&manual);
        for epoch in [2, 3] {
            assert_eq!(store.epoch, epoch);
            assert_eq!(store.get(&m.ns, &m.key).unwrap().bytes, m.bytes);
            assert_eq!(store.mutate(m.clone()), outcome);
            assert_eq!(store.state.lock().unwrap().next, 3);
            store.chunks.lock().unwrap().rewrite_live(UTC, 3).unwrap();
            drop(store);
            f.unlock();
            store = f.open(&manual);
        }
    }

}

mod retiring_states {
    use super::*;

    fn transition(seq: u64) -> chunks::Rec {
        chunks::Rec::Retiring {
            seq,
            next: 2,
            horizons: Vec::new(),
            expired: Vec::new(),
        }
    }

    #[test]
    fn retiring_an_open_populated_chunk_refuses_instead_of_deleting_it() {
        let (f, _, path, _, _) = seeded();
        append(&f, vec![transition(1)]);
        refused(
            &f,
            &path,
            "catalogue Retiring requires Closed or Retiring chunk 1",
        );
    }

    #[test]
    fn retiring_an_open_without_a_file_cannot_hide_another_chunks_value() {
        let (f, _, path, m, outcome) = seeded();
        append(
            &f,
            vec![
                chunks::Rec::Open {
                    seq: 2,
                    stripe: 0,
                    class: Class::FOREVER,
                    policy: "forever".into(),
                    next: 2,
                },
                chunks::Rec::Retiring {
                    seq: 2,
                    next: 2,
                    horizons: vec![(m.ns, m.key, outcome.version)],
                    expired: Vec::new(),
                },
            ],
        );
        refused(
            &f,
            &path,
            "catalogue Retiring requires Closed or Retiring chunk 2",
        );
    }

    #[test]
    fn restarting_retirement_of_a_retired_file_refuses_before_deletion() {
        let (f, _, path, _, _) = seeded();
        append(
            &f,
            vec![
                chunks::Rec::Close {
                    seq: 1,
                    bytes: fs::metadata(&path).unwrap().len(),
                    next: 2,
                },
                transition(1),
                chunks::Rec::Retired { seq: 1 },
                transition(1),
            ],
        );
        refused(
            &f,
            &path,
            "catalogue Retiring requires Closed or Retiring chunk 1",
        );
    }

    #[test]
    fn restarting_an_empty_retired_identity_cannot_add_horizons_or_summaries() {
        let (f, _, path, m, outcome) = seeded();
        append(
            &f,
            vec![
                chunks::Rec::Open {
                    seq: 2,
                    stripe: 0,
                    class: Class::FOREVER,
                    policy: "forever".into(),
                    next: 2,
                },
                chunks::Rec::Retired { seq: 2 },
                chunks::Rec::Retiring {
                    seq: 2,
                    next: 2,
                    horizons: vec![(m.ns.clone(), m.key.clone(), outcome.version)],
                    expired: vec![(m.ns, m.key)],
                },
            ],
        );
        refused(
            &f,
            &path,
            "catalogue Retiring requires Closed or Retiring chunk 2",
        );
    }

    #[test]
    fn an_incomplete_retiring_prefix_does_not_change_an_open_chunks_lifecycle() {
        let (f, manual, path, m, outcome) = seeded();
        append(
            &f,
            vec![chunks::Rec::RetiringPart {
                seq: 1,
                next: 999,
                index: 0,
                count: 2,
                horizons: vec![(m.ns.clone(), m.key.clone(), outcome.version)],
                expired: vec![(m.ns.clone(), m.key.clone())],
            }],
        );
        let store = f.open(&manual);
        assert!(path.exists());
        assert_eq!(store.get(&m.ns, &m.key).unwrap().bytes, m.bytes);
        assert_eq!(store.mutate(m), outcome);
        assert_eq!(store.state.lock().unwrap().next, 2);
        assert!(store.replay_report().retirements_finished.is_empty());
    }
}

mod retiring_duplicates {
    use super::*;

    fn changed(kind: &str) {
        let (f, _, path, m, outcome) = seeded();
        append(
            &f,
            vec![
                chunks::Rec::Close {
                    seq: 1,
                    bytes: fs::metadata(&path).unwrap().len(),
                    next: 2,
                },
                chunks::Rec::Retiring {
                    seq: 1,
                    next: 2,
                    horizons: Vec::new(),
                    expired: Vec::new(),
                },
            ],
        );
        let at_ms = match kind {
            "later" => UTC + 1,
            "earlier" => UTC - 1,
            _ => UTC,
        };
        let horizons = if kind == "horizons" {
            vec![(m.ns.clone(), m.key.clone(), outcome.version)]
        } else {
            Vec::new()
        };
        let expired = if kind == "expired" {
            vec![(m.ns, m.key)]
        } else {
            Vec::new()
        };
        let mut cat = chunks::Catalogue::open(&f.0.join("chunks/catalogue.log")).unwrap();
        cat.append(&chunks::CatRecord {
            at_ms,
            rec: chunks::Rec::Retiring {
                seq: 1,
                next: 9,
                horizons,
                expired,
            },
        })
        .unwrap();
        drop(cat);
        refused(
            &f,
            &path,
            "catalogue Retiring changes committed transition for chunk 1",
        );
    }

    #[test]
    fn repeated_retiring_cannot_change_committed_metadata() {
        for kind in ["horizons", "expired", "later", "earlier"] { changed(kind); }
    }

    #[test]
    fn identical_retiring_with_a_newer_highwater_retains_its_existing_meaning() {
        let (f, manual, path, _, _) = seeded();
        append(
            &f,
            vec![
                chunks::Rec::Close {
                    seq: 1,
                    bytes: fs::metadata(&path).unwrap().len(),
                    next: 2,
                },
                chunks::Rec::Retiring {
                    seq: 1,
                    next: 2,
                    horizons: Vec::new(),
                    expired: Vec::new(),
                },
                chunks::Rec::Retiring {
                    seq: 1,
                    next: 9,
                    horizons: Vec::new(),
                    expired: Vec::new(),
                },
            ],
        );
        let store = f.open(&manual);
        assert!(!path.exists());
        assert_eq!(store.state.lock().unwrap().next, 9);
        assert_eq!(store.replay_report().retirements_finished, vec![1]);
        assert_eq!(store.chunks.lock().unwrap().table[&1].retired_ms, Some(UTC));
    }

    #[test]
    fn reordered_sets_and_redundant_members_match_a_complete_multipart_repeat() {
        let (f, manual, path, _, _) = seeded();
        append(
            &f,
            vec![
                chunks::Rec::Close {
                    seq: 1,
                    bytes: fs::metadata(&path).unwrap().len(),
                    next: 2,
                },
                chunks::Rec::Retiring {
                    seq: 1,
                    next: 2,
                    horizons: vec![
                        ("ns".into(), "a".into(), 4),
                        ("ns".into(), "b".into(), 3),
                        ("ns".into(), "a".into(), 2),
                    ],
                    expired: vec![("ns".into(), "b".into()), ("ns".into(), "a".into())],
                },
                chunks::Rec::RetiringPart {
                    seq: 1,
                    next: 3,
                    index: 0,
                    count: 2,
                    horizons: vec![("ns".into(), "b".into(), 3)],
                    expired: vec![("ns".into(), "a".into())],
                },
                chunks::Rec::RetiringPart {
                    seq: 1,
                    next: 3,
                    index: 1,
                    count: 2,
                    horizons: vec![("ns".into(), "a".into(), 4)],
                    expired: vec![("ns".into(), "b".into()), ("ns".into(), "a".into())],
                },
            ],
        );
        let store = f.open(&manual);
        assert!(!path.exists());
        assert_eq!(store.state.lock().unwrap().next, 3);
        let cs = store.chunks.lock().unwrap();
        assert_eq!(cs.table[&1].retired_ms, Some(UTC));
        assert_eq!(cs.summaries.keys.len(), 2);
        assert_eq!(
            cs.summaries.list.keys().copied().collect::<Vec<_>>(),
            vec![UTC]
        );
    }

    #[test]
    fn a_live_list_rewrite_of_repeated_retiring_preserves_the_original_summary_age() {
        let (f, manual, path, m, _) = seeded();
        append(
            &f,
            vec![
                chunks::Rec::Close {
                    seq: 1,
                    bytes: fs::metadata(&path).unwrap().len(),
                    next: 2,
                },
                chunks::Rec::Retiring {
                    seq: 1,
                    next: 2,
                    horizons: Vec::new(),
                    expired: vec![(m.ns.clone(), m.key.clone())],
                },
                chunks::Rec::Retiring {
                    seq: 1,
                    next: 2,
                    horizons: Vec::new(),
                    expired: vec![(m.ns, m.key)],
                },
            ],
        );
        let dir = f.0.join("chunks");
        let mut rp = Replayed::empty(dir.clone());
        replay_into(
            &mut rp,
            &f.0,
            &dir,
            true,
            &f.config(&manual),
            &chunks::Clock::new(Some(manual.clone())),
        )
        .unwrap();
        let live = rp.chunks.live_list(UTC + 50, rp.next);
        chunks::Catalogue::open(&dir.join("catalogue.log"))
            .unwrap()
            .rewrite(&live)
            .unwrap();
        manual.advance(50);
        let store = f.open(&manual);
        assert!(!path.exists());
        let cs = store.chunks.lock().unwrap();
        assert_eq!(cs.table[&1].retired_ms, Some(UTC));
        assert_eq!(
            cs.summaries.list.keys().copied().collect::<Vec<_>>(),
            vec![UTC]
        );
        assert_eq!(store.epoch, 2);
    }
}
