//! Recovery admits the final charged summary population before exposing a Store.
use super::*;

fn seed(f: &Fixture, clock: &Arc<ManualClock>, live: bool, keys: Vec<String>) {
    let store = f.open(clock);
    if live {
        assert_eq!(
            store
                .mutate(Mutation {
                    ack: 2,
                    ..mutation("summary-admission-live", u64::MAX)
                })
                .code,
            0
        );
    }
    store
        .chunks
        .lock()
        .unwrap()
        .log(
            UTC,
            chunks::Rec::Summary {
                retired_ms: UTC,
                keys: keys
                    .into_iter()
                    .map(|key| ("summary".into(), key))
                    .collect(),
            },
        )
        .unwrap();
    drop(store);
    f.unlock();
}

fn assert_refused(f: &Fixture, error: &io::Error, keys: u64) {
    assert_eq!(
        error.to_string(),
        "replayed state exceeds configured limits"
    );
    let report = fs::read_to_string(f.0.join("replay.json")).unwrap();
    assert!(report.contains("\"stage\":\"refused\""), "{report}");
    assert_eq!(elapsed_json_number(&report, "ready_ms"), 0);
    assert_eq!(elapsed_json_number(&report, "epoch"), 2);
    assert_eq!(elapsed_json_number(&report, "keys"), keys);
    let history = fs::read_to_string(f.0.join("replay.log")).unwrap();
    assert_eq!(history.lines().last(), Some(report.trim_end()));
}

#[test]
fn retained_summaries_cannot_reopen_above_the_global_index_cap() {
    for live in [false, true] {
        let f = Fixture::new();
        let manual = ManualClock::new(UTC);
        seed(
            &f,
            &manual,
            live,
            vec!["expired-a".into(), "expired-b".into()],
        );
        let catalogue = fs::read(f.0.join("chunks/catalogue.log")).unwrap();
        let mut config = f.config(&manual);
        config.index_cap = 1 + usize::from(live);
        let result = Store::open(&f.0, config);
        assert!(
            result.is_err(),
            "a serving Store must not exceed its index cap through summaries"
        );
        let error = result.err().unwrap();
        assert_refused(&f, &error, 2 + u64::from(live));
        assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 2u64.to_le_bytes());
        // Summary records remain intact; an open live chunk may have gained Close.
        assert!(fs::read(f.0.join("chunks/catalogue.log"))
            .unwrap()
            .starts_with(&catalogue));
        f.unlock();
        let store = f.open(&manual);
        assert_eq!(store.epoch, 3);
        assert_eq!(store.replay_report().keys, 2 + usize::from(live));
        assert_eq!(store.get_status("summary", "expired-a"), (1, 0));
        if live {
            assert_eq!(
                store.get("clock", "key").unwrap().bytes,
                b"summary-admission-live"
            );
        }
    }
}

#[test]
fn final_index_cap_observes_summaries_retained_after_a_backward_publication_correction() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    seed(
        &f,
        &manual,
        false,
        vec!["expired-a".into(), "expired-b".into()],
    );
    manual.advance(WINDOW);
    let root = f.0.clone();
    let clock = manual.clone();
    let mut config = f.config(&manual);
    config.index_cap = 1;
    config.summary_ms = WINDOW;
    config.recovery_dir_sync = Some(Arc::new(move |dir| {
        if dir == root && fs::read(root.join("epoch"))? == 2u64.to_le_bytes() {
            clock.jump_utc(-1);
        }
        Ok(())
    }));
    let result = Store::open(&f.0, config);
    assert!(
        result.is_err(),
        "current summary ages at final cleanup determine serving slots"
    );
    assert_refused(&f, &result.err().unwrap(), 2);
}

#[test]
fn retained_summaries_at_the_exact_cap_keep_their_slots_and_live_counts() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    seed(&f, &manual, true, vec!["expired".into()]);
    let mut config = f.config(&manual);
    config.index_cap = 2;
    let store = Store::open(&f.0, config).unwrap();
    assert_eq!(store.replay_report().keys, 2);
    assert_eq!(store.replay_report().namespace_counts.len(), 1);
    assert_eq!(store.replay_report().namespace_counts["clock"], 1);
    assert_eq!(store.get_status("summary", "expired"), (1, 0));
    assert_eq!(
        store
            .mutate(Mutation {
                key: "new".into(),
                ..mutation("at-cap", u64::MAX)
            })
            .code,
        9
    );
}

#[test]
fn expired_summaries_do_not_spend_slots_in_the_final_recovered_index() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    seed(
        &f,
        &manual,
        false,
        vec!["expired-a".into(), "expired-b".into()],
    );
    manual.advance(WINDOW);
    let mut config = f.config(&manual);
    config.summary_ms = WINDOW;
    config.index_cap = 0;
    let store = Store::open(&f.0, config).unwrap();
    assert_eq!(store.replay_report().keys, 0);
    assert_eq!(store.get_status("summary", "expired-a"), (2, 0));
    assert_eq!(store.health2().keys, 0);
}

fn expired_summary_budget_case(expires_during_publication: bool) {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let key = "r".repeat(MAX_ID);
    seed(&f, &manual, false, vec![key.clone(), "s".repeat(MAX_ID)]);
    let mut config = f.config(&manual);
    config.summary_ms = WINDOW;
    config.capacity = 8192;
    if expires_during_publication {
        let root = f.0.clone();
        let clock = manual.clone();
        config.recovery_dir_sync = Some(Arc::new(move |dir| {
            if dir == root && fs::read(root.join("epoch"))? == 2u64.to_le_bytes() {
                clock.advance(WINDOW);
            }
            Ok(())
        }));
    } else {
        manual.advance(WINDOW);
    }
    let store = Store::open(&f.0, config).unwrap_or_else(|error| {
        panic!("obsolete summary memory must not refuse an empty serving projection: {error}")
    });
    assert_eq!(store.replay_report().keys, 0);
    assert_eq!(store.get_status("summary", &key), (2, 0));
    assert_eq!(store.chunks.lock().unwrap().index_bytes, 0);
    assert_eq!(
        store
            .mutate(Mutation {
                ack: 2,
                ..mutation("fits", u64::MAX)
            })
            .code,
        0
    );
    assert_eq!(store.get("clock", "key").unwrap().bytes, b"fits");
    assert!(store.health2().memory_bytes <= 8192);
}

#[test]
fn obsolete_summary_memory_does_not_refuse_an_empty_restart() {
    expired_summary_budget_case(false);
}

#[test]
fn summary_memory_expiring_during_publication_does_not_refuse_startup() {
    expired_summary_budget_case(true);
}

#[test]
fn current_summary_memory_still_refuses_above_the_serving_budget() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let key = "r".repeat(MAX_ID);
    seed(&f, &manual, false, vec![key, "s".repeat(MAX_ID)]);
    let catalogue = fs::read(f.0.join("chunks/catalogue.log")).unwrap();
    let mut config = f.config(&manual);
    config.capacity = 8192;
    config.summary_ms = WINDOW;
    let result = Store::open(&f.0, config);
    assert!(
        result.is_err(),
        "current summaries must still fit the serving memory budget"
    );
    assert_refused(&f, &result.err().unwrap(), 2);
    assert_eq!(
        fs::read(f.0.join("chunks/catalogue.log")).unwrap(),
        catalogue
    );
}

#[test]
fn summary_memory_equal_to_the_serving_budget_remains_admitted() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let key = "at-budget".to_owned();
    seed(&f, &manual, false, vec![key.clone()]);
    let charge = crate::store::index_charge(&("summary".into(), key.clone()));
    let capacity = (charge..=charge * 2)
        .find(|cap| cap - cap / 8 == charge)
        .unwrap();
    let mut config = f.config(&manual);
    config.capacity = capacity;
    let store = Store::open(&f.0, config).unwrap();
    assert_eq!(store.chunks.lock().unwrap().index_bytes, charge);
    assert_eq!(store.replay_report().keys, 1);
    assert_eq!(store.get_status("summary", &key), (1, 0));
    assert!(store.health2().memory_bytes <= capacity);
}
