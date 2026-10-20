    use super::*;
    fn store(label: &str, stop: u8) -> (Store, Arc<ManualClock>) {
        let p = PathBuf::from(std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()))
            .join(format!("retirement-failure-{label}-{}-{}", std::process::id(), now_ms()));
        let clock = ManualClock::new(now_ms());
        let s = Store::open(&p, Config { clock: Some(clock.clone()), tick_ms: 3_600_000,
            retire_ms: 3_600_000, rotation_ms: 1, retire_stop_after: stop, ..Config::default() }).unwrap();
        let day = 86_400_000;
        assert_eq!(s.set_policy("expiry", "", Some(Class::expire(2 * day)), now_ms() + 10_000).code, 0);
        let a = "a";
        let b = (0..100).map(|i| format!("b{i}")).find(|k| stripe("expiry", k) != stripe("expiry", a)).unwrap();
        for (n, k) in [a, &b].iter().enumerate() {
            assert_eq!(s.mutate(Mutation { ns: "expiry".into(), key: k.to_string(), op_id: format!("put{n}"),
                deadline: now_ms() + 10_000, ack: 1, expected: None, bytes: b"value".to_vec(), remove: false }).code, 0);
        }
        clock.advance(2);
        s.tick();
        clock.advance(2 * day);
        (s, clock)
    }
    fn expired_chunks(s: &Store) -> Vec<(u64, u64)> {
        s.chunks.lock().unwrap().table.values().filter(|c| c.class.kind == 2).map(|c| (c.id, c.bytes)).collect()
    }
    fn inject_after(s: &Store, n: usize) { s.chunks.lock().unwrap().cat.as_mut().unwrap().fail_after_once = Some(n); }
    fn assert_readable_index_agrees(s: &Store) {
        let state = s.state.lock().unwrap();
        let committed = s.committed.read().unwrap();
        for (k, v) in committed.iter() {
            assert!(state.metadata.get(k).is_some_and(|m| m.version == v.version), "readable expired value left behind: {k:?}");
        }
    }
    #[test]
    fn later_retiring_failure_cleans_up_earlier_expired_values() {
        let (s, _) = store("index", 0);
        let original: HashMap<_, _> = s.committed.read().unwrap().iter()
            .filter(|(k, _)| k.0 == "expiry").map(|(k, v)| (k.clone(), (**v).clone())).collect();
        assert_eq!(original.len(), 2);
        inject_after(&s, 1);
        assert!(s.maint.retire(None).is_err());
        assert_readable_index_agrees(&s);
        let closed = s.chunks.lock().unwrap().table.values().filter(|c| c.class.kind == 2 && c.state == chunks::ChunkState::Closed).count();
        assert_eq!(closed, 1);
        let retained: Vec<_> = s.state.lock().unwrap().metadata.iter()
            .filter(|(k, _)| k.0 == "expiry").map(|(k, m)| (k.clone(), m.version)).collect();
        assert_eq!(retained.len(), 1, "failed second retirement retains one live value");
        for (key, version) in &retained {
            let value = s.get(&key.0, &key.1).expect("still-unretired value must remain readable");
            assert_eq!(value.version, *version);
            assert_eq!(value.bytes, original[key].bytes);
            assert_eq!(value.version, original[key].version);
        }
        for key in original.keys().filter(|k| !retained.iter().any(|(r, _)| r == *k)) {
            assert!(s.get(&key.0, &key.1).is_none(), "successfully retired prefix must leave the read index");
        }
        s.maint.retire(None).unwrap();
        s.retry_retiring().unwrap();
        assert_eq!(s.slots("expiry").1, 0);
        assert_readable_index_agrees(&s);
    }
    #[test]
    fn unresolved_retiring_append_refuses_restart_without_deleting_chunks() {
        let (s, _) = store("restart-unresolved", 0);
        let original: Vec<_> = s.committed.read().unwrap().iter()
            .filter(|(k, _)| k.0 == "expiry").map(|(k, v)| (k.clone(), (**v).clone())).collect();
        assert_eq!(original.len(), 2);
        let files: Vec<_> = s.chunks.lock().unwrap().table.values()
            .filter(|c| c.class.kind == 2).map(|c| (c.path.clone(), fs::read(&c.path).unwrap())).collect();
        assert_eq!(files.len(), 2);
        {
            let mut chunks = s.chunks.lock().unwrap();
            let cat = chunks.cat.as_mut().unwrap();
            cat.fail_sync_once = true;
            cat.fail_recovery_once = true;
        }
        assert!(s.maint.retire(None).unwrap_err().to_string().contains("boundary recovery failed"));
        for (key, old) in &original {
            let value = s.get(&key.0, &key.1).expect("failed retirement must preserve live values");
            assert_eq!(value.version, old.version);
            assert_eq!(value.bytes, old.bytes);
        }
        let path = s.path.clone();
        let catalogue = fs::read(path.join("chunks/catalogue.log")).unwrap();
        drop(s);
        let error = Store::open(&path, Config::default()).err().expect("unresolved retirement must refuse replay");
        assert!(error.to_string().contains("append unresolved"), "{error}");
        assert_eq!(fs::read(path.join("chunks/catalogue.log")).unwrap(), catalogue);
        for (file, bytes) in files {
            assert_eq!(fs::read(file).unwrap(), bytes, "restart refusal must retain every protected chunk");
        }
        let report = fs::read_to_string(path.join("replay.json")).unwrap();
        assert!(report.contains("append unresolved"), "recovery responsibility must remain observable");
    }
    #[test]
    fn later_retired_failure_keeps_successful_byte_reclamation() {
        let (s, _) = store("bytes", 0);
        let ids = expired_chunks(&s);
        assert_eq!(ids.len(), 2);
        let before = s.state.lock().unwrap().persisted_bytes;
        inject_after(&s, 3); // two Retiring records, one successful Retired record, then failure
        assert!(s.maint.retire(None).is_err());
        assert_eq!(s.state.lock().unwrap().persisted_bytes, before - ids[0].1);
        assert_readable_index_agrees(&s);
        assert_eq!(s.retry_retiring().unwrap(), 1);
        assert_eq!(s.state.lock().unwrap().persisted_bytes, before - ids.iter().map(|x| x.1).sum::<u64>());
    }
    #[test]
    fn retry_failure_keeps_prior_successful_byte_reclamation() {
        let (s, _) = store("retry", 2);
        let ids = expired_chunks(&s);
        let before = s.state.lock().unwrap().persisted_bytes;
        s.maint.retire(None).unwrap();
        inject_after(&s, 1);
        assert!(s.retry_retiring().is_err());
        assert_eq!(s.state.lock().unwrap().persisted_bytes, before - ids[0].1);
        assert_eq!(s.retry_retiring().unwrap(), 1);
        assert_eq!(s.state.lock().unwrap().persisted_bytes, before - ids.iter().map(|x| x.1).sum::<u64>());
    }
use crate as bus;
#[path = "../../tests/support/retirement_fixture.rs"]
mod retiring_fixture;

#[cfg(windows)]
#[test]
fn retiring_parts_crash_writer() {
    let Ok(handoff) = std::env::var("MEMBUS_RETIRING_CRASH_HANDOFF") else {
        return;
    };
    let f = retiring_fixture::Fixture::new("retiring-process-crash", 8100);
    let s = f.open();
    f.admit_policy(&s);
    let before = s.chunks.lock().unwrap().cat_bytes();
    s.chunks
        .lock()
        .unwrap()
        .cat
        .as_mut()
        .unwrap()
        .fail_after_once = Some(1);
    assert!(s.maint.retire(None).is_err());
    assert!(
        s.chunks.lock().unwrap().cat_bytes() > before,
        "the first part must be durable before termination"
    );
    f.assert_live(&s);
    fs::write(handoff, f.path.to_str().unwrap()).unwrap();
    // Terminate without Store::drop or thread teardown, between resolved part appends.
    std::process::exit(0);
}

#[cfg(windows)]
#[test]
fn retiring_parts_abrupt_process_exit_replays_prefix_then_retires() {
    use std::os::windows::process::CommandExt;
    use std::process::{Command, Stdio};
    // This child runs the Store witness without helper processes. Keep its handle through
    // every assertion so panic/timeout also terminates and reaps that process.
    struct PartWriter(std::process::Child);
    impl Drop for PartWriter {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let root = retiring_fixture::path("retiring-crash-handoff");
    fs::create_dir_all(&root).unwrap();
    let handoff = root.join("state-path.txt");
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "retirement_failure_tests::retiring_parts_crash_writer",
            "--nocapture",
        ])
        .env("MEMBUS_RETIRING_CRASH_HANDOFF", &handoff)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    let mut child = PartWriter(command.creation_flags(0x08000000).spawn().unwrap());
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "the crash witness exceeded its deadline"
        );
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(
        status.success(),
        "the part writer failed before its abrupt exit"
    );
    let f = retiring_fixture::Fixture::resume(
        PathBuf::from(fs::read_to_string(handoff).unwrap()),
        8100,
    );
    let s = f.open();
    assert!(s.replay_report().retirements_finished.is_empty());
    assert!(s.chunks.lock().unwrap().horizons.is_empty());
    assert!(s.chunks.lock().unwrap().summaries.keys.is_empty());
    assert_eq!(s.slots("aggregate").1, 8100);
    assert_eq!(
        s.chunk_rows().iter().find(|row| row.0 == 2).unwrap().3,
        "Closed"
    );
    assert!(chunks::chunk_path(&f.path.join("chunks"), Class::expire(1000), 0, 2).exists());
    f.assert_live(&s);
    s.retirement_pass();
    f.assert_expired(&s);
    assert_eq!(
        s.chunk_rows().iter().find(|row| row.0 == 2).unwrap().3,
        "Retired"
    );
    println!("abrupt part-writer exit observed; direct child waited and reaped; prefix ignored and retirement retried");
}

#[test]
fn retiring_parts_partial_failure_replays_without_effects_then_retries() {
    let f = retiring_fixture::Fixture::new("retiring-partial", 8100);
    assert!(chunks::encode(&f.transition(30_000)).len() - 16 > MAX_FRAME);
    let s = f.open();
    f.admit_policy(&s);
    let before_bytes = s.chunks.lock().unwrap().cat_bytes();
    let before_counts = s.state.lock().unwrap().namespace_counts.clone();
    let before_slots = s.slots("aggregate");
    let before_disk = s.state.lock().unwrap().persisted_bytes;
    let before_next = s.state.lock().unwrap().next;
    let file = chunks::chunk_path(&f.path.join("chunks"), Class::expire(1000), 0, 2);
    let original = fs::read(&file).unwrap();
    s.chunks
        .lock()
        .unwrap()
        .cat
        .as_mut()
        .unwrap()
        .fail_after_once = Some(1);
    assert!(s.maint.retire(None).is_err());
    assert!(
        s.chunks.lock().unwrap().cat_bytes() > before_bytes,
        "a durable first part must precede the interruption"
    );
    assert!(s.storage_error());
    assert_eq!(
        s.chunk_rows().iter().find(|row| row.0 == 2).unwrap().3,
        "Closed"
    );
    assert_eq!(s.slots("aggregate"), before_slots);
    assert_eq!(s.state.lock().unwrap().namespace_counts, before_counts);
    assert_eq!(s.state.lock().unwrap().persisted_bytes, before_disk);
    assert_eq!(s.state.lock().unwrap().next, before_next);
    assert!(s.chunks.lock().unwrap().horizons.is_empty());
    assert!(s.chunks.lock().unwrap().summaries.keys.is_empty());
    assert_eq!(fs::read(&file).unwrap(), original);
    f.assert_live(&s);
    drop(s); // Model a process interruption between two complete, resolved catalogue appends.
    let s = f.open();
    assert_eq!(
        s.chunk_rows().iter().find(|row| row.0 == 2).unwrap().3,
        "Closed"
    );
    assert_eq!(s.slots("aggregate"), before_slots);
    assert_eq!(s.state.lock().unwrap().namespace_counts, before_counts);
    assert_eq!(s.state.lock().unwrap().persisted_bytes, before_disk);
    assert_eq!(s.state.lock().unwrap().next, before_next);
    assert!(s.chunks.lock().unwrap().horizons.is_empty());
    assert!(s.chunks.lock().unwrap().summaries.keys.is_empty());
    assert_eq!(fs::read(&file).unwrap(), original);
    f.assert_live(&s);
    let rewrite_path = f.path.join("chunks/catalogue.log.tmp");
    fs::create_dir(&rewrite_path).unwrap();
    assert!(s.maint.retire(None).is_err());
    assert!(s.health2().catalogue_rewrite_error.is_some());
    assert_eq!(
        s.chunk_rows().iter().find(|row| row.0 == 2).unwrap().3,
        "Closed"
    );
    assert_eq!(fs::read(&file).unwrap(), original);
    f.assert_live(&s);
    fs::remove_dir(&rewrite_path).unwrap();
    s.retirement_pass();
    assert!(s.health2().catalogue_rewrite_error.is_none());
    f.assert_expired(&s);
    assert_eq!(
        s.chunk_rows().iter().find(|row| row.0 == 2).unwrap().3,
        "Retired"
    );
    let records = chunks::read_catalogue(&f.path.join("chunks/catalogue.log"))
        .unwrap()
        .0;
    // Only the complete retry remains: its abandoned prefix was compacted first,
    // even with ordinary threshold-based catalogue rewrites disabled.
    let parts = records
        .iter()
        .filter(|row| chunks::encode(row)[16] == 10)
        .count();
    assert_eq!(parts, 2);
    drop(s);
    f.assert_expired(&f.open());
}

#[test]
fn retiring_parts_live_rewrite_preserves_complete_transition_and_age() {
    let mut f = retiring_fixture::Fixture::new("retiring-rewrite", 8100);
    f.config.retire_stop_after = 1;
    f.config.catalogue_rewrite_bytes = 1;
    let s = f.open();
    f.admit_policy(&s);
    let original_time = f.clock.utc_ms();
    s.maint
        .retire(None)
        .expect("the large transition must publish completely");
    f.assert_expired(&s);
    assert_eq!(
        s.chunk_rows().iter().find(|row| row.0 == 2).unwrap().3,
        "Retiring"
    );
    let file = chunks::chunk_path(&f.path.join("chunks"), Class::expire(1000), 0, 2);
    assert!(file.exists());
    f.clock.advance(50);
    s.maint.rewrite_catalogue();
    assert!(s.health2().catalogue_rewrite_error.is_none());
    let records = chunks::read_catalogue(&f.path.join("chunks/catalogue.log"))
        .unwrap()
        .0;
    assert!(records
        .iter()
        .all(|row| chunks::encode(row).len() - 16 <= MAX_FRAME));
    assert!(records
        .iter()
        .filter(|row| chunks::encode(row)[16] == 10)
        .all(|row| row.at_ms == original_time));
    drop(s);
    let s = f.open();
    f.assert_expired(&s);
    assert!(s.replay_report().retirements_finished.contains(&2));
    assert!(!file.exists());
}
