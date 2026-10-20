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
