    use super::*;
    fn fixture(label: &str, mut config: Config) -> Store {
        let path = PathBuf::from(std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()))
            .join(format!("namespace-{label}-{}-{}", std::process::id(), now_ms()));
        config.tick_ms = 3_600_000;
        config.retire_ms = 3_600_000;
        Store::open(&path, config).unwrap()
    }
    fn mutation(ns: &str, key: &str, id: &str, ack: u8) -> Mutation {
        Mutation { ns: ns.into(), key: key.into(), op_id: id.into(), ack,
            deadline: now_ms() + 10_000, expected: None, bytes: vec![], remove: false }
    }
    #[test]
    fn mem_completion_releases_namespace_immediately() {
        let s = fixture("mem", Config { allow_mem: true, ..Config::default() });
        assert_eq!(s.mutate(mutation("disposable", "k", "mem", 0)).code, 0);
        let state = s.state.lock().unwrap();
        assert_eq!(state.dedup.len(), 1, "retry identity is still retained");
        assert_eq!(state.keys, 0);
        assert!(state.namespace_counts.is_empty(), "no namespace slot remains after mem completion");
    }
    #[test]
    fn retry_expiry_ring_eviction_and_cleanup_keep_no_empty_namespaces() {
        let s = fixture("retry-ring", Config { allow_mem: true, retry_ms: 0, dedup_cap: 8,
            index_cap: 1, quota: 1, capacity: 65536, ..Config::default() });
        let mut first = String::new();
        for n in 0..257 {
            std::thread::sleep(Duration::from_millis(2));
            let ns = format!("{n:08}{}", "n".repeat(1016));
            if n == 0 { first = ns.clone(); }
            assert_eq!(s.mutate(mutation(&ns, "k", "mem", 0)).code, 0);
        }
        s.retirement_pass();
        s.maint.prune_discharged();
        let state = s.state.lock().unwrap();
        assert_eq!(state.dedup.len(), 1);
        assert_eq!(state.retired.len(), 8);
        assert!(!state.dedup.contains_key(&(first.clone(), "mem".into())));
        assert!(!state.retired_set.contains(&(first, "mem".into())), "oldest identity left the ring");
        assert_eq!(state.keys, 0);
        assert_eq!(state.persisted_bytes, 0);
        assert!(state.pending.is_empty());
        assert!(s.committed.read().unwrap().is_empty());
        assert_eq!(state.namespace_counts.len(), 0, "historical namespace strings must be released");
    }
    #[test]
    fn durable_remove_keeps_only_namespaces_with_live_keys() {
        let s = fixture("remove", Config::default());
        assert_eq!(s.mutate(mutation("live", "a", "a", 1)).code, 0);
        assert_eq!(s.mutate(mutation("live", "b", "b", 1)).code, 0);
        let mut remove = mutation("live", "a", "remove-a", 1);
        remove.remove = true;
        assert_eq!(s.mutate(remove).code, 0);
        assert_eq!(s.state.lock().unwrap().namespace_counts.get("live"), Some(&1));
        let mut remove = mutation("live", "b", "remove-b", 1);
        remove.remove = true;
        assert_eq!(s.mutate(remove).code, 0);
        s.retirement_pass();
        assert!(s.state.lock().unwrap().namespace_counts.is_empty());
    }
    #[test]
    fn failed_append_rolls_back_namespace_reservation() {
        let s = fixture("rollback", Config { fail_partial_append: true, ..Config::default() });
        assert_eq!(s.mutate(mutation("failed", "k", "failed", 1)).code, 16);
        let state = s.state.lock().unwrap();
        assert!(state.namespace_counts.is_empty());
        assert!(state.pending.is_empty());
        assert_eq!(state.keys, 0);
    }
    #[test]
    fn expiry_releases_namespace_while_summary_remains() {
        let clock = ManualClock::new(now_ms());
        let s = fixture("expire", Config { clock: Some(clock.clone()), retry_ms: 0, rotation_ms: 1,
            ..Config::default() });
        s.policies.write().unwrap().set("finite", "", Some(Class::expire(10)));
        assert_eq!(s.mutate(mutation("finite", "k", "finite", 1)).code, 0);
        clock.advance(2);
        s.tick();
        clock.advance(10);
        s.retirement_pass();
        s.maint.prune_discharged();
        assert_eq!(s.get_status("finite", "k"), (1, 0));
        assert!(s.state.lock().unwrap().namespace_counts.is_empty());
    }
    #[test]
    fn rewrite_failure_cause_is_visible_in_health2_and_clears_on_success() {
        let s = fixture("rewrite-health", Config { catalogue_rewrite_bytes: 1, ..Config::default() });
        assert_eq!(s.mutate(mutation("live", "k", "live", 1)).code, 0);
        let tmp = s.path.join("chunks/catalogue.log.tmp");
        fs::create_dir(&tmp).unwrap();
        let cause = File::create(&tmp).unwrap_err().to_string();
        s.maint.rewrite_catalogue();
        assert!(s.health2().storage_error);
        let mut payload = (now_ms() + 10_000).to_le_bytes().to_vec();
        lp(&mut payload, b"health"); lp(&mut payload, b"live");
        let reply = s.dispatch(&frame(HEALTH2, 1, &payload)[4..]);
        assert_eq!(&reply[17..19], &[0, 0]);
        let mut c = Cursor::new(&reply[19..]);
        let n = c.u16().unwrap();
        let fields: Vec<_> = (0..n).map(|_| String::from_utf8(c.bytes(MAX_FRAME).unwrap().to_vec()).unwrap()).collect();
        c.end().unwrap();
        assert!(fields.contains(&format!("catalogue_rewrite_error={cause}")), "missing cause: {fields:?}");
        fs::remove_dir(&tmp).unwrap();
        s.maint.rewrite_catalogue();
        assert!(!s.health2().fields().iter().any(|(key, _)| key == "catalogue_rewrite_error"));
        assert_eq!(s.get("live", "k").unwrap().bytes, b"");
    }
