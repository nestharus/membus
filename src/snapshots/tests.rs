    use super::*;
    #[test]
    fn snapshot_registry_waiting_capture_does_not_block_existing_view() {
        let root = PathBuf::from(std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()))
            .join(format!("registry-wait-{}-{}", std::process::id(), now_ms()));
        let s = Arc::new(Store::open(&root, Config { snapshot_max: 4, snapshot_ttl_ms: 30000,
            tick_ms: 3_600_000, ..Config::default() }).unwrap());
        let (token, pinned) = s.snapshot_open("ready").unwrap();
        let blocked = s.committed.write().unwrap();
        let opening = {let s=s.clone(); std::thread::spawn(move || s.snapshot_open("waiting").unwrap())};
        let until = Instant::now()+Duration::from_secs(2);
        while s.capture_waiters.load(Ordering::SeqCst)==0 {
            assert!(Instant::now()<until); std::thread::yield_now();
        }
        let (tx,rx)=mpsc::channel();
        let reader = {let s=s.clone(); std::thread::spawn(move || {
            let view=s.snapshot_get("ready", &token).unwrap();
            s.snapshot_release("ready", &token).unwrap();
            tx.send(view.rows.len()).unwrap();
        })};
        let completed=rx.recv_timeout(Duration::from_millis(500));
        // Settle both workers and the fixture even when the old lock scope fails.
        drop(blocked);
        let (token,other)=opening.join().unwrap();
        reader.join().unwrap();
        s.snapshot_release("waiting", &token).unwrap();
        drop(other); drop(pinned);
        assert_eq!(s.scan_usage.count.load(Ordering::Acquire),0);
        assert_eq!(s.scan_usage.bytes.load(Ordering::Acquire),0);
        drop(s); std::fs::remove_dir_all(root).unwrap();
        assert_eq!(completed.expect("waiting capture blocked the admitted snapshot registry"),0);
    }

    #[test]
    fn snapshot_registry_parallel_reservations_keep_count_and_byte_limits() {
        for (label, maximum, bytes, expected) in [("count", 2, 4096, 2), ("bytes", 8, 512, 1)] {
            let root = PathBuf::from(std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()))
                .join(format!("registry-reserve-{label}-{}-{}",std::process::id(),now_ms()));
            let s=Arc::new(Store::open(&root,Config {snapshot_max:maximum,snapshot_bytes:bytes,
                snapshot_ttl_ms:30000,tick_ms:3_600_000,..Config::default()}).unwrap());
            let barrier=Arc::new(std::sync::Barrier::new(12));
            let workers:Vec<_>=(0..12).map(|_| {let s=s.clone();let barrier=barrier.clone();
                std::thread::spawn(move|| {barrier.wait();s.snapshot_open("empty")})}).collect();
            let mut admitted=Vec::new();
            for worker in workers {match worker.join().unwrap() {
                Ok(view)=>admitted.push(view), Err(code)=>assert_eq!(code,22),
            }}
            assert_eq!(admitted.len(),expected);
            assert_eq!(s.scan_usage.count.load(Ordering::Acquire),expected);
            assert_eq!(s.scan_usage.bytes.load(Ordering::Acquire),expected*261);
            for (token,_) in &admitted {s.snapshot_release("empty",token).unwrap();}
            assert_eq!(s.scan_usage.count.load(Ordering::Acquire),expected);
            drop(admitted);
            assert_eq!(s.scan_usage.count.load(Ordering::Acquire),0);
            assert_eq!(s.scan_usage.bytes.load(Ordering::Acquire),0);
            assert!(s.snapshot_open("empty").is_ok());
            drop(s);std::fs::remove_dir_all(root).unwrap();
        }
    }
    fn store(label: &str, ttl: u64) -> Store {
        let path = std::path::PathBuf::from(
            std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()),
        )
        .join(format!(
            "scan-retention-{label}-{}-{}",
            std::process::id(),
            now_ms()
        ));
        Store::open(
            &path,
            Config {
                snapshot_max: 1,
                snapshot_ttl_ms: ttl,
                ..Config::default()
            },
        )
        .unwrap()
    }
    #[test]
    fn release_keeps_in_flight_snapshot_charged() {
        let s = store("pinned", 30000);
        let (token, pinned) = s.snapshot_open("ledger").unwrap();
        s.snapshot_release("ledger", &token).unwrap();
        assert_eq!(s.scan_usage.count.load(Ordering::Acquire), 1);
        assert_eq!(s.snapshot_open("ledger").err(), Some(22));
        drop(pinned);
        assert_eq!(s.scan_usage.count.load(Ordering::Acquire), 0);
        assert_eq!(s.scan_usage.bytes.load(Ordering::Acquire), 0);
        assert!(s.snapshot_open("ledger").is_ok());
    }
    #[test]
    fn idle_expiry_reclaims_without_another_request() {
        let s = store("idle", 40);
        let (_, pinned) = s.snapshot_open("ledger").unwrap();
        drop(pinned);
        let until = Instant::now() + Duration::from_secs(2);
        while s.scan_usage.count.load(Ordering::Acquire) != 0 && Instant::now() < until {
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(s.scan_usage.count.load(Ordering::Acquire), 0);
        assert_eq!(s.scan_usage.bytes.load(Ordering::Acquire), 0);
        println!("idle expiry reclaimed snapshot without dispatch");
    }
    #[test]
    fn capture_shares_maximum_value_payload() {
        let s = store("arc", 30000);
        let mutation = Mutation {
            ns: "ledger".into(),
            key: "large".into(),
            op_id: "max".into(),
            deadline: now_ms() + 10000,
            ack: 1,
            expected: None,
            bytes: vec![7; MAX_VALUE],
            remove: false,
        };
        assert_eq!(s.mutate(mutation).code, 0);
        let live = s.get_handle("ledger", "large").unwrap();
        let (token, snapshot) = s.snapshot_open("ledger").unwrap();
        let captured = snapshot.rows[0].1.load(&s, "ledger", "large").unwrap();
        assert!(Arc::ptr_eq(&live, &captured.value));
        assert_eq!(live.bytes.as_ptr(), captured.bytes.as_ptr());
        let mut suffix = Vec::new();
        lp(&mut suffix, &token);
        lp(&mut suffix, &[]);
        suffix.extend(1u16.to_le_bytes());
        suffix.extend((MAX_FRAME as u32).to_le_bytes());
        lp(&mut suffix, b"");
        let page = s
            .scan_page("ledger", &mut Cursor::new(&suffix), SCAN_PREFIX, MAX_FRAME)
            .unwrap();
        assert_eq!(page.len(), 27 + 4 + 5 + 14 + MAX_VALUE);
        assert_eq!(
            s.scan_page("ledger", &mut Cursor::new(&suffix), SCAN_PREFIX, 1024)
                .err(),
            Some(4)
        );
    }

    #[test]
    fn blocked_cold_page_releases_index_and_survives_retirement() {
        let clock=ManualClock::new(now_ms());
        let root=PathBuf::from(std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_|".test-output".into()))
            .join(format!("cold-capture-{}-{}",std::process::id(),now_ms()));
        let s=Arc::new(Store::open(&root,Config { clock:Some(clock.clone()),
            retry_ms:10,rotation_ms:1,tick_ms:3_600_000,retire_ms:3_600_000,..Config::default() }).unwrap());
        assert_eq!(s.set_policy("cold","",Some(Class::expire(20)),now_ms()+10000).code,0);
        let mutation=|ns:&str,id:&str| Mutation { ns:ns.into(),key:"k".into(),op_id:id.into(),
            deadline:now_ms()+10000,ack:1,expected:None,bytes:b"snapshot value".to_vec(),remove:false };
        assert_eq!(s.mutate(mutation("cold","cold-put")).code,0);
        clock.advance(2);s.tick();clock.advance(20);
        let entry=s.committed.read().unwrap().get(&("cold".into(),"k".into())).unwrap().clone();
        s.cache.remove(entry.version,false);
        let snapshot=s.snapshot_open("cold").unwrap().1;
        let blocked=entry.location.file.lock().unwrap();
        let capture={let s=s.clone();std::thread::spawn(move|| {
            let value=snapshot.rows[0].1.load(&s,"cold","k").unwrap();
            (snapshot,value)
        })};
        let limit=Instant::now()+Duration::from_secs(2);
        while *s.cold_readers.0.lock().unwrap()==0 { assert!(Instant::now()<limit);std::thread::yield_now(); }
        let (tx,rx)=mpsc::channel();
        let writer={let s=s.clone();std::thread::spawn(move|| {let o=s.mutate(mutation("hot","hot-put"));tx.send(o).unwrap();})};
        let completed=rx.recv_timeout(Duration::from_secs(2));
        // Always release the fixture's blocked file before asserting, so a failure cannot hang the suite.
        if completed.is_ok() {s.retirement_pass();}
        drop(blocked);
        writer.join().unwrap();
        let (snapshot,value)=capture.join().unwrap();
        assert_eq!(completed.expect("cold page held the write publication lock").code,0);
        assert_eq!(s.get_status("cold","k"),(1,0));
        assert_eq!(value.bytes,b"snapshot value");
        assert!(Arc::ptr_eq(&value.value,&snapshot.rows[0].1.load(&s,"cold","k").unwrap().value));
        assert!(!s.cache_stats().residency.iter().any(|(class,_,_)|class=="expire(20ms)"),
            "a cold reader finishing after retirement must not reinsert the retired version");
    }

    #[test]
    fn namespace_capture_scope_ignores_unrelated_resident_keys() {
        let root = PathBuf::from(std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()))
            .join(format!("namespace-scope-{}-{}", std::process::id(), now_ms()));
        let s = Store::open(&root, Config { cache_bytes: 0, tick_ms: 3_600_000, ..Config::default() }).unwrap();
        for i in 0..2052 {
            let ns = if i < 4 { "focus" } else { "other" };
            assert_eq!(s.mutate(Mutation { ns: ns.into(), key: format!("row/{i:04}"),
                op_id: format!("put{i}"), deadline: now_ms()+30000, ack: 1, expected: None,
                bytes: vec![7; 16], remove: false }).code, 0);
        }
        s.capture_visits.store(0, Ordering::Relaxed);
        let (token, captured) = s.snapshot_open("focus").unwrap();
        let count = captured.rows.len();
        s.snapshot_release("focus", &token).unwrap();
        drop(captured);
        let visits = s.capture_visits.load(Ordering::Relaxed);
        s.capture_visits.store(0, Ordering::Relaxed);
        let (token, empty) = s.snapshot_open("missing").unwrap();
        assert!(empty.rows.is_empty());
        s.snapshot_release("missing", &token).unwrap();
        drop(empty);
        let missing_visits = s.capture_visits.load(Ordering::Relaxed);
        drop(s);
        std::fs::remove_dir_all(root).unwrap();
        assert_eq!(count, 4);
        assert!(visits <= 10, "two passes may enumerate four namespace keys and one boundary each; saw {visits}");
        assert!(missing_visits <= 2, "missing namespace enumerated {missing_visits} unrelated keys");
    }

    #[test]
    fn namespace_capture_scope_keeps_exact_unicode_boundaries_after_replay() {
        let root = PathBuf::from(std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()))
            .join(format!("namespace-boundaries-{}-{}", std::process::id(), now_ms()));
        let config = Config { tick_ms: 3_600_000, ..Config::default() };
        {
            let s = Store::open(&root, config.clone()).unwrap();
            for ns in ["a", "a/", "ab", "é", "ê"] {
                for i in 0..4 {
                    assert_eq!(s.mutate(Mutation { ns: ns.into(), key: format!("row/{i}"),
                        op_id: format!("put{i}"), deadline: now_ms()+30000, ack: 1, expected: None,
                        bytes: ns.as_bytes().to_vec(), remove: false }).code, 0);
                }
            }
            assert_eq!(s.mutate(Mutation { ns: "a".into(), key: "row/0".into(), op_id: "remove".into(),
                deadline: now_ms()+30000, ack: 1, expected: None, bytes: vec![], remove: true }).code, 0);
        }
        let s = Store::open(&root, config).unwrap();
        for ns in ["a", "a/", "ab", "é", "ê"] {
            let (token, view) = s.snapshot_open(ns).unwrap();
            let keys: Vec<_> = view.rows.iter().map(|(key, _)| key.clone()).collect();
            let expected: Vec<_> = (if ns == "a" {1} else {0}..4).map(|i| format!("row/{i}")).collect();
            assert_eq!(keys, expected);
            for key in &keys { assert_eq!(s.try_get(ns, key).unwrap().unwrap().bytes, ns.as_bytes()); }
            s.snapshot_release(ns, &token).unwrap();
        }
        assert_eq!(s.scan_usage.count.load(Ordering::Acquire), 0);
        assert_eq!(s.scan_usage.bytes.load(Ordering::Acquire), 0);
        drop(s);
        std::fs::remove_dir_all(root).unwrap();
    }
