    use super::*;
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
        assert!(Arc::ptr_eq(&live, &snapshot.rows[0].1));
        assert_eq!(live.bytes.as_ptr(), snapshot.rows[0].1.bytes.as_ptr());
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
