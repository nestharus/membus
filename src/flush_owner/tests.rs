    use super::*;
    fn mutation(key: &str) -> Mutation {
        Mutation {
            ns: "ledger".into(),
            key: key.into(),
            op_id: key.into(),
            deadline: now_ms() + 10000,
            ack: 2,
            expected: Some(0),
            bytes: vec![1; 1039],
            remove: false,
        }
    }
    fn path(label: &str) -> std::path::PathBuf {
        std::path::PathBuf::from(
            std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()),
        )
        .join(format!(
            "policy-{label}-{}-{}",
            std::process::id(),
            now_ms()
        ))
    }
    #[test]
    fn consecutive_idle_flushes_bypass_gather_and_spacing() {
        let store = Store::open(
            &path("idle"),
            Config {
                flush_ms: 2000,
                ..Config::default()
            },
        )
        .unwrap();
        for key in ["key1", "key3", "key4"] {
            let start = std::time::Instant::now();
            assert_eq!(store.mutate(mutation(key)).ack, 2);
            assert!(
                start.elapsed() < Duration::from_millis(1000),
                "idle flush waited gather interval"
            );
        }
        assert_eq!(store.io_metrics().2, 3);
    }
    #[test]
    fn notice_arriving_during_sync_keeps_its_gather_window() {
        let gate = Arc::new((Mutex::new((false, false)), Condvar::new()));
        let (notice, notices) = mpsc::channel();
        let store = Arc::new(
            Store::open(
                &path("during"),
                Config {
                    flush_ms: 200,
                    sync_test_gate: Some(gate.clone()),
                    sync_test_notice: Some(notice),
                    ..Config::default()
                },
            )
            .unwrap(),
        );
        assert_eq!(crc32(b"ledger\0key1") % 4, crc32(b"ledger\0key3") % 4);
        let a = store.clone();
        let first = std::thread::spawn(move || a.mutate(mutation("key1")));
        let mut state = gate.0.lock().unwrap();
        while !state.0 {
            state = gate
                .1
                .wait_timeout(state, Duration::from_secs(2))
                .unwrap()
                .0;
        }
        drop(state);
        notices.recv_timeout(Duration::from_secs(2)).unwrap();
        let b = store.clone();
        let second = std::thread::spawn(move || b.mutate(mutation("key3")));
        let timeout = std::time::Instant::now();
        while store.io_metrics().1 < 2 {
            assert!(timeout.elapsed() < Duration::from_secs(2));
            std::thread::yield_now();
        }
        // This witnesses actual enqueue, without relying on a scheduling sleep.
        notices.recv_timeout(Duration::from_secs(2)).unwrap();
        let released = std::time::Instant::now();
        gate.0.lock().unwrap().1 = true;
        gate.1.notify_all();
        assert_eq!(first.join().unwrap().ack, 2);
        assert_eq!(second.join().unwrap().ack, 2);
        assert!(released.elapsed() >= Duration::from_millis(190));
        assert_eq!(store.io_metrics().2, 2);
    }

    #[test]
    fn preempted_idle_sender_cannot_be_overtaken_by_queued_sender() {
        let (sender, receiver) = mpsc::channel();
        let outstanding = Arc::new(AtomicUsize::new(0));
        let gate = Arc::new(Mutex::new(()));
        let (entered, arrived) = mpsc::channel();
        let (release, resume) = mpsc::channel();
        let (done, _) = mpsc::channel();
        let wait = |end| FlushWait {
            stripe: 0,
            end,
            bytes: 1,
            done: done.clone(),
            gathered: false,
        };
        let first = wait(1);
        let second = wait(2);
        let (s, o, g) = (sender.clone(), outstanding.clone(), gate.clone());
        let a = std::thread::spawn(move || {
            enqueue_flush(first, &s, &o, &g, || {
                entered.send(()).unwrap();
                resume.recv_timeout(Duration::from_secs(2)).unwrap();
            })
        });
        arrived.recv_timeout(Duration::from_secs(2)).unwrap();
        let (s, o, g) = (sender.clone(), outstanding.clone(), gate.clone());
        let b = std::thread::spawn(move || enqueue_flush(second, &s, &o, &g, || {}));
        assert!(matches!(
            receiver.recv_timeout(Duration::from_millis(20)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));
        release.send(()).unwrap();
        a.join().unwrap();
        b.join().unwrap();
        let first = receiver.recv().unwrap();
        let second = receiver.recv().unwrap();
        assert_eq!((first.end, first.gathered), (1, false));
        assert_eq!((second.end, second.gathered), (2, true));
        assert_eq!(outstanding.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn late_notice_for_already_covered_extent_needs_no_second_sync() {
        let dir = path("late-covered");
        fs::create_dir_all(&dir).unwrap();
        let logs: Vec<_> = (0..4)
            .map(|n| {
                Arc::new(Mutex::new(Log {
                    file: OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(dir.join(format!("stripe{n}")))
                        .unwrap(),
                    bytes: 0,
                    write_calls: 0,
                    flushes: 0,
                    poisoned: false,
                }))
            })
            .collect();
        let (mut appenders, worker) = start_owners(
            &logs,
            &Config {
                flush_ms: 2000,
                ..Config::default()
            },
        );
        let (done, complete) = mpsc::channel();
        appenders[0]
            .sender
            .as_ref()
            .unwrap()
            .send(Append {
                record: vec![1; 128],
                ack: 2,
                done: done.clone(),
            })
            .unwrap();
        complete
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        // An empty append creates a genuinely late notice for the same complete
        // extent. This owner-level seam tests coverage reuse, not wire admission.
        appenders[0]
            .sender
            .as_ref()
            .unwrap()
            .send(Append {
                record: vec![],
                ack: 2,
                done,
            })
            .unwrap();
        complete
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!(logs[0].lock().unwrap().flushes, 1);
        for owner in &mut appenders {
            owner.sender.take();
        }
        for owner in &mut appenders {
            owner.handle.take().unwrap().join().unwrap();
        }
        worker.join().unwrap();
    }
