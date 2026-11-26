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
                let p = dir.join(format!("stripe{n}"));
                let file = Arc::new(
                    OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&p)
                        .unwrap(),
                );
                let flush_handle = Arc::new(OpenOptions::new().write(true).open(&p).unwrap());
                Arc::new(Mutex::new(Log {
                    file,
                    flush_handle,
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
                chunk_index: None,
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
                chunk_index: None,
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

    #[test]
    fn multi_stripe_flushes_do_not_serialize_on_cross_sink_spacing() {
        let dir = path("multi-stripe-spacing");
        let store = Arc::new(
            Store::open(
                &dir,
                Config {
                    flush_ms: 120,
                    ..Config::default()
                },
            )
            .unwrap(),
        );
        // Pick two distinct keys for each of the 4 stripes.
        let mut by_stripe: [Vec<String>; 4] = std::array::from_fn(|_| Vec::new());
        for i in 0..500 {
            let k = format!("ms-{i}");
            let s = crate::routing::stripe("ledger", &k);
            if by_stripe[s].len() < 2 {
                by_stripe[s].push(k);
            }
            if by_stripe.iter().all(|v| v.len() == 2) {
                break;
            }
        }
        // Seed one ack=2 write per stripe so all 4 sinks are open and recorded in last_by_sink.
        std::thread::scope(|scope| {
            let jobs: Vec<_> = (0..4)
                .map(|s| {
                    let st = &store;
                    let k = by_stripe[s][0].clone();
                    scope.spawn(move || {
                        assert_eq!(st.mutate(mutation(&k)).ack, 2);
                    })
                })
                .collect();
            for j in jobs {
                j.join().unwrap();
            }
        });
        // Immediately fire 8 concurrent ack=2 writes across all 4 stripes.
        // Same-stripe spacing must still hold (>= 1 interval), while all 4 distinct
        // stripes must settle without serializing to 4 * 120ms = 480ms.
        let start = std::time::Instant::now();
        let barrier = Arc::new(std::sync::Barrier::new(8));
        std::thread::scope(|scope| {
            let mut jobs = Vec::new();
            for s in 0..4 {
                for slot in 0..2 {
                    let st = &store;
                    let b = barrier.clone();
                    let key = format!("{}-r2-{slot}", by_stripe[s][slot]);
                    // Keep exact stripe assignment by using the pre-selected keys with fresh op_id.
                    let _ = key;
                    let k = by_stripe[s][slot].clone();
                    let op = format!("r2-{s}-{slot}");
                    jobs.push(scope.spawn(move || {
                        b.wait();
                        let o = st.mutate(Mutation {
                            ns: "ledger".into(),
                            key: k,
                            op_id: op,
                            deadline: now_ms() + 10_000,
                            ack: 2,
                            expected: None,
                            bytes: vec![2; 1039],
                            remove: false,
                        });
                        assert_eq!((o.code, o.ack), (0, 2));
                    }));
                }
            }
            for j in jobs {
                j.join().unwrap();
            }
        });
        let elapsed = start.elapsed();
        drop(store);
        let _ = fs::remove_dir_all(&dir);
        assert!(
            elapsed < Duration::from_millis(320),
            "4 stripes serialized across global spacing: elapsed={elapsed:?}"
        );
    }

    #[test]
    fn newly_opened_sink_wakes_immediately_during_other_sink_cooldown() {
        let dir = path("unseeded-wake");
        let store = Arc::new(
            Store::open(
                &dir,
                Config {
                    flush_ms: 300,
                    ..Config::default()
                },
            )
            .unwrap(),
        );
        let k_s0_a = (0..200)
            .map(|i| format!("s0a-{i}"))
            .find(|k| crate::routing::stripe("ledger", k) == 0)
            .unwrap();
        let k_s0_b = (0..200)
            .map(|i| format!("s0b-{i}"))
            .find(|k| crate::routing::stripe("ledger", k) == 0)
            .unwrap();
        let k_s1 = (0..200)
            .map(|i| format!("s1-{i}"))
            .find(|k| crate::routing::stripe("ledger", k) == 1)
            .unwrap();
        // Seed stripe 0 so its sink is in cooldown; stripe 1 has never been opened/flushed.
        assert_eq!(store.mutate(mutation(&k_s0_a)).ack, 2);
        // Submit a second write on stripe 0 so FlushOwner enters stripe 0's 300ms cooldown wait.
        let st0 = store.clone();
        let h0 = std::thread::spawn(move || {
            let o = st0.mutate(mutation(&k_s0_b));
            assert_eq!((o.code, o.ack), (0, 2));
        });
        std::thread::sleep(Duration::from_millis(25));
        // Now write to the unseeded stripe 1: it must wake FlushOwner and settle immediately,
        // without waiting out stripe 0's remaining ~275ms cooldown.
        let t1 = std::time::Instant::now();
        let o1 = store.mutate(mutation(&k_s1));
        let s1_elapsed = t1.elapsed();
        h0.join().unwrap();
        drop(store);
        let _ = fs::remove_dir_all(&dir);
        assert_eq!((o1.code, o1.ack), (0, 2));
        assert!(
            s1_elapsed < Duration::from_millis(150),
            "unseeded stripe 1 stalled behind stripe 0 cooldown: elapsed={s1_elapsed:?}"
        );
    }

    #[test]
    fn flush_owner_disconnect_flushes_pending_immediately_without_sleeping_interval() {
        let dir = path("disconnect-no-sleep");
        fs::create_dir_all(&dir).unwrap();
        let logs: Vec<_> = (0..4)
            .map(|n| {
                let p = dir.join(format!("stripe{n}"));
                let file = Arc::new(
                    OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&p)
                        .unwrap(),
                );
                let flush_handle = Arc::new(OpenOptions::new().write(true).open(&p).unwrap());
                Arc::new(Mutex::new(Log {
                    file,
                    flush_handle,
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
                flush_ms: 5000,
                ..Config::default()
            },
        );
        // First flush records last_by_sink[0] = now.
        let (done1, complete1) = mpsc::channel();
        appenders[0]
            .sender
            .as_ref()
            .unwrap()
            .send(Append {
                record: vec![1; 64],
                ack: 2,
                chunk_index: None,
                done: done1,
            })
            .unwrap();
        complete1
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        // Second flush arrives while last_by_sink[0] is fresh (5000ms cooldown),
        // then ingress closes by dropping appenders. FlushOwner must break out immediately.
        let (done2, complete2) = mpsc::channel();
        let (done3, complete3) = mpsc::channel();
        appenders[0]
            .sender
            .as_ref()
            .unwrap()
            .send(Append {
                record: vec![2; 64],
                ack: 2,
                chunk_index: None,
                done: done2,
            })
            .unwrap();
        appenders[0]
            .sender
            .as_ref()
            .unwrap()
            .send(Append {
                record: vec![3; 64],
                ack: 2,
                chunk_index: None,
                done: done3,
            })
            .unwrap();
        let start = std::time::Instant::now();
        for owner in &mut appenders {
            owner.sender.take();
        }
        for owner in &mut appenders {
            owner.handle.take().unwrap().join().unwrap();
        }
        worker.join().unwrap();
        let elapsed = start.elapsed();
        complete2.recv_timeout(Duration::from_secs(1)).unwrap().unwrap();
        complete3.recv_timeout(Duration::from_secs(1)).unwrap().unwrap();
        let _ = fs::remove_dir_all(&dir);
        assert!(
            elapsed < Duration::from_millis(1000),
            "FlushOwner slept full flush_ms after disconnect: elapsed={elapsed:?}"
        );
    }

    #[test]
    fn manual_clock_gathered_wait_does_not_double_sleep() {
        let dir = path("manual-clock-no-double-sleep");
        let clock = crate::ManualClock::new(1_000_000);
        let store = Arc::new(
            Store::open(
                &dir,
                Config {
                    flush_ms: 120,
                    clock: Some(clock),
                    ..Config::default()
                },
            )
            .unwrap(),
        );
        let keys: Vec<_> = (0..200)
            .map(|i| format!("mc-{i}"))
            .filter(|k| crate::routing::stripe("ledger", k) == 0)
            .take(3)
            .collect();
        assert_eq!(store.mutate(mutation(&keys[0])).ack, 2);
        let start = std::time::Instant::now();
        let st1 = store.clone();
        let k1 = keys[1].clone();
        let h1 = std::thread::spawn(move || st1.mutate(mutation(&k1)));
        std::thread::sleep(Duration::from_millis(20));
        let st2 = store.clone();
        let k2 = keys[2].clone();
        let h2 = std::thread::spawn(move || st2.mutate(mutation(&k2)));
        assert_eq!(h1.join().unwrap().ack, 2);
        assert_eq!(h2.join().unwrap().ack, 2);
        let elapsed = start.elapsed();
        drop(store);
        let _ = fs::remove_dir_all(&dir);
        assert!(
            elapsed < Duration::from_millis(230),
            "ManualClock gathered flush double-slept: elapsed={elapsed:?}"
        );
    }
