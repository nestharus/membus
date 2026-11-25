    use super::*;
    const DAY: u64 = 86_400_000;
    fn fixture(label: &str) -> (PathBuf, Arc<chunks::ManualClock>, Config) {
        let path = PathBuf::from(std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()))
            .join(format!("receiving-{label}-{}-{}", std::process::id(), now_ms()));
        let clock = chunks::ManualClock::new(now_ms());
        let config = Config { clock: Some(clock.clone()), tick_ms: 3_600_000, retire_ms: 3_600_000,
            rotation_ms: 1, ..Config::default() };
        (path, clock, config)
    }
    fn put(ns: &str, key: &str, id: &str) -> Mutation {
        Mutation { ns: ns.into(), key: key.into(), op_id: id.into(), deadline: now_ms()+10_000,
            ack: 1, expected: None, bytes: b"retained".to_vec(), remove: false }
    }
    fn policy(s: &Store, ns: &str, prefix: &str, class: Option<Class>, id: &str) -> Outcome {
        let mut p = (now_ms()+10_000).to_le_bytes().to_vec();
        lp(&mut p, id.as_bytes()); lp(&mut p, ns.as_bytes()); lp(&mut p, prefix.as_bytes());
        p.push(class.map_or(255, |c| c.kind)); p.extend(class.map_or(0, |c| c.retention_ms).to_le_bytes());
        let r = s.dispatch(&frame(POLICY_SET, 17, &p)[4..]);
        let code = u16::from_le_bytes(r[17..19].try_into().unwrap());
        if code != 0 { Outcome::error(code) } else {
            Outcome { code, version: u64::from_le_bytes(r[19..27].try_into().unwrap()), ack: r[27] }
        }
    }
    #[test]
    fn policy_retry_identity_survives_interposed_policy_and_restart() {
        let (path, _, config) = fixture("policy-id");
        let s = Store::open(&path, config.clone()).unwrap();
        let first = policy(&s, "caller", "p/", Some(Class::expire(DAY)), "A");
        assert_eq!(first.code, 0);
        assert_eq!(policy(&s,"caller","p/",Some(Class::FOREVER),"B").code,0);
        assert_eq!(policy(&s,"caller","p/",Some(Class::expire(DAY)),"A"),first);
        assert_eq!(s.policies().into_iter().find(|r|r.0=="caller").unwrap().2,Class::FOREVER);
        for (prefix, class) in [("other/",Some(Class::expire(DAY))), ("p/",None), ("p/",Some(Class::LATEST))] {
            assert_eq!(policy(&s,"caller",prefix,class,"A").code,14);
        }
        assert_eq!(s.mutate(put("caller","ordinary","A")).code,14);
        assert_eq!(policy(&s,"another","p/",Some(Class::LATEST),"A").code,0);
        drop(s);
        let s = Store::open(&path, config).unwrap();
        assert_eq!(policy(&s,"caller","p/",Some(Class::expire(DAY)),"A"),first);
        assert_eq!(s.policies().into_iter().find(|r|r.0=="caller").unwrap().2,Class::FOREVER);
    }
    fn paused_setters(fill: bool) {
        let (path, _, config) = fixture(if fill {"cap-race"} else {"policy-order"});
        let s = Arc::new(Store::open(&path, config).unwrap());
        if fill { for i in 0..61 {
            assert_eq!(s.set_policy("classes",&format!("{i}/"),Some(Class::expire(DAY+i)),now_ms()+10_000).code,0);
        } }
        let (reached, committed) = mpsc::channel(); let (resume, resumed) = mpsc::channel();
        *s.policy_after_commit.lock().unwrap() = Some((reached,resumed));
        let a = s.clone();
        let first = std::thread::spawn(move || a.set_policy("ordered","",Some(Class::expire(2*DAY)),now_ms()+10_000));
        committed.recv_timeout(Duration::from_secs(5)).unwrap();
        let b = s.clone(); let (finished, result) = mpsc::channel();
        let second = std::thread::spawn(move || {
            let got=b.set_policy(if fill {"other"} else {"ordered"},"",Some(if fill {Class::expire(3*DAY)} else {Class::FOREVER}),now_ms()+10_000);
            finished.send(got).unwrap();
        });
        let early=result.recv_timeout(Duration::from_millis(100));
        resume.send(()).unwrap();
        assert_eq!(first.join().unwrap().code,0);
        second.join().unwrap();
        let completed_early=early.is_ok(); let got=early.unwrap_or_else(|_|result.recv().unwrap());
        assert!(!completed_early,"another setter must not pass committed-but-unpublished policy state");
        assert_eq!(got.code,if fill {7} else {0});
        if !fill {
            assert_eq!(s.mutate(put("ordered","later","later")).code,0);
            let chunks=s.chunks.lock().unwrap();
            assert!(chunks.table.values().filter(|c|c.keys.contains_key(&("ordered".into(),"later".into())))
                .all(|c|c.class==Class::FOREVER));
            drop(chunks);drop(s);
            let reopened=Store::open(&path,Config::default()).unwrap();
            assert_eq!(reopened.policies().into_iter().find(|r|r.0=="ordered").unwrap().2,Class::FOREVER);
        }
    }
    #[test] fn policy_publication_orders_concurrent_setters() { paused_setters(false); }
    #[test] fn policy_cap_counts_competing_admissions() { paused_setters(true); }
    #[test]
    fn policy_cap_counts_retained_classes_after_prefix_replacement() {
        let (path, clock, config)=fixture("class-churn");let s=Store::open(&path,config).unwrap();
        for i in 0..62 {
            assert_eq!(s.set_policy("classes","",Some(Class::expire(DAY+i)),now_ms()+10_000).code,0);
            assert_eq!(s.mutate(put("classes","same-key",&format!("put-{i}"))).code,0);
        }
        assert_eq!(s.set_policy("classes","",Some(Class::expire(2*DAY)),now_ms()+10_000).code,7);
        clock.advance(2);s.tick();
        let old=s.chunk_rows().into_iter().find(|c|c.2==format!("expire({DAY}ms)")).unwrap().0;
        assert!(s.retire_chunk(old).unwrap());
        assert_eq!(s.set_policy("classes","",Some(Class::expire(2*DAY)),now_ms()+10_000).code,0,
            "retired historical class names must not consume a lifetime quota");
    }
    #[test]
    fn idle_close_sync_failure_is_visible_before_next_mutation() {
        let (path, clock, mut config)=fixture("idle-health");config.fail_sync=true;
        let s=Store::open(&path,config).unwrap();
        // Select the finite class in the fixture; fail_sync must first act at idle close, not a policy flush.
        s.policies.write().unwrap().set("idle","",Some(Class::LATEST));
        assert_eq!(s.mutate(put("idle","k","idle-os")).code,0);
        clock.advance(2);s.tick();
        assert!(s.health2().storage_error);
        assert!(!s.health2().read_only,"no later client failure has exposed the fence yet");
    }
    #[test]
    fn rewrite_does_not_repeat_oversized_unchanged_live_state() {
        let (path,_,mut config)=fixture("rewrite");config.catalogue_rewrite_bytes=1;
        let s=Store::open(&path,config).unwrap();
        assert_eq!(s.mutate(put("rewrite","k","one")).code,0);
        s.maint.rewrite_catalogue();
        assert_eq!(s.chunks.lock().unwrap().cat.as_ref().unwrap().rewrites,1);
        for _ in 0..3 {s.maint.rewrite_catalogue();}
        assert_eq!(s.chunks.lock().unwrap().cat.as_ref().unwrap().rewrites,1);
        s.chunks.lock().unwrap().log(now_ms(),chunks::Rec::Next(100)).unwrap();
        s.maint.rewrite_catalogue();
        assert_eq!(s.chunks.lock().unwrap().cat.as_ref().unwrap().rewrites,2);
    }
    #[test]
    fn truncation_evidence_survives_failed_start_and_clean_restart() {
        use std::io::Write;
        let (path,_,config)=fixture("truncation");let s=Store::open(&path,config.clone()).unwrap();
        assert_eq!(s.mutate(put("evidence","k","tail")).code,0);
        let file=s.chunks.lock().unwrap().table.values().find(|c|c.keys.contains_key(&("evidence".into(),"k".into()))).unwrap().path.clone();
        drop(s);let before=fs::metadata(&file).unwrap().len();
        OpenOptions::new().append(true).open(&file).unwrap().write_all(b"MBK").unwrap();
        let mut interrupted=config.clone();interrupted.stop_after_replay_truncation=true;
        assert!(Store::open(&path,interrupted).is_err());
        assert_eq!(fs::metadata(&file).unwrap().len(),before);
        let report=fs::read_to_string(path.join("replay.json")).unwrap();
        assert!(report.contains("truncation_pending")&&report.contains("short header")&&report.contains("\"ready_ms\":0"));
        drop(Store::open(&path,config).unwrap());
        assert!(fs::read_to_string(path.join("replay.log")).unwrap().contains("truncation_pending"));
    }
    #[cfg(windows)]
    #[test]
    fn blocked_retirement_rewrite_preserves_original_summary_age() {
        use std::os::windows::fs::OpenOptionsExt;
        let (path,clock,mut config)=fixture("summary-age");config.catalogue_rewrite_bytes=1;
        let s=Store::open(&path,config.clone()).unwrap();
        assert_eq!(s.set_policy("summary","",Some(Class::expire(DAY)),now_ms()+10_000).code,0);
        assert_eq!(s.mutate(put("summary","k","value")).code,0);
        clock.advance(2);s.tick();
        let file=s.chunks.lock().unwrap().table.values().find(|c|c.class==Class::expire(DAY)).unwrap().path.clone();
        let held=OpenOptions::new().read(true).share_mode(3).open(&file).unwrap();
        clock.advance(DAY);s.retirement_pass();
        assert_eq!(s.retire_blocked(),0);assert_eq!(s.get_status("summary","k").0,0);
        // Retry remains inclusive at the boundary; the blocked-retirement witness starts after it.
        clock.advance(1);let original=clock.utc_ms();s.retirement_pass();
        assert_eq!(s.retire_blocked(),1);assert_eq!(s.get_status("summary","k"),(1,0));
        clock.advance(3_600_000);s.maint.rewrite_catalogue();
        let records=chunks::read_catalogue(&path.join("chunks/catalogue.log")).unwrap().0;
        assert!(records.iter().any(|r|matches!(r.rec,chunks::Rec::Retiring{..})&&r.at_ms==original));
        drop(held);drop(s);
        let s=Store::open(&path,config).unwrap();
        let charged=s.slots("summary").0;
        clock.advance(30*DAY-3_600_000);s.retirement_pass();
        assert_eq!(s.get_status("summary","k"),(2,0));
        assert_eq!(s.slots("summary").0,charged-1);
    }
    #[test]
    fn repeated_fixed_set_churn_reclaims_discharged_bookkeeping() {
        let (path,clock,mut config)=fixture("bookkeeping");config.retry_ms=1;
        let covered=Arc::new(AtomicUsize::new(0));config.flush_covered_count=Some(covered.clone());
        let s=Store::open(&path,config).unwrap();
        assert_eq!(s.set_policy("churn","",Some(Class::LATEST),now_ms()+10_000).code,0);
        for i in 0..100 {
            let mut m=put("churn","one-key",&format!("churn-{i}"));m.ack=2;
            assert_eq!(s.mutate(m).code,0);
            clock.advance(2);s.tick();s.retirement_pass();s.maint.prune_discharged();
            assert!(s.chunks.lock().unwrap().table.len()<=2,"historical chunk rows survived discharge");
            assert!(s.router.table.read().unwrap().len()<=2,"historical closed sink rows survived discharge");
            assert!(covered.load(Ordering::SeqCst)<=4,"flush coverage grew with historical churn");
        }
        assert_eq!(s.get("churn","one-key").unwrap().bytes,b"retained");
        assert!(s.health2().retired_chunks>=99);
    }
    #[test]
    fn delayed_flush_notice_keeps_its_closed_sink_until_settlement() {
        let (path,clock,mut config)=fixture("late-sink");config.flush_notice_delay_ms=200;
        let s=Arc::new(Store::open(&path,config).unwrap());
        assert_eq!(s.set_policy("late","",Some(Class::LATEST),now_ms()+10_000).code,0);
        assert_eq!(s.mutate(put("late","k","before")).code,0);
        clock.advance(2);s.tick();
        let old=s.chunks.lock().unwrap().table.values().find(|c|c.class==Class::LATEST).unwrap().id;
        let other=s.clone();let writer=std::thread::spawn(move||{let mut m=put("late","k","delayed");m.ack=2;other.mutate(m)});
        let until=Instant::now()+Duration::from_secs(5);
        let new=loop {
            let found=s.chunks.lock().unwrap().table.values().find(|c|c.class==Class::LATEST&&c.id!=old&&c.keys.contains_key(&("late".into(),"k".into()))).map(|c|c.id);
            if let Some(id)=found {break id} assert!(Instant::now()<until);std::thread::sleep(Duration::from_millis(1));
        };
        clock.advance(2);s.tick();
        assert!(!s.retire_chunk(new).unwrap());s.maint.prune_discharged();
        assert!(s.router.sink(new as usize).is_some());
        assert_eq!(writer.join().unwrap().code,0,"late closed-sink flush must retain its covering extent");
        assert!(s.retire_chunk(old).unwrap());s.maint.prune_discharged();
        assert!(s.router.sink(old as usize).is_none());assert!(s.router.sink(new as usize).is_some());
    }
    #[test]
    fn selected_uninstantiated_class_is_counted_during_policy_replacement() {
        let (path,_,config)=fixture("selected-class");let s=Arc::new(Store::open(&path,config).unwrap());
        for i in 0..61 {
            let prefix=format!("p{i}/");
            assert_eq!(s.set_policy("classes",&prefix,Some(Class::expire(DAY+i)),now_ms()+10000).code,0);
            assert_eq!(s.mutate(put("classes",&(prefix+"k"),&format!("fill-{i}"))).code,0);
        }
        assert_eq!(s.set_policy("classes","L/",Some(Class::LATEST),now_ms()+10000).code,0);
        assert_eq!(s.mutate(put("classes","L/k","latest-fill")).code,0);
        assert_eq!(s.set_policy("race","p/",Some(Class::expire(2*DAY)),now_ms()+10000).code,0);
        let policy_stripe=stripe(POLICY_NS,&chunks::policy_key("race","p/").unwrap());
        let key=(0..100).map(|n|format!("p/{n}")).find(|k|stripe("race",k)!=policy_stripe).unwrap();
        let (reached,selection)=mpsc::channel();let(resume,resumed)=mpsc::channel();
        *s.router.selection_hook.lock().unwrap()=Some((reached,resumed));
        let a=s.clone();let writer=std::thread::spawn(move||a.mutate(put("race",&key,"selected")));
        selection.recv_timeout(Duration::from_secs(5)).unwrap();
        // Remove the old policy while X has no chunk: only its in-flight selection owns it now.
        assert_eq!(s.set_policy("race","p/",None,now_ms()+10000).code,0);
        let b=s.clone();let(finished,result)=mpsc::channel();let setter=std::thread::spawn(move||{
            let r=b.set_policy("race","p/",Some(Class::expire(3*DAY)),now_ms()+10000);finished.send(r).unwrap();
        });
        let early=result.recv_timeout(Duration::from_secs(5));resume.send(()).unwrap();
        assert_eq!(writer.join().unwrap().code,0);setter.join().unwrap();
        let got=early.expect("competing policy admission must complete while selected append remains paused");
        assert_eq!(got.code,7,"an already selected X must not disappear while Y is admitted");
        let mut classes=s.policies.read().unwrap().classes();classes.extend(s.chunks.lock().unwrap().table.values()
            .filter(|c|c.state!=chunks::ChunkState::Retired).map(|c|c.class));assert_eq!(classes.len(),64);
    }
    #[test]
    fn pending_policy_duplicate_observes_its_deadline_without_cancelling_original() {
        let(path,_,mut config)=fixture("policy-pending");
        // This witness advances real time while I/O is held; use the live instance clock.
        config.clock=None;
        let gate=Arc::new((Mutex::new((false,false)),Condvar::new()));config.sync_test_gate=Some(gate.clone());
        let s=Arc::new(Store::open(&path,config).unwrap());let first=s.clone();
        let original=std::thread::spawn(move||policy(&first,"deadline","",Some(Class::LATEST),"A"));
        {let(lock,ready)=&*gate;let mut status=lock.lock().unwrap();while !status.0 {
            let(next,timeout)=ready.wait_timeout(status,Duration::from_secs(5)).unwrap();status=next;assert!(!timeout.timed_out());
        }}
        let second=s.clone();let(finished,result)=mpsc::channel();let duplicate=std::thread::spawn(move||{
            let mut p=(now_ms()+20).to_le_bytes().to_vec();lp(&mut p,b"A");lp(&mut p,b"deadline");lp(&mut p,b"");p.push(1);p.extend(0u64.to_le_bytes());
            let reply=second.dispatch(&frame(POLICY_SET,23,&p)[4..]);finished.send(u16::from_le_bytes(reply[17..19].try_into().unwrap())).unwrap();
        });
        let before_release=result.recv_timeout(Duration::from_millis(200));
        {let(lock,ready)=&*gate;let mut status=lock.lock().unwrap();status.1=true;ready.notify_all();}
        let original=original.join().unwrap();duplicate.join().unwrap();let was_pending=before_release.is_ok();
        let code=before_release.unwrap_or_else(|_|result.recv().unwrap());
        assert!(was_pending,"a pending duplicate must return while original I/O is held");assert_eq!(code,17);
        assert_eq!(original.code,0);assert_eq!(policy(&s,"deadline","",Some(Class::LATEST),"A"),original);
    }
    #[test]
    fn aggregated_summary_timestamp_rewrites_as_replayable_bounded_frames() {
        let(path,_,_)=fixture("summary-frame");let dir=path.join("chunks");fs::create_dir_all(&dir).unwrap();
        let mut chunks=chunks::ChunkStore::new(dir.clone());
        let count=2*(MAX_FRAME/(MAX_ID+17)/2+2);
        for part in 0..2 {
            let keys=(part*count/2..(part+1)*count/2).map(|i|("aggregate".into(),format!("{i:08}{}","k".repeat(1016)))).collect();
            chunks.summaries.add(10,keys);
        }
        let live=chunks.live_list(20,1);let mut catalogue=chunks::Catalogue::open(&dir.join("catalogue.log")).unwrap();
        catalogue.rewrite(&live).unwrap();
        let records=chunks::read_catalogue(&dir.join("catalogue.log")).expect("a valid aggregate must survive the bounded replay reader").0;
        let saved:HashSet<Key>=records.iter().flat_map(|r|match &r.rec{chunks::Rec::Summary{keys,retired_ms}=>{
            assert_eq!(*retired_ms,10); assert_eq!(r.at_ms,10); keys.clone()
        },_=>Vec::new()}).collect();
        assert_eq!(saved,chunks.summaries.keys.keys().cloned().collect());
        assert_eq!(saved.len(),2*(count/2));
        assert!(records.iter().all(|r|chunks::encode(r).len()-16<=MAX_FRAME));
    }
    #[test]
    fn policy_duplicate_waits_for_projection_and_keeps_completed_outcome() {
        let(path,_,mut config)=fixture("policy-projection"); config.clock=None;
        let s=Arc::new(Store::open(&path,config.clone()).unwrap());
        let(reached,committed)=mpsc::channel();let(resume,resumed)=mpsc::channel();
        *s.policy_after_commit.lock().unwrap()=Some((reached,resumed));
        let first=s.clone();let writer=std::thread::spawn(move||policy(&first,"projection","",Some(Class::LATEST),"A"));
        committed.recv_timeout(Duration::from_secs(5)).unwrap();
        let second=s.clone();let(finished,result)=mpsc::channel();let duplicate=std::thread::spawn(move||{
            finished.send(second.set_policy_with_id("projection","",Some(Class::LATEST),"A",now_ms()+20)).unwrap();
        });
        let before_release=result.recv_timeout(Duration::from_millis(300));
        let other=s.clone();let(mismatch_sent,mismatch_result)=mpsc::channel();let mismatch=std::thread::spawn(move||{
            mismatch_sent.send(other.set_policy_with_id("projection","other",Some(Class::LATEST),"A",now_ms()+20)).unwrap();
        });
        let other=s.clone();let(fresh_sent,fresh_result)=mpsc::channel();let fresh=std::thread::spawn(move||{
            fresh_sent.send(other.set_policy_with_id("fresh","",Some(Class::LATEST),"B",now_ms()+20)).unwrap();
        });
        let mismatch_before=mismatch_result.recv_timeout(Duration::from_millis(300));
        let fresh_before=fresh_result.recv_timeout(Duration::from_millis(300));
        let next=s.next_issued.load(Ordering::SeqCst);
        resume.send(()).unwrap();let original=writer.join().unwrap();duplicate.join().unwrap();
        mismatch.join().unwrap();fresh.join().unwrap();
        assert_eq!(before_release.expect("duplicate must finish before projection is released").code,17);
        assert_eq!(mismatch_before.expect("mismatch must finish before projection is released").code,14);
        assert_eq!(fresh_before.expect("fresh deadline must finish before projection is released").code,5);
        assert_eq!(original.code,0);
        assert_eq!(s.set_policy_with_id("projection","",Some(Class::LATEST),"A",0),original);
        assert_eq!(s.next_issued.load(Ordering::SeqCst),next,"retries must not mint another version");
        assert_eq!(s.policies().iter().find(|r|r.0=="projection").unwrap().2,Class::LATEST);
        drop(s);let reopened=Store::open(&path,config).unwrap();
        assert_eq!(reopened.set_policy_with_id("projection","",Some(Class::LATEST),"A",0),original);
    }
    #[test]
    fn pending_policy_duplicate_joins_successfully_before_its_longer_deadline() {
        let(path,_,config)=fixture("policy-successful-join");
        let s=Arc::new(Store::open(&path,config.clone()).unwrap());
        let(reached,committed)=mpsc::channel();let(resume,resumed)=mpsc::channel();
        *s.policy_after_commit.lock().unwrap()=Some((reached,resumed));
        let first=s.clone();let original=std::thread::spawn(move||policy(&first,"join","",Some(Class::LATEST),"A"));
        committed.recv_timeout(Duration::from_secs(5)).unwrap();
        let(entered,observed)=mpsc::channel();*s.policy_join_observed.lock().unwrap()=Some(entered);
        let other=s.clone();let(sent,result)=mpsc::channel();let duplicate=std::thread::spawn(move||{
            let out=other.set_policy_with_id("join","",Some(Class::LATEST),"A",now_ms()+10_000);
            sent.send(out.clone()).unwrap();out
        });
        let entry=observed.recv_timeout(Duration::from_secs(5));
        let while_held=result.recv_timeout(Duration::from_millis(50));
        let next=s.next_issued.load(Ordering::SeqCst);
        resume.send(()).unwrap();let original=original.join().unwrap();let joined=duplicate.join().unwrap();
        assert!(entry.is_ok(),"the duplicate must actually reach its pending wait");
        assert!(matches!(while_held,Err(mpsc::RecvTimeoutError::Timeout)),"no outcome before publication: {while_held:?}");
        assert_eq!(joined,original);assert_eq!(joined.code,0);
        assert_eq!(s.next_issued.load(Ordering::SeqCst),next);
        assert_eq!(s.policies().iter().find(|r|r.0=="join").unwrap().2,Class::LATEST);
        drop(s);let reopened=Store::open(&path,config).unwrap();
        assert_eq!(reopened.set_policy_with_id("join","",Some(Class::LATEST),"A",0),original);
    }
    #[test]
    fn prospective_policy_class_survives_durable_wait_and_failure_cleans_it() {
        let(path,_,mut config)=fixture("prospective-class");
        let gate=Arc::new((Mutex::new((false,false)),Condvar::new()));config.sync_test_gate=Some(gate.clone());
        let s=Arc::new(Store::open(&path,config).unwrap());
        s.set_read_only(true);
        assert_eq!(s.set_policy("failed","",Some(Class::expire(DAY)),now_ms()+10000).code,10);
        assert!(s.class_reservations.lock().unwrap().is_empty());
        assert!(s.state.lock().unwrap().policy_publishing.is_empty());s.set_read_only(false);
        let first=s.clone();let writer=std::thread::spawn(move||first.set_policy("new","",Some(Class::expire(DAY)),now_ms()+10000));
        {let(lock,ready)=&*gate;let mut status=lock.lock().unwrap();while !status.0 {
            let(next,timeout)=ready.wait_timeout(status,Duration::from_secs(5)).unwrap();status=next;assert!(!timeout.timed_out());
        }}
        assert_eq!(s.class_reservations.lock().unwrap().get(&Class::expire(DAY)),Some(&1));
        // The old route remains usable while its replacement is durable but not published.
        assert_eq!(s.mutate(put("new","old-route","ordinary")).code,0);
        assert!(s.chunks.lock().unwrap().table.values().any(|c|c.class==Class::FOREVER&&c.keys.contains_key(&("new".into(),"old-route".into()))));
        {let(lock,ready)=&*gate;let mut status=lock.lock().unwrap();status.1=true;ready.notify_all();}
        assert_eq!(writer.join().unwrap().code,0);assert!(s.class_reservations.lock().unwrap().is_empty());
        assert_eq!(s.mutate(put("new","new-route","following")).code,0);
        assert!(s.chunks.lock().unwrap().table.values().any(|c|c.class==Class::expire(DAY)&&c.keys.contains_key(&("new".into(),"new-route".into()))));
    }
    #[test]
    fn catalogue_summary_utf8_boundary_and_writer_refusal_preserve_all_bytes() {
        let(path,_,_)=fixture("frame-boundary");fs::create_dir_all(&path).unwrap();
        let payload=MAX_FRAME-21;let n=payload/1038;
        let base:Vec<Key>=(0..n-1).map(|i|("ééé".into(),format!("{i:08}{}","k".repeat(1016)))).collect();
        for delta in [-1i32,0,1] {
            let mut keys=base.clone();
            keys.push((format!("ééé{}","x".repeat((1+delta.max(0))as usize)),"z".repeat((1024+delta.min(0))as usize)));
            let original=chunks::CatRecord{at_ms:10,rec:chunks::Rec::Summary{retired_ms:10,keys:keys.clone()}};
            assert_eq!(chunks::encode(&original).len()-16,(MAX_FRAME as i64+delta as i64)as usize);
            let mut cs=chunks::ChunkStore::new(path.clone());cs.summaries.add(10,keys.clone());
            let live=cs.live_list(20,1);let p=path.join(format!("{delta}.log"));let mut cat=chunks::Catalogue::open(&p).unwrap();
            cat.rewrite(&live).unwrap();let read=chunks::read_catalogue(&p).unwrap().0;
            let got:HashSet<Key>=read.iter().flat_map(|r|match &r.rec{chunks::Rec::Summary{keys,retired_ms}=>{assert_eq!(*retired_ms,10);keys.clone()},_=>Vec::new()}).collect();
            assert_eq!(got,keys.into_iter().collect());
            assert!(read.iter().all(|r|chunks::encode(r).len()-16<=MAX_FRAME));
            if delta>0 {
                let before=fs::read(&p).unwrap();assert!(cat.append(&original).is_err());assert_eq!(fs::read(&p).unwrap(),before);
                assert!(cat.rewrite(&[original]).is_err());assert_eq!(fs::read(&p).unwrap(),before);
            }
        }
        let mut summaries=chunks::Summaries::default();let k=("ns".into(),"key".into());
        assert_eq!(summaries.add(10,vec![k.clone()]),1);assert_eq!(summaries.add(10,vec![k.clone()]),0);
        assert_eq!(summaries.add(9,vec![k.clone()]),0);assert_eq!(summaries.add(12,vec![k.clone()]),0);
        assert_eq!(summaries.expire(11,1),0);assert_eq!(summaries.keys.get(&k),Some(&12));assert_eq!(summaries.expire(13,1),1);
    }
    #[test]
    fn replay_history_trim_failure_preserves_history_and_torn_source() {
        let(path,_,config)=fixture("trim-evidence");let s=Store::open(&path,config.clone()).unwrap();
        assert_eq!(s.mutate(put("trim","key","A")).code,0);let data=s.chunks.lock().unwrap().table.values()
            .find(|c|c.keys.contains_key(&("trim".into(),"key".into()))).unwrap().path.clone();drop(s);
        OpenOptions::new().append(true).open(&data).unwrap().write_all(b"MBK").unwrap();let source=fs::read(&data).unwrap();
        let mut history=b"old line\n".repeat(140000);history.extend(b"truncation_pending witness\n\xff\n");
        fs::write(path.join("replay.log"),&history).unwrap();
        assert!(Store::open(&path,config.clone()).is_err());assert_eq!(fs::read(path.join("replay.log")).unwrap(),history);
        assert_eq!(fs::read(&data).unwrap(),source,"publication failure must precede any truncation");
        let lines:Vec<String>=(0..300).map(|n|format!("{n:03} {}","h".repeat(4000))).collect();
        fs::write(path.join("replay.log"),format!("{}\n",lines.join("\n"))).unwrap();
        let report=chunks::ReplayReport{stage:"ready".into(),..Default::default()};report.write(&path).unwrap();
        let retained=fs::read_to_string(path.join("replay.log")).unwrap();let kept:Vec<&str>=retained.lines().collect();
        assert_eq!(kept.len(),257);assert_eq!(kept[0],lines[44]);assert_eq!(kept[255],lines[299]);assert_eq!(kept[256],report.json());
    }
    #[test]
    fn maintenance_summary_cohort_reopens_with_unrelated_live_value() {
        let(path,clock,mut config)=fixture("summary-cohort");config.chunk_bytes=4*1024*1024;
        let s=Store::open(&path,config.clone()).unwrap();
        assert_eq!(s.set_policy("aggregate","",Some(Class::expire(DAY)),now_ms()+10000).code,0);
        let retained=s.mutate(put("other","retained","other"));assert_eq!(retained.code,0);
        let expected:HashSet<Key>=(0..17000).map(|i|("aggregate".into(),format!("{i:08}{}","k".repeat(1016)))).collect();
        for (i,(_,key)) in expected.iter().enumerate(){assert_eq!(s.mutate(put("aggregate",key,&format!("cohort-{i}"))).code,0);}
        clock.advance(2);s.tick();clock.advance(DAY+1);assert!(s.maint.retire(None).unwrap().len()>=2);
        assert_eq!(s.chunks.lock().unwrap().summaries.keys.keys().cloned().collect::<HashSet<_>>(),expected);
        s.maint.rewrite_catalogue();drop(s);
        let reopened=Store::open(&path,config).unwrap();
        assert_eq!(reopened.get("other","retained").unwrap().version,retained.version);
        assert_eq!(reopened.get("other","retained").unwrap().bytes,b"retained");
        assert_eq!(reopened.chunks.lock().unwrap().summaries.keys.keys().cloned().collect::<HashSet<_>>(),expected);
        assert!(expected.iter().all(|(ns,key)|reopened.get_status(ns,key)==(1,0)));
    }
    #[test]
    fn drop_tombstones_preserves_unpublished_policy_clear() {
        let (path, _, config) = fixture("policy-clear-tombstone");
        let s = Arc::new(Store::open(&path, config.clone()).unwrap());
        assert_eq!(s.set_policy("clear-race", "p/", Some(Class::expire(DAY)), now_ms() + 10_000).code, 0);
        let (reached, committed) = mpsc::channel();
        let (resume, resumed) = mpsc::channel();
        *s.policy_after_commit.lock().unwrap() = Some((reached, resumed));
        let a = s.clone();
        let clearing = std::thread::spawn(move || a.set_policy("clear-race", "p/", None, now_ms() + 10_000));
        committed.recv_timeout(Duration::from_secs(5)).unwrap();
        // Run tombstone reclamation while the policy clear is committed but unpublished.
        s.maint.drop_tombstones();
        resume.send(()).unwrap();
        assert_eq!(clearing.join().unwrap().code, 0);
        assert!(s.policies().into_iter().all(|r| !(r.0 == "clear-race" && r.1 == "p/")));
        assert_eq!(s.mutate(put("clear-race", "p/k", "after-clear")).code, 0);
        let chunks = s.chunks.lock().unwrap();
        assert!(chunks.table.values()
            .filter(|c| c.keys.contains_key(&("clear-race".into(), "p/k".into())))
            .all(|c| c.class == Class::FOREVER));
        drop(chunks);
        drop(s);
        let reopened = Store::open(&path, config).unwrap();
        assert!(reopened.policies().into_iter().all(|r| !(r.0 == "clear-race" && r.1 == "p/")));
    }
