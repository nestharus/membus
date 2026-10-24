//! CR3 (docs/CHUNK-RETIREMENT.md section 10): HEALTH2 and the heartbeat fields. Test T9 of section 7.
use membus::*;
use std::{collections::HashMap, path::PathBuf};
#[path = "support/writer_progress.rs"]
mod writer_progress;
fn path(label: &str) -> PathBuf {
    let p = std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into());
    PathBuf::from(p).join(format!("{label}-{}-{}", std::process::id(), now_ms()))
}
fn put(key: &str, id: &str) -> Mutation {
    Mutation {
        ns: "ledger".into(),
        key: key.into(),
        op_id: id.into(),
        deadline: now_ms() + 10000,
        ack: 1,
        expected: None,
        bytes: b"value".to_vec(),
        remove: false,
    }
}
/// HEALTH2 over the wire, as a map.
fn wire_health2(s: &Store) -> HashMap<String, String> {
    let mut p = (now_ms() + 10000).to_le_bytes().to_vec();
    lp(&mut p, b"health2-op");
    lp(&mut p, b"ledger");
    let reply = s.dispatch(&frame(HEALTH2, 7, &p)[4..]);
    assert_eq!(u16::from_le_bytes(reply[17..19].try_into().unwrap()), 0, "HEALTH2 refused");
    let mut c = Cursor::new(&reply[19..]);
    let n = c.u16().unwrap();
    let mut out = HashMap::new();
    for _ in 0..n {
        let kv = String::from_utf8(c.bytes(MAX_FRAME).unwrap().to_vec()).unwrap();
        let (k, v) = kv.split_once('=').unwrap();
        assert!(out.insert(k.to_owned(), v.to_owned()).is_none(), "duplicate {k}");
    }
    c.end().unwrap();
    out
}

#[test]
fn t9_health2_fields_equal_the_store_state() {
    let p = path("t9");
    let c = Config { dedup_cap: 10, quota: 10, chunk_bytes: 100, ..Config::default() };
    {
        let s = Store::open(&p, c.clone()).unwrap();
        for i in 0..6 {
            assert_eq!(s.mutate(put(&format!("k{i}"), &format!("op{i}"))).code, 0);
        }
        let h = s.health2();
        assert!(!h.dedup_high_water && !h.namespace_high_water, "{h:?}");
        assert_eq!(s.mutate(put("k6", "op6")).code, 0);
        let h = s.health2();
        // 7 of 10: both high waters at 70%.
        assert_eq!(h.dedup_entries, 7);
        assert!(h.dedup_high_water);
        assert_eq!(h.namespaces, vec![("ledger".to_owned(), 7)]);
        assert!(h.namespace_high_water);
        // Each field equals the store's own reading.
        assert_eq!(h.state_bytes, s.state_bytes());
        assert_eq!(h.keys, s.slots("ledger").0);
        assert_eq!(h.chunks_open, s.open_chunks());
        let rows = s.chunk_rows();
        assert_eq!(h.chunks_closed, rows.iter().filter(|r| r.3 == "Closed").count());
        assert!(h.chunks_closed > 0);
        assert_eq!(h.retained_total, rows.iter().map(|r| r.4).sum::<u64>());
        assert_eq!(h.retained_total, h.state_bytes);
        assert_eq!(h.retained_bytes, vec![("forever".to_owned(), h.retained_total)]);
        assert!(!h.read_only && !h.storage_error && !h.expiry_degraded && !h.state_high_water);
        assert_eq!((h.pinned_bytes, h.retire_blocked, h.retired_chunks), (0, 0, 0));
        // The wire carries the same list.
        let wire = wire_health2(&s);
        let fields: HashMap<String, String> = s.health2().fields().into_iter().collect();
        assert_eq!(wire, fields);
        assert_eq!(wire["namespace.ledger"], "7");
        assert_eq!(wire["dedup_high_water"], "true");
        assert_eq!(wire["retained_bytes.forever"], wire["retained_bytes"]);
    }
    // read_only turns true after a failed append.
    #[cfg(not(windows))]
    let _ = std::fs::remove_file(p.join("owner.lock"));
    let s = Store::open(&p, Config { fail_partial_append: true, ..c }).unwrap();
    assert!(!s.health2().read_only);
    assert_eq!(s.mutate(put("k7", "op7")).code, 16);
    assert!(s.health2().read_only);
    assert_eq!(wire_health2(&s)["read_only"], "true");
    println!("T9: HEALTH2 fields equal the store's state; high waters at 70%; read_only after a failed append");
}

#[test]
fn t9_heartbeat_stays_under_512_bytes() {
    // Wide values: a 7-digit pid, a 4-digit epoch, a year's counters at a 30 s beat (1,051,200 beats of about 500 B),
    // 999 failures, the 8 GiB limit full and 100 GB retired.
    let h = Health2 {
        state_bytes: 8 * 1024 * 1024 * 1024,
        state_limit_bytes: 8 * 1024 * 1024 * 1024,
        state_high_water: true,
        read_only: false,
        storage_error: false,
        expiry_degraded: false,
        dedup_entries: 262_144,
        dedup_high_water: false,
        namespace_high_water: false,
        pinned_high_water: false,
        retire_blocked_high_water: false,
        retained_total: 8 * 1024 * 1024 * 1024,
        retired_bytes: 99_999_999_999,
        ..Health2::default()
    };
    let beat = heartbeat_json(9_999_999, 9_999, 1_799_999_999_999, 9_999_999, 999_999_999, 9_999_999, 999_999_999, 999, &h);
    assert!(beat.len() < 512, "{} B: {beat}", beat.len());
    assert!(beat.contains("\"state_high_water\":true"));
    for flag in [
        "read_only",
        "storage_error",
        "expiry_degraded",
        "dedup_high_water",
        "namespace_high_water",
        "pinned_high_water",
        "retire_blocked_high_water",
    ] {
        assert!(beat.contains(&format!("\"{flag}\":0")) || beat.contains(&format!("\"{flag}\":1")), "{flag}");
    }
    for count in ["dedup_entries", "retained_bytes", "retired_bytes", "heartbeat_failures", "unix_ms"] {
        assert!(beat.contains(&format!("\"{count}\":")), "{count}");
    }
    println!("T9: widest heartbeat {} B", beat.len());
}

#[test]
fn t9_health2_alongside_writers_that_open_chunks() {
    // The resident heartbeat reads HEALTH2 while writes rotate chunks: neither may wait on the other's lock.
    use std::sync::{atomic::{AtomicBool, Ordering}, mpsc, Arc};
    let p = path("t9-concurrent");
    let s = Arc::new(Store::open(&p, Config { chunk_bytes: 100, ..Config::default() }).unwrap());
    let stop = Arc::new(AtomicBool::new(false));
    let reader = {
        let (s, stop) = (s.clone(), stop.clone());
        std::thread::spawn(move || {
            let mut reads = 0u64;
            while !stop.load(Ordering::SeqCst) {
                let _ = s.health2();
                reads += 1;
            }
            reads
        })
    };
    let (done, finished) = mpsc::channel();
    let started = std::time::Instant::now();
    let writers: Vec<_> = (0..4)
        .map(|w| {
            let (s, done) = (s.clone(), done.clone());
            std::thread::spawn(move || {
                for i in 0..300 {
                    assert_eq!(s.mutate(put(&format!("w{w}/{i}"), &format!("w{w}-{i}"))).code, 0);
                    done.send((w, i + 1, started.elapsed())).unwrap();
                }
            })
        })
        .collect();
    drop(done);
    let mut progress = writer_progress::WriterProgress::new(4, 300, std::time::Duration::from_secs(60));
    loop {
        // Use actual completion timestamps, draining queued observations before evaluating staleness.
        while let Ok((writer, completed, at)) = finished.try_recv() {
            progress.record(writer, completed, at);
        }
        let wait = match progress.remaining(started.elapsed()) {
            Ok(None) => break,
            Ok(Some(wait)) => wait,
            Err(writer) => panic!("writer {writer} stalled beside HEALTH2: {progress:?}"),
        };
        match finished.recv_timeout(wait) {
            Ok((writer, completed, at)) => progress.record(writer, completed, at),
            Err(error) => panic!("writers stalled beside HEALTH2: {error:?}; {progress:?}"),
        }
    }
    stop.store(true, Ordering::SeqCst);
    let reads = reader.join().unwrap();
    for w in writers {
        w.join().unwrap();
    }
    assert!(reads > 0);
    assert!(s.health2().chunks_closed > 100);
    println!("T9 concurrent: 1,200 puts over {} closed chunks beside {reads} HEALTH2 reads", s.health2().chunks_closed);
}
