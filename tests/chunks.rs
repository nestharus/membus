//! CR1 (docs/CHUNK-RETIREMENT.md section 10): the catalogue, chunks, rotation, replay by version and the
//! replay record. Tests T1, T6, T7 and T12 of section 7.
use membus::*;
use std::{
    fs,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
fn path(label: &str) -> PathBuf {
    let p = std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into());
    PathBuf::from(p).join(format!("{label}-{}-{}", std::process::id(), now_ms()))
}
fn put(key: &str, id: &str, value: &[u8]) -> Mutation {
    Mutation {
        ns: "ledger".into(),
        key: key.into(),
        op_id: id.into(),
        deadline: now_ms() + 10000,
        ack: 1,
        expected: None,
        bytes: value.to_vec(),
        remove: false,
    }
}
fn stripe_of(key: &str) -> u32 {
    let mut b = b"ledger\0".to_vec();
    b.extend(key.as_bytes());
    crc32(&b) % 4
}
/// Keys after `first` that share its stripe.
fn same_stripe(first: &str, n: usize) -> Vec<String> {
    (0..1000)
        .map(|i| format!("{first}/{i}"))
        .filter(|k| stripe_of(k) == stripe_of(first))
        .take(n)
        .collect()
}
fn chunk_files(p: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for class in fs::read_dir(p.join("chunks")).into_iter().flatten().flatten() {
        if class.path().is_dir() {
            for f in fs::read_dir(class.path()).unwrap().flatten() {
                out.push(f.path());
            }
        }
    }
    out.sort();
    out
}
fn chunk_file(p: &Path, id: u64) -> PathBuf {
    chunk_files(p)
        .into_iter()
        .find(|f| f.file_stem().unwrap().to_str().unwrap().ends_with(&format!("-{id}")))
        .unwrap_or_else(|| panic!("chunk {id} has no file"))
}
fn row(s: &Store, id: u64) -> (u64, usize, String, String, u64) {
    s.chunk_rows().into_iter().find(|r| r.0 == id).unwrap()
}
/// One record per chunk: the second write to a stripe closes the first chunk.
fn one_per_chunk() -> Config {
    Config { chunk_bytes: 1, ..Config::default() }
}
fn reopen(p: &Path, c: Config) -> Store {
    #[cfg(not(windows))]
    let _ = fs::remove_file(p.join("owner.lock"));
    Store::open(p, c).unwrap()
}

#[test]
fn t1_rotation_by_size_and_replay_by_version() {
    let p = path("t1-size");
    let c = Config { chunk_bytes: 300, ..Config::default() };
    let mut written = Vec::new();
    {
        let s = Store::open(&p, c.clone()).unwrap();
        for i in 0..40 {
            let key = format!("k{}", i % 10);
            let o = s.mutate(put(&key, &format!("op{i}"), format!("value-{i}").as_bytes()));
            assert_eq!(o.code, 0);
            written.push((key, o.version, format!("value-{i}")));
        }
        let rows = s.chunk_rows();
        let closed: Vec<_> = rows.iter().filter(|r| r.3 == "Closed").collect();
        assert!(closed.len() >= 4, "{rows:?}");
        for r in &rows {
            assert!(r.4 > 0, "an empty chunk exists: {r:?}");
            assert!(r.4 <= 300, "chunk past its size: {r:?}");
            assert_eq!(r.2, "forever");
        }
        // Every chunk has its file; the catalogue sits beside the class folders.
        assert_eq!(chunk_files(&p).len(), rows.len());
        assert!(p.join("chunks").join("catalogue.log").exists());
        assert!(!p.join("stripe-0.log").exists());
    }
    let s = reopen(&p, c);
    // Replay by version across chunks: each key's latest write wins.
    for i in 0..10 {
        let key = format!("k{i}");
        let (_, version, value) = written.iter().rev().find(|w| w.0 == key).unwrap();
        let got = s.get("ledger", &key).unwrap();
        assert_eq!(got.version, *version);
        assert_eq!(got.bytes, value.as_bytes());
    }
    let report = s.replay_report();
    let records: u64 = report.stripes.iter().map(|r| r.records).sum();
    assert_eq!(records, 40);
    assert_eq!(report.keys, 10);
    assert!(report.storage_errors.is_empty());
    // The chunks open at the stop closed at start, each at its replayed length.
    assert!(!report.closed_at_start.is_empty());
    let json = fs::read_to_string(p.join("replay.json")).unwrap();
    assert!(json.contains("\"records\":"), "{json}");
    assert!(fs::read_to_string(p.join("replay.log")).unwrap().lines().count() >= 2);
    assert!(s.chunk_rows().iter().all(|r| r.3 == "Closed"));
    // New writes after the restart take versions above every replayed one.
    let max = written.iter().map(|w| w.1).max().unwrap();
    assert!(s.mutate(put("k0", "after", b"after")).version > max);
    println!("T1 size: 40 records over {} chunks of at most 300 B; replay by version kept each latest", records);
}

#[test]
fn t1_rotation_by_age_and_idle_close_by_the_tick() {
    let p = path("t1-age");
    let c = Config { tick_ms: 50, rotation_ms: 300, ..Config::default() };
    let s = Store::open(&p, c).unwrap();
    assert_eq!(s.set_policy("ledger", "t/", Some(Class::LATEST), now_ms() + 10000).code, 0);
    assert_eq!(s.policies(), vec![("ledger".into(), "t/".into(), Class::LATEST)]);
    let wrote = Instant::now();
    assert_eq!(s.mutate(put("t/1", "t1", b"x")).code, 0);
    assert_eq!(s.mutate(put("f/1", "f1", b"x")).code, 0);
    let latest = s.chunk_rows().into_iter().find(|r| r.2 == "latest").unwrap();
    assert_eq!(latest.3, "Open");
    while row(&s, latest.0).3 == "Open" {
        assert!(wrote.elapsed() < Duration::from_secs(5), "the idle tick never closed the latest chunk");
        std::thread::sleep(Duration::from_millis(10));
    }
    let closed_after = wrote.elapsed();
    // Closed at its age boundary by the tick, within 1 s of it; never before.
    assert!(closed_after >= Duration::from_millis(300), "{closed_after:?}");
    assert!(closed_after < Duration::from_millis(1300), "{closed_after:?}");
    // Forever rotates by size only: its chunk stays open.
    assert!(s.chunk_rows().iter().any(|r| r.2 == "forever" && r.3 == "Open"));
    // The next latest write opens a new chunk.
    assert_eq!(s.mutate(put("t/2", "t2", b"y")).code, 0);
    assert!(s.chunk_rows().iter().any(|r| r.2 == "latest" && r.3 == "Open" && r.0 != latest.0));
    drop(s);
    // The policy is durable: it is read back at start.
    let s = reopen(&p, Config::default());
    assert_eq!(s.policies(), vec![("ledger".into(), "t/".into(), Class::LATEST)]);
    assert_eq!(s.get("ledger", "t/1").unwrap().bytes, b"x");
    println!("T1 age: latest chunk closed by the tick {closed_after:?} after its write (rotation 300 ms, tick 50 ms)");
}

/// a's first version in chunk X; a's second in Y; X is closed and holds no latest record.
fn superseded_chunk(p: &Path, c: Config) -> (Store, u64, u64) {
    let s = Store::open(p, c).unwrap();
    let keys = same_stripe("a", 1);
    let v1 = s.mutate(put("a", "a1", b"v1"));
    let v2 = s.mutate(put("a", "a2", b"v2"));
    assert_eq!(s.mutate(put(&keys[0], "b1", b"b")).code, 0);
    let mut ids: Vec<_> = s
        .chunk_rows()
        .into_iter()
        .filter(|r| r.1 as u32 == stripe_of("a"))
        .map(|r| r.0)
        .collect();
    ids.sort();
    assert_eq!(ids.len(), 3);
    assert!(v2.version > v1.version);
    (s, ids[0], ids[1])
}

#[test]
fn t6_kill_at_each_retirement_step() {
    for stop in ["before", "after-retiring", "after-delete", "whole"] {
        let p = path(&format!("t6-{stop}"));
        let c = Config {
            retire_stop_after: match stop {
                "after-retiring" => 1,
                "after-delete" => 2,
                _ => 0,
            },
            ..one_per_chunk()
        };
        let (s, x, y) = superseded_chunk(&p, c);
        let a = s.get("ledger", "a").unwrap();
        // Y holds a's latest version: it is never eligible.
        assert!(!s.retire_chunk(y).unwrap());
        let file = chunk_file(&p, x);
        match stop {
            "before" => {}
            "after-retiring" => {
                assert!(!s.retire_chunk(x).unwrap());
                assert_eq!(row(&s, x).3, "Retiring");
                assert!(file.exists());
            }
            "after-delete" => {
                assert!(!s.retire_chunk(x).unwrap());
                assert_eq!(row(&s, x).3, "Retiring");
                assert!(!file.exists());
            }
            _ => {
                assert!(s.retire_chunk(x).unwrap());
                assert_eq!(row(&s, x).3, "Retired");
                assert!(!file.exists());
            }
        }
        drop(s);
        let s = reopen(&p, one_per_chunk());
        let report = s.replay_report();
        let after = s.get("ledger", "a").unwrap();
        assert_eq!(after, a, "{stop}: a's latest value and version changed");
        match stop {
            "before" => {
                assert!(file.exists());
                assert_eq!(row(&s, x).3, "Closed");
                assert!(report.retirements_finished.is_empty());
            }
            "after-retiring" | "after-delete" => {
                assert!(!file.exists());
                assert_eq!(row(&s, x).3, "Retired");
                assert_eq!(report.retirements_finished, vec![x], "{stop}");
                let json = fs::read_to_string(p.join("replay.json")).unwrap();
                assert!(json.contains(&format!("\"retirements_finished\":[{x}]")), "{json}");
            }
            _ => {
                assert!(!file.exists());
                assert!(report.retirements_finished.is_empty());
            }
        }
        assert!(report.storage_errors.is_empty(), "{stop}: {:?}", report.storage_errors);
        // Anti-resurrection and the version high-water hold across the retirement.
        assert!(s.mutate(put("c", "c1", b"c")).version > a.version);
        drop(s);
        let s = reopen(&p, one_per_chunk());
        assert_eq!(s.get("ledger", "a").unwrap(), a);
        println!("T6 {stop}: a stays version {} after restart; finished {:?}", a.version, report.retirements_finished);
    }
}

#[test]
fn t7_replay_record_torn_tails() {
    // A short header and a short body in an open chunk are truncated and listed.
    for (label, tail) in [
        ("header", b"MBK1\x20\x00".to_vec()),
        ("body", {
            let mut t = b"MBK1".to_vec();
            t.extend(32u32.to_le_bytes());
            t.extend((!32u32).to_le_bytes());
            t.extend(0u32.to_le_bytes());
            t.extend(b"abcd");
            t
        }),
    ] {
        let p = path(&format!("t7-{label}"));
        {
            let s = Store::open(&p, Config::default()).unwrap();
            assert_eq!(s.mutate(put("a", "a", b"value")).code, 0);
        }
        let file = chunk_files(&p).pop().unwrap();
        let whole = fs::metadata(&file).unwrap().len();
        {
            use std::io::Write;
            fs::OpenOptions::new().append(true).open(&file).unwrap().write_all(&tail).unwrap();
        }
        let s = reopen(&p, Config::default());
        assert_eq!(s.get("ledger", "a").unwrap().bytes, b"value");
        assert_eq!(fs::metadata(&file).unwrap().len(), whole);
        let report = s.replay_report();
        let t: Vec<_> = report.stripes.iter().flat_map(|r| r.truncations.clone()).collect();
        assert_eq!(t.len(), 1, "{t:?}");
        assert!(t[0].0.ends_with(file.file_name().unwrap().to_str().unwrap()));
        assert_eq!((t[0].1, t[0].2), (whole, tail.len() as u64));
        assert!(!t[0].3.is_empty());
        let json = fs::read_to_string(p.join("replay.json")).unwrap();
        assert!(json.contains("truncations"), "{json}");
        println!("T7 short {label}: {} B truncated at offset {whole}: {}", tail.len(), t[0].3);
    }
}

#[test]
fn t7_corruption_refuses_start_with_the_record_written() {
    // A CRC failure in a complete final frame of the open chunk refuses start, as today.
    let p = path("t7-final-crc");
    {
        let s = Store::open(&p, Config::default()).unwrap();
        assert_eq!(s.mutate(put("a", "a", b"value")).code, 0);
    }
    let file = chunk_files(&p).pop().unwrap();
    let mut b = fs::read(&file).unwrap();
    b[12] ^= 1;
    fs::write(&file, &b).unwrap();
    #[cfg(not(windows))]
    let _ = fs::remove_file(p.join("owner.lock"));
    let e = Store::open(&p, Config::default()).err().unwrap();
    assert!(e.to_string().contains("CRC"), "{e}");
    assert_eq!(fs::read(&file).unwrap(), b);
    let json = fs::read_to_string(p.join("replay.json")).unwrap();
    assert!(json.contains("\"refusal\":") && json.contains("CRC"), "{json}");

    // Interior corruption in a closed chunk refuses start, and the record names it.
    let p = path("t7-closed-interior");
    let keys = same_stripe("a", 1);
    {
        let s = Store::open(&p, one_per_chunk()).unwrap();
        assert_eq!(s.mutate(put("a", "a", b"value")).code, 0);
        assert_eq!(s.mutate(put(&keys[0], "b", b"value")).code, 0);
    }
    // The lower-numbered chunk was closed by rotation; the other was open at the stop.
    let first = chunk_files(&p)
        .into_iter()
        .min_by_key(|f| f.file_stem().unwrap().to_str().unwrap().split_once('-').unwrap().1.parse::<u64>().unwrap())
        .unwrap();
    let mut b = fs::read(&first).unwrap();
    b[20] ^= 1;
    fs::write(&first, &b).unwrap();
    #[cfg(not(windows))]
    let _ = fs::remove_file(p.join("owner.lock"));
    let e = Store::open(&p, one_per_chunk()).err().unwrap();
    assert!(e.to_string().contains("interior-corrupt"), "{e}");
    let json = fs::read_to_string(p.join("replay.json")).unwrap();
    assert!(json.contains("interior-corrupt"), "{json}");
    println!("T7 refusals: final-frame CRC and closed-chunk interior corruption, each in replay.json");
}

#[test]
fn t7_storage_errors_start_read_only() {
    // A closed chunk's file missing with no retiring record.
    let p = path("t7-missing");
    let keys = same_stripe("a", 1);
    let x;
    {
        let s = Store::open(&p, one_per_chunk()).unwrap();
        assert_eq!(s.mutate(put("a", "a", b"value")).code, 0);
        assert_eq!(s.mutate(put(&keys[0], "b", b"value")).code, 0);
        x = s.chunk_rows().into_iter().find(|r| r.3 == "Closed").unwrap().0;
    }
    fs::remove_file(chunk_file(&p, x)).unwrap();
    let s = reopen(&p, one_per_chunk());
    assert!(s.storage_error());
    assert_eq!(s.mutate(put("c", "c", b"value")).code, 10);
    let report = s.replay_report();
    assert!(report.storage_errors[0].contains("missing"), "{:?}", report.storage_errors);
    drop(s);

    // A chunk file with no open record.
    let p = path("t7-stray");
    {
        let s = Store::open(&p, Config::default()).unwrap();
        assert_eq!(s.mutate(put("a", "a", b"value")).code, 0);
    }
    fs::write(p.join("chunks").join("forever").join("0-999.log"), b"").unwrap();
    let s = reopen(&p, Config::default());
    assert!(s.storage_error());
    assert_eq!(s.mutate(put("c", "c", b"value")).code, 10);
    assert_eq!(s.get("ledger", "a").unwrap().bytes, b"value");
    assert!(s.replay_report().storage_errors[0].contains("no open record"));
    println!("T7 storage errors: a missing closed chunk and a stray chunk file each start read_only");
}

#[cfg(windows)]
#[test]
fn t7_held_delete_stays_retiring_and_is_retried() {
    use std::os::windows::fs::OpenOptionsExt;
    let p = path("t7-held");
    let (s, x, _) = superseded_chunk(&p, one_per_chunk());
    let file = chunk_file(&p, x);
    // An outside reader that denies delete (FILE_SHARE_READ only).
    let holder = fs::OpenOptions::new().read(true).share_mode(1).open(&file).unwrap();
    assert!(!s.retire_chunk(x).unwrap());
    assert_eq!(row(&s, x).3, "Retiring");
    assert_eq!(s.retire_blocked(), 1);
    assert!(file.exists());
    assert_eq!(s.retry_retiring().unwrap(), 0);
    drop(holder);
    assert_eq!(s.retry_retiring().unwrap(), 1);
    assert_eq!(s.retire_blocked(), 0);
    assert_eq!(row(&s, x).3, "Retired");
    assert!(!file.exists());
    println!("T7 held delete: retiring and counted while held; retried and retired once released");
}

#[test]
fn t12_replay_after_failed_sync_and_a_cut_put() {
    let p = path("t12");
    let (os, flush);
    {
        let s = Store::open(&p, Config { flush_ms: 0, ..Config::default() }).unwrap();
        os = s.mutate(put("os", "os", b"value"));
        let mut m = put("flush", "flush", b"value");
        m.ack = 2;
        flush = s.mutate(m);
        assert_eq!((os.code, flush.code), (0, 0));
    }
    {
        let s = reopen(&p, Config { fail_sync: true, flush_ms: 0, ..Config::default() });
        let mut m = put("unsynced", "unsynced", b"value");
        m.ack = 2;
        assert_eq!(s.mutate(m).code, 16);
        assert_eq!(s.mutate(put("after", "after", b"value")).code, 10);
    }
    let before_cut = chunk_files(&p);
    {
        let s = reopen(&p, Config { fail_partial_append: true, ..Config::default() });
        assert_eq!(s.mutate(put("cut", "cut", b"value")).code, 16);
    }
    let cut: Vec<_> = chunk_files(&p).into_iter().filter(|f| !before_cut.contains(f)).collect();
    assert_eq!(cut.len(), 1);
    let s = reopen(&p, Config::default());
    let report = s.replay_report();
    let t: Vec<_> = report.stripes.iter().flat_map(|r| r.truncations.clone()).collect();
    // Only the open chunk's short tail is truncated; the cut chunk held nothing whole, so it retires.
    assert_eq!(t.len(), 1, "{t:?}");
    assert!(t[0].0.ends_with(cut[0].file_name().unwrap().to_str().unwrap()));
    assert_eq!(t[0].1, 0);
    assert!(!cut[0].exists());
    assert_eq!(s.get("ledger", "os").unwrap().version, os.version);
    assert_eq!(s.get("ledger", "flush").unwrap().version, flush.version);
    assert!(s.get("ledger", "cut").is_none());
    assert!(!s.storage_error());
    assert!(s.mutate(put("next", "next", b"value")).version > flush.version);
    println!("T12: acknowledged os and flush puts replayed; the cut put's {} B tail truncated and its empty chunk retired", t[0].2);
}
