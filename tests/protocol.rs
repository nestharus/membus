use membus::*;
use std::{
    fs,
    io::Cursor as IoCursor,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
fn path(label: &str) -> PathBuf {
    let p = std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into());
    PathBuf::from(p).join(format!("{label}-{}-{}", std::process::id(), now_ms()))
}
/// Chunk files under the state's chunks/ (CHUNK-RETIREMENT.md 5.2: chunks/<class>/<stripe>-<seq>.log).
fn chunk_files(p: &std::path::Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for class in fs::read_dir(p.join("chunks")).into_iter().flatten().flatten() {
        if class.path().is_dir() {
            for f in fs::read_dir(class.path()).unwrap().flatten() {
                if f.path().extension().is_some_and(|x| x == "log") {
                    out.push(f.path());
                }
            }
        }
    }
    out.sort_by_key(|f| chunk_name(f));
    out
}
/// (stripe, seq) from a chunk file name.
fn chunk_name(f: &std::path::Path) -> (u32, u64) {
    let stem = f.file_stem().unwrap().to_str().unwrap();
    let (stripe, seq) = stem.split_once('-').unwrap();
    (stripe.parse().unwrap(), seq.parse().unwrap())
}
/// A stripe's persisted bytes: its chunk files together (the pre-chunk `stripe-{i}.log` length).
fn stripe_bytes(p: &std::path::Path, stripe: u32) -> u64 {
    chunk_files(p)
        .iter()
        .filter(|f| chunk_name(f).0 == stripe)
        .map(|f| fs::metadata(f).unwrap().len())
        .sum()
}
fn m(key: &str, id: &str) -> Mutation {
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
fn wire(m: &Mutation, op: u8) -> Vec<u8> {
    let mut p = m.deadline.to_le_bytes().to_vec();
    lp(&mut p, m.op_id.as_bytes());
    lp(&mut p, m.ns.as_bytes());
    lp(&mut p, m.key.as_bytes());
    p.push(m.ack);
    if op == CONDITIONAL_PUT || op == REMOVE {
        p.extend(m.expected.unwrap_or(0).to_le_bytes())
    }
    if op != REMOVE {
        lp(&mut p, &m.bytes)
    }
    frame(op, 42, &p)[4..].to_vec()
}
fn code(s: &Store, b: &[u8]) -> u16 {
    u16::from_le_bytes(s.dispatch(b)[17..19].try_into().unwrap())
}
#[test]
fn golden_and_bounds() {
    let mut x = m("row/0001", "golden-op");
    x.deadline = 123456789;
    x.ack = 2;
    x.bytes = b"hello".to_vec();
    let b = frame(PUT, 42, &wire(&x, PUT)[13..]);
    let hex = b.iter().map(|x| format!("{x:02x}")).collect::<String>();
    assert_eq!(hex, include_str!("fixtures/put.hex").trim());
    println!("golden={hex}");
    assert!(read_frame(&mut IoCursor::new((MAX_FRAME as u32 + 1).to_le_bytes())).is_err());
    assert!(read_frame(&mut IoCursor::new([12u8, 0, 0, 0])).is_err());
    assert!(read_frame(&mut IoCursor::new(&b[..b.len() - 1])).is_err());
    let s = Store::open(&path("bounds"), Config::default()).unwrap();
    assert_eq!(code(&s, &[0; 2]), 3);
    let mut bad = wire(&x, PUT);
    bad[0] = 2;
    assert_eq!(code(&s, &bad), 1);
    assert_eq!(code(&s, &frame(99, 42, &[0])[4..]), 3);
    let mut p = (now_ms() + 10000).to_le_bytes().to_vec();
    lp(&mut p, b"id");
    lp(&mut p, b"ns");
    assert_eq!(code(&s, &frame(99, 42, &p)[4..]), 2);
    let mut bad = wire(&m("key", "utf8"), PUT);
    bad[25] = 255;
    assert_eq!(code(&s, &bad), 3);
    let mut x = m("key", "oversize");
    x.bytes = vec![0; MAX_VALUE + 1];
    assert_eq!(code(&s, &wire(&x, PUT)), 4);
}
#[test]
fn every_refusal_on_wire() {
    let s = Store::open(&path("codes"), Config::default()).unwrap();
    let mut x = m("key", "expired");
    x.deadline = now_ms() - 1;
    assert_eq!(code(&s, &wire(&x, PUT)), 5);
    s.set_read_only(true);
    assert_eq!(code(&s, &wire(&m("key", "ro"), PUT)), 10);
    s.set_read_only(false);
    s.set_warming(true);
    assert_eq!(code(&s, &wire(&m("key", "warm"), PUT)), 11);
    s.set_warming(false);
    let mut x = m("key", "mem");
    x.ack = 0;
    assert_eq!(code(&s, &wire(&x, PUT)), 15);
    assert_eq!(s.mutate(m("key", "one")).code, 0);
    let mut x = m("key", "two");
    x.expected = Some(0);
    assert_eq!(code(&s, &wire(&x, CONDITIONAL_PUT)), 12);
    let mut x = m("key", "one");
    x.bytes = b"different".to_vec();
    assert_eq!(code(&s, &wire(&x, PUT)), 14);
    for (label, c, expected) in [
        (
            "capacity",
            Config {
                capacity: 0,
                ..Config::default()
            },
            7,
        ),
        (
            "quota",
            Config {
                quota: 0,
                ..Config::default()
            },
            8,
        ),
        (
            "index",
            Config {
                index_cap: 0,
                ..Config::default()
            },
            9,
        ),
        (
            "busy",
            Config {
                queue_bytes: 0,
                ..Config::default()
            },
            6,
        ),
    ] {
        let s = Store::open(&path(label), c).unwrap();
        assert_eq!(code(&s, &wire(&m("key", label), PUT)), expected);
    }
    let s = Store::open(
        &path("retry"),
        Config {
            retry_ms: 1,
            ..Config::default()
        },
    )
    .unwrap();
    let x = m("key", "old");
    let o = s.mutate(x.clone());
    assert_eq!(o.code, 0);
    std::thread::sleep(Duration::from_millis(5));
    assert_eq!(code(&s, &wire(&x, PUT)), 13);
    let s = Store::open(
        &path("dedup-full"),
        Config {
            dedup_cap: 0,
            ..Config::default()
        },
    )
    .unwrap();
    assert_eq!(code(&s, &wire(&m("key", "full"), PUT)), 18);
    let s = Store::open(
        &path("io-failed"),
        Config {
            fail_after_append: true,
            ..Config::default()
        },
    )
    .unwrap();
    assert_eq!(code(&s, &wire(&m("key", "io"), PUT)), 16);
    assert!(s.get("ledger", "key").is_none());
    assert_eq!(code(&s, &wire(&m("another", "readonly"), PUT)), 10);
    println!("closed codes 1..16 and 18 produced on wire; pending 17 in concurrency test; transport bound before allocation");
}
#[test]
fn pending_conflict_duplicate_and_hot_read() {
    let s = Arc::new(
        Store::open(
            &path("pending"),
            Config {
                write_delay_ms: 150,
                ..Config::default()
            },
        )
        .unwrap(),
    );
    assert_eq!(s.mutate(m("hot", "seed")).code, 0);
    let mut x = m("key", "pending");
    x.expected = Some(0);
    x.ack = 2;
    let s2 = s.clone();
    let x2 = x.clone();
    let writer = std::thread::spawn(move || s2.mutate(x2));
    while !s.pending("ledger", "key") {
        std::thread::yield_now()
    }
    let started = Instant::now();
    assert!(s.get("ledger", "hot").is_some());
    assert!(started.elapsed() < Duration::from_millis(100));
    let mut competing = m("key", "competing");
    competing.expected = Some(0);
    assert_eq!(code(&s, &wire(&competing, CONDITIONAL_PUT)), 19);
    assert!(s.get("ledger", "key").is_none());
    let mut short = x.clone();
    short.deadline = now_ms() + 5;
    assert_eq!(code(&s, &wire(&short, CONDITIONAL_PUT)), 17);
    let duplicate = s.mutate(x);
    assert_eq!(writer.join().unwrap(), duplicate);
    assert!(s.get("ledger", "key").is_some());
    println!("pending absent conflict=19; committed conflict=12 separately; duplicate waited for original outcome; hot get completed without writer/flush wait");
}

#[test]
fn golden_hello_and_reply() {
    let hex = |b: Vec<u8>| b.iter().map(|x| format!("{x:02x}")).collect::<String>();
    let mut p = (MAX_FRAME as u32).to_le_bytes().to_vec();
    p.push(0);
    lp(&mut p, b"python");
    assert_eq!(
        hex(frame(HELLO, 1, &p)),
        include_str!("fixtures/hello.hex").trim()
    );
    let mut p = vec![1];
    p.extend(7u64.to_le_bytes());
    p.push(1);
    lp(&mut p, b"hello");
    assert_eq!(
        hex(reply(GET, 42, 0, &p)),
        include_str!("fixtures/get-reply.hex").trim()
    );
    println!("golden hello and get reply byte-identical fixtures");
}

#[test]
fn past_window_ring_restart_and_length_guard() {
    let p = path("past-window");
    let c = Config {
        retry_ms: 1,
        dedup_cap: 2,
        ..Config::default()
    };
    {
        let s = Store::open(&p, c.clone()).unwrap();
        let first = m("key", "past");
        assert_eq!(s.mutate(first.clone()).code, 0);
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(s.mutate(m("new", "new")).code, 0);
        assert_eq!(s.mutate(first).code, 13);
    }
    #[cfg(not(windows))]
    fs::remove_file(p.join("owner.lock")).unwrap();
    std::thread::sleep(Duration::from_millis(5));
    {
        let s = Store::open(&p, c).unwrap();
        assert_eq!(s.mutate(m("key", "past")).code, 0);
        assert!(s.metrics().0 > 0);
    }
    #[cfg(not(windows))]
    fs::remove_file(p.join("owner.lock")).unwrap();
    let log = chunk_files(&p)
        .into_iter()
        .find(|p| fs::metadata(p).unwrap().len() > 16)
        .unwrap();
    let mut bytes = fs::read(&log).unwrap();
    bytes[4] ^= 0x10;
    fs::write(&log, &bytes).unwrap();
    let e = Store::open(&p, Config::default()).err().unwrap();
    assert!(e.to_string().contains("length complement"));
    assert_eq!(fs::read(log).unwrap(), bytes);
    println!("expired retired id=13; after restart unknown id executes; interior length corruption reported without truncation");
}

#[test]
fn partial_append_fences_admitted_followers_and_replays_prior_ack() {
    let p = path("partial-fence");
    let seed = m("seed", "seed");
    {
        let s = Store::open(&p, Config::default()).unwrap();
        assert_eq!(s.mutate(seed).code, 0);
    }
    #[cfg(not(windows))]
    fs::remove_file(p.join("owner.lock")).unwrap();
    let keystripe = |k: &str| {
        let mut b = b"ledger\0".to_vec();
        b.extend(k.as_bytes());
        crc32(&b) % 4
    };
    let wanted = keystripe("seed");
    let keys: Vec<_> = (0..100)
        .map(|n| format!("failed/{n}"))
        .filter(|k| keystripe(k) == wanted)
        .take(2)
        .collect();
    let prior = stripe_bytes(&p, wanted);
    {
        let s = Arc::new(
            Store::open(
                &p,
                Config {
                    fail_partial_append: true,
                    write_delay_ms: 100,
                    ..Config::default()
                },
            )
            .unwrap(),
        );
        let a = s.clone();
        let first = m(&keys[0], "first");
        let t1 = std::thread::spawn(move || a.mutate(first));
        while !s.pending("ledger", &keys[0]) {
            std::thread::yield_now()
        }
        let a = s.clone();
        let second = m(&keys[1], "second");
        let t2 = std::thread::spawn(move || a.mutate(second));
        while !s.pending("ledger", &keys[1]) {
            std::thread::yield_now()
        }
        assert_eq!(t1.join().unwrap().code, 16);
        assert_eq!(t2.join().unwrap().code, 16);
        assert_eq!(s.get("ledger", "seed").unwrap().bytes, b"value");
    }
    #[cfg(not(windows))]
    fs::remove_file(p.join("owner.lock")).unwrap();
    let torn = stripe_bytes(&p, wanted);
    assert!(torn > prior);
    {
        let s = Store::open(&p, Config::default()).unwrap();
        assert_eq!(s.get("ledger", "seed").unwrap().bytes, b"value");
        assert!(s.get("ledger", &keys[0]).is_none());
        assert!(s.get("ledger", &keys[1]).is_none());
        assert_eq!(stripe_bytes(&p, wanted), prior);
    }
    println!("partial append fenced both admitted followers; torn bytes={} discarded; prior acknowledged key replayed",torn-prior);
}
#[test]
fn crc_replay_tombstone_torn_and_corrupt() {
    let p = path("replay");
    {
        let s = Store::open(
            &p,
            Config {
                flush_ms: 0,
                ..Config::default()
            },
        )
        .unwrap();
        assert_eq!(s.mutate(m("os", "os")).code, 0);
        let mut x = m("flush", "flush");
        x.ack = 2;
        assert_eq!(s.mutate(x).code, 0);
        let v = s.mutate(m("remove", "create"));
        let mut x = m("remove", "delete");
        x.remove = true;
        x.bytes.clear();
        x.expected = Some(v.version);
        assert_eq!(s.mutate(x).code, 0);
        println!("replay write stats {:?}", s.metrics());
    }
    #[cfg(not(windows))]
    fs::remove_file(p.join("owner.lock")).unwrap();
    {
        use std::io::Write;
        // The newest chunk was open when the store stopped: its torn tail is truncated at replay.
        let newest = chunk_files(&p).into_iter().max_by_key(|f| chunk_name(f).1).unwrap();
        let mut f = fs::OpenOptions::new()
            .append(true)
            .open(newest)
            .unwrap();
        f.write_all(b"MBK1\x20\x00").unwrap();
    }
    {
        let s = Store::open(&p, Config::default()).unwrap();
        assert_eq!(s.get("ledger", "os").unwrap().bytes, b"value");
        assert_eq!(s.get("ledger", "flush").unwrap().bytes, b"value");
        assert!(s.get("ledger", "remove").is_none());
        let duplicate = s.mutate(m("os", "os"));
        assert_eq!(duplicate.code, 0);
        assert_eq!(s.metrics().0, 0);
    }
    #[cfg(not(windows))]
    fs::remove_file(p.join("owner.lock")).unwrap();
    let log = chunk_files(&p)
        .into_iter()
        .find(|p| fs::metadata(p).unwrap().len() > 12)
        .unwrap();
    let mut b = fs::read(&log).unwrap();
    b[12] ^= 1;
    fs::write(log, b).unwrap();
    let e = Store::open(&p, Config::default()).err().unwrap();
    assert!(e.to_string().contains("interior-corrupt"));
    println!("acknowledged os/flush ids replayed; tombstone persists; torn tail discarded; interior CRC reported");
}
