use membus::*;
use std::{path::PathBuf, sync::Arc, time::Instant};
fn path(label: &str) -> PathBuf {
    PathBuf::from(std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()))
        .join(format!("group-{label}-{}-{}", std::process::id(), now_ms()))
}
fn mutation(i: usize) -> Mutation {
    Mutation {
        ns: "ledger".into(),
        key: format!("key{i}"),
        op_id: format!("op{i}"),
        deadline: now_ms() + 10000,
        ack: 2,
        expected: Some(0),
        bytes: vec![b'x'; 1039],
        remove: false,
    }
}
#[test]
fn covering_flush_groups_and_hot_reads() {
    let s = Arc::new(Store::open(&path("cover"), Config::default()).unwrap());
    let start = Instant::now();
    std::thread::scope(|scope| {
        let handles: Vec<_> = (0..32)
            .map(|i| {
                let s = &s;
                scope.spawn(move || s.mutate(mutation(i)))
            })
            .collect();
        for h in handles {
            let out = h.join().unwrap();
            assert_eq!(out.code, 0);
            assert_eq!(out.ack, 2);
        }
    });
    let (bytes, writes, flushes) = s.io_metrics();
    assert_eq!(writes, 32);
    assert!(flushes <= 8, "{flushes}");
    assert!(bytes > 32 * 1039);
    for i in 0..32 {
        assert_eq!(
            s.get("ledger", &format!("key{i}")).unwrap().bytes.len(),
            1039
        );
    }
    println!(
        "covering32 seconds={} bytes={bytes} writes={writes} flushes={flushes}",
        start.elapsed().as_secs_f64()
    );
}
#[test]
fn exact_disk_cap_and_empty_queue_refusal() {
    let dir = path("cap");
    let bytes = Store::open(&dir, Config::default()).unwrap();
    let mut m = mutation(0);
    m.ack = 1;
    assert_eq!(bytes.mutate(m.clone()).code, 0);
    let used = bytes.state_bytes();
    drop(bytes);
    let s = Store::open(
        &dir,
        Config {
            state_limit_bytes: used,
            ..Config::default()
        },
    )
    .unwrap();
    assert_eq!(s.state_bytes(), used);
    assert_eq!(s.mutate(mutation(1)).code, 7);
    assert!(s.get("ledger", "key0").is_some());
    drop(s);
    let s = Store::open(
        &path("emptyqueue"),
        Config {
            queue_bytes: 1,
            ..Config::default()
        },
    )
    .unwrap();
    let mut m = mutation(1);
    m.bytes.clear();
    assert_eq!(s.mutate(m).code, 6);
    assert_eq!(s.metrics(), (0, 0, 0));
}
#[test]
fn batch_same_key_order_and_reply_order() {
    let s = Store::open(&path("batchorder"), Config::default()).unwrap();
    let mut p = (now_ms() + 10000).to_le_bytes().to_vec();
    lp(&mut p, b"batch");
    lp(&mut p, b"ledger");
    p.extend(3u16.to_le_bytes());
    for (id, key, value) in [
        ("first", "same", "one"),
        ("independent", "other", "two"),
        ("second", "same", "three"),
    ] {
        p.push(PUT);
        lp(&mut p, id.as_bytes());
        lp(&mut p, key.as_bytes());
        p.push(2);
        lp(&mut p, value.as_bytes());
    }
    let reply = s.dispatch(&frame(BATCH, 1, &p)[4..]);
    assert_eq!(u16::from_le_bytes(reply[17..19].try_into().unwrap()), 0);
    assert_eq!(s.get("ledger", "same").unwrap().bytes, b"three");
    assert_eq!(s.get("ledger", "other").unwrap().bytes, b"two");
    let versions: Vec<_> = (0..3)
        .map(|i| {
            let offset = 21 + i * 11;
            assert_eq!(&reply[offset..offset + 2], &[0, 0]);
            u64::from_le_bytes(reply[offset + 2..offset + 10].try_into().unwrap())
        })
        .collect();
    assert!(versions[0] < versions[2]);
}

#[test]
fn late_flush_notification_uses_covering_sync() {
    let s = Arc::new(
        Store::open(
            &path("late-notice"),
            Config {
                flush_ms: 20,
                flush_notice_delay_ms: 100,
                ..Config::default()
            },
        )
        .unwrap(),
    );
    // key1/key3 share a stripe. The first notice arrives around100ms; the
    // owner then appends key3 but holds its notice until200ms. The first sync
    // gathers until120ms, covering both complete records before key3's notice.
    assert_eq!(crc32(b"ledger\0key1") % 4, crc32(b"ledger\0key3") % 4);
    let first = s.clone();
    let thread = std::thread::spawn(move || first.mutate(mutation(1)));
    std::thread::sleep(std::time::Duration::from_millis(20));
    let second = s.clone();
    let other = std::thread::spawn(move || second.mutate(mutation(3)));
    assert!(s.get("ledger", "key1").is_none());
    assert_eq!(thread.join().unwrap().ack, 2);
    assert_eq!(other.join().unwrap().ack, 2);
    assert_eq!(s.io_metrics().2, 1);
}
