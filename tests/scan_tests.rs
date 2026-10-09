use membus::*;
use std::{
    path::PathBuf,
    sync::{Arc, Barrier},
    time::Duration,
};

fn path(label: &str) -> PathBuf {
    PathBuf::from(std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()))
        .join(format!("scan-{label}-{}-{}", std::process::id(), now_ms()))
}
fn put(s: &Store, ns: &str, key: &str, id: &str, bytes: Vec<u8>, ack: u8, remove: bool) {
    let o = s.mutate(Mutation {
        ns: ns.into(),
        key: key.into(),
        op_id: id.into(),
        deadline: now_ms() + 10000,
        ack,
        expected: None,
        bytes,
        remove,
    });
    assert_eq!(o.code, 0);
}
fn request(s: &Store, op: u8, ns: &str, suffix: &[u8], max: usize) -> Result<Vec<u8>, u16> {
    let mut p = (now_ms() + 10000).to_le_bytes().to_vec();
    lp(&mut p, b"scan-read");
    lp(&mut p, ns.as_bytes());
    p.extend(suffix);
    let r = s.dispatch_for_limit(&frame(op, 42, &p)[4..], false, max);
    assert!(r.len() - 4 <= max);
    let code = u16::from_le_bytes(r[17..19].try_into().unwrap());
    if code != 0 {
        Err(code)
    } else {
        Ok(r[19..].to_vec())
    }
}
fn open(s: &Store, ns: &str) -> Result<Vec<u8>, u16> {
    let b = request(s, SNAPSHOT_OPEN, ns, &[], MAX_FRAME)?;
    let mut c = Cursor::new(&b);
    let token = c.bytes(16)?.to_vec();
    c.end()?;
    Ok(token)
}
fn release(s: &Store, ns: &str, token: &[u8]) -> Result<(), u16> {
    let mut b = Vec::new();
    lp(&mut b, token);
    request(s, SNAPSHOT_RELEASE, ns, &b, MAX_FRAME).map(|b| assert!(b.is_empty()))
}
#[derive(Debug, PartialEq)]
struct Page {
    snapshot: Vec<u8>,
    cursor: Vec<u8>,
    done: bool,
    rows: Vec<(String, u64, u8, Vec<u8>)>,
}
fn suffix(
    op: u8,
    token: &[u8],
    cursor: &[u8],
    lo: &str,
    hi: &str,
    limit: u16,
    bytes: u32,
) -> Vec<u8> {
    let mut p = Vec::new();
    lp(&mut p, token);
    lp(&mut p, cursor);
    p.extend(limit.to_le_bytes());
    p.extend(bytes.to_le_bytes());
    lp(&mut p, lo.as_bytes());
    if op == SCAN_RANGE {
        lp(&mut p, hi.as_bytes());
    }
    p
}
fn page(
    s: &Store,
    op: u8,
    ns: &str,
    token: &[u8],
    cursor: &[u8],
    lo: &str,
    hi: &str,
    limit: u16,
    budget: u32,
    max: usize,
) -> Result<Page, u16> {
    let b = request(
        s,
        op,
        ns,
        &suffix(op, token, cursor, lo, hi, limit, budget),
        max,
    )?;
    let mut c = Cursor::new(&b);
    let snapshot = c.bytes(16)?.to_vec();
    let cursor = c.bytes(2 * MAX_ID + 48)?.to_vec();
    let done = c.u8()? != 0;
    let n = c.u16()?;
    let mut rows = Vec::new();
    for _ in 0..n {
        let key = c.text()?;
        assert_eq!(c.u8()?, 1);
        rows.push((key, c.u64()?, c.u8()?, c.bytes(MAX_VALUE)?.to_vec()));
    }
    c.end()?;
    assert_eq!(done, cursor.is_empty());
    Ok(Page {
        snapshot,
        cursor,
        done,
        rows,
    })
}
fn all(s: &Store, ns: &str, token: &[u8]) -> Page {
    page(
        s,
        SCAN_PREFIX,
        ns,
        token,
        &[],
        "",
        "",
        128,
        MAX_FRAME as u32,
        MAX_FRAME,
    )
    .unwrap()
}

#[test]
fn shared_snapshot_tombstones_utf8_ranges_and_visibility() {
    let s = Store::open(
        &path("shared"),
        Config {
            allow_mem: true,
            ..Config::default()
        },
    )
    .unwrap();
    for (i, k) in ["a", "aa", "b", "é", "界"].iter().enumerate() {
        put(
            &s,
            "ledger",
            k,
            &format!("seed{i}"),
            k.as_bytes().to_vec(),
            if i == 0 { 2 } else { 1 },
            false,
        );
    }
    put(
        &s,
        "other",
        "secret",
        "secret",
        b"secret".to_vec(),
        1,
        false,
    );
    put(&s, "ledger", "gone", "tombstone", vec![], 1, true);
    put(&s, "ledger", "mem", "mem", b"mem".to_vec(), 0, false);
    let token = open(&s, "ledger").unwrap();
    let before = all(&s, "ledger", &token);
    assert_eq!(
        before.rows.iter().map(|r| r.0.as_str()).collect::<Vec<_>>(),
        ["a", "aa", "b", "é", "界"]
    );
    assert_eq!(before.rows[0].2, 2);
    put(&s, "ledger", "a", "replace", b"new".to_vec(), 1, false);
    put(&s, "ledger", "b", "remove", vec![], 1, true);
    put(&s, "ledger", "az", "insert", b"later".to_vec(), 2, false);
    assert_eq!(all(&s, "ledger", &token), before);
    let prefix = page(
        &s,
        SCAN_PREFIX,
        "ledger",
        &token,
        &[],
        "a",
        "",
        128,
        1024,
        1024,
    )
    .unwrap();
    assert_eq!(prefix.rows, before.rows[..2]);
    let range = page(
        &s,
        SCAN_RANGE,
        "ledger",
        &token,
        &[],
        "aa",
        "界",
        128,
        1024,
        1024,
    )
    .unwrap();
    assert_eq!(range.rows, before.rows[1..4]);
    assert!(page(
        &s,
        SCAN_RANGE,
        "ledger",
        &token,
        &[],
        "é",
        "é",
        128,
        1024,
        1024
    )
    .unwrap()
    .rows
    .is_empty());
    assert_eq!(
        page(
            &s,
            SCAN_RANGE,
            "ledger",
            &token,
            &[],
            "界",
            "",
            128,
            1024,
            1024
        )
        .unwrap()
        .rows,
        before.rows[4..]
    );
    let later = open(&s, "ledger").unwrap();
    let now = all(&s, "ledger", &later);
    assert_eq!(now.rows[0].3, b"new");
    assert!(now.rows.iter().any(|r| r.0 == "az"));
    assert!(!now.rows.iter().any(|r| r.0 == "b"));
    assert_eq!(
        all(&s, "other", &open(&s, "other").unwrap()).rows[0].0,
        "secret"
    );
    assert_eq!(
        page(&s, SCAN_PREFIX, "other", &token, &[], "", "", 1, 1024, 1024),
        Err(21)
    );
    assert_eq!(release(&s, "other", &token), Err(21));
    assert_eq!(all(&s, "ledger", &token), before);
    release(&s, "ledger", &token).unwrap();
    assert_eq!(release(&s, "ledger", &token), Err(20));
}

#[test]
fn retryable_query_bound_cursors_and_invalid_pages() {
    let s = Store::open(&path("cursors"), Config::default()).unwrap();
    for k in ["row/1", "row/2", "row/3"] {
        put(&s, "ledger", k, k, vec![42; 50], 1, false);
    }
    let first = page(
        &s,
        SCAN_PREFIX,
        "ledger",
        &[],
        &[],
        "row/",
        "",
        1,
        1024,
        1024,
    )
    .unwrap();
    assert!(!first.done);
    assert_eq!(first.rows.len(), 1);
    let next = page(
        &s,
        SCAN_PREFIX,
        "ledger",
        &first.snapshot,
        &first.cursor,
        "row/",
        "",
        1,
        1024,
        1024,
    )
    .unwrap();
    assert_eq!(
        page(
            &s,
            SCAN_PREFIX,
            "ledger",
            &first.snapshot,
            &first.cursor,
            "row/",
            "",
            1,
            1024,
            1024
        )
        .unwrap(),
        next
    );
    assert_eq!(next.rows[0].0, "row/2");
    assert_eq!(
        page(
            &s,
            SCAN_PREFIX,
            "ledger",
            &first.snapshot,
            &first.cursor,
            "row",
            "",
            1,
            1024,
            1024
        ),
        Err(21)
    );
    assert_eq!(
        page(
            &s,
            SCAN_RANGE,
            "ledger",
            &first.snapshot,
            &first.cursor,
            "row/",
            "",
            1,
            1024,
            1024
        ),
        Err(21)
    );
    assert_eq!(
        page(
            &s,
            SCAN_PREFIX,
            "ledger",
            &[],
            &first.cursor,
            "row/",
            "",
            1,
            1024,
            1024
        ),
        Err(21)
    );
    let mut invalid = first.cursor.clone();
    invalid[16..24].copy_from_slice(&999u64.to_le_bytes());
    assert_eq!(
        page(
            &s,
            SCAN_PREFIX,
            "ledger",
            &first.snapshot,
            &invalid,
            "row/",
            "",
            1,
            1024,
            1024
        ),
        Err(21)
    );
    let mut forged = first.cursor.clone();
    forged[28] = b'x';
    assert_eq!(
        page(
            &s,
            SCAN_PREFIX,
            "ledger",
            &first.snapshot,
            &forged,
            "xow/",
            "",
            1,
            1024,
            1024
        ),
        Err(21)
    );
    let mut skipped = first.cursor.clone();
    skipped[16..24].copy_from_slice(&2u64.to_le_bytes());
    assert_eq!(
        page(
            &s,
            SCAN_PREFIX,
            "ledger",
            &first.snapshot,
            &skipped,
            "row/",
            "",
            1,
            1024,
            1024
        ),
        Err(21)
    );
    let other = open(&s, "ledger").unwrap();
    assert_eq!(
        page(
            &s,
            SCAN_PREFIX,
            "ledger",
            &other,
            &first.cursor,
            "row/",
            "",
            1,
            1024,
            1024
        ),
        Err(21)
    );
    for (limit, budget) in [(0, 1024), (129, 1024), (1, 41)] {
        assert_eq!(
            page(
                &s,
                SCAN_PREFIX,
                "ledger",
                &[],
                &[],
                "",
                "",
                limit,
                budget,
                1024
            ),
            Err(3)
        );
    }
    assert_eq!(
        page(&s, SCAN_RANGE, "ledger", &[], &[], "z", "a", 1, 1024, 1024),
        Err(3)
    );
    assert_eq!(
        page(&s, SCAN_PREFIX, "ledger", &[1], &[], "", "", 1, 1024, 1024),
        Err(21)
    );
    let mut bad = suffix(SCAN_PREFIX, &[], &[], "a", "", 1, 1024);
    *bad.last_mut().unwrap() = 255;
    assert_eq!(request(&s, SCAN_PREFIX, "ledger", &bad, 1024), Err(3));
    bad = suffix(SCAN_PREFIX, &[], &[], "", "", 1, 1024);
    bad.push(1);
    assert_eq!(request(&s, SCAN_PREFIX, "ledger", &bad, 1024), Err(3));
    assert_eq!(
        page(
            &s,
            SCAN_PREFIX,
            "ledger",
            &first.snapshot,
            &[],
            &"x".repeat(1025),
            "",
            1,
            4096,
            4096
        ),
        Err(4)
    );
    let end = page(
        &s,
        SCAN_PREFIX,
        "ledger",
        &first.snapshot,
        &next.cursor,
        "row/",
        "",
        128,
        1024,
        1024,
    )
    .unwrap();
    assert!(end.done);
    assert_eq!(end.rows[0].0, "row/3");
}

#[test]
fn page_bytes_negotiated_limit_large_values_and_refusal_cleanup() {
    let s = Store::open(
        &path("bytes"),
        Config {
            snapshot_max: 1,
            ..Config::default()
        },
    )
    .unwrap();
    put(&s, "ledger", "row/large", "large", vec![5; 59499], 1, false);
    // Implicit creation must release its slot on first-item refusal.
    for _ in 0..3 {
        assert_eq!(
            page(
                &s,
                SCAN_PREFIX,
                "ledger",
                &[],
                &[],
                "",
                "",
                128,
                MAX_FRAME as u32,
                1024
            ),
            Err(4)
        );
    }
    let token = open(&s, "ledger").unwrap();
    assert_eq!(
        page(
            &s,
            SCAN_PREFIX,
            "ledger",
            &token,
            &[],
            "",
            "",
            128,
            1024,
            MAX_FRAME
        ),
        Err(4)
    );
    let large = page(
        &s,
        SCAN_PREFIX,
        "ledger",
        &token,
        &[],
        "",
        "",
        128,
        60000,
        MAX_FRAME,
    )
    .unwrap();
    assert_eq!(large.rows[0].3.len(), 59499);
    assert!(large.done);
    release(&s, "ledger", &token).unwrap();
    put(&s, "ledger", "row/large", "remove-large", vec![], 1, true);
    for i in 0..10 {
        put(
            &s,
            "ledger",
            &format!("small/{i}"),
            &format!("small/{i}"),
            vec![1; 200],
            1,
            false,
        );
    }
    let first = page(
        &s,
        SCAN_PREFIX,
        "ledger",
        &[],
        &[],
        "small/",
        "",
        128,
        MAX_FRAME as u32,
        1024,
    )
    .unwrap();
    assert_eq!(first.rows.len(), 4);
    assert!(!first.done);
    let second = page(
        &s,
        SCAN_PREFIX,
        "ledger",
        &first.snapshot,
        &first.cursor,
        "small/",
        "",
        2,
        1024,
        1024,
    )
    .unwrap();
    assert_eq!(second.rows.len(), 2);
    assert_eq!(second.rows[0].0, "small/4");
    let smaller = page(
        &s,
        SCAN_PREFIX,
        "ledger",
        &first.snapshot,
        &[],
        "small/",
        "",
        128,
        320,
        MAX_FRAME,
    )
    .unwrap();
    assert_eq!(smaller.rows.len(), 1);
    // Terminal single row can fit without charging a nonexistent next cursor.
    let exact = page(
        &s,
        SCAN_RANGE,
        "ledger",
        &first.snapshot,
        &[],
        "small/9",
        "",
        1,
        42 + 4 + 7 + 14 + 200,
        MAX_FRAME,
    )
    .unwrap();
    assert!(exact.done);
}

#[test]
fn fixed_expiry_and_restart_invalidate_tokens() {
    let dir = path("expiry");
    let config = Config {
        snapshot_ttl_ms: 160,
        snapshot_max: 1,
        ..Config::default()
    };
    let s = Store::open(&dir, config.clone()).unwrap();
    put(&s, "ledger", "key", "seed", b"durable".to_vec(), 2, false);
    let token = open(&s, "ledger").unwrap();
    std::thread::sleep(Duration::from_millis(80));
    assert_eq!(all(&s, "ledger", &token).rows.len(), 1);
    std::thread::sleep(Duration::from_millis(100));
    assert_eq!(
        page(
            &s,
            SCAN_PREFIX,
            "ledger",
            &token,
            &[],
            "",
            "",
            1,
            1024,
            1024
        ),
        Err(20)
    );
    let live = open(&s, "ledger").unwrap();
    let epoch = s.epoch;
    drop(s);
    let s = Store::open(&dir, config).unwrap();
    assert_eq!(s.epoch, epoch + 1);
    assert_eq!(
        page(&s, SCAN_PREFIX, "ledger", &live, &[], "", "", 1, 1024, 1024),
        Err(20)
    );
    let token = open(&s, "ledger").unwrap();
    assert_eq!(all(&s, "ledger", &token).rows[0].3, b"durable");
    release(&s, "ledger", &token).unwrap();
    assert!(open(&s, "ledger").is_ok());
}

#[test]
fn whole_namespace_budget_and_concurrent_creation_reservations() {
    let s = Store::open(
        &path("budget"),
        Config {
            snapshot_bytes: 512,
            ..Config::default()
        },
    )
    .unwrap();
    put(&s, "ledger", "large", "large", vec![7; 1000], 1, false);
    assert_eq!(
        page(
            &s,
            SCAN_PREFIX,
            "ledger",
            &[],
            &[],
            "missing",
            "",
            1,
            1024,
            1024
        ),
        Err(22)
    );
    put(&s, "ledger", "large", "delete", vec![], 1, true);
    let token = open(&s, "ledger").unwrap();
    assert!(all(&s, "ledger", &token).rows.is_empty());
    assert_eq!(open(&s, "other"), Err(22));
    release(&s, "ledger", &token).unwrap();
    assert!(open(&s, "other").is_ok());
    for (label, config) in [
        (
            "racing-slots",
            Config {
                snapshot_max: 1,
                ..Config::default()
            },
        ),
        (
            "racing-bytes",
            Config {
                snapshot_bytes: 256 * (128 + 8 + 2048) + 262,
                ..Config::default()
            },
        ),
    ] {
        let s = Arc::new(Store::open(&path(label), config).unwrap());
        for i in 0..256 {
            put(
                &s,
                "ledger",
                &format!("key/{i:04}"),
                &format!("seed/{i}"),
                vec![3; 2048],
                1,
                false,
            );
        }
        let barrier = Arc::new(Barrier::new(16));
        let results = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..16)
                .map(|_| {
                    let s = &s;
                    let barrier = &barrier;
                    scope.spawn(move || {
                        barrier.wait();
                        open(s, "ledger")
                    })
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 1);
        assert_eq!(results.iter().filter(|r| **r == Err(22)).count(), 15);
        release(
            &s,
            "ledger",
            results.iter().find_map(|r| r.as_ref().ok()).unwrap(),
        )
        .unwrap();
        assert!(open(&s, "ledger").is_ok());
    }
    let disabled = Store::open(
        &path("disabled"),
        Config {
            snapshot_max: 0,
            ..Config::default()
        },
    )
    .unwrap();
    assert_eq!(open(&disabled, "ledger"), Err(22));
}

#[test]
fn slow_scan_concurrent_writers_preserve_view_and_make_progress() {
    let s = Arc::new(Store::open(&path("writers"), Config::default()).unwrap());
    for i in 0..64 {
        put(
            &s,
            "ledger",
            &format!("row/{i:04}"),
            &format!("seed/{i}"),
            vec![i as u8; 128],
            1,
            false,
        );
    }
    let token = open(&s, "ledger").unwrap();
    let expected = all(&s, "ledger", &token).rows;
    let (send, done) = std::sync::mpsc::channel();
    let writer = {
        let s = s.clone();
        std::thread::spawn(move || {
            for i in 0..64 {
                put(
                    &s,
                    "ledger",
                    &format!("row/{i:04}"),
                    &format!("writer/{i}"),
                    vec![255; 128],
                    1,
                    false,
                );
            }
            send.send(()).unwrap();
        })
    };
    let mut cursor = Vec::new();
    let mut actual = Vec::new();
    for i in 0..64 {
        let p = page(
            &s,
            SCAN_PREFIX,
            "ledger",
            &token,
            &cursor,
            "row/",
            "",
            1,
            1024,
            1024,
        )
        .unwrap();
        actual.extend(p.rows);
        cursor = p.cursor;
        if i == 0 {
            assert!(!cursor.is_empty());
            // Require writer completion before consuming the remaining pages.
            done.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    writer.join().unwrap();
    assert_eq!(actual, expected);
    assert!(cursor.is_empty());
    assert_eq!(s.get("ledger", "row/0000").unwrap().bytes, vec![255; 128]);
    println!("slow scan: 64 old-view pages; 64 concurrent os writes completed");
}

#[test]
fn hello_preserves_legacy_codes_and_opt_in_adds_scan_codes() {
    let s = Store::open(&path("hello"), Config::default()).unwrap();
    for (client, expected) in [("python-stdlib", 19), ("python-stdlib;scans=1", 22)] {
        let mut p = 1024u32.to_le_bytes().to_vec();
        p.push(0);
        lp(&mut p, client.as_bytes());
        let r = s.dispatch(&frame(HELLO, 1, &p)[4..]);
        assert_eq!(&r[17..19], &[0, 0]);
        let mut c = Cursor::new(&r[19..]);
        c.u64().unwrap();
        c.u32().unwrap();
        c.u32().unwrap();
        c.u16().unwrap();
        c.u8().unwrap();
        c.u64().unwrap();
        assert_eq!(c.u16().unwrap(), expected);
        for (code, name, transient) in CODES.iter().take(expected as usize) {
            assert_eq!(c.u16().unwrap(), *code);
            assert_eq!(c.u8().unwrap(), *transient as u8);
            assert_eq!(c.text().unwrap(), *name);
        }
        c.end().unwrap();
        assert!(r.len() - 4 <= 1024);
    }
}
