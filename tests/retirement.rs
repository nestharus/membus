//! CR2 (docs/CHUNK-RETIREMENT.md section 10): policies and the gated policy namespace, expire and latest
//! retirement, key slots, anti-resurrection and the clock. Tests T2-T5, T8, T10 and T13 of section 7.
use membus::*;
use std::{
    fs,
    path::{Path, PathBuf},
    sync::Arc,
};
const MIN: u64 = 60_000;
const HOUR: u64 = 60 * MIN;
const DAY: u64 = 24 * HOUR;
fn path(label: &str) -> PathBuf {
    let p = std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into());
    PathBuf::from(p).join(format!("{label}-{}-{}", std::process::id(), now_ms()))
}
fn put(s: &Store, key: &str, id: &str, value: &[u8]) -> Mutation {
    Mutation {
        ns: "ledger".into(),
        key: key.into(),
        op_id: id.into(),
        deadline: deadline(s),
        ack: 1,
        expected: None,
        bytes: value.to_vec(),
        remove: false,
    }
}
fn remove(s: &Store, key: &str, id: &str) -> Mutation {
    Mutation { remove: true, bytes: Vec::new(), ..put(s, key, id, b"") }
}
/// A store on a hand-moved clock: no background tick or pass, rotation at 1 min.
fn config(clock: &Arc<ManualClock>) -> Config {
    Config {
        clock: Some(clock.clone()),
        tick_ms: 3_600_000,
        retire_ms: 3_600_000,
        rotation_ms: MIN,
        ..Config::default()
    }
}
fn reopen(p: &Path, c: Config) -> Store {
    #[cfg(not(windows))]
    let _ = fs::remove_file(p.join("owner.lock"));
    Store::open(p, c).unwrap()
}
#[test]
fn horizon_rewrite_splits_large_metadata_and_replays_both_forms() {
    use membus::chunks::{CatRecord, Catalogue, ChunkStore, Rec};
    let p = path("horizon-rewrite");
    let dir = p.join("chunks");
    fs::create_dir_all(&dir).unwrap();
    let mut chunks = ChunkStore::new(dir.clone());
    for n in 0..17_000 {
        chunks.horizons.insert(("aggregate".into(), format!("{n:08}{}", "k".repeat(1016))), n + 1);
    }
    // UTF-8 byte sizes, including a key at the identifier limit.
    chunks.horizons.insert(("aggregate".into(), "é".repeat(MAX_ID / 2)), 20_000);
    let aggregate = CatRecord { at_ms: 10, rec: Rec::Horizons(chunks.horizons.iter()
        .map(|((ns, key), v)| (ns.clone(), key.clone(), *v)).collect()) };
    assert!(chunks::encode(&aggregate).len() - 16 > MAX_FRAME);
    let live = chunks.live_list(20, 50_000);
    let mut catalogue = Catalogue::open(&dir.join("catalogue.log")).unwrap();
    catalogue.rewrite(&live).expect("accepted aggregate horizons must be rewritable");
    drop(catalogue);
    let (records, torn) = chunks::read_catalogue(&dir.join("catalogue.log")).unwrap();
    assert!(torn.is_none());
    let parts: Vec<_> = records.iter().filter_map(|r| match &r.rec {
        Rec::Horizons(h) => Some(h), _ => None,
    }).collect();
    assert!(parts.len() > 1);
    assert!(records.iter().all(|r| chunks::encode(r).len() - 16 <= MAX_FRAME));
    let saved: Vec<_> = parts.iter().flat_map(|h| h.iter().cloned()).collect();
    assert_eq!(chunks::horizon_set(&saved), chunks.horizons);
    let clock = ManualClock::new(now_ms());
    let s = reopen(&p, config(&clock));
    assert_eq!(s.replay_report().horizons, chunks.horizons.len());
    assert!(s.mutate(put(&s, "after", "after", b"live")).version >= 50_000);
    drop(s);
    // The original on-disk single-record representation remains readable.
    let old = path("horizon-rewrite-single");
    fs::create_dir_all(old.join("chunks")).unwrap();
    let mut cat = Catalogue::open(&old.join("chunks/catalogue.log")).unwrap();
    cat.append(&CatRecord { at_ms: 10, rec: Rec::Horizons(vec![("old".into(), "key".into(), 7)]) }).unwrap();
    drop(cat);
    let s = reopen(&old, config(&clock));
    assert_eq!(s.replay_report().horizons, 1);
    println!("{} horizons over {} bytes rewritten into {} bounded records and replayed; single-record replay passed",
        chunks.horizons.len(), chunks::encode(&aggregate).len() - 16, parts.len());
}
fn row(s: &Store, id: u64) -> (u64, usize, String, String, u64) {
    s.chunk_rows().into_iter().find(|r| r.0 == id).unwrap()
}
/// The chunk ids of one class, in order.
fn class_chunks(s: &Store, label: &str) -> Vec<u64> {
    s.chunk_rows().into_iter().filter(|r| r.2 == label).map(|r| r.0).collect()
}
/// Move both clocks past the rotation age and run the tick, so open finite chunks close.
fn close_finite(s: &Store, clock: &ManualClock) -> u64 {
    clock.advance(MIN);
    let at = clock.utc_ms();
    s.tick();
    at
}
fn retained(s: &Store) -> u64 {
    s.chunk_rows().iter().filter(|r| r.3 != "Retired").map(|r| r.4).sum()
}
fn deadline(s: &Store) -> u64 {
    // This suite advances UTC by days between admissions.
    s.clock().utc_ms() + 10000
}

#[test]
fn t2_expire_class_on_a_mocked_clock() {
    let p = path("t2");
    let clock = ManualClock::new(now_ms());
    let s = Store::open(&p, config(&clock)).unwrap();
    // A retention shorter than the 24 h retry window is refused (dedup must outlive a restart).
    assert_eq!(s.set_policy("ledger", "e/", Some(Class::expire(HOUR)), deadline(&s)).code, 3);
    assert_eq!(s.set_policy("ledger", "e/", Some(Class::expire(2 * DAY)), deadline(&s)).code, 0);
    let base = s.slots("ledger");
    assert_eq!(s.mutate(put(&s, "e/1", "e1", b"x")).code, 0);
    let chunk = class_chunks(&s, "expire(172800000ms)")[0];
    let closed_at = close_finite(&s, &clock);
    assert_eq!(row(&s, chunk).3, "Closed");
    let file = fs::read_dir(p.join("chunks").join("expire-172800000ms")).unwrap().next().unwrap().unwrap().path();
    // Never before close + retention.
    clock.advance(closed_at + 2 * DAY - 1 - clock.utc_ms());
    s.retirement_pass();
    assert!(file.exists());
    assert_eq!(s.get("ledger", "e/1").unwrap().bytes, b"x");
    // A record in an open chunk waits, however old its chunk's class says it may be.
    assert_eq!(s.mutate(put(&s, "e/2", "e2", b"y")).code, 0);
    let open = *class_chunks(&s, "expire(172800000ms)").last().unwrap();
    assert_eq!(row(&s, open).3, "Open");
    // The first pass at close + retention retires it.
    clock.advance(1);
    s.retirement_pass();
    assert_eq!(row(&s, chunk).3, "Retired");
    assert!(!file.exists());
    assert!(s.get("ledger", "e/1").is_none());
    assert_eq!(s.get_status("ledger", "e/1"), (1, 0));
    assert_eq!(s.get("ledger", "e/2").unwrap().bytes, b"y");
    assert_eq!(row(&s, open).3, "Open");
    // The expired key's live slot is free; its summary holds one index entry.
    let after = s.slots("ledger");
    assert_eq!(after.1, base.1 + 1, "only e/2 is live");
    assert_eq!(after.0, base.0 + 2, "e/2 and e/1's summary");
    // A conditional put expecting 0 succeeds on the expired key.
    let mut m = put(&s, "e/1", "e1-again", b"z");
    m.expected = Some(0);
    assert_eq!(s.mutate(m).code, 0);
    assert_eq!(s.get_status("ledger", "e/1").0, 0);
    // After the summary's 30 days, an expired key reads missing.
    assert_eq!(s.mutate(put(&s, "e/3", "e3", b"x")).code, 0);
    close_finite(&s, &clock);
    clock.advance(2 * DAY + MIN);
    s.retirement_pass();
    assert_eq!(s.get_status("ledger", "e/3"), (1, 0));
    clock.advance(30 * DAY);
    s.retirement_pass();
    assert_eq!(s.get_status("ledger", "e/3"), (2, 0));
    println!("T2: expire(2 d) chunk kept at close+2d-1ms, retired at close+2d; expired then missing after 30 days");
}

#[test]
fn t3_latest_class() {
    let p = path("t3");
    let clock = ManualClock::new(now_ms());
    let s = Store::open(&p, config(&clock)).unwrap();
    assert_eq!(s.set_policy("ledger", "l/", Some(Class::LATEST), deadline(&s)).code, 0);
    // a and b on different stripes, so each has its own chunk.
    let stripe = |k: &str| {
        let mut b = b"ledger\0".to_vec();
        b.extend(k.as_bytes());
        crc32(&b) % 4
    };
    let b_key = (0..100).map(|i| format!("l/b{i}")).find(|k| stripe(k) != stripe("l/a")).unwrap();
    assert_eq!(s.mutate(put(&s, "l/a", "a1", b"1")).code, 0);
    assert_eq!(s.mutate(put(&s, &b_key, "b1", b"1")).code, 0);
    let first = class_chunks(&s, "latest");
    assert_eq!(first.len(), 2);
    let closed_at = close_finite(&s, &clock);
    assert_eq!(s.mutate(put(&s, "l/a", "a2", b"2")).code, 0);
    let a_chunk = first.iter().copied().find(|c| row(&s, *c).1 as u32 == stripe("l/a")).unwrap();
    let b_chunk = first.iter().copied().find(|c| *c != a_chunk).unwrap();
    // The superseded chunk retires only after close + 24 h.
    clock.advance(closed_at + DAY - 1 - clock.utc_ms());
    s.retirement_pass();
    assert_eq!(row(&s, a_chunk).3, "Closed");
    clock.advance(1);
    s.retirement_pass();
    assert_eq!(row(&s, a_chunk).3, "Retired");
    assert_eq!(s.get("ledger", "l/a").unwrap().bytes, b"2");
    // A chunk holding one latest version stays until that key is rewritten.
    assert_eq!(row(&s, b_chunk).3, "Closed");
    clock.advance(10 * DAY);
    s.retirement_pass();
    assert_eq!(row(&s, b_chunk).3, "Closed");
    assert_eq!(s.mutate(put(&s, &b_key, "b2", b"2")).code, 0);
    s.retirement_pass();
    assert_eq!(row(&s, b_chunk).3, "Retired");
    assert_eq!(s.get("ledger", &b_key).unwrap().bytes, b"2");
    drop(s);
    let s = reopen(&p, config(&clock));
    assert_eq!(s.get("ledger", "l/a").unwrap().bytes, b"2");
    assert_eq!(s.get("ledger", &b_key).unwrap().bytes, b"2");
    println!("T3: superseded latest chunk kept at close+24h-1ms and retired at close+24h; one holding a latest version kept until rewritten");
}

#[test]
fn t4_removes_tombstones_versions_and_anti_resurrection() {
    let p = path("t4");
    let clock = ManualClock::new(now_ms());
    let s = Store::open(&p, Config { quota: 2, ..config(&clock) }).unwrap();
    // A remove frees its quota slot at commit; a put to the removed key takes it back.
    assert_eq!(s.mutate(put(&s, "x", "x1", b"x")).code, 0);
    assert_eq!(s.mutate(put(&s, "y", "y1", b"y")).code, 0);
    assert_eq!(s.mutate(put(&s, "z", "z1", b"z")).code, 8);
    let (keys, live) = s.slots("ledger");
    assert_eq!(live, 2);
    assert_eq!(s.mutate(remove(&s, "x", "x-rm")).code, 0);
    assert_eq!(s.slots("ledger"), (keys, 1), "the tombstone keeps its index entry");
    assert_eq!(s.mutate(put(&s, "z", "z2", b"z")).code, 0);
    assert_eq!(s.mutate(put(&s, "x", "x2", b"x")).code, 8, "the namespace is full");
    assert_eq!(s.mutate(remove(&s, "y", "y-rm")).code, 0);
    assert_eq!(s.mutate(put(&s, "x", "x3", b"x")).code, 0);
    assert_eq!(s.slots("ledger").1, 2);
    // A missing-key remove charges index_cap alone; with no older record retained its entry drops at the pass.
    let before = s.slots("ledger");
    assert_eq!(s.mutate(remove(&s, "never", "never-rm")).code, 0);
    assert_eq!(s.slots("ledger"), (before.0 + 1, before.1));
    // y's tombstone drops too: its older record shares the tombstone's chunk, so the two retire together and no
    // other retained chunk can bring y back.
    s.retirement_pass();
    assert_eq!(s.slots("ledger"), (before.0 - 1, before.1));
    assert!(s.get("ledger", "y").is_none());
    drop(s);
    let s = reopen(&p, config(&clock));
    assert!(s.get("ledger", "y").is_none());
    assert_eq!(s.slots("ledger"), (before.0 - 1, before.1), "replay drops the same tombstones at start");
    // A tombstone whose older record is in another retained chunk keeps its entry: w's put closes in a latest
    // chunk, its remove lands in the next one, and the entry drops only when the older chunk retires.
    assert_eq!(s.set_policy("ledger", "w/", Some(Class::LATEST), deadline(&s)).code, 0);
    assert_eq!(s.mutate(put(&s, "w/1", "w1", b"w")).code, 0);
    close_finite(&s, &clock);
    assert_eq!(s.mutate(remove(&s, "w/1", "w1-rm")).code, 0);
    let held = s.slots("ledger");
    s.retirement_pass();
    assert_eq!(s.slots("ledger"), held, "the older record is still retained");
    clock.advance(DAY);
    s.retirement_pass();
    assert_eq!(s.slots("ledger"), (held.0 - 1, held.1));
    drop(s);

    // Anti-resurrection after a policy change that retires the newer chunk first.
    let s = reopen(&p, config(&clock));
    let old = s.mutate(put(&s, "p/k", "pk1", b"old"));
    assert_eq!(s.set_policy("ledger", "p/", Some(Class::expire(2 * DAY)), deadline(&s)).code, 0);
    let new = s.mutate(put(&s, "p/k", "pk2", b"new"));
    assert!(new.version > old.version);
    close_finite(&s, &clock);
    clock.advance(2 * DAY);
    s.retirement_pass();
    assert!(s.get("ledger", "p/k").is_none(), "the older value must not return");
    assert_eq!(s.get_status("ledger", "p/k"), (1, 0));
    // The highest-versioned record has now retired: a restart's next stays above every version issued.
    let issued = s.mutate(put(&s, "q", "q1", b"q")).version;
    assert_eq!(s.set_policy("ledger", "r/", Some(Class::expire(DAY)), deadline(&s)).code, 0);
    let top = s.mutate(put(&s, "r/top", "top", b"t")).version;
    assert!(top > issued);
    close_finite(&s, &clock);
    clock.advance(DAY);
    s.retirement_pass();
    assert!(s.get("ledger", "r/top").is_none());
    drop(s);
    let s = reopen(&p, config(&clock));
    assert!(s.get("ledger", "p/k").is_none(), "a restart returned the older value");
    let report = s.replay_report();
    assert!(report.horizons >= 1, "{}", report.json());
    assert!(report.stripes.iter().map(|r| r.horizons_applied).sum::<u64>() >= 1);
    assert!(s.mutate(put(&s, "after", "after", b"a")).version > top);
    println!("T4: slots split; missing-key tombstone dropped; horizon hid p/k's older value across restart; next above {top}");
}

#[test]
fn t5_thousand_key_run_slots_and_retained_bytes() {
    let p = path("t5");
    let clock = ManualClock::new(now_ms());
    let s = Store::open(&p, Config { chunk_bytes: 64 * 1024, ..config(&clock) }).unwrap();
    assert_eq!(s.set_policy("ledger", "s/", Some(Class::LATEST), deadline(&s)).code, 0);
    let (base_keys, _) = s.slots("ledger");
    let value = vec![b'v'; 150];
    for i in 0..1000 {
        assert_eq!(s.mutate(put(&s, &format!("s/{i}"), &format!("put{i}"), &value)).code, 0);
    }
    for i in 0..1000 {
        assert_eq!(s.mutate(put(&s, &format!("s/{i}"), &format!("over{i}"), &value)).code, 0);
    }
    for i in 0..1000 {
        assert_eq!(s.mutate(remove(&s, &format!("s/{i}"), &format!("rm{i}"))).code, 0);
    }
    // Slots return to 0 after the removes; the tombstones hold index entries until no older record is retained.
    assert_eq!(s.slots("ledger"), (base_keys + 1000, 0));
    let before = s.state_bytes();
    close_finite(&s, &clock);
    clock.advance(DAY);
    s.retirement_pass();
    let after = s.state_bytes();
    assert_eq!(after, retained(&s), "state bytes are the retained chunks' bytes");
    assert!(after < 680_000, "retained {after} B (before retirement {before} B)");
    assert!(after < before);
    // Tombstones whose older records went with the retired chunks leave the index; those whose older record
    // shares a retained chunk with other keys' tombstones wait for it.
    let (keys, live) = s.slots("ledger");
    assert_eq!(live, 0);
    assert!(keys < base_keys + 1000, "{keys}");
    println!("T5: 3,000 records; live slots 0 after removes; retained {before} B -> {after} B after retirement (measured production baseline: 680,000 B); tombstone entries left {}", keys - base_keys);
}

#[test]
fn t8_dedup_across_retirement_and_restart() {
    let p = path("t8");
    let clock = ManualClock::new(now_ms());
    let s = Store::open(&p, config(&clock)).unwrap();
    assert_eq!(s.set_policy("ledger", "d/", Some(Class::LATEST), deadline(&s)).code, 0);
    let first = put(&s, "d/k", "d-first", b"one");
    let original = s.mutate(first.clone());
    close_finite(&s, &clock);
    assert_eq!(s.mutate(put(&s, "d/k", "d-second", b"two")).code, 0);
    // Within the 24 h window: a restart rebuilds the dedup entry from the retained record.
    clock.advance(23 * HOUR);
    s.retirement_pass();
    drop(s);
    let s = reopen(&p, config(&clock));
    assert_eq!(s.mutate(first.clone()), original);
    assert_eq!(s.metrics().0, 0, "a deduplicated retry writes nothing");
    // Across retirement, the original retry window still bounds the in-memory entry.
    let latest = class_chunks(&s, "latest");
    clock.advance(2 * HOUR);
    s.retirement_pass();
    assert!(latest.iter().any(|c| row(&s, *c).3 == "Retired"));
    assert_eq!(s.mutate(first).code, 13);
    assert_eq!(s.metrics().0, 0);
    assert_eq!(s.get("ledger", "d/k").unwrap().bytes, b"two");
    println!("T8: retry after restart returned version {} and wrote nothing; expired after elapsed time passed its original window", original.version);
}

#[test]
fn t10_clock_jumps() {
    // A forward UTC jump of a day mid-run retires nothing early: monotonic time must agree.
    let p = path("t10-forward");
    let clock = ManualClock::new(now_ms());
    let s = Store::open(&p, config(&clock)).unwrap();
    assert_eq!(s.set_policy("ledger", "l/", Some(Class::LATEST), deadline(&s)).code, 0);
    assert_eq!(s.mutate(put(&s, "l/a", "a1", b"1")).code, 0);
    let chunk = class_chunks(&s, "latest")[0];
    close_finite(&s, &clock);
    assert_eq!(s.mutate(put(&s, "l/a", "a2", b"2")).code, 0);
    clock.jump_utc(DAY as i64 + MIN as i64);
    s.tick();
    s.retirement_pass();
    assert_eq!(row(&s, chunk).3, "Closed");
    assert!(!s.expiry_degraded());
    clock.advance(DAY);
    s.retirement_pass();
    assert_eq!(row(&s, chunk).3, "Retired");
    drop(s);

    // A backward jump across a restart sets expiry_degraded and pauses retirement until `expiry resume`.
    let p = path("t10-backward");
    let clock = ManualClock::new(now_ms());
    let chunk;
    {
        let s = Store::open(&p, config(&clock)).unwrap();
        assert_eq!(s.set_policy("ledger", "l/", Some(Class::LATEST), deadline(&s)).code, 0);
        assert_eq!(s.mutate(put(&s, "l/a", "a1", b"1")).code, 0);
        chunk = class_chunks(&s, "latest")[0];
        close_finite(&s, &clock);
        assert_eq!(s.mutate(put(&s, "l/a", "a2", b"2")).code, 0);
    }
    clock.jump_utc(-(HOUR as i64));
    let s = reopen(&p, config(&clock));
    assert!(s.expiry_degraded());
    assert!(s.replay_report().expiry_degraded.unwrap().contains("earlier than the catalogue"));
    clock.advance(2 * DAY);
    s.retirement_pass();
    assert_eq!(row(&s, chunk).3, "Closed", "retirement must pause while the clock is in doubt");
    fs::write(p.join("expiry.resume"), b"").unwrap();
    s.tick();
    assert!(!s.expiry_degraded());
    assert!(!p.join("expiry.resume").exists());
    s.retirement_pass();
    assert_eq!(row(&s, chunk).3, "Retired");
    drop(s);

    // More than 24 h past the heartbeat's last beat also pauses; a rotation age of stable ticks resumes.
    let p = path("t10-heartbeat");
    let clock = ManualClock::new(now_ms());
    drop(Store::open(&p, config(&clock)).unwrap());
    fs::write(p.join("heartbeat.json"), format!("{{\"unix_ms\":{}}}", clock.utc_ms())).unwrap();
    clock.advance(DAY + HOUR);
    let s = reopen(&p, config(&clock));
    assert!(s.expiry_degraded());
    assert!(s.replay_report().expiry_degraded.unwrap().contains("heartbeat"));
    s.tick();
    for _ in 0..4 {
        clock.advance(MIN / 4);
        s.tick();
    }
    assert!(!s.expiry_degraded());
    println!("T10: forward jump retired nothing early; backward jump paused until expiry.resume; heartbeat gap paused until stable ticks");
}

#[test]
fn t13_cold_restart_near_the_cap() {
    let p = path("t13");
    let clock = ManualClock::new(now_ms());
    // Scaled down: a 64 KiB limit with an 8 KiB reserve stands for 8 GiB and 1 GiB.
    let c = Config {
        state_limit_bytes: 64 * 1024,
        maintenance_reserve_bytes: 8 * 1024,
        chunk_bytes: 2048,
        ..config(&clock)
    };
    {
        let s = Store::open(&p, c.clone()).unwrap();
        assert_eq!(s.set_policy("ledger", "l/", Some(Class::LATEST), deadline(&s)).code, 0);
        let value = vec![b'v'; 400];
        let mut n = 0;
        loop {
            let o = s.mutate(put(&s, "l/hot", &format!("hot{n}"), &value));
            if o.code == 7 {
                break;
            }
            assert_eq!(o.code, 0);
            n += 1;
        }
        // Refused at the ordinary ceiling, within one record of it.
        let bytes = s.state_bytes();
        assert!(bytes <= 56 * 1024 && bytes + 500 > 56 * 1024, "{bytes}");
        close_finite(&s, &clock);
    }
    clock.advance(DAY + MIN);
    // Start succeeds at the ceiling with retirement due.
    let s = reopen(&p, c);
    let value = vec![b'v'; 400];
    assert_eq!(s.mutate(put(&s, "l/new", "new0", &value)).code, 7);
    s.retirement_pass();
    assert!(s.state_bytes() < 4 * 1024, "{}", s.state_bytes());
    assert_eq!(s.mutate(put(&s, "l/new", "new1", &value)).code, 0);
    assert_eq!(s.get("ledger", "l/hot").unwrap().bytes.len(), 400);
    println!("T13: start at the 56 KiB ceiling, retirement wrote its catalogue records and freed the bytes, puts admitted again");
}
