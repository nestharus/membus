use crate::now_ms;
use super::*;
use crate::{CachePolicy, Config};
use std::{
    fs::OpenOptions,
    io::{Seek, SeekFrom, Write},
    path::PathBuf,
};
struct Fixture(PathBuf);
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn fixture(label: &str) -> (Fixture, Store) {
    let base = PathBuf::from(
        std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()),
    );
    let f = Fixture(base.join(format!(
        "read-preflight-{label}-{}-{}",
        std::process::id(),
        now_ms()
    )));
    let s = Store::open(
        &f.0,
        Config {
            capacity: 16 * 1024 * 1024,
            cache_bytes: 12 * 1024,
            signal_bytes: 0,
            tick_ms: 3_600_000,
            retire_ms: 3_600_000,
            cache_rules: vec![],
            cache_policy: CachePolicy {
                initial_ttl: 0,
                active_ttl: 60_000,
                ..CachePolicy::default()
            },
            ..Config::default()
        },
    )
    .unwrap();
    (f, s)
}
fn put(s: &Store, key: &str, byte: u8) -> u64 {
    let result = s.mutate(Mutation {
        ns: "ledger".into(),
        key: key.into(),
        op_id: format!("seed-{key}"),
        deadline: now_ms() + 30_000,
        ack: 1,
        expected: None,
        bytes: vec![byte; 8192],
        remove: false,
    });
    assert_eq!(result.code, 0);
    result.version
}
fn call(s: &Store, op: u8, keys: &[&str], max: usize) -> Vec<u8> {
    let mut p = (now_ms() + 30_000).to_le_bytes().to_vec();
    lp(&mut p, b"preflight-read");
    lp(&mut p, b"ledger");
    if op == GET_MANY {
        p.extend((keys.len() as u16).to_le_bytes());
    }
    for key in keys {
        lp(&mut p, key.as_bytes());
    }
    s.dispatch_for_limit(&frame(op, 37, &p)[4..], false, max)
}
fn code(reply: &[u8]) -> u16 {
    u16::from_le_bytes(reply[17..19].try_into().unwrap())
}
fn refused_without_pollution(op: u8) {
    let (f, s) = fixture(if op == GET { "single" } else { "many" });
    put(&s, "large", 7);
    put(&s, "working", 9);
    assert_eq!(
        s.try_get("ledger", "working").unwrap().unwrap().bytes,
        vec![9; 8192]
    );
    let before = s.cache_stats();
    let reply = call(
        &s,
        op,
        if op == GET {
            &["large"]
        } else {
            &["large", "large"]
        },
        if op == GET { 1024 } else { 16 * 1024 },
    );
    let after = s.cache_stats();
    let working = s.try_get("ledger", "working").unwrap().unwrap();
    let final_stats = s.cache_stats();
    assert_eq!(working.bytes, vec![9; 8192]);
    println!(
        "op={op} refused={} cold={} hits={} misses={} evicted={} working_cold={}",
        code(&reply),
        after.cold_reads - before.cold_reads,
        after.hits - before.hits,
        after.misses - before.misses,
        after.evicted_budget - before.evicted_budget,
        final_stats.cold_reads - after.cold_reads
    );
    drop(s);
    drop(f);
    assert_eq!(code(&reply), 4);
    assert_eq!(
        after.cold_reads, before.cold_reads,
        "oversized read loaded a rejected payload"
    );
    assert_eq!(after.hits, before.hits);
    assert_eq!(after.misses, before.misses);
    assert_eq!(after.evicted_budget, before.evicted_budget);
    assert_eq!(
        final_stats.cold_reads, after.cold_reads,
        "refused read evicted a working value"
    );
}
#[test]
fn read_preflight_get_refusal_preserves_cache() {
    refused_without_pollution(GET);
}
#[test]
fn read_preflight_many_refusal_preserves_cache() {
    refused_without_pollution(GET_MANY);
}
#[test]
fn read_preflight_exact_fit_preserves_order_missing_duplicates_and_wire() {
    let (f, s) = fixture("wire");
    let version = put(&s, "one", 7);
    let keys = ["one", "missing", "one"];
    let mut payload = 3u16.to_le_bytes().to_vec();
    let value = Value {
        version,
        ack: 1,
        bytes: vec![7; 8192],
        tombstone: false,
    };
    encode_value(&mut payload, Some(&value));
    encode_value(&mut payload, None);
    encode_value(&mut payload, Some(&value));
    let expected = reply(GET_MANY, 37, 0, &payload);
    let actual = call(&s, GET_MANY, &keys, payload.len() + 15);
    let empty = call(&s, GET_MANY, &[], 1024);
    drop(s);
    drop(f);
    assert_eq!(actual, expected);
    assert_eq!(empty, reply(GET_MANY, 37, 0, &0u16.to_le_bytes()));
}
#[test]
fn read_preflight_unread_corruption_is_not_validated_but_fitting_read_refuses_io() {
    let (f, s) = fixture("corruption");
    put(&s, "one", 7);
    let entry = s
        .committed
        .read()
        .unwrap()
        .get(&("ledger".into(), "one".into()))
        .unwrap()
        .clone();
    let original = entry.location.payload().unwrap();
    let path = std::fs::read_dir(f.0.join("chunks/forever"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|x| x == "log"))
        .unwrap();
    {
        let _read = entry.location.file.lock().unwrap();
        let mut file = OpenOptions::new().write(true).open(path).unwrap();
        file.seek(SeekFrom::Start(entry.location.offset + 16))
            .unwrap();
        file.write_all(&[original[0] ^ 1]).unwrap();
        file.flush().unwrap();
    }
    let small = call(&s, GET, &["one"], 1024);
    let before = s.cache_stats();
    let flagged_early = s.storage_error();
    let fits = call(&s, GET, &["one"], MAX_FRAME);
    let after = s.cache_stats();
    let flagged = s.storage_error();
    drop(entry);
    drop(s);
    drop(f);
    assert_eq!(
        code(&small),
        4,
        "oversized request must refuse before reading an unselected frame"
    );
    assert_eq!((before.cold_reads, flagged_early), (0, false));
    assert_eq!(code(&fits), 16);
    assert!(flagged);
    assert_eq!((after.cold_reads, after.entries), (1, 0));
}
