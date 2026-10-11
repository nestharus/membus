//! T15 (docs/CHUNK-RETIREMENT.md section 7): a production-shaped replay. On a copy of production state it imports
//! with the first policies, then re-puts the copy's last 24 h of records in their own time order and spacing, on a
//! hand-moved clock, for 73 simulated hours with the idle tick and the retirement pass each simulated minute, and
//! compares the retained bytes with section 5.7's estimate (about 6.1 MB). Ignored by default; run with
//! MEMBUS_T15_SOURCE (a state folder, only read) and MEMBUS_T15_POLICIES (an import-policies.txt) set, through lineup.
use membus::*;
use std::{fs, path::PathBuf};
const MIN: u64 = 60_000;
const HOUR: u64 = 60 * MIN;
const DAY: u64 = 24 * HOUR;

struct Rec {
    at: u64,
    remove: bool,
    ns: String,
    key: String,
    bytes: Vec<u8>,
}
fn read_records(dir: &std::path::Path) -> Vec<Rec> {
    let mut out = Vec::new();
    for i in 0..4 {
        let Ok(data) = fs::read(dir.join(format!("stripe-{i}.log"))) else { continue };
        let mut at = 0usize;
        while at + 16 <= data.len() {
            let size = u32::from_le_bytes(data[at + 4..at + 8].try_into().unwrap()) as usize;
            if at + 16 + size > data.len() {
                break;
            }
            let mut c = Cursor::new(&data[at + 16..at + 16 + size]);
            let stamp = c.u64().unwrap();
            let _version = c.u64().unwrap();
            let _ack = c.u8().unwrap();
            let remove = c.u8().unwrap() != 0;
            let ns = c.text().unwrap();
            let key = c.text().unwrap();
            let _op = c.text().unwrap();
            let _expected = c.u64().unwrap();
            let bytes = c.bytes(MAX_VALUE).unwrap().to_vec();
            out.push(Rec { at: stamp, remove, ns, key, bytes });
            at += 16 + size;
        }
    }
    out.sort_by_key(|r| r.at);
    out
}

#[test]
#[ignore]
fn t15_production_shaped_replay() {
    let source = PathBuf::from(std::env::var("MEMBUS_T15_SOURCE").expect("MEMBUS_T15_SOURCE"));
    let policies = PathBuf::from(std::env::var("MEMBUS_T15_POLICIES").expect("MEMBUS_T15_POLICIES"));
    let out = PathBuf::from(std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()))
        .join(format!("t15-{}-{}", std::process::id(), now_ms()));
    fs::create_dir_all(&out).unwrap();
    for name in ["epoch", "stripe-0.log", "stripe-1.log", "stripe-2.log", "stripe-3.log"] {
        if source.join(name).exists() {
            fs::copy(source.join(name), out.join(name)).unwrap();
        }
    }
    fs::copy(&policies, out.join("import-policies.txt")).unwrap();
    let records = read_records(&out);
    let last = records.last().unwrap().at;
    let window: Vec<&Rec> = records.iter().filter(|r| r.at + DAY > last).collect();
    let window_bytes: usize = window.iter().map(|r| 16 + 60 + r.ns.len() + r.key.len() + r.bytes.len()).sum();
    let clock = ManualClock::new(now_ms());
    let start = clock.utc_ms();
    let s = Store::open(
        &out,
        Config { clock: Some(clock.clone()), tick_ms: 3_600_000, retire_ms: 3_600_000, ..Config::default() },
    )
    .unwrap();
    let imported = s.health2();
    let first = window[0].at;
    let mut minute = start + MIN;
    let mut puts = 0u64;
    let mut refused = 0u64;
    let end = start + 73 * HOUR;
    'cycles: for cycle in 0.. {
        for r in &window {
            let target = start + cycle * DAY + (r.at - first);
            if target >= end {
                break 'cycles;
            }
            // The idle tick and the retirement pass at every simulated minute up to the record's time.
            while minute <= target {
                clock.advance(minute - clock.utc_ms());
                s.tick();
                s.retirement_pass();
                minute += MIN;
            }
            if target > clock.utc_ms() {
                clock.advance(target - clock.utc_ms());
            }
            let m = Mutation {
                ns: r.ns.clone(),
                key: r.key.clone(),
                op_id: format!("t15-{puts}"),
                deadline: now_ms() + 10000,
                ack: 1,
                expected: None,
                bytes: r.bytes.clone(),
                remove: r.remove,
            };
            puts += 1;
            if s.mutate(m).code != 0 {
                refused += 1;
            }
        }
    }
    while minute <= end {
        clock.advance(minute - clock.utc_ms());
        s.tick();
        s.retirement_pass();
        minute += MIN;
    }
    let h = s.health2();
    let estimate = 6_100_000f64;
    let ratio = h.retained_total as f64 / estimate;
    let report = format!(
        "T15: imported {} B ({:?}); window {} records (~{} B) from the last 24 h of the copy; {} puts ({} refused) over 73 simulated hours; retained {} B = {:.3} of the 6.1 MB estimate; by class {:?}; chunks open {} closed {}; retired {} chunks {} B; keys {}; pinned {} B; io {:?}",
        imported.retained_total,
        imported.retained_bytes,
        window.len(),
        window_bytes,
        puts,
        refused,
        h.retained_total,
        ratio,
        h.retained_bytes,
        h.chunks_open,
        h.chunks_closed,
        h.retired_chunks,
        h.retired_bytes,
        h.keys,
        h.pinned_bytes,
        s.io_metrics()
    );
    println!("{report}");
    fs::write(out.join("t15-report.txt"), &report).unwrap();
    assert_eq!(refused, 0);
    assert!((0.9..=1.1).contains(&ratio), "{report}");
}
