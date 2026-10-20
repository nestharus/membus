//! Large configured chunks and retained catalogue representations survive retirement/restart.
use bus::{
    chunks::{self, CatRecord, Catalogue, ChunkStore, Rec},
    *,
};
use membus as bus;
use std::fs;
#[path = "support/retirement_fixture.rs"]
mod fixture;

#[test]
fn allowed_large_chunk_retires_without_exposing_older_values() {
    let f = fixture::Fixture::new("retiring-large", 8100);
    let payload = chunks::encode(&f.transition(30_000)).len() - 16;
    assert_eq!(payload, 16_929_033);
    assert!(payload > MAX_FRAME);
    let s = f.open();
    assert!(!s.expiry_degraded());
    assert!(!s.storage_error());
    f.admit_policy(&s);
    f.assert_live(&s);
    let mut states = Vec::new();
    for _ in 0..2 {
        s.retirement_pass();
        let state = s.chunk_rows().into_iter().find(|row| row.0 == 2).unwrap().3;
        println!(
            "oversized transition: payload={payload}, state={state}, storage_error={}",
            s.storage_error()
        );
        if state == "Closed" {
            assert!(
                s.storage_error(),
                "the baseline must exhibit the reviewer's refusal"
            );
        }
        if state == "Retired" {
            assert!(!s.storage_error());
            f.assert_expired(&s);
        } else {
            f.assert_live(&s);
        }
        states.push(state);
    }
    assert_eq!(
        states,
        ["Retired", "Retired"],
        "a permitted chunk must have a representable retirement on repeated passes"
    );
    assert!(!chunks::chunk_path(&f.path.join("chunks"), Class::expire(1000), 0, 2).exists());
    drop(s);
    let s = f.open();
    f.assert_expired(&s);
    let records = chunks::read_catalogue(&f.path.join("chunks/catalogue.log"))
        .unwrap()
        .0;
    assert!(records
        .iter()
        .all(|row| chunks::encode(row).len() - 16 <= MAX_FRAME));
}

#[test]
fn old_single_record_retiring_still_replays() {
    let f = fixture::Fixture::new("retiring-old", 2);
    let s = f.open();
    f.admit_policy(&s);
    drop(s);
    let path = f.path.join("chunks/catalogue.log");
    Catalogue::open(&path)
        .unwrap()
        .append(&f.transition(30_000))
        .unwrap();
    let s = f.open();
    f.assert_expired(&s);
    assert!(s.replay_report().retirements_finished.contains(&2));
    assert!(!chunks::chunk_path(&f.path.join("chunks"), Class::expire(1000), 0, 2).exists());
}

#[test]
fn split_horizons_suppress_real_forever_records_in_both_parts() {
    let p = fixture::path("horizons-older-data");
    let mut chunks = ChunkStore::new(p.join("chunks"));
    for n in 0..17_000 {
        chunks.horizons.insert(
            ("aggregate".into(), format!("{n:08}{}", "k".repeat(1016))),
            50_000,
        );
    }
    let parts = chunks.live_list(now_ms(), 100_000);
    let hs: Vec<_> = parts
        .iter()
        .filter_map(|row| match &row.rec {
            Rec::Horizons(h) => Some(h),
            _ => None,
        })
        .collect();
    assert!(hs.len() > 1);
    let first = hs[0][0].1.clone();
    let last = hs.last().unwrap()[0].1.clone();
    assert_ne!(first, last);
    let config = Config {
        tick_ms: 3_600_000,
        retire_ms: 3_600_000,
        ..Config::default()
    };
    {
        let s = Store::open(&p, config.clone()).unwrap();
        for (ns, key, id, value) in [
            (
                "aggregate",
                first.as_str(),
                "first",
                b"old-first".as_slice(),
            ),
            ("aggregate", last.as_str(), "last", b"old-last".as_slice()),
            (
                "compat",
                "legacy-single",
                "single",
                b"old-single".as_slice(),
            ),
            ("compat", "control", "control", b"must-survive".as_slice()),
        ] {
            assert_eq!(
                s.mutate(Mutation {
                    ns: ns.into(),
                    key: key.into(),
                    op_id: id.into(),
                    deadline: now_ms() + 30_000,
                    ack: 1,
                    expected: None,
                    bytes: value.into(),
                    remove: false
                })
                .code,
                0
            );
        }
    }
    drop(Store::open(&p, config.clone()).unwrap());
    let catpath = p.join("chunks/catalogue.log");
    let (mut records, torn) = chunks::read_catalogue(&catpath).unwrap();
    assert!(torn.is_none());
    assert!(records
        .iter()
        .any(|row| matches!(row.rec, Rec::Close { .. })));
    records.push(CatRecord {
        at_ms: now_ms(),
        rec: Rec::Horizons(vec![("compat".into(), "legacy-single".into(), 50_000)]),
    });
    records.extend(parts);
    records.push(CatRecord {
        at_ms: now_ms(),
        rec: Rec::Horizons(vec![("aggregate".into(), first.clone(), 0)]),
    });
    Catalogue::open(&catpath)
        .unwrap()
        .rewrite(&records)
        .unwrap();
    let s = Store::open(&p, config).unwrap();
    assert_eq!(s.replay_report().horizons, 17_001);
    assert!(s.get("aggregate", &first).is_none());
    assert!(s.get("aggregate", &last).is_none());
    assert!(s.get("compat", "legacy-single").is_none());
    assert_eq!(s.get("compat", "control").unwrap().bytes, b"must-survive");
    assert!(fs::metadata(&catpath).unwrap().len() > MAX_FRAME as u64);
    println!("17001 horizons replayed; both split parts and old single form suppress retained data; lower duplicate and unrelated control checked");
}
