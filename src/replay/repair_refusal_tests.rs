//! Real write-sharing denials during repair retain pending evidence and record refusal.
use super::*;
use std::os::windows::fs::OpenOptionsExt;

fn repair_refusal(deny_catalogue: bool) {
    let f = Fixture::new();
    let config = Config {
        clock: Some(chunks::ManualClock::new(1_000_000)),
        flush_ms: 0,
        tick_ms: 3_600_000,
        retire_ms: 3_600_000,
        ..Config::default()
    };
    let mutation = Mutation {
        ns: "caller".into(),
        key: "key".into(),
        op_id: "retained".into(),
        deadline: u64::MAX,
        ack: 2,
        expected: None,
        bytes: b"kept".to_vec(),
        remove: false,
    };
    let store = Store::open(&f.0, config.clone()).unwrap();
    let outcome = store.mutate(mutation.clone());
    assert_eq!(outcome.code, 0);
    drop(store);
    f.unlock();
    let files = chunks::list_chunk_files(&f.0.join("chunks")).unwrap();
    assert_eq!(files.len(), 1);
    let data = &files[0].3;
    let cat = f.0.join("chunks/catalogue.log");
    let original = fs::read(data).unwrap();
    let catalogue = fs::read(&cat).unwrap();
    OpenOptions::new()
        .append(true)
        .open(data)
        .unwrap()
        .write_all(b"MBK")
        .unwrap();
    OpenOptions::new()
        .append(true)
        .open(&cat)
        .unwrap()
        .write_all(b"MBC")
        .unwrap();
    let torn_data = fs::read(data).unwrap();
    let torn_cat = fs::read(&cat).unwrap();
    let target = if deny_catalogue { &cat } else { data };
    let held = OpenOptions::new()
        .read(true)
        .share_mode(1 | 4)
        .open(target)
        .unwrap();
    let expected = OpenOptions::new().write(true).open(target).unwrap_err();
    let error = Store::open(&f.0, config.clone())
        .err()
        .expect("held repair target must refuse");
    assert_eq!(error.raw_os_error(), expected.raw_os_error());
    assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 1u64.to_le_bytes());
    assert!(!f.0.join("epoch.tmp").exists());
    assert_eq!(fs::read(&cat).unwrap(), torn_cat);
    assert_eq!(
        fs::read(data).unwrap(),
        if deny_catalogue {
            original.clone()
        } else {
            torn_data
        }
    );
    let report = fs::read_to_string(f.0.join("replay.json")).unwrap();
    assert!(report.contains("\"stage\":\"refused\""), "{report}");
    assert!(report.contains("\"ready_ms\":0"), "{report}");
    assert!(
        report.contains(&format!("\"refusal\":{:?}", error.to_string())),
        "{report}"
    );
    assert!(report.contains("\"truncations\":[{\"chunk\""), "{report}");
    assert!(
        report.contains(&format!("\"offset\":{},\"bytes\":3", original.len())),
        "{report}"
    );
    assert!(
        report.contains(&format!(
            "\"catalogue_truncation\":{{\"offset\":{},\"bytes\":3",
            catalogue.len()
        )),
        "{report}"
    );
    let history = fs::read_to_string(f.0.join("replay.log")).unwrap();
    let lines: Vec<_> = history.lines().collect();
    assert!(lines[lines.len() - 2].contains("\"stage\":\"truncation_pending\""));
    assert!(lines.last().unwrap().contains("\"stage\":\"refused\""));
    drop(held);
    f.unlock();
    let store = Store::open(&f.0, config).unwrap();
    assert_eq!(store.epoch, 2);
    assert_eq!(store.get("caller", "key").unwrap().bytes, b"kept");
    assert_eq!(store.mutate(mutation), outcome);
    assert_eq!(store.replay_report().stage, "ready");
    assert_eq!(fs::read(data).unwrap(), original);
}

#[test]
fn data_tail_write_denial_records_refusal_before_either_repair_or_epoch() {
    repair_refusal(false);
}
#[test]
fn catalogue_tail_write_denial_records_refusal_without_undoing_prior_data_repair() {
    repair_refusal(true);
}
