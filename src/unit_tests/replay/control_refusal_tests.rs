//! Late timer/policy projection failures retain refused startup evidence.
use super::*;

fn config() -> Config {
    Config {
        clock: Some(chunks::ManualClock::new(1_000_000)),
        flush_ms: 0,
        tick_ms: 3_600_000,
        retire_ms: 3_600_000,
        ..Config::default()
    }
}
fn schedule(id: &str) -> Vec<u8> {
    let mut bytes = b"MTS1".to_vec();
    for text in ["caller", id, "wake/key"] {
        crate::lp(&mut bytes, text.as_bytes());
    }
    for n in [1_000_000u64, 100, 0, 0, 1] {
        bytes.extend(n.to_le_bytes());
    }
    crate::lp(&mut bytes, b"authored");
    bytes
}
fn put(store: &Store, key: &str, bytes: Vec<u8>, remove: bool, id: &str) {
    assert_eq!(
        store
            .mutate(Mutation {
                ns: timers::NS.into(),
                key: key.into(),
                op_id: id.into(),
                deadline: u64::MAX,
                ack: 2,
                expected: None,
                bytes,
                remove,
            })
            .code,
        0
    );
}
fn observe_unregistered(f: &Fixture) {
    let path = chunks::chunk_path(&f.0.join("chunks"), Class::FOREVER, 0, 1000);
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(path, b"").unwrap();
}
fn seed_timer(f: &Fixture, bytes: Vec<u8>) -> Config {
    let cfg = config();
    let store = Store::open(&f.0, cfg.clone()).unwrap();
    // These are authored low-level control fixtures; public timer admission validates schedules.
    put(&store, "s/caller\0timer", bytes, false, "schedule");
    drop(store);
    f.unlock();
    observe_unregistered(f);
    cfg
}
fn assert_refused_after_epoch(f: &Fixture, error: &io::Error, ns: &str) {
    let report = fs::read_to_string(f.0.join("replay.json")).unwrap();
    assert!(report.contains("\"stage\":\"refused\""), "{report}");
    assert!(report.contains("\"ready_ms\":0"), "{report}");
    assert!(report.starts_with("{\"epoch\":2,"), "the refused attempt must identify its consumed epoch: {report}");
    let observed: usize = report.split("\"records\":").skip(1)
        .map(|part| part.split(|c: char| !c.is_ascii_digit()).next().unwrap().parse::<usize>().unwrap()).sum();
    assert!(report.contains(&format!("\"keys\":{observed},")), "{report}");
    assert!(report.contains("\"dedup_entries\":1,"), "timer progress is excluded from retries: {report}");
    assert!(report.contains(&format!("\"namespace_counts\":{{\"{ns}\":{observed}}}")), "{report}");
    assert!(
        report.contains(&format!("\"refusal\":{:?}", error.to_string())),
        "{report}"
    );
    assert!(
        report.contains("chunk file with no open record") && report.contains("1000.log"),
        "{report}"
    );
    let history = fs::read_to_string(f.0.join("replay.log")).unwrap();
    assert!(history.lines().last().unwrap().starts_with("{\"epoch\":2,"));
    assert!(history
        .lines()
        .last()
        .unwrap()
        .contains("\"stage\":\"refused\""));
    // Projection remains after durable epoch publication; no rollback or reordering is introduced.
    assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 2u64.to_le_bytes());
}
fn schema_refusal(bytes: Vec<u8>, reason: &str) {
    let f = Fixture::new();
    let cfg = seed_timer(&f, bytes);
    let error = Store::open(&f.0, cfg)
        .err()
        .expect("invalid control projection must refuse");
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert_eq!(error.to_string(), reason);
    assert_refused_after_epoch(&f, &error, timers::NS);
}

#[test]
fn malformed_schedule_records_refusal_after_published_epoch() {
    schema_refusal(
        b"invalid schedule".to_vec(),
        "invalid persisted timer schedule",
    );
}

fn cold_refusal(policy: bool) {
    let f = Fixture::new();
    let mut cfg = config();
    let store = Store::open(&f.0, cfg.clone()).unwrap();
    if policy {
        assert_eq!(
            store
                .set_policy("caller", "", Some(Class::LATEST), u64::MAX)
                .code,
            0
        );
    } else {
        put(
            &store,
            "s/caller\0timer",
            schedule("timer"),
            false,
            "schedule",
        );
    }
    let files = chunks::list_chunk_files(&f.0.join("chunks")).unwrap();
    assert_eq!(files.len(), 1);
    let path = files[0].3.clone();
    let original = fs::read(&path).unwrap();
    drop(store);
    f.unlock();
    observe_unregistered(&f);
    let root = f.0.clone();
    let changed = path.clone();
    let mut damaged = original.clone();
    *damaged.last_mut().unwrap() ^= 1;
    cfg.recovery_dir_sync = Some(Arc::new(move |dir| {
        if dir == root && fs::read(root.join("epoch"))? == 2u64.to_le_bytes() {
            fs::write(&changed, &damaged)?;
        }
        Ok(())
    }));
    let error = Store::open(&f.0, cfg.clone())
        .err()
        .expect("late cold control read must refuse");
    assert_refused_after_epoch(&f, &error, if policy { chunks::POLICY_NS } else { timers::NS });
    fs::write(path, original).unwrap();
    f.unlock();
    cfg.recovery_dir_sync = None;
    let store = Store::open(&f.0, cfg).unwrap();
    assert_eq!(store.epoch, 3);
    assert_eq!(store.replay_report().stage, "ready");
    if policy {
        assert_eq!(
            store.policies.read().unwrap().resolve("caller", "key").0,
            Class::LATEST
        );
    } else {
        assert_eq!(timer_status(&store)[0], 1);
    }
}
#[test]
fn late_timer_read_failure_records_refusal_and_corrected_input_reopens() {
    cold_refusal(false);
}
#[test]
fn late_policy_read_failure_records_refusal_and_corrected_input_reopens() {
    cold_refusal(true);
}

fn timer_status(store: &Store) -> Vec<u8> {
    let mut bytes = Vec::new();
    crate::lp(&mut bytes, b"timer");
    store
        .timer_call(
            crate::TIMER_STATUS,
            "caller".into(),
            "status".into(),
            u64::MAX,
            &mut crate::Cursor::new(&bytes),
        )
        .unwrap()
}
#[test]
fn report_write_failure_does_not_replace_the_original_control_error() {
    let f = Fixture::new();
    let cfg = seed_timer(&f, b"invalid schedule".to_vec());
    let ready = fs::read(f.0.join("replay.json")).unwrap();
    fs::create_dir(f.0.join("replay.json.tmp")).unwrap();
    let error = Store::open(&f.0, cfg).err().unwrap();
    assert_eq!(error.to_string(), "invalid persisted timer schedule");
    assert_eq!(fs::read(f.0.join("replay.json")).unwrap(), ready);
    assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 2u64.to_le_bytes());
}
