//! A failed pending report refuses before repair and preserves the publication error.
use super::*;

fn torn(f: &Fixture, manual: &Arc<ManualClock>) -> (Mutation, PathBuf, Vec<u8>, Vec<u8>) {
    let original = Mutation {
        ack: 2,
        ..mutation("pending-original", u64::MAX)
    };
    let store = f.open(manual);
    assert_eq!(store.mutate(original.clone()).code, 0);
    drop(store);
    f.unlock();
    let data = chunks::list_chunk_files(&f.0.join("chunks")).unwrap()[0]
        .3
        .clone();
    OpenOptions::new()
        .append(true)
        .open(&data)
        .unwrap()
        .write_all(b"MBK")
        .unwrap();
    let bytes = fs::read(&data).unwrap();
    let catalogue = fs::read(f.0.join("chunks/catalogue.log")).unwrap();
    (original, data, bytes, catalogue)
}

#[test]
fn each_pending_directory_failure_reports_refusal_without_repair_or_epoch_publication() {
    for stop in 1..=3 {
        let f = Fixture::new();
        let manual = ManualClock::new(UTC);
        let (original, data, bytes, catalogue) = torn(&f, &manual);
        fs::write(f.0.join("replay.log"), "previous history\n".repeat(70_000)).unwrap();
        let root = f.0.clone();
        let clock = manual.clone();
        let calls = Arc::new(AtomicU64::new(0));
        let seen = calls.clone();
        let mut config = f.config(&manual);
        config.replay_report_dir_sync = Some(Arc::new(move |_| {
            let json = fs::read_to_string(root.join("replay.json"))?;
            if json.contains("\"stage\":\"truncation_pending\"")
                && seen.fetch_add(1, Ordering::SeqCst) + 1 == stop
            {
                clock.advance(11);
                clock.jump_utc(-50_000);
                return Err(io::Error::from_raw_os_error(5));
            }
            Ok(())
        }));
        let error = Store::open(&f.0, config)
            .err()
            .expect("pending publication must fail");
        assert_eq!(error.raw_os_error(), Some(5));
        assert_eq!(calls.load(Ordering::SeqCst), stop);
        assert_eq!(fs::read(&data).unwrap(), bytes);
        assert_eq!(
            fs::read(f.0.join("chunks/catalogue.log")).unwrap(),
            catalogue
        );
        assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 1u64.to_le_bytes());
        let report = fs::read_to_string(f.0.join("replay.json")).unwrap();
        assert!(report.contains("\"stage\":\"refused\""), "{report}");
        assert!(report.contains(&format!("\"refusal\":{:?}", error.to_string())));
        assert!(
            report.contains("\"bytes\":3,\"reason\":\"short header\""),
            "{report}"
        );
        assert_eq!(elapsed_json_number(&report, "epoch"), 2);
        assert_eq!(elapsed_json_number(&report, "ready_ms"), 0);
        assert_eq!(elapsed_json_number(&report, "duration_ms"), 11);
        assert_eq!(elapsed_json_number(&report, "keys"), 1);
        assert_eq!(elapsed_json_number(&report, "dedup_entries"), 1);
        let history = fs::read_to_string(f.0.join("replay.log")).unwrap();
        assert_eq!(history.lines().last(), Some(report.trim_end()));
        manual.jump_utc(50_000);
        f.unlock();
        let store = f.open(&manual);
        assert_eq!(store.epoch, 2);
        assert_eq!(store.get("clock", "key").unwrap().bytes, original.bytes);
        assert_eq!(store.mutate(original).version, 1);
    }
}

#[cfg(windows)]
#[test]
fn actual_pending_history_denial_reports_refusal_and_preserves_torn_input() {
    use std::os::windows::fs::OpenOptionsExt;
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let (original, data, bytes, catalogue) = torn(&f, &manual);
    let history = fs::read(f.0.join("replay.log")).unwrap();
    let held = OpenOptions::new()
        .read(true)
        .share_mode(1)
        .open(f.0.join("replay.log"))
        .unwrap();
    let error = Store::open(&f.0, f.config(&manual))
        .err()
        .expect("history append must fail");
    assert_eq!(error.raw_os_error(), Some(32));
    let report = fs::read_to_string(f.0.join("replay.json")).unwrap();
    assert!(report.contains("\"stage\":\"refused\""), "{report}");
    assert!(
        report.contains("\"bytes\":3,\"reason\":\"short header\""),
        "{report}"
    );
    assert!(report.contains(&format!("\"refusal\":{:?}", error.to_string())));
    assert_eq!(fs::read(&data).unwrap(), bytes);
    assert_eq!(
        fs::read(f.0.join("chunks/catalogue.log")).unwrap(),
        catalogue
    );
    assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 1u64.to_le_bytes());
    assert_eq!(fs::read(f.0.join("replay.log")).unwrap(), history);
    drop(held);
    let store = f.open(&manual);
    assert_eq!(store.epoch, 2);
    assert_eq!(store.get("clock", "key").unwrap().bytes, original.bytes);
    assert_eq!(store.mutate(original).version, 1);
}
