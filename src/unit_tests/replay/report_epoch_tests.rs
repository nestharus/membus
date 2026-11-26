//! Non-ready reports identify a validated attempt, independent of epoch publication.
use super::*;

fn assert_report(f: &Fixture, epoch: u64, stage: &str) {
    let report = fs::read_to_string(f.0.join("replay.json")).unwrap();
    assert!(report.starts_with(&format!("{{\"epoch\":{epoch},")), "{report}");
    assert!(report.contains(&format!("\"stage\":\"{stage}\"")), "{report}");
    assert!(report.contains("\"ready_ms\":0"), "{report}");
    let history = fs::read_to_string(f.0.join("replay.log")).unwrap();
    assert_eq!(history.lines().last(), Some(report.trim_end()));
}

fn seed(f: &Fixture, clock: &Arc<ManualClock>) -> PathBuf {
    let store = f.open(clock);
    assert_eq!(store.mutate(mutation("epoch-evidence", u64::MAX)).code, 0);
    assert_eq!(store.epoch, 1);
    drop(store);
    f.unlock();
    chunks::list_chunk_files(&f.0.join("chunks")).unwrap()[0].3.clone()
}

#[test]
fn pending_truncation_report_identifies_attempt_before_epoch_publication() {
    let f = Fixture::new();
    let clock = ManualClock::new(UTC);
    let path = seed(&f, &clock);
    OpenOptions::new().append(true).open(&path).unwrap().write_all(b"MB").unwrap();
    let mut config = f.config(&clock);
    config.stop_after_replay_truncation = true;
    assert_eq!(Store::open(&f.0, config).err().unwrap().to_string(), "test stop after replay truncation");
    assert_report(&f, 2, "truncation_pending");
    assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 1u64.to_le_bytes());
    f.unlock();
    let store = f.open(&clock);
    assert_eq!(store.epoch, 2, "a pending report must not consume the epoch");
    assert_eq!(store.replay_report().epoch, 2);
    assert_eq!(store.get("clock", "key").unwrap().bytes, b"epoch-evidence");
}

#[test]
fn data_sync_refusal_identifies_attempt_without_consuming_epoch() {
    let f = Fixture::new();
    let clock = ManualClock::new(UTC);
    let path = seed(&f, &clock);
    let before = fs::read(&path).unwrap();
    let mut config = f.config(&clock);
    config.fail_recovery_sync = true;
    assert!(Store::open(&f.0, config).err().unwrap().to_string().contains("recovery sync failure"));
    assert_report(&f, 2, "refused");
    assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 1u64.to_le_bytes());
    assert_eq!(fs::read(path).unwrap(), before);
    f.unlock();
    let store = f.open(&clock);
    assert_eq!(store.replay_report().epoch, 2);
    assert_eq!(store.replay_report().stage, "ready");
}

#[test]
fn replay_corruption_refusal_identifies_attempt_before_reconstruction() {
    let f = Fixture::new();
    let clock = ManualClock::new(UTC);
    let path = seed(&f, &clock);
    let mut bytes = fs::read(&path).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    fs::write(&path, &bytes).unwrap();
    assert!(Store::open(&f.0, f.config(&clock)).err().unwrap().to_string().contains("interior-corrupt"));
    assert_report(&f, 2, "refused");
    assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 1u64.to_le_bytes());
    assert_eq!(fs::read(path).unwrap(), bytes);
}

#[test]
fn first_import_refusal_identifies_validated_attempt() {
    let f = Fixture::new();
    let clock = ManualClock::new(UTC);
    f.legacy(&[(mutation("import-epoch", u64::MAX), 7, UTC)]);
    fs::write(f.0.join("import-policies.txt"), b"invalid authored policy").unwrap();
    let before = fs::read(f.0.join("stripe-0.log")).unwrap();
    assert!(Store::open(&f.0, f.config(&clock)).is_err());
    assert_report(&f, 1, "refused");
    assert!(!f.0.join("epoch").exists());
    assert_eq!(fs::read(f.0.join("stripe-0.log")).unwrap(), before);
}

#[test]
fn epoch_publication_refusal_identifies_unpublished_attempt() {
    let f = Fixture::new();
    let clock = ManualClock::new(UTC);
    seed(&f, &clock);
    fs::create_dir(f.0.join("epoch.tmp")).unwrap();
    assert!(Store::open(&f.0, f.config(&clock)).is_err());
    assert_report(&f, 2, "refused");
    assert_eq!(fs::read(f.0.join("epoch")).unwrap(), 1u64.to_le_bytes());
}

#[test]
fn invalid_or_exhausted_epoch_keeps_unknown_attempt_zero() {
    for bytes in [b"invalid".to_vec(), u64::MAX.to_le_bytes().to_vec()] {
        let f = Fixture::new();
        fs::create_dir_all(&f.0).unwrap();
        fs::write(f.0.join("epoch"), &bytes).unwrap();
        assert!(Store::open(&f.0, f.config(&ManualClock::new(UTC))).is_err());
        assert_report(&f, 0, "refused");
        assert_eq!(fs::read(f.0.join("epoch")).unwrap(), bytes);
    }
}
