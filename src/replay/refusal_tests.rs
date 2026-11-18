//! Owned recovery failures must replace a prior ready report and retain observations.
use super::*;

#[derive(Clone, Copy, Debug)]
enum RecoveryCase {
    Close,
    Retiring,
    Missing,
    Empty,
}

fn seed(f: &Fixture, case: RecoveryCase) -> (Config, PathBuf, u64) {
    let config = Config {
        clock: Some(chunks::ManualClock::new(1_000_000)),
        tick_ms: 3_600_000,
        retire_ms: 3_600_000,
        flush_ms: 0,
        ..Config::default()
    };
    let store = Store::open(&f.0, config.clone()).unwrap();
    let data;
    let seq;
    if matches!(case, RecoveryCase::Close | RecoveryCase::Retiring) {
        assert_eq!(
            store
                .mutate(Mutation {
                    ns: "caller".into(),
                    key: "key".into(),
                    op_id: "retained".into(),
                    deadline: u64::MAX,
                    ack: 2,
                    expected: None,
                    bytes: b"kept".to_vec(),
                    remove: false,
                })
                .code,
            0
        );
        let cs = store.chunks.lock().unwrap();
        let chunk = cs
            .table
            .values()
            .find(|c| c.state == chunks::ChunkState::Open)
            .unwrap();
        data = chunk.path.clone();
        seq = chunk.id;
    } else {
        seq = 1;
        data = chunks::chunk_path(&f.0.join("chunks"), Class::FOREVER, 0, seq);
    }
    drop(store);
    f.unlock();
    let mut catalogue = chunks::Catalogue::open(&f.0.join("chunks/catalogue.log")).unwrap();
    if matches!(case, RecoveryCase::Missing | RecoveryCase::Empty) {
        catalogue
            .append(&chunks::CatRecord {
                at_ms: 1_000_000,
                rec: chunks::Rec::Open {
                    seq,
                    stripe: 0,
                    class: Class::FOREVER,
                    policy: String::new(),
                    next: 2,
                },
            })
            .unwrap();
    }
    if matches!(case, RecoveryCase::Retiring) {
        catalogue
            .append(&chunks::CatRecord {
                at_ms: 1_000_000,
                rec: chunks::Rec::Close {
                    seq,
                    bytes: fs::metadata(&data).unwrap().len(),
                    next: 2,
                },
            })
            .unwrap();
        catalogue
            .append(&chunks::CatRecord {
                at_ms: 1_000_000,
                rec: chunks::Rec::Retiring {
                    seq,
                    next: 2,
                    horizons: vec![],
                    expired: vec![],
                },
            })
            .unwrap();
    }
    drop(catalogue);
    fs::create_dir_all(data.parent().unwrap()).unwrap();
    if matches!(case, RecoveryCase::Empty) {
        fs::write(&data, b"").unwrap();
    }
    // A located observation must survive a later failure in a different startup stage.
    let unregistered = chunks::chunk_path(&f.0.join("chunks"), Class::FOREVER, 0, 1000);
    fs::write(unregistered, b"").unwrap();
    assert!(fs::read_to_string(f.0.join("replay.json"))
        .unwrap()
        .contains("\"stage\":\"ready\""));
    (config, data, seq)
}

fn assert_refused(f: &Fixture, epoch: &[u8], catalogue: &[u8]) {
    let report = fs::read_to_string(f.0.join("replay.json")).unwrap();
    assert!(report.contains("\"stage\":\"refused\""), "{report}");
    assert!(report.contains("\"ready_ms\":0"), "{report}");
    assert!(
        report.contains("chunk file with no open record"),
        "{report}"
    );
    assert!(report.contains("1000.log"), "{report}");
    let history = fs::read_to_string(f.0.join("replay.log")).unwrap();
    assert!(history
        .lines()
        .last()
        .unwrap()
        .contains("\"stage\":\"refused\""));
    assert_eq!(fs::read(f.0.join("epoch")).unwrap(), epoch);
    assert_eq!(
        fs::read(f.0.join("chunks/catalogue.log")).unwrap(),
        catalogue
    );
    assert!(!f.0.join("epoch.tmp").exists());
}

fn append_refusal(case: RecoveryCase) {
    let f = Fixture::new();
    let (mut config, data, seq) = seed(&f, case);
    let epoch = fs::read(f.0.join("epoch")).unwrap();
    let catalogue = fs::read(f.0.join("chunks/catalogue.log")).unwrap();
    let root = f.0.join("chunks");
    let marker = root.join("catalogue.log.append-pending");
    let pending = marker.clone();
    let expected = Arc::new(Mutex::new(None));
    let observed = expected.clone();
    config.recovery_dir_sync = Some(Arc::new(move |dir| {
        if dir == root {
            fs::create_dir(&pending)?;
            *observed.lock().unwrap() = Some(
                OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&pending)
                    .unwrap_err()
                    .raw_os_error()
                    .unwrap(),
            );
        }
        Ok(())
    }));
    let error = Store::open(&f.0, config.clone())
        .err()
        .expect("blocked catalogue intent must refuse");
    assert_eq!(error.raw_os_error(), *expected.lock().unwrap());
    assert!(marker.is_dir());
    assert_eq!(data.exists(), matches!(case, RecoveryCase::Close));
    assert_refused(&f, &epoch, &catalogue);
    // Deletion already performed before the failed Retired append is not undone.
    fs::remove_dir(marker).unwrap();
    f.unlock();
    config.recovery_dir_sync = None;
    let store = Store::open(&f.0, config).unwrap();
    assert_eq!(store.epoch, 2);
    assert_eq!(store.replay_report().stage, "ready");
    assert!(
        store.storage_error(),
        "the unregistered observation remains sticky"
    );
    let state = store.chunks.lock().unwrap().table[&seq].state.clone();
    if matches!(case, RecoveryCase::Close) {
        assert_eq!(state, chunks::ChunkState::Closed);
        assert_eq!(store.get("caller", "key").unwrap().bytes, b"kept");
    } else {
        assert_eq!(state, chunks::ChunkState::Retired);
    }
}

#[test]
fn close_append_failure_replaces_the_previous_ready_report() {
    append_refusal(RecoveryCase::Close);
}
#[test]
fn retiring_completion_append_failure_replaces_the_previous_ready_report() {
    append_refusal(RecoveryCase::Retiring);
}
#[test]
fn missing_open_retired_append_failure_replaces_the_previous_ready_report() {
    append_refusal(RecoveryCase::Missing);
}
#[test]
fn empty_open_retired_append_failure_replaces_the_previous_ready_report() {
    append_refusal(RecoveryCase::Empty);
}

#[cfg(windows)]
#[test]
fn late_catalogue_sharing_denial_records_refusal_with_prior_observations() {
    use std::os::windows::fs::OpenOptionsExt;
    let f = Fixture::new();
    let (mut config, data, _) = seed(&f, RecoveryCase::Close);
    let epoch = fs::read(f.0.join("epoch")).unwrap();
    let catalogue = fs::read(f.0.join("chunks/catalogue.log")).unwrap();
    let original_data = fs::read(&data).unwrap();
    let orphan = chunks::chunk_path(&f.0.join("chunks"), Class::FOREVER, 0, 1000);
    let cat = f.0.join("chunks/catalogue.log");
    let parent = data.parent().unwrap().to_owned();
    let expected = Arc::new(Mutex::new(None));
    let observed = expected.clone();
    config.recovery_dir_sync = Some(Arc::new(move |dir| {
        if dir == parent {
            // The owned writer is already open. This new handle's share flags deny it.
            let error = OpenOptions::new()
                .read(true)
                .share_mode(1 | 4)
                .open(&cat)
                .unwrap_err();
            *observed.lock().unwrap() = error.raw_os_error();
            return Err(error);
        }
        Ok(())
    }));
    let error = Store::open(&f.0, config.clone())
        .err()
        .expect("owned catalogue must refuse the conflicting reader");
    assert_eq!(error.raw_os_error(), Some(32));
    assert_eq!(error.raw_os_error(), *expected.lock().unwrap());
    assert_refused(&f, &epoch, &catalogue);
    assert_eq!(fs::read(&data).unwrap(), original_data);
    assert!(fs::read(&orphan).unwrap().is_empty());
    let report = fs::read_to_string(f.0.join("replay.json")).unwrap();
    assert!(report.contains("\"epoch\":2"), "{report}");
    assert!(report.contains("\"records\":1"), "{report}");
    f.unlock();
    config.recovery_dir_sync = None;
    let store = Store::open(&f.0, config).unwrap();
    assert_eq!(store.epoch, 2);
    assert_eq!(store.replay_report().stage, "ready");
    assert!(store.storage_error());
    assert!(fs::read(orphan).unwrap().is_empty());
    let value = store.get("caller", "key").unwrap();
    assert_eq!((value.version, value.ack, value.bytes), (1, 2, b"kept".to_vec()));
    assert_eq!(store.mutate(Mutation {
        ns: "caller".into(), key: "key".into(), op_id: "retained".into(),
        deadline: u64::MAX, ack: 2, expected: None, bytes: b"kept".to_vec(), remove: false,
    }), crate::Outcome { code: 0, version: 1, ack: 2 });
}

#[cfg(windows)]
#[test]
fn empty_open_delete_denial_records_refusal_without_changing_intent() {
    use std::os::windows::fs::OpenOptionsExt;
    let f = Fixture::new();
    let (config, data, _) = seed(&f, RecoveryCase::Empty);
    let epoch = fs::read(f.0.join("epoch")).unwrap();
    let catalogue = fs::read(f.0.join("chunks/catalogue.log")).unwrap();
    let held = OpenOptions::new()
        .read(true)
        .share_mode(1 | 2)
        .open(&data)
        .unwrap();
    let expected = fs::remove_file(&data).unwrap_err();
    let error = Store::open(&f.0, config.clone())
        .err()
        .expect("held empty chunk must deny deletion");
    assert_eq!(error.raw_os_error(), expected.raw_os_error());
    assert!(data.exists());
    assert_refused(&f, &epoch, &catalogue);
    drop(held);
    f.unlock();
    let store = Store::open(&f.0, config).unwrap();
    assert_eq!(store.epoch, 2);
    assert!(!data.exists());
}
