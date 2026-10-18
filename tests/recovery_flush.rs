//! Public Store qualification of the startup prerequisite for recovered Flush.
use membus::{chunks, now_ms, Config, Mutation, Store};
use std::{
    fs,
    path::{Path, PathBuf},
};

fn fixture(label: &str) -> PathBuf {
    PathBuf::from(std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()))
        .join(format!(
            "recovery-flush-{label}-{}-{}",
            std::process::id(),
            now_ms()
        ))
}
fn mutation(id: &str, value: &[u8]) -> Mutation {
    Mutation {
        ns: "recovery-fixture".into(),
        key: "state".into(),
        op_id: id.into(),
        deadline: now_ms() + 30_000,
        ack: 2,
        expected: None,
        bytes: value.into(),
        remove: false,
    }
}
fn open(path: &Path, config: Config) -> std::io::Result<Store> {
    #[cfg(not(windows))]
    let _ = fs::remove_file(path.join("owner.lock"));
    Store::open(path, config)
}
fn data_hashes(path: &Path) -> Vec<(PathBuf, String)> {
    chunks::list_chunk_files(&path.join("chunks"))
        .unwrap()
        .into_iter()
        .map(|(_, _, _, p)| {
            let hash = chunks::sha256_file(&p).unwrap();
            (p, hash)
        })
        .collect()
}

#[test]
fn complete_uncovered_record_requires_recovery_sync_before_value_and_retry_publication() {
    let path = fixture("uncovered");
    let config = Config {
        flush_ms: 60_000,
        ..Config::default()
    };
    let store = open(
        &path,
        Config {
            fail_after_append: true,
            ..config.clone()
        },
    )
    .unwrap();
    let mut request = mutation("uncertain-original", b"complete unsynced record");
    assert_eq!(store.mutate(request.clone()).code, 16);
    assert!(store.get("recovery-fixture", "state").is_none());
    assert!(store.state_bytes() > 0);
    drop(store);
    let original_data = data_hashes(&path);
    assert!(!original_data.is_empty());
    let catalogue_before = chunks::sha256_file(&path.join("chunks/catalogue.log")).unwrap();
    let failure = open(
        &path,
        Config {
            fail_recovery_sync: true,
            ..config.clone()
        },
    );
    assert!(
        failure.is_err(),
        "startup must not expose requested Flush as achieved"
    );
    assert!(failure
        .err()
        .unwrap()
        .to_string()
        .contains("recovery sync failure"));
    assert_eq!(data_hashes(&path), original_data);
    assert_eq!(
        chunks::sha256_file(&path.join("chunks/catalogue.log")).unwrap(),
        catalogue_before,
        "failed data barrier must precede recovered Close publication"
    );
    let report = fs::read_to_string(path.join("replay.json")).unwrap();
    assert!(report.contains("\"stage\":\"refused\""));
    assert!(report.contains("\"ready_ms\":0"));

    let store = open(&path, config.clone()).unwrap();
    let value = store.get("recovery-fixture", "state").unwrap();
    assert_eq!(value.bytes, request.bytes);
    assert_eq!(value.ack, 2);
    request.deadline = now_ms() + 30_000;
    let original = store.mutate(request.clone());
    assert_eq!(
        (original.code, original.ack, original.version),
        (0, 2, value.version)
    );
    assert_eq!(store.metrics().0, 0, "original retry must not append");
    request.bytes.push(0);
    assert_eq!(store.mutate(request).code, 14);
    drop(store);
    let closed_failure = open(
        &path,
        Config {
            fail_recovery_sync: true,
            ..config
        },
    )
    .err()
    .expect("already closed recovered chunks still need the startup barrier");
    assert!(closed_failure.to_string().contains("recovery sync failure"));
}

#[test]
fn refused_recovery_retains_missing_file_evidence_already_found_by_replay() {
    let path = fixture("combined-refusal");
    let config = Config {
        flush_ms: 0,
        ..Config::default()
    };
    let store = open(&path, config.clone()).unwrap();
    let first = mutation("first", b"one");
    let stripe = membus::crc32(b"recovery-fixture\0state") % 4;
    let mut second = mutation("second", b"two");
    second.key = (0..100)
        .map(|n| format!("other-{n}"))
        .find(|key| membus::crc32(format!("recovery-fixture\0{key}").as_bytes()) % 4 != stripe)
        .unwrap();
    assert_eq!(store.mutate(first).code, 0);
    assert_eq!(store.mutate(second).code, 0);
    drop(store);
    // Close both at a successful startup, then remove only this owned fixture's
    // first data file. Replay must retain its missing-closed-file observation
    // when the remaining present file's required sync subsequently fails.
    drop(open(&path, config.clone()).unwrap());
    let files = data_hashes(&path);
    assert_eq!(files.len(), 2);
    fs::remove_file(&files[0].0).unwrap();
    let failed = open(
        &path,
        Config {
            fail_recovery_sync: true,
            ..config
        },
    );
    assert!(failed
        .err()
        .unwrap()
        .to_string()
        .contains("recovery sync failure"));
    let report = fs::read_to_string(path.join("replay.json")).unwrap();
    assert!(report.contains("\"stage\":\"refused\""));
    assert!(report.contains("\"ready_ms\":0"));
    assert!(
        report.contains("\"storage_errors\":[\"chunk file missing with no retiring record:"),
        "an early sync refusal must not discard the replay observation: {report}"
    );
}

#[test]
fn corrupt_closed_chunk_refusal_retains_the_prior_unregistered_file_observation() {
    let path = fixture("replay-error-report");
    let store = open(
        &path,
        Config {
            flush_ms: 0,
            ..Config::default()
        },
    )
    .unwrap();
    assert_eq!(
        store
            .mutate(mutation("original", b"public API record"))
            .code,
        0
    );
    drop(store);
    // Successful reopening closes the original public mutation's chunk.
    drop(open(&path, Config::default()).unwrap());
    let files = chunks::list_chunk_files(&path.join("chunks")).unwrap();
    assert_eq!(files.len(), 1);
    let (_, stripe, id, original) = &files[0];
    let unregistered = chunks::chunk_path(
        &path.join("chunks"),
        membus::Class::FOREVER,
        *stripe,
        id + 1000,
    );
    fs::copy(original, &unregistered).unwrap();
    let len = fs::metadata(original).unwrap().len();
    fs::OpenOptions::new()
        .write(true)
        .open(original)
        .unwrap()
        .set_len(len - 1)
        .unwrap();
    let files_before = data_hashes(&path);
    let catalogue_before = chunks::sha256_file(&path.join("chunks/catalogue.log")).unwrap();
    let epoch_before = fs::read(path.join("epoch")).unwrap();
    let error = open(&path, Config::default())
        .err()
        .expect("corruption refuses startup");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("torn tail in a closed chunk"));
    let report = fs::read_to_string(path.join("replay.json")).unwrap();
    assert!(report.contains("\"stage\":\"refused\""));
    assert!(report.contains("\"ready_ms\":0"));
    assert!(report.contains("interior-corrupt"));
    assert!(report.contains("torn tail in a closed chunk"));
    let observation = format!("chunk file with no open record: {}", unregistered.display());
    assert!(
        report.contains(&format!("{observation:?}")),
        "the refused report must retain the specific earlier observation: {report}"
    );
    assert_eq!(data_hashes(&path), files_before);
    assert_eq!(
        chunks::sha256_file(&path.join("chunks/catalogue.log")).unwrap(),
        catalogue_before
    );
    assert_eq!(fs::read(path.join("epoch")).unwrap(), epoch_before);
}

#[cfg(windows)]
#[test]
fn actual_recovery_write_open_denial_refuses_flush_publication_and_preserves_data() {
    struct Restore {
        path: PathBuf,
        permissions: fs::Permissions,
    }
    impl Drop for Restore {
        fn drop(&mut self) {
            if let Err(error) = fs::set_permissions(&self.path, self.permissions.clone()) {
                eprintln!("owned fixture permission restore failed: {error}");
            }
        }
    }
    let path = fixture("actual-open-denial");
    let config = Config {
        flush_ms: 0,
        ..Config::default()
    };
    let store = open(
        &path,
        Config {
            fail_after_append: true,
            ..config.clone()
        },
    )
    .unwrap();
    assert_eq!(
        store
            .mutate(mutation("uncertain-os-case", b"retained complete bytes"))
            .code,
        16
    );
    drop(store);
    let before = data_hashes(&path);
    assert_eq!(before.len(), 1);
    let data = &before[0].0;
    let permissions = fs::metadata(data).unwrap().permissions();
    assert!(!permissions.readonly());
    let restore = Restore {
        path: data.clone(),
        permissions: permissions.clone(),
    };
    let mut readonly = permissions;
    readonly.set_readonly(true);
    fs::set_permissions(data, readonly).unwrap();
    // No injected recovery fault: Windows itself refuses the required writable
    // handle. This changes only this fresh owned active-file fixture.
    let error = open(&path, config.clone())
        .err()
        .expect("required OS write-open failure must refuse startup");
    assert_eq!(error.kind(), std::io::ErrorKind::PermissionDenied);
    assert!(error.to_string().contains("recovery data sync"));
    assert_eq!(data_hashes(&path), before);
    let report = fs::read_to_string(path.join("replay.json")).unwrap();
    assert!(report.contains("\"stage\":\"refused\""));
    assert!(report.contains("\"ready_ms\":0"));
    drop(restore);
    assert!(!fs::metadata(data).unwrap().permissions().readonly());
    let store = open(&path, config).unwrap();
    assert_eq!(store.get("recovery-fixture", "state").unwrap().ack, 2);
    assert_eq!(data_hashes(&path), before);
}
