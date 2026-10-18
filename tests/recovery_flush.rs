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
