//! Epoch admission must precede migration and destructive replay repair.
use membus::{chunks, crc32, lp, now_ms, Config, Mutation, Store};
use std::{
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into());
        let path = PathBuf::from(root).join(format!(
            "recovery-epoch-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn open(&self) -> io::Result<Store> {
        // The non-Windows owner lock is a create-new file, even after a refusal.
        #[cfg(not(windows))]
        let _ = fs::remove_file(self.0.join("owner.lock"));
        Store::open(
            &self.0,
            Config {
                flush_ms: 0,
                ..Config::default()
            },
        )
    }
    fn legacy(&self) -> (PathBuf, Vec<u8>) {
        // One authored legacy record, in the admitted MBK1 stripe encoding.
        let mut payload = Vec::new();
        payload.extend(now_ms().to_le_bytes());
        payload.extend(7u64.to_le_bytes());
        payload.extend([1, 0]);
        lp(&mut payload, b"epoch-fixture");
        lp(&mut payload, b"key");
        lp(&mut payload, b"original");
        payload.extend(u64::MAX.to_le_bytes());
        lp(&mut payload, b"retained value");
        let mut bytes = b"MBK1".to_vec();
        bytes.extend((payload.len() as u32).to_le_bytes());
        bytes.extend((!(payload.len() as u32)).to_le_bytes());
        bytes.extend(crc32(&payload).to_le_bytes());
        bytes.extend(payload);
        let stripe = crc32(b"epoch-fixture\0key") % 4;
        let path = self.0.join(format!("stripe-{stripe}.log"));
        fs::write(&path, &bytes).unwrap();
        (path, bytes)
    }
    fn assert_legacy_untouched(&self, original: &(PathBuf, Vec<u8>)) {
        assert_eq!(fs::read(&original.0).unwrap(), original.1);
        assert!(!fs::metadata(&original.0).unwrap().permissions().readonly());
        for entry in fs::read_dir(&self.0).unwrap() {
            let name = entry.unwrap().file_name();
            let name = name.to_str().unwrap();
            assert!(
                !name.starts_with("chunks") && !name.starts_with("legacy-"),
                "epoch refusal must precede import output: {name}"
            );
        }
        assert!(!self.0.join("epoch.tmp").exists());
    }
    fn assert_refused(&self, reason: &str) {
        let report = fs::read_to_string(self.0.join("replay.json")).unwrap();
        assert!(report.contains("\"stage\":\"refused\""), "{report}");
        assert!(report.contains("\"ready_ms\":0"), "{report}");
        assert!(report.contains(reason), "{report}");
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        // Old-code import can have moved and marked the fixture read-only.
        fn clear_readonly(dir: &Path) {
            for entry in fs::read_dir(dir).unwrap() {
                let path = entry.unwrap().path();
                if path.is_dir() {
                    clear_readonly(&path);
                } else {
                    let mut permissions = fs::metadata(&path).unwrap().permissions();
                    #[allow(clippy::permissions_set_readonly_false)]
                    permissions.set_readonly(false);
                    fs::set_permissions(path, permissions).unwrap();
                }
            }
        }
        clear_readonly(&self.0);
        fs::remove_dir_all(&self.0).unwrap();
    }
}

#[test]
fn malformed_epoch_refuses_before_legacy_import_and_can_be_corrected() {
    for length in [0, 1, 7, 9] {
        let fixture = Fixture::new();
        let original = fixture.legacy();
        let invalid = vec![0x42; length];
        fs::write(fixture.0.join("epoch"), &invalid).unwrap();
        let error = fixture.open().err().expect("invalid epoch must refuse");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        fixture.assert_legacy_untouched(&original);
        assert_eq!(fs::read(fixture.0.join("epoch")).unwrap(), invalid);
        fixture.assert_refused("invalid epoch");

        // A failed admission consumes no import or epoch; a corrected input retries normally.
        fs::write(fixture.0.join("epoch"), 7u64.to_le_bytes()).unwrap();
        let store = fixture.open().unwrap();
        assert_eq!(store.epoch, 8);
        assert_eq!(
            store.get("epoch-fixture", "key").unwrap().bytes,
            b"retained value"
        );
        assert!(fixture.0.join("legacy-8").exists());
    }
}

#[test]
fn epoch_read_error_is_not_treated_as_a_missing_epoch() {
    let fixture = Fixture::new();
    let original = fixture.legacy();
    // An epoch directory supplies a real read failure on Unix and Windows.
    fs::create_dir(fixture.0.join("epoch")).unwrap();
    let marker = fixture.0.join("epoch/retained-marker");
    fs::write(&marker, b"keep").unwrap();
    let expected_kind = fs::read(fixture.0.join("epoch")).unwrap_err().kind();
    assert_ne!(expected_kind, io::ErrorKind::NotFound);
    let error = fixture.open().err().expect("unreadable epoch must refuse");
    assert_eq!(error.kind(), expected_kind);
    fixture.assert_legacy_untouched(&original);
    assert_eq!(fs::read(marker).unwrap(), b"keep");
    fixture.assert_refused("epoch");
}

#[test]
fn exhausted_epoch_refuses_without_panicking_or_changing_inputs() {
    let fixture = Fixture::new();
    let original = fixture.legacy();
    fs::write(fixture.0.join("epoch"), u64::MAX.to_le_bytes()).unwrap();
    let result = std::panic::catch_unwind(|| fixture.open());
    let error = result
        .expect("an exhausted persisted epoch must return an error, not panic")
        .err()
        .expect("exhausted epoch must refuse");
    assert!(error.to_string().contains("epoch exhausted"));
    fixture.assert_legacy_untouched(&original);
    assert_eq!(
        fs::read(fixture.0.join("epoch")).unwrap(),
        u64::MAX.to_le_bytes()
    );
    fixture.assert_refused("epoch exhausted");
}

#[test]
fn invalid_epoch_refuses_before_torn_tail_repair_and_close_publication() {
    let fixture = Fixture::new();
    let store = fixture.open().unwrap();
    assert_eq!(
        store
            .mutate(Mutation {
                ns: "epoch-fixture".into(),
                key: "key".into(),
                op_id: "original".into(),
                deadline: u64::MAX,
                ack: 2,
                expected: None,
                bytes: b"keep".to_vec(),
                remove: false,
            })
            .code,
        0
    );
    drop(store);
    let files = chunks::list_chunk_files(&fixture.0.join("chunks")).unwrap();
    assert_eq!(files.len(), 1);
    let data = &files[0].3;
    fs::OpenOptions::new()
        .append(true)
        .open(data)
        .unwrap()
        .write_all(b"MBK")
        .unwrap();
    let before = fs::read(data).unwrap();
    let catalogue = fs::read(fixture.0.join("chunks/catalogue.log")).unwrap();
    fs::write(fixture.0.join("epoch"), b"invalid").unwrap();
    assert!(fixture.open().is_err());
    assert_eq!(
        fs::read(data).unwrap(),
        before,
        "epoch admission must precede truncation"
    );
    assert_eq!(
        fs::read(fixture.0.join("chunks/catalogue.log")).unwrap(),
        catalogue
    );
    assert_eq!(fs::read(fixture.0.join("epoch")).unwrap(), b"invalid");
    fixture.assert_refused("invalid epoch");
}

#[test]
fn epoch_refusal_replaces_a_previous_ready_report() {
    let fixture = Fixture::new();
    drop(fixture.open().unwrap());
    let ready = fs::read_to_string(fixture.0.join("replay.json")).unwrap();
    assert!(ready.contains("\"stage\":\"ready\""));
    fs::write(fixture.0.join("epoch"), b"invalid").unwrap();
    assert!(fixture.open().is_err());
    fixture.assert_refused("invalid epoch");
}

#[test]
fn missing_epoch_bootstraps_and_each_successful_restart_increments_once() {
    let fixture = Fixture::new();
    for expected in 1u64..=3 {
        let store = fixture.open().unwrap();
        assert_eq!(store.epoch, expected);
        assert_eq!(
            fs::read(fixture.0.join("epoch")).unwrap(),
            expected.to_le_bytes()
        );
        assert_eq!(store.replay_report().stage, "ready");
    }
}

#[test]
fn last_available_epoch_is_used_consistently_for_import_and_publication() {
    let fixture = Fixture::new();
    fixture.legacy();
    fs::write(fixture.0.join("epoch"), (u64::MAX - 1).to_le_bytes()).unwrap();
    let store = fixture.open().unwrap();
    assert_eq!(store.epoch, u64::MAX);
    assert_eq!(
        fs::read(fixture.0.join("epoch")).unwrap(),
        u64::MAX.to_le_bytes()
    );
    assert!(fixture.0.join(format!("legacy-{}", u64::MAX)).exists());
    assert!(store
        .replay_report()
        .import
        .unwrap()
        .contains(&format!("epoch {}", u64::MAX)));
    assert_eq!(
        store.get("epoch-fixture", "key").unwrap().bytes,
        b"retained value"
    );
}
