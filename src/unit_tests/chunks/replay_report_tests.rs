//! Report publication barriers before history changes and dependent recovery.
use super::*;
use std::sync::atomic::AtomicU64;

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".test-output")
            .join(format!("report-directories-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::SeqCst)));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn history(&self) -> String {
        let lines: String = (0..300).map(|i| format!("{i:03}:{}\n", "x".repeat(3600))).collect();
        fs::write(self.0.join("replay.log"), &lines).unwrap();
        lines
    }
}
impl Drop for Fixture {
    fn drop(&mut self) { fs::remove_dir_all(&self.0).unwrap(); }
}
fn report() -> ReplayReport {
    ReplayReport { epoch: 9, start_ms: 10, ready_ms: 0, stage: "truncation_pending".into(), ..Default::default() }
}

#[test]
fn snapshot_directory_barrier_precedes_history_mutation() {
    let f = Fixture::new();
    let old = f.history();
    let report = report();
    let mut calls = 0;
    report.write_with_sync(&f.0, |dir| {
        calls += 1;
        assert_eq!(dir, f.0);
        if calls == 1 {
            assert_eq!(fs::read_to_string(f.0.join("replay.json"))?, format!("{}\n", report.json()));
            assert!(!f.0.join("replay.json.tmp").exists());
            assert_eq!(fs::read_to_string(f.0.join("replay.log"))?, old);
            assert!(!f.0.join("replay.log.tmp").exists());
        }
        crate::log_io::sync_dir(dir)
    }).unwrap();
    assert_eq!(calls, 3, "snapshot, rotation and appended history names need barriers");
}

#[test]
fn rotation_directory_barrier_precedes_the_new_history_line() {
    let f = Fixture::new();
    let old = f.history();
    let expected = old.lines().skip(44).collect::<Vec<_>>().join("\n") + "\n";
    let mut calls = 0;
    report().write_with_sync(&f.0, |dir| {
        calls += 1;
        if calls == 2 {
            assert_eq!(fs::read_to_string(f.0.join("replay.log"))?, expected);
            assert!(!f.0.join("replay.log.tmp").exists());
        }
        crate::log_io::sync_dir(dir)
    }).unwrap();
    assert_eq!(calls, 3);
    assert_eq!(fs::read_to_string(f.0.join("replay.log")).unwrap(), expected + &report().json() + "\n");
}

#[test]
fn new_history_name_is_synced_after_its_complete_line() {
    let f = Fixture::new();
    let mut calls = 0;
    report().write_with_sync(&f.0, |dir| {
        calls += 1;
        if calls == 1 {
            assert!(!f.0.join("replay.log").exists());
        } else {
            assert_eq!(fs::read_to_string(f.0.join("replay.log"))?, report().json() + "\n");
        }
        crate::log_io::sync_dir(dir)
    }).unwrap();
    assert_eq!(calls, 2, "a newly created history needs its own publication barrier");
}

#[test]
fn report_directory_failures_refuse_at_each_stage_and_allow_retry() {
    for stop in 1..=3 {
        let f = Fixture::new();
        let old = f.history();
        let mut calls = 0;
        let error = report().write_with_sync(&f.0, |dir| {
            calls += 1;
            if calls == stop {
                Err(io::Error::from_raw_os_error(5))
            } else {
                crate::log_io::sync_dir(dir)
            }
        }).expect_err("the report must refuse a failed directory barrier");
        assert_eq!(error.raw_os_error(), Some(5), "retain the original OS error");
        assert_eq!(calls, stop, "later writes/barriers must not follow refusal");
        let log = fs::read_to_string(f.0.join("replay.log")).unwrap();
        if stop == 1 { assert_eq!(log, old); }
        if stop == 2 { assert!(!log.contains("truncation_pending")); }
        if stop == 3 { assert!(log.ends_with(&(report().json() + "\n"))); }
        report().write(&f.0).unwrap();
        assert_eq!(fs::read_to_string(f.0.join("replay.json")).unwrap(), report().json() + "\n");
        assert!(fs::read_to_string(f.0.join("replay.log")).unwrap().ends_with(&(report().json() + "\n")));
    }
}
