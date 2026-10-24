//! Native process controls for the public writer admission contract. Child mode only accesses the
//! fixture path supplied by its parent; no installed service or production state is involved.
use membus::chunks::{CatRecord, Catalogue, Rec};
use std::{fs, path::PathBuf, process::{Command, Stdio}, time::{Duration, Instant}};

#[test]
fn catalogue_owner_child() {
    let Some(path) = std::env::var_os("MEMBUS_CATALOGUE_CHILD") else { return };
    let admitted = Catalogue::open(&PathBuf::from(path)).is_ok();
    assert_eq!(admitted, std::env::var("MEMBUS_CATALOGUE_ADMIT").unwrap() == "yes");
}
fn child(path: &std::path::Path, admitted: bool) {
    let mut command = Command::new(std::env::current_exe().unwrap());
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    let mut process = command
        .args(["--exact", "catalogue_owner_child"])
        .env("MEMBUS_CATALOGUE_CHILD", path)
        .env("MEMBUS_CATALOGUE_ADMIT", if admitted { "yes" } else { "no" })
        .stdout(Stdio::null()).spawn().unwrap();
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        if let Some(status) = process.try_wait().unwrap() { assert!(status.success()); break }
        if Instant::now() >= until {
            let _ = process.kill();
            let _ = process.wait();
            panic!("private writer fixture timed out");
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}
#[test]
fn native_writer_exclusion_survives_rewrite_and_releases_on_drop() {
    let dir = PathBuf::from(std::env::var_os("MEMBUS_TEST_OUTPUT").unwrap_or_else(|| ".test-output".into()))
        .join(format!("native-catalogue-owner-{}-{}", std::process::id(), membus::now_ms()));
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join("catalogue.log");
    let record = CatRecord { at_ms: 7, rec: Rec::Next(8) };
    let mut owner = Catalogue::open(&path).unwrap();
    owner.append(&record).unwrap();
    child(&path, false);
    owner.rewrite(&[record.clone()]).unwrap();
    child(&path, false);
    // A distinct hardlink spelling must not evade writer exclusion on the underlying file.
    let aliases = dir.join("aliases");
    fs::create_dir(&aliases).unwrap();
    let alias = aliases.join("catalogue.log");
    fs::hard_link(&path, &alias).unwrap();
    child(&alias, false);
    assert_eq!(membus::chunks::read_catalogue(&path).unwrap(), (vec![record], None));
    drop(owner);
    child(&path, true);
    fs::remove_dir_all(&dir).unwrap();
    let mut lock = dir.file_name().unwrap().to_os_string();
    lock.push(".catalogue-owner");
    fs::remove_file(dir.with_file_name(lock)).unwrap();
}

#[test]
fn historical_resume_stays_unknown_through_store_replay_and_compaction() {
    let dir = PathBuf::from(std::env::var_os("MEMBUS_TEST_OUTPUT").unwrap_or_else(|| ".test-output".into()))
        .join(format!("legacy-resume-{}-{}", std::process::id(), membus::now_ms()));
    fs::create_dir_all(dir.join("chunks")).unwrap();
    let record = CatRecord { at_ms: membus::now_ms(), rec: Rec::Resume };
    fs::write(dir.join("chunks/catalogue.log"), membus::chunks::encode(&record)).unwrap();
    let config = membus::Config { catalogue_rewrite_bytes: 1, ..Default::default() };
    let store = membus::Store::open(&dir, config.clone()).unwrap();
    assert_eq!(store.expiry_resume_record(), Some(record.clone()));
    assert_eq!(store.maintain_catalogue().unwrap(), membus::CatalogueMaintenance::Compacted);
    drop(store);
    let reopened = membus::Store::open(&dir, config).unwrap();
    assert_eq!(reopened.expiry_resume_record(), Some(record));
    drop(reopened);
    fs::remove_dir_all(dir).unwrap();
}
