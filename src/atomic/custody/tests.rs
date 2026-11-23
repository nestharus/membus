use super::*;
use crate::atomic::{BorrowedOriginal, OriginalBudget};
use std::{
    fs,
    os::windows::{fs::OpenOptionsExt, io::AsRawHandle},
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(".test-output");
        fs::create_dir_all(&root).unwrap();
        let path = root.join(format!(
            "held-mar-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

// Authored MARs, including a value larger than the writer's scratch. The
// expected on-disk bytes do not come from the encoder under test.
fn original(wide: bool) -> Vec<u8> {
    let mut bytes = if wide { b"MAR2" } else { b"MAR1" }.to_vec();
    crate::lp(&mut bytes, b"dataset");
    crate::lp(&mut bytes, b"op");
    bytes.extend(1u64.to_le_bytes());
    bytes.extend(99u64.to_le_bytes());
    bytes.push(if wide { 1 } else { 2 });
    bytes.extend(0u32.to_le_bytes());
    if wide {
        bytes.extend([17; 32]);
        bytes.extend(8u32.to_le_bytes());
        bytes.extend(65536u64.to_le_bytes());
        bytes.extend(4u16.to_le_bytes());
        bytes.extend(65536u32.to_le_bytes());
    }
    bytes.extend(1u16.to_le_bytes());
    crate::lp(&mut bytes, b"key");
    bytes.push(0);
    crate::lp(&mut bytes, &[0xa5; 16384]);
    bytes
}

fn funded(bytes: &[u8], headroom: usize) -> (AtomicRequest, OriginalBudget) {
    let needed = BorrowedOriginal::from_canonical_bytes(bytes)
        .unwrap()
        .allocation_bytes()
        .unwrap();
    let budget = OriginalBudget::new(needed + headroom);
    (
        AtomicRequest::from_bytes_funded(bytes, &budget).unwrap(),
        budget,
    )
}

#[test]
fn both_mar_formats_save_at_exact_headroom_and_legacy_returns_unit() {
    for wide in [false, true] {
        let fixture = Fixture::new();
        let bytes = original(wide);
        let (request, budget) = funded(&bytes, HeldOriginalFile::SAVE_BYTES);
        let original_charge = budget.used();
        let path = fixture.0.join("held");
        let held = request.persist_new_held(&path).unwrap();
        assert_eq!(
            held.file_id(),
            &file_id(&File::open(&path).unwrap()).unwrap()
        );
        assert_eq!(fs::read(&path).unwrap(), bytes);
        let legacy = fixture.0.join("legacy");
        let (): () = request.persist_new(&legacy).unwrap();
        assert_eq!(fs::read(&legacy).unwrap(), bytes);
        assert_ne!(
            held.file_id(),
            &file_id(&File::open(&legacy).unwrap()).unwrap()
        );
        assert_eq!(
            request.persist_new(&legacy).unwrap_err().kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(
            budget.used(),
            original_charge + size_of::<HeldOriginalFile>()
        );
        drop(request);
        assert_eq!(budget.used(), size_of::<HeldOriginalFile>());
        drop(held);
        assert_eq!(budget.used(), 0);
    }
}

#[test]
fn insufficient_headroom_refuses_before_creation_and_legacy_needs_no_headroom() {
    let fixture = Fixture::new();
    let bytes = original(false);
    for headroom in [0, HeldOriginalFile::SAVE_BYTES - 1] {
        let (request, budget) = funded(&bytes, headroom);
        let original_charge = budget.used();
        let path = fixture.0.join(headroom.to_string());
        assert_eq!(
            request.persist_new_held(&path).err().unwrap().kind(),
            io::ErrorKind::OutOfMemory
        );
        assert_eq!(budget.used(), original_charge);
        assert!(!path.try_exists().unwrap());
        request.persist_new(&path).unwrap();
        assert_eq!(fs::read(path).unwrap(), bytes);
    }
}

#[test]
fn relative_saves_acknowledge_the_selected_current_directory() {
    use std::{env, os::windows::process::CommandExt, process::Command};

    const CHILD_DIR: &str = "MEMBUS_RELATIVE_CUSTODY_TEST_DIR";
    if let Some(selected) = env::var_os(CHILD_DIR) {
        let selected = Path::new(&selected);
        assert_eq!(env::current_dir().unwrap(), selected);
        for (ordinary, held) in [
            ("ordinary.mar", "held.mar"),
            (r".\dot-ordinary.mar", r".\dot-held.mar"),
        ] {
            check_relative_saves(selected, Path::new(ordinary), Path::new(held));
        }
        return;
    }

    // Give just this child the selected CWD. Parallel tests keep their own CWD.
    let fixture = Fixture::new();
    let test = std::thread::current();
    let result = Command::new(env::current_exe().unwrap())
        .args(["--exact", test.name().unwrap(), "--nocapture"])
        .current_dir(&fixture.0)
        .env(CHILD_DIR, &fixture.0)
        .creation_flags(0x08000000) // CREATE_NO_WINDOW
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    for name in [
        "ordinary.mar",
        "held.mar",
        "dot-ordinary.mar",
        "dot-held.mar",
    ] {
        assert_eq!(fs::read(fixture.0.join(name)).unwrap(), original(false));
    }
}

fn check_relative_saves(selected: &Path, ordinary: &Path, held: &Path) {
    let bytes = original(false);
    let (legacy, legacy_budget) = funded(&bytes, 0);
    let legacy_charge = legacy_budget.used();
    assert_eq!(
        legacy.persist_new_held(held).err().unwrap().kind(),
        io::ErrorKind::OutOfMemory
    );
    assert!(!held.try_exists().unwrap());
    let (): () = legacy.persist_new(ordinary).unwrap();

    let (request, budget) = funded(&bytes, HeldOriginalFile::SAVE_BYTES);
    let original_charge = budget.used();
    let stream = held.with_extension("mar:other");
    assert_eq!(
        request.persist_new_held(&stream).err().unwrap().kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(!held.try_exists().unwrap());
    let guard = request.persist_new_held(held).unwrap();
    assert_eq!(
        guard.file_id(),
        &file_id(&File::open(held).unwrap()).unwrap()
    );
    assert_eq!(
        File::options()
            .write(true)
            .open(held)
            .unwrap_err()
            .raw_os_error(),
        Some(32)
    );
    drop(guard);
    File::options().write(true).open(held).unwrap();

    check_relative_directory_failure(selected, &ordinary.with_extension("failed"), &bytes, |p| {
        legacy.persist_new(p)
    });
    check_relative_directory_failure(selected, &held.with_extension("failed"), &bytes, |p| {
        request.persist_new_held(p)
    });
    assert_eq!(legacy.to_bytes(), bytes);
    assert_eq!(request.to_bytes(), bytes);
    assert_eq!(legacy_budget.used(), legacy_charge);
    assert_eq!(budget.used(), original_charge);
}

fn check_relative_directory_failure<T>(
    selected: &Path,
    path: &Path,
    bytes: &[u8],
    save: impl Fn(&Path) -> io::Result<T>,
) {
    // Deny the directory writer on the absolute fixture path. Both relative
    // spellings must reach this directory's barrier before acknowledging.
    let directory = File::options()
        .read(true)
        .share_mode(1)
        .custom_flags(0x02000000)
        .open(selected)
        .unwrap();
    assert_eq!(save(path).err().unwrap().raw_os_error(), Some(32));
    drop(directory);
    let id = file_id(&File::options().read(true).write(true).open(path).unwrap()).unwrap();
    assert_eq!(fs::read(path).unwrap(), bytes);
    assert_eq!(
        save(path).err().unwrap().kind(),
        io::ErrorKind::AlreadyExists
    );
    assert_eq!(file_id(&File::open(path).unwrap()).unwrap(), id);
    assert_eq!(fs::read(path).unwrap(), bytes);
}

#[test]
fn prepared_store_original_lends_identity_before_any_publication() {
    use crate::atomic::{AtomicAck, FinalChange};
    let fixture = Fixture::new();
    let store = crate::Store::open(&fixture.0.join("state"), crate::Config::default()).unwrap();
    let view = store.capture_namespace_funded("jobs", 65536).unwrap();
    let request = view
        .prepare(
            "allocate",
            AtomicAck::Flush,
            vec![
                FinalChange::put("allocator", vec![1]),
                FinalChange::put("ticket", vec![2]),
                FinalChange::put("native", vec![3]),
                FinalChange::put("launch", vec![4]),
            ],
        )
        .unwrap();
    drop(view);
    let path = fixture.0.join("prepared.mar");
    let held = request.persist_new_held(&path).unwrap();
    assert_eq!(
        &file_id(&File::open(&path).unwrap()).unwrap(),
        held.file_id()
    );
    assert_eq!(fs::read(path).unwrap(), request.to_bytes());
    assert!(store
        .capture_namespace_funded("jobs", 65536)
        .unwrap()
        .rows()
        .is_empty());
}

#[test]
fn same_writer_excludes_replacement_at_file_barrier_between_barriers_and_after_return() {
    let fixture = Fixture::new();
    let path = fixture.0.join("original");
    let other = fixture.0.join("same-bytes");
    let bytes = original(false);
    let (request, _budget) = funded(&bytes, HeldOriginalFile::SAVE_BYTES);
    fs::write(&other, &bytes).unwrap();
    let other_id = file_id(&File::open(&other).unwrap()).unwrap();
    let try_replace = || {
        assert!(matches!(
            fs::rename(&other, &path).unwrap_err().raw_os_error(),
            Some(5 | 32) // ACCESS_DENIED or SHARING_VIOLATION
        ));
        assert_eq!(
            File::options()
                .write(true)
                .open(&path)
                .unwrap_err()
                .raw_os_error(),
            Some(32)
        );
    };
    let mut at_barrier = None;
    let held = request
        .persist_new_held_with(&path, |writer| {
            at_barrier = Some((writer.as_raw_handle(), file_id(writer)?));
            try_replace();
            log_io::sync_file(writer)?;
            try_replace();
            Ok(())
        })
        .unwrap();
    let (handle, id) = at_barrier.unwrap();
    assert_eq!(held._writer.as_raw_handle(), handle);
    assert_eq!(*held.file_id(), id);
    assert_ne!(id, other_id);
    try_replace();
    drop(held);
    fs::rename(&other, &path).unwrap();
    assert_eq!(file_id(&File::open(&path).unwrap()).unwrap(), other_id);
}

#[test]
fn failed_file_or_directory_barrier_preserves_original_and_new_only_output() {
    let fixture = Fixture::new();
    let bytes = original(false);
    let (request, budget) = funded(&bytes, HeldOriginalFile::SAVE_BYTES);
    let original_charge = budget.used();
    let file_failure = fixture.0.join("file-barrier");
    let err =
        request.persist_new_held_with(&file_failure, |_| Err(io::Error::from_raw_os_error(1117)));
    assert_eq!(err.err().unwrap().raw_os_error(), Some(1117));

    // Deny the directory writer only after the real file barrier succeeds.
    // If directory sync were skipped or moved earlier, the save would succeed.
    let mut directory = None;
    let dir_failure = fixture.0.join("directory-barrier");
    let err = request.persist_new_held_with(&dir_failure, |writer| {
        log_io::sync_file(writer)?;
        directory = Some(
            File::options()
                .read(true)
                .share_mode(1)
                .custom_flags(0x02000000)
                .open(&fixture.0)?,
        );
        Ok(())
    });
    assert!(directory.is_some());
    assert_eq!(err.err().unwrap().raw_os_error(), Some(32));
    drop(directory);
    for path in [file_failure, dir_failure] {
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert_eq!(
            request.persist_new_held(&path).err().unwrap().kind(),
            io::ErrorKind::AlreadyExists
        );
        assert_eq!(request.to_bytes(), bytes);
        assert_eq!(budget.used(), original_charge);
    }
}

#[test]
fn partial_write_preserves_the_written_prefix_and_the_prepared_request() {
    struct LimitedWriter(File, usize);
    impl Write for LimitedWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.1 == 0 {
                return Err(io::Error::from_raw_os_error(112)); // ERROR_DISK_FULL
            }
            let written = self.0.write(&bytes[..bytes.len().min(self.1)])?;
            self.1 -= written;
            Ok(written)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.0.flush()
        }
    }
    let fixture = Fixture::new();
    let bytes = original(false);
    let request = AtomicRequest::from_bytes(&bytes).unwrap();
    let path = fixture.0.join("partial");
    let mut writer = LimitedWriter(File::create_new(&path).unwrap(), 257);
    assert_eq!(
        request
            .write_original(&mut writer)
            .unwrap_err()
            .raw_os_error(),
        Some(112)
    );
    drop(writer);
    assert_eq!(fs::read(path).unwrap(), bytes[..257]);
    assert_eq!(request.to_bytes(), bytes);
}

#[test]
fn named_stream_does_not_issue_a_file_identity_for_another_stream() {
    let fixture = Fixture::new();
    let path = fixture.0.join("primary");
    fs::write(&path, b"prior").unwrap();
    let request = AtomicRequest::from_bytes(&original(false)).unwrap();
    let result = request.persist_new_held(&fixture.0.join("primary:other"));
    assert_eq!(result.err().unwrap().kind(), io::ErrorKind::InvalidInput);
    assert_eq!(fs::read(path).unwrap(), b"prior");
}
