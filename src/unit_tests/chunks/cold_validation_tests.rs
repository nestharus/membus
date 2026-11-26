use crate::{Config, Mutation, Store};
use std::{
    cell::RefCell,
    io::{Seek, SeekFrom, Write},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, Arc,
    },
    time::Duration,
};

thread_local! {
    static BEFORE_VALIDATION: RefCell<Option<Box<dyn FnOnce()>>> = RefCell::new(None);
}
pub(super) fn before_validation() {
    let hook = BEFORE_VALIDATION.with(|slot| slot.borrow_mut().take());
    if let Some(hook) = hook {
        hook();
    }
}

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let base = PathBuf::from(
            std::env::var_os("MEMBUS_TEST_OUTPUT").unwrap_or_else(|| ".test-output".into()),
        );
        std::fs::create_dir_all(&base).unwrap();
        Self(std::fs::canonicalize(base).unwrap().join(format!(
            "cold-validation-{}-{}-{}",
            std::process::id(),
            crate::now_ms(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}

fn shared_cursor_progress(corrupt: bool) {
    let fixture = Fixture::new();
    let store = Arc::new(Store::open(&fixture.0, Config::default()).unwrap());
    let keys: Vec<_> = (0..100)
        .map(|i| format!("part/{i}"))
        .filter(|key| crate::routing::stripe("ledger", key) == 0)
        .take(2)
        .collect();
    let mut versions = Vec::new();
    for (i, key) in keys.iter().enumerate() {
        let outcome = store.mutate(Mutation {
            ns: "ledger".into(),
            key: key.clone(),
            op_id: format!("seed{i}"),
            deadline: crate::now_ms() + 30_000,
            ack: 1,
            expected: None,
            bytes: vec![i as u8 + 1; 32 * 1024],
            remove: false,
        });
        assert_eq!(outcome.code, 0);
        store.cache.remove(outcome.version, false);
        versions.push(outcome.version);
    }
    let entries: Vec<_> = {
        let committed = store.committed.read().unwrap();
        keys.iter()
            .map(|key| committed[&("ledger".into(), key.clone())].clone())
            .collect()
    };
    assert!(Arc::ptr_eq(
        &entries[0].location.file,
        &entries[1].location.file
    ));
    assert_ne!(entries[0].location.offset, entries[1].location.offset);
    let mut saved_byte = [0];
    let mut repair = if corrupt {
        use std::io::Read;
        let row = store
            .chunk_rows()
            .into_iter()
            .find(|row| row.1 == 0)
            .unwrap();
        let path = super::chunk_path(&fixture.0.join("chunks"), entries[0].class, row.1, row.0);
        // Locate the authored chunk by its public catalogue identity.
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .unwrap();
        let offset = entries[0].location.offset + entries[0].location.record_len - 1;
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.read_exact(&mut saved_byte).unwrap();
        file.seek(SeekFrom::Start(offset)).unwrap();
        file.write_all(&[saved_byte[0] ^ 1]).unwrap();
        Some((file, offset))
    } else {
        None
    };
    let (captured_tx, captured_rx) = mpsc::channel();
    let (resume_tx, resume_rx) = mpsc::channel();
    let first = {
        let store = store.clone();
        let key = keys[0].clone();
        std::thread::spawn(move || {
            BEFORE_VALIDATION.with(|slot| {
                *slot.borrow_mut() = Some(Box::new(move || {
                    captured_tx.send(()).unwrap();
                    resume_rx.recv().unwrap();
                }))
            });
            store.get_handle_result("ledger", &key)
        })
    };
    let captured = captured_rx.recv_timeout(Duration::from_secs(10)).is_ok();
    let (second_tx, second_rx) = mpsc::channel();
    let second = {
        let store = store.clone();
        let key = keys[1].clone();
        std::thread::spawn(move || {
            let value = store.get_handle_result("ledger", &key);
            second_tx.send(()).unwrap();
            value
        })
    };
    let independent_progress = second_rx.recv_timeout(Duration::from_secs(5)).is_ok();
    let before_resume = store.cache_stats();
    resume_tx.send(()).unwrap();
    // Release and join both readers before any assertion about progress.
    let first_value = first.join().unwrap();
    let second_value = second.join().unwrap().unwrap().unwrap();
    assert_eq!(second_value.version, versions[1]);
    assert_eq!(second_value.bytes, vec![2; 32 * 1024]);
    if corrupt {
        assert!(first_value.is_err());
        assert!(store.storage_error());
        assert!(store.cache.after_miss(versions[0]).is_none());
        let (file, offset) = repair.as_mut().unwrap();
        file.seek(SeekFrom::Start(*offset)).unwrap();
        file.write_all(&saved_byte).unwrap();
        let retried = store
            .get_handle_result("ledger", &keys[0])
            .unwrap()
            .unwrap();
        assert_eq!(retried.version, versions[0]);
        assert_eq!(retried.bytes, vec![1; 32 * 1024]);
    } else {
        let first_value = first_value.unwrap().unwrap();
        assert_eq!(first_value.version, versions[0]);
        assert_eq!(first_value.bytes, vec![1; 32 * 1024]);
    }
    let after = store.cache_stats();
    assert_eq!(after.cold_reads, if corrupt { 3 } else { 2 });
    assert_eq!(after.entries, 2);
    assert!(after.bytes <= after.limit);
    assert!(
        captured,
        "first read must reach the captured-frame validation boundary"
    );
    assert!(
        independent_progress,
        "a different record on the same cursor must finish while checksum validation is paused"
    );
    assert_eq!(
        before_resume.entries, 1,
        "only the validated second record may be cached"
    );
    drop(repair);
    drop(entries);
    drop(store);
}

#[test]
fn cold_cursor_validating_one_frame_does_not_block_another_record() {
    shared_cursor_progress(false);
}
#[test]
fn cold_cursor_corrupt_frame_remains_uncached_and_retryable() {
    shared_cursor_progress(true);
}
