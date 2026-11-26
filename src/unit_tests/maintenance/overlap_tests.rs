//! Retained private specimens for overlapping retirement and once-only DATA discharge.
use super::*;
use crate::namespace_reserve::{ReserveError, ReserveRequest};
use crate::{Mutation, Outcome, Store};
use std::path::Path;
use std::sync::{mpsc, Condvar};

struct Gate {
    entered: mpsc::Receiver<()>,
    released: Arc<(Mutex<bool>, Condvar)>,
}
impl Gate {
    fn new() -> (Self, Arc<dyn Fn() + Send + Sync>) {
        let (send, entered) = mpsc::channel();
        let released = Arc::new((Mutex::new(false), Condvar::new()));
        let wait = released.clone();
        (
            Self { entered, released },
            Arc::new(move || {
                send.send(()).unwrap();
                let _guard = wait
                    .1
                    .wait_while(wait.0.lock().unwrap(), |released| !*released)
                    .unwrap();
            }),
        )
    }
    fn release(&self) {
        *self.released.0.lock().unwrap() = true;
        self.released.1.notify_all();
    }
}
impl Drop for Gate {
    fn drop(&mut self) {
        self.release();
    }
}
fn root(label: &str) -> PathBuf {
    let base = PathBuf::from(
        std::env::var_os("MEMBUS_NAMESPACE_RESERVE_TEST_ROOT")
            .expect("explicit retained fixture root"),
    );
    assert!(base.is_absolute() && base.is_dir());
    let p = base.join(format!(
        "retirement-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&p).unwrap();
    println!("retained retirement specimen {}", p.display());
    p
}
fn config() -> Config {
    Config {
        cache_bytes: 0,
        signal_bytes: 0,
        flush_ms: 0,
        chunk_bytes: 1,
        tick_ms: 3_600_000,
        retire_ms: 3_600_000,
        retry_ms: 10,
        state_limit_bytes: 4096,
        ..Config::default()
    }
}
fn mutation(id: &str, bytes: usize) -> Mutation {
    Mutation {
        ns: "retained".into(),
        key: "key".into(),
        op_id: id.into(),
        deadline: u64::MAX,
        ack: 2,
        expected: None,
        bytes: vec![id.as_bytes()[0]; bytes],
        remove: false,
    }
}
fn seed(
    path: &Path,
    config: Config,
    old_count: usize,
) -> (Store, Vec<chunks::ChunkInfo>, Mutation, Outcome) {
    let clock = config.clock.clone().unwrap();
    let s = Store::open(path, config).unwrap();
    for n in 0..old_count {
        assert_eq!(s.mutate(mutation(&format!("old-{n}"), 64)).code, 0);
    }
    clock.advance(20);
    let current = mutation("current", 512);
    let receipt = s.mutate(current.clone());
    assert_eq!((receipt.code, receipt.ack), (0, 2));
    let old = s
        .chunks
        .lock()
        .unwrap()
        .table
        .values()
        .filter(|c| c.state == chunks::ChunkState::Closed)
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(old.len(), old_count);
    (s, old, current, receipt)
}
fn data_bytes(s: &Store) -> u64 {
    chunks::list_chunk_files(&s.path.join("chunks"))
        .unwrap()
        .iter()
        .map(|(_, _, _, p)| fs::read(p).unwrap().len() as u64)
        .sum()
}
fn retired(s: &Store, id: u64) -> usize {
    chunks::read_catalogue(&s.path.join("chunks/catalogue.log"))
        .unwrap()
        .0
        .iter()
        .filter(|r| matches!(r.rec, chunks::Rec::Retired { seq } if seq == id))
        .count()
}
fn retained(s: &Store, current: &Mutation, receipt: &Outcome) {
    assert_eq!(
        s.mutate(current.clone()),
        *receipt,
        "original retry outcome remains identical"
    );
    let value = s
        .try_get("retained", "key")
        .expect("fallible retained read")
        .expect("current value");
    assert_eq!(
        (value.version, value.ack, value.bytes),
        (receipt.version, 2, current.bytes.clone())
    );
}
fn request(bytes: u64) -> ReserveRequest {
    ReserveRequest {
        bytes,
        appends: 1,
        lifetime_ms: 60_000,
    }
}

fn overlap(prune: bool) {
    let p = root(if prune { "pruned-overlap" } else { "overlap" });
    let (gate, hook) = Gate::new();
    let clock = chunks::ManualClock::new(1_000_000);
    let (s, old, current, receipt) = seed(
        &p,
        Config {
            clock: Some(clock),
            retirement_started: Some(hook),
            ..config()
        },
        1,
    );
    let c = &old[0];
    let original_chunk = fs::read(&c.path).unwrap();
    fs::write(p.join("control-retiring-chunk.mbk1"), &original_chunk).unwrap();
    println!(
        "retiring input bytes={} sha256={}",
        original_chunk.len(),
        chunks::sha256_file(&c.path).unwrap()
    );
    let before = data_bytes(&s);
    assert_eq!(s.state_bytes(), before);
    let lease = s.reserve_namespace("guarded", request(64)).unwrap();
    let status = lease.status().unwrap();
    let (retry, first) = std::thread::scope(|scope| {
        let job = scope.spawn(|| s.retire_chunk(c.id));
        gate.entered.recv().unwrap(); // actual public call released both preparation locks
        let release_on_unwind = &gate;
        struct Release<'a>(&'a Gate);
        impl Drop for Release<'_> {
            fn drop(&mut self) {
                self.0.release();
            }
        }
        let _release = Release(release_on_unwind);
        assert_eq!(
            s.chunks.lock().unwrap().table[&c.id].state,
            chunks::ChunkState::Retiring
        );
        assert!(c.path.exists());
        let retry = s.retry_retiring(); // supported completion of the SAME obligation
        if prune {
            s.maint.prune_discharged();
        }
        gate.release();
        (retry, job.join())
    });
    let physical = data_bytes(&s);
    let charged = s.state_bytes();
    let markers = retired(&s, c.id);
    let first = first
        .expect("stale retirement must not panic after completion/pruning")
        .unwrap();
    let excess = s.config.state_limit_bytes - physical - 64 + 1;
    let overgrant = s.reserve_namespace("excess", request(excess));
    println!("overlap prune={prune} before={before} chunk={} physical={physical} charged={charged} markers={markers} first={first} retry={retry:?} excess={excess} overgrant={overgrant:?}", c.bytes);
    println!(
        "completed catalogue sha256={}",
        chunks::sha256_file(&p.join("chunks/catalogue.log")).unwrap()
    );
    assert_eq!(retry.unwrap(), 1);
    assert_eq!(
        physical,
        before - c.bytes,
        "only the intended whole file was deleted"
    );
    assert_eq!(
        charged, physical,
        "one obligation must discharge DATA charge exactly once"
    );
    assert!(
        !first,
        "the original caller must not claim the retry's completion"
    );
    assert_eq!(
        markers, 1,
        "one completed obligation publishes one Retired marker"
    );
    assert_eq!(overgrant.unwrap_err(), ReserveError::Resource);
    assert_eq!(lease.status().unwrap(), status);
    retained(&s, &current, &receipt);
    let mut reopen_config = s.config.clone();
    reopen_config.retirement_started = None;
    drop(lease);
    drop(s);
    let reopened = Store::open(&p, reopen_config).unwrap();
    assert_eq!(reopened.state_bytes(), physical);
    retained(&reopened, &current, &receipt);
}

#[test]
fn retry_completing_prepared_retirement_discharges_once() {
    overlap(false);
}

#[test]
fn completed_and_pruned_retirement_is_not_reused_by_stale_caller() {
    overlap(true);
}

#[test]
fn distinct_prepared_chunks_each_discharge_their_own_obligation() {
    let p = root("distinct");
    let (gate, hook) = Gate::new();
    let (s, old, current, receipt) = seed(
        &p,
        Config {
            clock: Some(chunks::ManualClock::new(1_000_000)),
            retirement_started: Some(hook),
            ..config()
        },
        2,
    );
    let before = data_bytes(&s);
    let results = std::thread::scope(|scope| {
        struct Release<'a>(&'a Gate);
        impl Drop for Release<'_> {
            fn drop(&mut self) {
                self.0.release();
            }
        }
        let _release = Release(&gate);
        let a = scope.spawn(|| s.retire_chunk(old[0].id));
        let b = scope.spawn(|| s.retire_chunk(old[1].id));
        gate.entered.recv().unwrap();
        gate.entered.recv().unwrap(); // both distinct public calls are prepared together
        assert!(old.iter().all(|c| c.path.exists()));
        assert_eq!(s.state_bytes(), before);
        gate.release();
        (a.join().unwrap().unwrap(), b.join().unwrap().unwrap())
    });
    assert_eq!(results, (true, true));
    assert_eq!(
        s.state_bytes(),
        before - old.iter().map(|c| c.bytes).sum::<u64>()
    );
    assert_eq!(s.state_bytes(), data_bytes(&s));
    assert!(old.iter().all(|c| retired(&s, c.id) == 1));
    assert_eq!(s.retry_retiring().unwrap(), 0);
    retained(&s, &current, &receipt);
}

#[test]
fn failed_retry_leaves_the_prepared_caller_owning_the_unfinished_obligation() {
    use std::sync::atomic::AtomicBool;
    let p = root("failed-overlap");
    let refuse = Arc::new(AtomicBool::new(false));
    let r = refuse.clone();
    let (gate, hook) = Gate::new();
    let (s, old, current, receipt) = seed(
        &p,
        Config {
            clock: Some(chunks::ManualClock::new(1_000_000)),
            retirement_started: Some(hook),
            chunk_dir_sync: Some(Arc::new(move |_| {
                if r.load(Ordering::SeqCst) {
                    Err(io::Error::other("authored overlapping retry refusal"))
                } else {
                    Ok(())
                }
            })),
            ..config()
        },
        1,
    );
    let before = data_bytes(&s);
    let result = std::thread::scope(|scope| {
        struct Release<'a>(&'a Gate);
        impl Drop for Release<'_> {
            fn drop(&mut self) {
                self.0.release();
            }
        }
        let _release = Release(&gate);
        let job = scope.spawn(|| s.retire_chunk(old[0].id));
        gate.entered.recv().unwrap();
        refuse.store(true, Ordering::SeqCst);
        assert!(s.retry_retiring().is_err());
        assert!(!old[0].path.exists());
        assert_eq!(s.state_bytes(), before);
        assert_eq!(retired(&s, old[0].id), 0);
        refuse.store(false, Ordering::SeqCst);
        gate.release();
        job.join().unwrap().unwrap()
    });
    assert!(result, "Retiring remains completable after a failed retry");
    assert_eq!(s.state_bytes(), before - old[0].bytes);
    assert_eq!(s.state_bytes(), data_bytes(&s));
    assert_eq!(retired(&s, old[0].id), 1);
    assert_eq!(s.retry_retiring().unwrap(), 0);
    retained(&s, &current, &receipt);
}

#[test]
fn missing_after_owned_delete_retains_charge_until_retry_or_startup_barriers() {
    for (stop, restart) in [(2, false), (1, true), (2, true)] {
        let p = root("stopped");
        let (s, old, current, receipt) = seed(
            &p,
            Config {
                clock: Some(chunks::ManualClock::new(1_000_000)),
                retire_stop_after: stop,
                ..config()
            },
            1,
        );
        let before = data_bytes(&s);
        assert!(!s.retire_chunk(old[0].id).unwrap());
        assert_eq!(old[0].path.exists(), stop == 1);
        assert_eq!(retired(&s, old[0].id), 0);
        assert_eq!(
            s.state_bytes(),
            before,
            "a stopped phase has not discharged DATA"
        );
        let s = if restart {
            let config = s.config.clone();
            drop(s);
            let reopened = Store::open(&p, config).unwrap();
            assert_eq!(
                reopened.replay_report().retirements_finished,
                vec![old[0].id]
            );
            reopened
        } else {
            assert_eq!(s.retry_retiring().unwrap(), 1);
            s
        };
        assert_eq!(retired(&s, old[0].id), 1);
        assert_eq!(s.state_bytes(), before - old[0].bytes);
        assert_eq!(s.state_bytes(), data_bytes(&s));
        retained(&s, &current, &receipt);
    }
}

#[cfg(windows)]
#[test]
fn held_delete_is_still_retiring_and_charged_until_actual_release() {
    use std::os::windows::fs::OpenOptionsExt;
    let p = root("held");
    let (s, old, current, receipt) = seed(
        &p,
        Config {
            clock: Some(chunks::ManualClock::new(1_000_000)),
            ..config()
        },
        1,
    );
    let before = data_bytes(&s);
    let held = fs::OpenOptions::new()
        .read(true)
        .share_mode(3)
        .open(&old[0].path)
        .unwrap();
    assert!(!s.retire_chunk(old[0].id).unwrap());
    assert_eq!(s.retry_retiring().unwrap(), 0);
    assert_eq!(retired(&s, old[0].id), 0);
    assert_eq!(s.retire_blocked(), 1);
    assert_eq!(s.state_bytes(), before);
    assert_eq!(data_bytes(&s), before);
    retained(&s, &current, &receipt);
    drop(held);
    assert_eq!(s.retry_retiring().unwrap(), 1);
    assert_eq!(s.state_bytes(), before - old[0].bytes);
    assert_eq!(s.state_bytes(), data_bytes(&s));
    assert_eq!(s.retire_blocked(), 0);
    assert_eq!(retired(&s, old[0].id), 1);
}

#[test]
fn failed_completion_barriers_keep_charge_until_owned_recovery_and_retry() {
    use std::sync::atomic::{AtomicBool, AtomicUsize};
    for mode in ["directory", "catalogue", "uncertain-catalogue"] {
        let p = root(mode);
        let armed = Arc::new(AtomicBool::new(false));
        let refuse = Arc::new(AtomicBool::new(mode == "directory"));
        let barriers = Arc::new(AtomicUsize::new(0));
        let (a, r, b, cat) = (
            armed.clone(),
            refuse.clone(),
            barriers.clone(),
            p.join("chunks/catalogue.log"),
        );
        let (gate, hook) = Gate::new();
        let (s, old, current, receipt) = seed(
            &p,
            Config {
                clock: Some(chunks::ManualClock::new(1_000_000)),
                retirement_started: Some(hook),
                chunk_dir_sync: Some(Arc::new(move |_| {
                    if a.load(Ordering::SeqCst) {
                        b.fetch_add(1, Ordering::SeqCst);
                        let records = chunks::read_catalogue(&cat)?.0;
                        assert!(
                            !records
                                .iter()
                                .any(|x| matches!(x.rec, chunks::Rec::Retired { .. })),
                            "the directory barrier precedes Retired publication"
                        );
                        if r.load(Ordering::SeqCst) {
                            return Err(io::Error::other("authored directory refusal"));
                        }
                    }
                    Ok(())
                })),
                ..config()
            },
            1,
        );
        let before = data_bytes(&s);
        let result = std::thread::scope(|scope| {
            struct Release<'a>(&'a Gate);
            impl Drop for Release<'_> {
                fn drop(&mut self) {
                    self.0.release();
                }
            }
            let _release = Release(&gate);
            let job = scope.spawn(|| s.retire_chunk(old[0].id));
            gate.entered.recv().unwrap();
            armed.store(true, Ordering::SeqCst);
            let mut cs = s.chunks.lock().unwrap();
            let cat = cs.cat.as_mut().unwrap();
            if mode == "catalogue" {
                cat.fail_writes = true;
            }
            if mode == "uncertain-catalogue" {
                cat.fail_sync_once = true;
                cat.fail_recovery_once = true;
            }
            drop(cs);
            gate.release();
            job.join().unwrap()
        });
        let error = result.unwrap_err();
        println!("completion mode={mode} error={error} before={before} physical={} charged={} barriers={}",
            data_bytes(&s), s.state_bytes(), barriers.load(Ordering::SeqCst));
        assert!(!old[0].path.exists());
        assert_eq!(barriers.load(Ordering::SeqCst), 1);
        assert!(s.storage_error());
        assert_eq!(
            s.state_bytes(),
            before,
            "failed/uncertain publication must not release charge"
        );
        assert_eq!(
            s.chunks.lock().unwrap().table[&old[0].id].state,
            chunks::ChunkState::Retiring
        );
        retained(&s, &current, &receipt);
        refuse.store(false, Ordering::SeqCst);
        armed.store(false, Ordering::SeqCst);
        s.chunks.lock().unwrap().cat.as_mut().unwrap().fail_writes = false;
        if mode == "uncertain-catalogue" {
            assert!(
                s.retry_retiring().is_err(),
                "uncertain catalogue remains fenced"
            );
            assert_eq!(s.state_bytes(), before);
            assert!(matches!(
                s.maintain_catalogue().unwrap(),
                CatalogueMaintenance::Recovered { .. }
            ));
        } else {
            assert_eq!(retired(&s, old[0].id), 0);
        }
        assert_eq!(s.retry_retiring().unwrap(), 1);
        assert_eq!(s.state_bytes(), before - old[0].bytes);
        assert_eq!(s.state_bytes(), data_bytes(&s));
        assert_eq!(retired(&s, old[0].id), 1);
        assert_eq!(s.retry_retiring().unwrap(), 0);
        assert_eq!(s.state_bytes(), data_bytes(&s));
        assert!(
            s.storage_error(),
            "successful retry preserves sticky failure evidence"
        );
        retained(&s, &current, &receipt);
    }
}
