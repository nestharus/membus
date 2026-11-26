//! Retirement cannot publish reclaimed bytes before the removed names are synced.
use super::*;
use crate::{Mutation, Store};
use std::sync::atomic::{AtomicBool, AtomicUsize};

static NEXT: AtomicUsize = AtomicUsize::new(0);
#[derive(Clone, Debug)]
struct Observation { retired: Vec<u64>, missing: usize }
struct Fixture {
    path: PathBuf,
    armed: Arc<AtomicBool>,
    fail_at: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<Observation>>>,
}
impl Fixture {
    fn new() -> Self {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".test-output")
            .join(format!("retirement-directories-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::SeqCst)));
        fs::create_dir_all(&path).unwrap();
        Self { path, armed: Arc::new(AtomicBool::new(false)), fail_at: Arc::new(AtomicUsize::new(0)),
            seen: Arc::new(Mutex::new(Vec::new())) }
    }
    fn seed(&self, stop: u8) -> (Store, Vec<(u64, PathBuf, u64)>) {
        let root = self.path.join("chunks");
        let armed = self.armed.clone();
        let fail_at = self.fail_at.clone();
        let seen = self.seen.clone();
        let clock = chunks::ManualClock::new(1_000_000);
        let config = Config {
            clock: Some(clock.clone()), retry_ms: 10, chunk_bytes: 1,
            tick_ms: 3_600_000, retire_ms: 3_600_000, retire_stop_after: stop,
            chunk_dir_sync: Some(Arc::new(move |dir| {
                if !armed.load(Ordering::SeqCst) { return Ok(()); }
                assert_eq!(dir, root.join("latest"));
                let records = chunks::read_catalogue(&root.join("catalogue.log"))?.0;
                assert!(records.iter().any(|r| matches!(r.rec, chunks::Rec::Retiring { .. })),
                    "the retirement intent must precede deletion");
                let retired = records.iter().filter_map(|r| match r.rec {
                    chunks::Rec::Retired { seq } => Some(seq), _ => None,
                }).collect();
                let files = chunks::list_chunk_files(&root)?;
                let missing = 3 - files.len();
                assert!(missing > 0, "deletion must precede the directory barrier");
                assert!(files.iter().any(|(_, _, id, p)| *id == 3 && fs::metadata(p).is_ok_and(|m| m.len() > 0)),
                    "the latest flushed value must stay retained");
                let mut seen = seen.lock().unwrap();
                seen.push(Observation { retired, missing });
                if seen.len() == fail_at.load(Ordering::SeqCst) {
                    Err(io::Error::from_raw_os_error(5))
                } else { Ok(()) }
            })),
            ..Config::default()
        };
        let store = Store::open(&self.path, config).unwrap();
        store.policies.write().unwrap().set("caller", "", Some(chunks::Class::LATEST));
        for n in 1..=3 {
            let request = Mutation { ns: "caller".into(), key: "key".into(), op_id: format!("op-{n}"),
                deadline: u64::MAX, ack: 2, expected: None, bytes: vec![n], remove: false };
            assert_eq!(store.mutate(request).code, 0);
        }
        let old = store.chunks.lock().unwrap().table.values()
            .filter(|c| c.state == chunks::ChunkState::Closed)
            .map(|c| (c.id, c.path.clone(), c.bytes)).collect::<Vec<_>>();
        assert_eq!(old.len(), 2);
        clock.advance(20);
        (store, old)
    }
    fn retired(&self) -> Vec<u64> {
        chunks::read_catalogue(&self.path.join("chunks/catalogue.log")).unwrap().0.iter()
            .filter_map(|r| match r.rec { chunks::Rec::Retired { seq } => Some(seq), _ => None }).collect()
    }
    fn check_bytes(&self, store: &Store, before: u64, old: &[(u64, PathBuf, u64)]) {
        let retired = self.retired();
        let freed: u64 = old.iter().filter(|(id, _, _)| retired.contains(id)).map(|(_, _, b)| *b).sum();
        assert_eq!(store.state.lock().unwrap().persisted_bytes, before - freed);
        assert_eq!(store.get("caller", "key").unwrap().bytes, vec![3]);
    }
}
impl Drop for Fixture {
    fn drop(&mut self) { fs::remove_dir_all(&self.path).unwrap(); }
}

#[test]
fn retirement_directory_failure_keeps_only_prior_successful_reclamation() {
    for stop in 1..=2 {
        let f = Fixture::new();
        let (store, old) = f.seed(0);
        let before = store.state.lock().unwrap().persisted_bytes;
        f.fail_at.store(stop, Ordering::SeqCst);
        f.armed.store(true, Ordering::SeqCst);
        let error = store.maint.retire(None).expect_err("failed deletion sync must refuse retirement publication");
        assert_eq!(error.raw_os_error(), Some(5));
        assert_eq!(f.seen.lock().unwrap().len(), stop);
        for (index, observation) in f.seen.lock().unwrap().iter().enumerate() {
            assert_eq!((observation.missing, observation.retired.len()), (index + 1, index));
        }
        assert_eq!(f.retired().len(), stop - 1);
        assert!(store.storage_error());
        f.check_bytes(&store, before, &old);
        f.fail_at.store(0, Ordering::SeqCst);
        assert_eq!(store.retry_retiring().unwrap(), 3 - stop);
        assert_eq!(f.seen.lock().unwrap().len(), 3, "retry the absent file's directory barrier too");
        let last = f.seen.lock().unwrap().last().unwrap().clone();
        assert_eq!((last.missing, last.retired.len()), (2, 1));
        assert_eq!(f.retired().len(), 2);
        assert!(store.storage_error(), "successful retry must not erase the sticky failure observation");
        f.check_bytes(&store, before, &old);
    }
}

#[test]
fn retry_retirement_directory_failure_preserves_budget_until_success() {
    for stop in 1..=2 {
        let f = Fixture::new();
        let (store, old) = f.seed(1);
        let before = store.state.lock().unwrap().persisted_bytes;
        assert!(store.maint.retire(None).unwrap().is_empty());
        assert!(old.iter().all(|(_, p, _)| p.exists()));
        f.fail_at.store(stop, Ordering::SeqCst);
        f.armed.store(true, Ordering::SeqCst);
        let error = store.retry_retiring().expect_err("failed retry deletion sync must retain retirement responsibility");
        assert_eq!(error.raw_os_error(), Some(5));
        assert_eq!(f.retired().len(), stop - 1);
        assert_eq!(f.seen.lock().unwrap().len(), stop);
        f.check_bytes(&store, before, &old);
        f.fail_at.store(0, Ordering::SeqCst);
        assert_eq!(store.retry_retiring().unwrap(), 3 - stop);
        assert_eq!(f.retired().len(), 2);
        f.check_bytes(&store, before, &old);
    }
}

#[cfg(windows)]
#[test]
fn held_retirement_delete_never_reaches_a_directory_barrier() {
    use std::os::windows::fs::OpenOptionsExt;
    let f = Fixture::new();
    let (store, old) = f.seed(0);
    let held: Vec<_> = old.iter().map(|(_, p, _)| std::fs::OpenOptions::new().read(true).share_mode(3).open(p).unwrap()).collect();
    f.armed.store(true, Ordering::SeqCst);
    assert!(store.maint.retire(None).unwrap().is_empty());
    assert!(f.seen.lock().unwrap().is_empty());
    assert!(f.retired().is_empty());
    assert!(old.iter().all(|(_, p, _)| p.exists()));
    assert_eq!(store.retire_blocked(), 2);
    assert_eq!(store.get("caller", "key").unwrap().bytes, vec![3]);
    drop(held);
}
