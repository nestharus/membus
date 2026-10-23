//! Live chunk-name barriers before routing, append and successful Flush.
use super::*;
use crate::{Mutation, Store};
use std::{path::PathBuf, sync::atomic::AtomicUsize};

static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".test-output")
            .join(format!("live-directories-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::SeqCst)));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }
    fn unlock(&self) {
        #[cfg(not(windows))]
        fs::remove_file(self.0.join("owner.lock")).unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) { fs::remove_dir_all(&self.0).unwrap(); }
}
fn config() -> Config {
    Config { clock: Some(chunks::ManualClock::new(1_000_000)), tick_ms: 3_600_000,
        retire_ms: 3_600_000, rotation_ms: 3_600_000, ..Config::default() }
}
fn mutation(id: &str, bytes: &[u8]) -> Mutation {
    Mutation { ns: "caller".into(), key: "key".into(), op_id: id.into(), deadline: u64::MAX,
        ack: 2, expected: None, bytes: bytes.to_vec(), remove: false }
}

#[test]
fn new_chunk_directory_barriers_precede_the_route_and_first_frame() {
    for class in [Class::FOREVER, Class::LATEST, Class::expire(86_400_000)] {
        let f = Fixture::new();
        let root = f.0.join("chunks");
        let router = Arc::new(Mutex::new(None::<std::sync::Weak<ChunkRouter>>));
        let route = router.clone();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let observed = seen.clone();
        let cfg = Config {
            chunk_dir_sync: Some(Arc::new(move |dir| {
                observed.lock().unwrap().push(dir.to_owned());
                let records = chunks::read_catalogue(&root.join("catalogue.log"))?.0;
                let id = records.iter().find_map(|r| match &r.rec {
                    chunks::Rec::Open { seq, class: c, .. } if *c == class => Some(*seq), _ => None,
                }).expect("the Open record must already be flushed");
                let file = chunks::chunk_path(&root, class, stripe("caller", "key"), id);
                assert_eq!(fs::metadata(file)?.len(), 0, "no first frame before both name barriers");
                let route = route.lock().unwrap().as_ref().unwrap().upgrade().unwrap();
                assert!(route.sink(id as usize).is_none(), "a flushable route must not precede its name barriers");
                assert!(route.locations.lock().unwrap().is_empty());
                Ok(())
            })),
            ..config()
        };
        let store = Store::open(&f.0, cfg.clone()).unwrap();
        *router.lock().unwrap() = Some(Arc::downgrade(&store.router));
        store.policies.write().unwrap().set("caller", "", Some(class));
        assert_eq!(store.mutate(mutation("first", b"value")).code, 0);
        assert_eq!(*seen.lock().unwrap(), vec![f.0.join("chunks").join(class.dir()), f.0.join("chunks")]);
        assert_eq!(store.get("caller", "key").unwrap().bytes, b"value");
    }
}

#[test]
fn each_chunk_directory_failure_fences_writes_and_restart_can_retry() {
    for stop in 1..=2 {
        let f = Fixture::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let observed = calls.clone();
        let mut cfg = config();
        cfg.chunk_dir_sync = Some(Arc::new(move |_| {
            if observed.fetch_add(1, Ordering::SeqCst) + 1 == stop {
                Err(io::Error::from_raw_os_error(5))
            } else { Ok(()) }
        }));
        let store = Store::open(&f.0, cfg.clone()).unwrap();
        let request = mutation("refused", b"value");
        assert_eq!(store.mutate(request.clone()).code, 16, "no successful receipt after a name-barrier failure");
        assert_eq!(calls.load(Ordering::SeqCst), stop);
        assert!(store.get("caller", "key").is_none());
        assert!(store.health2().read_only);
        assert!(store.router.table.read().unwrap().is_empty());
        assert!(store.chunks.lock().unwrap().table.is_empty());
        let files = chunks::list_chunk_files(&f.0.join("chunks")).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(fs::metadata(&files[0].3).unwrap().len(), 0);
        assert_ne!(store.mutate(mutation("later", b"later")).code, 0);
        assert_eq!(calls.load(Ordering::SeqCst), stop, "a fence must stop later opens");
        drop(store);
        f.unlock();
        cfg.chunk_dir_sync = None;
        let store = Store::open(&f.0, cfg).unwrap();
        assert!(store.get("caller", "key").is_none(), "an empty interrupted Open is not a committed mutation");
        let outcome = store.mutate(request.clone());
        assert_eq!((outcome.code, outcome.ack), (0, 2));
        assert!(outcome.version > 1, "do not reuse the reserved version after restart");
        assert_eq!(store.mutate(request), outcome);
        assert_eq!(store.get("caller", "key").unwrap().bytes, b"value");
        assert!(store.chunks.lock().unwrap().table.keys().any(|id| *id > files[0].2));
    }
}

#[test]
fn rotation_repeats_directory_barriers_for_an_existing_class() {
    let f = Fixture::new();
    let root = f.0.join("chunks");
    let calls = Arc::new(Mutex::new(Vec::new()));
    let observed = calls.clone();
    let cfg = Config {
        chunk_bytes: 1,
        chunk_dir_sync: Some(Arc::new(move |dir| {
            observed.lock().unwrap().push(dir.to_owned());
            let records = chunks::read_catalogue(&root.join("catalogue.log"))?.0;
            let id = records.iter().filter_map(|r| match r.rec {
                chunks::Rec::Open { seq, .. } => Some(seq), _ => None,
            }).max().unwrap();
            assert_eq!(fs::metadata(chunks::chunk_path(&root, Class::FOREVER, stripe("caller", "key"), id))?.len(), 0);
            Ok(())
        })),
        ..config()
    };
    let store = Store::open(&f.0, cfg.clone()).unwrap();
    assert_eq!(store.mutate(mutation("one", b"one")).code, 0);
    assert_eq!(store.mutate(mutation("two", b"two")).code, 0);
    let pair = vec![f.0.join("chunks/forever"), f.0.join("chunks")];
    assert_eq!(*calls.lock().unwrap(), [pair.clone(), pair].concat());
    drop(store);
    f.unlock();
    let store = Store::open(&f.0, cfg).unwrap();
    assert_eq!(store.get("caller", "key").unwrap().bytes, b"two");
    assert_eq!(store.mutate(mutation("one", b"one")).version, 1);
}

#[test]
fn chunk_directory_failures_preserve_the_os_error_and_reserve_the_sequence() {
    for stop in 1..=2 {
        let f = Fixture::new();
        let store = Store::open(&f.0, config()).unwrap();
        let observed = Arc::new(AtomicUsize::new(0));
        let count = observed.clone();
        let mut cfg = config();
        cfg.chunk_dir_sync = Some(Arc::new(move |_| {
            if count.fetch_add(1, Ordering::SeqCst) + 1 == stop {
                Err(io::Error::from_raw_os_error(5))
            } else { Ok(()) }
        }));
        let error = store.router.open_chunk(0, Class::FOREVER, String::new(), &cfg)
            .err().expect("a failed directory sync must refuse the new route");
        assert_eq!(error.raw_os_error(), Some(5));
        assert_eq!(observed.load(Ordering::SeqCst), stop);
        assert_eq!(store.chunks.lock().unwrap().next_id, 2);
        assert!(store.chunks.lock().unwrap().table.is_empty());
        cfg.chunk_dir_sync = None;
        let chunk = store.router.open_chunk(0, Class::FOREVER, String::new(), &cfg).unwrap();
        assert_eq!(chunk.id, 2, "a failed open's durable catalogue sequence must not be reused");
    }
}

#[test]
fn rotation_directory_failure_preserves_the_previously_flushed_value() {
    for stop in 3..=4 {
        let f = Fixture::new();
        let count = Arc::new(AtomicUsize::new(0));
        let observed = count.clone();
        let mut cfg = Config { chunk_bytes: 1, ..config() };
        cfg.chunk_dir_sync = Some(Arc::new(move |_| {
            if observed.fetch_add(1, Ordering::SeqCst) + 1 == stop {
                Err(io::Error::from_raw_os_error(5))
            } else { Ok(()) }
        }));
        let store = Store::open(&f.0, cfg.clone()).unwrap();
        let prior = store.mutate(mutation("one", b"one"));
        assert_eq!((prior.code, prior.ack), (0, 2));
        assert_eq!(store.mutate(mutation("two", b"two")).code, 16);
        assert_eq!(count.load(Ordering::SeqCst), stop);
        assert_eq!(store.get("caller", "key").unwrap().bytes, b"one");
        let files = chunks::list_chunk_files(&f.0.join("chunks")).unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(fs::metadata(&files[1].3).unwrap().len(), 0);
        drop(store);
        f.unlock();
        cfg.chunk_dir_sync = None;
        let store = Store::open(&f.0, cfg).unwrap();
        assert_eq!(store.get("caller", "key").unwrap().bytes, b"one");
        assert_eq!(store.mutate(mutation("one", b"one")), prior);
        assert_eq!(store.mutate(mutation("two", b"two")).code, 0);
        assert_eq!(store.get("caller", "key").unwrap().bytes, b"two");
    }
}
