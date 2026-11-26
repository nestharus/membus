//! Automatic retirement must preserve retryable records in both clock domains.
#[path = "resume_tests.rs"]
mod resume_tests;
#[path = "recovered_retry_tests.rs"]
mod recovered_retry_tests;
#[cfg(windows)]
#[path = "blocked_clock_tests.rs"]
mod blocked_clock_tests;
use super::*;
use crate::{Class, Mutation, Outcome, Store};

const UTC: u64 = 1_000_000;
const WINDOW: u64 = 100;
static SEQ: AtomicU64 = AtomicU64::new(0);

struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        Self(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".test-output").join(format!(
            "retirement-clock-{}-{}", std::process::id(), SEQ.fetch_add(1, Ordering::SeqCst))))
    }
    fn seed(&self, utc: u64, retry: u64, class: Class)
        -> (Store, Arc<chunks::ManualClock>, Mutation, Outcome, u64, PathBuf)
    {
        let clock = chunks::ManualClock::new(utc);
        let store = Store::open(&self.0, Config {
            clock: Some(clock.clone()), retry_ms: retry, chunk_bytes: 1, flush_ms: 0,
            tick_ms: 3_600_000, retire_ms: 3_600_000, pinned_high_water_bytes: 1,
            ..Config::default()
        }).unwrap();
        store.policies.write().unwrap().set("retire-clock", "", Some(class));
        let original = Mutation {
            ns: "retire-clock".into(), key: "key".into(), op_id: "original".into(),
            deadline: u64::MAX, ack: 2, expected: None, bytes: b"first".to_vec(), remove: false,
        };
        let outcome = store.mutate(original.clone());
        assert_eq!(outcome.code, 0);
        let latest = store.mutate(Mutation {
            op_id: "latest".into(), bytes: b"second".to_vec(), ..original.clone()
        });
        assert_eq!(latest.code, 0);
        assert_eq!(latest.version, outcome.version + 1);
        let (id, path) = {
            let chunks = store.chunks.lock().unwrap();
            let old = chunks.table.values().find(|c| c.state == chunks::ChunkState::Closed).unwrap();
            assert_eq!(old.close_ms, Some(utc));
            assert_eq!(old.close_mono, Some(0));
            (old.id, old.path.clone())
        };
        (store, clock, original, outcome, id, path)
    }
    fn unlock(&self) {
        #[cfg(not(windows))]
        fs::remove_file(self.0.join("owner.lock")).unwrap();
    }
}
impl Drop for Fixture {
    fn drop(&mut self) { if self.0.exists() { fs::remove_dir_all(&self.0).unwrap(); } }
}

fn boundary_restart(class: Class, retry: u64) {
    let f = Fixture::new();
    let (store, clock, original, outcome, id, path) = f.seed(UTC, retry, class);
    let bytes = fs::read(&path).unwrap();
    clock.advance(retry);
    let retired = store.maint.retire(None).unwrap();
    // Memory still knows this outcome even if an early retirement lost its disk record.
    assert_eq!(store.mutate(original.clone()), outcome);
    let mut config = store.config.clone();
    drop(store);
    f.unlock();
    let clock = chunks::ManualClock::new(UTC + retry);
    config.clock = Some(clock.clone());
    let store = Store::open(&f.0, config).unwrap();
    assert_eq!(store.epoch, 2);
    assert_eq!(store.mutate(original.clone()), outcome, "restart must retain the original retry outcome");
    assert!(retired.is_empty(), "the inclusive retry boundary cannot authorize automatic retirement");
    assert_eq!(fs::read(&path).unwrap(), bytes);
    let value = store.get("retire-clock", "key").unwrap();
    assert_eq!((value.version, value.bytes), (outcome.version + 1, b"second".to_vec()));
    clock.advance(1);
    assert_eq!(store.mutate(original).code, 13);
    assert_eq!(store.maint.retire(None).unwrap(), vec![id]);
    assert!(!path.exists(), "the next elapsed millisecond may retire the old chunk");
    assert_eq!(store.get("retire-clock", "key").unwrap().bytes, b"second");
}

#[test]
fn automatic_retirement_preserves_inclusive_retry_boundaries_across_restart() {
    for (class, retry) in [(Class::LATEST, WINDOW), (Class::expire(WINDOW), WINDOW), (Class::LATEST, 0)] {
        boundary_restart(class, retry);
    }
}

#[test]
fn automatic_retirement_waits_past_the_monotonic_retry_boundary_after_utc_jumps() {
    let f = Fixture::new();
    let (store, clock, _, _, id, path) = f.seed(UTC, WINDOW, Class::LATEST);
    clock.advance(WINDOW);
    clock.jump_utc(5_000);
    assert!(store.maint.retire(None).unwrap().is_empty(), "UTC cannot bypass the inclusive local retry bound");
    assert!(path.exists());
    clock.advance(1);
    assert_eq!(store.maint.retire(None).unwrap(), vec![id]);
}

fn recovered_maximum_age(class: Class) {
    let f = Fixture::new();
    let (store, _, _, _, id, path) = f.seed(u64::MAX - 5, 10, class);
    let mut config = store.config.clone();
    let bytes = fs::read(&path).unwrap();
    drop(store);
    f.unlock();
    config.clock = Some(chunks::ManualClock::new(u64::MAX));
    let store = Store::open(&f.0, config).unwrap();
    assert_eq!(store.chunks.lock().unwrap().table[&id].close_mono, None);
    assert!(store.maint.retire(None).unwrap().is_empty(), "five UTC milliseconds cannot satisfy the retention/retry window");
    assert_eq!(fs::read(path).unwrap(), bytes);
    assert_eq!(store.get("retire-clock", "key").unwrap().bytes, b"second");
}

#[test]
fn recovered_age_near_maximum_utc_does_not_shorten_retention() {
    for class in [Class::LATEST, Class::expire(WINDOW)] { recovered_maximum_age(class); }
}

#[test]
fn longer_expire_retention_keeps_its_existing_exact_retention_boundary() {
    let f = Fixture::new();
    let (store, clock, _, _, id, path) = f.seed(UTC, WINDOW, Class::expire(2 * WINDOW));
    clock.advance(2 * WINDOW);
    assert_eq!(store.maint.retire(None).unwrap(), vec![id]);
    assert!(!path.exists());
    assert_eq!(store.get("retire-clock", "key").unwrap().bytes, b"second");
}

#[test]
fn explicit_forced_retirement_keeps_its_existing_clock_override() {
    let f = Fixture::new();
    let (store, _, _, _, id, path) = f.seed(UTC, WINDOW, Class::LATEST);
    assert!(store.retire_chunk(id).unwrap());
    assert!(!path.exists());
    assert_eq!(store.get("retire-clock", "key").unwrap().bytes, b"second");
}

fn pinned_store(f: &Fixture, utc: u64) -> (Store, Arc<chunks::ManualClock>, u64) {
    let (store, clock, original, _, _, _) = f.seed(utc, WINDOW, Class::LATEST);
    let stripe = crate::routing::stripe(&original.ns, &original.key);
    let next_key = (0..100).map(|n| format!("other-{n}"))
        .find(|key| crate::routing::stripe(&original.ns, key) == stripe).unwrap();
    assert_eq!(store.mutate(Mutation {
        key: next_key, op_id: "close-latest".into(), bytes: b"third".to_vec(), ..original
    }).code, 0);
    let bytes = {
        let state = store.state.lock().unwrap();
        let id = state.metadata[&("retire-clock".into(), "key".into())].chunk;
        let chunks = store.chunks.lock().unwrap();
        assert_eq!(chunks.table[&id].state, chunks::ChunkState::Closed);
        chunks.table[&id].bytes
    };
    (store, clock, bytes)
}

fn assert_pinned(store: &Store, bytes: u64) {
    let health = store.health2();
    assert_eq!(health.pinned_bytes, bytes);
    assert_eq!(health.pinned_high_water, bytes > 0);
    assert!(!health.storage_error);
    assert!(!health.read_only);
}

#[test]
fn pinned_health_does_not_turn_a_utc_jump_into_elapsed_local_retention() {
    let f = Fixture::new();
    let (store, clock, bytes) = pinned_store(&f, UTC);
    clock.jump_utc(5_000);
    assert_pinned(&store, 0);
    clock.advance(WINDOW);
    assert_pinned(&store, 0);
    clock.advance(1);
    assert_pinned(&store, bytes);
}

#[test]
fn pinned_health_recovered_maximum_utc_keeps_the_actual_age() {
    let f = Fixture::new();
    let (store, _, _) = pinned_store(&f, u64::MAX - 5);
    let mut config = store.config.clone();
    drop(store);
    f.unlock();
    config.clock = Some(chunks::ManualClock::new(u64::MAX));
    let store = Store::open(&f.0, config).unwrap();
    assert_pinned(&store, 0);
}
