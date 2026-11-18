//! Recovered outcomes outlive an earlier chunk-close UTC after a clock correction.
use super::*;
use std::sync::mpsc;
use std::time::Duration;

const CORRECTION: u64 = 5_000;
const RESTART: u64 = UTC + 2 * WINDOW + 1;

struct Seed {
    store: Store,
    config: Config,
    original: Mutation,
    first: Outcome,
    latest: Outcome,
    old_id: u64,
    old_path: PathBuf,
    old_bytes: u64,
}

fn seed(f: &Fixture, class: Class, supersede: bool) -> Seed {
    let clock = chunks::ManualClock::new(UTC + CORRECTION);
    let config = Config {
        clock: Some(clock.clone()), retry_ms: WINDOW, chunk_bytes: 1, flush_ms: 0,
        tick_ms: 3_600_000, retire_ms: 3_600_000, pinned_high_water_bytes: 1,
        ..Config::default()
    };
    let store = Store::open(&f.0, config.clone()).unwrap();
    store.policies.write().unwrap().set("retire-clock", "", Some(class));
    let original = Mutation {
        ns: "retire-clock".into(), key: "key".into(), op_id: "before-correction".into(),
        deadline: u64::MAX, ack: 2, expected: None, bytes: b"first".to_vec(), remove: false,
    };
    let (reached, entered) = mpsc::channel();
    let (resume, resumed) = mpsc::channel();
    *store.router.selection_hook.lock().unwrap() = Some((reached, resumed));
    let first = std::thread::scope(|scope| {
        let writer = scope.spawn(|| store.mutate(original.clone()));
        let observed = entered.recv_timeout(Duration::from_secs(5));
        if observed.is_ok() { clock.jump_utc(-(CORRECTION as i64)); }
        // Always release the writer before any assertion can unwind the scope.
        let _ = resume.send(());
        observed.expect("the writer must reach its route after recording admission UTC");
        writer.join().unwrap()
    });
    assert_eq!(first.code, 0);
    let key = if supersede { original.key.clone() } else {
        let stripe = crate::routing::stripe(&original.ns, &original.key);
        (0..100).map(|n| format!("other-{n}"))
            .find(|key| crate::routing::stripe(&original.ns, key) == stripe).unwrap()
    };
    let latest = store.mutate(Mutation {
        key, op_id: "after-correction".into(), bytes: b"second".to_vec(), ..original.clone()
    });
    assert_eq!(latest.code, 0);
    let (old_id, old_path, old_bytes) = {
        let chunks = store.chunks.lock().unwrap();
        let old = chunks.table.values().find(|c| c.state == chunks::ChunkState::Closed).unwrap();
        assert_eq!(old.close_ms, Some(UTC));
        assert_eq!(old.close_mono, Some(0));
        (old.id, old.path.clone(), old.bytes)
    };
    let location = fs::read(&old_path).unwrap();
    let (_, version, at) = crate::codec::parse_record(&location[16..]).unwrap();
    assert_eq!((version, at), (first.version, UTC + CORRECTION));
    assert_eq!(store.mutate(original.clone()), first);
    Seed { store, config, original, first, latest, old_id, old_path, old_bytes }
}

fn reopen(f: &Fixture, mut config: Config, utc: u64) -> (Store, Arc<chunks::ManualClock>) {
    f.unlock();
    let clock = chunks::ManualClock::new(utc);
    config.clock = Some(clock.clone());
    let store = Store::open(&f.0, config).unwrap();
    assert!(!store.expiry_degraded(), "the catalogue close is older than current UTC");
    (store, clock)
}

fn preserve_recovered_retry(class: Class) {
    let f = Fixture::new();
    let Seed { store, config, original, first, latest, old_id, old_path, .. } = seed(&f, class, true);
    drop(store);
    let (store, _) = reopen(&f, config.clone(), RESTART);
    assert_eq!(store.mutate(original.clone()), first);
    let retired = store.maint.retire(None).unwrap();
    drop(store);
    let (store, clock) = reopen(&f, config, RESTART);
    assert_eq!(store.mutate(original.clone()), first,
        "automatic retirement must preserve the original recovered outcome across another restart");
    assert!(retired.is_empty());
    assert!(old_path.exists());
    let value = store.get("retire-clock", "key").unwrap();
    assert_eq!((value.version, value.bytes), (latest.version, b"second".to_vec()));
    clock.advance(WINDOW);
    assert_eq!(store.mutate(original.clone()), first, "the recovered bound remains inclusive");
    assert!(store.maint.retire(None).unwrap().is_empty());
    clock.advance(1);
    assert_eq!(store.mutate(original).code, 13);
    assert_eq!(store.maint.retire(None).unwrap(), vec![old_id]);
    assert!(!old_path.exists());
    assert_eq!(store.get("retire-clock", "key").unwrap().version, latest.version);
}

#[test]
fn recovered_retry_latest_retirement_keeps_the_original_outcome_after_a_backward_correction() {
    preserve_recovered_retry(Class::LATEST);
}

#[test]
fn recovered_retry_expire_retirement_keeps_the_original_outcome_after_a_backward_correction() {
    preserve_recovered_retry(Class::expire(2 * WINDOW));
}

#[test]
fn recovered_retry_pinned_health_waits_for_the_actual_recovered_bound() {
    let f = Fixture::new();
    let Seed { store, config, old_bytes, first, .. } = seed(&f, Class::LATEST, false);
    drop(store);
    let (store, clock) = reopen(&f, config, RESTART);
    let mature_bytes = chunks::list_chunk_files(&f.0.join("chunks")).unwrap().into_iter()
        .filter(|(label, _, _, _)| label.as_str() == "latest")
        .map(|(_, _, _, path)| fs::metadata(path).unwrap().len()).sum::<u64>();
    assert!(mature_bytes > old_bytes, "both distinct latest keys retain a chunk");
    assert_eq!(store.get("retire-clock", "key").unwrap().version, first.version);
    assert_pinned(&store, 0);
    clock.jump_utc(100_000);
    assert_pinned(&store, 0);
    clock.advance(WINDOW);
    assert_pinned(&store, 0);
    clock.advance(1);
    assert_pinned(&store, mature_bytes);
}

#[test]
fn recovered_retry_already_expired_outcomes_do_not_hold_a_chunk() {
    let f = Fixture::new();
    let Seed { store, config, old_id, old_path, latest, .. } = seed(&f, Class::LATEST, true);
    drop(store);
    let (store, _) = reopen(&f, config, UTC + CORRECTION + WINDOW + 1);
    assert_eq!(store.replay_report().dedup_entries, 0);
    assert_eq!(store.maint.retire(None).unwrap(), vec![old_id]);
    assert!(!old_path.exists());
    assert_eq!(store.get("retire-clock", "key").unwrap().version, latest.version);
}

#[test]
fn recovered_retry_pins_ordinary_chunk_even_during_explicit_retirement() {
    let f = Fixture::new();
    let Seed { store, config, original, first, old_id, old_path, latest, .. } =
        seed(&f, Class::LATEST, true);
    drop(store);
    let catalogue = f.0.join("chunks/catalogue.log");
    let (store, _) = reopen(&f, config.clone(), RESTART);
    let retained_bytes = fs::read(&old_path).unwrap();
    let retained_catalogue = fs::read(&catalogue).unwrap();
    assert!(!store.retire_chunk(old_id).unwrap());
    assert_eq!(fs::read(&old_path).unwrap(), retained_bytes);
    assert_eq!(fs::read(&catalogue).unwrap(), retained_catalogue);
    drop(store);

    let (store, clock) = reopen(&f, config.clone(), RESTART);
    assert_eq!(store.mutate(original.clone()), first);
    clock.advance(WINDOW);
    assert_eq!(store.mutate(original.clone()), first);
    assert!(!store.retire_chunk(old_id).unwrap());
    assert_eq!(fs::read(&old_path).unwrap(), retained_bytes);
    assert_eq!(fs::read(&catalogue).unwrap(), retained_catalogue);
    clock.advance(1);
    assert_eq!(store.mutate(original).code, 13);
    assert!(store.retire_chunk(old_id).unwrap());
    assert!(!old_path.exists());
    drop(store);
    let (store, _) = reopen(&f, config, RESTART + WINDOW + 1);
    assert_eq!(store.get("retire-clock", "key").unwrap().version, latest.version);
}
