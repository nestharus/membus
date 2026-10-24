//! Recovery clock doubt must reach retirement, HEALTH2 and startup evidence after publication I/O.
use super::*;

const START: u64 = UTC + 1000;
const DAY: u64 = 86_400_000;

fn seed(f: &Fixture, manual: &Arc<ManualClock>, open: bool) -> (Config, u64, PathBuf, Vec<u8>) {
    let mut config = f.config(manual);
    config.chunk_bytes = if open { u64::MAX } else { 1 };
    config.rotation_ms = WINDOW;
    let store = Store::open(&f.0, config.clone()).unwrap();
    store.policies.write().unwrap().set("clock", "", Some(Class::expire(WINDOW)));
    assert_eq!(store.mutate(Mutation { ack: 2, ..mutation("clock-safety", u64::MAX) }).code, 0);
    if !open {
        manual.advance(WINDOW);
        store.tick();
    }
    let (id, path) = {
        let chunks = store.chunks.lock().unwrap();
        let chunk = chunks.table.values().find(|c| c.class.kind == 2 && c.bytes > 0).unwrap();
        assert_eq!(chunk.state, if open { chunks::ChunkState::Open } else { chunks::ChunkState::Closed });
        (chunk.id, chunk.path.clone())
    };
    manual.advance(if open { 1000 } else { 1000 - WINDOW });
    if !open {
        store.chunks.lock().unwrap().log(START, chunks::Rec::Resume).unwrap();
    }
    let bytes = fs::read(&path).unwrap();
    drop(store);
    f.unlock();
    (config, id, path, bytes)
}

fn after_epoch(f: &Fixture, manual: &Arc<ManualClock>, mut config: Config, advance: u64, jump: i64)
    -> (Config, Arc<std::sync::atomic::AtomicBool>)
{
    let root = f.0.clone();
    let changed = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let observed = changed.clone();
    let clock = manual.clone();
    config.recovery_dir_sync = Some(Arc::new(move |dir| {
        if dir == root && fs::read(root.join("epoch"))? == 2u64.to_le_bytes()
            && !changed.swap(true, Ordering::SeqCst) {
            clock.advance(advance);
            clock.jump_utc(jump);
        }
        Ok(())
    }));
    (config, observed)
}

fn assert_pause(store: &Store, f: &Fixture, paused: bool) {
    assert_eq!(store.expiry_degraded(), paused);
    assert_eq!(store.health2().expiry_degraded, paused);
    let report = store.replay_report();
    assert_eq!(report.expiry_degraded.is_some(), paused);
    assert_eq!(store.chunks.lock().unwrap().degraded.is_some(), paused);
    assert_eq!(fs::read_to_string(f.0.join("replay.json")).unwrap().trim_end(), report.json());
}

fn publication_case(advance: u64, jump: i64, heartbeat: Option<u64>, paused: bool, stable_resume: bool) {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let (config, id, path, bytes) = seed(&f, &manual, false);
    if let Some(at) = heartbeat {
        fs::write(f.0.join("heartbeat.json"), format!("{{\"unix_ms\":{at}}}")).unwrap();
    }
    let (config, changed) = after_epoch(&f, &manual, config, advance, jump);
    let store = Store::open(&f.0, config).unwrap();
    assert!(changed.load(Ordering::SeqCst));
    let retired = store.maint.retire(None).unwrap();
    if paused {
        assert!(retired.is_empty(), "startup clock doubt must preserve the chunk: retired={retired:?}, degraded={}", store.expiry_degraded());
        assert_eq!(fs::read(&path).unwrap(), bytes);
        assert_eq!(store.get("clock", "key").unwrap().bytes, b"clock-safety");
    } else {
        assert_eq!(retired, vec![id]);
        assert!(!path.exists());
    }
    assert_pause(&store, &f, paused);
    if paused {
        if stable_resume {
            store.tick();
            assert!(store.expiry_degraded());
            manual.advance(WINDOW);
            store.tick();
        } else {
            fs::write(f.0.join("expiry.resume"), b"").unwrap();
            store.tick();
            assert!(!f.0.join("expiry.resume").exists());
        }
        assert!(!store.expiry_degraded());
        assert!(!store.health2().expiry_degraded);
        assert_eq!(store.maint.retire(None).unwrap(), vec![id]);
        assert!(!path.exists());
        assert_eq!(store.get_status("clock", "key"), (1, 0));
    }
}

#[test]
fn backward_publication_correction_pauses_retirement_until_explicit_resume() {
    publication_case(0, -1, None, true, false);
}
#[test]
fn forward_publication_correction_past_heartbeat_gap_pauses_until_stable() {
    publication_case(0, DAY as i64 + 1, Some(START), true, true);
}
#[test]
fn elapsed_publication_crossing_heartbeat_gap_pauses_retirement() {
    publication_case(DAY + 1, 0, Some(START), true, false);
}
#[test]
fn new_recovery_close_is_part_of_the_clock_high_water_mark() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let (config, id, path, bytes) = seed(&f, &manual, true);
    let (config, changed) = after_epoch(&f, &manual, config, 0, -1);
    let store = Store::open(&f.0, config).unwrap();
    assert!(changed.load(Ordering::SeqCst));
    assert_eq!(store.chunks.lock().unwrap().cat.as_ref().unwrap().last_ms, START);
    assert!(store.replay_report().closed_at_start.iter().any(|(chunk, _)| *chunk == id),
            "this Close must be published by recovery itself");
    manual.advance(WINDOW + 2);
    let retired = store.maint.retire(None).unwrap();
    assert!(retired.is_empty(), "the recovery Close also needs clock protection: chunk={id}, retired={retired:?}");
    assert_eq!(fs::read(path).unwrap(), bytes);
    assert_pause(&store, &f, true);
}
#[test]
fn exact_heartbeat_gap_remains_eligible_for_retirement() {
    publication_case(DAY, 0, Some(START), false, false);
}
#[test]
fn elapsed_time_without_net_utc_age_does_not_create_clock_doubt() {
    publication_case(DAY + 1, -(DAY as i64 + 1), Some(START), false, false);
}
#[test]
fn an_initial_clock_pause_is_not_cleared_by_a_correction_during_publication() {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let (config, _, path, bytes) = seed(&f, &manual, false);
    manual.jump_utc(-1);
    let (config, _) = after_epoch(&f, &manual, config, 0, 1);
    let store = Store::open(&f.0, config).unwrap();
    assert_pause(&store, &f, true);
    assert!(store.maint.retire(None).unwrap().is_empty());
    assert_eq!(fs::read(path).unwrap(), bytes);
}

fn heartbeat_format_case(format: u8, paused: bool) {
    let f = Fixture::new();
    let manual = ManualClock::new(UTC);
    let (config, id, path, bytes) = seed(&f, &manual, false);
    let text = match format {
        0 => format!("{{\"unix_ms\":{START}}}"),
        1 => format!("{{\"unix_ms\": {START}}}"),
        2 => format!("{{\"unix_ms\" :{START}}}"),
        3 => format!("{{\n  \"unix_ms\"\t:\n  {START}\n}}"),
        _ => "{\"unix_ms\":null}".into(),
    };
    fs::write(f.0.join("heartbeat.json"), text).unwrap();
    manual.advance(DAY + 1);
    let store = Store::open(&f.0, config).unwrap();
    let retired = store.maint.retire(None).unwrap();
    if paused {
        assert!(retired.is_empty(), "valid heartbeat JSON must protect the chunk: format={format}, retired={retired:?}");
        assert_eq!(fs::read(path).unwrap(), bytes);
        assert_eq!(store.get("clock", "key").unwrap().bytes, b"clock-safety");
    } else {
        assert_eq!(retired, vec![id]);
        assert!(!path.exists());
    }
    assert_pause(&store, &f, paused);
}

#[test]
fn heartbeat_json_whitespace_preserves_the_clock_guard_and_missing_timestamp_fallback() {
    for format in 0..=4 { heartbeat_format_case(format, format < 4); }
}
