//! Real held deletes must age by local elapsed time, including after recovery.
use super::*;
use std::os::windows::fs::OpenOptionsExt;

const HOUR: u64 = 3_600_000;
fn hold(path: &std::path::Path) -> fs::File {
    fs::OpenOptions::new().read(true).share_mode(3).open(path).unwrap()
}
fn blocked(store: &Store, count: usize, age: u64) {
    let health = store.health2();
    assert_eq!(store.retire_blocked(), count);
    assert_eq!(health.retire_blocked, count);
    assert_eq!(health.retire_blocked_oldest_ms, age);
    assert_eq!(health.retire_blocked_high_water, age > HOUR);
    let fields = health.fields();
    assert_eq!(fields.iter().find(|(key, _)| key == "retire_blocked_oldest_ms").unwrap().1,
               age.to_string());
}

#[test]
fn blocked_retirement_age_ignores_forward_utc_correction() {
    let f = Fixture::new();
    let (store, clock, _, _, id, path) = f.seed(UTC, WINDOW, Class::LATEST);
    let held = hold(&path);
    clock.advance(37);
    assert!(!store.retire_chunk(id).unwrap());
    blocked(&store, 1, 0);
    clock.jump_utc(2 * HOUR as i64);
    blocked(&store, 1, 0);
    clock.advance(12);
    blocked(&store, 1, 12);
    drop(held);
    assert_eq!(store.retry_retiring().unwrap(), 1);
    blocked(&store, 0, 0);
    assert_eq!(store.get("retire-clock", "key").unwrap().bytes, b"second");
}

#[test]
fn blocked_retirement_age_survives_backward_utc_correction() {
    let f = Fixture::new();
    let (store, clock, _, _, id, path) = f.seed(UTC, WINDOW, Class::LATEST);
    let held = hold(&path);
    clock.advance(37);
    assert!(!store.retire_chunk(id).unwrap());
    clock.advance(HOUR + 1);
    blocked(&store, 1, HOUR + 1);
    clock.jump_utc(-(10 * HOUR as i64));
    blocked(&store, 1, HOUR + 1);
    drop(held);
    assert_eq!(store.retry_retiring().unwrap(), 1);
    blocked(&store, 0, 0);
}

#[test]
fn blocked_retirement_high_water_keeps_its_strict_elapsed_hour_boundary() {
    let f = Fixture::new();
    let (store, clock, _, _, id, path) = f.seed(UTC, WINDOW, Class::LATEST);
    let held = hold(&path);
    clock.advance(37);
    assert!(!store.retire_chunk(id).unwrap());
    blocked(&store, 1, 0);
    clock.advance(HOUR);
    blocked(&store, 1, HOUR);
    clock.advance(1);
    blocked(&store, 1, HOUR + 1);
    drop(held);
    assert_eq!(store.retry_retiring().unwrap(), 1);
    blocked(&store, 0, 0);
}

#[test]
fn recovered_blocked_retirement_starts_a_new_local_observation() {
    let f = Fixture::new();
    let (store, clock, _, _, id, path) = f.seed(UTC, WINDOW, Class::LATEST);
    let held = hold(&path);
    clock.advance(37);
    assert!(!store.retire_chunk(id).unwrap());
    let mut config = store.config.clone();
    drop(store);
    let clock = chunks::ManualClock::new(UTC + 10 * HOUR);
    clock.advance(12_345);
    config.clock = Some(clock.clone());
    let store = Store::open(&f.0, config).unwrap();
    assert_eq!(store.epoch, 2);
    blocked(&store, 1, 0);
    clock.jump_utc(2 * HOUR as i64);
    blocked(&store, 1, 0);
    clock.advance(12);
    assert_eq!(store.retry_retiring().unwrap(), 0);
    blocked(&store, 1, 12);
    drop(held);
    assert_eq!(store.retry_retiring().unwrap(), 1);
    blocked(&store, 0, 0);
    assert_eq!(store.get("retire-clock", "key").unwrap().bytes, b"second");
}

fn third(store: &Store, original: Mutation, first_id: u64) -> (u64, PathBuf) {
    assert_eq!(store.mutate(Mutation {
        op_id: "third".into(), bytes: b"third".to_vec(), ..original
    }).code, 0);
    let chunks = store.chunks.lock().unwrap();
    let second = chunks.table.values()
        .find(|c| c.id != first_id && c.state == chunks::ChunkState::Closed).unwrap();
    (second.id, second.path.clone())
}

#[test]
fn oldest_blocked_retirement_uses_elapsed_order_across_utc_correction() {
    let f = Fixture::new();
    let (store, clock, original, _, first_id, first_path) = f.seed(UTC, WINDOW, Class::LATEST);
    let (second_id, second_path) = third(&store, original, first_id);
    let first = hold(&first_path);
    let second = hold(&second_path);
    clock.advance(37);
    assert!(!store.retire_chunk(first_id).unwrap());
    clock.advance(500);
    clock.jump_utc(-(20 * HOUR as i64));
    assert!(!store.retire_chunk(second_id).unwrap());
    blocked(&store, 2, 500);
    drop(first);
    clock.advance(20);
    assert_eq!(store.retry_retiring().unwrap(), 1);
    blocked(&store, 1, 20);
    drop(second);
    assert_eq!(store.retry_retiring().unwrap(), 1);
    blocked(&store, 0, 0);
    assert_eq!(store.get("retire-clock", "key").unwrap().bytes, b"third");
}

#[test]
fn retry_retiring_records_its_first_denied_delete_after_an_interrupted_pass() {
    let f = Fixture::new();
    let (store, clock, original, _, first_id, first_path) = f.seed(UTC, WINDOW, Class::LATEST);
    let (_, second_path) = third(&store, original, first_id);
    let first = hold(&first_path);
    let second = hold(&second_path);
    clock.advance(WINDOW + 1);
    // The first Retiring intent is durable; the later failure stops the pass
    // before any delete. Retry must observe the first denial itself.
    store.chunks.lock().unwrap().cat.as_mut().unwrap().fail_after_once = Some(1);
    assert!(store.maint.retire(None).is_err());
    blocked(&store, 0, 0);
    assert!(store.storage_error(), "the earlier append failure remains sticky");
    let before = store.state_bytes();
    assert_eq!(store.retry_retiring().unwrap(), 0);
    blocked(&store, 1, 0);
    assert_eq!(store.state_bytes(), before);
    clock.advance(HOUR + 1);
    clock.jump_utc(-(10 * HOUR as i64));
    assert_eq!(store.retry_retiring().unwrap(), 0);
    blocked(&store, 1, HOUR + 1);
    drop(first);
    drop(second);
    assert_eq!(store.retry_retiring().unwrap(), 1);
    blocked(&store, 0, 0);
    assert!(store.state_bytes() < before);
    assert!(store.storage_error());
    assert_eq!(store.get("retire-clock", "key").unwrap().bytes, b"third");
}
