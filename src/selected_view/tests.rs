use super::*;
use crate::{
    chunks, Config, Cursor, ManualClock, Mutation, SCAN_KEYS_PREFIX, SCAN_PREFIX, SCAN_RANGE,
};
use std::{fs, path::PathBuf, sync::atomic::AtomicU64};

static NEXT: AtomicU64 = AtomicU64::new(0);
thread_local! {
    static BEFORE_PUBLISH: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = std::cell::RefCell::new(None);
    static MATERIALIZED: std::cell::RefCell<Option<MaterializationObserver>> = const { std::cell::RefCell::new(None) };
}
type MaterializationObserver =
    Box<dyn FnMut(&Store, &[(String, SnapshotValue)], usize, Option<&Snapshot>, bool)>;
pub(super) fn before_materialization(s: &Store) {
    MATERIALIZED.with(|hook| {
        if let Some(hook) = hook.borrow_mut().as_mut() {
            hook(s, &[], 0, None, true);
        }
    });
}
// Called immediately after actual Vec reservation, each key/reference copy and
// Snapshot construction. The observer owns a fixed-size record, not row copies.
pub(super) fn materialized(
    s: &Store,
    rows: &[(String, SnapshotValue)],
    capacity: usize,
    view: Option<&Snapshot>,
) {
    MATERIALIZED.with(|hook| {
        if let Some(hook) = hook.borrow_mut().as_mut() {
            hook(s, rows, capacity, view, false);
        }
    });
}
pub(super) fn before_publish() {
    BEFORE_PUBLISH.with(|hook| {
        if let Some(hook) = hook.take() {
            hook();
        }
    });
}
struct Fixture(PathBuf);
impl Fixture {
    fn new() -> Self {
        let root = PathBuf::from(
            std::env::var_os("MEMBUS_TEST_OUTPUT").unwrap_or_else(|| ".test-output".into()),
        );
        fs::create_dir_all(&root).unwrap();
        Self(fs::canonicalize(root).unwrap().join(format!(
            "selected-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )))
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}
fn config() -> Config {
    Config {
        capacity: 16 * 1024 * 1024,
        signal_bytes: 0,
        cache_bytes: 0,
        snapshot_bytes: 1024 * 1024,
        snapshot_max: 8,
        snapshot_ttl_ms: 60_000,
        tick_ms: 3_600_000,
        retire_ms: 3_600_000,
        ..Config::default()
    }
}
fn budget() -> AcquisitionBudget {
    AcquisitionBudget {
        max_candidates: 100,
        max_metadata_bytes: 100_000,
        max_retained_bytes: 100_000,
    }
}
fn put(s: &Store, ns: &str, key: &str, bytes: &[u8], remove: bool) {
    assert_eq!(
        s.mutate(Mutation {
            ns: ns.into(),
            key: key.into(),
            op_id: format!("op-{ns}-{key}-{remove}-{}", bytes.len()),
            deadline: s.clock.utc_ms() + 60_000,
            ack: 1,
            expected: None,
            bytes: bytes.to_vec(),
            remove
        })
        .code,
        0
    );
}
fn seed(s: &Store) {
    put(s, "before", "x", &[255; 500], false);
    put(s, "ledger", "a", b"ignored invalid JSON", false);
    put(s, "ledger", "keep/1", &[0, 255, 10], false);
    put(s, "ledger", "keep/2", b"", true);
    put(s, "ledger", "keep/3", b"", false);
    put(s, "ledger", "z", &[42; 500], false);
    put(s, "zzafter", "x", &[42; 500], false);
}
fn clean(s: &Store) {
    assert_eq!(s.scan_usage.count.load(Ordering::Acquire), 0);
    assert_eq!(s.scan_usage.bytes.load(Ordering::Acquire), 0);
    assert!(s.scans.lock().unwrap().snapshots.is_empty());
}
fn page(
    s: &Store,
    ns: &str,
    token: &[u8],
    op: u8,
    lower: &str,
    upper: &str,
    cursor: &[u8],
    limit: u16,
) -> Result<Vec<u8>, u16> {
    let mut b = Vec::new();
    crate::lp(&mut b, token);
    crate::lp(&mut b, cursor);
    b.extend(limit.to_le_bytes());
    b.extend(65_536u32.to_le_bytes());
    crate::lp(&mut b, lower.as_bytes());
    if op == SCAN_RANGE {
        crate::lp(&mut b, upper.as_bytes());
    }
    s.scan_page(ns, &mut Cursor::new(&b), op, 65_536)
}
fn decode(b: &[u8], keys: bool) -> (Vec<u8>, Vec<(String, Vec<u8>)>, bool) {
    let mut c = Cursor::new(b);
    assert_eq!(c.bytes(16).unwrap().len(), 16);
    let cursor = c.bytes(4096).unwrap().to_vec();
    let done = c.u8().unwrap() == 1;
    let count = c.u16().unwrap();
    let mut rows = Vec::new();
    for _ in 0..count {
        let key = c.text().unwrap();
        let value = if keys {
            vec![]
        } else {
            assert_eq!(c.u8().unwrap(), 1);
            assert!(c.u64().unwrap() > 0);
            assert_eq!(c.u8().unwrap(), 1);
            c.bytes(crate::MAX_VALUE).unwrap().to_vec()
        };
        rows.push((key, value));
    }
    c.end().unwrap();
    (cursor, rows, done)
}

#[test]
fn exact_independent_work_and_byte_bounds_charge_ignored_tombstone_and_stop_witness() {
    let f = Fixture::new();
    let s = Store::open(&f.0, config()).unwrap();
    seed(&s);
    let selection = Selection::Prefix("keep/");
    // Six visited entries per pass: earlier namespace, ignored name, two live
    // selected rows, selected tombstone, and first nonmatching boundary.
    let exact = AcquisitionBudget {
        max_candidates: 12,
        max_metadata_bytes: 1650,
        max_retained_bytes: 538,
    };
    let entry = s
        .committed
        .read()
        .unwrap()
        .get(&("ledger".into(), "keep/1".into()))
        .unwrap()
        .clone();
    let refs = Arc::strong_count(&entry);
    for b in [
        AcquisitionBudget {
            max_candidates: 11,
            ..exact
        },
        AcquisitionBudget {
            max_metadata_bytes: 1649,
            ..exact
        },
        AcquisitionBudget {
            max_retained_bytes: 537,
            ..exact
        },
    ] {
        let result = s.snapshot_open_selected("ledger", selection, b);
        assert_eq!(
            result.unwrap_err(),
            if b.max_retained_bytes < 538 {
                AcquisitionError::RetainedBudget
            } else {
                AcquisitionError::WorkBudget
            }
        );
        clean(&s);
        assert_eq!(Arc::strong_count(&entry), refs);
    }
    s.capture_visits.store(0, Ordering::Relaxed);
    let view = s
        .snapshot_open_selected("ledger", selection, exact)
        .unwrap();
    assert_eq!(
        (
            view.population,
            view.candidates,
            view.metadata_bytes,
            view.retained_bytes
        ),
        (2, 12, 1650, 538)
    );
    assert_eq!(s.capture_visits.load(Ordering::Relaxed), 12);
    assert_eq!(Arc::strong_count(&entry), refs + 1);
    let (_, rows, done) = decode(
        &page(
            &s,
            "ledger",
            &view.snapshot,
            SCAN_KEYS_PREFIX,
            "keep/",
            "",
            &[],
            128,
        )
        .unwrap(),
        true,
    );
    assert!(done);
    assert_eq!(
        rows.iter().map(|r| r.0.as_str()).collect::<Vec<_>>(),
        ["keep/1", "keep/3"]
    );
    assert_eq!(s.cache_stats().cold_reads, 0);
    s.snapshot_release("ledger", &view.snapshot).unwrap();
    clean(&s);
    assert_eq!(Arc::strong_count(&entry), refs);
}

#[test]
fn budgets_refuse_before_metadata_walk_or_any_key_pin() {
    let f = Fixture::new();
    let s = Store::open(&f.0, config()).unwrap();
    seed(&s);
    let start_id = s.scans.lock().unwrap().next;
    for b in [
        AcquisitionBudget {
            max_candidates: 1,
            ..budget()
        },
        AcquisitionBudget {
            max_metadata_bytes: 255,
            ..budget()
        },
        AcquisitionBudget {
            max_retained_bytes: 266,
            ..budget()
        },
    ] {
        s.capture_visits.store(0, Ordering::Relaxed);
        assert!(s
            .snapshot_open_selected("ledger", Selection::Prefix("keep/"), b)
            .is_err());
        assert_eq!(s.capture_visits.load(Ordering::Relaxed), 0);
        assert_eq!(s.scans.lock().unwrap().next, start_id);
        clean(&s);
    }
}

#[test]
fn empty_is_qualified_only_after_bounded_discovery_and_keeps_base_charge() {
    let f = Fixture::new();
    let s = Store::open(&f.0, config()).unwrap();
    let v = s
        .snapshot_open_selected(
            "empty",
            Selection::Prefix("p"),
            AcquisitionBudget {
                max_candidates: 0,
                max_metadata_bytes: 0,
                max_retained_bytes: 262,
            },
        )
        .unwrap();
    assert_eq!(
        (
            v.population,
            v.candidates,
            v.metadata_bytes,
            v.retained_bytes
        ),
        (0, 0, 0, 262)
    );
    let (_, rows, done) = decode(
        &page(&s, "empty", &v.snapshot, SCAN_PREFIX, "p", "", &[], 1).unwrap(),
        false,
    );
    assert!(rows.is_empty() && done);
    s.snapshot_release("empty", &v.snapshot).unwrap();
    clean(&s);
    put(&s, "empty", "ignored", b"bad", false);
    assert_eq!(
        s.snapshot_open_selected(
            "empty",
            Selection::Prefix("p"),
            AcquisitionBudget {
                max_candidates: 1,
                ..budget()
            }
        )
        .unwrap_err(),
        AcquisitionError::WorkBudget
    );
    let v = s
        .snapshot_open_selected("empty", Selection::Prefix("p"), budget())
        .unwrap();
    assert_eq!((v.population, v.candidates, v.retained_bytes), (0, 2, 262));
    s.snapshot_release("empty", &v.snapshot).unwrap();
    clean(&s);
}

#[test]
fn token_binds_namespace_query_cursor_and_immutable_population() {
    let f = Fixture::new();
    let s = Store::open(&f.0, config()).unwrap();
    seed(&s);
    let v = s
        .snapshot_open_selected(
            "ledger",
            Selection::Range {
                lower: "keep/",
                upper: "l",
            },
            budget(),
        )
        .unwrap();
    assert_eq!(v.population, 2);
    put(&s, "ledger", "keep/1", b"replacement", false);
    put(&s, "ledger", "keep/0", b"new", false);
    let (cursor, rows, done) = decode(
        &page(&s, "ledger", &v.snapshot, SCAN_RANGE, "keep/", "l", &[], 1).unwrap(),
        false,
    );
    assert!(!done);
    assert_eq!(rows, [("keep/1".into(), vec![0, 255, 10])]);
    assert_eq!(
        page(&s, "ledger", &v.snapshot, SCAN_PREFIX, "keep/", "", &[], 1).unwrap_err(),
        21
    );
    assert_eq!(
        page(&s, "ledger", &v.snapshot, SCAN_RANGE, "keep/", "m", &[], 1).unwrap_err(),
        21
    );
    assert_eq!(
        page(&s, "other", &v.snapshot, SCAN_RANGE, "keep/", "l", &[], 1).unwrap_err(),
        21
    );
    let other = s
        .snapshot_open_selected(
            "ledger",
            Selection::Range {
                lower: "keep/",
                upper: "l",
            },
            budget(),
        )
        .unwrap();
    assert_eq!(
        page(
            &s,
            "ledger",
            &other.snapshot,
            SCAN_RANGE,
            "keep/",
            "l",
            &cursor,
            1
        )
        .unwrap_err(),
        21
    );
    let (_, rows, done) = decode(
        &page(
            &s,
            "ledger",
            &v.snapshot,
            SCAN_RANGE,
            "keep/",
            "l",
            &cursor,
            1,
        )
        .unwrap(),
        false,
    );
    assert!(done);
    assert_eq!(rows, [("keep/3".into(), vec![])]);
    for t in [v.snapshot, other.snapshot] {
        s.snapshot_release("ledger", &t).unwrap();
    }
    clean(&s);
}

#[test]
fn caller_and_store_caps_share_admission_with_ordinary_and_inflight_snapshots() {
    let f = Fixture::new();
    let s = Store::open(
        &f.0,
        Config {
            snapshot_max: 1,
            ..config()
        },
    )
    .unwrap();
    seed(&s);
    let (ordinary, held) = s.snapshot_open("ledger").unwrap();
    assert_eq!(
        s.snapshot_open_selected("ledger", Selection::Prefix("keep/"), budget())
            .unwrap_err(),
        AcquisitionError::SnapshotFull
    );
    s.snapshot_release("ledger", &ordinary).unwrap();
    assert_eq!(
        s.snapshot_open_selected("ledger", Selection::Prefix("keep/"), budget())
            .unwrap_err(),
        AcquisitionError::SnapshotFull
    );
    drop(held);
    clean(&s);
    let v = s
        .snapshot_open_selected("ledger", Selection::Prefix("keep/"), budget())
        .unwrap();
    let held = s.snapshot_get("ledger", &v.snapshot).unwrap();
    s.snapshot_release("ledger", &v.snapshot).unwrap();
    assert_eq!(s.scan_usage.bytes.load(Ordering::Acquire), 538);
    assert_eq!(s.snapshot_open("ledger").err(), Some(22));
    assert_eq!(s.snapshot_release("ledger", &v.snapshot), Err(20));
    drop(held);
    clean(&s);
    // Explicit selected payload storage is charged even with a zero live cache.
    let f2 = Fixture::new();
    let s2 = Store::open(
        &f2.0,
        Config {
            snapshot_bytes: 537,
            ..config()
        },
    )
    .unwrap();
    seed(&s2);
    assert_eq!(
        s2.snapshot_open_selected("ledger", Selection::Prefix("keep/"), budget())
            .unwrap_err(),
        AcquisitionError::SnapshotFull
    );
    clean(&s2);
}

#[test]
fn process_capacity_is_checked_before_materialization() {
    let f = Fixture::new();
    let s = Store::open(
        &f.0,
        Config {
            capacity: 4 * 1024 * 1024,
            snapshot_bytes: 16 * 1024 * 1024,
            snapshot_max: 32,
            ..config()
        },
    )
    .unwrap();
    put(&s, "ledger", "x", &vec![7; 600 * 1024], false);
    let mut accepted = Vec::new();
    for _ in 0..10 {
        let (result, trace) = trace_acquisition(
            &s,
            "x",
            Selection::Prefix("x"),
            AcquisitionBudget {
                max_retained_bytes: 1024 * 1024,
                ..budget()
            },
        );
        eprintln!("process capacity: result={result:?} trace={trace:?}");
        match result {
            Ok(v) => {
                trace.funded(v.retained_bytes as usize).unwrap();
                assert_eq!(trace.len, 4); // before allocation, allocated rows, copied row, owned view
                accepted.push(v);
            }
            Err(e) => {
                assert_eq!(e, AcquisitionError::SnapshotFull);
                trace.refused().unwrap();
                break;
            }
        }
    }
    assert!(!accepted.is_empty() && accepted.len() < 8);
    assert!(s.health2().memory_bytes <= 4 * 1024 * 1024);
    for v in accepted {
        s.snapshot_release("ledger", &v.snapshot).unwrap();
    }
    clean(&s);
}

#[derive(Clone, Copy, Debug, Default)]
struct Materialization {
    before_allocation: bool,
    count: usize,
    bytes: usize,
    row_capacity: usize,
    rows: usize,
    copied_key: bool,
    watched_refs: usize,
    watched_row: bool,
    owned_namespace: usize,
    owned_selector: usize,
}
#[derive(Clone, Copy, Debug, Default)]
struct MaterializationTrace {
    before_count: usize,
    before_bytes: usize,
    before_refs: usize,
    observations: [Materialization; 8],
    len: usize,
}
impl MaterializationTrace {
    fn funded(&self, charge: usize) -> Result<(), &'static str> {
        if self.len == 0 || !self.observations[0].before_allocation {
            return Err("no actual retained materialization observed");
        }
        for event in &self.observations[..self.len] {
            if event.count != self.before_count + 1 || event.bytes != self.before_bytes + charge {
                return Err("retained materialization preceded its Store count/byte reservation");
            }
            if (!event.before_allocation && event.row_capacity == 0)
                || (event.before_allocation && (event.rows != 0 || event.row_capacity != 0))
                || (event.rows != 0 && !event.copied_key)
                || event.watched_refs != self.before_refs + usize::from(event.watched_row)
            {
                return Err("actual allocated rows/key copies/entry references were not witnessed");
            }
        }
        let last = self.observations[self.len - 1];
        if last.owned_namespace == 0 || last.owned_selector == 0 {
            return Err("owned namespace and selector were not witnessed");
        }
        Ok(())
    }
    fn refused(&self) -> Result<(), &'static str> {
        if self.len != 0 {
            return Err("refused acquisition transiently materialized retained metadata");
        }
        Ok(())
    }
}

fn trace_acquisition(
    s: &Store,
    watched_key: &str,
    selection: Selection<'_>,
    budget: AcquisitionBudget,
) -> (Result<SelectedView, AcquisitionError>, MaterializationTrace) {
    use std::{cell::RefCell, rc::Rc};
    let committed = s.committed.read().unwrap();
    let (key, entry) = committed
        .get_key_value(&("ledger".into(), watched_key.into()))
        .unwrap();
    let key_address = key.1.as_ptr() as usize;
    let watched = entry.clone();
    drop(committed);
    let record = Rc::new(RefCell::new(MaterializationTrace {
        before_count: s.scan_usage.count.load(Ordering::Acquire),
        before_bytes: s.scan_usage.bytes.load(Ordering::Acquire),
        before_refs: Arc::strong_count(&watched),
        ..MaterializationTrace::default()
    }));
    let out = record.clone();
    let watched_key = watched_key.to_owned();
    MATERIALIZED.with(|hook| {
        assert!(hook.borrow().is_none());
        *hook.borrow_mut() = Some(Box::new(
            move |store, rows, capacity, view, before_allocation| {
                let mut out = out.borrow_mut();
                assert!(
                    out.len < out.observations.len(),
                    "bounded observation record exhausted"
                );
                let watched_row = rows.iter().find(|(k, _)| k == &watched_key);
                let (owned_namespace, owned_selector) = view.map_or((0, 0), |v| {
                    let selector = match v.selection.as_ref().unwrap() {
                        CapturedSelection::Prefix(p) => p.capacity(),
                        CapturedSelection::Range(lo, hi) => lo.capacity() + hi.capacity(),
                    };
                    (v.ns.capacity(), selector)
                });
                let event = Materialization {
                    before_allocation,
                    count: store.scan_usage.count.load(Ordering::Acquire),
                    bytes: store.scan_usage.bytes.load(Ordering::Acquire),
                    row_capacity: capacity,
                    rows: rows.len(),
                    copied_key: watched_row
                        .is_some_and(|(k, _)| k.as_ptr() as usize != key_address),
                    watched_refs: Arc::strong_count(&watched),
                    watched_row: watched_row.is_some(),
                    owned_namespace,
                    owned_selector,
                };
                let n = out.len;
                out.observations[n] = event;
                out.len += 1;
            },
        ));
    });
    struct Clear;
    impl Drop for Clear {
        fn drop(&mut self) {
            MATERIALIZED.with(|hook| {
                hook.take();
            });
        }
    }
    let clear = Clear;
    let result = s.snapshot_open_selected("ledger", selection, budget);
    drop(clear);
    let trace = *record.borrow();
    (result, trace)
}

#[test]
fn temporal_oracle_observes_funding_at_actual_row_key_reference_and_selector_materialization() {
    let f = Fixture::new();
    let s = Store::open(&f.0, config()).unwrap();
    seed(&s);
    let (result, trace) = trace_acquisition(&s, "keep/1", Selection::Prefix("keep/"), budget());
    eprintln!("accepted materialization: result={result:?} trace={trace:?}");
    let v = result.unwrap();
    trace.funded(v.retained_bytes as usize).unwrap();
    assert_eq!(trace.len, 5);
    assert_eq!(
        trace.observations[..5]
            .iter()
            .map(|e| e.rows)
            .collect::<Vec<_>>(),
        [0, 0, 1, 2, 2]
    );
    assert_eq!(v.retained_bytes, 538);
    s.snapshot_release("ledger", &v.snapshot).unwrap();
    clean(&s);
}

#[test]
fn temporal_oracle_observes_no_retained_materialization_on_store_refusal() {
    for c in [
        Config {
            snapshot_max: 0,
            ..config()
        },
        Config {
            snapshot_bytes: 537,
            ..config()
        },
    ] {
        let f = Fixture::new();
        let s = Store::open(&f.0, c).unwrap();
        seed(&s);
        let (result, trace) = trace_acquisition(&s, "keep/1", Selection::Prefix("keep/"), budget());
        eprintln!("refused materialization: result={result:?} trace={trace:?}");
        assert_eq!(result.unwrap_err(), AcquisitionError::SnapshotFull);
        trace.refused().unwrap();
        clean(&s);
    }
}

#[test]
fn expiry_during_lock_wait_is_no_partial_open_and_utc_jump_is_not_expiry() {
    let f = Fixture::new();
    let clock = ManualClock::new(crate::now_ms());
    let s = Arc::new(
        Store::open(
            &f.0,
            Config {
                clock: Some(clock.clone()),
                snapshot_ttl_ms: 1000,
                ..config()
            },
        )
        .unwrap(),
    );
    let locked = s.committed.write().unwrap();
    let worker = {
        let s = s.clone();
        std::thread::spawn(move || {
            s.snapshot_open_selected("ledger", Selection::Prefix(""), budget())
        })
    };
    let until = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while s.capture_waiters.load(Ordering::SeqCst) == 0 && std::time::Instant::now() < until {
        std::thread::yield_now();
    }
    let waited = s.capture_waiters.load(Ordering::SeqCst) > 0;
    clock.advance(1000);
    drop(locked);
    let result = worker.join().unwrap();
    assert!(waited);
    assert_eq!(result.unwrap_err(), AcquisitionError::Expired);
    clean(&s);
    let v = s
        .snapshot_open_selected("ledger", Selection::Prefix(""), budget())
        .unwrap();
    clock.jump_utc(86_400_000);
    assert!(s.snapshot_get("ledger", &v.snapshot).is_ok());
    clock.advance(1000);
    assert_eq!(s.snapshot_get("ledger", &v.snapshot).err(), Some(20));
    clean(&s);
}

#[test]
fn replay_epoch_refuses_old_tokens_and_recaptures_the_persisted_selection() {
    let f = Fixture::new();
    let s = Store::open(&f.0, config()).unwrap();
    seed(&s);
    let v = s
        .snapshot_open_selected("ledger", Selection::Prefix("keep/"), budget())
        .unwrap();
    drop(s);
    let s = Store::open(&f.0, config()).unwrap();
    assert_eq!(s.snapshot_get("ledger", &v.snapshot).err(), Some(20));
    let fresh = s
        .snapshot_open_selected("ledger", Selection::Prefix("keep/"), budget())
        .unwrap();
    assert_ne!(&fresh.snapshot[..8], &v.snapshot[..8]);
    assert_eq!(fresh.population, 2);
    let (_, rows, done) = decode(
        &page(
            &s,
            "ledger",
            &fresh.snapshot,
            SCAN_PREFIX,
            "keep/",
            "",
            &[],
            128,
        )
        .unwrap(),
        false,
    );
    assert!(done);
    assert_eq!(
        rows,
        [
            ("keep/1".into(), vec![0, 255, 10]),
            ("keep/3".into(), vec![])
        ]
    );
    s.snapshot_release("ledger", &fresh.snapshot).unwrap();
    clean(&s);
}

#[test]
fn unavailable_and_corrupt_storage_never_becomes_empty_success() {
    use std::{
        fs::OpenOptions,
        io::{Seek, SeekFrom, Write},
    };
    let f = Fixture::new();
    let s = Store::open(&f.0, config()).unwrap();
    seed(&s);
    s.set_warming(true);
    assert_eq!(
        s.snapshot_open_selected("ledger", Selection::Prefix("keep/"), budget())
            .unwrap_err(),
        AcquisitionError::Warming
    );
    s.set_warming(false);
    s.set_read_only(true);
    assert_eq!(
        s.snapshot_open_selected("ledger", Selection::Prefix("keep/"), budget())
            .unwrap_err(),
        AcquisitionError::ReadOnly
    );
    s.set_read_only(false);
    clean(&s);
    let v = s
        .snapshot_open_selected("ledger", Selection::Prefix("keep/"), budget())
        .unwrap();
    let entry = s
        .committed
        .read()
        .unwrap()
        .get(&("ledger".into(), "keep/1".into()))
        .unwrap()
        .clone();
    let payload = entry.location.payload().unwrap();
    let original = chunks::frame(b"MBK1", &payload);
    let path = fs::read_dir(f.0.join("chunks/forever"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| p.extension().is_some_and(|e| e == "log"))
        .unwrap();
    // Change this exact selected frame, retaining the file and index metadata.
    {
        let _g = entry.location.file.lock().unwrap();
        let mut file = OpenOptions::new().write(true).open(&path).unwrap();
        file.seek(SeekFrom::Start(entry.location.offset)).unwrap();
        let mut corrupt = original.clone();
        corrupt[16] ^= 1;
        file.write_all(&corrupt).unwrap();
        file.flush().unwrap();
    }
    assert_eq!(
        page(
            &s,
            "ledger",
            &v.snapshot,
            SCAN_PREFIX,
            "keep/",
            "",
            &[],
            128
        )
        .unwrap_err(),
        16
    );
    assert_eq!(
        s.snapshot_open_selected("empty", Selection::Prefix(""), budget())
            .unwrap_err(),
        AcquisitionError::StorageUnavailable
    );
    s.snapshot_release("ledger", &v.snapshot).unwrap();
    clean(&s);
}

#[test]
fn simultaneous_selected_acquisitions_cannot_overbook_store_bytes() {
    let f = Fixture::new();
    let s = Arc::new(
        Store::open(
            &f.0,
            Config {
                snapshot_bytes: 538,
                ..config()
            },
        )
        .unwrap(),
    );
    seed(&s);
    let barrier = Arc::new(std::sync::Barrier::new(6));
    let workers: Vec<_> = (0..6)
        .map(|_| {
            let (s, b) = (s.clone(), barrier.clone());
            std::thread::spawn(move || {
                b.wait();
                s.snapshot_open_selected("ledger", Selection::Prefix("keep/"), budget())
            })
        })
        .collect();
    let mut accepted = Vec::new();
    for w in workers {
        match w.join().unwrap() {
            Ok(v) => accepted.push(v),
            Err(e) => assert_eq!(e, AcquisitionError::SnapshotFull),
        }
    }
    assert_eq!(accepted.len(), 1);
    s.snapshot_release("ledger", &accepted[0].snapshot).unwrap();
    clean(&s);
}

#[test]
fn failure_after_pinning_returns_the_entire_store_reservation_and_entry_references() {
    let f = Fixture::new();
    let clock = ManualClock::new(crate::now_ms());
    let s = Arc::new(
        Store::open(
            &f.0,
            Config {
                clock: Some(clock.clone()),
                snapshot_ttl_ms: 1000,
                ..config()
            },
        )
        .unwrap(),
    );
    seed(&s);
    let entry = s
        .committed
        .read()
        .unwrap()
        .get(&("ledger".into(), "keep/1".into()))
        .unwrap()
        .clone();
    let refs = Arc::strong_count(&entry);
    let held = s.clone();
    let advance = clock.clone();
    BEFORE_PUBLISH.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            assert_eq!(held.scan_usage.bytes.load(Ordering::Acquire), 538);
            assert_eq!(held.scan_usage.count.load(Ordering::Acquire), 1);
            assert!(held.scans.lock().unwrap().snapshots.is_empty());
            advance.advance(1000);
        }))
    });
    assert_eq!(
        s.snapshot_open_selected("ledger", Selection::Prefix("keep/"), budget())
            .unwrap_err(),
        AcquisitionError::Expired
    );
    clean(&s);
    assert_eq!(Arc::strong_count(&entry), refs);
    let held = s.clone();
    BEFORE_PUBLISH.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            held.flags.storage_error.store(true, Ordering::Release)
        }))
    });
    assert_eq!(
        s.snapshot_open_selected("ledger", Selection::Prefix("keep/"), budget())
            .unwrap_err(),
        AcquisitionError::StorageUnavailable
    );
    clean(&s);
    assert_eq!(Arc::strong_count(&entry), refs);
}

#[test]
fn selected_view_is_ineligible_for_snapshot_require_complete() {
    let f = Fixture::new();
    let s = Store::open(&f.0, config()).unwrap();
    seed(&s);
    for selection in [Selection::Prefix("keep/"), Selection::Prefix("")] {
        let view = s
            .snapshot_open_selected("ledger", selection, budget())
            .unwrap();
        assert_eq!(s.snapshot_require_complete("ledger", &view.snapshot), Err(16));
        s.snapshot_release("ledger", &view.snapshot).unwrap();
    }
    let (whole, _) = s.snapshot_open("ledger").unwrap();
    assert_eq!(s.snapshot_require_complete("ledger", &whole), Ok(()));
    s.snapshot_release("ledger", &whole).unwrap();
    clean(&s);
}
