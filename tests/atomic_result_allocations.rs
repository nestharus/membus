//! Public typed inspection consumes existing bounded originals without owning copies.
use membus::{
    atomic::{
        AtomicAck, AtomicResultEvidence, BorrowedOriginal, FinalChange, OriginalBudget,
        OriginalRef, RetainedAtomicResult, RetryRetention, RESULT_INSPECTION_SCRATCH_BYTES,
    },
    Config, Store,
};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    path::PathBuf,
};

struct Counting;
thread_local! {
    static TRACK:Cell<bool>=const{Cell::new(false)};
    static ALLOCS:Cell<usize>=const{Cell::new(0)};
    static LIVE:Cell<usize>=const{Cell::new(0)};
    static PEAK:Cell<usize>=const{Cell::new(0)};
    static MAX:Cell<usize>=const{Cell::new(0)};
}
fn allocation(bytes: usize) {
    TRACK.with(|active| {
        if active.get() {
            ALLOCS.with(|n| n.set(n.get() + 1));
            MAX.with(|n| n.set(n.get().max(bytes)));
            LIVE.with(|n| {
                n.set(n.get() + bytes);
                PEAK.with(|peak| peak.set(peak.get().max(n.get())))
            });
        }
    });
}
fn deallocation(bytes: usize) {
    TRACK.with(|active| {
        if active.get() {
            LIVE.with(|n| {
                n.set(
                    n.get()
                        .checked_sub(bytes)
                        .expect("only query-owned scratch may be freed"),
                )
            })
        }
    });
}
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        allocation(l.size());
        unsafe { System.alloc(l) }
    }
    unsafe fn alloc_zeroed(&self, l: Layout) -> *mut u8 {
        allocation(l.size());
        unsafe { System.alloc_zeroed(l) }
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        deallocation(l.size());
        unsafe { System.dealloc(p, l) }
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, n: usize) -> *mut u8 {
        deallocation(l.size());
        allocation(n);
        unsafe { System.realloc(p, l, n) }
    }
}
#[global_allocator]
static ALLOCATOR: Counting = Counting;
#[derive(Debug)]
struct Allocation {
    count: usize,
    maximum: usize,
    peak: usize,
    remaining: usize,
}
fn measured<T>(f: impl FnOnce() -> T) -> (T, Allocation) {
    ALLOCS.with(|n| n.set(0));
    LIVE.with(|n| n.set(0));
    PEAK.with(|n| n.set(0));
    MAX.with(|n| n.set(0));
    TRACK.with(|n| n.set(true));
    let value = f();
    TRACK.with(|n| n.set(false));
    (
        value,
        Allocation {
            count: ALLOCS.with(Cell::get),
            maximum: MAX.with(Cell::get),
            peak: PEAK.with(Cell::get),
            remaining: LIVE.with(Cell::get),
        },
    )
}
fn blob(out: &mut Vec<u8>, b: &[u8]) {
    out.extend((b.len() as u32).to_le_bytes());
    out.extend(b);
}
fn authored_original() -> Vec<u8> {
    let mut b = b"MAR1".to_vec();
    blob(&mut b, b"allocation");
    blob(&mut b, b"one-original");
    b.extend(7u64.to_le_bytes());
    b.extend(1234u64.to_le_bytes());
    b.push(2);
    b.extend(4161u32.to_le_bytes());
    for i in 0..4161 {
        blob(&mut b, format!("guard-{i:08}").as_bytes());
        b.extend((i as u64 + 1).to_le_bytes());
    }
    b.extend(1u16.to_le_bytes());
    blob(&mut b, b"result");
    b.push(0);
    blob(&mut b, &vec![0x5a; membus::MAX_VALUE]);
    b
}

#[test]
fn funded_4161_guard_max_value_inspection_has_bounded_hash_scratch_without_body_copy() {
    let root = PathBuf::from(
        std::env::var_os("MEMBUS_PROVIDER_TEST_ROOT").expect("explicit retained fixture root"),
    );
    assert!(root.is_absolute() && root.is_dir());
    let path = root.join(format!(
        "public-result-allocation-{}-{}",
        std::process::id(),
        membus::now_ms()
    ));
    let s = Store::open(
        &path,
        Config {
            cache_bytes: 0,
            signal_bytes: 0,
            signal_history: 0,
            tick_ms: 3_600_000,
            retire_ms: 3_600_000,
            flush_ms: 0,
            ..Config::default()
        },
    )
    .unwrap();
    let bytes = authored_original();
    let borrowed = BorrowedOriginal::from_bytes(&bytes).unwrap();
    let budget = OriginalBudget::new(16 * 1024 * 1024);
    let owned = borrowed.to_owned(&budget).unwrap();
    let used = budget.used();
    assert_eq!(owned.to_bytes(), bytes);
    assert_eq!(borrowed.population().count(), 4161);
    let before = (s.io_metrics(), s.state_bytes(), s.slots("allocation"));
    let scratch = OriginalBudget::new(RESULT_INSPECTION_SCRATCH_BYTES);
    let ((a, b), allocations) = measured(|| {
        (
            s.inspect_atomic_result(OriginalRef::Request(&owned), &scratch),
            s.inspect_atomic_result(OriginalRef::Borrowed(&borrowed), &scratch),
        )
    });
    // Existing SHA uses small temporary buffers. Copying the frame or guard
    // population would breach this independent fixed scratch allowance.
    assert!(
        allocations.maximum <= 128 && allocations.peak <= 256,
        "{allocations:?}"
    );
    assert_eq!(allocations.remaining, 0);
    println!("large original inspection scratch: {allocations:?}");
    let (a, b) = (a.unwrap(), b.unwrap());
    assert_eq!(a.evidence, AtomicResultEvidence::NotRetained);
    assert_eq!(a.fingerprint, b.fingerprint);
    assert_eq!(budget.used(), used);
    assert_eq!(scratch.used(), 0);
    assert_eq!(
        (s.io_metrics(), s.state_bytes(), s.slots("allocation")),
        before
    );

    let r = s
        .capture_namespace_funded("actual", 64 * 1024)
        .unwrap()
        .prepare(
            "known",
            AtomicAck::Flush,
            vec![FinalChange::put("value", b"opaque".to_vec())],
        )
        .unwrap();
    let result = s.publish_atomic(&r, membus::now_ms() + 30_000).unwrap();
    let bytes = r.to_bytes();
    let borrowed = BorrowedOriginal::from_bytes(&bytes).unwrap();
    let ((a, b), allocations) = measured(|| {
        (
            s.inspect_atomic_result(OriginalRef::Request(&r), &scratch),
            s.inspect_atomic_result(OriginalRef::Borrowed(&borrowed), &scratch),
        )
    });
    assert!(
        allocations.maximum <= 128 && allocations.peak <= 256,
        "{allocations:?}"
    );
    assert_eq!(allocations.remaining, 0);
    println!("retained inspection scratch: {allocations:?}");
    for i in [a.unwrap(), b.unwrap()] {
        assert!(matches!(
            i.evidence,
            AtomicResultEvidence::Matching {
                retention: RetryRetention::WithinWindow,
                result: RetainedAtomicResult::Committed {
                    version: 1,
                    achieved: AtomicAck::Flush
                },
                ..
            }
        ));
    }
    assert_eq!(result.version, 1);
    assert_eq!(scratch.used(), 0);

    // Validation is the existing bounded borrowed validator, before any query.
    for invalid in [&b"MAR1"[..], &bytes[..bytes.len() - 1]] {
        assert!(BorrowedOriginal::from_bytes(invalid).is_err());
    }
    let oversized = vec![0; membus::MAX_FRAME / 3];
    assert!(BorrowedOriginal::from_bytes(&oversized).is_err());
    let (_, probe) = measured(|| {
        let mut v = Vec::<u8>::with_capacity(64);
        v.push(1);
        drop(std::hint::black_box(v));
    });
    assert!(
        probe.count > 0 && probe.remaining == 0,
        "counting allocator hook must observe heap allocations: {probe:?}"
    );
    assert_eq!(
        allocations.count, 0,
        "stack-buffered SHA-256 inspection performs zero heap allocations"
    );
    println!("public inspection: guards=4161 selected-value={} original_budget_before_after={} retained_version=1",membus::MAX_VALUE,used);
}
