//! Allocation cost and frozen wire compatibility of the public framing helpers.
use membus::{frame, read_frame, reply, GET, MAX_FRAME};
use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
    io::Cursor,
};

thread_local! {
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static CALLS: Cell<usize> = const { Cell::new(0) };
    static BYTES: Cell<usize> = const { Cell::new(0) };
    static MINIMUM: Cell<usize> = const { Cell::new(0) };
}
struct Tracked;
fn record(size: usize) {
    let _ = ACTIVE.try_with(|active| {
        if active.get()
            && MINIMUM
                .try_with(Cell::get)
                .is_ok_and(|minimum| size >= minimum)
        {
            let _ = CALLS.try_with(|n| n.set(n.get().saturating_add(1)));
            let _ = BYTES.try_with(|n| n.set(n.get().saturating_add(size)));
        }
    });
}
// SAFETY: This test-only allocator forwards every allocation and deallocation
// unchanged to System. Const thread-local Cells record requests without heap
// allocation; tracking is confined to the current test thread's measured call.
unsafe impl GlobalAlloc for Tracked {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record(layout.size());
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            record(layout.size());
        }
        ptr
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, size: usize) -> *mut u8 {
        let ptr = unsafe { System.realloc(ptr, layout, size) };
        if !ptr.is_null() {
            record(size);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}
#[global_allocator]
static ALLOCATOR: Tracked = Tracked;
struct Stop;
impl Drop for Stop {
    fn drop(&mut self) {
        ACTIVE.with(|v| v.set(false));
    }
}
fn observed(call: impl FnOnce() -> Vec<u8>) -> (Vec<u8>, usize, usize) {
    observed_above(0, call)
}
fn observed_above(minimum: usize, call: impl FnOnce() -> Vec<u8>) -> (Vec<u8>, usize, usize) {
    CALLS.with(|n| n.set(0));
    BYTES.with(|n| n.set(0));
    MINIMUM.with(|n| n.set(minimum));
    ACTIVE.with(|v| v.set(true));
    let stop = Stop;
    let output = call();
    drop(stop);
    (output, CALLS.with(Cell::get), BYTES.with(Cell::get))
}
#[test]
fn frame_uses_one_exact_output_allocation() {
    let payload = vec![7; 65_536];
    let (output, calls, bytes) = observed(|| frame(GET, 42, &payload));
    eprintln!(
        "frame allocation requests={calls} requested_bytes={bytes} output_bytes={}",
        output.len()
    );
    assert_eq!(output.len(), 17 + payload.len());
    assert_eq!(
        calls, 1,
        "framing must not allocate intermediate payload buffers"
    );
    assert_eq!(bytes, output.len(), "reserve only the complete output size");
}
#[test]
fn reply_uses_one_exact_output_allocation() {
    let payload = vec![9; 65_536];
    let (output, calls, bytes) = observed(|| reply(GET, 42, 16, &payload));
    eprintln!(
        "reply allocation requests={calls} requested_bytes={bytes} output_bytes={}",
        output.len()
    );
    assert_eq!(output.len(), 19 + payload.len());
    assert_eq!(
        calls, 1,
        "replies must not allocate intermediate status or frame buffers"
    );
    assert_eq!(bytes, output.len(), "reserve only the complete output size");
}
// Frozen pre-optimization framing layout, independent of either public helper.
fn original(op: u8, id: u64, status: Option<u16>, payload: &[u8]) -> Vec<u8> {
    let mut body = vec![1, 0, 0, 0, op];
    body.extend(id.to_le_bytes());
    if let Some(code) = status {
        body.extend(code.to_le_bytes());
    }
    body.extend(payload);
    let mut out = (body.len() as u32).to_le_bytes().to_vec();
    out.extend(body);
    out
}
#[test]
fn framing_preserves_frozen_wire_images_and_all_opcode_bits() {
    let id = 0x0102030405060708;
    assert_eq!(
        frame(GET, id, &[0, 127, 255]),
        vec![16, 0, 0, 0, 1, 0, 0, 0, 2, 8, 7, 6, 5, 4, 3, 2, 1, 0, 127, 255]
    );
    assert_eq!(
        reply(GET, id, 16, &[0, 127, 255]),
        vec![18, 0, 0, 0, 1, 0, 0, 0, 130, 8, 7, 6, 5, 4, 3, 2, 1, 16, 0, 0, 127, 255]
    );
    for op in 0..=u8::MAX {
        for id in [0, u64::MAX, 0x1122334455667788] {
            let payload = [op; 33];
            for len in [0, 1, 7, 8, 15, 16, 17, 31, 32, 33] {
                let payload = &payload[..len];
                assert_eq!(frame(op, id, payload), original(op, id, None, payload));
                for code in [0, 16, u16::MAX] {
                    assert_eq!(
                        reply(op, id, code, payload),
                        original(op | 0x80, id, Some(code), payload)
                    );
                }
            }
        }
    }
}
#[test]
fn framing_preserves_large_payloads_and_the_exact_maximum_reply() {
    for len in [4096, 65_536, 4 * 1024 * 1024, MAX_FRAME - 15] {
        let payload = vec![0x9d; len];
        let request = frame(GET, u64::MAX, &payload);
        assert_eq!(request, original(GET, u64::MAX, None, &payload));
        let response = reply(GET, u64::MAX, 16, &payload);
        assert_eq!(response, original(GET | 0x80, u64::MAX, Some(16), &payload));
        let decoded = read_frame(&mut Cursor::new(&response)).unwrap();
        assert_eq!(decoded, &response[4..]);
        assert_eq!(decoded.len(), len + 15);
    }
}

struct PageFixture(std::path::PathBuf);
impl Drop for PageFixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn page_fixture(label: &str) -> PageFixture {
    let stamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let path = std::env::var_os("MEMBUS_TEST_OUTPUT")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".test-output")
        })
        .join(format!("page-{label}-{}-{stamp}", std::process::id()));
    std::fs::create_dir_all(&path).unwrap();
    PageFixture(path)
}
fn page_store(fixture: &PageFixture) -> membus::Store {
    membus::Store::open(
        &fixture.0,
        membus::Config {
            capacity: 64 * 1024 * 1024,
            cache_bytes: 8 * 1024 * 1024,
            snapshot_bytes: 32 * 1024 * 1024,
            signal_bytes: 0,
            clock: Some(membus::ManualClock::new(membus::now_ms())),
            tick_ms: 3_600_000,
            retire_ms: 3_600_000,
            cache_policy: membus::CachePolicy {
                initial_ttl: 0,
                active_ttl: 3_600_000,
                ..membus::CachePolicy::default()
            },
            ..membus::Config::default()
        },
    )
    .unwrap()
}
fn page_request(op: u8, id: u64, suffix: &[u8]) -> Vec<u8> {
    let mut p = (membus::now_ms() + 30_000).to_le_bytes().to_vec();
    membus::lp(&mut p, b"scan-output");
    membus::lp(&mut p, b"page-buffers");
    p.extend_from_slice(suffix);
    frame(op, id, &p)[4..].to_vec()
}
fn page_token(store: &membus::Store) -> Vec<u8> {
    let out = store.dispatch(&page_request(membus::SNAPSHOT_OPEN, 1, &[]));
    assert_eq!(&out[17..19], &[0, 0]);
    membus::Cursor::new(&out[19..]).bytes(16).unwrap().to_vec()
}
fn page_command(op: u8, token: &[u8], count: usize, budget: usize) -> Vec<u8> {
    let mut suffix = Vec::new();
    membus::lp(&mut suffix, token);
    membus::lp(&mut suffix, &[]);
    suffix.extend((count as u16).to_le_bytes());
    suffix.extend((budget as u32).to_le_bytes());
    membus::lp(&mut suffix, &[]);
    if matches!(op, membus::SCAN_RANGE | membus::SCAN_KEYS_RANGE) {
        membus::lp(&mut suffix, &[]);
    }
    page_request(op, 0x1122334455667788, &suffix)
}
fn scan_cost(keys_only: bool) {
    let mut failures = Vec::new();
    let counts: &[usize] = if keys_only { &[80] } else { &[1, 8] };
    for &count in counts {
        let fixture = page_fixture(if keys_only { "keys" } else { "values" });
        let store = page_store(&fixture);
        let mut rows = Vec::new();
        for i in 0..count {
            let key = if keys_only {
                format!("{i:04}{}", "k".repeat(1020))
            } else {
                format!("row/{i:04}")
            };
            let value = vec![(i % 251) as u8; if keys_only { 1 } else { 65_536 }];
            let result = store.mutate(membus::Mutation {
                ns: "page-buffers".into(),
                key: key.clone(),
                op_id: format!("seed-{i}"),
                deadline: membus::now_ms() + 30_000,
                ack: 1,
                expected: None,
                bytes: value.clone(),
                remove: false,
            });
            assert_eq!(result.code, 0);
            rows.push((key, value, result.version));
        }
        let token = page_token(&store);
        let mut payload = Vec::new();
        membus::lp(&mut payload, &token);
        membus::lp(&mut payload, &[]);
        payload.push(1);
        payload.extend((count as u16).to_le_bytes());
        for (key, value, version) in &rows {
            membus::lp(&mut payload, key.as_bytes());
            if !keys_only {
                payload.push(1);
                payload.extend(version.to_le_bytes());
                payload.push(1);
                membus::lp(&mut payload, value);
            }
        }
        let ops = if keys_only {
            [membus::SCAN_KEYS_PREFIX, membus::SCAN_KEYS_RANGE]
        } else {
            [membus::SCAN_PREFIX, membus::SCAN_RANGE]
        };
        for op in ops {
            let request = page_command(op, &token, count, MAX_FRAME);
            let expected = original(op | 0x80, 0x1122334455667788, Some(0), &payload);
            assert_eq!(store.dispatch(&request), expected);
            let cold_before = store.cache_stats().cold_reads;
            let (out, calls, bytes) = observed_above(65_536, || store.dispatch(&request));
            assert_eq!(out, expected, "frozen page wire layout must be preserved");
            assert_eq!(store.cache_stats().cold_reads, cold_before);
            eprintln!("scan op={op} rows={count} large_allocations={calls} requested_bytes={bytes} output_bytes={}", out.len());
            if calls != 1 || bytes != out.len() {
                failures.push((op, count, calls, bytes, out.len()));
            }
        }
        if keys_only {
            assert_eq!(store.cache_stats().cold_reads, 0);
        }
        assert!(store.cache_stats().bytes <= store.cache_stats().limit);
        let mut suffix = Vec::new();
        membus::lp(&mut suffix, &token);
        let released = store.dispatch(&page_request(membus::SNAPSHOT_RELEASE, 2, &suffix));
        assert_eq!(&released[17..], &[0, 0]);
    }
    assert!(
        failures.is_empty(),
        "temporary full-page buffers: {failures:?}"
    );
}
#[test]
fn snapshot_value_pages_use_one_large_output_allocation() {
    scan_cost(false);
}
#[test]
fn snapshot_key_pages_use_one_large_output_allocation() {
    scan_cost(true);
}
#[test]
fn snapshot_reply_preserves_exact_transport_limit_and_refusal_identity() {
    let fixture = page_fixture("limit");
    let store = page_store(&fixture);
    assert_eq!(
        store
            .mutate(membus::Mutation {
                ns: "page-buffers".into(),
                key: "row/0000".into(),
                op_id: "seed-limit".into(),
                deadline: membus::now_ms() + 30_000,
                ack: 1,
                expected: None,
                bytes: vec![4; 65_536],
                remove: false,
            })
            .code,
        0
    );
    let token = page_token(&store);
    let body_limit = 42 + 4 + 8 + 14 + 65_536;
    for op in [membus::SCAN_PREFIX, membus::SCAN_RANGE] {
        let request = page_command(op, &token, 1, MAX_FRAME);
        let accepted = store.dispatch_for_limit(&request, false, body_limit);
        assert_eq!(accepted.len(), body_limit + 4);
        assert_eq!(&accepted[17..19], &[0, 0]);
        assert_eq!(
            u32::from_le_bytes(accepted[..4].try_into().unwrap()) as usize,
            body_limit
        );
        assert_eq!(
            read_frame(&mut Cursor::new(&accepted)).unwrap(),
            accepted[4..]
        );
        let refused = store.dispatch_for_limit(&request, false, body_limit - 1);
        assert_eq!(
            refused,
            original(op | 0x80, 0x1122334455667788, Some(4), &[])
        );
        assert_eq!(
            store.dispatch_for_limit(&request, false, body_limit),
            accepted
        );
    }
}

fn committed_read_cost(op: u8) {
    let fixture = page_fixture(if op == GET { "get" } else { "get-many" });
    let store = page_store(&fixture);
    let value = vec![0x6d; 65_536];
    let result = store.mutate(membus::Mutation {
        ns: "page-buffers".into(),
        key: "definition/1".into(),
        op_id: "seed-definition".into(),
        deadline: membus::now_ms() + 30_000,
        ack: 1,
        expected: None,
        bytes: value.clone(),
        remove: false,
    });
    assert_eq!(result.code, 0);
    let mut suffix = Vec::new();
    let mut payload = Vec::new();
    if op == membus::GET_MANY {
        suffix.extend(3u16.to_le_bytes());
        payload.extend(3u16.to_le_bytes());
    }
    let keys: &[&str] = if op == GET {
        &["definition/1"]
    } else {
        &["definition/1", "missing", "definition/1"]
    };
    for key in keys {
        membus::lp(&mut suffix, key.as_bytes());
        if *key == "missing" {
            payload.push(0);
        } else {
            payload.push(1);
            payload.extend(result.version.to_le_bytes());
            payload.push(1);
            membus::lp(&mut payload, &value);
        }
    }
    let request = page_request(op, 0x1122334455667788, &suffix);
    let expected = original(op | 0x80, 0x1122334455667788, Some(0), &payload);
    assert_eq!(store.dispatch(&request), expected);
    let before = store.cache_stats();
    let (output, calls, bytes) = observed_above(65_536, || store.dispatch(&request));
    let after = store.cache_stats();
    assert_eq!(output, expected);
    assert_eq!(after.cold_reads, before.cold_reads);
    assert_eq!(after.hits - before.hits, if op == GET { 1 } else { 2 });
    eprintln!(
        "committed op={op} large_allocations={calls} requested_bytes={bytes} output_bytes={}",
        output.len()
    );
    // A byte smaller refuses the complete result; an exact fit preserves it.
    let limit = expected.len() - 4;
    assert_eq!(store.dispatch_for_limit(&request, false, limit), expected);
    assert_eq!(
        store.dispatch_for_limit(&request, false, limit - 1),
        original(op | 0x80, 0x1122334455667788, Some(4), &[])
    );
    assert_eq!(store.cache_stats().cold_reads, after.cold_reads);
    assert_eq!(calls, 1, "temporary committed-read payload buffer");
    assert_eq!(bytes, output.len(), "reserve only the final wire image");
}
#[test]
fn committed_get_uses_one_large_output_allocation() {
    committed_read_cost(GET);
}
#[test]
fn committed_many_uses_one_large_output_allocation() {
    committed_read_cost(membus::GET_MANY);
}

#[test]
fn cold_metadata_validation_avoids_owned_identifier_copies() {
    let fixture = page_fixture("cold-metadata");
    let store = page_store(&fixture);
    // Maximum-length identifiers make discarded metadata copies distinguishable
    // from the small parser/cache bookkeeping allocations in this measurement.
    let ns = "n".repeat(1024);
    let key = "k".repeat(1024);
    let operation = "o".repeat(1024);
    let value = vec![0x6d; 8192];
    let written = store.mutate(membus::Mutation {
        ns: ns.clone(),
        key: key.clone(),
        op_id: operation.clone(),
        deadline: membus::now_ms() + 30_000,
        ack: 1,
        expected: None,
        bytes: value.clone(),
        remove: false,
    });
    assert_eq!(written.code, 0);
    let mut body = (membus::now_ms() + 30_000).to_le_bytes().to_vec();
    membus::lp(&mut body, operation.as_bytes());
    membus::lp(&mut body, ns.as_bytes());
    membus::lp(&mut body, key.as_bytes());
    let input = frame(GET, 99, &body)[4..].to_vec();
    let mut payload = vec![1];
    payload.extend(written.version.to_le_bytes());
    payload.push(1);
    membus::lp(&mut payload, &value);
    let expected = original(GET | 0x80, 99, Some(0), &payload);
    let before = store.cache_stats();
    let (cold, cold_calls, cold_bytes) = observed_above(1024, || store.dispatch(&input));
    let loaded = store.cache_stats();
    let (warm, warm_calls, warm_bytes) = observed_above(1024, || store.dispatch(&input));
    let reused = store.cache_stats();
    assert_eq!(cold, expected);
    assert_eq!(warm, expected);
    assert_eq!(loaded.cold_reads - before.cold_reads, 1);
    assert_eq!(reused.cold_reads, loaded.cold_reads);
    assert_eq!(reused.hits - loaded.hits, 1);
    assert_eq!(loaded.bytes, reused.bytes);
    eprintln!("cold metadata allocations={cold_calls} bytes={cold_bytes}; warm allocations={warm_calls} bytes={warm_bytes}; extra={}", cold_calls.saturating_sub(warm_calls));
    // Cold reads need the checked record buffer and final value allocation.
    // Namespace/key/operation identity should be compared in the record buffer.
    assert_eq!(
        cold_calls,
        warm_calls + 2,
        "discarded owned record identifiers"
    );
}

fn request_identifier_cost(op: u8) {
    let fixture = page_fixture(if op == GET {
        "request-single"
    } else {
        "request-many"
    });
    let store = page_store(&fixture);
    let ns = format!("{}n", "界".repeat(341));
    let key = format!("{}k", "键".repeat(341));
    let operation = format!("{}o", "操".repeat(341));
    let missing = format!("{}x", "缺".repeat(341));
    assert_eq!(ns.len(), membus::MAX_ID);
    let bytes = vec![0x73; 8192];
    let written = store.mutate(membus::Mutation {
        ns: ns.clone(),
        key: key.clone(),
        op_id: "seed-request".into(),
        deadline: membus::now_ms() + 30_000,
        ack: 1,
        expected: None,
        bytes: bytes.clone(),
        remove: false,
    });
    assert_eq!(written.code, 0);
    let keys: Vec<&str> = if op == GET {
        vec![&key]
    } else {
        vec![&key, &missing, &key]
    };
    let mut body = (membus::now_ms() + 30_000).to_le_bytes().to_vec();
    membus::lp(&mut body, operation.as_bytes());
    membus::lp(&mut body, ns.as_bytes());
    let mut payload = Vec::new();
    if op == membus::GET_MANY {
        body.extend((keys.len() as u16).to_le_bytes());
        payload.extend((keys.len() as u16).to_le_bytes());
    }
    for item in &keys {
        membus::lp(&mut body, item.as_bytes());
        if *item == missing {
            payload.push(0);
        } else {
            payload.push(1);
            payload.extend(written.version.to_le_bytes());
            payload.push(1);
            membus::lp(&mut payload, &bytes);
        }
    }
    let input = frame(op, 0x9988776655443322, &body)[4..].to_vec();
    let frozen = original(op | 0x80, 0x9988776655443322, Some(0), &payload);
    assert_eq!(store.dispatch(&input), frozen);
    let before = store.cache_stats();
    let (output, calls, requested) = observed_above(1024, || store.dispatch(&input));
    let after = store.cache_stats();
    assert_eq!(output, frozen);
    assert_eq!(after.cold_reads, before.cold_reads);
    assert_eq!(after.hits - before.hits, if op == GET { 1 } else { 2 });
    assert_eq!(after.misses, before.misses);
    assert_eq!(after.bytes, before.bytes);
    let index_copies = 2 * keys.len();
    eprintln!("request identifiers op={op} allocations={calls} requested_bytes={requested} output_bytes={} cache_hits={} cache_bytes={}", output.len(), after.hits-before.hits, after.bytes);
    // Allow the final output and the existing owned index-lookup pairs. Wire
    // identifiers need no additional owned copies. Smaller future costs pass.
    assert!(
        calls <= index_copies + 1,
        "discarded request identifier allocations: {calls}"
    );
    assert!(
        requested <= output.len() + index_copies * membus::MAX_ID,
        "discarded request identifier bytes: {requested}"
    );
}
#[test]
fn request_identifiers_cached_get_avoids_discarded_copies() {
    request_identifier_cost(GET);
}
#[test]
fn request_identifiers_cached_many_avoids_discarded_copies() {
    request_identifier_cost(membus::GET_MANY);
}

// Author the complete request layout independently of frame()/lp().
fn identifier_request(op: u8, operation: &[u8], ns: &[u8], key: &[u8], deadline: u64) -> Vec<u8> {
    let mut out = vec![1, 0, 0, 0, op];
    out.extend(91u64.to_le_bytes());
    out.extend(deadline.to_le_bytes());
    for item in [operation, ns, key] {
        out.extend((item.len() as u32).to_le_bytes());
        out.extend(item);
    }
    out
}
#[test]
fn request_identifiers_keep_frozen_validation_and_owned_mutations() {
    let fixture = page_fixture("request-validation");
    let store = page_store(&fixture);
    let deadline = membus::now_ms() + 30_000;
    let valid = identifier_request(GET, b"read", "名字".as_bytes(), b"missing", deadline);
    assert_eq!(
        store.dispatch(&valid),
        original(GET | 0x80, 91, Some(0), &[0])
    );
    let before = store.cache_stats();
    for end in 0..valid.len() {
        let response = store.dispatch(&valid[..end]);
        assert_eq!(
            u16::from_le_bytes(response[17..19].try_into().unwrap()),
            3,
            "request truncation at {end}"
        );
    }
    for position in 0..3 {
        for (invalid, code) in [(Vec::new(), 3u16), (vec![0xff], 3), (vec![b'x'; 1025], 4)] {
            let mut fields = [b"read".to_vec(), b"name".to_vec(), b"missing".to_vec()];
            fields[position] = invalid;
            let input = identifier_request(GET, &fields[0], &fields[1], &fields[2], deadline);
            assert_eq!(
                store.dispatch(&input),
                original(GET | 0x80, 91, Some(code), &[])
            );
        }
    }
    let mut trailing = valid.clone();
    trailing.push(0);
    assert_eq!(
        store.dispatch(&trailing),
        original(GET | 0x80, 91, Some(3), &[])
    );
    assert_eq!(
        store.dispatch(&identifier_request(GET, b"read", b"name", b"missing", 0)),
        original(GET | 0x80, 91, Some(5), &[])
    );
    assert_eq!(
        store.dispatch(&identifier_request(
            GET,
            b"read",
            membus::POLICY_NS.as_bytes(),
            b"missing",
            deadline
        )),
        original(GET | 0x80, 91, Some(3), &[])
    );
    let after = store.cache_stats();
    assert_eq!(
        (before.hits, before.misses, before.cold_reads),
        (after.hits, after.misses, after.cold_reads)
    );
    // Mutation ownership survives destruction/reuse of the incoming wire bytes.
    let mut mutation = identifier_request(membus::PUT, b"owned-write", b"owned", b"item", deadline);
    mutation.push(1); // OS acknowledgment.
    mutation.extend(5u32.to_le_bytes());
    mutation.extend(b"value");
    let reply = store.dispatch(&mutation);
    assert_eq!(&reply[17..19], &[0, 0]);
    mutation.fill(0xff);
    drop(mutation);
    let stored = store.try_get("owned", "item").unwrap().unwrap();
    assert_eq!(stored.bytes, b"value");
    assert_eq!(stored.ack, 1);
}
