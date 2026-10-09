use membus::*;
use std::{path::Path, time::Instant};
#[repr(C)]
#[derive(Default)]
struct Memory {
    cb: u32,
    faults: u32,
    peak: usize,
    working: usize,
    peak_page: usize,
    page: usize,
    peak_nonpage: usize,
    nonpage: usize,
    pagefile: usize,
    peak_pagefile: usize,
}
#[repr(C)]
#[derive(Default)]
struct Io {
    reads: u64,
    writes: u64,
    other: u64,
    read_bytes: u64,
    write_bytes: u64,
    other_bytes: u64,
}
#[link(name = "kernel32")]
extern "system" {
    fn GetCurrentProcess() -> isize;
    fn GetProcessIoCounters(p: isize, out: *mut Io) -> i32;
}
#[link(name = "psapi")]
extern "system" {
    fn GetProcessMemoryInfo(p: isize, out: *mut Memory, cb: u32) -> i32;
}
fn memory() -> Memory {
    let mut m = Memory::default();
    m.cb = std::mem::size_of::<Memory>() as u32;
    unsafe {
        assert_ne!(GetProcessMemoryInfo(GetCurrentProcess(), &mut m, m.cb), 0);
    }
    m
}
fn counters() -> Io {
    let mut m = Io::default();
    unsafe {
        assert_ne!(GetProcessIoCounters(GetCurrentProcess(), &mut m), 0);
    }
    m
}
fn main() {
    let args: Vec<_> = std::env::args().collect();
    let count: usize = args[2].parse().unwrap();
    let config = Config {
        capacity: 2 * 1024 * 1024 * 1024,
        dedup_cap: 400_000,
        ..Config::default()
    };
    let s = Store::open(Path::new(&args[1]), config).unwrap();
    let base = memory();
    let start = Instant::now();
    for i in 1..=count {
        let size = if i % 3070 == 1533 {
            59499
        } else if i % 2 == 1 {
            1039
        } else {
            1311
        };
        let r = s.mutate(Mutation {
            ns: "ledger".into(),
            key: format!("row/{i:016}"),
            op_id: format!("memory-op-{i}"),
            deadline: now_ms() + 60_000,
            ack: 1,
            expected: Some(0),
            bytes: vec![b'x'; size],
            remove: false,
        });
        assert_eq!(r.code, 0, "key{i}");
        if i % 50_000 == 0 {
            eprintln!("keys={i} elapsed={:.3}", start.elapsed().as_secs_f64());
        }
    }
    let loaded = memory();
    let before = counters();
    let read_start = Instant::now();
    for i in 1..=10_000 {
        assert!(s.get("ledger", &format!("row/{i:016}")).is_some());
    }
    let reads = counters();
    let io = s.io_metrics();
    let mut body = now_ms().saturating_add(10_000).to_le_bytes().to_vec();
    lp(&mut body, b"health");
    lp(&mut body, b"ledger");
    let health = s.dispatch(&frame(HEALTH, 1, &body)[4..]);
    assert_eq!(u16::from_le_bytes([health[17], health[18]]), 0);
    assert_eq!(health[68], 1);
    assert_eq!(health[67], 0);
    println!("{{\"keys\":{count},\"seconds\":{},\"rss_before\":{},\"rss_after\":{},\"rss_delta_per_key\":{},\"private_bytes_after\":{},\"logical_bytes\":{},\"write_calls\":{},\"flush_calls\":{},\"read_operation_delta\":{},\"read_bytes_delta\":{},\"ten_thousand_read_seconds\":{},\"namespace_quota\":500000,\"global_cap\":917000,\"namespace_high_water\":true,\"global_high_water\":false,\"explicit_capacity_bytes\":2147483648,\"dedup_cap\":400000}}",start.elapsed().as_secs_f64(),base.working,loaded.working,(loaded.working as f64-base.working as f64)/count as f64,loaded.pagefile,io.0,io.1,io.2,reads.reads-before.reads,reads.read_bytes-before.read_bytes,read_start.elapsed().as_secs_f64());
}
