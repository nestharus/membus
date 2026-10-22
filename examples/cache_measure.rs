//! Compare resident payload retention and hot/restart read latency on an authored working set.
use membus::*;
use std::{path::PathBuf, time::Instant};
#[cfg(windows)]
fn memory() -> usize {
    #[repr(C)]
    struct Counters {
        size: u32,
        faults: u32,
        peak: usize,
        working: usize,
        pool_peak: usize,
        pool: usize,
        non_peak: usize,
        non: usize,
        page: usize,
        page_peak: usize,
        private: usize,
    }
    #[link(name = "kernel32")]
    extern "system" {
        fn GetCurrentProcess() -> *mut std::ffi::c_void;
    }
    #[link(name = "psapi")]
    extern "system" {
        fn GetProcessMemoryInfo(p: *mut std::ffi::c_void, c: *mut Counters, n: u32) -> i32;
    }
    let mut c: Counters = unsafe { std::mem::zeroed() };
    c.size = std::mem::size_of::<Counters>() as u32;
    assert_ne!(
        unsafe {
            GetProcessMemoryInfo(
                GetCurrentProcess(),
                &mut c,
                std::mem::size_of::<Counters>() as u32,
            )
        },
        0
    );
    c.private
}
#[cfg(not(windows))]
fn memory() -> usize {
    0
}
fn reads(s: &Store, count: usize, repeats: usize) -> (u128, u128) {
    let mut times = Vec::new();
    for _ in 0..repeats {
        for i in 0..count {
            let t = Instant::now();
            let v = s.get("measure", &format!("k{i}")).unwrap();
            times.push(t.elapsed().as_nanos());
            assert_eq!(v.bytes.len(), 32768);
            assert!(v.bytes.iter().all(|b| *b == (i % 251) as u8));
        }
    }
    times.sort_unstable();
    (times[times.len() / 2], times[times.len() * 99 / 100])
}
fn main() {
    let p = PathBuf::from(std::env::args().nth(1).expect("owned fixture directory"));
    let clock = ManualClock::new(now_ms());
    let c = Config {
        capacity: 512 * 1024 * 1024,
        clock: Some(clock.clone()),
        signal_bytes: 0,
        tick_ms: 3_600_000,
        retire_ms: 3_600_000,
        ..Config::default()
    };
    let base = memory();
    let s = Store::open(&p, c.clone()).unwrap();
    for i in 0..4096 {
        assert_eq!(
            s.mutate(Mutation {
                ns: "measure".into(),
                key: format!("k{i}"),
                op_id: format!("w{i}"),
                deadline: now_ms() + 60_000,
                ack: 1,
                expected: None,
                bytes: vec![(i % 251) as u8; 32768],
                remove: false
            })
            .code,
            0
        );
    }
    clock.advance(3_600_000);
    s.tick();
    let written = memory();
    let _ = reads(&s, 32, 1);
    let hot = reads(&s, 32, 20);
    let retained = memory();
    drop(s);
    #[cfg(not(windows))]
    std::fs::remove_file(p.join("owner.lock")).unwrap();
    let t = Instant::now();
    let s = Store::open(&p, c).unwrap();
    let replay_ms = t.elapsed().as_millis();
    let replay_mem = memory();
    let first = reads(&s, 4096, 1);
    let _ = reads(&s, 32, 1);
    let hot_restart = reads(&s, 32, 20);
    println!("working_set_bytes=134217728 base_private_bytes={base} after_expiry_private_bytes={written} after_hot_private_bytes={retained} replay_private_bytes={replay_mem} replay_ms={replay_ms} hot_p50_ns={} hot_p99_ns={} first_read_p50_ns={} first_read_p99_ns={} restart_hot_p50_ns={} restart_hot_p99_ns={}",hot.0,hot.1,first.0,first.1,hot_restart.0,hot_restart.1);
    for (k, v) in s.health2().fields() {
        if k.starts_with("cache_") {
            println!("{k}={v}");
        }
    }
}
