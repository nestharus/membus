use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use http_body_util::{BodyExt, Empty};
use hyper::{Method, Request as HyperRequest};
use hyper_util::rt::{TokioExecutor, TokioIo};
use membus::{
    Ack, AdmissionConfig, BfsOptions, Client, Endpoint, ExpectedVersion, GraphDirection,
    GraphEdgeEntry, IndexMutations, MembusError, MembusServer, NoopPublisher, OrderedIndexEntry,
    OrderedKeyFilter, OrderedQueryOptions, RocksDbStorage, SearchConfig, SearchDocument,
    SearchEngine, SearchMode, SearchQuery, SearchVisibility, ServerConfig, SignalBroker,
    SignalConfig, SignalPublisher, SignalRing, StorageConfig, StorageEngine,
};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message as WsMessage;

struct LatencyStats {
    ops: usize,
    elapsed: Duration,
    p50_us: f64,
    p95_us: f64,
    p99_us: f64,
    max_us: f64,
}

impl LatencyStats {
    fn from_samples_ns(mut samples_ns: Vec<u64>, elapsed: Duration) -> Self {
        samples_ns.sort_unstable();
        let ops = samples_ns.len();
        let pct = |p: f64| -> f64 {
            if ops == 0 {
                return 0.0;
            }
            let idx = ((ops as f64 - 1.0) * p).round() as usize;
            samples_ns[idx.min(ops - 1)] as f64 / 1000.0
        };
        Self {
            ops,
            elapsed,
            p50_us: pct(0.50),
            p95_us: pct(0.95),
            p99_us: pct(0.99),
            max_us: pct(1.00),
        }
    }

    fn ops_per_sec(&self) -> f64 {
        let secs = self.elapsed.as_secs_f64();
        if secs <= 0.0 {
            0.0
        } else {
            self.ops as f64 / secs
        }
    }

    fn print(&self, label: &str) {
        println!(
            "{label:<58} | {:>10.0} ops/s | p50 {:>8.1} us | p95 {:>8.1} us | p99 {:>8.1} us | max {:>9.1} us",
            self.ops_per_sec(),
            self.p50_us,
            self.p95_us,
            self.p99_us,
            self.max_us,
        );
    }
}

fn make_bench_dir(root: &std::path::Path, name: &str) -> PathBuf {
    let dir = root.join(format!("membus_bench_{name}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create bench dir");
    dir
}

#[tokio::main]
async fn main() {
    let mut root = std::env::temp_dir().join("membus_v2_bench");
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--bench-dir" {
            if let Some(dir) = args.next() {
                root = PathBuf::from(dir);
            }
        }
    }
    std::fs::create_dir_all(&root).expect("create root bench dir");

    println!("=== Membus v2 Release Benchmark Suite ===");
    println!("Benchmark scratch root: {}", root.display());

    bench_in_process_fast_write_read(&root);
    bench_ipc_fast_write_read_and_signal_wake(&root).await;
    bench_cross_process_signal_wake(&root).await;
    bench_group_commit_flush(&root, 1_000, 16, 4).await;
    bench_group_commit_flush(&root, 10_000, 32, 2).await;
    bench_backpressure_saturation(&root).await;
    bench_warm_cold_reads_and_mvcc_snapshots(&root).await;
    bench_partial_ordered_queue_and_graph_bfs(&root);
    bench_tantivy_and_tier0_search(&root);
    bench_http2_and_websocket(&root).await;

    let _ = std::fs::remove_dir_all(&root);
    println!("=== Benchmarks Complete (scratch cleaned) ===");
}

fn bench_in_process_fast_write_read(root: &std::path::Path) {
    let dir = make_bench_dir(root, "inproc_fast");
    let storage =
        RocksDbStorage::open(&dir, StorageConfig::default(), Arc::new(NoopPublisher))
            .expect("open");

    let n = 20_000usize;
    let payload = vec![0xABu8; 256];
    let mut write_lat = Vec::with_capacity(n);
    let mut read_lat = Vec::with_capacity(n);
    let mut write_elapsed = Duration::ZERO;
    let mut read_elapsed = Duration::ZERO;

    for i in 0..n {
        let key = format!("k/{i:06}");
        let t0 = Instant::now();
        storage
            .put("bench", key.as_bytes(), &payload, Ack::Fast, ExpectedVersion::Any)
            .expect("put");
        let dt_w = t0.elapsed();
        write_elapsed += dt_w;
        write_lat.push(dt_w.as_nanos() as u64);

        let t1 = Instant::now();
        let rec = storage
            .get("bench", key.as_bytes())
            .expect("get")
            .expect("immediate recall");
        let dt_r = t1.elapsed();
        read_elapsed += dt_r;
        read_lat.push(dt_r.as_nanos() as u64);
        assert_eq!(rec.value.len(), payload.len());
    }
    LatencyStats::from_samples_ns(write_lat, write_elapsed)
        .print("1a. In-proc Ack::Fast put (256B, immediate RAM)");
    LatencyStats::from_samples_ns(read_lat, read_elapsed)
        .print("1b. In-proc immediate point get (256B)");

    drop(storage);
    let _ = std::fs::remove_dir_all(&dir);
}

async fn bench_ipc_fast_write_read_and_signal_wake(root: &std::path::Path) {
    let dir = make_bench_dir(root, "ipc_fast");
    let signal_cfg = SignalConfig::default();
    let ring = Arc::new(SignalRing::new(signal_cfg));
    let storage = Arc::new(
        RocksDbStorage::open(
            &dir,
            StorageConfig::default(),
            Arc::clone(&ring) as Arc<dyn SignalPublisher>,
        )
        .expect("open"),
    );
    let broker = Arc::new(SignalBroker::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        ring,
        signal_cfg,
    ));
    let server = MembusServer::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        broker,
        ServerConfig::default(),
    );

    let tcp_handle = server
        .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind tcp");
    let tcp_client = Client::connect(tcp_handle.endpoint())
        .await
        .expect("tcp client");
    run_ipc_latency_probe("2a. TCP IPC Ack::Fast put+get (256B)", &tcp_client, 5_000).await;

    let tcp_sub_client = Client::connect(tcp_handle.endpoint())
        .await
        .expect("tcp sub client");
    run_signal_wake_probe(
        "3a. TCP Signal Wake (pub -> sub)",
        &tcp_client,
        &tcp_sub_client,
        "sig_tcp",
        2_000,
    )
    .await;

    #[cfg(windows)]
    {
        let pipe_name = format!(r"\\.\pipe\membus-v2-bench-{}", std::process::id());
        let pipe_handle = server
            .bind_named_pipe(&pipe_name)
            .await
            .expect("bind pipe");
        let pipe_client = Client::connect_named_pipe(&pipe_name)
            .await
            .expect("pipe client");
        run_ipc_latency_probe(
            "2b. NamedPipe IOCP Ack::Fast put+get (256B)",
            &pipe_client,
            5_000,
        )
        .await;

        let pipe_sub_client = Client::connect_named_pipe(&pipe_name)
            .await
            .expect("pipe sub client");
        run_signal_wake_probe(
            "3b. NamedPipe IOCP Signal Wake (pub -> sub)",
            &pipe_client,
            &pipe_sub_client,
            "sig_pipe",
            2_000,
        )
        .await;
        pipe_handle.shutdown().await;
    }

    tcp_handle.shutdown().await;
    drop(server);
    drop(storage);
    let _ = std::fs::remove_dir_all(&dir);
}

async fn run_ipc_latency_probe(label: &str, client: &Client, n: usize) {
    let payload = vec![0xCDu8; 256];
    let mut samples = Vec::with_capacity(n);
    let start = Instant::now();
    for i in 0..n {
        let key = format!("ipc/{i:06}");
        let t0 = Instant::now();
        client
            .put("ipc", key.as_bytes(), &payload, Ack::Fast)
            .await
            .expect("ipc put");
        let rec = client
            .get("ipc", key.as_bytes())
            .await
            .expect("ipc get")
            .expect("present");
        assert_eq!(rec.value.len(), payload.len());
        samples.push(t0.elapsed().as_nanos() as u64);
    }
    LatencyStats::from_samples_ns(samples, start.elapsed()).print(label);
}

async fn run_signal_wake_probe(
    label: &str,
    pub_client: &Client,
    sub_client: &Client,
    ns: &str,
    n: usize,
) {
    let mut sub = sub_client
        .subscribe_each_change(ns, b"", 0, 0)
        .await
        .expect("subscribe");
    let mut samples = Vec::with_capacity(n);
    let start = Instant::now();
    for i in 0..n {
        let key = format!("ev/{i:06}");
        let t0 = Instant::now();
        pub_client
            .put(ns, key.as_bytes(), b"wake", Ack::Fast)
            .await
            .expect("pub");
        let ev = sub.next_event().await.expect("sub ok").expect("event");
        assert_eq!(ev.key, key.as_bytes());
        samples.push(t0.elapsed().as_nanos() as u64);
    }
    LatencyStats::from_samples_ns(samples, start.elapsed()).print(label);
    let _ = sub.cancel().await;
}

async fn bench_cross_process_signal_wake(root: &std::path::Path) {
    let exe = std::env::current_exe().expect("current_exe");
    let bin_name = if cfg!(windows) { "membus.exe" } else { "membus" };
    let Some(server_bin) = exe
        .parent()
        .and_then(|p| p.parent())
        .map(|p| p.join(bin_name))
        .filter(|p| p.exists())
    else {
        println!(
            "3c. Cross-process Signal Wake                              | skipped (build `membus` binary first)"
        );
        return;
    };

    let dir = make_bench_dir(root, "xproc_sig");
    #[cfg(windows)]
    let pipe_name = format!(r"\\.\pipe\membus-v2-xproc-bench-{}", std::process::id());
    let mut cmd = Command::new(&server_bin);
    cmd.args([
        "serve",
        "--data-dir",
        dir.to_str().expect("utf8 path"),
        "--tcp",
        "127.0.0.1:0",
    ]);
    #[cfg(windows)]
    cmd.args(["--pipe", &pipe_name]);

    let mut child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn membus resident");

    let stdout = child.stdout.take().expect("stdout");
    let mut reader = BufReader::new(stdout);
    let mut tcp_line = String::new();
    reader.read_line(&mut tcp_line).expect("read tcp line");
    let tcp_addr: SocketAddr = tcp_line
        .trim()
        .strip_prefix("listening tcp=")
        .expect("tcp listening line")
        .parse()
        .expect("parse tcp addr");

    #[cfg(windows)]
    {
        let mut pipe_line = String::new();
        reader.read_line(&mut pipe_line).expect("read pipe line");
    }

    let pub_tcp = Client::connect_tcp(tcp_addr).await.expect("pub tcp");
    let sub_tcp = Client::connect_tcp(tcp_addr).await.expect("sub tcp");
    run_signal_wake_probe(
        "3c. Cross-process TCP Signal Wake (pub -> child -> sub)",
        &pub_tcp,
        &sub_tcp,
        "xproc_tcp",
        2_000,
    )
    .await;

    #[cfg(windows)]
    {
        let pub_pipe = Client::connect_named_pipe(&pipe_name)
            .await
            .expect("pub pipe");
        let sub_pipe = Client::connect_named_pipe(&pipe_name)
            .await
            .expect("sub pipe");
        run_signal_wake_probe(
            "3d. Cross-process NamedPipe Signal Wake (pub->child->sub)",
            &pub_pipe,
            &sub_pipe,
            "xproc_pipe",
            2_000,
        )
        .await;
    }

    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&dir);
}

async fn bench_group_commit_flush(
    root: &std::path::Path,
    logical_writers: usize,
    connections: usize,
    rounds_per_writer: usize,
) {
    let dir = make_bench_dir(root, &format!("flush_{logical_writers}"));
    let storage_cfg = StorageConfig {
        admission: AdmissionConfig {
            max_in_flight_ops: logical_writers * 2,
            max_in_flight_bytes: 256 * 1024 * 1024,
            retry_after_ms: 5,
        },
        ..StorageConfig::default()
    };
    let server_cfg = ServerConfig {
        max_in_flight_per_conn: (logical_writers / connections + 128).max(2048),
        outbound_queue_capacity: 8192,
        ..ServerConfig::default()
    };
    let signal_cfg = SignalConfig::default();
    let ring = Arc::new(SignalRing::new(signal_cfg));
    let storage = Arc::new(
        RocksDbStorage::open(
            &dir,
            storage_cfg,
            Arc::clone(&ring) as Arc<dyn SignalPublisher>,
        )
        .expect("open"),
    );
    let broker = Arc::new(SignalBroker::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        ring,
        signal_cfg,
    ));
    let server = MembusServer::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        broker,
        server_cfg,
    );
    let handle = server
        .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind");

    let mut clients = Vec::with_capacity(connections);
    for _ in 0..connections {
        clients.push(Client::connect(handle.endpoint()).await.expect("connect"));
    }

    let payload = Arc::new(vec![0x5Au8; 128]);
    let start = Instant::now();
    let mut tasks = Vec::with_capacity(logical_writers);
    for writer_id in 0..logical_writers {
        let client = clients[writer_id % connections].clone();
        let data = Arc::clone(&payload);
        tasks.push(tokio::spawn(async move {
            let mut lats = Vec::with_capacity(rounds_per_writer);
            for r in 0..rounds_per_writer {
                let key = format!("w/{writer_id:05}/{r}");
                let t0 = Instant::now();
                client
                    .put("gc", key.as_bytes(), &data, Ack::Flush)
                    .await
                    .expect("flush put");
                lats.push(t0.elapsed().as_nanos() as u64);
            }
            lats
        }));
    }

    let mut all_samples = Vec::with_capacity(logical_writers * rounds_per_writer);
    for task in tasks {
        all_samples.extend(task.await.expect("writer join"));
    }
    let elapsed = start.elapsed();

    LatencyStats::from_samples_ns(all_samples, elapsed).print(&format!(
        "4. Ack::Flush group-commit ({logical_writers} writers / {connections} conns)"
    ));

    drop(clients);
    handle.shutdown().await;
    drop(server);
    drop(storage);
    let _ = std::fs::remove_dir_all(&dir);
}

async fn bench_backpressure_saturation(root: &std::path::Path) {
    let dir = make_bench_dir(root, "backpressure");
    let storage_cfg = StorageConfig {
        admission: AdmissionConfig {
            max_in_flight_ops: 16,
            max_in_flight_bytes: 512 * 1024,
            retry_after_ms: 2,
        },
        ..StorageConfig::default()
    };
    let server_cfg = ServerConfig {
        max_in_flight_per_conn: 16,
        ..ServerConfig::default()
    };
    let signal_cfg = SignalConfig::default();
    let ring = Arc::new(SignalRing::new(signal_cfg));
    let storage = Arc::new(
        RocksDbStorage::open(
            &dir,
            storage_cfg,
            Arc::clone(&ring) as Arc<dyn SignalPublisher>,
        )
        .expect("open"),
    );
    let broker = Arc::new(SignalBroker::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        ring,
        signal_cfg,
    ));
    let server = MembusServer::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        broker,
        server_cfg,
    );
    let handle = server
        .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind");

    let mut clients = Vec::new();
    for _ in 0..8 {
        clients.push(Client::connect(handle.endpoint()).await.expect("connect"));
    }

    let ok_ops = Arc::new(AtomicUsize::new(0));
    let busy_ops = Arc::new(AtomicUsize::new(0));
    let err_ops = Arc::new(AtomicUsize::new(0));

    let mut tasks = Vec::new();
    for t in 0..512usize {
        let c = clients[t % clients.len()].clone();
        let ok_ref = Arc::clone(&ok_ops);
        let busy_ref = Arc::clone(&busy_ops);
        let err_ref = Arc::clone(&err_ops);
        tasks.push(tokio::spawn(async move {
            let payload = vec![0x33u8; 4096];
            for r in 0..4usize {
                match c
                    .put("bp", format!("k/{t}/{r}").as_bytes(), &payload, Ack::Flush)
                    .await
                {
                    Ok(_) => {
                        ok_ref.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(MembusError::Busy { .. }) => {
                        busy_ref.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        err_ref.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }
    for task in tasks {
        task.await.expect("bp join");
    }

    let dropped_conns = clients.iter().filter(|c| c.is_closed()).count();
    for c in &clients {
        c.ping().await.expect("post-saturation ping must succeed");
    }
    println!(
        "5. Backpressure saturation (512 writers)                   | ok={} busy={} unexpected_err={} dropped_conns={}",
        ok_ops.load(Ordering::Relaxed),
        busy_ops.load(Ordering::Relaxed),
        err_ops.load(Ordering::Relaxed),
        dropped_conns
    );
    assert_eq!(err_ops.load(Ordering::Relaxed), 0);
    assert_eq!(dropped_conns, 0);
    assert!(busy_ops.load(Ordering::Relaxed) > 0);

    drop(clients);
    handle.shutdown().await;
    drop(server);
    drop(storage);
    let _ = std::fs::remove_dir_all(&dir);
}

async fn bench_warm_cold_reads_and_mvcc_snapshots(root: &std::path::Path) {
    let dir = make_bench_dir(root, "reads_mvcc");
    let num_keys = 30_000usize;
    let payload = vec![0x77u8; 1024];

    {
        let storage = RocksDbStorage::open(
            &dir,
            StorageConfig::default(),
            Arc::new(NoopPublisher),
        )
        .expect("open seed");
        for i in 0..num_keys {
            storage
                .put(
                    "readset",
                    format!("r/{i:06}").as_bytes(),
                    &payload,
                    Ack::Fast,
                    ExpectedVersion::Any,
                )
                .expect("seed");
        }
        storage.flush_wal(true).expect("flush");
    }

    let cold_cfg = StorageConfig {
        block_cache_bytes: 1024 * 1024,
        ..StorageConfig::default()
    };
    let storage = Arc::new(
        RocksDbStorage::open(&dir, cold_cfg, Arc::new(NoopPublisher)).expect("reopen cold"),
    );

    let mut cold_lat = Vec::with_capacity(5_000);
    let start_cold = Instant::now();
    for i in 0..5_000usize {
        let idx = (i * 7919) % num_keys;
        let key = format!("r/{idx:06}");
        let t0 = Instant::now();
        let rec = storage
            .get("readset", key.as_bytes())
            .expect("cold get")
            .expect("present");
        cold_lat.push(t0.elapsed().as_nanos() as u64);
        assert_eq!(rec.value.len(), payload.len());
    }
    LatencyStats::from_samples_ns(cold_lat, start_cold.elapsed())
        .print("6a. Cold point get (1 KiB, 30k keys, 1 MiB cache)");

    let mut warm_lat = Vec::with_capacity(20_000);
    let start_warm = Instant::now();
    for i in 0..20_000usize {
        let idx = i % 512;
        let key = format!("r/{idx:06}");
        let t0 = Instant::now();
        let rec = storage
            .get("readset", key.as_bytes())
            .expect("warm get")
            .expect("present");
        warm_lat.push(t0.elapsed().as_nanos() as u64);
        assert_eq!(rec.value.len(), payload.len());
    }
    LatencyStats::from_samples_ns(warm_lat, start_warm.elapsed())
        .print("6b. Warm point get 1t (1 KiB, hot 512-key subset)");

    let start_8t = Instant::now();
    let mut reader_handles = Vec::new();
    for tid in 0..8usize {
        let s = Arc::clone(&storage);
        reader_handles.push(std::thread::spawn(move || {
            let mut lats = Vec::with_capacity(10_000);
            for i in 0..10_000usize {
                let idx = (tid * 97 + i) % 512;
                let key = format!("r/{idx:06}");
                let t0 = Instant::now();
                let rec = s.get("readset", key.as_bytes()).expect("8t get").expect("some");
                lats.push(t0.elapsed().as_nanos() as u64);
                assert_eq!(rec.value.len(), 1024);
            }
            lats
        }));
    }
    let mut lat_8t = Vec::with_capacity(80_000);
    for h in reader_handles {
        lat_8t.extend(h.join().expect("reader join"));
    }
    LatencyStats::from_samples_ns(lat_8t, start_8t.elapsed())
        .print("6c. Warm point get 8t (1 KiB, hot 512-key subset)");

    let stop = Arc::new(AtomicBool::new(false));
    let mut writer_handles = Vec::new();
    for w in 0..4usize {
        let s = Arc::clone(&storage);
        let stop_ref = Arc::clone(&stop);
        writer_handles.push(std::thread::spawn(move || {
            let mut n = 0usize;
            while !stop_ref.load(Ordering::Relaxed) {
                let _ = s.put(
                    "active",
                    format!("w{w}/k{}", n % 1024).as_bytes(),
                    b"active-write",
                    Ack::Fast,
                    ExpectedVersion::Any,
                );
                n += 1;
            }
        }));
    }

    let mut snap_lat = Vec::with_capacity(10_000);
    let start_snap = Instant::now();
    for _ in 0..10_000 {
        let t0 = Instant::now();
        let snap = storage.open_snapshot().expect("open_snapshot");
        snap_lat.push(t0.elapsed().as_nanos() as u64);
        drop(snap);
    }
    LatencyStats::from_samples_ns(snap_lat, start_snap.elapsed())
        .print("6d. O(1) MVCC open_snapshot under 4 active writers");

    stop.store(true, Ordering::Relaxed);
    for h in writer_handles {
        h.join().expect("join writer");
    }

    drop(storage);
    let _ = std::fs::remove_dir_all(&dir);
}

fn bench_partial_ordered_queue_and_graph_bfs(root: &std::path::Path) {
    let dir = make_bench_dir(root, "idx_graph");
    let storage =
        RocksDbStorage::open(&dir, StorageConfig::default(), Arc::new(NoopPublisher))
            .expect("open");

    // Seed 10,000 tasks where 1,000 are active in the partial "ready_queue" index.
    for i in 0..10_000u32 {
        let idx = if i % 10 == 0 {
            let prio = (i % 4) as u8;
            let mut sort_key = Vec::with_capacity(5);
            sort_key.push(prio);
            sort_key.extend_from_slice(&i.to_be_bytes());
            Some(IndexMutations {
                ordered: vec![OrderedIndexEntry {
                    index_id: "ready_queue".to_owned(),
                    sort_key,
                    summary: format!("task:{i}").into_bytes(),
                }],
                edges: Vec::new(),
            })
        } else {
            None
        };
        storage
            .put_indexed(
                "work",
                format!("task/{i:05}").as_bytes(),
                b"task-payload",
                Ack::Fast,
                ExpectedVersion::Any,
                idx.as_ref(),
                None,
            )
            .expect("seed task");
    }

    let snap = storage.open_snapshot().expect("snap");
    let q_opts = OrderedQueryOptions {
        filter: OrderedKeyFilter::Prefix(vec![0u8]),
        after_cursor: None,
        limit: 32,
        max_bytes: 256 * 1024,
    };
    let mut q_lats = Vec::with_capacity(5_000);
    let t_start = Instant::now();
    for _ in 0..5_000 {
        let t0 = Instant::now();
        let page = storage
            .query_ordered_index(&snap, "work", "ready_queue", &q_opts)
            .expect("ordered scan");
        q_lats.push(t0.elapsed().as_nanos() as u64);
        assert_eq!(page.entries.len(), 32);
    }
    LatencyStats::from_samples_ns(q_lats, t_start.elapsed())
        .print("7. Partial ordered queue scan (top-32 / 10k tasks)");
    drop(snap);

    // Seed 2,000-node graph with out-degree 4.
    let nodes = 2_000usize;
    for u in 0..nodes {
        let mut edges = Vec::with_capacity(4);
        for d in 1..=4usize {
            let v = (u * 3 + d * 17) % nodes;
            edges.push(GraphEdgeEntry {
                index_id: "deps".to_owned(),
                src_id: format!("n{u:04}").into_bytes(),
                rel_label: b"links".to_vec(),
                dst_id: format!("n{v:04}").into_bytes(),
                meta: b"w1".to_vec(),
            });
        }
        storage
            .put_indexed(
                "graph",
                format!("n{u:04}").as_bytes(),
                b"node",
                Ack::Fast,
                ExpectedVersion::Any,
                Some(&IndexMutations {
                    ordered: Vec::new(),
                    edges,
                }),
                None,
            )
            .expect("seed node");
    }

    let g_snap = storage.open_snapshot().expect("g_snap");
    let bfs_opts = BfsOptions {
        direction: GraphDirection::Out,
        rel_filter: Some(b"links".to_vec()),
        max_hops: 2,
        max_nodes: 256,
    };
    let mut bfs_lats = Vec::with_capacity(2_000);
    let b_start = Instant::now();
    for i in 0..2_000usize {
        let seed = format!("n{:04}", i % nodes);
        let t0 = Instant::now();
        let res = storage
            .bfs_traverse(&g_snap, "graph", "deps", seed.as_bytes(), &bfs_opts)
            .expect("bfs");
        bfs_lats.push(t0.elapsed().as_nanos() as u64);
        assert!(res.visited_nodes.len() >= 5);
    }
    LatencyStats::from_samples_ns(bfs_lats, b_start.elapsed())
        .print("8. 2-hop graph BFS traversal (deg-4, 2k-node graph)");
    drop(g_snap);

    drop(storage);
    let _ = std::fs::remove_dir_all(&dir);
}

fn bench_tantivy_and_tier0_search(root: &std::path::Path) {
    let dir = make_bench_dir(root, "tantivy");
    let storage = Arc::new(
        RocksDbStorage::open(&dir, StorageConfig::default(), Arc::new(NoopPublisher))
            .expect("open"),
    );
    let engine = SearchEngine::open(
        &dir,
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        SearchConfig {
            commit_interval_ms: 60_000,
            max_buffered_docs: 10_000,
            max_overlay_docs: 16_384,
            ..SearchConfig::default()
        },
    )
    .expect("open search");

    let num_docs = 5_000usize;
    for i in 0..num_docs {
        let topic = match i % 5 {
            0 => "consensus storage durability replication",
            1 => "routing pipeline backpressure scheduler",
            2 => "graph adjacency traversal index",
            3 => "security certificate mutual authentication",
            _ => "telemetry latency histogram benchmark",
        };
        engine
            .index_document(
                SearchDocument {
                    namespace: "corpus".to_owned(),
                    doc_id: format!("doc/{i:05}"),
                    kind: "article".to_owned(),
                    title: format!("Membus Technical Note {i} ({topic})"),
                    tags: vec![format!("grp{}", i % 10), "v2".to_owned()],
                    body: format!(
                        "Document {i} discusses {topic} with TokenMarker_{:04} and exact literal RefCode_X{i:04}.",
                        i % 100
                    ),
                    visibility: SearchVisibility::Public,
                },
                Ack::Fast,
            )
            .expect("seed doc");
    }
    engine.flush_sync().expect("flush corpus");

    // 9a. Tantivy BM25 search over disk-persisted segments.
    let bm25_q = SearchQuery {
        namespace: Some("corpus".to_owned()),
        kind: Some("article".to_owned()),
        query: "consensus durability".to_owned(),
        mode: SearchMode::Bm25,
        limit: 20,
    };
    let mut bm25_lats = Vec::with_capacity(1_000);
    let t_bm25 = Instant::now();
    for _ in 0..1_000 {
        let t0 = Instant::now();
        let page = engine.search(&bm25_q).expect("bm25");
        bm25_lats.push(t0.elapsed().as_nanos() as u64);
        assert_eq!(page.hits.len(), 20);
    }
    LatencyStats::from_samples_ns(bm25_lats, t_bm25.elapsed())
        .print("9a. Tantivy BM25 search (5k docs, disk-persisted)");

    // 9b. Tantivy case-sensitive literal trigram substring search.
    let mut lit_lats = Vec::with_capacity(1_000);
    let t_lit = Instant::now();
    for i in 0..1_000usize {
        let q = SearchQuery {
            namespace: Some("corpus".to_owned()),
            kind: None,
            query: format!("RefCode_X{:04}", (i * 3) % num_docs),
            mode: SearchMode::LiteralSubstring,
            limit: 10,
        };
        let t0 = Instant::now();
        let page = engine.search(&q).expect("literal");
        lit_lats.push(t0.elapsed().as_nanos() as u64);
        assert_eq!(page.hits.len(), 1);
    }
    LatencyStats::from_samples_ns(lit_lats, t_lit.elapsed())
        .print("9b. Tantivy LiteralSubstring trigram search (5k docs)");

    // 9c. 0 ms Tier-0 RAM overlay search-your-writes BEFORE Tantivy commit.
    let mut t0_lats = Vec::with_capacity(200);
    let t_overlay = Instant::now();
    for i in 0..200usize {
        let marker = format!("LiveInstantToken_{i:04}");
        let start = Instant::now();
        engine
            .index_document(
                SearchDocument {
                    namespace: "corpus".to_owned(),
                    doc_id: format!("live/{i:04}"),
                    kind: "live".to_owned(),
                    title: format!("Live {marker}"),
                    tags: vec!["tier0".to_owned()],
                    body: format!("immediate overlay payload {marker}"),
                    visibility: SearchVisibility::Public,
                },
                Ack::Fast,
            )
            .expect("live index");
        let page = engine
            .search(&SearchQuery {
                namespace: Some("corpus".to_owned()),
                kind: Some("live".to_owned()),
                query: marker,
                mode: SearchMode::LiteralSubstring,
                limit: 5,
            })
            .expect("live search");
        t0_lats.push(start.elapsed().as_nanos() as u64);
        assert_eq!(page.hits.len(), 1);
    }
    LatencyStats::from_samples_ns(t0_lats, t_overlay.elapsed())
        .print("9c. Tier-0 RAM overlay 0ms index+search (pre-commit)");

    drop(engine);
    drop(storage);
    let _ = std::fs::remove_dir_all(&dir);
}

async fn bench_http2_and_websocket(root: &std::path::Path) {
    let dir = make_bench_dir(root, "http_ws");
    let signal_cfg = SignalConfig::default();
    let ring = Arc::new(SignalRing::new(signal_cfg));
    let storage = Arc::new(
        RocksDbStorage::open(
            &dir,
            StorageConfig::default(),
            Arc::clone(&ring) as Arc<dyn SignalPublisher>,
        )
        .expect("open"),
    );
    let broker = Arc::new(SignalBroker::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        ring,
        signal_cfg,
    ));
    let server = MembusServer::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        broker,
        ServerConfig::default(),
    );
    let http_handle = server
        .bind_http("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind http");
    let Endpoint::Http(http_addr) = *http_handle.endpoint() else {
        panic!("expected http endpoint");
    };

    for i in 0..256usize {
        storage
            .put(
                "httpbench",
                format!("k{i}").as_bytes(),
                b"hello-http2-payload",
                Ack::Fast,
                ExpectedVersion::Any,
            )
            .expect("seed");
    }

    // 10a. HTTP/2 prior-knowledge multiplexed GETs (64 concurrent streams over 1 TCP conn).
    let tcp = TcpStream::connect(http_addr).await.expect("connect h2");
    let _ = tcp.set_nodelay(true);
    let (h2_sender, h2_conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(tcp))
            .await
            .expect("h2 handshake");
    tokio::spawn(async move {
        let _ = h2_conn.await;
    });

    let total_h2 = 2_560usize;
    let concurrency = 64usize;
    let per_task = total_h2 / concurrency;
    let start_h2 = Instant::now();
    let mut h2_tasks = Vec::with_capacity(concurrency);
    for tid in 0..concurrency {
        let mut sender = h2_sender.clone();
        h2_tasks.push(tokio::spawn(async move {
            let mut lats = Vec::with_capacity(per_task);
            for r in 0..per_task {
                let k = (tid * per_task + r) % 256;
                let req = HyperRequest::builder()
                    .method(Method::GET)
                    .uri(format!("http://{http_addr}/v2/kv/httpbench/k{k}"))
                    .body(Empty::<Bytes>::new())
                    .expect("req");
                let t0 = Instant::now();
                let resp = sender.send_request(req).await.expect("h2 send");
                assert_eq!(resp.status(), hyper::StatusCode::OK);
                let _ = resp.into_body().collect().await.expect("body");
                lats.push(t0.elapsed().as_nanos() as u64);
            }
            lats
        }));
    }
    let mut h2_samples = Vec::with_capacity(total_h2);
    for t in h2_tasks {
        h2_samples.extend(t.await.expect("h2 join"));
    }
    LatencyStats::from_samples_ns(h2_samples, start_h2.elapsed())
        .print("10a. Axum HTTP/2 (h2c) multiplexed GET (64 streams/1 conn)");

    // 10b. WebSocket sequential put+get round-trips over `/v2/ws`.
    let ws_url = format!("ws://{http_addr}/v2/ws");
    let (ws_stream, _) = tokio_tungstenite::connect_async(&ws_url)
        .await
        .expect("ws connect");
    let (mut ws_tx, mut ws_rx) = ws_stream.split();
    let ws_rounds = 1_000usize;
    let mut ws_samples = Vec::with_capacity(ws_rounds);
    let start_ws = Instant::now();
    for i in 0..ws_rounds {
        let t0 = Instant::now();
        let put_msg = format!(
            r#"{{"id":{},"op":"put","namespace":"wsbench","key_utf8":"k{i}","value_utf8":"v{i}"}}"#,
            i * 2 + 1
        );
        ws_tx
            .send(WsMessage::Text(put_msg.into()))
            .await
            .expect("ws send put");
        let _ = ws_rx.next().await.expect("ws put resp").expect("ok");

        let get_msg = format!(
            r#"{{"id":{},"op":"get","namespace":"wsbench","key_utf8":"k{i}"}}"#,
            i * 2 + 2
        );
        ws_tx
            .send(WsMessage::Text(get_msg.into()))
            .await
            .expect("ws send get");
        let _ = ws_rx.next().await.expect("ws get resp").expect("ok");
        ws_samples.push(t0.elapsed().as_nanos() as u64);
    }
    LatencyStats::from_samples_ns(ws_samples, start_ws.elapsed())
        .print("10b. Axum WebSocket put+get round-trip (/v2/ws)");
    let _ = ws_tx.close().await;

    http_handle.shutdown().await;
    drop(server);
    drop(storage);
    let _ = std::fs::remove_dir_all(&dir);
}
