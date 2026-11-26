use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use membus::{
    Ack, Client, ExpectedVersion, MembusError, MembusServer, NoopPublisher, RocksDbStorage,
    ServerConfig, SignalBroker, SignalConfig, SignalPublisher, SignalRing, StorageConfig,
    StorageEngine,
};
use tempfile::TempDir;

#[tokio::test]
async fn child_process_hard_kill_preserves_all_ack_flush_writes() {
    let dir = TempDir::new().expect("tempdir");
    let bin = env!("CARGO_BIN_EXE_membus");

    let mut child = Command::new(bin)
        .args([
            "serve",
            "--data-dir",
            dir.path().to_str().expect("utf8 path"),
            "--tcp",
            "127.0.0.1:0",
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .expect("spawn membus resident");

    let stdout = child.stdout.take().expect("child stdout");
    let mut reader = BufReader::new(stdout);
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .expect("read startup listening line");
    let addr_str = line
        .trim()
        .strip_prefix("listening tcp=")
        .unwrap_or_else(|| panic!("unexpected startup line: {line:?}"));
    let addr: SocketAddr = addr_str.parse().expect("parse bound addr");

    // Verify `membus health` CLI subcommand against live child resident.
    let health_out = Command::new(bin)
        .args(["health", "--tcp", addr_str])
        .output()
        .expect("run membus health");
    assert!(
        health_out.status.success(),
        "health CLI failed: {}",
        String::from_utf8_lossy(&health_out.stderr)
    );
    let health_stdout = String::from_utf8_lossy(&health_out.stdout);
    assert!(health_stdout.contains("ok epoch="), "got: {health_stdout}");

    let client = Client::connect_tcp(addr).await.expect("connect child");

    // Write 150 acknowledged Ack::Flush keys concurrently.
    let mut committed = Vec::with_capacity(150);
    let mut futs = Vec::with_capacity(150);
    for i in 0..150u32 {
        let c = client.clone();
        futs.push(tokio::spawn(async move {
            let key = format!("durable/{i:04}");
            let val = format!("flush-value-{i:04}");
            let res = c
                .cas(
                    "crash",
                    key.as_bytes(),
                    val.as_bytes(),
                    Ack::Flush,
                    ExpectedVersion::Absent,
                )
                .await
                .expect("flush put");
            (key, val, res.seq)
        }));
    }
    for f in futs {
        committed.push(f.await.expect("join"));
    }

    // Start background concurrent writers and hard-kill the child process mid-load.
    let bg_client = client.clone();
    let bg_writer = tokio::spawn(async move {
        let mut idx = 0u64;
        while !bg_client.is_closed() && idx < 500 {
            let _ = bg_client
                .put(
                    "crash_bg",
                    format!("bg/{idx}").as_bytes(),
                    b"inflight",
                    Ack::Flush,
                )
                .await;
            idx += 1;
        }
    });
    tokio::time::sleep(Duration::from_millis(25)).await;

    child.kill().expect("hard kill child server");
    let _ = child.wait();
    let _ = bg_writer.await;

    // Reopen RocksDB directly on the same state dir and verify every acknowledged
    // Ack::Flush write survived the hard crash intact.
    let recovered = RocksDbStorage::open(
        dir.path(),
        StorageConfig::default(),
        Arc::new(NoopPublisher),
    )
    .expect("reopen after hard kill");

    for (key, val, seq) in committed {
        let rec = recovered
            .get("crash", key.as_bytes())
            .expect("get recovered")
            .unwrap_or_else(|| panic!("acknowledged Ack::Flush key '{key}' missing after crash"));
        assert_eq!(rec.value, val.as_bytes());
        assert_eq!(rec.meta.version, seq);
    }
}

#[tokio::test]
async fn live_zero_downtime_checkpoint_under_concurrent_writes() {
    let live_dir = TempDir::new().expect("live dir");
    let ckpt_parent = TempDir::new().expect("ckpt parent");
    let ckpt_dir = ckpt_parent.path().join("snapshot_1");

    let signal_cfg = SignalConfig::default();
    let ring = Arc::new(SignalRing::new(signal_cfg));
    let storage = Arc::new(
        RocksDbStorage::open(
            live_dir.path(),
            StorageConfig::default(),
            Arc::clone(&ring) as Arc<dyn SignalPublisher>,
        )
        .expect("open live storage"),
    );
    let broker = Arc::new(SignalBroker::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        ring,
        signal_cfg,
    ));
    let server_cfg = ServerConfig {
        checkpoint_root: Some(ckpt_parent.path().to_path_buf()),
        ..ServerConfig::default()
    };
    let server = MembusServer::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        broker,
        server_cfg,
    );
    let handle = server
        .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind tcp");

    let client = Client::connect(handle.endpoint()).await.expect("connect");

    // Seed 200 baseline keys.
    for i in 0..200u32 {
        client
            .put(
                "base",
                format!("k/{i:04}").as_bytes(),
                format!("baseline-{i:04}").as_bytes(),
                Ack::Flush,
            )
            .await
            .expect("seed baseline");
    }

    // Hammer the live DB concurrently from 8 writer tasks while creating the checkpoint.
    let stop = Arc::new(AtomicBool::new(false));
    let mut writers = Vec::new();
    for worker in 0..8u32 {
        let c = client.clone();
        let stop_ref = Arc::clone(&stop);
        writers.push(tokio::spawn(async move {
            let mut n = 0u32;
            while !stop_ref.load(Ordering::Relaxed) {
                let ack = if n % 4 == 0 { Ack::Flush } else { Ack::Fast };
                let _ = c
                    .put(
                        "hot",
                        format!("w{worker}/k{n}").as_bytes(),
                        format!("payload-{worker}-{n}").as_bytes(),
                        ack,
                    )
                    .await;
                n += 1;
            }
        }));
    }

    tokio::time::sleep(Duration::from_millis(30)).await;
    let ckpt_seq = client
        .checkpoint("snapshot_1")
        .await
        .expect("live checkpoint");
    assert!(ckpt_seq >= 200);

    // Mutate baseline keys AFTER checkpoint to verify checkpoint isolation.
    client
        .put("base", b"k/0000", b"mutated-after-checkpoint", Ack::Flush)
        .await
        .expect("post-checkpoint mutate");

    stop.store(true, Ordering::Relaxed);
    for w in writers {
        w.await.expect("writer join");
    }
    handle.shutdown().await;

    // Open the checkpoint directory as an independent RocksDbStorage instance.
    let restored = RocksDbStorage::open(
        &ckpt_dir,
        StorageConfig::default(),
        Arc::new(NoopPublisher),
    )
    .expect("open checkpoint DB");

    for i in 0..200u32 {
        let key = format!("k/{i:04}");
        let rec = restored
            .get("base", key.as_bytes())
            .expect("get from checkpoint")
            .unwrap_or_else(|| panic!("baseline key {key} missing in checkpoint"));
        assert_eq!(rec.value, format!("baseline-{i:04}").as_bytes());
    }
}

#[tokio::test]
async fn checkpoint_refuses_unset_root_and_path_traversal() {
    let live_dir = TempDir::new().expect("live dir");
    let ckpt_root = TempDir::new().expect("ckpt root");

    // 1. Unset checkpoint_root refuses all remote checkpoint RPCs.
    {
        let signal_cfg = SignalConfig::default();
        let ring = Arc::new(SignalRing::new(signal_cfg));
        let storage = Arc::new(
            RocksDbStorage::open(
                live_dir.path(),
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
        let handle = server
            .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
            .await
            .expect("bind");
        let client = Client::connect(handle.endpoint()).await.expect("connect");

        assert!(matches!(
            client.checkpoint("snap1").await,
            Err(MembusError::InvalidArgument(_))
        ));
        handle.shutdown().await;
    }

    // 2. Configured checkpoint_root blocks traversal and absolute paths, allows safe relative path.
    let signal_cfg = SignalConfig::default();
    let ring = Arc::new(SignalRing::new(signal_cfg));
    let storage = Arc::new(
        RocksDbStorage::open(
            live_dir.path(),
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
        ServerConfig {
            checkpoint_root: Some(ckpt_root.path().to_path_buf()),
            ..ServerConfig::default()
        },
    );
    let handle = server
        .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind");
    let client = Client::connect(handle.endpoint()).await.expect("connect");

    for bad in ["", ".", "../escape", "sub/../../escape", "/abs/path"] {
        assert!(
            matches!(
                client.checkpoint(bad).await,
                Err(MembusError::InvalidArgument(_))
            ),
            "expected InvalidArgument for bad checkpoint dest '{bad}'"
        );
    }

    let abs_dir = ckpt_root.path().join("abs_attempt");
    assert!(matches!(
        client
            .checkpoint(abs_dir.to_str().expect("utf8"))
            .await,
        Err(MembusError::InvalidArgument(_))
    ));

    client
        .checkpoint("safe_sub")
        .await
        .expect("valid relative checkpoint under root");
    assert!(ckpt_root.path().join("safe_sub").exists());
    handle.shutdown().await;
}
