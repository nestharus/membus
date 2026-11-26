use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Bytes, BytesMut};
use membus::cli::{parse_args, Command};
use membus::protocol::{
    decode_request, decode_response, encode_frame_into, encode_request, read_frame,
    try_decode_frame, Frame, FrameKind, Request, Response, DEFAULT_MAX_FRAME_BODY,
    FRAME_HEADER_LEN, WIRE_VERSION,
};
use membus::{
    Ack, Client, Endpoint, ExpectedVersion, MembusError, MembusServer, MutationOp, RocksDbStorage,
    ScanOptions, ServerConfig, SignalBroker, SignalConfig, SignalPublisher, SignalRing,
    StorageConfig, StorageEngine, VersionGuard, WriteMutation,
};
use tempfile::TempDir;
use tokio::time::timeout;

fn build_server(
    dir: &TempDir,
    storage_cfg: StorageConfig,
    server_cfg: ServerConfig,
) -> (Arc<RocksDbStorage>, MembusServer) {
    let signal_cfg = SignalConfig::default();
    let ring = Arc::new(SignalRing::new(signal_cfg));
    let storage = Arc::new(
        RocksDbStorage::open(
            dir.path(),
            storage_cfg,
            Arc::clone(&ring) as Arc<dyn SignalPublisher>,
        )
        .expect("open storage"),
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
    (storage, server)
}

#[test]
fn wire_frame_header_layout_and_validation() {
    let frame = Frame::new(FrameKind::Request, 0x0102_0304_0506_0708, Bytes::from_static(b"hello"));
    let mut buf = BytesMut::new();
    encode_frame_into(&frame, &mut buf, DEFAULT_MAX_FRAME_BODY).expect("encode");

    assert_eq!(buf.len(), FRAME_HEADER_LEN + 5);
    assert_eq!(&buf[0..4], &5u32.to_be_bytes());
    assert_eq!(&buf[4..6], &WIRE_VERSION.to_be_bytes());
    assert_eq!(buf[6], FrameKind::Request as u8);
    assert_eq!(buf[7], 0);
    assert_eq!(&buf[8..16], &0x0102_0304_0506_0708u64.to_be_bytes());

    let decoded = try_decode_frame(&mut buf, DEFAULT_MAX_FRAME_BODY)
        .expect("decode")
        .expect("complete");
    assert_eq!(decoded, frame);

    // Wrong version rejected cleanly.
    let mut bad = BytesMut::new();
    encode_frame_into(&frame, &mut bad, DEFAULT_MAX_FRAME_BODY).expect("encode");
    bad[4] = 0;
    bad[5] = 99;
    assert!(matches!(
        try_decode_frame(&mut bad, DEFAULT_MAX_FRAME_BODY),
        Err(MembusError::Protocol(_))
    ));
}

#[test]
fn cli_argument_parser_handles_serve_health_and_checkpoint() {
    let serve = parse_args([
        "serve",
        "--data-dir",
        "state_dir",
        "--tcp",
        "127.0.0.1:7420",
        "--max-in-flight-ops",
        "1024",
    ])
    .expect("parse serve");
    assert!(matches!(serve, Command::Serve(args) if args.max_in_flight_ops == Some(1024)));

    let health = parse_args(["health", "--tcp", "127.0.0.1:7420"]).expect("parse health");
    assert!(matches!(health, Command::Health(c) if matches!(c.endpoint, Endpoint::Tcp(_))));

    let ckpt = parse_args([
        "checkpoint",
        "--tcp",
        "127.0.0.1:7420",
        "--dest",
        "backup_dir",
    ])
    .expect("parse checkpoint");
    assert!(matches!(ckpt, Command::Checkpoint { .. }));

    assert!(parse_args(["serve", "--data-dir", "only_dir"]).is_err());
    assert!(parse_args(["unknown"]).is_err());
}

async fn run_multiplexed_pipeline_scenario(client: Client) {
    let mut sub_each = client
        .subscribe_each_change("pipe", b"k/", 0, 0)
        .await
        .expect("subscribe_each_change");
    let mut sub_live = client
        .subscribe_live("pipe", b"hot/", Some(32))
        .await
        .expect("subscribe_live");

    // Blast 128 concurrent pipelined RPCs over the single shared connection.
    let mut tasks = Vec::new();
    for i in 0..128u32 {
        let c = client.clone();
        tasks.push(tokio::spawn(async move {
            let key = format!("k/{i:04}");
            let val = format!("v-{i:04}");
            let commit = c
                .cas("pipe", key.as_bytes(), val.as_bytes(), Ack::Fast, ExpectedVersion::Absent)
                .await
                .expect("cas put");
            let got = c.get("pipe", key.as_bytes()).await.expect("get").expect("some");
            assert_eq!(got.value, val.as_bytes());
            assert_eq!(got.meta.version, commit.seq);
        }));
    }
    for task in tasks {
        task.await.expect("task join");
    }

    // Verify all 128 events arrive cleanly on the multiplexed EachChange stream.
    let mut seen = 0usize;
    let mut prev_seq = 0u64;
    while seen < 128 {
        let ev = timeout(Duration::from_secs(5), sub_each.next_event())
            .await
            .expect("stream timeout")
            .expect("stream ok")
            .expect("event");
        assert!(ev.seq > prev_seq);
        prev_seq = ev.seq;
        seen += 1;
    }

    // Verify multiplexed LiveCoalesced stream on the same connection.
    for i in 0..20u32 {
        client
            .put("pipe", b"hot/x", format!("h-{i}").as_bytes(), Ack::Fast)
            .await
            .expect("put hot");
    }
    let live_ev = timeout(Duration::from_secs(2), sub_live.next_event())
        .await
        .expect("live timeout")
        .expect("live ok")
        .expect("live event");
    assert_eq!(live_ev.key, b"hot/x");

    // Snapshot & paginated scan + commit_tx over the same connection.
    let snap = client.open_snapshot().await.expect("open_snapshot");
    let page = snap
        .scan_prefix(
            "pipe",
            b"k/",
            ScanOptions {
                after_key: None,
                limit: 50,
                max_bytes: 1024 * 1024,
            },
        )
        .await
        .expect("scan_prefix");
    assert_eq!(page.records.len(), 50);
    assert!(page.truncated);
    snap.close().await.expect("close snapshot");

    let tx_commit = client
        .commit_tx(
            vec![VersionGuard {
                namespace: "pipe".into(),
                key: b"tx/1".to_vec(),
                expected: ExpectedVersion::Absent,
            }],
            vec![WriteMutation {
                namespace: "pipe".into(),
                key: b"tx/1".to_vec(),
                op: MutationOp::Put {
                    value: b"ok".to_vec(),
                },
            }],
            Ack::Flush,
        )
        .await
        .expect("commit_tx");
    assert!(tx_commit.seq > prev_seq);

    sub_each.cancel().await.expect("cancel each");
    sub_live.cancel().await.expect("cancel live");
}

#[tokio::test]
async fn tcp_single_connection_pipelined_rpcs_and_live_streams() {
    let dir = TempDir::new().expect("tempdir");
    let (_storage, server) = build_server(&dir, StorageConfig::default(), ServerConfig::default());
    let handle = server
        .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind tcp");

    let client = Client::connect(handle.endpoint()).await.expect("connect");
    run_multiplexed_pipeline_scenario(client).await;
    handle.shutdown().await;
}

#[cfg(windows)]
#[tokio::test]
async fn named_pipe_single_connection_pipelined_rpcs_and_live_streams() {
    let dir = TempDir::new().expect("tempdir");
    let (_storage, server) = build_server(&dir, StorageConfig::default(), ServerConfig::default());
    let pipe_name = format!(r"\\.\pipe\membus-v2-pipe-test-{}", std::process::id());
    let handle = server
        .bind_named_pipe(&pipe_name)
        .await
        .expect("bind named pipe");

    let client = Client::connect_named_pipe(&pipe_name)
        .await
        .expect("connect named pipe");
    run_multiplexed_pipeline_scenario(client).await;
    handle.shutdown().await;
}

#[tokio::test]
async fn supports_over_256_simultaneous_tcp_connections() {
    let dir = TempDir::new().expect("tempdir");
    let (_storage, server) = build_server(&dir, StorageConfig::default(), ServerConfig::default());
    let handle = server
        .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind tcp");
    let endpoint = handle.endpoint().clone();

    let mut clients = Vec::with_capacity(260);
    for _ in 0..260 {
        clients.push(Client::connect(&endpoint).await.expect("connect tcp"));
    }

    let health = clients[0].health().await.expect("health");
    assert!(
        health.active_connections >= 260,
        "expected >= 260 active TCP connections, got {}",
        health.active_connections
    );

    let mut futs = Vec::with_capacity(260);
    for (idx, client) in clients.iter().enumerate() {
        let c = client.clone();
        futs.push(tokio::spawn(async move {
            let k = format!("c/{idx:04}");
            c.put("scale", k.as_bytes(), b"ok", Ack::Fast)
                .await
                .expect("put");
            let rec = c.get("scale", k.as_bytes()).await.expect("get").expect("some");
            assert_eq!(rec.value, b"ok");
        }));
    }
    for f in futs {
        f.await.expect("join");
    }

    drop(clients);
    handle.shutdown().await;
}

#[cfg(windows)]
#[tokio::test]
async fn supports_over_256_simultaneous_named_pipe_connections() {
    let dir = TempDir::new().expect("tempdir");
    let (_storage, server) = build_server(&dir, StorageConfig::default(), ServerConfig::default());
    let pipe_name = format!(r"\\.\pipe\membus-v2-scale260-{}", std::process::id());
    let handle = server
        .bind_named_pipe(&pipe_name)
        .await
        .expect("bind named pipe");

    let mut clients = Vec::with_capacity(260);
    for _ in 0..260 {
        clients.push(
            Client::connect_named_pipe(&pipe_name)
                .await
                .expect("connect pipe"),
        );
    }

    let health = clients[0].health().await.expect("health");
    assert!(
        health.active_connections >= 260,
        "expected >= 260 active Named Pipe connections, got {}",
        health.active_connections
    );

    let mut futs = Vec::with_capacity(260);
    for (idx, client) in clients.iter().enumerate() {
        let c = client.clone();
        futs.push(tokio::spawn(async move {
            let k = format!("p/{idx:04}");
            c.put("pipe260", k.as_bytes(), b"ok", Ack::Fast)
                .await
                .expect("put");
            let rec = c
                .get("pipe260", k.as_bytes())
                .await
                .expect("get")
                .expect("some");
            assert_eq!(rec.value, b"ok");
        }));
    }
    for f in futs {
        f.await.expect("join");
    }

    drop(clients);
    handle.shutdown().await;
}

#[tokio::test]
async fn credit_window_backpressure_returns_busy_without_dropping_connection() {
    let dir = TempDir::new().expect("tempdir");
    let server_cfg = ServerConfig {
        max_in_flight_per_conn: 2,
        ..ServerConfig::default()
    };
    let (_storage, server) = build_server(&dir, StorageConfig::default(), server_cfg);
    let handle = server
        .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind tcp");

    let client = Client::connect(handle.endpoint()).await.expect("connect");

    let mut tasks = Vec::new();
    for i in 0..64u32 {
        let c = client.clone();
        tasks.push(tokio::spawn(async move {
            c.put(
                "bp",
                format!("k/{i}").as_bytes(),
                b"payload",
                Ack::Flush,
            )
            .await
        }));
    }

    let mut ok_count = 0usize;
    let mut busy_count = 0usize;
    for t in tasks {
        match t.await.expect("join") {
            Ok(_) => ok_count += 1,
            Err(MembusError::Busy { .. }) => busy_count += 1,
            Err(other) => panic!("unexpected error under backpressure: {other}"),
        }
    }
    assert!(ok_count > 0, "some requests should succeed");
    assert!(busy_count > 0, "expected Busy backpressure responses when credit window is 2");
    assert!(
        !client.is_closed(),
        "connection must stay open after Busy responses"
    );

    // Same connection continues working cleanly after burst settles.
    client.ping().await.expect("ping after busy");
    client
        .put("bp", b"after", b"recovered", Ack::Fast)
        .await
        .expect("put after busy");
    assert_eq!(
        client.get("bp", b"after").await.unwrap().unwrap().value,
        b"recovered"
    );

    handle.shutdown().await;
}

#[test]
fn truncated_wire_batch_counts_fail_without_oversized_allocation() {
    // GetMany opcode=2, ns="a" (len=1), count=65536, truncated immediately.
    let raw = [2u8, 0, 0, 0, 1, b'a', 0, 1, 0, 0];
    assert!(matches!(
        decode_request(&raw),
        Err(MembusError::Protocol(_))
    ));
}

#[tokio::test]
async fn duplicate_active_stream_id_is_rejected_without_clobbering_existing_stream() {
    use tokio::io::AsyncWriteExt;

    let dir = TempDir::new().expect("tempdir");
    let (_storage, server) = build_server(&dir, StorageConfig::default(), ServerConfig::default());
    let handle = server
        .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind tcp");

    let addr = match handle.endpoint() {
        Endpoint::Tcp(a) => *a,
        _ => unreachable!(),
    };
    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");

    let sub_req = encode_request(&Request::SubscribeLive {
        namespace: "dup".into(),
        key_prefix: Vec::new(),
        max_pending: Some(16),
    })
    .expect("encode subscribe");

    // Send first SubscribeLive on stream_id=7 -> expect Subscribed.
    let mut out = BytesMut::new();
    encode_frame_into(
        &Frame::new(FrameKind::Request, 7, sub_req.clone()),
        &mut out,
        DEFAULT_MAX_FRAME_BODY,
    )
    .unwrap();
    stream.write_all(&out).await.unwrap();

    let mut read_buf = BytesMut::new();
    let f1 = read_frame(&mut stream, &mut read_buf, DEFAULT_MAX_FRAME_BODY)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        decode_response(&f1.payload).unwrap(),
        Response::Subscribed { .. }
    ));

    // Reusing active stream_id=7 must fail cleanly with Protocol error.
    out.clear();
    encode_frame_into(
        &Frame::new(FrameKind::Request, 7, sub_req),
        &mut out,
        DEFAULT_MAX_FRAME_BODY,
    )
    .unwrap();
    stream.write_all(&out).await.unwrap();

    let f2 = read_frame(&mut stream, &mut read_buf, DEFAULT_MAX_FRAME_BODY)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        decode_response(&f2.payload).unwrap(),
        Response::Error(MembusError::Protocol(_))
    ));

    drop(stream);
    handle.shutdown().await;
}

#[tokio::test]
async fn slow_client_subscription_never_deadlocks_shared_connection_rpcs() {
    let dir = TempDir::new().expect("tempdir");
    let (_storage, server) = build_server(&dir, StorageConfig::default(), ServerConfig::default());
    let handle = server
        .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind tcp");

    let addr = match handle.endpoint() {
        Endpoint::Tcp(a) => *a,
        _ => unreachable!(),
    };
    let stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let client = Client::from_stream_with_capacity(stream, DEFAULT_MAX_FRAME_BODY, 4);

    // Open a subscription with a 4-slot client buffer and NEVER drain it during writes.
    let mut stalled_sub = client
        .subscribe_each_change("flood", b"", 0, 0)
        .await
        .expect("subscribe");

    for i in 0..40u32 {
        timeout(
            Duration::from_secs(2),
            client.put("flood", format!("k/{i}").as_bytes(), b"payload", Ack::Fast),
        )
        .await
        .expect("put must not deadlock on slow local subscription")
        .expect("put ok");
    }

    // Unrelated RPCs on the exact same Client keep working cleanly.
    timeout(Duration::from_secs(2), client.ping())
        .await
        .expect("ping must not deadlock")
        .expect("ping ok");
    assert!(!client.is_closed());

    // Draining the lagging subscription eventually surfaces explicit overflow Busy error.
    let mut saw_overflow = false;
    for _ in 0..50 {
        match timeout(Duration::from_secs(2), stalled_sub.next_event())
            .await
            .expect("recv timeout")
        {
            Ok(Some(_)) => continue,
            Err(MembusError::Busy { .. }) => {
                saw_overflow = true;
                break;
            }
            other => panic!("unexpected lagging stream result: {other:?}"),
        }
    }
    assert!(saw_overflow, "expected explicit overflow error on lagged subscription");

    handle.shutdown().await;
}

#[tokio::test]
async fn client_timeout_drop_cleans_up_pending_rpc_map() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind dummy");
    let addr = listener.local_addr().unwrap();
    let _server = tokio::spawn(async move {
        let (_sock, _) = listener.accept().await.unwrap();
        // Hold socket open without replying.
        tokio::time::sleep(Duration::from_secs(5)).await;
    });

    let client = Client::connect_tcp(addr).await.expect("connect");
    let timed_out = timeout(Duration::from_millis(40), client.get("ns", b"k")).await;
    assert!(timed_out.is_err(), "expected timeout");
    assert_eq!(
        client.pending_rpc_count(),
        0,
        "dropped future must remove pending RPC map entry cleanly"
    );
}

#[tokio::test]
async fn outgoing_frame_size_limit_and_scan_limit_preserve_connection() {
    let dir = TempDir::new().expect("tempdir");
    let server_cfg = ServerConfig {
        max_frame_body: 512,
        ..ServerConfig::default()
    };
    let (storage, server) = build_server(&dir, StorageConfig::default(), server_cfg);
    // Insert a 1024-byte value directly into storage so server response exceeds 512B.
    storage
        .put("big", b"k1", &vec![0xAB; 1024], Ack::Fast, ExpectedVersion::Any)
        .expect("seed big");

    let handle = server
        .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind");
    let addr = match handle.endpoint() {
        Endpoint::Tcp(a) => *a,
        _ => unreachable!(),
    };
    let stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let client = Client::from_stream(stream, 512);

    // Server-side oversized response returns FrameTooLarge without dropping connection.
    let err = client.get("big", b"k1").await.unwrap_err();
    assert!(matches!(err, MembusError::FrameTooLarge { .. }));
    assert!(!client.is_closed());

    // Client-side oversized request returns FrameTooLarge without dropping connection.
    let client_err = client
        .put("big", b"k2", &vec![0xCD; 1024], Ack::Fast)
        .await
        .unwrap_err();
    assert!(matches!(client_err, MembusError::FrameTooLarge { .. }));
    assert!(!client.is_closed());

    // Checked ScanOptions::limit rejects > u32::MAX cleanly.
    #[cfg(target_pointer_width = "64")]
    {
        let limit_err = client
            .scan_prefix(
                "big",
                b"",
                ScanOptions {
                    after_key: None,
                    limit: (u32::MAX as usize) + 1,
                    max_bytes: 1024,
                },
                None,
            )
            .await
            .unwrap_err();
        assert!(matches!(limit_err, MembusError::InvalidArgument(_)));
    }

    // Connection remains fully healthy.
    client.ping().await.expect("ping after FrameTooLarge");
    handle.shutdown().await;
}

#[tokio::test]
async fn snapshot_ttl_reaps_idle_snapshots_and_enforces_cap() {
    let dir = TempDir::new().expect("tempdir");
    let server_cfg = ServerConfig {
        max_snapshots_per_conn: 2,
        snapshot_ttl: Duration::from_millis(80),
        ..ServerConfig::default()
    };
    let (_storage, server) = build_server(&dir, StorageConfig::default(), server_cfg);
    let handle = server
        .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind");
    let client = Client::connect(handle.endpoint()).await.expect("connect");

    let s1 = client.open_snapshot().await.expect("s1");
    let _s2 = client.open_snapshot().await.expect("s2");
    assert!(matches!(
        client.open_snapshot().await,
        Err(MembusError::Busy { .. })
    ));

    // Wait past snapshot_ttl: idle snapshots are reaped automatically.
    tokio::time::sleep(Duration::from_millis(130)).await;
    assert!(matches!(
        s1.get("ns", b"k").await,
        Err(MembusError::NotFound(_))
    ));
    let s3 = client.open_snapshot().await.expect("s3 after reap");
    s3.close().await.expect("close s3");

    handle.shutdown().await;
}

#[tokio::test]
async fn server_shutdown_does_not_hang_on_stalled_client() {
    use tokio::io::AsyncWriteExt;

    let dir = TempDir::new().expect("tempdir");
    let server_cfg = ServerConfig {
        outbound_queue_capacity: 2,
        ..ServerConfig::default()
    };
    let (_storage, server) = build_server(&dir, StorageConfig::default(), server_cfg);
    let handle = server
        .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind");
    let addr = match handle.endpoint() {
        Endpoint::Tcp(a) => *a,
        _ => unreachable!(),
    };

    // Connect a client that blasts pings and never reads any replies.
    let mut stalled = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let mut out = BytesMut::new();
    for id in 1..=64u64 {
        encode_frame_into(
            &Frame::new(FrameKind::Ping, id, Bytes::from(vec![0u8; 4096])),
            &mut out,
            DEFAULT_MAX_FRAME_BODY,
        )
        .unwrap();
    }
    let _ = stalled.write_all(&out).await;

    timeout(Duration::from_secs(2), handle.shutdown())
        .await
        .expect("server shutdown must not hang on stalled client");
    drop(stalled);
}

#[tokio::test]
async fn dropping_client_closes_server_connection_promptly() {
    let dir = TempDir::new().expect("tempdir");
    let (_storage, server) = build_server(&dir, StorageConfig::default(), ServerConfig::default());
    let handle = server
        .bind_tcp("127.0.0.1:0".parse::<SocketAddr>().unwrap())
        .await
        .expect("bind");

    let client = Client::connect(handle.endpoint()).await.expect("connect");
    client.ping().await.expect("ping");
    assert_eq!(handle.active_connections(), 1);

    drop(client);

    let closed = timeout(Duration::from_secs(2), async {
        while handle.active_connections() > 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(
        closed.is_ok(),
        "dropping all Client handles must tear down server connection promptly (active={})",
        handle.active_connections()
    );

    handle.shutdown().await;
}
