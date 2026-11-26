use std::sync::Arc;
use std::time::Duration;

use membus::{
    Ack, ExpectedVersion, MembusError, MutationOp, RocksDbStorage, SignalBroker, SignalConfig,
    SignalKind, SignalPublisher, SignalRing, StorageConfig, StorageEngine, WriteMutation,
};
use tempfile::TempDir;
use tokio::time::timeout;

fn setup_broker(
    dir: &TempDir,
    signal_cfg: SignalConfig,
) -> (Arc<RocksDbStorage>, Arc<SignalBroker>) {
    let ring = Arc::new(SignalRing::new(signal_cfg));
    let storage = Arc::new(
        RocksDbStorage::open(
            dir.path(),
            StorageConfig::default(),
            Arc::clone(&ring) as Arc<dyn SignalPublisher>,
        )
        .expect("open storage"),
    );
    let broker = Arc::new(SignalBroker::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        ring,
        signal_cfg,
    ));
    (storage, broker)
}

#[tokio::test]
async fn each_change_instant_ram_ring_delivery_and_prefix_filter() {
    let dir = TempDir::new().expect("tempdir");
    let (storage, broker) = setup_broker(&dir, SignalConfig::default());

    let mut sub = broker
        .subscribe_each_change("events", b"order/".to_vec(), 0, 0)
        .expect("subscribe");

    let s = Arc::clone(&storage);
    tokio::spawn(async move {
        s.put("events", b"ignore/1", b"x", Ack::Fast, ExpectedVersion::Any)
            .unwrap();
        s.put("events", b"order/1", b"a", Ack::Fast, ExpectedVersion::Any)
            .unwrap();
        s.put("other", b"order/2", b"b", Ack::Fast, ExpectedVersion::Any)
            .unwrap();
        s.remove("events", b"order/1", Ack::Fast, ExpectedVersion::Any)
            .unwrap();
    });

    let ev1 = timeout(Duration::from_secs(2), sub.next_event())
        .await
        .expect("timeout")
        .expect("ok")
        .expect("some");
    assert_eq!(ev1.namespace, "events");
    assert_eq!(ev1.key, b"order/1");
    assert_eq!(ev1.kind, SignalKind::Put);
    assert_eq!(ev1.value_hint.as_deref(), Some(b"a".as_slice()));

    let ev2 = timeout(Duration::from_secs(2), sub.next_event())
        .await
        .expect("timeout")
        .expect("ok")
        .expect("some");
    assert_eq!(ev2.key, b"order/1");
    assert_eq!(ev2.kind, SignalKind::Delete);
    assert!(ev2.seq > ev1.seq);
}

#[tokio::test]
async fn lagging_subscriber_catches_up_from_log_cf_and_transitions_to_ram_ring() {
    let dir = TempDir::new().expect("tempdir");
    let signal_cfg = SignalConfig {
        ring_capacity_per_namespace: 16,
        default_live_max_pending: 64,
        log_catchup_batch_size: 11,
        ..SignalConfig::default()
    };
    let (storage, broker) = setup_broker(&dir, signal_cfg);

    // Write 120 events so the 16-slot RAM ring evicts the first ~104 events.
    for i in 1..=120u64 {
        storage
            .put(
                "stream",
                format!("k/{i:04}").as_bytes(),
                format!("v-{i}").as_bytes(),
                Ack::Fast,
                ExpectedVersion::Any,
            )
            .expect("put");
    }

    let mut sub = broker
        .subscribe_each_change("stream", Vec::new(), broker.epoch(), 0)
        .expect("subscribe");

    let mut last_seq = 0u64;
    for i in 1..=120u64 {
        let ev = timeout(Duration::from_secs(2), sub.next_event())
            .await
            .expect("timeout")
            .expect("ok")
            .expect("event");
        assert!(ev.seq > last_seq, "non-monotonic seq {} <= {}", ev.seq, last_seq);
        assert_eq!(ev.key, format!("k/{i:04}").as_bytes());
        assert_eq!(ev.value_hint.as_deref(), Some(format!("v-{i}").as_bytes()));
        last_seq = ev.seq;
    }

    // Publish 25 more live events and verify seamless transition onto RAM ring.
    let s = Arc::clone(&storage);
    tokio::spawn(async move {
        for i in 121..=145u64 {
            s.put(
                "stream",
                format!("k/{i:04}").as_bytes(),
                format!("v-{i}").as_bytes(),
                Ack::Fast,
                ExpectedVersion::Any,
            )
            .expect("put live");
        }
    });

    for i in 121..=145u64 {
        let ev = timeout(Duration::from_secs(2), sub.next_event())
            .await
            .expect("timeout")
            .expect("ok")
            .expect("live event");
        assert!(ev.seq > last_seq);
        assert_eq!(ev.key, format!("k/{i:04}").as_bytes());
        last_seq = ev.seq;
    }
}

#[tokio::test]
async fn multi_key_commit_tx_events_never_split_across_batch_boundary() {
    let dir = TempDir::new().expect("tempdir");
    let signal_cfg = SignalConfig {
        ring_capacity_per_namespace: 16,
        default_live_max_pending: 64,
        log_catchup_batch_size: 1,
        ..SignalConfig::default()
    };
    let (storage, broker) = setup_broker(&dir, signal_cfg);

    let mut sub = broker
        .subscribe_each_change("txns", Vec::new(), 0, 0)
        .expect("subscribe");

    let mutations: Vec<WriteMutation> = (0..5)
        .map(|i| WriteMutation {
            namespace: "txns".into(),
            key: format!("k{i}").into_bytes(),
            op: MutationOp::Put {
                value: vec![i as u8],
            },
        })
        .collect();
    let res = storage
        .commit_tx(&[], &mutations, Ack::Fast)
        .expect("commit_tx");

    let mut keys = Vec::new();
    for _ in 0..5 {
        let ev = timeout(Duration::from_secs(2), sub.next_event())
            .await
            .expect("timeout")
            .expect("ok")
            .expect("event");
        assert_eq!(ev.seq, res.seq);
        keys.push(ev.key);
    }
    assert_eq!(
        keys,
        vec![
            b"k0".to_vec(),
            b"k1".to_vec(),
            b"k2".to_vec(),
            b"k3".to_vec(),
            b"k4".to_vec(),
        ]
    );
}

#[tokio::test]
async fn trimmed_horizon_and_wrong_epoch_return_explicit_gap() {
    let dir = TempDir::new().expect("tempdir");
    let (storage, broker) = setup_broker(&dir, SignalConfig::default());

    for i in 1..=30u64 {
        storage
            .put("trim", format!("k{i}").as_bytes(), b"v", Ack::Fast, ExpectedVersion::Any)
            .expect("put");
    }

    // Wrong epoch fails immediately with explicit Gap.
    let epoch_err = broker
        .subscribe_each_change("trim", Vec::new(), broker.epoch() + 99, 0)
        .err()
        .expect("epoch mismatch");
    assert!(matches!(epoch_err, MembusError::Gap { .. }));

    // Open a live subscriber at seq 5 before trimming.
    let mut stalled = broker
        .subscribe_each_change("trim", Vec::new(), broker.epoch(), 5)
        .expect("sub before trim");

    storage.trim_log_before(20).expect("trim_log_before");

    // New subscription behind horizon gets explicit Gap immediately.
    let gap_err = broker
        .subscribe_each_change("trim", Vec::new(), broker.epoch(), 10)
        .err()
        .expect("gap expected");
    assert!(matches!(
        gap_err,
        MembusError::Gap {
            earliest_available_seq: 20,
            ..
        }
    ));

    // Existing stalled subscriber also observes explicit Gap on next_event().
    let stalled_err = stalled.next_event().await.unwrap_err();
    assert!(matches!(
        stalled_err,
        MembusError::Gap {
            earliest_available_seq: 20,
            ..
        }
    ));

    // Subscribing at seq 19 (at horizon) succeeds and yields seq 20..=30 cleanly.
    let mut valid = broker
        .subscribe_each_change("trim", Vec::new(), broker.epoch(), 19)
        .expect("valid at horizon");
    let ev = timeout(Duration::from_secs(2), valid.next_event())
        .await
        .expect("timeout")
        .expect("ok")
        .expect("event");
    assert_eq!(ev.seq, 20);
}

#[tokio::test]
async fn each_change_replays_persisted_log_after_restart() {
    let dir = TempDir::new().expect("tempdir");
    {
        let (storage, _broker) = setup_broker(&dir, SignalConfig::default());
        for i in 1..=15u64 {
            storage
                .put(
                    "durable",
                    format!("k{i}").as_bytes(),
                    b"v",
                    Ack::Flush,
                    ExpectedVersion::Any,
                )
                .expect("put flush");
        }
    }

    let (storage, broker) = setup_broker(&dir, SignalConfig::default());
    let mut sub = broker
        .subscribe_each_change("durable", Vec::new(), broker.epoch(), 0)
        .expect("subscribe after restart");

    for i in 1..=15u64 {
        let ev = timeout(Duration::from_secs(2), sub.next_event())
            .await
            .expect("timeout")
            .expect("ok")
            .expect("replayed");
        assert_eq!(ev.seq, i);
        assert_eq!(ev.key, format!("k{i}").as_bytes());
    }

    storage
        .put("durable", b"k16", b"live", Ack::Fast, ExpectedVersion::Any)
        .expect("live write");
    let live = timeout(Duration::from_secs(2), sub.next_event())
        .await
        .expect("timeout")
        .expect("ok")
        .expect("live");
    assert_eq!(live.seq, 16);
    assert_eq!(live.key, b"k16");
}

#[tokio::test]
async fn live_coalesced_preserves_oldest_fifo_ticket_anti_starvation() {
    let dir = TempDir::new().expect("tempdir");
    let (storage, broker) = setup_broker(&dir, SignalConfig::default());

    let mut sub = broker
        .subscribe_live("field", Vec::new(), Some(64))
        .expect("subscribe_live");

    // Arrival order: key_a (ticket 1), key_b (ticket 2), key_c (ticket 3).
    storage
        .put("field", b"key_a", b"a-0", Ack::Fast, ExpectedVersion::Any)
        .unwrap();
    storage
        .put("field", b"key_b", b"b-0", Ack::Fast, ExpectedVersion::Any)
        .unwrap();
    storage
        .put("field", b"key_c", b"c-0", Ack::Fast, ExpectedVersion::Any)
        .unwrap();

    // Spam bursts on key_a and key_b: must coalesce in place without changing FIFO order.
    for i in 1..=50u32 {
        storage
            .put(
                "field",
                b"key_a",
                format!("a-{i}").as_bytes(),
                Ack::Fast,
                ExpectedVersion::Any,
            )
            .unwrap();
    }
    for i in 1..=10u32 {
        storage
            .put(
                "field",
                b"key_b",
                format!("b-{i}").as_bytes(),
                Ack::Fast,
                ExpectedVersion::Any,
            )
            .unwrap();
    }

    assert_eq!(sub.pending_count(), 3);

    let e1 = sub.next_event().await.unwrap().unwrap();
    let e2 = sub.next_event().await.unwrap().unwrap();
    let e3 = sub.next_event().await.unwrap().unwrap();

    assert_eq!(e1.key, b"key_a");
    assert_eq!(e1.value_hint.as_deref(), Some(b"a-50".as_slice()));
    assert_eq!(e2.key, b"key_b");
    assert_eq!(e2.value_hint.as_deref(), Some(b"b-10".as_slice()));
    assert_eq!(e3.key, b"key_c");
    assert_eq!(e3.value_hint.as_deref(), Some(b"c-0".as_slice()));

    // Now key_d arrives before a fresh update to key_a -> key_d must dequeue first.
    storage
        .put("field", b"key_d", b"d-1", Ack::Fast, ExpectedVersion::Any)
        .unwrap();
    storage
        .put("field", b"key_a", b"a-51", Ack::Fast, ExpectedVersion::Any)
        .unwrap();
    let e4 = sub.next_event().await.unwrap().unwrap();
    let e5 = sub.next_event().await.unwrap().unwrap();
    assert_eq!(e4.key, b"key_d");
    assert_eq!(e5.key, b"key_a");
}

#[tokio::test]
async fn slow_subscribers_never_block_writers_or_fast_subscribers() {
    let dir = TempDir::new().expect("tempdir");
    let (storage, broker) = setup_broker(&dir, SignalConfig::default());

    let _stalled_each = broker
        .subscribe_each_change("iso", Vec::new(), 0, 0)
        .expect("stalled each");
    let mut stalled_live = broker
        .subscribe_live("iso", Vec::new(), Some(8))
        .expect("stalled live");
    let mut fast_each = broker
        .subscribe_each_change("iso", Vec::new(), 0, 0)
        .expect("fast each");

    assert_eq!(broker.active_subscriptions(), 3);

    for i in 0..300u32 {
        storage
            .put(
                "iso",
                format!("k/{i:04}").as_bytes(),
                b"v",
                Ack::Fast,
                ExpectedVersion::Any,
            )
            .expect("write must never block");
    }

    for i in 0..300u32 {
        let ev = timeout(Duration::from_secs(2), fast_each.next_event())
            .await
            .expect("fast subscriber timeout")
            .expect("ok")
            .expect("event");
        assert_eq!(ev.key, format!("k/{i:04}").as_bytes());
    }

    // Stalled LiveCoalesced surfaces explicit Gap immediately on overflow so sustained
    // distinct-key overload cannot hide dropped keys behind a refilled FIFO queue.
    let overflow = stalled_live.next_event().await.unwrap_err();
    assert!(matches!(overflow, MembusError::Gap { .. }));
}

#[tokio::test]
async fn atomic_multi_key_commit_tx_wakes_atomically_and_handles_oversized_batch() {
    let dir = TempDir::new().expect("tempdir");
    let signal_cfg = SignalConfig {
        ring_capacity_per_namespace: 16,
        log_catchup_batch_size: 8,
        ..SignalConfig::default()
    };
    let (storage, broker) = setup_broker(&dir, signal_cfg);

    let mut sub = broker
        .subscribe_each_change("batch_ns", Vec::new(), 0, 0)
        .expect("subscribe");

    // 30 concurrent 4-key atomic transactions while subscriber is actively reading.
    let s = Arc::clone(&storage);
    let writer = tokio::spawn(async move {
        for tx_idx in 0..30u32 {
            let muts: Vec<WriteMutation> = (0..4)
                .map(|k| WriteMutation {
                    namespace: "batch_ns".into(),
                    key: format!("t{tx_idx:02}/k{k}").into_bytes(),
                    op: MutationOp::Put {
                        value: vec![tx_idx as u8, k as u8],
                    },
                })
                .collect();
            s.commit_tx(&[], &muts, Ack::Fast).expect("commit_tx");
        }
    });

    for tx_idx in 0..30u32 {
        let mut expected_seq = None;
        for k in 0..4u32 {
            let ev = timeout(Duration::from_secs(2), sub.next_event())
                .await
                .expect("timeout")
                .expect("ok")
                .expect("event");
            if let Some(seq) = expected_seq {
                assert_eq!(ev.seq, seq, "all keys of commit_tx must share one seq");
            } else {
                expected_seq = Some(ev.seq);
            }
            assert_eq!(ev.key, format!("t{tx_idx:02}/k{k}").as_bytes());
        }
    }
    writer.await.expect("writer join");

    // Now commit a single 24-key atomic tx larger than ring capacity (16):
    // must evict whole-seq cleanly and catch up all 24 keys from CF_LOG without loss.
    let huge_muts: Vec<WriteMutation> = (0..24u32)
        .map(|k| WriteMutation {
            namespace: "batch_ns".into(),
            key: format!("huge/{k:02}").into_bytes(),
            op: MutationOp::Put {
                value: vec![k as u8],
            },
        })
        .collect();
    let huge_res = storage
        .commit_tx(&[], &huge_muts, Ack::Fast)
        .expect("huge commit_tx");
    storage
        .put("batch_ns", b"after_huge", b"ok", Ack::Fast, ExpectedVersion::Any)
        .expect("post huge put");

    for k in 0..24u32 {
        let ev = timeout(Duration::from_secs(2), sub.next_event())
            .await
            .expect("huge timeout")
            .expect("ok")
            .expect("event");
        assert_eq!(ev.seq, huge_res.seq);
        assert_eq!(ev.key, format!("huge/{k:02}").as_bytes());
    }
    let tail = timeout(Duration::from_secs(2), sub.next_event())
        .await
        .expect("tail timeout")
        .expect("ok")
        .expect("event");
    assert_eq!(tail.key, b"after_huge");
}

#[tokio::test]
async fn each_change_cursor_seq_tracks_delivered_events_and_rejects_future_since_seq() {
    let dir = TempDir::new().expect("tempdir");
    let (storage, broker) = setup_broker(&dir, SignalConfig::default());

    for i in 1..=20u64 {
        storage
            .put("cur", format!("k{i}").as_bytes(), b"v", Ack::Fast, ExpectedVersion::Any)
            .expect("put");
    }

    // Future since_seq > latest_seq must surface explicit Gap (rollback/wipe guard).
    let future_err = broker
        .subscribe_each_change("cur", Vec::new(), broker.epoch(), 999)
        .err()
        .expect("future since_seq must fail with Gap");
    assert!(matches!(future_err, MembusError::Gap { .. }));

    // cursor_seq() advances one delivered event at a time, never jumping ahead to batch end.
    let mut sub = broker
        .subscribe_each_change("cur", Vec::new(), broker.epoch(), 0)
        .expect("subscribe");
    assert_eq!(sub.cursor_seq(), 0);
    for expected in 1..=20u64 {
        let ev = sub.next_event().await.expect("ok").expect("event");
        assert_eq!(ev.seq, expected);
        assert_eq!(sub.cursor_seq(), expected);
    }
}

#[tokio::test]
async fn signal_ring_bounds_ephemeral_namespaces_and_clamps_live_max_pending() {
    let dir = TempDir::new().expect("tempdir");
    let signal_cfg = SignalConfig {
        max_namespaces: 64,
        max_live_pending: 8,
        ..SignalConfig::default()
    };
    let ring = Arc::new(SignalRing::new(signal_cfg));
    let storage = Arc::new(
        RocksDbStorage::open(
            dir.path(),
            StorageConfig::default(),
            Arc::clone(&ring) as Arc<dyn SignalPublisher>,
        )
        .expect("open"),
    );
    let broker = Arc::new(SignalBroker::new(
        Arc::clone(&storage) as Arc<dyn StorageEngine>,
        Arc::clone(&ring),
        signal_cfg,
    ));

    for i in 0..200u32 {
        storage
            .put(
                &format!("ephemeral_{i}"),
                b"k",
                b"v",
                Ack::Fast,
                ExpectedVersion::Any,
            )
            .expect("put");
    }
    assert!(
        ring.namespace_count() <= 64,
        "expected bounded namespace channels <= 64, got {}",
        ring.namespace_count()
    );

    // Client requesting 100_000 pending slots is clamped to server's max_live_pending (8).
    let mut sub = broker
        .subscribe_live("clamped", Vec::new(), Some(100_000))
        .expect("sub live");
    for i in 0..12u32 {
        storage
            .put("clamped", format!("k{i}").as_bytes(), b"v", Ack::Fast, ExpectedVersion::Any)
            .expect("put");
    }
    let err = sub.next_event().await.unwrap_err();
    assert!(matches!(err, MembusError::Gap { .. }));
}

#[tokio::test]
async fn need_log_catchup_non_matching_page_delivers_ram_event_without_hanging() {
    let dir = TempDir::new().expect("tempdir");
    let signal_cfg = SignalConfig {
        ring_capacity_per_namespace: 16,
        log_catchup_batch_size: 20,
        ..SignalConfig::default()
    };
    let (storage, broker) = setup_broker(&dir, signal_cfg);

    // Push 20 non-matching events so the 16-slot ring evicts seqs 1..=4, then push
    // a matching event at seq 21 sitting in RAM right beyond the 20-entry catch-up page.
    for i in 1..=20u32 {
        storage
            .put(
                "mix",
                format!("ignore/{i:03}").as_bytes(),
                b"skip",
                Ack::Fast,
                ExpectedVersion::Any,
            )
            .expect("put non-matching");
    }
    let hit_res = storage
        .put("mix", b"want/hit", b"target", Ack::Fast, ExpectedVersion::Any)
        .expect("put matching");

    let mut sub = broker
        .subscribe_each_change("mix", b"want/".to_vec(), broker.epoch(), 0)
        .expect("subscribe");

    let ev = timeout(Duration::from_secs(2), sub.next_event())
        .await
        .expect("must not hang after non-matching catch-up page")
        .expect("ok")
        .expect("event");
    assert_eq!(ev.seq, hit_res.seq);
    assert_eq!(ev.key, b"want/hit");
    assert_eq!(sub.cursor_seq(), hit_res.seq);
}

#[tokio::test]
async fn evicted_idle_namespace_channel_preserves_watermark_and_catches_up_via_log() {
    let dir = TempDir::new().expect("tempdir");
    let signal_cfg = SignalConfig {
        max_namespaces: 64, // 1 channel slot per shard across 64 shards
        ..SignalConfig::default()
    };
    let (storage, broker) = setup_broker(&dir, signal_cfg);

    let first = storage
        .put("evict_target", b"k1", b"v1", Ack::Fast, ExpectedVersion::Any)
        .expect("put target");

    // Churn 256 ephemeral namespaces so `evict_target`'s idle channel is evicted from its shard.
    for i in 0..256u32 {
        storage
            .put(
                &format!("ephem_{i}"),
                b"k",
                b"v",
                Ack::Fast,
                ExpectedVersion::Any,
            )
            .expect("put ephemeral");
    }

    // Subscribing at since_seq=0 recreates the channel with the shard's preserved
    // `evicted_through_seq`, forcing CF_LOG catch-up so pre-eviction `k1` is never lost.
    let mut sub = broker
        .subscribe_each_change("evict_target", Vec::new(), broker.epoch(), 0)
        .expect("subscribe recreated namespace");

    let ev1 = timeout(Duration::from_secs(2), sub.next_event())
        .await
        .expect("timeout receiving pre-eviction event")
        .expect("ok")
        .expect("event");
    assert_eq!(ev1.seq, first.seq);
    assert_eq!(ev1.key, b"k1");

    let second = storage
        .put("evict_target", b"k2", b"v2", Ack::Fast, ExpectedVersion::Any)
        .expect("put live");
    let ev2 = timeout(Duration::from_secs(2), sub.next_event())
        .await
        .expect("timeout receiving post-recreation live event")
        .expect("ok")
        .expect("event");
    assert_eq!(ev2.seq, second.seq);
    assert_eq!(ev2.key, b"k2");
}
