//! Typed deletion through real Store dispatch, persistence and replay, on explicit authored state.
use membus::{client::*, Config, Store};
use std::{
    collections::VecDeque,
    fs,
    io::{self, Read, Write},
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

struct Wire {
    store: Arc<Store>,
    input: VecDeque<u8>,
    lose_remove_reply: bool,
}
impl Read for Wire {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let n = bytes.len().min(self.input.len()).min(11);
        for b in &mut bytes[..n] {
            *b = self.input.pop_front().unwrap();
        }
        Ok(n)
    }
}
impl Write for Wire {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        assert_eq!(
            bytes.len(),
            4 + u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize
        );
        let reply = self.store.dispatch(&bytes[4..]);
        if !(self.lose_remove_reply && bytes[8] == membus::REMOVE) {
            self.input.extend(reply);
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Transport for Wire {
    fn deadline(&mut self, until: Instant) -> io::Result<()> {
        if Instant::now() >= until {
            Err(io::ErrorKind::TimedOut.into())
        } else {
            Ok(())
        }
    }
}
fn deadline() -> Deadline {
    Deadline {
        unix_ms: membus::now_ms() + 30000,
        local: Instant::now() + Duration::from_secs(30),
    }
}
fn config() -> Config {
    Config {
        cache_bytes: 65536,
        signal_bytes: 65536,
        flush_ms: 0,
        tick_ms: 60000,
        retire_ms: 60000,
        ..Config::default()
    }
}
fn root(label: &str) -> PathBuf {
    let base = PathBuf::from(
        std::env::var_os("MEMBUS_REMOVE_TEST_ROOT")
            .expect("explicit private authored state root required"),
    );
    assert!(base.is_absolute() && base.is_dir());
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = base.join(format!("{label}-{}-{stamp}", std::process::id()));
    fs::create_dir(&root).unwrap();
    println!("authored retained state {}", root.display());
    root
}
fn client(store: &Arc<Store>, lose_remove_reply: bool) -> Client<Wire> {
    Client::handshake(
        Wire {
            store: store.clone(),
            input: VecDeque::new(),
            lose_remove_reply,
        },
        Options::new("authored-remove", 65536),
        deadline().local,
    )
    .unwrap()
}
fn put(client: &mut Client<Wire>, id: &str, key: &str) -> u64 {
    let mut attempt = client
        .prepare(MutationRequest {
            operation_id: id.into(),
            namespace: "authored".into(),
            key: key.into(),
            value: id.as_bytes().to_vec(),
            ack: Ack::Os,
            expected: None,
        })
        .unwrap();
    client.mutate(&mut attempt, deadline()).unwrap().version
}
fn removal(id: &str, key: &str, expected: u64, ack: Ack) -> RemoveRequest {
    RemoveRequest {
        operation_id: id.into(),
        namespace: "authored".into(),
        key: key.into(),
        expected,
        ack,
    }
}
#[test]
fn typed_remove_conflict_absence_original_retry_and_replay() {
    let path = root("replay");
    let (old_epoch, removed_version, mut old_attempt, next_version) = {
        let store = Arc::new(Store::open(&path, config()).unwrap());
        let mut c = client(&store, false);
        let original = put(&mut c, "first", "item");
        let mut stale = c
            .prepare_remove(removal("stale", "item", original + 1, Ack::Flush))
            .unwrap();
        assert!(matches!(
            c.remove(&mut stale, deadline()),
            Err(Error::Refused(Refusal { code: 12, .. }))
        ));
        assert_eq!(
            c.get("authored", "item", "still-first", deadline())
                .unwrap()
                .unwrap()
                .version,
            original
        );
        let mut a = c
            .prepare_remove(removal("remove", "item", original, Ack::Flush))
            .unwrap();
        let removed = c.remove(&mut a, deadline()).unwrap();
        assert_eq!(removed.achieved, Ack::Flush);
        assert!(c
            .get("authored", "item", "absent", deadline())
            .unwrap()
            .is_none());
        let mut absent = c
            .prepare_remove(removal("absent-remove", "absent", 0, Ack::Os))
            .unwrap();
        c.remove(&mut absent, deadline()).unwrap();
        let next = put(&mut c, "successor", "item");
        assert_eq!(
            c.remove(&mut a, deadline()).unwrap().version,
            removed.version
        );
        assert_eq!(
            c.get(
                "authored",
                "item",
                "duplicate-did-not-delete-successor",
                deadline()
            )
            .unwrap()
            .unwrap()
            .version,
            next
        );
        (c.negotiated().epoch, removed.version, a, next)
    };
    let store = Arc::new(Store::open(&path, config()).unwrap());
    let mut c = client(&store, false);
    assert_ne!(c.negotiated().epoch, old_epoch);
    assert!(matches!(
        c.remove(&mut old_attempt, deadline()),
        Err(Error::EpochChanged { .. })
    ));
    let value = c
        .get("authored", "item", "replayed-successor", deadline())
        .unwrap()
        .unwrap();
    assert_eq!(value.version, next_version);
    assert_eq!(value.bytes, b"successor");
    assert!(c
        .get("authored", "absent", "replayed-tombstone", deadline())
        .unwrap()
        .is_none());
    assert!(next_version > removed_version);
    assert!(!store.health2().storage_error && !store.health2().read_only);
}
#[test]
fn reply_lost_after_real_remove_commit_is_not_no_effect() {
    let path = root("lost-reply");
    let store = Arc::new(Store::open(&path, config()).unwrap());
    let mut c = client(&store, false);
    let v = put(&mut c, "first", "item");
    let mut lost = client(&store, true);
    let mut a = lost
        .prepare_remove(removal("remove-lost", "item", v, Ack::Os))
        .unwrap();
    assert!(matches!(
        lost.remove(&mut a, deadline()),
        Err(Error::Uncertain { .. })
    ));
    assert!(!lost.is_usable());
    assert!(c
        .get("authored", "item", "observe-actual-commit", deadline())
        .unwrap()
        .is_none());
    let recovered = c.remove(&mut a, deadline()).unwrap();
    let newer = put(&mut c, "newer", "item");
    assert_eq!(
        c.remove(&mut a, deadline()).unwrap().version,
        recovered.version
    );
    assert_eq!(
        c.get("authored", "item", "retained-newer", deadline())
            .unwrap()
            .unwrap()
            .version,
        newer
    );
}
#[test]
fn failed_covering_flush_remains_uncertain_and_recovery_barrier_is_required() {
    let path = root("failed-flush");
    {
        let store = Arc::new(
            Store::open(
                &path,
                Config {
                    fail_sync: true,
                    ..config()
                },
            )
            .unwrap(),
        );
        let mut c = client(&store, false);
        let original = put(&mut c, "first", "item");
        let mut a = c
            .prepare_remove(removal("uncovered-remove", "item", original, Ack::Flush))
            .unwrap();
        assert!(matches!(
            c.remove(&mut a, deadline()),
            Err(Error::Uncertain { .. })
        ));
    }
    assert!(Store::open(
        &path,
        Config {
            fail_recovery_sync: true,
            ..config()
        }
    )
    .is_err());
    let store = Arc::new(Store::open(&path, config()).unwrap());
    let mut c = client(&store, false);
    assert!(c
        .get(
            "authored",
            "item",
            "complete-tombstone-replayed",
            deadline()
        )
        .unwrap()
        .is_none());
    assert!(!store.health2().storage_error && !store.health2().read_only);
}

// These are executable counterexamples to substituting older primitives for
// conditional publication. Passing means the limitation was observed, not that
// atomic publication or a scheduler was qualified.
#[test]
fn independent_cas_allows_two_claims_from_the_same_pinned_population() {
    let path = root("independent-cas");
    let store = Arc::new(Store::open(&path, config()).unwrap());
    let mut first = client(&store, false);
    put(&mut first, "one-unit", "capacity");
    let mut options = Options::new("pinned-counterexample", 65536);
    options.scans = true;
    let mut observer = Client::handshake(
        Wire {
            store: store.clone(),
            input: VecDeque::new(),
            lose_remove_reply: false,
        },
        options,
        deadline().local,
    )
    .unwrap();
    let captured = observer
        .snapshot_open("authored", "capture", deadline())
        .unwrap();
    let before = observer
        .scan_prefix(
            "authored",
            "",
            &captured,
            &[],
            128,
            65536,
            "before",
            deadline(),
        )
        .unwrap();
    assert!(before.done);
    assert_eq!(before.items.len(), 1);
    assert_eq!(before.items[0].0, "capacity");
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let workers: Vec<_> = ["claim-a", "claim-b"]
        .into_iter()
        .map(|key| {
            let store = store.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                let mut c = client(&store, false);
                let mut attempt = c
                    .prepare(MutationRequest {
                        operation_id: key.into(),
                        namespace: "authored".into(),
                        key: key.into(),
                        value: vec![1],
                        ack: Ack::Os,
                        expected: Some(0),
                    })
                    .unwrap();
                barrier.wait();
                c.mutate(&mut attempt, deadline()).unwrap()
            })
        })
        .collect();
    barrier.wait();
    let outcomes: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(outcomes.len(), 2);
    assert_ne!(outcomes[0].version, outcomes[1].version);
    let old = observer
        .scan_prefix(
            "authored",
            "claim",
            &captured,
            &[],
            128,
            65536,
            "old",
            deadline(),
        )
        .unwrap();
    assert!(old.done && old.items.is_empty());
    let fresh = observer
        .snapshot_open("authored", "fresh", deadline())
        .unwrap();
    let claims = observer
        .scan_prefix(
            "authored",
            "claim",
            &fresh,
            &[],
            128,
            65536,
            "claims",
            deadline(),
        )
        .unwrap();
    assert!(claims.done);
    assert_eq!(claims.items.len(), 2);
    assert_eq!(
        claims
            .items
            .iter()
            .map(|row| row.1.bytes[0] as u32)
            .sum::<u32>(),
        2
    );
    observer
        .snapshot_release("authored", &captured, "release-old", deadline())
        .unwrap();
    observer
        .snapshot_release("authored", &fresh, "release-new", deadline())
        .unwrap();
}

#[test]
fn unchanged_batch_keeps_one_effect_when_peer_admission_hits_quota() {
    let path = root("batch-partial");
    let store = Store::open(
        &path,
        Config {
            quota: 1,
            ..config()
        },
    )
    .unwrap();
    let mut payload = deadline().unix_ms.to_le_bytes().to_vec();
    membus::lp(&mut payload, b"batch-counterexample");
    membus::lp(&mut payload, b"authored");
    payload.extend(2u16.to_le_bytes());
    for key in ["first", "second"] {
        payload.push(membus::PUT);
        membus::lp(&mut payload, key.as_bytes());
        membus::lp(&mut payload, key.as_bytes());
        payload.push(1);
        membus::lp(&mut payload, &[1]);
    }
    let response = store.dispatch(&membus::frame(membus::BATCH, 1, &payload)[4..]);
    assert_eq!(&response[17..19], &[0, 0]);
    let mut c = membus::Cursor::new(&response[19..]);
    assert_eq!(c.u16().unwrap(), 2);
    let mut codes = Vec::new();
    for key in ["first", "second"] {
        let code = c.u16().unwrap();
        let version = c.u64().unwrap();
        let ack = c.u8().unwrap();
        let value = store.try_get("authored", key).unwrap();
        match code {
            0 => {
                let value = value.unwrap();
                assert_eq!(value.bytes, vec![1]);
                assert_eq!(value.version, version);
                assert_eq!(ack, 1);
            }
            8 => {
                assert!(value.is_none());
                assert_eq!(version, 0);
                assert_eq!(ack, 0);
            }
            code => panic!("unexpected batch item refusal {code}"),
        }
        codes.push(code);
    }
    c.end().unwrap();
    // Different keys may execute concurrently; either input item may win.
    codes.sort();
    assert_eq!(codes, vec![0, 8]);
}
