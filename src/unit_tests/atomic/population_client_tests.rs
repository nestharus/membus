//! Private dispatcher-backed controls; roots and protocol traces are retained.
use super::*;
use crate::{client::Options, Config, ManualClock};
use std::{
    io::{self, Read, Write},
    sync::{atomic::Ordering, Mutex},
    time::{Duration, Instant},
};

#[derive(Clone, Copy, Default)]
enum Fault {
    #[default]
    None,
    PageAndRelease,
    Release,
    WrongToken,
    WrongEpoch,
    Expire,
    LegacyHello,
    TruncatedHello,
    ExcessPageCount,
}
#[derive(Default)]
struct Trace {
    requests: Vec<Vec<u8>>,
    header: Option<Vec<u8>>,
    fault: Fault,
}
struct Peer {
    store: Arc<Store>,
    clock: Arc<ManualClock>,
    trace: Arc<Mutex<Trace>>,
    page_limit: u16,
    pending: Vec<u8>,
    reply: Vec<u8>,
}
impl Read for Peer {
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let n = out.len().min(self.reply.len());
        out[..n].copy_from_slice(&self.reply[..n]);
        self.reply.drain(..n);
        Ok(n)
    }
}
impl Write for Peer {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.pending.extend(bytes);
        if self.pending.len() >= 4 {
            let len = u32::from_le_bytes(self.pending[..4].try_into().unwrap()) as usize;
            if self.pending.len() == len + 4 {
                let mut request = std::mem::take(&mut self.pending);
                let op = request[8];
                let id = u64::from_le_bytes(request[9..17].try_into().unwrap());
                let mut trace = self.trace.lock().unwrap();
                trace.requests.push(request.clone());
                if op == crate::SCAN_PREFIX && matches!(trace.fault, Fault::Expire) {
                    self.clock.advance(self.store.config.snapshot_ttl_ms);
                }
                if op == crate::HELLO && matches!(trace.fault, Fault::LegacyHello) {
                    let mut payload = 4096u32.to_le_bytes().to_vec();
                    payload.push(0);
                    crate::lp(&mut payload, b"legacy");
                    request = crate::frame(op, id, &payload);
                }
                if op == crate::SCAN_PREFIX && matches!(trace.fault, Fault::ExcessPageCount) {
                    let mut c = Cursor::new(&request[17..]);
                    c.u64().unwrap();
                    c.text().unwrap();
                    c.text().unwrap();
                    c.bytes(16).unwrap();
                    if !c.bytes(2096).unwrap().is_empty() {
                        // Have the dispatcher produce a valid continuation with an extra row.
                        let at = request.len() - c.remainder().len();
                        request[at..at + 2].copy_from_slice(&(self.page_limit + 1).to_le_bytes());
                    }
                }
                self.reply = self.store.dispatch_for_limit(&request[4..], false, 4096);
                if op == crate::HELLO {
                    let at = 19 + 8 + 4 + 4; // Frame header, epoch, max_frame, max_value.
                    assert_eq!(
                        self.reply[at..at + 2],
                        (crate::MAX_BATCH as u16).to_le_bytes()
                    );
                    if matches!(trace.fault, Fault::TruncatedHello) {
                        self.reply.truncate(at + 1);
                        self.reply[..4].copy_from_slice(&((at - 3) as u32).to_le_bytes());
                    } else {
                        self.reply[at..at + 2].copy_from_slice(&self.page_limit.to_le_bytes());
                    }
                }
                if op == crate::ATOMIC_POPULATION {
                    trace.header = Some(self.reply[19..].to_vec());
                    if matches!(trace.fault, Fault::WrongEpoch) {
                        // MAS2 epoch follows magic and the length-prefixed namespace.
                        let at = 19 + 5 + 4 + 4 + "authored".len();
                        self.reply[at] ^= 1;
                    }
                }
                if op == crate::SCAN_PREFIX {
                    if matches!(trace.fault, Fault::WrongToken) {
                        self.reply[23] ^= 1;
                    }
                    if matches!(trace.fault, Fault::PageAndRelease) {
                        self.reply = crate::codec::reply_header(op, id, 21, 0);
                    }
                }
                if op == crate::SNAPSHOT_RELEASE
                    && matches!(trace.fault, Fault::Release | Fault::PageAndRelease)
                {
                    // The real release occurs first; alter only its associated reply.
                    self.reply = crate::codec::reply_header(op, id, 20, 0);
                }
            }
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
impl Transport for Peer {
    fn deadline(&mut self, _: Instant) -> io::Result<()> {
        Ok(())
    }
}
fn deadline() -> Deadline {
    Deadline {
        unix_ms: crate::now_ms() + 120_000,
        local: Instant::now() + Duration::from_secs(120),
    }
}
fn limits() -> PopulationLimits {
    PopulationLimits {
        max_keys: 1024,
        max_capture_bytes: 64 * 1024 * 1024,
        max_changes: 1024,
        max_original_bytes: (crate::MAX_FRAME / 3) as u32,
    }
}
fn peer_fixture(label: &str, page_limit: u16, fault: Fault) -> (Peer, std::path::PathBuf) {
    let base = std::path::PathBuf::from(
        std::env::var_os("MEMBUS_PROVIDER_TEST_ROOT").expect("explicit retained root"),
    );
    assert!(base.is_absolute() && base.is_dir());
    let root = base.join(format!(
        "client-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    println!("retained {}", root.display());
    let clock = ManualClock::new(crate::now_ms());
    clock.advance(1);
    let store = Arc::new(
        Store::open(
            &root,
            Config {
                cache_bytes: 0,
                signal_bytes: 0,
                tick_ms: 60_000,
                retire_ms: u64::MAX,
                flush_ms: 0,
                clock: Some(clock.clone()),
                ..Config::default()
            },
        )
        .unwrap(),
    );
    let view = store
        .capture_namespace_bounded("authored", limits())
        .unwrap();
    let request = view
        .prepare(
            "seed",
            AtomicAck::Flush,
            (0..300)
                .map(|n| FinalChange::put(&format!("k{n:04}"), format!("value-{n}").into_bytes()))
                .collect(),
        )
        .unwrap();
    store.publish_atomic(&request, deadline().unix_ms).unwrap();
    drop(view);
    let trace = Arc::new(Mutex::new(Trace {
        fault,
        ..Trace::default()
    }));
    (
        Peer {
            store,
            clock,
            trace,
            page_limit,
            pending: vec![],
            reply: vec![],
        },
        root,
    )
}
fn handshake(peer: Peer, scans: bool) -> Result<Client<Peer>, Error> {
    let mut options = Options::new("bounded-client-control", 4096);
    options.scans = scans;
    Client::handshake(peer, options, deadline().local)
}
fn fixture(
    label: &str,
    scans: bool,
    fault: Fault,
) -> (
    Arc<Store>,
    Client<Peer>,
    Arc<Mutex<Trace>>,
    std::path::PathBuf,
) {
    let (peer, root) = peer_fixture(label, crate::MAX_BATCH as u16, fault);
    let (store, trace) = (peer.store.clone(), peer.trace.clone());
    let client = handshake(peer, scans).unwrap();
    trace.lock().unwrap().requests.clear();
    (store, client, trace, root)
}
fn save(trace: &Arc<Mutex<Trace>>, root: &std::path::Path, store: &Store) {
    let trace = trace.lock().unwrap();
    for (n, frame) in trace.requests.iter().enumerate() {
        std::fs::write(root.join(format!("request-{n}.bin")), frame).unwrap();
    }
    if let Some(header) = &trace.header {
        std::fs::write(root.join("capture-header.bin"), header).unwrap();
    }
    let count = store.scan_usage.count.load(Ordering::Acquire);
    let charge = store.scan_usage.bytes.load(Ordering::Acquire);
    println!(
        "opcodes={:?} snapshots={count} charged_bytes={charge}",
        trace.requests.iter().map(|r| r[8]).collect::<Vec<_>>()
    );
    std::fs::write(
        root.join("usage.txt"),
        format!("snapshots={count}\ncharged_bytes={charge}\n"),
    )
    .unwrap();
}
#[test]
fn scans_disabled_refuses_before_header_and_retains_no_charge() {
    let (store, mut client, trace, root) = fixture("disabled", false, Fault::None);
    let result = client.capture_atomic_namespace_bounded("authored", "read", limits(), deadline());
    save(&trace, &root, &store);
    assert!(matches!(result, Err(Error::Invalid("scans unavailable"))));
    assert!(
        trace.lock().unwrap().requests.is_empty(),
        "no opcode 30 header acquisition"
    );
    assert_eq!(store.scan_usage.count.load(Ordering::Acquire), 0);
    assert_eq!(store.scan_usage.bytes.load(Ordering::Acquire), 0);
    assert!(client.is_usable());
    // Legacy unpaged capture continues to work with the same default options.
    assert_eq!(
        client
            .capture_atomic_namespace("empty-legacy", "legacy", deadline())
            .unwrap()
            .rows()
            .len(),
        0
    );
}

#[test]
fn subscription_and_timer_offers_advertise_scans_independently() {
    for (label, subscriptions, timers) in [
        ("subscription-scans", true, false),
        ("timer-scans", false, true),
    ] {
        let (peer, root) = peer_fixture(label, 128, Fault::None);
        let (store, trace) = (peer.store.clone(), peer.trace.clone());
        let mut options = Options::new("scan-discovery-control", 4096);
        assert!(!options.scans);
        options.subscriptions = subscriptions;
        let mut client = Client::handshake_with_features(
            peer,
            options,
            crate::client::AdditionalFeatures { timers },
            deadline().local,
        )
        .unwrap();
        assert!(client.negotiated().scans);
        assert_eq!(client.negotiated().subscriptions, subscriptions);
        assert_eq!(client.feature_limits().timers.is_some(), timers);
        let view = client
            .capture_atomic_namespace_bounded("authored", "read", limits(), deadline())
            .unwrap();
        assert_eq!(view.rows().len(), 300);
        assert_eq!(
            trace.lock().unwrap().requests.last().unwrap()[8],
            crate::SNAPSHOT_RELEASE
        );
        assert_eq!(store.scan_usage.bytes.load(Ordering::Acquire), 0);
        save(&trace, &root, &store);
    }
}
#[test]
fn enabled_pages_retain_exact_token_epoch_horizon_values_and_release() {
    for page_limit in [1, 2, crate::MAX_BATCH as u16] {
        assert_complete_population(page_limit);
    }
}
fn assert_complete_population(page_limit: u16) {
    let (peer, root) = peer_fixture(&format!("limit-{page_limit}"), page_limit, Fault::None);
    let (store, trace) = (peer.store.clone(), peer.trace.clone());
    let mut client = handshake(peer, true).unwrap();
    assert_eq!(client.negotiated().max_batch, usize::from(page_limit));
    let d = deadline();
    let view = client
        .capture_atomic_namespace_bounded("authored", "read", limits(), d)
        .unwrap();
    save(&trace, &root, &store);
    let trace = trace.lock().unwrap();
    let header =
        Header::decode(crate::atomic_wire::decode(trace.header.as_ref().unwrap()).unwrap())
            .unwrap();
    assert_eq!((view.epoch, view.expires), (header.epoch, header.expires));
    let original = view
        .prepare(
            "original",
            AtomicAck::Flush,
            vec![FinalChange::put("next", vec![7])],
        )
        .unwrap();
    let original_bytes = original.to_bytes();
    let original = crate::atomic::BorrowedOriginal::from_canonical_bytes(&original_bytes).unwrap();
    assert_eq!(
        (original.epoch(), original.expires_mono()),
        (header.epoch, header.expires)
    );
    assert_eq!(header.epoch, store.epoch);
    assert_eq!(view.rows().len(), 300);
    for (n, (key, value)) in view.rows().iter().enumerate() {
        assert_eq!(key, &format!("k{n:04}"));
        assert_eq!(value.bytes, format!("value-{n}").as_bytes());
        assert_eq!(value.ack, 2);
        assert!(value.version > 0);
    }
    let mut pages = 0;
    for request in &trace.requests {
        if matches!(request[8], crate::SCAN_PREFIX | crate::SNAPSHOT_RELEASE) {
            let mut c = Cursor::new(&request[17..]);
            let sent_deadline = c.u64().unwrap();
            assert_eq!(c.text().unwrap(), "read");
            assert_eq!(c.text().unwrap(), "authored");
            assert_eq!(c.bytes(16).unwrap(), header.token);
            if request[8] == crate::SCAN_PREFIX {
                assert_eq!(sent_deadline, d.unix_ms);
                assert_eq!(c.bytes(2096).unwrap().is_empty(), pages == 0);
                assert_eq!(c.u16().unwrap(), page_limit);
                assert_eq!(c.u32().unwrap(), 4096);
                assert_eq!(c.bound_ref().unwrap(), "");
                pages += 1;
            } else {
                assert!(sent_deadline < d.unix_ms);
            }
            c.end().unwrap();
        }
    }
    assert!(pages > 1);
    assert!(pages >= 300usize.div_ceil(usize::from(page_limit)));
    assert_eq!(trace.requests.last().unwrap()[8], crate::SNAPSHOT_RELEASE);
    assert_eq!(store.scan_usage.count.load(Ordering::Acquire), 0);
    assert_eq!(store.scan_usage.bytes.load(Ordering::Acquire), 0);
}
#[test]
fn rejected_hello_capability_never_acquires_a_provider_view() {
    for (label, page_limit, fault) in [
        ("zero-limit", 0, Fault::None),
        ("oversize-limit", crate::MAX_BATCH as u16 + 1, Fault::None),
        ("legacy-hello", 1, Fault::LegacyHello),
        ("truncated-hello", 1, Fault::TruncatedHello),
    ] {
        let (peer, root) = peer_fixture(label, page_limit, fault);
        let (store, trace) = (peer.store.clone(), peer.trace.clone());
        match (fault, handshake(peer, true)) {
            (Fault::None, Err(Error::Protocol("HELLO limits")))
            | (Fault::TruncatedHello, Err(Error::Protocol("malformed reply"))) => {}
            (Fault::LegacyHello, Ok(mut client)) => {
                assert!(matches!(
                    client.capture_atomic_namespace_bounded(
                        "authored",
                        "read",
                        limits(),
                        deadline()
                    ),
                    Err(Error::Invalid("scans unavailable"))
                ));
                assert!(client.is_usable());
            }
            _ => panic!("unexpected SCANS negotiation result"),
        }
        save(&trace, &root, &store);
        assert_eq!(
            trace
                .lock()
                .unwrap()
                .requests
                .iter()
                .map(|r| r[8])
                .collect::<Vec<_>>(),
            [crate::HELLO]
        );
        assert_eq!(store.scan_usage.count.load(Ordering::Acquire), 0);
        assert_eq!(store.scan_usage.bytes.load(Ordering::Acquire), 0);
    }
}
#[test]
fn excessive_continuation_count_refuses_the_whole_view() {
    let (peer, root) = peer_fixture("excess-count", 1, Fault::ExcessPageCount);
    let (store, trace) = (peer.store.clone(), peer.trace.clone());
    let mut client = handshake(peer, true).unwrap();
    let result = client.capture_atomic_namespace_bounded("authored", "read", limits(), deadline());
    save(&trace, &root, &store);
    assert!(matches!(result, Err(Error::Protocol("scan progress"))));
    assert!(!client.is_usable());
    assert_eq!(
        trace
            .lock()
            .unwrap()
            .requests
            .iter()
            .map(|r| r[8])
            .collect::<Vec<_>>(),
        [
            crate::HELLO,
            crate::ATOMIC_POPULATION,
            crate::SCAN_PREFIX,
            crate::SCAN_PREFIX,
            crate::SNAPSHOT_RELEASE
        ]
    );
    assert_eq!(store.scan_usage.count.load(Ordering::Acquire), 0);
    assert_eq!(store.scan_usage.bytes.load(Ordering::Acquire), 0);
}
#[test]
fn primary_page_refusal_precedes_cleanup_refusal() {
    let (store, mut client, trace, root) = fixture("precedence", true, Fault::PageAndRelease);
    let result = client.capture_atomic_namespace_bounded("authored", "read", limits(), deadline());
    save(&trace, &root, &store);
    assert!(matches!(result, Err(Error::Refused(ref r)) if r.code == 21));
    assert_eq!(
        trace.lock().unwrap().requests.last().unwrap()[8],
        crate::SNAPSHOT_RELEASE
    );
    assert_eq!(store.scan_usage.count.load(Ordering::Acquire), 0);
}
#[test]
fn successful_pages_do_not_hide_cleanup_refusal() {
    let (store, mut client, trace, root) = fixture("release", true, Fault::Release);
    let result = client.capture_atomic_namespace_bounded("authored", "read", limits(), deadline());
    save(&trace, &root, &store);
    assert!(matches!(result, Err(Error::Refused(ref r)) if r.code == 20));
    assert_eq!(store.scan_usage.count.load(Ordering::Acquire), 0);
}
#[test]
fn wrong_page_token_poisons_client_without_returning_view() {
    let (store, mut client, trace, root) = fixture("token", true, Fault::WrongToken);
    let result = client.capture_atomic_namespace_bounded("authored", "read", limits(), deadline());
    save(&trace, &root, &store);
    assert!(matches!(result, Err(Error::Protocol("scan progress"))));
    assert!(!client.is_usable());
    assert_eq!(
        trace.lock().unwrap().requests.last().unwrap()[8],
        crate::SNAPSHOT_RELEASE
    );
    assert_eq!(store.scan_usage.count.load(Ordering::Acquire), 0);
}
#[test]
fn wrong_header_epoch_refuses_before_paging_and_expiry_bounds_custody() {
    let (store, mut client, trace, root) = fixture("epoch", true, Fault::WrongEpoch);
    let result = client.capture_atomic_namespace_bounded("authored", "read", limits(), deadline());
    save(&trace, &root, &store);
    assert!(matches!(result, Err(Error::Protocol(_))));
    assert!(!client.is_usable());
    assert_eq!(trace.lock().unwrap().requests.len(), 1);
    assert_eq!(store.scan_usage.count.load(Ordering::Acquire), 1);
    store
        .config
        .clock
        .as_ref()
        .unwrap()
        .advance(store.config.snapshot_ttl_ms);
    store.purge_scans(&mut store.scans.lock().unwrap());
    assert_eq!(store.scan_usage.count.load(Ordering::Acquire), 0);
    assert_eq!(store.scan_usage.bytes.load(Ordering::Acquire), 0);
}
#[test]
fn expired_page_preserves_refusal_and_original_horizon() {
    let (store, mut client, trace, root) = fixture("expiry", true, Fault::Expire);
    let result = client.capture_atomic_namespace_bounded("authored", "read", limits(), deadline());
    save(&trace, &root, &store);
    assert!(matches!(result, Err(Error::Refused(ref r)) if r.code == 20));
    assert_eq!(store.scan_usage.count.load(Ordering::Acquire), 0);
}
