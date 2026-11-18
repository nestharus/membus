//! Literal opcode checks through the connection dispatcher used by the resident.
use membus::{frame, lp, Config, ConnectionDispatch, Store};
use std::path::PathBuf;

fn fixture(label: &str) -> Store {
    let dir = PathBuf::from(
        std::env::var_os("MEMBUS_TEST_OUTPUT").unwrap_or_else(|| ".test-output".into()),
    )
    .join(format!(
        "connection-contract-{label}-{}-{}",
        std::process::id(),
        membus::now_ms()
    ));
    Store::open(
        &dir,
        Config {
            signal_history: 0,
            signal_bytes: 0,
            ..Config::default()
        },
    )
    .unwrap()
}

fn call(
    store: &Store,
    connection: &mut ConnectionDispatch,
    op: u8,
    payload: &[u8],
) -> (u16, Vec<u8>) {
    let result = connection.dispatch(store, &frame(op, 73, payload)[4..]);
    assert_eq!(result[8], op | 0x80);
    assert_eq!(&result[9..17], &73u64.to_le_bytes());
    (
        u16::from_le_bytes(result[17..19].try_into().unwrap()),
        result[19..].to_vec(),
    )
}

fn greeting(identity: &str) -> Vec<u8> {
    let mut payload = 4096u32.to_le_bytes().to_vec();
    payload.push(0);
    lp(&mut payload, identity.as_bytes());
    payload
}

fn prefix() -> Vec<u8> {
    let mut payload = u64::MAX.to_le_bytes().to_vec();
    lp(&mut payload, b"original-op");
    lp(&mut payload, b"actual-scope");
    payload
}

#[test]
fn only_an_affirmative_hello_changes_opcode_selection() {
    let store = fixture("affirmative");
    let request = prefix();
    let mut raw = ConnectionDispatch::default();
    assert_eq!(call(&store, &mut raw, 28, &request), (3, vec![]));
    assert!(!raw.is_ready());

    let plain = greeting("ordinary");
    let reply = call(&store, &mut raw, 1, &plain);
    assert_eq!(reply.0, 0);
    assert_eq!(store.dispatch(&frame(1, 73, &plain)[4..])[19..], reply.1);
    assert!(call(&store, &mut raw, 28, &request).1.starts_with(b"MAW1"));

    let offer = greeting("receiver;timers=1;subscribe=1;subscription-contract=2;scans=1");
    let mut finite = ConnectionDispatch::default();
    assert!(call(&store, &mut finite, 1, &offer)
        .1
        .ends_with(b"MSH2\x01\x00\x01\x00"));
    assert!(!store
        .dispatch(&frame(1, 73, &offer)[4..])
        .ends_with(b"MSH2\x01\x00\x01\x00"));
    assert_eq!(call(&store, &mut finite, 28, &request), (3, vec![]));
    assert_eq!(call(&store, &mut finite, 31, &request), (2, vec![]));
    assert_eq!(call(&store, &mut finite, 1, &plain), (3, vec![]));
    assert_eq!(call(&store, &mut raw, 1, &offer), (3, vec![]));
    assert_eq!(call(&store, &mut finite, 28, &request), (3, vec![]));
    assert!(call(&store, &mut raw, 28, &request).1.starts_with(b"MAW1"));

    let mut create = request.clone();
    create.extend(1u16.to_le_bytes());
    create.extend(20_000u64.to_le_bytes());
    create.extend(100u32.to_le_bytes());
    lp(&mut create, &[7; 16]);
    lp(&mut create, &[9; 16]);
    lp(&mut create, b"quiet/");
    lp(&mut create, br"\\.\pipe\unused-connection-contract");
    assert_eq!(call(&store, &mut raw, 28, &create), (3, vec![]));
    let (code, receipt) = call(&store, &mut finite, 28, &create);
    assert_eq!(code, 0);
    assert_eq!(receipt.len(), 64);
    let mut control = request.clone();
    lp(&mut control, &receipt);
    assert!(call(&store, &mut raw, 29, &control).1.starts_with(b"MAW1"));
    assert!(call(&store, &mut raw, 30, &control).0 != 0);
    assert_eq!(call(&store, &mut finite, 30, &control), (0, vec![]));
    assert_eq!(call(&store, &mut finite, 29, &control), (24, vec![]));
}

#[test]
fn negative_discovery_and_incomplete_offers_leave_atomic_dispatch_selected() {
    let store = fixture("negative");
    for (identity, suffix) in [
        (
            "receiver;subscription-contract=1",
            &b"MSC1\x01\x00\x00\x01\x00"[..],
        ),
        (
            "receiver;subscribe=1;subscription-contract=1",
            &b"MSC1\x01\x00\x00\x01\x00"[..],
        ),
        ("receiver;subscription-contract=2", &b""[..]),
        ("receiver;subscribe=1;subscription-contract=2x", &b""[..]),
        ("receiver;subscribe=1;subscription-contract=3", &b""[..]),
    ] {
        let mut session = ConnectionDispatch::default();
        let (status, response) = call(&store, &mut session, 1, &greeting(identity));
        assert_eq!(status, 0, "{identity}");
        assert!(response.ends_with(suffix));
        assert!(!response.ends_with(b"MSH2\x01\x00\x01\x00"));
        assert!(call(&store, &mut session, 28, &prefix())
            .1
            .starts_with(b"MAW1"));
    }
    let mut session = ConnectionDispatch::default();
    let conflict = greeting("receiver;subscribe=1;subscription-contract=1;subscription-contract=2");
    assert_eq!(call(&store, &mut session, 1, &conflict), (2, vec![]));
    assert!(!session.is_ready());
    assert_eq!(call(&store, &mut session, 28, &prefix()), (3, vec![]));
}
