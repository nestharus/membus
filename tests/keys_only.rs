use membus::*;
use std::path::PathBuf;
fn req(s: &Store, op: u8, p: Vec<u8>) -> Result<Vec<u8>, u16> {
    let mut b = (now_ms() + 10000).to_le_bytes().to_vec();
    lp(&mut b, b"keys-read");
    lp(&mut b, b"ledger");
    b.extend(p);
    let r = s.dispatch(&frame(op, 1, &b)[4..]);
    let code = u16::from_le_bytes(r[17..19].try_into().unwrap());
    if code == 0 {
        Ok(r[19..].to_vec())
    } else {
        Err(code)
    }
}
fn suffix(
    token: &[u8],
    cursor: &[u8],
    lo: &str,
    hi: Option<&str>,
    limit: u16,
    budget: u32,
) -> Vec<u8> {
    let mut p = Vec::new();
    lp(&mut p, token);
    lp(&mut p, cursor);
    p.extend(limit.to_le_bytes());
    p.extend(budget.to_le_bytes());
    lp(&mut p, lo.as_bytes());
    if let Some(hi) = hi {
        lp(&mut p, hi.as_bytes());
    }
    p
}
#[test]
fn keys_only_watermark_pages_do_not_transfer_values_and_share_snapshot() {
    let path = PathBuf::from(
        std::env::var("MEMBUS_TEST_OUTPUT").unwrap_or_else(|_| ".test-output".into()),
    )
    .join(format!("keys-only-{}-{}", std::process::id(), now_ms()));
    let s = Store::open(&path, Config::default()).unwrap();
    for seq in [1, 2, 4] {
        assert_eq!(
            s.mutate(Mutation {
                ns: "ledger".into(),
                key: format!("row/{seq:016}"),
                op_id: format!("row-{seq}"),
                deadline: now_ms() + 10000,
                ack: 1,
                expected: Some(0),
                bytes: vec![42; 59499],
                remove: false
            })
            .code,
            0
        );
    }
    let r = req(
        &s,
        SCAN_KEYS_RANGE,
        suffix(&[], &[], "row/", Some("row0"), 1, 128),
    )
    .unwrap();
    assert!(r.len() + 15 <= 128);
    let mut d = Cursor::new(&r);
    let token = d.bytes(16).unwrap().to_vec();
    let next = d.bytes(4096).unwrap().to_vec();
    assert_eq!(d.u8().unwrap(), 0);
    assert_eq!(d.u16().unwrap(), 1);
    assert_eq!(d.text().unwrap(), "row/0000000000000001");
    d.end().unwrap();
    let r = req(
        &s,
        SCAN_KEYS_RANGE,
        suffix(&token, &next, "row/", Some("row0"), 128, 1024),
    )
    .unwrap();
    let mut d = Cursor::new(&r);
    assert_eq!(d.bytes(16).unwrap(), token);
    assert!(d.bytes(4096).unwrap().is_empty());
    assert_eq!(d.u8().unwrap(), 1);
    assert_eq!(d.u16().unwrap(), 2);
    assert_eq!(d.text().unwrap(), "row/0000000000000002");
    assert_eq!(d.text().unwrap(), "row/0000000000000004");
    d.end().unwrap();
    // Missing seq3 is discoverable without receiving 178497 bytes of values.
    assert_eq!(
        req(
            &s,
            SCAN_RANGE,
            suffix(&token, &next, "row/", Some("row0"), 128, 1024)
        ),
        Err(21)
    );
    assert_eq!(
        req(
            &s,
            SCAN_PREFIX,
            suffix(&token, &[], "row/", None, 128, 1024)
        ),
        Err(4)
    );
    let full = req(
        &s,
        SCAN_RANGE,
        suffix(
            &token,
            &[],
            "row/0000000000000001",
            Some("row/0000000000000002"),
            128,
            MAX_FRAME as u32,
        ),
    )
    .unwrap();
    let mut d = Cursor::new(&full);
    assert_eq!(d.bytes(16).unwrap(), token);
    assert!(d.bytes(4096).unwrap().is_empty());
    assert_eq!(d.u8().unwrap(), 1);
    assert_eq!(d.u16().unwrap(), 1);
    assert_eq!(d.text().unwrap(), "row/0000000000000001");
    assert_eq!(d.u8().unwrap(), 1);
    d.u64().unwrap();
    d.u8().unwrap();
    assert_eq!(d.bytes(MAX_VALUE).unwrap().len(), 59499);
    d.end().unwrap();
    let empty = req(
        &s,
        SCAN_KEYS_PREFIX,
        suffix(&token, &[], "src/", None, 128, 1024),
    )
    .unwrap();
    let mut d = Cursor::new(&empty);
    assert_eq!(d.bytes(16).unwrap(), token);
    assert!(d.bytes(4096).unwrap().is_empty());
    assert_eq!(d.u8().unwrap(), 1);
    assert_eq!(d.u16().unwrap(), 0);
    d.end().unwrap();
}
