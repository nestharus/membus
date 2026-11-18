use super::*;

fn image(
    ns: &[u8],
    key: &[u8],
    op_id: &[u8],
    ack: u8,
    remove: u8,
    expected: u64,
    value: &[u8],
) -> Vec<u8> {
    // Frozen durable layout, independent of mutation_bytes and either decoder.
    let mut bytes = 17u64.to_le_bytes().to_vec();
    bytes.extend(29u64.to_le_bytes());
    bytes.push(ack);
    bytes.push(remove);
    for text in [ns, key, op_id] {
        bytes.extend((text.len() as u32).to_le_bytes());
        bytes.extend(text);
    }
    bytes.extend(expected.to_le_bytes());
    bytes.extend((value.len() as u32).to_le_bytes());
    bytes.extend(value);
    bytes
}
#[test]
fn durable_record_frozen_layout_and_truncation_remain_strict() {
    for (ack, remove, expected) in [(1, 0, u64::MAX), (2, 1, 0)] {
        let bytes = image(
            "é".as_bytes(),
            "e\u{301}".as_bytes(),
            b"append-identity",
            ack,
            remove,
            expected,
            &[0, 127, 255],
        );
        let (m, version, at) = parse_record(&bytes).unwrap();
        assert_eq!((version, at, m.ack, m.remove), (29, 17, ack, remove != 0));
        assert_eq!(m.ns, "é");
        assert_eq!(m.key, "e\u{301}");
        assert_eq!(m.op_id, "append-identity");
        assert_eq!(
            m.expected,
            if expected == u64::MAX {
                None
            } else {
                Some(expected)
            }
        );
        assert_eq!(m.bytes, vec![0, 127, 255]);
        assert_eq!(mutation_bytes(&m, version, at), bytes);
        for end in 0..bytes.len() {
            assert!(
                parse_record(&bytes[..end]).is_err(),
                "accepted truncation at {end}"
            );
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert!(parse_record(&trailing).is_err());
    }
}
#[test]
fn noncanonical_scalar_remove_byte_refuses() {
    assert_eq!(
        parse_record(&image("é".as_bytes(), "e\u{301}".as_bytes(),
            b"append-identity", 2, 255, 37, &[0, 127, 255])).unwrap_err(),
        3,
    );
}
#[test]
fn durable_record_identifiers_ack_and_value_bounds_remain_strict() {
    for position in 0..3 {
        for bad in [&b""[..], &b"\xff"[..], &vec![b'x'; MAX_ID + 1][..]] {
            let mut names = [&b"namespace"[..], &b"key"[..], &b"operation"[..]];
            names[position] = bad;
            assert!(parse_record(&image(names[0], names[1], names[2], 1, 0, 0, b"value")).is_err());
        }
    }
    for ack in [0, 3, 255] {
        assert!(parse_record(&image(b"n", b"k", b"o", ack, 0, 0, b"v")).is_err());
    }
    let maximum = vec![b'x'; MAX_ID];
    let payload = vec![7; MAX_VALUE];
    let bytes = image(&maximum, &maximum, &maximum, 2, 0, u64::MAX, &payload);
    let (m, _, _) = parse_record(&bytes).unwrap();
    assert_eq!(m.bytes, payload);
    assert_eq!(m.op_id.len(), MAX_ID);
    assert!(parse_record(&image(b"n", b"k", b"o", 1, 0, 0, &vec![0; MAX_VALUE + 1])).is_err());
    assert!(parse_record(&image(b"n", b"k", b"o", 1, 0, 0, b""))
        .unwrap()
        .0
        .bytes
        .is_empty());
}
