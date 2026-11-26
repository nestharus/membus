use super::{crc32, crc32_update, encode_mbk1_mutation, fingerprint_mutation, mutation_bytes, mutation_encoded_len};
use crate::{chunks, store::hash::Sha256, Mutation};

// Frozen pre-table definition, independent of the production implementation.
fn bitwise(bytes: &[u8]) -> u32 {
    let mut remainder = u32::MAX;
    for byte in bytes {
        remainder ^= u32::from(*byte);
        for _ in 0..8 {
            remainder = if remainder & 1 == 0 {
                remainder >> 1
            } else {
                (remainder >> 1) ^ 0xedb8_8320
            };
        }
    }
    !remainder
}

#[test]
fn crc32_preserves_canonical_vectors() {
    assert_eq!(crc32(b""), 0);
    assert_eq!(crc32(b"123456789"), 0xcbf4_3926);
    assert_eq!(crc32(&(0..=255).collect::<Vec<u8>>()), 0x2905_8c73);
}

#[test]
fn crc32_matches_frozen_binary_boundaries() {
    for byte in 0..=255 {
        assert_eq!(crc32(&[byte]), bitwise(&[byte]));
    }
    for seed in [0x1234_5678u32, 0x9abc_def0, 0xffff_ffff] {
        let mut state = seed;
        let input: Vec<_> = (0..65_536)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 17;
                state ^= state << 5;
                state as u8
            })
            .collect();
        for len in [
            0, 1, 2, 3, 7, 8, 15, 16, 31, 32, 63, 64, 127, 128, 255, 256, 257, 1023, 1024, 4095,
            4096, 65_535, 65_536,
        ] {
            assert_eq!(
                crc32(&input[..len]),
                bitwise(&input[..len]),
                "seed={seed} len={len}"
            );
        }
        for offset in 1..8 {
            assert_eq!(
                crc32(&input[offset..offset + 4097]),
                bitwise(&input[offset..offset + 4097]),
                "seed={seed} offset={offset}"
            );
        }
    }
    for ns in ["", "a", "ledger", "namespace-with-length-over-eight"] {
        for key in ["", "k", "key-0001", "long-key-crossing-eight-byte-boundary-12345"] {
            let mut combined = ns.as_bytes().to_vec();
            combined.push(0);
            combined.extend_from_slice(key.as_bytes());
            let c = crc32_update(!0u32, ns.as_bytes());
            let c = crc32_update(c, &[0]);
            let c = crc32_update(c, key.as_bytes());
            assert_eq!(!c, bitwise(&combined));
        }
    }
}

#[test]
fn zero_alloc_mutation_fingerprint_and_frame_match_reference() {
    let cases = [
        Mutation {
            ns: "n".into(),
            key: "k".into(),
            op_id: "op".into(),
            deadline: 100,
            ack: 1,
            expected: None,
            bytes: Vec::new(),
            remove: false,
        },
        Mutation {
            ns: "界/ns".into(),
            key: "é/key".into(),
            op_id: "op-cond-remove".into(),
            deadline: 200,
            ack: 2,
            expected: Some(0),
            bytes: Vec::new(),
            remove: true,
        },
        Mutation {
            ns: "ledger".into(),
            key: "row/large".into(),
            op_id: "op-large-payload".into(),
            deadline: 300,
            ack: 2,
            expected: Some(42),
            bytes: (0..65_539).map(|i| (i % 251) as u8).collect(),
            remove: false,
        },
    ];
    for (i, m) in cases.iter().enumerate() {
        let version = (i as u64) + 7;
        let at = 1_700_000_000_000 + i as u64;
        let ref_payload = mutation_bytes(m, version, at);
        assert_eq!(mutation_encoded_len(m), ref_payload.len());

        let ref_frame = chunks::frame(b"MBK1", &ref_payload);
        let fast_frame = encode_mbk1_mutation(m, version, at);
        assert_eq!(fast_frame, ref_frame);

        let mut ref_hash = Sha256::new();
        ref_hash.update(&mutation_bytes(m, 0, 0));
        let expected_fp = ref_hash.finish();
        assert_eq!(fingerprint_mutation(m), expected_fp);

        let mut slice_hash = Sha256::new();
        slice_hash.update(&[0u8; 16]);
        slice_hash.update(&fast_frame[32..]);
        assert_eq!(slice_hash.finish(), expected_fp);
    }
}

