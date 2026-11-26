use super::crc32;

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
    }
}

