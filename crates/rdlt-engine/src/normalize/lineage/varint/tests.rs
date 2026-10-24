use proptest::prelude::*;

use super::{len, write};

/// The number `bytes` begin with and how many bytes it takes, read as SQLite 4 documents its
/// variable-length integers, written apart from the encoder.
fn read(bytes: &[u8]) -> Option<(u64, usize)> {
    let first = *bytes.first()?;
    let byte = |at: usize| bytes.get(at).copied().map(u64::from);
    match first {
        0..=240 => Some((u64::from(first), 1)),
        241..=248 => Some((240 + 256 * (u64::from(first) - 241) + byte(1)?, 2)),
        249 => Some((2288 + 256 * byte(1)? + byte(2)?, 3)),
        _ => {
            let width = usize::from(first - 247);
            let value = (1..=width).try_fold(0_u64, |value, at| Some(value << 8 | byte(at)?))?;
            Some((value, width + 1))
        }
    }
}

/// `value`'s bytes.
fn written(value: u64) -> Vec<u8> {
    let mut bytes = Vec::new();
    write(value, &mut bytes);
    bytes
}

/// Each length's least and greatest number, with their bytes.
const RECORDED: &[(u64, &[u8])] = &[
    (0, &[0x00]),
    (240, &[0xf0]),
    (241, &[0xf1, 0x01]),
    (2287, &[0xf8, 0xff]),
    (2288, &[0xf9, 0x00, 0x00]),
    (67_823, &[0xf9, 0xff, 0xff]),
    (67_824, &[0xfa, 0x01, 0x08, 0xf0]),
    (0xff_ffff, &[0xfa, 0xff, 0xff, 0xff]),
    (0x100_0000, &[0xfb, 0x01, 0x00, 0x00, 0x00]),
    (0xffff_ffff, &[0xfb, 0xff, 0xff, 0xff, 0xff]),
    (0x1_0000_0000, &[0xfc, 0x01, 0x00, 0x00, 0x00, 0x00]),
    (0xff_ffff_ffff, &[0xfc, 0xff, 0xff, 0xff, 0xff, 0xff]),
    (0x100_0000_0000, &[0xfd, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00]),
    (
        0xffff_ffff_ffff,
        &[0xfd, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
    ),
    (
        0x1_0000_0000_0000,
        &[0xfe, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
    ),
    (
        0xff_ffff_ffff_ffff,
        &[0xfe, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
    ),
    (
        0x100_0000_0000_0000,
        &[0xff, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
    ),
    (
        u64::MAX,
        &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff],
    ),
];

#[test]
fn each_length_s_edges_take_the_bytes_recorded_for_them() {
    for (value, bytes) in RECORDED {
        assert_eq!(written(*value), *bytes, "{value}");
        assert_eq!(len(*value), bytes.len(), "{value}");
        assert_eq!(read(bytes), Some((*value, bytes.len())), "{value}");
    }
}

/// A number near one of the edges where the encoding takes another byte, or any number.
fn number() -> impl Strategy<Value = u64> {
    let edges: Vec<u64> = RECORDED.iter().map(|(value, _)| *value).collect();
    prop_oneof![
        3 => (proptest::sample::select(edges), -2_i64..=2)
            .prop_map(|(edge, offset)| edge.saturating_add_signed(offset)),
        1 => any::<u64>(),
        1 => 0_u64..70_000,
    ]
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(rdlt_testkit::cases(1024)))]

    #[test]
    fn five_numbers_written_one_after_another_read_back_as_they_were(
        tuple in proptest::array::uniform5(number()),
    ) {
        let mut bytes = Vec::new();
        for value in tuple {
            write(value, &mut bytes);
        }
        let mut read_back = Vec::new();
        let mut at = 0;
        while at < bytes.len() && read_back.len() < tuple.len() {
            let (value, taken) = read(&bytes[at..]).expect("a number begins here");
            read_back.push(value);
            at += taken;
        }
        prop_assert_eq!(at, bytes.len());
        prop_assert_eq!(read_back, tuple.to_vec());
        let lengths: usize = tuple.iter().map(|value| len(*value)).sum();
        prop_assert_eq!(lengths, bytes.len());
    }

    #[test]
    fn five_numbers_written_one_after_another_compare_as_their_tuple(
        left in proptest::array::uniform5(number()),
        right in proptest::array::uniform5(number()),
        shared in 0_usize..5,
    ) {
        // Tuples sharing their first fields compare by a later one, as rows of a piece do.
        let mut right = right;
        right[..shared].copy_from_slice(&left[..shared]);
        let bytes = |tuple: &[u64; 5]| {
            let mut bytes = Vec::new();
            for value in tuple {
                write(*value, &mut bytes);
            }
            bytes
        };
        prop_assert_eq!(bytes(&left).cmp(&bytes(&right)), left.cmp(&right));
    }
}
