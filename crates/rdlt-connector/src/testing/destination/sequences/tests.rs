use super::{ordinal, sequence};

#[test]
fn sequences_order_as_their_ordinals_bytewise_and_otherwise_misorder_some_pair() {
    for low in 0..u8::MAX {
        for high in low + 1..=u8::MAX {
            let (low_bytes, high_bytes) = (sequence(low), sequence(high));
            assert!(low_bytes < high_bytes, "{low} and {high}");
            // The row's half alone orders every pair the other way.
            assert!(low_bytes[8..] > high_bytes[8..], "{low} and {high}");
        }
    }
    let signed = |n| i128::from_be_bytes(sequence(n));
    assert!(
        signed(7) > signed(8),
        "a signed comparison flips the pair across the top bit"
    );
    assert!(signed(0) < signed(7) && signed(8) < signed(15));
}

#[test]
fn a_sequence_names_its_ordinal_and_other_bytes_none() {
    for n in 0..=u8::MAX {
        assert_eq!(ordinal(&sequence(n)), Some(n));
    }
    let mut low_half = sequence(9);
    low_half[..8].fill(0);
    for bytes in [&low_half[..], &sequence(9)[..15], &[0; 16], &[]] {
        assert_eq!(ordinal(bytes), None, "{bytes:?}");
    }
}
