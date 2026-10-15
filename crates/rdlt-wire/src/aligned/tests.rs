use bytes::Bytes;
use proptest::prelude::*;

use super::{ALIGNMENT, Aligned, ensure, is_aligned};

proptest! {
    /// Whatever it grows to and holds, its bytes start at a multiple of the alignment, are those
    /// appended, and its allocation, frozen, holds them and no more than their room and the
    /// alignment beside them.
    #[test]
    fn what_it_holds_starts_aligned_and_its_allocation_is_its_room(
        steps in proptest::collection::vec(
            (0_usize..4_096, proptest::collection::vec(any::<u8>(), 0..512)),
            1..16,
        ),
    ) {
        let mut aligned = Aligned::default();
        let mut expected = Vec::new();
        for (room, data) in steps {
            aligned.grow(room);
            prop_assert!(aligned.room() >= room.max(aligned.len()));
            aligned.extend(&data);
            expected.extend_from_slice(&data);
            prop_assert!(is_aligned(aligned.as_slice()) || aligned.is_empty());
            prop_assert_eq!(aligned.as_slice(), &expected[..]);
        }
        let room = aligned.room();
        let frozen = aligned.freeze();
        prop_assert_eq!(&frozen.bytes[..], &expected[..]);
        prop_assert!(frozen.bytes.is_empty() || is_aligned(&frozen.bytes));
        let start = frozen.bytes.as_ptr().addr() - frozen.allocation.as_ptr().addr();
        prop_assert!(start + frozen.bytes.len() <= frozen.allocation.len());
        prop_assert!(frozen.allocation.len() < room + ALIGNMENT);
    }
}

#[test]
fn zeroed_bytes_are_read_into_and_truncated_to_what_was_read() {
    let mut aligned = Aligned::zeroed(100);
    assert_eq!(aligned.len(), 100);
    assert!(aligned.as_slice().iter().all(|byte| *byte == 0));
    aligned.as_mut_slice()[..3].copy_from_slice(b"abc");
    aligned.truncate(3);
    assert_eq!(aligned.freeze().bytes, Bytes::from_static(b"abc"));
}

#[test]
fn room_asked_for_is_kept_until_more_is_asked() {
    let mut aligned = Aligned::with_room(10);
    assert_eq!((aligned.room(), aligned.len()), (10, 0));
    aligned.grow(4);
    assert_eq!(aligned.room(), 10, "less room than it has takes none away");
    aligned.extend(&[1; 12]);
    assert_eq!(
        aligned.room(),
        12,
        "what it must hold grows it to exactly that"
    );
}

#[test]
fn bytes_at_any_address_are_ensured_aligned_and_copied_only_where_they_are_not() {
    let frozen = Aligned::copied(&[7; 200]);
    let aligned = frozen.bytes.clone();
    assert_eq!(ensure(aligned.clone()).as_ptr(), aligned.as_ptr());
    for skew in 1..ALIGNMENT {
        let off = frozen.bytes.slice(skew..skew + 100);
        assert!(!is_aligned(&off), "{skew}");
        let ensured = ensure(off.clone());
        assert!(is_aligned(&ensured), "{skew}");
        assert_ne!(ensured.as_ptr(), off.as_ptr(), "copied: {skew}");
        assert_eq!(ensured, off, "{skew}");
    }
}
