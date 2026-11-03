use std::sync::Arc;

use bytes::Bytes;
use parking_lot::Mutex;
use proptest::prelude::*;

use super::{ALIGNMENT, Aligned, Spare, is_aligned};

/// Where an allocation frozen with a home goes back to.
type Home = Arc<Mutex<Option<Spare>>>;

/// `data` frozen in an allocation of exactly its room, handed to the home returned once nothing
/// holds it.
fn returning(data: &[u8]) -> (Bytes, Bytes, Home) {
    let home = Home::default();
    let mut aligned = Aligned::with_room(data.len());
    aligned.extend(data);
    let returned = Arc::clone(&home);
    let frozen = aligned.freeze_returning(move |spare| *returned.lock() = Some(spare));
    (frozen.bytes, frozen.allocation, home)
}

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

    /// A message held in a spare of any room is held at an aligned first byte, in an allocation
    /// no longer than a fresh one of the message's room.
    #[test]
    fn a_message_in_a_spare_is_held_as_in_a_fresh_allocation(
        spared in 0_usize..8_192,
        data in proptest::collection::vec(any::<u8>(), 0..4_096),
    ) {
        let (bytes, allocation, home) = returning(&vec![0xff; spared]);
        drop((bytes, allocation));
        let spare = home.lock().take().expect("handed back");
        let mut held = Aligned::in_spare(spare, data.len());
        held.extend(&data);
        let frozen = held.freeze();
        prop_assert_eq!(&frozen.bytes[..], &data[..]);
        prop_assert!(frozen.bytes.is_empty() || is_aligned(&frozen.bytes));
        prop_assert_eq!(
            frozen.allocation.len(),
            Aligned::with_room(data.len()).freeze().allocation.len()
        );
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
fn bytes_at_any_address_are_copied_aligned() {
    let frozen = Aligned::copied(&[7; 200]);
    assert!(is_aligned(&frozen.bytes));
    for skew in 1..ALIGNMENT {
        let off = frozen.bytes.slice(skew..skew + 100);
        assert!(!is_aligned(&off), "{skew}");
        let copied = Aligned::copied(&off).bytes;
        assert!(is_aligned(&copied), "{skew}");
        assert_eq!(copied, off, "{skew}");
    }
}

#[test]
fn what_is_read_is_held_aligned_and_no_more_than_the_reader_gives() {
    let data: Vec<u8> = (0..=255).collect();
    let mut aligned = Aligned::default();
    aligned.read_from(&data[..100], 60).expect("reads");
    assert_eq!(aligned.as_slice(), &data[..60]);
    // A reader that ends first gives what it has.
    aligned.read_from(&data[100..120], 500).expect("reads");
    assert_eq!(aligned.as_slice(), [&data[..60], &data[100..120]].concat());
    let frozen = aligned.freeze();
    assert!(is_aligned(&frozen.bytes));
    assert!(frozen.allocation.len() < 60 + 500 + ALIGNMENT);
}

#[test]
fn an_allocation_frozen_with_a_home_goes_back_to_it_once_nothing_holds_any_of_it() {
    let (bytes, allocation, home) = returning(&[7; 300]);
    assert_eq!(&bytes[..], &[7; 300]);
    let (start, len) = (allocation.as_ptr().addr(), allocation.len());
    let slice = bytes.slice(10..20);
    drop((bytes, allocation));
    assert!(home.lock().is_none(), "a slice of it still holds it");
    drop(slice);
    let spare = home
        .lock()
        .take()
        .expect("handed back once its last slice went");
    assert_eq!(spare.bytes(), len, "the whole allocation");
    // Held again for as much room as it was frozen with, it is the same allocation.
    let mut again = Aligned::in_spare(spare, 300);
    again.extend(&[9; 300]);
    let frozen = again.freeze();
    assert_eq!(frozen.allocation.as_ptr().addr(), start);
    assert_eq!(frozen.allocation.len(), len);
    assert_eq!(&frozen.bytes[..], &[9; 300]);
}

#[test]
fn a_spare_holds_less_room_shrunk_to_what_a_fresh_allocation_takes_and_zeroed_beside_it() {
    let (bytes, allocation, home) = returning(&[0xff; 1000]);
    drop((bytes, allocation));
    let spare = home.lock().take().expect("handed back");
    let mut held = Aligned::in_spare(spare, 600);
    assert_eq!((held.room(), held.len()), (600, 0));
    held.extend(&[1; 600]);
    let frozen = held.freeze();
    assert!(is_aligned(&frozen.bytes));
    assert_eq!(&frozen.bytes[..], &[1; 600]);
    let fresh = Aligned::with_room(600).freeze().allocation.len();
    assert_eq!(
        frozen.allocation.len(),
        fresh,
        "no longer than a fresh allocation"
    );
    let start = frozen.bytes.as_ptr().addr() - frozen.allocation.as_ptr().addr();
    let beside = [
        &frozen.allocation[..start],
        &frozen.allocation[start + 600..],
    ]
    .concat();
    assert!(
        beside.iter().all(|byte| *byte == 0),
        "nothing of the message before is left beside what it holds"
    );
}

#[test]
fn a_spare_fits_a_message_of_half_its_room_to_all_of_it() {
    let (bytes, allocation, home) = returning(&[0; 1000]);
    drop((bytes, allocation));
    let spare = home.lock().take().expect("handed back");
    assert!(spare.fits(1000) && spare.fits(500));
    assert!(!spare.fits(1001), "more than its room");
    assert!(!spare.fits(499), "less than half its room");
}

#[test]
fn a_spare_held_for_no_room_takes_no_allocation_as_a_fresh_one_takes_none() {
    let (bytes, allocation, home) = returning(&[1]);
    drop((bytes, allocation));
    let spare = home.lock().take().expect("handed back");
    let frozen = Aligned::in_spare(spare, 0).freeze();
    assert_eq!(
        frozen.allocation.len(),
        Aligned::with_room(0).freeze().allocation.len()
    );
    assert!(frozen.bytes.is_empty());
}
