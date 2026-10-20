//! Proofs that the lengths a log keeps room for are what its frames take, without overflow,
//! for every input within each proof's bound.

use super::super::HEAD;
use super::{framed_len, listed};

/// The most numbers a list the proof counts holds.
const NUMBERS: usize = 1;

/// A frame is its payload and its head, saturating at the largest length rather than wrapping.
///
/// Bound: none; every payload length.
#[kani::proof]
fn a_frame_is_its_payload_and_its_head_saturating_at_the_largest() {
    let payload: u64 = kani::any();
    let head = u64::try_from(HEAD).expect("a head of a few bytes");
    let framed = framed_len(payload);
    assert!(framed >= payload);
    assert_eq!(framed, payload.checked_add(head).unwrap_or(u64::MAX));
}

/// A list of numbers takes exactly the bytes JSON writes of it: its brackets and each number's
/// digits, without overflow.
///
/// Bound: every list of up to `NUMBERS` numbers, each any `u64`; the commas between
/// numbers are not covered.
#[kani::proof]
#[kani::unwind(24)]
fn a_list_of_numbers_takes_what_json_writes_of_it() {
    let numbers: [u64; NUMBERS] = kani::any();
    let numbers = &numbers[..kani::any_where(|count: &usize| *count <= NUMBERS)];
    let written = serde_json::to_vec(numbers).expect("numbers encode");
    assert_eq!(
        listed(numbers.iter().copied()),
        u64::try_from(written.len()).expect("a short list")
    );
}
