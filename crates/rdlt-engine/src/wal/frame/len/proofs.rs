//! Proofs that the lengths a log keeps room for are what its frames take, without overflow,
//! for every input within each proof's bound.

use super::super::HEAD;
use super::framed_len;

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
