//! Proof that a frame's head is parsed from any sixteen bytes without panicking, accepted only
//! within its kind's bound, and parsed as the bytes a sender encodes for it.

use super::{Bound, Bounds, HEAD_BYTES, Head, HeadError};
use crate::limits::Limit;

/// Any bound a kind may have.
fn any_bound(limit: Limit) -> Bound {
    Bound {
        bytes: kani::any(),
        limit,
    }
}

/// Any bounds an end of a call may send within, each kind sent or not.
fn any_bounds() -> Bounds {
    Bounds {
        batch: kani::any::<bool>().then(|| any_bound(Limit::FrameBytes)),
        json: kani::any::<bool>().then(|| any_bound(Limit::JsonPushBytes)),
        message: any_bound(Limit::ControlMessageBytes),
    }
}

/// The bound of `head`'s kind in `bounds`, where the end sends that kind.
fn bound(head: &Head, bounds: &Bounds) -> Option<Bound> {
    match head.kind {
        super::Kind::Batch { .. } => bounds.batch,
        super::Kind::Json => bounds.json,
        super::Kind::Message { .. } => Some(bounds.message),
    }
}

/// Any sixteen bytes parse, without panicking, as a head of a kind the end sends within that
/// kind's bound, whose encoding is those bytes, or are refused; a body beyond its bound is
/// refused by the bound's limit.
///
/// Bound: every sixteen bytes and every bounds.
#[kani::proof]
fn any_head_is_parsed_within_its_bound_as_it_is_encoded() {
    let bytes: [u8; HEAD_BYTES] = kani::any();
    let bounds = any_bounds();
    match Head::parse(bytes, &bounds) {
        Ok(head) => {
            let within = bound(&head, &bounds).expect("an accepted kind is one the end sends");
            assert!(head.len <= within.bytes);
            assert_eq!(head.encode(), bytes);
        }
        Err(HeadError::Refused(refusal)) => {
            assert!(refusal.actual > refusal.limit);
            assert_eq!(
                refusal.actual,
                u64::from(u32::from_le_bytes(
                    [bytes[0], bytes[1], bytes[2], bytes[3],]
                ))
            );
        }
        Err(HeadError::Malformed(_)) => {}
    }
}
