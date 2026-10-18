//! Proofs that a call's credit holds, for every input within each proof's bound.

use super::{Granting, Spending, ZeroGrant};
use crate::limits::{FRAME_BYTES, Limits};

/// Steps of a call the sequence proof takes, each a frame sent or a frame taken.
const STEPS: usize = 6;

/// Whether `granting` keeps a floor of a byte at least and its window between floor and limit.
fn bounded(granting: &Granting) -> bool {
    1 <= granting.floor
        && granting.floor <= granting.window
        && granting.window <= granting.limit.max(granting.floor)
}

/// A receiver opens with its floor as its window, within its bounds, whatever its floor and limits.
///
/// Bound: none; every floor and every frame limit.
#[kani::proof]
fn a_receiver_opens_with_its_floor_within_its_bounds() {
    let limits = Limits {
        frame_bytes: kani::any(),
        ..Limits::default()
    };
    let granting = Granting::new(kani::any(), &limits);
    assert!(bounded(&granting));
    assert_eq!(granting.opening(), granting.window());
}

/// Taking a frame of any size keeps a receiver within its bounds without overflow, never shrinks
/// its window, and grants the frame's bytes with whatever the window grew by.
///
/// Bound: none; one frame from any state within the bounds, which covers every sequence.
#[kani::proof]
fn taking_a_frame_keeps_a_receiver_within_its_bounds() {
    let mut granting = Granting {
        floor: kani::any(),
        limit: kani::any(),
        window: kani::any(),
    };
    kani::assume(bounded(&granting));
    let (window, bytes) = (granting.window, kani::any::<u64>());
    let grant = granting.taken(bytes);
    assert!(bounded(&granting));
    assert!(granting.window >= window);
    assert_eq!(grant, bytes.saturating_add(granting.window - window));
}

/// A grant of no bytes is refused and leaves the credit as it was, and any other raises it
/// without wrapping.
///
/// Bound: none; every credit and every grant.
#[kani::proof]
fn a_grant_of_no_bytes_is_refused() {
    let (credit, bytes) = (kani::any::<i64>(), kani::any::<u64>());
    let mut spending = Spending { credit };
    let granted = spending.grant(bytes);
    if bytes == 0 {
        assert_eq!(granted, Err(ZeroGrant));
        assert_eq!(spending.credit(), credit);
    } else {
        assert_eq!(granted, Ok(()));
        assert!(spending.credit() > credit || credit == i64::MAX);
    }
}

/// A sender that sends while in credit and a receiver that grants each frame it takes keep the
/// sender's credit at what was granted less what was spent, above minus its largest frame.
///
/// Once every frame sent is taken, the credit is the window.
///
/// Bound: every call of up to `STEPS` steps, each frame from a byte to the receiver's limit, with
/// a frame limit up to `FRAME_BYTES`.
#[kani::proof]
#[kani::unwind(7)]
fn credit_is_what_was_granted_less_what_was_spent() {
    let limits = Limits {
        frame_bytes: kani::any_where(|bytes: &u64| *bytes <= FRAME_BYTES),
        ..Limits::default()
    };
    let mut granting = Granting::new(kani::any(), &limits);
    let mut spending = Spending::default();
    assert_eq!(spending.grant(granting.opening()), Ok(()));
    let (mut granted, mut spent) = (i128::from(granting.opening()), 0_i128);
    let mut largest = 1_i64;
    let mut in_flight = [0_u64; STEPS];
    let (mut sent, mut taken) = (0, 0);
    for _ in 0..STEPS {
        if spending.may_send() && kani::any::<bool>() {
            let bytes = kani::any_where(|bytes: &u64| (1..=granting.limit).contains(bytes));
            spending.spend(bytes);
            in_flight[sent] = bytes;
            sent += 1;
            spent += i128::from(bytes);
            largest = largest.max(i64::try_from(bytes).unwrap_or(i64::MAX));
        } else if taken < sent {
            let grant = granting.taken(in_flight[taken]);
            taken += 1;
            assert_eq!(spending.grant(grant), Ok(()));
            granted += i128::from(grant);
        }
        assert_eq!(i128::from(spending.credit()), granted - spent);
        assert!(spending.credit() > -largest);
        if taken == sent {
            assert_eq!(i128::from(spending.credit()), i128::from(granting.window()));
        }
    }
}
