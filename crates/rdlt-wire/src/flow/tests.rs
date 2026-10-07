use proptest::prelude::*;

use super::{Granting, Spending, Transport, ZeroGrant};
use crate::limits::{CREDIT_FLOOR, Class, Limits, MAX_CALLS, MIN_FRAME_BYTES};

/// Bytes: the most a frame of a data-plane call takes on the wire within `limits`.
fn limit(limits: &Limits) -> u64 {
    u64::try_from(limits.decoding(Class::Data)).unwrap()
}

#[test]
fn a_receiver_opens_with_its_floor() {
    assert_eq!(
        Granting::new(CREDIT_FLOOR, &Limits::default()).opening(),
        CREDIT_FLOOR
    );
}

#[test]
fn a_frame_within_half_the_floor_comes_back_alone() {
    let mut granting = Granting::new(CREDIT_FLOOR, &Limits::default());
    assert_eq!(granting.taken(1 << 20), 1 << 20);
    assert_eq!(granting.window(), CREDIT_FLOOR);
}

#[test]
fn the_window_grows_to_two_of_the_largest_frames_and_never_shrinks() {
    let mut granting = Granting::new(CREDIT_FLOOR, &Limits::default());
    let frame = 7_000_000;
    assert_eq!(granting.taken(frame), frame + (2 * frame - CREDIT_FLOOR));
    assert_eq!(granting.window(), 2 * frame);
    assert_eq!(granting.taken(1_000), 1_000);
    assert_eq!(granting.window(), 2 * frame);
}

#[test]
fn the_window_never_passes_the_receivers_frame_limit() {
    let limits = Limits {
        frame_bytes: MIN_FRAME_BYTES,
        ..Limits::default()
    };
    let mut granting = Granting::new(CREDIT_FLOOR, &limits);
    let largest = limit(&limits);
    assert_eq!(
        granting.taken(largest),
        largest + (limit(&limits) - CREDIT_FLOOR)
    );
    // A frame the receiver would have refused grows nothing beyond the limit either.
    granting.taken(u64::MAX / 4);
    assert_eq!(granting.window(), limit(&limits));
}

#[test]
fn a_floor_above_the_frame_limit_opens_at_the_limit() {
    let limits = Limits {
        frame_bytes: MIN_FRAME_BYTES,
        ..Limits::default()
    };
    let granting = Granting::new(u64::MAX, &limits);
    assert_eq!(granting.opening(), limit(&limits));
    assert_eq!(Granting::new(0, &limits).opening(), 1);
}

#[test]
fn a_floor_of_one_byte_keeps_a_read_two_frames_ahead() {
    let mut granting = Granting::new(1, &Limits::default());
    assert_eq!(granting.opening(), 1);
    assert_eq!(granting.taken(100), 100 + 199);
    assert_eq!(granting.window(), 200);
}

#[test]
fn a_sender_sends_while_its_credit_lasts_and_may_end_one_frame_below_it() {
    let mut spending = Spending::default();
    assert!(!spending.may_send());
    spending.grant(10).unwrap();
    assert!(spending.may_send());
    spending.spend(25);
    assert!(!spending.may_send());
    assert_eq!(spending.credit(), -15);
    spending.grant(15).unwrap();
    assert!(!spending.may_send());
    spending.grant(1).unwrap();
    assert!(spending.may_send());
}

#[test]
fn a_grant_of_no_bytes_is_refused() {
    let mut spending = Spending::default();
    assert_eq!(spending.grant(0), Err(ZeroGrant));
    assert_eq!(spending.credit(), 0);
}

#[test]
fn credit_saturates_rather_than_wraps() {
    let mut spending = Spending::default();
    spending.grant(u64::MAX).unwrap();
    spending.grant(u64::MAX).unwrap();
    assert_eq!(spending.credit(), i64::MAX);
    spending.spend(u64::MAX);
    spending.spend(u64::MAX);
    spending.spend(u64::MAX);
    assert_eq!(spending.credit(), i64::MIN);
}

#[test]
fn the_connection_window_holds_every_calls_stream_window_and_the_heartbeats() {
    let transport = Transport::of(&Limits::default());
    let calls = u64::from(MAX_CALLS) + 1;
    assert!(u64::from(transport.connection_window) >= calls * u64::from(transport.stream_window));
    assert!(transport.connection_window < 1 << 31);
}

#[test]
fn a_stream_window_holds_four_transport_frames_and_the_floor() {
    let transport = Transport::of(&Limits::default());
    assert!(transport.stream_window >= 4 * transport.max_frame);
    assert!(u64::from(transport.stream_window) >= CREDIT_FLOOR);
    assert!((16_384..=16_777_215).contains(&transport.max_frame));
}

proptest! {
    #[test]
    fn once_every_frame_is_taken_the_credit_is_the_window(
        frames in proptest::collection::vec(1..=68_000_000_u64, 1..64),
        floor in 1..=CREDIT_FLOOR,
    ) {
        let limits = Limits::default();
        let mut granting = Granting::new(floor, &limits);
        let mut spending = Spending::default();
        spending.grant(granting.opening()).unwrap();
        let mut window = granting.window();
        for frame in frames.into_iter().map(|frame| frame.min(limit(&limits))) {
            spending.spend(frame);
            prop_assert!(spending.credit() > -i64::try_from(frame).unwrap() - 1);
            spending.grant(granting.taken(frame)).unwrap();
            prop_assert_eq!(spending.credit(), i64::try_from(granting.window()).unwrap());
            prop_assert!(granting.window() >= window);
            prop_assert!(granting.window() <= limit(&limits));
            window = granting.window();
        }
    }
}
