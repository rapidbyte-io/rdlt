use super::MAX_ACKNOWLEDGEABLE;

#[test]
fn the_acknowledgeable_positions_have_their_specified_value() {
    assert_eq!(MAX_ACKNOWLEDGEABLE, 1 << 18);
}
