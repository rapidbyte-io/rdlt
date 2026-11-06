use super::{LITERAL_BYTES, NAMED_COLUMNS, TRANSFORMS_PER_STREAM};

#[test]
fn the_transform_limits_are_the_documented_sizes() {
    assert_eq!(TRANSFORMS_PER_STREAM, 64);
    assert_eq!(NAMED_COLUMNS, 10_000);
    assert_eq!(LITERAL_BYTES, 4096);
}
