use super::{
    MAX_ACKNOWLEDGEABLE, MAX_BATCH_BYTES, MAX_BATCH_ROWS, MAX_BATCH_VALUES, MAX_COLUMNS,
    MAX_CONFIG_BYTES, MAX_CURSOR_BYTES, MAX_JSON_PUSH_BYTES, MAX_NESTING_DEPTH, MAX_VIEW_BYTES,
};

#[test]
fn limits_have_their_specified_values() {
    let mib = 1 << 20;
    assert_eq!(MAX_JSON_PUSH_BYTES, 64 * mib);
    assert_eq!(MAX_BATCH_ROWS, 1 << 20);
    assert_eq!(MAX_BATCH_VALUES, 64 << 20);
    assert_eq!(MAX_VIEW_BYTES, 64 * mib);
    assert_eq!(MAX_BATCH_BYTES, 64 * mib);
    assert_eq!(MAX_COLUMNS, 10_000);
    assert_eq!(MAX_NESTING_DEPTH, 64);
    assert_eq!(MAX_CURSOR_BYTES, 4 * mib);
    assert_eq!(MAX_CONFIG_BYTES, 8 * mib);
    assert_eq!(MAX_ACKNOWLEDGEABLE, 1 << 18);
}
