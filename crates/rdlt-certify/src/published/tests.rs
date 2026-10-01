use super::within;
use crate::limits::{PUBLISHED_BYTES, PUBLISHED_ROWS};

#[test]
fn a_table_is_read_back_up_to_its_limits_and_no_further() {
    for limit in [PUBLISHED_BYTES, PUBLISHED_ROWS, 1, 0] {
        assert_eq!(within(0, limit, limit), Some(limit));
        assert_eq!(within(limit, 0, limit), Some(limit));
        assert_eq!(within(limit, 1, limit), None);
        assert_eq!(within(0, limit + 1, limit), None);
    }
    assert_eq!(
        within(PUBLISHED_BYTES - 10, 10, PUBLISHED_BYTES),
        Some(PUBLISHED_BYTES)
    );
    assert_eq!(within(PUBLISHED_BYTES - 10, 11, PUBLISHED_BYTES), None);
    assert_eq!(within(usize::MAX, 1, usize::MAX), None);
}
