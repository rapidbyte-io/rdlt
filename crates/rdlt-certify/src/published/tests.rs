use super::{PUBLISHED_BYTES, within_cap};

#[test]
fn a_table_is_read_back_up_to_its_cap_and_no_further() {
    assert_eq!(within_cap(0, PUBLISHED_BYTES), Some(PUBLISHED_BYTES));
    assert_eq!(within_cap(PUBLISHED_BYTES - 10, 10), Some(PUBLISHED_BYTES));
    assert_eq!(within_cap(PUBLISHED_BYTES - 10, 11), None);
    assert_eq!(within_cap(usize::MAX, 1), None);
}
