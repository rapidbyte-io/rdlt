use super::{LEAST, MOST, segment};
use crate::SQL_BYTES;

#[test]
fn the_stack_a_text_is_worked_on_grows_with_its_length_within_its_bounds() {
    assert_eq!(segment(0), LEAST);
    assert_eq!(segment(256), LEAST);
    assert_eq!(segment(257), LEAST + (4 << 10));
    assert_eq!(segment(1 << 10), 4 << 20);
    assert_eq!(segment(8 << 10), 32 << 20);
    assert_eq!(segment(SQL_BYTES), MOST);
    assert_eq!(segment(SQL_BYTES + 1), MOST);
    assert_eq!(segment(usize::MAX), MOST);
}

#[test]
fn a_level_short_of_stack_is_given_a_segment_beyond_the_red_zone() {
    let left = std::thread::Builder::new()
        .stack_size(64 << 10)
        .spawn(|| {
            assert!(stacker::remaining_stack().unwrap() < super::RED_ZONE);
            super::nest(|| stacker::remaining_stack().unwrap())
        })
        .unwrap()
        .join()
        .unwrap();
    assert!(left > 512 << 10, "{left}");
}

#[test]
fn planning_short_of_stack_is_given_a_segment_of_its_own() {
    let left = std::thread::Builder::new()
        .stack_size(64 << 10)
        .spawn(|| super::grown(|| stacker::remaining_stack().unwrap()))
        .unwrap()
        .join()
        .unwrap();
    assert!(left > 4 << 20, "{left}");
}
