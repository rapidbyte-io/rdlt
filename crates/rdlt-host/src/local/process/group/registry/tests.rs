use super::{OWNED, REMAINING, remember, room};

#[test]
fn a_process_owns_up_to_its_groups_and_no_more() {
    assert!(room(0).is_ok());
    assert!(room(OWNED - 1).is_ok());
    assert!(room(OWNED).is_err());
    assert!(room(usize::MAX).is_err());
}

#[test]
fn the_latest_groups_that_kept_a_member_are_remembered_and_no_more() {
    let mut remaining = Vec::new();
    let most = u32::try_from(REMAINING).expect("it fits");
    for id in 0..most {
        remember(&mut remaining, id);
    }
    assert_eq!(remaining.len(), REMAINING);
    remember(&mut remaining, most);
    assert_eq!(remaining.len(), REMAINING);
    assert_eq!((remaining[0], remaining[REMAINING - 1]), (1, most));
}
