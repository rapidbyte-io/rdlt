use super::Due;

#[test]
fn rows_count_from_writing_to_their_commit_but_not_once_a_commit_passed_them_by() {
    let mut due = Due::default();
    due.written(0, 4, 40);
    due.written(1, 6, 60);
    assert_eq!((due.rows(), due.bytes()), (10, 100));
    due.sealed(1);
    assert_eq!(
        (due.rows(), due.bytes()),
        (10, 100),
        "sealed, they still count"
    );
    due.committing();
    assert_eq!(
        (due.rows(), due.bytes()),
        (0, 0),
        "the seal went, p0's rows were passed by"
    );
    due.written(0, 5, 50);
    due.written(1, 3, 30);
    assert_eq!(
        (due.rows(), due.bytes()),
        (3, 30),
        "p0 stays passed by until it seals"
    );
    due.sealed(0);
    assert_eq!((due.rows(), due.bytes()), (12, 120));
    due.abandoned(1);
    assert_eq!((due.rows(), due.bytes()), (9, 90));
    due.written(1, 2, 20);
    assert_eq!((due.rows(), due.bytes()), (11, 110));
    // A seal of nothing written since the last makes nothing more due.
    due.sealed(2);
    assert_eq!((due.rows(), due.bytes()), (11, 110));
}
