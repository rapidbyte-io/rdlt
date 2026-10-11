use super::Due;

#[test]
fn rows_of_a_partition_that_seals_on_its_own_count_once_sealed() {
    let mut due = Due::default();
    // p0 seals on its own: the coordinator tells only its seals.
    due.sealed(0, 4, 40);
    assert_eq!((due.rows(), due.bytes()), (4, 40));
    due.committing();
    assert_eq!(
        (due.rows(), due.bytes()),
        (0, 0),
        "the commit took the seal"
    );
    due.sealed(0, 6, 60);
    due.sealed(0, 3, 30);
    assert_eq!((due.rows(), due.bytes()), (9, 90));
}

#[test]
fn rows_a_barrier_seals_count_from_writing_until_sealed_once() {
    let mut due = Due::default();
    due.written(0, 4, 40);
    due.committing();
    assert_eq!(
        (due.rows(), due.bytes()),
        (4, 40),
        "a barrier seals them: they still count"
    );
    due.written(0, 6, 60);
    assert_eq!((due.rows(), due.bytes()), (10, 100));
    due.sealed(0, 10, 100);
    assert_eq!(
        (due.rows(), due.bytes()),
        (10, 100),
        "sealed, they count once"
    );
    due.committing();
    assert_eq!((due.rows(), due.bytes()), (0, 0));
    due.written(1, 5, 50);
    due.abandoned(1);
    assert_eq!(
        (due.rows(), due.bytes()),
        (0, 0),
        "abandoned rows count no more"
    );
    // A seal of nothing written makes nothing more due.
    due.sealed(2, 0, 0);
    assert_eq!((due.rows(), due.bytes()), (0, 0));
}

#[test]
fn rows_a_partition_held_when_it_did_not_answer_count_no_more_until_sealed() {
    let mut due = Due::default();
    due.written(0, 10, 100);
    due.unanswered(0);
    assert_eq!((due.rows(), due.bytes()), (0, 0));
    due.committing();
    assert_eq!((due.rows(), due.bytes()), (0, 0), "nor after a commit");
    due.written(0, 3, 30);
    assert_eq!(
        (due.rows(), due.bytes()),
        (3, 30),
        "what it writes next counts"
    );
    due.sealed(0, 13, 130);
    assert_eq!(
        (due.rows(), due.bytes()),
        (13, 130),
        "all it wrote counts once sealed"
    );
    due.unanswered(1);
    assert_eq!((due.rows(), due.bytes()), (13, 130));
}

#[test]
fn a_seal_takes_only_the_rows_it_seals_from_those_written() {
    let mut due = Due::default();
    // Rows of the next segment heard before the seal of its predecessor.
    due.written(0, 4, 40);
    due.written(0, 3, 30);
    due.sealed(0, 4, 40);
    assert_eq!(
        (due.rows(), due.bytes()),
        (7, 70),
        "four sealed, three still written"
    );
    due.committing();
    assert_eq!((due.rows(), due.bytes()), (3, 30));
    due.sealed(0, 3, 30);
    assert_eq!(
        (due.rows(), due.bytes()),
        (3, 30),
        "the three sealed count once"
    );
}
