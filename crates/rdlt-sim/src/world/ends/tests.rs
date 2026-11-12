use std::collections::BTreeSet;

use super::{Ends, end};
use crate::workload::End;

#[test]
fn a_read_ends_on_rows_after_a_checkpoint_only_where_one_followed_a_row_it_sent() {
    assert_eq!(end(Some(9), 4..9), Some(End::Checkpoint));
    assert_eq!(end(Some(6), 4..9), Some(End::Tail));
    assert_eq!(
        end(Some(4), 4..9),
        Some(End::Unmarked),
        "a checkpoint at its start comes after none of its rows"
    );
    assert_eq!(end(None, 4..9), Some(End::Unmarked));
    assert_eq!(end(Some(4), 4..4), None, "it sent nothing");
}

#[test]
fn a_tail_is_kept_only_where_the_engine_took_the_read() {
    let ends = Ends::default();
    for stream in [0, 1] {
        ends.note(stream, End::Tail);
        ends.note(stream, End::Unmarked);
    }
    ends.keep(&BTreeSet::from([1]));
    assert_eq!(ends.of(0), [End::Tail, End::Unmarked]);
    assert_eq!(
        ends.of(1),
        [End::Unmarked],
        "a refused read's tail reached nothing"
    );
    ends.note(1, End::Tail);
    assert_eq!(
        ends.of(1),
        [End::Unmarked],
        "kept only once its attempt ended"
    );
    ends.keep(&BTreeSet::new());
    assert_eq!(ends.of(1), [End::Tail, End::Unmarked]);
}
