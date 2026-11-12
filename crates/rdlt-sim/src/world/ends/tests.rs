use super::end;
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
