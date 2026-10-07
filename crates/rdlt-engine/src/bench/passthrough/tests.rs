use std::num::NonZeroUsize;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;

use super::{Passthrough, events};
use crate::Cores;

fn ids(batch: &RecordBatch) -> Vec<i64> {
    batch
        .column(0)
        .as_primitive::<Int64Type>()
        .values()
        .to_vec()
}

#[test]
fn events_hold_ten_columns_of_rows_whose_ids_run_from_the_first() {
    let batch = events(100, 5);
    assert_eq!((batch.num_rows(), batch.num_columns()), (5, 10));
    assert_eq!(ids(&batch), [100, 101, 102, 103, 104]);
}

#[test]
fn the_bare_loop_and_the_engine_each_move_every_row_on_every_run() {
    let cores = Cores::from_count(NonZeroUsize::new(2).unwrap());
    let passthrough = Passthrough::try_new(cores, 3, 10).unwrap();
    let all: Vec<i64> = passthrough.batches().iter().flat_map(ids).collect();
    assert_eq!(all, (0..30).collect::<Vec<_>>());
    for _ in 0..2 {
        assert_eq!(passthrough.bare_loop(), 30);
        assert_eq!(passthrough.engine_run(), 30);
    }
}
