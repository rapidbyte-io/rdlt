use std::num::NonZeroUsize;

use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int32Type, Int64Type, TimestampMicrosecondType};

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
fn each_event_s_columns_follow_from_its_id() {
    let batch = events(2_999, 3);
    let column = |name: &str| batch.column_by_name(name).unwrap();
    let ints = |name: &str| column(name).as_primitive::<Int64Type>().values().to_vec();
    let floats = |name: &str| column(name).as_primitive::<Float64Type>().values().to_vec();
    let texts = |name: &str| {
        let text = column(name).as_string::<i32>();
        text.iter()
            .map(|value| value.unwrap().to_owned())
            .collect::<Vec<_>>()
    };
    assert_eq!(ints("a"), [20_993, 21_000, 21_007]);
    assert_eq!(ints("b"), [38_987, 39_000, 39_013]);
    assert_eq!(floats("x"), [1_499.5, 1_500.0, 1_500.5]);
    assert_eq!(floats("y"), [3_748.75, 3_750.0, 3_751.25]);
    assert_eq!(
        texts("name"),
        ["user-00002999", "user-00003000", "user-00003001"]
    );
    assert_eq!(
        texts("city"),
        ["city-00002999", "city-00003000", "city-00003001"]
    );
    let at = column("at").as_primitive::<TimestampMicrosecondType>();
    assert_eq!(
        at.values().to_vec(),
        [
            1_790_000_000_002_999,
            1_790_000_000_003_000,
            1_790_000_000_003_001
        ]
    );
    let flags: Vec<Option<bool>> = column("flag").as_boolean().iter().collect();
    assert_eq!(flags, [Some(false), Some(true), Some(false)]);
    let small = column("n").as_primitive::<Int32Type>().values().to_vec();
    assert_eq!(small, [999, 0, 1]);
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
