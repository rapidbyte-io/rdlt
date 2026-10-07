use arrow_array::RecordBatch;
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int32Type, Int64Type, TimestampMicrosecondType};

use super::{events, events_of};

fn ints(batch: &RecordBatch, name: &str) -> Vec<i64> {
    let column = batch.column_by_name(name).unwrap();
    column.as_primitive::<Int64Type>().values().to_vec()
}

#[test]
fn events_hold_their_width_of_rows_whose_ids_run_from_the_first() {
    let batch = events(100, 5, 10);
    assert_eq!((batch.num_rows(), batch.num_columns()), (5, 10));
    assert_eq!(ints(&batch, "id"), [100, 101, 102, 103, 104]);
    let narrow = events(100, 5, 3);
    let names: Vec<String> = narrow
        .schema()
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect();
    assert_eq!(names, ["id", "a", "b"]);
}

#[test]
fn each_event_s_columns_follow_from_its_id() {
    let batch = events(2_999, 3, 10);
    let column = |name: &str| batch.column_by_name(name).unwrap();
    let floats = |name: &str| column(name).as_primitive::<Float64Type>().values().to_vec();
    let texts = |name: &str| {
        let text = column(name).as_string::<i32>();
        text.iter()
            .map(|value| value.unwrap().to_owned())
            .collect::<Vec<_>>()
    };
    assert_eq!(ints(&batch, "a"), [20_993, 21_000, 21_007]);
    assert_eq!(ints(&batch, "b"), [38_987, 39_000, 39_013]);
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
fn a_wide_batch_takes_the_ten_kinds_again_each_round_shifted_by_it() {
    let batch = events(2_999, 3, 23);
    assert_eq!(batch.num_columns(), 23);
    let column = |name: &str| batch.column_by_name(name).unwrap();
    assert_eq!(ints(&batch, "id_1"), [3_000, 3_001, 3_002]);
    assert_eq!(ints(&batch, "a_2"), [20_995, 21_002, 21_009]);
    let floats = column("x_1")
        .as_primitive::<Float64Type>()
        .values()
        .to_vec();
    assert_eq!(floats, [1_500.5, 1_501.0, 1_501.5]);
    let names = column("name_1").as_string::<i32>();
    assert_eq!(names.value(0), "user1-00002999");
    let at = column("at_1").as_primitive::<TimestampMicrosecondType>();
    assert_eq!(at.value(0), 1_790_000_000_003_000);
    let flags: Vec<Option<bool>> = column("flag_1").as_boolean().iter().collect();
    assert_eq!(flags, [Some(true), Some(false), Some(false)]);
    let small = column("n_1").as_primitive::<Int32Type>().values().to_vec();
    assert_eq!(small, [0, 1, 2]);
    assert_eq!(ints(&batch, "b_2"), [38_989, 39_002, 39_015]);
}

#[test]
#[should_panic(expected = "a batch has a column")]
fn a_batch_of_no_columns_is_a_bug() {
    events(0, 1, 0);
}

#[test]
fn events_of_any_ids_shift_every_value_but_the_ids() {
    let batch = events_of(&[5, 3], 11, 7);
    assert_eq!(ints(&batch, "id"), [5, 3]);
    assert_eq!(ints(&batch, "a"), [42, 28]);
    assert_eq!(ints(&batch, "id_1"), [13, 11]);
    let names = batch.column_by_name("name").unwrap().as_string::<i32>();
    assert_eq!(names.value(0), "user7-00000005");
    let unshifted = events_of(&[5, 6], 10, 0);
    assert_eq!(unshifted, events(5, 2, 10));
}
