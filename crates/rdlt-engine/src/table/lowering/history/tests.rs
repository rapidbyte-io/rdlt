use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use arrow_array::cast::AsArray;
use arrow_array::types::{Int8Type, TimestampMicrosecondType};
use arrow_array::{
    Array, ArrayRef, Date32Array, Date64Array, DictionaryArray, Int32Array, Int64Array,
    RecordBatch, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{DataType, Field};
use rdlt_connector::StreamName;

use super::history_columns;
use crate::error::ErrorKind;

fn stream() -> StreamName {
    StreamName::new("orders").unwrap()
}

fn batch(columns: Vec<(&str, ArrayRef)>) -> RecordBatch {
    RecordBatch::try_from_iter(columns).unwrap()
}

fn hashes(data: &RecordBatch) -> Vec<Option<Vec<u8>>> {
    let [_, _, _, hash] = history_columns(&stream(), data, None, UNIX_EPOCH, &|_| false).unwrap();
    hash.as_binary::<i32>()
        .iter()
        .map(|hash| hash.map(<[u8]>::to_vec))
        .collect()
}

#[test]
fn equal_data_hashes_alike_and_other_data_differently() {
    let data = batch(vec![
        ("id", Arc::new(Int64Array::from(vec![1, 1, 2, 1]))),
        (
            "name",
            Arc::new(StringArray::from(vec!["a", "a", "a", "b"])),
        ),
    ]);
    let hashes = hashes(&data);
    assert!(
        hashes
            .iter()
            .all(|hash| hash.as_ref().is_some_and(|hash| hash.len() == 16))
    );
    assert_eq!(hashes[0], hashes[1]);
    assert_ne!(hashes[0], hashes[2]);
    assert_ne!(hashes[0], hashes[3]);
}

#[test]
fn a_hash_ignores_null_columns_the_order_of_columns_their_width_and_their_encoding() {
    let plain = batch(vec![
        ("id", Arc::new(Int64Array::from(vec![1]))),
        ("name", Arc::new(StringArray::from(vec!["a"]))),
    ]);
    // A column added since, null in the row; the columns in another order; the id narrower and
    // the name dictionary-encoded.
    let names: DictionaryArray<Int8Type> = vec!["a"].into_iter().collect();
    let other = batch(vec![
        ("name", Arc::new(names)),
        ("added", Arc::new(StringArray::from(vec![None::<&str>]))),
        ("id", Arc::new(Int32Array::from(vec![1]))),
    ]);
    assert_eq!(hashes(&plain), hashes(&other));
}

#[test]
fn a_version_begins_when_its_batch_arrived_unless_the_stream_names_its_change_time() {
    let data = batch(vec![("id", Arc::new(Int64Array::from(vec![1, 2])))]);
    let received = UNIX_EPOCH + Duration::from_micros(1_500);
    let [from, to, current, _] =
        history_columns(&stream(), &data, None, received, &|_| false).unwrap();
    let from = from.as_primitive::<TimestampMicrosecondType>();
    assert_eq!(from.values().to_vec(), [1_500, 1_500]);
    assert_eq!(from.timezone(), Some("UTC"));
    assert_eq!(to.null_count(), 2);
    assert!(
        current
            .as_boolean()
            .iter()
            .all(|current| current == Some(true))
    );
    // A change time in any unit and zone begins the version at its instant, in microseconds.
    let times: Vec<ArrayRef> = vec![
        Arc::new(TimestampNanosecondArray::from(vec![7_000, 9_000_000])),
        Arc::new(TimestampMillisecondArray::from(vec![0, 9]).with_timezone("+02:00")),
    ];
    let expected = [[7, 9_000], [0, 9_000]];
    for (time, expected) in times.iter().zip(expected) {
        let [from, ..] =
            history_columns(&stream(), &data, Some(time), received, &|_| false).unwrap();
        let from = from.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(from.values().to_vec(), expected);
        assert_eq!(from.timezone(), Some("UTC"));
    }
}

/// Two days, in microseconds.
const TWO_DAYS: i64 = 172_800_000_000;

/// Every time a change time may hold, plainly: the instants of two days, nothing, and two days.
fn times() -> Vec<ArrayRef> {
    let days = |days: Vec<Option<i64>>, per_day: i64| -> Vec<Option<i64>> {
        days.into_iter()
            .map(|day| day.map(|day| day * per_day))
            .collect()
    };
    let at = vec![Some(2), Some(0), Some(2)];
    vec![
        Arc::new(TimestampSecondArray::from(days(at.clone(), 86_400))),
        Arc::new(
            TimestampMillisecondArray::from(days(at.clone(), 86_400_000)).with_timezone("+02:00"),
        ),
        Arc::new(
            TimestampMicrosecondArray::from(days(at.clone(), 86_400_000_000)).with_timezone("UTC"),
        ),
        Arc::new(TimestampNanosecondArray::from(days(
            at.clone(),
            86_400_000_000_000,
        ))),
        Arc::new(Date32Array::from(vec![Some(2), Some(0), Some(2)])),
        Arc::new(Date64Array::from(days(at, 86_400_000))),
    ]
}

/// `plain` in every encoding a batch may carry it in: as it is, in a dictionary under every key
/// type, and run-end encoded under every run-end type.
fn encodings(plain: &ArrayRef) -> Vec<ArrayRef> {
    let values = Box::new(plain.data_type().clone());
    let keys = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ];
    let mut encoded = vec![Arc::clone(plain)];
    for key in keys {
        let dictionary = DataType::Dictionary(Box::new(key), values.clone());
        encoded.push(arrow_cast::cast(plain, &dictionary).unwrap());
    }
    for ends in [DataType::Int16, DataType::Int32, DataType::Int64] {
        let runs = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", ends, false)),
            Arc::new(Field::new("values", plain.data_type().clone(), true)),
        );
        encoded.push(arrow_cast::cast(plain, &runs).unwrap());
    }
    encoded
}

#[test]
fn a_change_time_of_every_time_type_and_encoding_begins_its_version_at_its_instant() {
    let data = batch(vec![("id", Arc::new(Int64Array::from(vec![1, 2, 3])))]);
    for plain in times() {
        for time in encodings(&plain) {
            let [from, ..] = history_columns(&stream(), &data, Some(&time), UNIX_EPOCH, &|_| false)
                .unwrap_or_else(|error| panic!("{}: {error}", time.data_type()));
            let from = from.as_primitive::<TimestampMicrosecondType>();
            assert_eq!(
                from.values().to_vec(),
                [TWO_DAYS, 0, TWO_DAYS],
                "{}",
                time.data_type()
            );
            assert_eq!(from.timezone(), Some("UTC"));
        }
    }
}

#[test]
fn a_change_time_missing_in_any_encoding_is_refused() {
    let data = batch(vec![("id", Arc::new(Int64Array::from(vec![1, 2, 3])))]);
    let missing: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![
        Some(TWO_DAYS),
        None,
        Some(TWO_DAYS),
    ]));
    for time in encodings(&missing) {
        let refused = history_columns(&stream(), &data, Some(&time), UNIX_EPOCH, &|_| false)
            .expect_err("a change without its time");
        assert_eq!(
            refused.code(),
            Some("change_time_null"),
            "{}",
            time.data_type()
        );
    }
}

#[test]
fn a_change_time_holding_no_time_in_any_encoding_is_refused() {
    let data = batch(vec![("id", Arc::new(Int64Array::from(vec![1, 2, 3])))]);
    let numbers: ArrayRef = Arc::new(Int64Array::from(vec![2, 0, 2]));
    for time in encodings(&numbers) {
        let refused = history_columns(&stream(), &data, Some(&time), UNIX_EPOCH, &|_| false)
            .expect_err("a change time holding numbers");
        assert_eq!(
            refused.code(),
            Some("change_time_invalid"),
            "{}",
            time.data_type()
        );
    }
}

#[test]
fn a_delete_carries_no_hash_and_a_change_without_its_time_is_refused() {
    let data = batch(vec![("id", Arc::new(Int64Array::from(vec![1, 2])))]);
    let [.., hash] = history_columns(&stream(), &data, None, UNIX_EPOCH, &|row| row == 1).unwrap();
    assert!(hash.is_valid(0) && hash.is_null(1));
    let time: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![Some(1), None]));
    let refused =
        history_columns(&stream(), &data, Some(&time), UNIX_EPOCH, &|_| false).unwrap_err();
    assert_eq!(refused.kind(), ErrorKind::Schema);
    assert_eq!(refused.code(), Some("change_time_null"));
}

#[test]
fn a_change_time_that_is_no_time_is_refused() {
    let data = batch(vec![("id", Arc::new(Int64Array::from(vec![1])))]);
    let time: ArrayRef = Arc::new(StringArray::from(vec!["2026-09-30T00:00:00Z"]));
    let refused =
        history_columns(&stream(), &data, Some(&time), UNIX_EPOCH, &|_| false).unwrap_err();
    assert_eq!(refused.kind(), ErrorKind::Schema);
    assert_eq!(refused.code(), Some("change_time_invalid"));
}

#[test]
fn a_change_time_beyond_what_microseconds_hold_is_refused() {
    let data = batch(vec![("id", Arc::new(Int64Array::from(vec![1])))]);
    let far: ArrayRef = Arc::new(TimestampSecondArray::from(vec![i64::MAX]));
    let refused =
        history_columns(&stream(), &data, Some(&far), UNIX_EPOCH, &|_| false).unwrap_err();
    assert_eq!(refused.kind(), ErrorKind::Schema);
    assert_eq!(refused.code(), Some("change_time_invalid"));
}
