use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use arrow_array::cast::AsArray;
use arrow_array::types::{Int8Type, TimestampMicrosecondType};
use arrow_array::{
    Array, ArrayRef, DictionaryArray, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray, TimestampNanosecondArray,
};
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
fn a_version_begins_when_its_load_started_unless_the_stream_names_its_change_time() {
    let data = batch(vec![("id", Arc::new(Int64Array::from(vec![1, 2])))]);
    let loaded_at = UNIX_EPOCH + Duration::from_micros(1_500);
    let [from, to, current, _] =
        history_columns(&stream(), &data, None, loaded_at, &|_| false).unwrap();
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
            history_columns(&stream(), &data, Some(time), loaded_at, &|_| false).unwrap();
        let from = from.as_primitive::<TimestampMicrosecondType>();
        assert_eq!(from.values().to_vec(), expected);
        assert_eq!(from.timezone(), Some("UTC"));
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
