use std::sync::Arc;

use arrow_array::{ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, UInt64Array};
use rdlt_connector::LogicalType;

use super::super::cells::{self, Stored, WRITTEN};
use super::{SOURCE, Sources};
use crate::reached::Reached;

/// `stored` merged into a column `n` now of type `logical`, held as `value`.
fn merged(stored: &Stored, logical: LogicalType, value: ArrayRef) -> Stored {
    let mut sources = Sources::default();
    sources.read([stored]);
    let batch = RecordBatch::try_from_iter([
        ("n", value),
        (SOURCE, Arc::new(UInt64Array::from(vec![0])) as ArrayRef),
    ])
    .unwrap();
    let columns = [("n".to_owned(), logical)].into();
    let mut rows = sources
        .restored(&[batch], &columns, &mut Reached::default())
        .unwrap();
    assert_eq!(rows.len(), 1);
    rows.remove(0)
}

/// The type the row's column `n` names as its value's written type.
fn written_as(row: &Stored) -> Option<String> {
    let schema = row.row.schema();
    let (_, field) = schema.column_with_name("n").unwrap();
    field.metadata().get(WRITTEN).cloned()
}

#[test]
fn a_column_widened_twice_keeps_the_type_its_value_was_written_as() {
    let stored = cells::rows(
        &RecordBatch::try_from_iter([("n", Arc::new(Int32Array::from(vec![7])) as ArrayRef)])
            .unwrap(),
    )
    .unwrap()
    .remove(0);
    let int32 = serde_json::to_string(&LogicalType::Int32).unwrap();
    let once = merged(
        &stored,
        LogicalType::Int64,
        Arc::new(Int64Array::from(vec![7])),
    );
    assert_eq!(written_as(&once), Some(int32.clone()));
    let twice = merged(
        &once,
        LogicalType::Float64,
        Arc::new(Float64Array::from(vec![7.0])),
    );
    assert_eq!(written_as(&twice), Some(int32));
}

#[test]
fn a_merged_row_that_names_no_source_is_refused() {
    let stored = cells::rows(
        &RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef)])
            .unwrap(),
    )
    .unwrap();
    let mut sources = Sources::default();
    let read = sources.read(&stored);
    assert_eq!(read.len(), 1);
    let batch = RecordBatch::try_from_iter([
        ("id", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        (
            SOURCE,
            Arc::new(UInt64Array::from(vec![None::<u64>])) as ArrayRef,
        ),
    ])
    .unwrap();
    let columns = [("id".to_owned(), LogicalType::Int64)].into();
    // The place of a row a merge gave back with no place is no source's, not the first one's.
    assert!(
        sources
            .restored(&[batch], &columns, &mut Reached::default())
            .is_err()
    );
}
