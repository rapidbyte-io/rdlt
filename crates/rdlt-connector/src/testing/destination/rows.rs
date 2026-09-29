//! The rows certification stages, each segment's with ids of its own, and how what a table
//! publishes is checked against the segments committed.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};

use crate::schema::TableSchema;
use crate::testing::Violation;
use crate::types::{Field, LogicalType};

#[cfg(test)]
mod tests;

/// The certification tables' schema.
pub(super) fn schema() -> TableSchema {
    TableSchema::new(vec![
        Field::new("id", LogicalType::Int64, false),
        Field::new("name", LogicalType::Utf8, true),
    ])
    .expect("the certification schema is valid")
}

/// The segment whose ids a fenced worker's late write carries: no clause commits it.
pub(super) const STALE: u64 = 9;

/// Three rows staged as `segment`, with ids of that segment's own: `segment` hundreds and 1 to 3.
pub(super) fn rows(segment: u64) -> RecordBatch {
    let first = i64::try_from(segment * 100).expect("certification segments are small");
    let ids: Arc<Int64Array> = Arc::new(Int64Array::from_iter_values(first + 1..=first + 3));
    let names: Arc<StringArray> = Arc::new(StringArray::from(vec![Some("ann"), None, Some("ola")]));
    RecordBatch::try_from_iter([("id", ids as _), ("name", names as _)])
        .expect("the certification batch is valid")
}

/// A published row: its id and name.
pub(super) type Row = (i64, Option<String>);

/// Whether `actual`, in order, holds exactly `segments`' rows, each once.
pub(super) fn expect_rows(actual: &[Row], segments: &[u64]) -> Result<(), Violation> {
    let mut expected: Vec<Row> = segments
        .iter()
        .flat_map(|segment| {
            let batch = rows(*segment);
            let ids = batch.column(0).as_primitive::<Int64Type>().clone();
            let names = batch.column(1).as_string::<i32>().clone();
            (0..batch.num_rows()).map(move |row| {
                (
                    ids.value(row),
                    names.is_valid(row).then(|| names.value(row).to_owned()),
                )
            })
        })
        .collect();
    expected.sort_unstable();
    if actual == expected.as_slice() {
        Ok(())
    } else {
        Err(format!("the published rows are {actual:?}, not those of segments {segments:?}").into())
    }
}
