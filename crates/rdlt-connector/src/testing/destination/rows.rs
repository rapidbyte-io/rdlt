//! The rows certification stages, each segment's with ids of its own, and how what a table
//! publishes is checked against the segments committed.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};

use crate::destination::TableRef;
use crate::schema::TableSchema;
use crate::testing::{Violation, bounded_call};
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

impl super::Bench<'_> {
    /// The rows the clause's table publishes, in order.
    pub(super) async fn published_rows(&self) -> Result<Vec<Row>, Violation> {
        self.rows_of(&self.table()).await
    }

    /// The `(id, name)` rows `table` publishes, in order.
    pub(super) async fn rows_of(&self, table: &TableRef) -> Result<Vec<Row>, Violation> {
        let batches = bounded_call("probe", self.probe.published(table)).await?;
        let mut rows = Vec::new();
        for batch in &batches {
            let column = |name: &str, logical: &arrow_schema::DataType| {
                let column = batch.column_by_name(name).ok_or_else(|| {
                    Violation::from(format!("a published batch has no {name} column"))
                })?;
                arrow_cast::cast(column, logical)
                    .map_err(|error| Violation::from(format!("the {name}s read back as {error}")))
            };
            let ids = column("id", &arrow_schema::DataType::Int64)?;
            let names = column("name", &arrow_schema::DataType::Utf8)?;
            let (ids, names) = (ids.as_primitive::<Int64Type>(), names.as_string::<i32>());
            for row in 0..batch.num_rows() {
                if ids.is_null(row) {
                    return Err("a published row has no id".into());
                }
                let name = names.is_valid(row).then(|| names.value(row).to_owned());
                rows.push((ids.value(row), name));
            }
        }
        rows.sort_unstable();
        Ok(rows)
    }
}
