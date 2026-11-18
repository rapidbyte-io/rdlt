//! The rows certification stages, each segment's with ids of its own, and how what a table
//! publishes is checked against the segments committed.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::DataType;

use crate::destination::TableRef;
use crate::schema::TableSchema;
use crate::testing::Violation;
use crate::testing::reason::Listed;
use crate::types::{Field, LogicalType};

#[cfg(test)]
mod tests;

/// The segment whose ids a fenced worker's late write carries: no clause commits it.
pub(super) const STALE: u64 = 9;

/// The rows staged as `segment`: ids of that segment's own, `segment` hundreds and 1 to 3, with
/// names.
pub(super) fn staged(segment: u64) -> Vec<Row> {
    let first = i64::try_from(segment * 100).expect("certification segments are small");
    let names = [Some("ann"), None, Some("ola")];
    (1..)
        .zip(names)
        .map(|(place, name)| (first + place, name.map(str::to_owned)))
        .collect()
}

/// A published row: its id and name.
pub(super) type Row = (i64, Option<String>);

/// Whether `actual`, in order, holds exactly `segments`' rows, each once.
pub(super) fn expect_rows(actual: &[Row], segments: &[u64]) -> Result<(), Violation> {
    let mut expected: Vec<Row> = segments.iter().copied().flat_map(staged).collect();
    expected.sort_unstable();
    if actual == expected.as_slice() {
        Ok(())
    } else {
        Err(Violation::from(format_args!(
            "the table publishes {}, not those of segments {segments:?}",
            Listed(actual)
        )))
    }
}

impl super::Bench<'_> {
    /// The certification tables' schema.
    pub(super) fn schema(&self) -> TableSchema {
        TableSchema::new(vec![
            Field::new(self.names.column("id"), LogicalType::Int64, false),
            Field::new(self.names.column("name"), LogicalType::Utf8, true),
        ])
        .expect("the certification schema is valid")
    }

    /// Three rows staged as `segment`, with ids of that segment's own: [`staged`] says which.
    pub(super) fn rows(&self, segment: u64) -> RecordBatch {
        let (ids, names): (Vec<i64>, Vec<Option<String>>) = staged(segment).into_iter().unzip();
        let ids: Arc<Int64Array> = Arc::new(Int64Array::from(ids));
        let names: Arc<StringArray> = Arc::new(StringArray::from(names));
        RecordBatch::try_from_iter([
            (self.names.column("id"), ids as _),
            (self.names.column("name"), names as _),
        ])
        .expect("the certification batch is valid")
    }

    /// The rows the clause's table publishes, in order.
    pub(super) async fn published_rows(&self) -> Result<Vec<Row>, Violation> {
        self.rows_of(&self.table()).await
    }

    /// The `(id, name)` rows `table` publishes, in order.
    pub(super) async fn rows_of(&self, table: &TableRef) -> Result<Vec<Row>, Violation> {
        let published = self.read(table).await?;
        let mut rows = Vec::with_capacity(published.rows());
        for batch in published.batches() {
            let ids = batch.required(&self.names.column("id"), &DataType::Int64)?;
            let names = batch.nullable(&self.names.column("name"), &DataType::Utf8)?;
            let (ids, names) = (ids.as_primitive::<Int64Type>(), names.as_string::<i32>());
            for row in 0..batch.rows() {
                let name = names.is_valid(row).then(|| names.value(row).to_owned());
                rows.push((ids.value(row), name));
            }
        }
        rows.sort_unstable();
        Ok(rows)
    }
}
