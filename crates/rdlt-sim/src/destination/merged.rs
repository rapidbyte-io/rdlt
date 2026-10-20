//! A merge table's rows through the SDK's Arrow merge, each merged row kept as the row it came
//! from was written: its columns in their order, each naming what its field named, so the oracle
//! reads a merged row back as it reads any other.
//!
//! The merge gives rows back under the table's schema. Each row it reads carries the place of the
//! stored row it is in a column of its own, which the merge carries as it carries any value, and
//! a merged row takes its source's fields again.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::Array as _;
use arrow_array::cast::AsArray;
use arrow_array::types::UInt64Type;
use arrow_array::{ArrayRef, RecordBatch, UInt64Array, new_null_array};
use arrow_schema::{DataType, Field as ArrowField, Schema, SchemaRef};
use rdlt_connector::{ConnectorError, Field, LogicalType, Result};

use super::cells::WRITTEN;
use super::{Stored, cells, columns};
use crate::reached::{Reach, Reached};

/// The column a row read by a merge carries its source's place in.
const SOURCE: &str = "_rdlt_sim_source";

/// The Arrow schema rows of a table of `columns` merge under: its columns, each nullable and of
/// the table's type, and the column of each row's source.
pub(super) fn schema(columns: &columns::Columns) -> SchemaRef {
    let mut fields: Vec<ArrowField> = columns
        .iter()
        .map(|(name, logical)| Field::new(name.as_str(), logical.clone(), true).to_arrow())
        .collect();
    fields.push(ArrowField::new(SOURCE, DataType::UInt64, true));
    Arc::new(Schema::new(fields))
}

/// The stored rows a merge reads, by place.
#[derive(Default)]
pub(super) struct Sources<'a>(Vec<&'a Stored>);

impl<'a> Sources<'a> {
    /// `rows` as the batches a merge reads, each carrying its place among the sources.
    pub(super) fn read(&mut self, rows: impl IntoIterator<Item = &'a Stored>) -> Vec<RecordBatch> {
        rows.into_iter()
            .map(|row| {
                let place = u64::try_from(self.0.len()).expect("a place fits 64 bits");
                self.0.push(row);
                let mut fields = row.row.schema_ref().fields().to_vec();
                fields.push(Arc::new(ArrowField::new(SOURCE, DataType::UInt64, false)));
                let mut columns = row.row.columns().to_vec();
                columns.push(Arc::new(UInt64Array::from(vec![place])));
                RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
                    .expect("a stored row with its place")
            })
            .collect()
    }

    /// The rows of `merged`, the batches a merge of rows of a table of `columns` gave back, each
    /// under the fields of the row it came from, in their order: a value at the type the table
    /// holds it, a column the row held and the merge left empty as nulls, and a column it never
    /// held after them.
    ///
    /// A value the merge widened to another type than it was written as adds [`Reach::Widened`]
    /// to `reached`.
    pub(super) fn restored(
        &self,
        merged: &[RecordBatch],
        columns: &columns::Columns,
        reached: &mut Reached,
    ) -> Result<Vec<Stored>> {
        let mut rows = Vec::new();
        for batch in merged {
            let places = batch
                .column_by_name(SOURCE)
                .ok_or_else(|| ConnectorError::internal("a merged batch names no source"))?;
            let places = places.as_primitive::<UInt64Type>();
            for row in 0..batch.num_rows() {
                if places.is_null(row) {
                    return Err(ConnectorError::internal("a merged row names no source"));
                }
                let place = usize::try_from(places.value(row)).unwrap_or(usize::MAX);
                let source = self
                    .0
                    .get(place)
                    .ok_or_else(|| ConnectorError::internal("a merged row names no source"))?;
                rows.extend(cells::rows(&written(
                    &batch.slice(row, 1),
                    source,
                    columns,
                    reached,
                ))?);
            }
        }
        Ok(rows)
    }
}

/// The type `source`'s value was written as, where the merge holds it as `typed`, another type
/// its column widened to since.
fn widened(source: &ArrowField, typed: &ArrowField) -> Option<LogicalType> {
    let json = |field: &ArrowField| Field::extension_type(field) == Some(LogicalType::Json);
    if typed.data_type() == source.data_type() && json(typed) == json(source) {
        return None;
    }
    Field::from_arrow(source)
        .ok()
        .map(|field| field.logical_type().clone())
}

/// `merged`, one row, under the fields of `source`, the stored row it came from, in their order
/// and naming what they named, at the types `merged` holds its values as; the columns of the
/// table `source` held that `merged` does not as nulls, and those it holds that `source` did not
/// after them.
///
/// A value widened to another type than its source adds [`Reach::Widened`] to `reached`.
fn written(
    merged: &RecordBatch,
    source: &Stored,
    columns: &columns::Columns,
    reached: &mut Reached,
) -> RecordBatch {
    let held = merged.schema_ref();
    let mut fields = Vec::new();
    let mut values: Vec<ArrayRef> = Vec::new();
    for field in source.row.schema_ref().fields() {
        let Some(logical) = columns.get(field.name()) else {
            // Only the columns that direct a change stream's merge are no column of its table.
            continue;
        };
        let (typed, value) = if let Some((index, typed)) = held.column_with_name(field.name()) {
            (typed.clone(), Arc::clone(merged.column(index)))
        } else {
            let typed = Field::new(field.name().as_str(), logical.clone(), true).to_arrow();
            let nulls = new_null_array(typed.data_type(), 1);
            (typed, nulls)
        };
        // A value a merge widened before keeps the type it was first written as.
        let written = field.metadata().get(WRITTEN).cloned().or_else(|| {
            widened(field, &typed).map(|written| {
                reached.add(Reach::Widened);
                serde_json::to_string(&written).expect("a logical type serializes")
            })
        });
        let mut metadata = field.metadata().clone();
        metadata.extend(typed.metadata().clone());
        if let Some(written) = written {
            metadata.insert(WRITTEN.to_owned(), written);
        }
        fields.push(typed.with_nullable(true).with_metadata(metadata));
        values.push(value);
    }
    for (index, typed) in held.fields().iter().enumerate() {
        let named = |field: &ArrowField| field.name() == typed.name();
        if typed.name() != SOURCE && !fields.iter().any(named) {
            fields.push(typed.as_ref().clone().with_nullable(true));
            values.push(Arc::clone(merged.column(index)));
        }
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), values).expect("one row of every column")
}
