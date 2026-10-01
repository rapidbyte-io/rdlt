//! Batches under a table's schema, where a column a batch never had costs one shared array of
//! nulls for every such column of its type, however many the table has gained since.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, RecordBatch, RecordBatchOptions, UInt64Array, new_null_array,
};
use arrow_schema::{ArrowError, DataType, SchemaRef};

use super::retype::retyped;

/// Arrays of nulls by type and length, made once and shared by every column that needs one.
#[derive(Debug, Default)]
pub(super) struct Nulls(HashMap<(DataType, usize), ArrayRef>);

impl Nulls {
    /// `rows` nulls of `kind`.
    pub(super) fn of(&mut self, kind: &DataType, rows: usize) -> ArrayRef {
        let nulls = self
            .0
            .entry((kind.clone(), rows))
            .or_insert_with(|| new_null_array(kind, rows));
        Arc::clone(nulls)
    }
}

/// Whether every value of `array` is null, as a column a batch never had is.
fn all_null(array: &dyn Array) -> bool {
    array.null_count() == array.len()
}

fn batch(
    schema: &SchemaRef,
    columns: Vec<ArrayRef>,
    rows: usize,
) -> Result<RecordBatch, ArrowError> {
    let options = RecordBatchOptions::new().with_row_count(Some(rows));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options)
}

/// `batch` under `schema`: columns found by name, each as the schema's type where that keeps
/// every value exactly, and missing columns null; a value the schema's type cannot hold fails.
pub(super) fn aligned(
    unaligned: &RecordBatch,
    schema: &SchemaRef,
    nulls: &mut Nulls,
) -> Result<RecordBatch, ArrowError> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| match unaligned.column_by_name(field.name()) {
            Some(column) => retyped(column, field.data_type()),
            None => Ok(nulls.of(field.data_type(), unaligned.num_rows())),
        })
        .collect::<Result<Vec<ArrayRef>, _>>()?;
    batch(schema, columns, unaligned.num_rows())
}

/// One batch holding `batches` under `schema`.
pub(super) fn concat(
    batches: &[RecordBatch],
    schema: &SchemaRef,
    nulls: &mut Nulls,
) -> Result<RecordBatch, ArrowError> {
    let mut aligned = batches
        .iter()
        .map(|batch| aligned(batch, schema, nulls))
        .collect::<Result<Vec<_>, _>>()?;
    if aligned.len() == 1 {
        // One batch is itself: nothing of it is copied.
        return Ok(aligned.swap_remove(0));
    }
    let rows = aligned.iter().map(RecordBatch::num_rows).sum();
    let columns = schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            let parts: Vec<&dyn Array> = aligned
                .iter()
                .map(|batch| batch.column(index).as_ref())
                .collect();
            if parts.iter().all(|part| all_null(*part)) {
                return Ok(nulls.of(field.data_type(), rows));
            }
            arrow_select::concat::concat(&parts)
        })
        .collect::<Result<Vec<ArrayRef>, _>>()?;
    batch(schema, columns, rows)
}

/// The rows of `whole` that `keep` marks.
pub(super) fn filtered(
    whole: &RecordBatch,
    keep: &BooleanArray,
    nulls: &mut Nulls,
) -> Result<RecordBatch, ArrowError> {
    let rows = keep.true_count();
    if rows == whole.num_rows() {
        return Ok(whole.clone());
    }
    let columns = whole
        .columns()
        .iter()
        .map(|column| {
            if all_null(column.as_ref()) {
                return Ok(nulls.of(column.data_type(), rows));
            }
            arrow_select::filter::filter(column.as_ref(), keep)
        })
        .collect::<Result<Vec<ArrayRef>, _>>()?;
    batch(&whole.schema(), columns, rows)
}

/// The rows of `whole` at `rows`, in that order.
pub(super) fn taken(
    whole: &RecordBatch,
    rows: &[usize],
    nulls: &mut Nulls,
) -> Result<RecordBatch, ArrowError> {
    let indices = UInt64Array::from_iter_values(rows.iter().map(|row| *row as u64));
    let columns = whole
        .columns()
        .iter()
        .map(|column| {
            if all_null(column.as_ref()) {
                return Ok(nulls.of(column.data_type(), rows.len()));
            }
            arrow_select::take::take(column.as_ref(), &indices, None)
        })
        .collect::<Result<Vec<ArrayRef>, _>>()?;
    batch(&whole.schema(), columns, rows.len())
}

/// A column of `kind` whose value at each row is that of `sources` at `indices`: the source and
/// the row within it.
pub(super) fn interleaved(
    sources: &[&dyn Array],
    indices: &[(usize, usize)],
    kind: &DataType,
    nulls: &mut Nulls,
) -> Result<ArrayRef, ArrowError> {
    if sources.iter().all(|source| all_null(*source)) {
        return Ok(nulls.of(kind, indices.len()));
    }
    arrow_select::interleave::interleave(sources, indices)
}
