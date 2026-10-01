//! Narrows a column to what its rows name, so that its frame carries nothing else and is
//! counted by its receiver as its sender weighed it.
//!
//! Arrow's writer sends the data buffers of views and the children of list views and dense
//! unions whole, however few rows name them, and does not move a union under a list whose
//! first offset is not zero to where the list's items begin. Only a column of a layout the
//! writer sends as its rows goes as it is; every other is rebuilt from the ranges of items its
//! rows name.

mod gather;
mod leaf;
mod lists;
#[cfg(test)]
mod tests;
mod unions;

use std::sync::Arc;

use arrow_array::{Array as _, ArrayRef, RecordBatch, RecordBatchOptions};
use arrow_schema::{ArrowError, DataType};

/// Whether Arrow's writer sends a column of `data_type` as the rows it names and nothing
/// else, whatever the column was sliced from.
///
/// Fixed-width values, bytes by offsets, and structs, lists and dictionaries of those do; a
/// type not named here is narrowed.
pub(super) fn plain(data_type: &DataType) -> bool {
    use DataType as T;
    match data_type {
        T::Null
        | T::Boolean
        | T::FixedSizeBinary(_)
        | T::Utf8
        | T::Binary
        | T::LargeUtf8
        | T::LargeBinary => true,
        T::List(item) | T::LargeList(item) | T::FixedSizeList(item, _) => plain(item.data_type()),
        T::Struct(fields) => fields.iter().all(|field| plain(field.data_type())),
        T::Dictionary(_, values) => plain(values),
        other => other.is_primitive(),
    }
}

/// `batch` with each column holding only what its rows name.
#[cfg(test)]
pub(super) fn compacted(batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    Narrower::default().batch(batch)
}

/// Narrows the pieces of one batch, rebuilding each dictionary's values once for them all.
#[derive(Debug, Default)]
pub(super) struct Narrower {
    /// The values of the batch's dictionaries that are not plain, and each rebuilt.
    rebuilt: Vec<(ArrayRef, ArrayRef)>,
}

impl Narrower {
    /// `batch` with each column holding only what its rows name.
    pub(super) fn batch(&mut self, batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
        let columns = batch.columns().iter();
        let columns: Result<Vec<_>, _> = columns.map(|array| self.column(array)).collect();
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        RecordBatch::try_new_with_options(batch.schema(), columns?, &options)
    }

    /// `array` as it is where its layout is plain, else rebuilt from its rows.
    fn column(&mut self, array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
        if plain(array.data_type()) {
            return Ok(Arc::clone(array));
        }
        self.gathered(array, &[(0, array.len())])
    }

    /// The values of a dictionary holding only what their rows name: the same array for every
    /// piece, so that they are sent once.
    fn values(&mut self, values: &ArrayRef) -> Result<ArrayRef, ArrowError> {
        let mut known = self.rebuilt.iter();
        if let Some((_, narrowed)) = known.find(|(original, _)| Arc::ptr_eq(original, values)) {
            return Ok(Arc::clone(narrowed));
        }
        let narrowed = self.column(values)?;
        self.rebuilt
            .push((Arc::clone(values), Arc::clone(&narrowed)));
        Ok(narrowed)
    }
}

/// Ranges of a column's items, each from its start to before its end, in the order named.
type Ranges = [(usize, usize)];

/// Adds the items from `start` to before `end` to `ranges`, joined to the last where they
/// follow it.
fn name(ranges: &mut Vec<(usize, usize)>, start: usize, end: usize) {
    if start >= end {
        return;
    }
    match ranges.last_mut() {
        Some(last) if last.1 == start => last.1 = end,
        _ => ranges.push((start, end)),
    }
}

/// How many items `ranges` name.
fn count(ranges: &Ranges) -> usize {
    let lengths = ranges.iter().map(|(start, end)| end.saturating_sub(*start));
    lengths.fold(0, usize::saturating_add)
}

/// `count` as an offset, a run end or an index of type `T`, or the error of a column that
/// outgrew it.
fn sized<T: TryFrom<usize>>(count: usize) -> Result<T, ArrowError> {
    T::try_from(count).map_err(|_| {
        ArrowError::InvalidArgumentError(format!(
            "{count} is beyond what the column's offsets hold"
        ))
    })
}
