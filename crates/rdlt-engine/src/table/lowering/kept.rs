//! The rows a schema policy keeps: those holding no value of a column whose policy drops rows,
//! nor a value of a column of JSON its own column does not hold, where the others drop rows.

use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, RecordBatchOptions};
use arrow_buffer::BooleanBuffer;
use arrow_schema::ArrowError;
use arrow_select::filter::FilterBuilder;

use super::super::resolve::Route;
use super::split::Fitted;
use super::unheld::Converted;
use super::{LoweringPlan, both};
use crate::report::ColumnDiscards;

/// The rows of `batch` that `keep` keeps, its columns copied one at a time, each let go once it
/// is: a batch nothing else holds is never held twice over while it is filtered.
pub(crate) fn filtered(batch: RecordBatch, keep: &BooleanArray) -> Result<RecordBatch, ArrowError> {
    let predicate = FilterBuilder::new(keep).optimize().build();
    let (schema, columns, _) = batch.into_parts();
    let columns = columns
        .into_iter()
        .map(|column| predicate.filter(column.as_ref()))
        .collect::<Result<Vec<_>, _>>()?;
    let rows = RecordBatchOptions::new().with_row_count(Some(predicate.count()));
    RecordBatch::try_new_with_options(schema, columns, &rows)
}

/// Which rows of `batch` hold no value in a column routed to [`Route::DiscardRows`], and that
/// `split` keeps, where some do not.
pub(super) fn kept_by(
    batch: &RecordBatch,
    routes: &[Route],
    split: Option<BooleanArray>,
) -> Option<BooleanArray> {
    let discarding: Vec<&ArrayRef> = routes
        .iter()
        .enumerate()
        .filter(|(_, route)| **route == Route::DiscardRows)
        .map(|(index, _)| batch.column(index))
        .collect();
    // Run-end and dictionary encodings hold their nulls in their values, so only their logical
    // nulls say which rows hold a value.
    let nulls: Vec<Option<arrow_buffer::NullBuffer>> =
        discarding.iter().map(Array::logical_nulls).collect();
    let empty = |nulls: &Option<arrow_buffer::NullBuffer>| {
        nulls
            .as_ref()
            .is_some_and(|nulls| nulls.null_count() == nulls.len())
    };
    if nulls.iter().all(empty) {
        return split;
    }
    // A row is kept where every discarding column is null in it; a column without nulls holds
    // a value in every row.
    let mut kept = BooleanBuffer::new_set(batch.num_rows());
    for nulls in &nulls {
        kept = match nulls {
            Some(nulls) => &kept & &!nulls.inner(),
            None => BooleanBuffer::new_unset(batch.num_rows()),
        };
    }
    let kept = match split {
        Some(split) => &kept & split.values(),
        None => kept,
    };
    Some(BooleanArray::new(kept, None))
}

/// The rows of a batch a schema policy keeps, and what keeping them found.
pub(super) struct Kept {
    pub(super) batch: RecordBatch,
    /// Which rows those are, where it drops some.
    pub(super) keep: Option<BooleanArray>,
    /// How many rows it drops.
    pub(super) dropped: u64,
    /// Which values of its columns of JSON their own columns hold.
    pub(super) fitted: Fitted,
    /// The columns the policies decide, converted.
    pub(super) converted: Converted,
    /// The rows each incoming column drops, by its name and why.
    pub(super) discards: ColumnDiscards,
}

impl LoweringPlan {
    /// The rows of `batch` the schema policy keeps, which those are where it drops some, how many
    /// it drops and how many each column drops, which values of its columns of JSON their own
    /// columns hold, and what the columns the policies decide converted to, each of the kept
    /// rows.
    pub(super) fn kept_rows(&self, batch: RecordBatch) -> Result<Kept, ArrowError> {
        let fitted = self.splits.fit(&batch, false)?;
        let (held, mut converted, unheld) = self.held(&batch)?;
        let split = both(self.splits.kept(&fitted, batch.num_rows()), held);
        let Some(keep) = kept_by(&batch, &self.routes, split) else {
            return Ok(Kept {
                batch,
                keep: None,
                dropped: 0,
                fitted,
                converted,
                discards: ColumnDiscards::default(),
            });
        };
        let mut discards = ColumnDiscards::default();
        for (index, reason, rows) in self.dropping(&batch, &fitted, unheld) {
            self.discard(&mut discards, index, reason, (rows, 0));
        }
        let rows = batch.num_rows();
        let own = converted.release(&batch);
        let kept_batch = filtered(batch, &keep)?;
        converted.filter(&own, &kept_batch, &keep)?;
        let fitted = fitted.filtered(&keep)?;
        let dropped = (rows - kept_batch.num_rows()) as u64;
        Ok(Kept {
            batch: kept_batch,
            keep: Some(keep),
            dropped,
            fitted,
            converted,
            discards,
        })
    }
}
