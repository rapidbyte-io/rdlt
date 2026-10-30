//! What a plan's schema policy discards of a batch, counted by the table's incoming column and
//! why: the rows each column drops and the values of each it nulls or leaves out.

use std::sync::Arc;

use arrow_array::{Array, RecordBatch};

use super::split::Fitted;
use super::{LoweringPlan, Prepared};
use crate::report::{ColumnDiscards, DiscardReason};
use crate::table::resolve::{Rest, Route};

impl LoweringPlan {
    /// A batch of no rows, every row of which the schema policy dropped: `dropped` of them, for
    /// `discards`.
    pub(super) fn emptied(&self, dropped: u64, discards: ColumnDiscards) -> Prepared {
        Prepared {
            batch: RecordBatch::new_empty(Arc::clone(&self.view.schema)),
            view: Arc::clone(&self.view),
            discarded_rows: dropped,
            discarded_values: 0,
            discards,
        }
    }

    /// How many values of `batch` the schema policy discards, each counted in `discards` by its
    /// column and why: of the columns it discards as a change and of its columns of JSON, which
    /// `fitted` read, those their own columns do not hold, and the values beyond their column
    /// `nulled` counts.
    pub(super) fn discarded(
        &self,
        batch: &RecordBatch,
        fitted: &Fitted,
        nulled: &[(usize, u64)],
        discards: &mut ColumnDiscards,
    ) -> u64 {
        let changing = self.discarded_values(batch, fitted);
        let mut values = 0_u64;
        for (counts, reason) in [
            (changing.as_slice(), DiscardReason::SchemaChange),
            (nulled, DiscardReason::OutOfRange),
        ] {
            for (index, count) in counts {
                values = values.saturating_add(*count);
                self.discard(discards, *index, reason, (0, *count));
            }
        }
        values
    }

    /// How many values of each incoming column of `batch` the schema policy discards as a schema
    /// change, by its place: of the columns it discards, and of its columns of JSON, which
    /// `fitted` read, those their own columns do not hold.
    fn discarded_values(&self, batch: &RecordBatch, fitted: &Fitted) -> Vec<(usize, u64)> {
        let mut values = self.splits.unfitted(batch, fitted, Rest::DiscardValues);
        for index in &self.discarded {
            let column = batch.column(*index);
            let held = (column.len() - column.logical_null_count()) as u64;
            if held > 0 {
                values.push((*index, held));
            }
        }
        values
    }

    /// Counts in `discards` the rows dropped and values nulled of the incoming column at `index`,
    /// `counts`, for `reason`, where the policy may discard the column's values.
    pub(super) fn discard(
        &self,
        discards: &mut ColumnDiscards,
        index: usize,
        reason: DiscardReason,
        counts: (u64, u64),
    ) {
        if let Some(column) = self.discarding.get(&index) {
            discards.add((&self.view.table.name, column), reason, counts);
        }
    }

    /// How many rows of `batch` each incoming column drops, by its place, and why: those holding
    /// a value of a change the policy drops rows for, of a column of JSON its own column does not
    /// hold, as `fitted` says, and the rows `unheld` says hold a value their column cannot.
    pub(super) fn dropping(
        &self,
        batch: &RecordBatch,
        fitted: &Fitted,
        unheld: Vec<(usize, u64)>,
    ) -> Vec<(usize, DiscardReason, u64)> {
        let change = DiscardReason::SchemaChange;
        let mut dropping: Vec<(usize, DiscardReason, u64)> = self
            .splits
            .unfitted(batch, fitted, Rest::DiscardRows)
            .into_iter()
            .map(|(index, rows)| (index, change, rows))
            .collect();
        let routes = self.routes.iter().enumerate();
        for (index, _) in routes.filter(|(_, route)| **route == Route::DiscardRows) {
            let column = batch.column(index);
            let held = (column.len() - column.logical_null_count()) as u64;
            if held > 0 {
                dropping.push((index, change, held));
            }
        }
        // A column refused with no row named is refused whole with its batch, so a count of
        // none is never reported.
        let beyond = unheld.into_iter();
        dropping.extend(beyond.map(|(index, rows)| (index, DiscardReason::OutOfRange, rows)));
        dropping
    }
}
