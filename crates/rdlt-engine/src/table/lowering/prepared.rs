//! A batch lowered for its table, and what it keeps alive.

use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::cost::Allocations;

use crate::table::TableView;

/// A batch ready for its table, and what the schema policy discarded from it.
#[derive(Debug)]
pub(crate) struct Prepared {
    pub(crate) batch: RecordBatch,
    /// The view the batch was lowered for, which names its schema version.
    pub(crate) view: Arc<TableView>,
    /// Rows dropped because they carried a discarded change.
    pub(crate) discarded_rows: u64,
    /// Values nulled because they carried a discarded change.
    pub(crate) discarded_values: u64,
}

impl Prepared {
    /// The bytes of the allocations the batch keeps alive beyond those `held` holds, which then
    /// holds them too.
    ///
    /// The load's constant columns count for nothing: they are built once and every batch
    /// shares them.
    pub(crate) fn growth(&self, held: &mut Allocations) -> u64 {
        let constants = self.view.model.columns.len();
        self.batch
            .columns()
            .iter()
            .enumerate()
            .filter(|(column, _)| !(constants..constants + 2).contains(column))
            .map(|(_, column)| held.add_array(column.as_ref()))
            .fold(0, u64::saturating_add)
    }
}
