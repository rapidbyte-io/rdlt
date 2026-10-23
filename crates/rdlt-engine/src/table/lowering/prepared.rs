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
    /// The bytes of the allocations the batch keeps alive beyond those `unit` holds, each counted
    /// once, `unit` left as it is.
    ///
    /// The load's constant columns count for nothing: they are built once and every batch
    /// shares them.
    pub(crate) fn growth(&self, unit: &Allocations) -> u64 {
        let constants = self.view.model.columns.len();
        let mut grown = Allocations::default();
        for (index, column) in self.batch.columns().iter().enumerate() {
            if !(constants..constants + 2).contains(&index) {
                grown.add_array_beyond(column.as_ref(), unit);
            }
        }
        grown.bytes()
    }
}
