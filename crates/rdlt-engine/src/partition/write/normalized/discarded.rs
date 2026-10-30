//! What planning a normalized unit's parts discards: the values of new arrays its stream
//! discards, by the table and column that would hold them, and the rows that go with dropped
//! parents.

use std::sync::Arc;

use super::PlannedParts;
use crate::normalize::Part;
use crate::report::{ColumnDiscards, DiscardReason};

/// What planning a unit's parts discarded: the values of new arrays, by the table and column
/// they would have been in, and the rows that went with dropped parents.
#[derive(Default)]
pub(super) struct Discarded {
    pub(super) values: u64,
    pub(super) rows: u64,
    pub(super) columns: ColumnDiscards,
}

impl Discarded {
    /// Counts the values of `part`, a new array its stream discards, under the table of `planned`
    /// that would hold it.
    pub(super) fn array(&mut self, planned: &PlannedParts, part: &Part) {
        let values = part.batch.num_rows() as u64;
        self.values += values;
        if let Some((table, column)) = holding(planned, &part.path) {
            let change = DiscardReason::SchemaChange;
            self.columns.add((&table, &column), change, (0, values));
        }
    }
}

/// The table that would hold the array at `path`, of the parts `planned`, and its column there:
/// the table of the planned part whose path is the longest before it, and the path below that,
/// its segments joined by dots; the array, discarded, is not among them.
fn holding(planned: &PlannedParts, path: &[Arc<str>]) -> Option<(Arc<str>, Arc<str>)> {
    let parent = planned
        .iter()
        .filter(|(_, part, _)| path.starts_with(&part.path))
        .max_by_key(|(_, part, _)| part.path.len())?;
    let (_, part, plan) = parent;
    let column = path[part.path.len()..].join(".");
    Some((Arc::clone(&plan.view().table.name), Arc::from(column)))
}
