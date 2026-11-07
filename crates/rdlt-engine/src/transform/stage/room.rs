//! What the stage holds as it runs: the room a step asks for before it runs, and what a part's
//! columns hold beyond its input once it has.

use arrow_array::RecordBatch;
use arrow_schema::Schema;
use rdlt_connector::cost::arrays;

use super::FilterRule;
use super::changes::Changes;
use super::contract::Declared;
use crate::transform::{Transform, allocated_beyond, column_bytes};

/// Bytes a step may hold beside what came before it, asked before it runs: its bound, its
/// output's arrays, and the stage's own work on its rows: a filter's answer and what the stream's
/// `rule` makes of the rows it does not keep, and a change batch's flags rebuilt along the
/// step's lineage.
pub(super) fn step_room(
    (transform, declared, rule): (&dyn Transform, &Declared, FilterRule),
    data: &RecordBatch,
    changes: Option<&Changes>,
    output: &Schema,
) -> u64 {
    let rows = u64::try_from(data.num_rows()).unwrap_or(u64::MAX);
    let filtering = if transform.filters() {
        rows.saturating_add(ruled(rule, data, changes))
    } else {
        0
    };
    let relining = match (changes, &declared.lineage) {
        (Some(changes), Some(_)) => changes.relined_bytes(output.fields().len()),
        (Some(_) | None, _) => 0,
    };
    declared
        .bound
        .as_ref()
        .map_or(0, |bound| bound.of(data))
        .saturating_add(arrays(output.fields()))
        .saturating_add(filtering)
        .saturating_add(relining)
}

/// Bytes: what a filter's `rule` allocates beside its answer for the rows of `data`: the ops of a
/// change batch it rewrites, or a copy of the rows it keeps.
fn ruled(rule: FilterRule, data: &RecordBatch, changes: Option<&Changes>) -> u64 {
    match (rule, changes) {
        (FilterRule::Delete, Some(changes)) => changes.ops_bytes(),
        (FilterRule::Delete | FilterRule::Drop, changes) => {
            let columns = data.columns().iter();
            let copied = columns.map(|column| column_bytes(column.as_ref()));
            let change_columns = changes.map_or(0, Changes::bytes);
            copied.fold(change_columns, u64::saturating_add)
        }
    }
}

/// Bytes: what `data` and its `changes` hold beyond `part`, the input they were made from, their
/// buffers and the arrays of `data`.
pub(super) fn held(part: &RecordBatch, data: &RecordBatch, changes: Option<&Changes>) -> u64 {
    let set_aside = changes.map_or(0, |changes| changes.beyond(part));
    allocated_beyond(data, part)
        .saturating_add(arrays(data.schema_ref().fields()))
        .saturating_add(set_aside)
}
