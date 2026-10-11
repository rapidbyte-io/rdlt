//! Admits a batch a source pushes: one the engine holds as it is, within the limits a frame of it
//! would meet on the wire and within what it may keep alive.

#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema};
use rdlt_wire::limits::{FRAME_BYTES, count};
use rdlt_wire::{Limit, Limits, Refusal, Weigher, Weight, child_fields};

use crate::cost::Allocations;

/// Rows: how many are weighed between two checks of the limits.
const STRETCH: usize = 1024;

/// Checks `batch` against `limits`, those on a batch a source pushes.
///
/// Its rows, its columns and how deep they nest come first, whoever receives it. A batch held
/// `whole`, as the engine holds one pushed in its own process, is then weighed as the wire weighs
/// a frame holding its rows: its values, the bytes its views name and its bytes, each
/// dictionary's values weighed as the frame of their own they would go in. Last, the bytes it
/// keeps alive, its dictionaries among them: no decoder keeps those beside it, so the limit on
/// what a read's decoder holds of them is not this batch's. A batch a served read sends is cut
/// to those limits as it is sent instead.
///
/// # Errors
///
/// The first [`Refusal`] found; weighing stops with the stretch of rows that passed it.
pub(super) fn admit(batch: &RecordBatch, whole: bool, limits: &Limits) -> Result<(), Refusal> {
    let rows = batch.num_rows();
    Limits::admit(Limit::BatchRows, limits.batch_rows, count(rows))?;
    columns(&batch.schema(), limits)?;
    if !whole {
        return Ok(());
    }
    let mut weigher = Weigher::new(batch);
    for dictionary in weigher.dictionaries() {
        within(&dictionary, limits)?;
    }
    weigher.begin();
    let mut weight = Weight::default();
    for start in (0..rows).step_by(STRETCH) {
        weight += weigher.weigh_rows(start..start.saturating_add(STRETCH));
        within(&weight, limits)?;
    }
    let held = Allocations::of(batch).bytes();
    Limits::admit(Limit::BatchBytes, limits.frame_bytes, held)
}

/// Checks what a frame would hold, `weight`, against `limits`.
///
/// Its bytes are held to the most any frame holds, not to `limits`' frame: what a batch held
/// whole takes of whoever holds it is what it keeps alive, which is checked apart, and what its
/// rows become is lowered a piece at a time.
fn within(weight: &Weight, limits: &Limits) -> Result<(), Refusal> {
    Limits::admit(Limit::BatchValues, limits.batch_values, weight.values)?;
    Limits::admit(Limit::ViewBytes, FRAME_BYTES, weight.view_bytes)?;
    Limits::admit(Limit::BatchBytes, FRAME_BYTES, weight.frame_bytes())
}

/// Counts the columns of `schema`, nested ones too, and how deep they nest, a top-level column
/// being the first level; a type nested beyond the limit is not looked into.
fn columns(schema: &Schema, limits: &Limits) -> Result<(), Refusal> {
    let mut columns = 0_u64;
    let mut pending: Vec<(&Field, u64)> = schema
        .fields()
        .iter()
        .map(|field| (field.as_ref(), 1))
        .collect();
    while let Some((field, depth)) = pending.pop() {
        Limits::admit(Limit::NestingDepth, limits.nesting_depth, depth)?;
        columns += 1;
        Limits::admit(Limit::SchemaColumns, limits.schema_columns, columns)?;
        pending.extend(child_fields(field.data_type()).map(|child| (child, depth + 1)));
    }
    Ok(())
}
