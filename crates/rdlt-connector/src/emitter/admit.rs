//! Admits a batch a source pushes: one the engine holds as it is, within the limits a frame of it
//! would meet on the wire and within what it may keep alive.

#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema};
use rdlt_wire::limits::count;
use rdlt_wire::{Limits, Weigher, Weight};

use crate::cost::Allocations;
use crate::error::LimitExceeded;
use crate::limits::{MAX_BATCH_BYTES, MAX_VIEW_BYTES};

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
/// The first [`LimitExceeded`] found; weighing stops with the stretch of rows that passed it.
pub(super) fn admit(
    batch: &RecordBatch,
    whole: bool,
    limits: &Limits,
) -> Result<(), LimitExceeded> {
    let rows = batch.num_rows();
    check("batch rows", count(rows), limits.batch_rows)?;
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
    check("batch bytes", held, limits.frame_bytes)
}

/// Checks what a frame would hold, `weight`, against `limits`.
///
/// Its bytes are held to the most any frame holds, not to `limits`' frame: what a batch held
/// whole takes of whoever holds it is what it keeps alive, which is checked apart, and what its
/// rows become is lowered a piece at a time.
fn within(weight: &Weight, limits: &Limits) -> Result<(), LimitExceeded> {
    check("batch values", weight.values, limits.batch_values)?;
    check("view bytes", weight.view_bytes, MAX_VIEW_BYTES)?;
    check("batch bytes", weight.frame_bytes(), MAX_BATCH_BYTES)
}

fn check(name: &'static str, actual: u64, limit: u64) -> Result<(), LimitExceeded> {
    if actual > limit {
        return Err(LimitExceeded {
            name,
            limit,
            actual,
        });
    }
    Ok(())
}

/// Counts the columns of `schema`, nested ones too, and how deep they nest, a top-level column
/// being the first level; a type nested beyond the limit is not looked into.
fn columns(schema: &Schema, limits: &Limits) -> Result<(), LimitExceeded> {
    let mut columns = 0_u64;
    let mut pending: Vec<(&DataType, u64)> = schema
        .fields()
        .iter()
        .map(|field| (field.data_type(), 1))
        .collect();
    while let Some((data_type, depth)) = pending.pop() {
        check("nesting depth", depth, limits.nesting_depth)?;
        columns += 1;
        check("batch columns", columns, limits.schema_columns)?;
        let below = depth + 1;
        match data_type {
            DataType::List(item)
            | DataType::LargeList(item)
            | DataType::ListView(item)
            | DataType::LargeListView(item)
            | DataType::FixedSizeList(item, _)
            | DataType::Map(item, _) => pending.push((item.data_type(), below)),
            DataType::Struct(fields) => {
                pending.extend(fields.iter().map(|field| (field.data_type(), below)));
            }
            DataType::Union(fields, _) => {
                pending.extend(fields.iter().map(|(_, field)| (field.data_type(), below)));
            }
            DataType::RunEndEncoded(ends, values) => {
                pending.push((ends.data_type(), below));
                pending.push((values.data_type(), below));
            }
            // A dictionary's values are its column, encoded.
            DataType::Dictionary(_, values) => {
                columns -= 1;
                pending.push((values, depth));
            }
            _ => {}
        }
    }
    Ok(())
}
