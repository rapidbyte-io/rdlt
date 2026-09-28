//! Units cut into slices the engine lowers within its budget: a few bytes of encoded columns
//! may decode to far more, so a unit is lowered a slice of its rows at a time.

#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;

use rdlt_connector::decoded_bytes;

use super::{Held, LOWERING_WINDOW};
use crate::budget::MemoryBudget;

/// The fewest bytes a slice holds, so a small budget still lowers rows in useful batches.
const MIN_SLICE: u64 = 64 << 10;

/// The most decoded bytes a slice holds: a window of lowerings takes half the budget, and the
/// batches lowered from it about the other half.
pub(super) fn slice_bytes(budget: &MemoryBudget) -> u64 {
    let window = u64::try_from(2 * LOWERING_WINDOW).unwrap_or(u64::MAX);
    (budget.capacity() / window).max(MIN_SLICE)
}

/// `units` cut into pieces of at most `max` decoded bytes, or of one row where a row is larger.
///
/// Pieces keep the rows' order. A unit's permits stay with its last piece, so they hold until
/// all of it is written; the others are charged as they are lowered.
pub(super) fn sliced(
    units: Vec<(Vec<RecordBatch>, Held)>,
    max: u64,
) -> Vec<(Vec<RecordBatch>, Held)> {
    let mut pieces = Vec::with_capacity(units.len());
    for (parts, held) in units {
        let mut cut: Vec<Vec<RecordBatch>> = Vec::new();
        let mut piece = Vec::new();
        let mut bytes = 0_u64;
        for part in parts {
            for slice in rows(&part, max) {
                let size = decoded_bytes(&slice);
                if !piece.is_empty() && bytes.saturating_add(size) > max {
                    cut.push(std::mem::take(&mut piece));
                    bytes = 0;
                }
                bytes = bytes.saturating_add(size);
                piece.push(slice);
            }
        }
        if !piece.is_empty() {
            cut.push(piece);
        }
        let mut held = Some(held);
        let last = cut.len().saturating_sub(1);
        for (index, piece) in cut.into_iter().enumerate() {
            let permits = if index == last { held.take() } else { None };
            pieces.push((
                piece,
                permits.unwrap_or(Held {
                    permits: Vec::new(),
                    bytes: 0,
                }),
            ));
        }
    }
    pieces
}

/// `batch` in slices of its rows of at most `max` decoded bytes each, or of one row; a batch
/// within `max`, or without rows, is one slice of itself.
fn rows(batch: &RecordBatch, max: u64) -> Vec<RecordBatch> {
    let count = batch.num_rows();
    let rows = u64::try_from(count.max(1)).unwrap_or(u64::MAX);
    let per_row = decoded_bytes(batch).div_ceil(rows).max(1);
    let step = usize::try_from(max / per_row).unwrap_or(usize::MAX).max(1);
    (0..count.max(1))
        .step_by(step)
        .map(|first| batch.slice(first, step.min(count - first)))
        .collect()
}
