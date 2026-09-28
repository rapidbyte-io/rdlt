//! Units cut into slices the engine lowers within its budget: a few bytes of encoded columns
//! may decode to far more, so a unit is lowered a slice of its rows at a time.

#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;

use rdlt_connector::{decoded_bytes, decoded_rows};

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

/// The memory `batch`'s own rows hold: a piece cut from a larger batch shares its buffers, so it
/// counts its rows alone, and an encoded column counts what it holds, not what it would decode to.
pub(super) fn held_bytes(batch: &RecordBatch) -> u64 {
    batch
        .columns()
        .iter()
        .map(|column| {
            let bytes = column
                .to_data()
                .get_slice_memory_size()
                .unwrap_or_else(|_| column.get_array_memory_size());
            u64::try_from(bytes).unwrap_or(u64::MAX)
        })
        .fold(0, u64::saturating_add)
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

/// `batch` in slices of its rows of at most `max` decoded bytes each, or of one row where a row
/// takes more; a batch within `max`, or without rows, is one slice of itself.
///
/// Rows are cut where their own values say, so a run or key skewed toward one large value is
/// cut as finely as that value needs.
fn rows(batch: &RecordBatch, max: u64) -> Vec<RecordBatch> {
    if decoded_bytes(batch) <= max {
        return vec![batch.clone()];
    }
    let mut slices = Vec::new();
    let (mut first, mut bytes) = (0, 0_u64);
    for (row, cost) in decoded_rows(batch).into_iter().enumerate() {
        if row > first && bytes.saturating_add(cost) > max {
            slices.push(batch.slice(first, row - first));
            (first, bytes) = (row, 0);
        }
        bytes = bytes.saturating_add(cost);
    }
    slices.push(batch.slice(first, batch.num_rows() - first));
    slices
}
