//! Units cut into slices the engine lowers within its budget: a few bytes of encoded columns
//! may decode to far more, so a unit is lowered a slice of its rows at a time.

#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;
use rdlt_connector::cost::Rendering;

use super::{Held, LOWERING_WINDOW};
use crate::budget::MemoryBudget;

/// The fewest bytes a slice holds, so a small budget still lowers rows in useful batches.
const MIN_SLICE: u64 = 64 << 10;

/// The most bytes a slice expands to: a window of lowerings takes half the budget, and the
/// batches lowered from it about the other half.
pub(super) fn slice_bytes(budget: &MemoryBudget) -> u64 {
    let window = u64::try_from(2 * LOWERING_WINDOW).unwrap_or(u64::MAX);
    (budget.capacity() / window).max(MIN_SLICE)
}

/// A row that alone expands beyond the budget.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct RowTooLarge {
    /// Bytes: what the row expands to, as far as it was measured.
    pub(super) expanded: u64,
    /// Bytes: the budget.
    pub(super) budget: u64,
}

/// `units` cut into pieces expanding to at most `max` bytes as `rendering` costs them, or of one
/// row where a row expands to more.
///
/// Pieces keep the rows' order. A unit's permits stay with its last piece, so they hold until
/// all of it is written; the others are charged as they are lowered.
///
/// # Errors
///
/// A [`RowTooLarge`] for a row that alone expands beyond `budget`.
pub(super) fn sliced(
    units: Vec<(Vec<RecordBatch>, Held)>,
    rendering: &Rendering,
    max: u64,
    budget: u64,
) -> Result<Vec<(Vec<RecordBatch>, Held)>, RowTooLarge> {
    let mut pieces = Vec::with_capacity(units.len());
    for (parts, held) in units {
        let mut cut: Vec<Vec<RecordBatch>> = Vec::new();
        let mut piece = Vec::new();
        let mut bytes = 0_u64;
        for part in parts {
            for (slice, size) in rows(&part, rendering, max, budget)? {
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
        let last = cut.len().saturating_sub(1);
        let shared: Vec<Held> = (0..last).map(|_| held.piece()).collect();
        for (piece, held) in cut.into_iter().zip(shared.into_iter().chain([held])) {
            pieces.push((piece, held));
        }
    }
    Ok(pieces)
}

/// `batch` in slices of its rows expanding to at most `max` bytes each, or of one row where a row
/// expands to more, each with what it expands to; a batch within `max`, or without rows, is one
/// slice of itself.
///
/// Rows are cut where their own values say, so a run or key skewed toward one large value is
/// cut as finely as that value needs.
fn rows(
    batch: &RecordBatch,
    rendering: &Rendering,
    max: u64,
    budget: u64,
) -> Result<Vec<(RecordBatch, u64)>, RowTooLarge> {
    let mut pieces = rendering.measure(batch, max);
    // A row beyond a slice is measured against the budget, each value it names once.
    let mut alone = rendering.measure(batch, budget);
    let cuts = pieces.cuts();
    let whole = cuts.len() == 1;
    let mut slices = Vec::with_capacity(cuts.len());
    let mut first = 0;
    for end in cuts {
        let size = pieces.expanded(first..end);
        if size > max {
            let expanded = alone.expanded(first..end);
            if expanded > budget {
                return Err(RowTooLarge { expanded, budget });
            }
        }
        let slice = if whole {
            batch.clone()
        } else {
            batch.slice(first, end - first)
        };
        slices.push((slice, size));
        first = end;
    }
    Ok(slices)
}
