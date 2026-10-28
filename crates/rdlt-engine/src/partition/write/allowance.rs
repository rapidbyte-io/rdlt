//! How a normalized piece's parts are cut, and the allowance they are lowered in: bytes the
//! piece reserved with its split, in one request, lent to its parts' pieces, which take what
//! lowering each holds in turn and give it back as they are written.
//!
//! A partition that needs more of its allowance waits for its own pieces to be written, which
//! takes no budget: it never waits on the budget while it holds what it reserved.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::cost::{Rendering, arrays};

use super::held::Held;
use super::normalized::PlannedParts;
use super::pieces::{Lowered, Pieces, RowTooLarge};
use crate::limits::MAX_PIECE_BYTES;
use crate::normalize::Lineage;
use crate::partition::PartitionContext;
use crate::table::LoweringPlan;

/// Rows of one part to lower together, and what lowering them holds at most.
pub(super) struct PartPiece {
    pub(super) table: usize,
    pub(super) plan: Arc<LoweringPlan>,
    pub(super) batch: RecordBatch,
    pub(super) lineage: Lineage,
    /// Bytes: what lowering the rows holds at once, as they were measured.
    pub(super) bytes: u64,
}

/// How a piece's parts are cut: by what its allowance may hold.
#[derive(Clone, Copy, Debug)]
pub(super) struct Cutter {
    /// Bytes: the most lowering one row may take: what the parts' lowering may take.
    row: u64,
    /// Bytes: the most lowering one piece holds, but for a row that alone takes more.
    piece: u64,
}

impl Cutter {
    /// A cutter of parts whose lowering may take `available` bytes, in pieces of `piece` bytes,
    /// a piece and a row within what a text array's offsets reach.
    pub(super) fn new(available: u64, piece: u64) -> Self {
        Self {
            row: available.min(MAX_PIECE_BYTES),
            piece: piece.min(MAX_PIECE_BYTES),
        }
    }

    /// Bytes: the most a piece and a row may take.
    #[cfg(test)]
    pub(super) fn bounds(&self) -> (u64, u64) {
        (self.piece, self.row)
    }

    /// A cutter of parts whose lowering may take `available` bytes of `context`'s budget.
    pub(super) fn within(context: &PartitionContext, available: u64) -> Self {
        Self::new(available, context.budget.shares().piece)
    }

    /// Each of `unit`'s parts cut by what lowering it into its own table holds, as `rendering`
    /// measures it: its columns as the table stores them, the nulls of the columns it lacks and
    /// the metadata lowering adds.
    ///
    /// # Errors
    ///
    /// A [`RowTooLarge`] for a row that alone takes more than the allowance.
    pub(super) fn cut(
        self,
        rendering: &Rendering,
        unit: PlannedParts,
    ) -> Result<Vec<PartPiece>, RowTooLarge> {
        let mut cut = Vec::new();
        for (table, part, plan) in unit {
            let lowered = Lowered {
                rendering: rendering.clone(),
                stored: plan.stored(),
                row: plan.row_bytes(),
                item: 0,
                piece: plan.piece_bytes(),
                part: arrays(part.batch.schema_ref().fields()),
                max: self.piece.min(self.row),
                limit: self.row,
            };
            let mut pieces = Pieces::new(Held::of(vec![part.batch.clone()], Vec::new()), lowered);
            let mut first = 0;
            while let Some(piece) = pieces.next()? {
                let batch = part.batch.slice(first, piece.rows);
                cut.push(PartPiece {
                    table,
                    plan: Arc::clone(&plan),
                    batch,
                    lineage: part.lineage.slice(first, piece.rows),
                    bytes: piece.bytes,
                });
                first += piece.rows;
            }
        }
        Ok(cut)
    }

    /// Bytes: the allowance `pieces` are lowered in, what the largest of them takes, or two
    /// ordinary pieces where the rows may take as much and the pieces come to it, so one is
    /// lowered while another is written.
    ///
    /// Every piece fits it whole, and it is never more than the parts' lowering may take.
    pub(super) fn allowance(self, pieces: &[PartPiece]) -> u64 {
        let (mut largest, mut all) = (0_u64, 0_u64);
        for piece in pieces {
            largest = largest.max(piece.bytes);
            all = all.saturating_add(piece.bytes);
        }
        let two = self.piece.saturating_mul(2).min(self.row);
        largest.max(all.min(two))
    }
}
