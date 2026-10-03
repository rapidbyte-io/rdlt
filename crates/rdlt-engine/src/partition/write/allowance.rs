//! The allowance a normalized piece lowers its parts in: bytes it reserved with its split, in
//! one request, which its parts' pieces take in turn and give back as they are written.
//!
//! A partition that needs more of its allowance waits for its own pieces to be written, which
//! takes no budget: it never waits on the budget while it holds what it reserved.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::cost::Rendering;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use super::normalized::PlannedParts;
use super::pieces::{Lowered, Pieces, RowTooLarge};
use super::reserved_times;
use crate::budget::MemoryBudget;
use crate::error::Error;
use crate::limits::MAX_PIECE_BYTES;
use crate::normalize::{Lineage, Parent};
use crate::partition::PartitionContext;
use crate::table::LoweringPlan;

/// Bytes: what one permit of an allowance stands for.
const UNIT: u64 = 1024;

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
    /// Bytes: the most lowering one row may take, in whole permits: what the parts' lowering
    /// may take, divided by the times it is held.
    row: u64,
    /// Bytes: the most lowering one piece holds, but for a row that alone takes more.
    piece: u64,
    /// How many times what lowering a piece takes is held for it.
    times: u32,
}

/// What a piece's parts are lowered in.
pub(super) struct Allowance {
    /// Bytes: the allowance, as its piece reserved it.
    pub(super) bytes: u64,
    times: u32,
    permits: Arc<Semaphore>,
}

/// What a part's piece took of its allowance: for the piece on its lane, and for its frame in
/// the log where the load keeps one.
pub(super) struct Taken {
    pub(super) piece: OwnedSemaphorePermit,
    pub(super) frame: Option<OwnedSemaphorePermit>,
}

/// `bytes` in permits, rounded up.
fn permits(bytes: u64) -> u32 {
    u32::try_from(bytes.div_ceil(UNIT)).unwrap_or(u32::MAX)
}

impl Cutter {
    /// A cutter of parts whose lowering may take `available` bytes, in pieces of `piece` bytes,
    /// each held `times` times, a piece and a row within what a text array's offsets reach.
    pub(super) fn new(available: u64, piece: u64, times: u32) -> Self {
        let each = u64::from(times.max(1));
        let bound = |bytes: u64| (bytes / each).min(MAX_PIECE_BYTES);
        Self {
            row: bound(available) / UNIT * UNIT,
            piece: bound(piece).max(UNIT) / UNIT * UNIT,
            times: times.max(1),
        }
    }

    /// Bytes: the most a piece and a row may take.
    #[cfg(test)]
    pub(super) fn bounds(&self) -> (u64, u64) {
        (self.piece, self.row)
    }

    /// A cutter of parts whose lowering may take `available` bytes of `context`'s budget.
    pub(super) fn within(context: &PartitionContext, available: u64) -> Self {
        let times = u32::try_from(reserved_times(context)).unwrap_or(u32::MAX);
        Self::new(available, context.budget.shares().piece, times)
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
                max: self.piece.min(self.row),
                limit: self.row,
            };
            let mut pieces = Pieces::new(vec![part.batch.clone()], lowered);
            let mut first = 0;
            while let Some(piece) = pieces.next()? {
                let batch = part.batch.slice(first, piece.rows);
                cut.push(PartPiece {
                    table,
                    plan: Arc::clone(&plan),
                    batch,
                    lineage: rows(&part.lineage, first, piece.rows),
                    bytes: piece.bytes,
                });
                first += piece.rows;
            }
        }
        Ok(cut)
    }

    /// The allowance `pieces` are lowered in: what the largest of them takes, or two ordinary
    /// pieces where the rows may take as much and the pieces come to it, so one is lowered while
    /// another is written.
    ///
    /// Every piece fits it whole, and it is never more than the parts' lowering may take.
    pub(super) fn for_pieces(self, pieces: &[PartPiece]) -> Allowance {
        let (mut largest, mut all) = (0_u32, 0_u32);
        for piece in pieces {
            let piece = permits(piece.bytes);
            largest = largest.max(piece);
            all = all.saturating_add(piece);
        }
        let two = permits(2 * self.piece).min(permits(self.row));
        let each = largest.max(all.min(two));
        let total = each.saturating_mul(self.times);
        Allowance {
            bytes: u64::from(total) * UNIT,
            times: self.times,
            permits: Arc::new(Semaphore::new(total as usize)),
        }
    }
}

impl Allowance {
    /// How many permits lowering `piece` takes: for itself, and for all it is held as.
    fn permits(&self, piece: &PartPiece) -> (u32, u32) {
        let each = permits(piece.bytes);
        (each, each.saturating_mul(self.times))
    }

    /// What `taken`, a piece's permits, `each` of them for each time it is held, holds apart.
    fn taken(&self, mut taken: OwnedSemaphorePermit, each: u32) -> Taken {
        let frame = (self.times > 1)
            .then(|| taken.split(each as usize))
            .flatten();
        Taken {
            piece: taken,
            frame,
        }
    }

    /// Takes what lowering `piece` holds of the allowance where it has that at once.
    pub(super) fn try_take(&self, piece: &PartPiece) -> Option<Taken> {
        let (each, all) = self.permits(piece);
        let taken = Arc::clone(&self.permits).try_acquire_many_owned(all).ok()?;
        Some(self.taken(taken, each))
    }

    /// Takes what lowering `piece` holds of the allowance, waiting for the pieces before it to
    /// be written, which this presses their lanes to do through `budget`; the wait ends when
    /// `cancel` fires.
    pub(super) async fn take(
        &self,
        budget: &MemoryBudget,
        cancel: &CancellationToken,
        piece: &PartPiece,
    ) -> Result<Taken, Error> {
        if let Some(taken) = self.try_take(piece) {
            return Ok(taken);
        }
        let (each, all) = self.permits(piece);
        let closed = |_| Error::internal("an allowance closed while its piece waited");
        let _pressing = budget.pressing();
        let taken = tokio::select! {
            biased;
            () = cancel.cancelled() => {
                return Err(Error::cancelled("the attempt was cancelled"));
            }
            taken = Arc::clone(&self.permits).acquire_many_owned(all) => taken.map_err(closed)?,
        };
        Ok(self.taken(taken, each))
    }
}

/// The `count` rows of `lineage` from `first`.
fn rows(lineage: &Lineage, first: usize, count: usize) -> Lineage {
    Lineage {
        id: lineage.id.slice(first, count),
        root_row: lineage.root_row.slice(first, count),
        parent: lineage.parent.as_ref().map(|parent| Parent {
            id: parent.id.slice(first, count),
            root: parent.root.slice(first, count),
            idx: parent.idx.slice(first, count),
            path: parent.path.clone(),
            row: parent.row.slice(first, count),
        }),
    }
}
