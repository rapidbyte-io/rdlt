//! The window of pieces a partition lowers together: reserved one by one, lowered at once, on
//! the partition's task where none copies a value and on the compute pool otherwise, then queued
//! on their lane in order.

use rdlt_connector::Permit;

use super::super::{OpenSegment, PartitionContext, PartitionJob};
use super::held::Share;
use super::queue::queue;
use super::{Room, reserve};
use crate::budget::{MemoryBudget, Reservation};
use crate::compute::Pool;
use crate::error::Error;
use crate::table::Prepared;

/// What holds a piece while it is lowered and until it is written: dropped, what it reserved is
/// released before what was charged for its unit.
pub(super) struct Lowering {
    /// What lowering the piece holds, reserved before it is lowered.
    pub(super) reserved: Reservation,
    /// A share of what was charged for its unit, so the charge holds until all of it is written.
    pub(super) unit: Share,
}

/// A piece's lowering, and the bytes of what it keeps alive beyond the parts it was lowered from.
pub(super) type Run = Box<dyn FnOnce() -> Result<(Prepared, u64), Error> + Send>;

/// Pieces reserved from a room and not yet lowered, in order: the runs are declared before what
/// holds their pieces, so they go first.
pub(super) struct Window {
    pub(super) room: Room,
    runs: Vec<Run>,
    held: Vec<Lowering>,
    /// Whether lowering a piece of the window copies values.
    copies: bool,
}

impl Window {
    pub(super) fn new(room: Room) -> Self {
        Self {
            room,
            runs: Vec::new(),
            held: Vec::new(),
            copies: false,
        }
    }

    /// Adds a piece, lowered by `run`, held by `lowering`, whose lowering `copies` values or not.
    pub(super) fn push(&mut self, run: Run, lowering: Lowering, copies: bool) {
        self.runs.push(run);
        self.held.push(lowering);
        self.copies |= copies;
    }

    pub(super) fn len(&self) -> usize {
        self.runs.len()
    }

    /// Reserves `bytes` for a piece to lower with those the window holds, where the budget has
    /// them at once; nothing where the window is empty, or the piece must wait: a partition
    /// waits only while it holds no piece it has not handed over.
    pub(super) fn reserve(&self, budget: &MemoryBudget, bytes: u64) -> Option<Reservation> {
        if self.held.is_empty() {
            return None;
        }
        self.room.try_reserve(budget, bytes)
    }

    /// Reserves `bytes` for a piece: with those the window holds where the budget has them at
    /// once, or after the window's pieces are lowered and handed to their lane, so the wait is
    /// for bytes a write releases and this partition holds none of them.
    pub(super) async fn reserved(
        &mut self,
        job: &PartitionJob,
        context: &PartitionContext,
        open: &mut OpenSegment,
        bytes: u64,
    ) -> Result<Reservation, Error> {
        if let Some(reserved) = self.reserve(&context.budget, bytes) {
            return Ok(reserved);
        }
        self.lower(job, context, open).await?;
        reserve(job, context, &self.room, bytes).await
    }

    /// The window's pieces lowered, in order, each beside what holds it: on the calling task
    /// where lowering none copies a value, on the compute pool otherwise.
    pub(super) async fn lowered(
        &mut self,
        pool: &Pool,
    ) -> Vec<(Result<(Prepared, u64), Error>, Lowering)> {
        let (runs, held) = (
            std::mem::take(&mut self.runs),
            std::mem::take(&mut self.held),
        );
        let prepared = if std::mem::take(&mut self.copies) {
            pool.run_all(runs).await
        } else {
            runs.into_iter().map(|run| run()).collect()
        };
        prepared.into_iter().zip(held).collect()
    }

    /// Lowers the window's pieces and queues them, in order: each then holds what it was lowered
    /// to beyond its unit's batches, where that is less than was reserved for lowering it.
    pub(super) async fn lower(
        &mut self,
        job: &PartitionJob,
        context: &PartitionContext,
        open: &mut OpenSegment,
    ) -> Result<(), Error> {
        let lowered: Vec<_> = self
            .lowered(&context.pool)
            .await
            .into_iter()
            .map(|(prepared, mut lowering)| {
                prepared.map(|(prepared, grown)| {
                    lowering.reserved.shrink(grown);
                    (prepared, lowering)
                })
            })
            .collect();
        for lowered in lowered {
            let (prepared, lowering) = lowered?;
            let reservation: Permit = Box::new((lowering.reserved, lowering.unit));
            queue(job, context, open, job.table, prepared, reservation).await?;
        }
        Ok(())
    }
}

/// Pieces lowered on the pool at once, at most: what bounds the memory they hold together is
/// what each reserved before it was lowered.
const LOWERING_WINDOW: usize = 8;

/// Whether a window of `pieces` holds as many as are lowered together.
pub(super) fn full(pieces: usize) -> bool {
    pieces >= LOWERING_WINDOW
}
