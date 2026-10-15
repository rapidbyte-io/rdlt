//! Writing a flush of JSON pushes: shredded into a batch a chunk and, where its batches take more
//! than the pushes were admitted for, lowered inside what is reserved with them.

use arrow_array::RecordBatch;
use bytes::Bytes;
use rdlt_connector::Permit;

use super::held::{self, Held};
use super::pieces::{Pieces, RowTooLarge};
use super::units::{self, Beside, Built, Units};
use super::{
    OpenSegment, PartitionContext, PartitionJob, Planned, Room, Schemas, lowered, planned,
    reserved_times, write,
};
use crate::budget::Shares;
use crate::error::{Error, ErrorKind};
use crate::limits::ROW_EXCEEDS_BUDGET;
use crate::normalize::PushKind;

/// Writes the JSON `pushes`, admitted with `permits`.
///
/// Batches that take no more than the pushes were admitted for are lowered as Arrow pushes are.
/// Where they take more, the partition holds that while they are lowered, so it never waits on
/// the budget until they are written:
///
/// - A normalized flush's batches are built inside a whole request, the rest of which is lent to
///   their splits.
/// - Any other flush's units are planned and cut before any is lowered, and what lowering the
///   largest piece holds is reserved beside the batches without a wait, lent to every piece.
///   Where the budget has no room for it, the batches are let go and built again once the two
///   fit in one request, which the partition waits for holding only its pushes.
///
/// # Errors
///
/// The shredder refuses the pushes, building them takes more than a request may, or one of their
/// rows takes more to lower than a request leaves beside what building them takes.
pub(super) async fn write_json(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    schemas: &mut Schemas,
    pushes: Vec<Bytes>,
    permits: Vec<Permit>,
) -> Result<(), Error> {
    let normalized = context.tables.shape(job.table).is_some();
    let mut beside = if normalized {
        Beside::Rest
    } else {
        Beside::Bytes(0)
    };
    loop {
        let Built {
            batches,
            excess,
            beyond,
            beside: reserved,
            counts,
        } = units::built(job, context, &pushes, beside).await?;
        open.shred.add(&counts);
        let reserved = match reserved {
            Some(reserved) if !normalized => reserved,
            reserved => {
                let room = reserved.map_or(Room::Budget, |reserved| Room::Lent(reserved.lend()));
                let held = held::shredded(permits, beyond, &batches);
                let units = batches.into_iter().map(|batch| vec![batch]).zip(held);
                let units = Units {
                    units: units.collect(),
                    room,
                };
                return write(job, context, open, schemas, units, PushKind::Json).await;
            }
        };
        let (mut planned, needed) = measured(job, context, open, schemas, &batches, excess).await?;
        let lent = if reserved.bytes() >= needed {
            let mut reserved = reserved;
            reserved.shrink(needed);
            reserved
        } else if let Some(lent) = context.budget.try_acquire_working(needed) {
            lent
        } else {
            // The batches are let go, their pieces never lowered, and built again inside a
            // request holding both.
            beside = Beside::Bytes(needed);
            continue;
        };
        let held = held::shredded(permits, beyond, &batches);
        if held.len() != planned.len() {
            return Err(Error::internal("a JSON flush's units and batches differ"));
        }
        for (unit, held) in planned.iter_mut().zip(held) {
            unit.held = held;
        }
        drop(pushes);
        return lowered(job, context, open, planned, Room::Lent(lent.lend())).await;
    }
}

/// `batches`, whose building took `excess` beyond their pushes, planned and cut into pieces, and
/// what lowering the largest piece holds.
async fn measured(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    schemas: &mut Schemas,
    batches: &[RecordBatch],
    excess: u64,
) -> Result<(Vec<Planned>, u64), Error> {
    let request = context.budget.shares().request;
    let shares = beside_built(context.budget.shares(), excess);
    let unheld = batches.iter().map(|batch| {
        let held = Held::of(Vec::new(), std::slice::from_ref(batch));
        (vec![batch.clone()], held)
    });
    let mut planned = planned(job, context, open, schemas, unheld.collect(), shares).await?;
    let refused = |row: RowTooLarge| row_beside_built(job, &row, excess, request);
    let largest = cut(context, &mut planned, refused).await?;
    let needed = largest.saturating_mul(reserved_times(context));
    Ok((planned, needed))
}

/// `shares` as lowering takes them beside batches whose building took `excess` beyond their
/// pushes: a request is what a request leaves beside them, and a piece no more.
fn beside_built(shares: Shares, excess: u64) -> Shares {
    let request = shares.request.saturating_sub(excess);
    Shares {
        request,
        piece: shares.piece.min(request),
        ..shares
    }
}

/// Cuts every piece of `planned` ahead, returning the bytes the largest of them takes to lower:
/// on the partition's task where cutting reads no value, and on the compute pool otherwise.
///
/// A row that takes more than a row may fails with `refused`.
async fn cut(
    context: &PartitionContext,
    planned: &mut [Planned],
    refused: impl Fn(RowTooLarge) -> Error,
) -> Result<u64, Error> {
    let mut largest = 0_u64;
    for unit in planned {
        if !unit.pieces.reads_values() {
            largest = largest.max(unit.pieces.cut_ahead().map_err(&refused)?);
            continue;
        }
        let mut pieces = std::mem::replace(&mut unit.pieces, Pieces::none());
        let cutting = move || {
            let cut = pieces.cut_ahead();
            (pieces, cut)
        };
        let (pieces, cut) = context
            .pool
            .run_all([cutting])
            .await
            .pop()
            .ok_or_else(|| Error::internal("the pool returned no pieces"))?;
        unit.pieces = pieces;
        largest = largest.max(cut.map_err(&refused)?);
    }
    Ok(largest)
}

/// The error for a row of a JSON flush that takes more to lower than a request of `request`
/// bytes leaves beside the `built` bytes building the flush's batches took beyond its pushes.
pub(super) fn row_beside_built(
    job: &PartitionJob,
    row: &RowTooLarge,
    built: u64,
    request: u64,
) -> Error {
    Error::new(
        ErrorKind::Source,
        format!(
            "stream {}: lowering one row takes more than {} bytes, more than the {} one request \
             may take of the memory budget leaves beside the {} its JSON's batches take beyond \
             their text",
            job.stream, row.limit, request, built
        ),
    )
    .with_code(ROW_EXCEEDS_BUDGET)
    .with_stream(&job.stream)
}
