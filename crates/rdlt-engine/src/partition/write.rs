//! Writing what a partition gathered: shredding JSON, fitting each batch to its table, lowering
//! it a piece at a time and queueing each piece on its lane with the memory it holds.

mod allowance;
mod changes;
mod held;
mod json;
mod normalized;
mod pieces;
mod queue;
mod refused;
mod schemas;
#[cfg(test)]
mod tests;
mod units;
mod window;

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::ColumnPath;
use rdlt_connector::cost::{Rendering, arrays};
use tokio_util::sync::CancellationToken;

use self::changes::{Ignored, split_changes};
use self::held::Held;
use self::pieces::{Lowered, Piece, Pieces, RowTooLarge};
use self::queue::queue;
use self::refused::{RequestHeld, row_beside, row_too_large, schema_invalid};
pub(super) use self::schemas::Schemas;
use self::units::Units;
use self::window::{Lowering, Window, full};
use super::coalesce::{Flushed, Unit};
use super::{ChangeMode, OpenSegment, PartitionContext, PartitionJob};
use crate::budget::{Denied, Lent, MemoryBudget, Reservation, Shares, TooLarge};
use crate::cost::CHANGE_ROW;
use crate::error::Error;
use crate::limits::MAX_PIECE_BYTES;
use crate::normalize::PushKind;
use crate::table::{Incoming, LoweringPlan, Prepared, Stamp};

/// Writes pushes gathered together: Arrow batches as one batch, JSON shredded into batches.
pub(super) async fn write_flushed(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    schemas: &mut Schemas,
    flushed: Flushed,
) -> Result<(), Error> {
    let Flushed { unit, permits, .. } = flushed;
    // The pushes are held with their permits again before anything waits.
    match unit {
        Unit::Arrow(batches) => {
            let units = units::arrow(job, context, Held::of(batches, permits)).await?;
            write(job, context, open, schemas, units, PushKind::Arrow).await
        }
        Unit::Json(text) => {
            let pushes = json::Pushes { text, permits };
            json::write_json(job, context, open, schemas, pushes).await
        }
    }
}

/// Where a flush's lowering reserves its bytes: the budget, or, for a JSON flush whose batches
/// hold bytes of lowering, what was reserved with them and is lent to their lowering, so the
/// partition never waits on the budget while it holds those batches.
pub(super) enum Room {
    Budget,
    Lent(Lent),
}

impl Room {
    /// The shares of `budget` as the flush's lowering takes them: a request is at most what is
    /// lent, and a piece the part of it a piece is of a request, so what a piece's split and its
    /// planned parts make leaves their lowering as large a part of what is lent.
    fn shares(&self, budget: &MemoryBudget) -> Shares {
        let shares = budget.shares();
        match self {
            Self::Budget => shares,
            Self::Lent(lent) => {
                let request = lent.bytes();
                let part = u128::from(shares.piece) * u128::from(request);
                let piece = part / u128::from(shares.request.max(1));
                Shares {
                    request,
                    piece: u64::try_from(piece)
                        .expect("a piece is at most a request, so its part of what is lent is too"),
                    ..shares
                }
            }
        }
    }

    /// Lends no more: what is lent and no piece holds goes back to the budget, and each piece's
    /// part as it is written.
    fn close(&self) {
        if let Self::Lent(lent) = self {
            lent.close();
        }
    }

    /// The error for a row that alone takes more to lower than a request of the room leaves it
    /// beside the `held` bytes kept for its piece's split.
    fn refusal(
        &self,
        job: &PartitionJob,
        budget: &MemoryBudget,
        row: &RowTooLarge,
        held: u64,
    ) -> Error {
        let request = budget.shares().request;
        let built = match self {
            Self::Budget => 0,
            Self::Lent(lent) => request.saturating_sub(lent.bytes()),
        };
        let beside = RequestHeld {
            request,
            built,
            held,
        };
        row_beside(job, row, beside)
    }

    /// Reserves `bytes` where they are free now, of what is lent or else of the budget; nothing
    /// otherwise.
    fn try_reserve(&self, budget: &MemoryBudget, bytes: u64) -> Option<Reservation> {
        match self {
            Self::Budget => budget.try_acquire_working(bytes),
            Self::Lent(lent) => lent
                .try_take(bytes)
                .or_else(|| budget.try_acquire_working(bytes)),
        }
    }
}

/// Reserves `bytes` from `room` for the lowering this partition does next, all of them in one
/// request, so it waits only for what other lowerings hold, or for its own pieces to be written
/// where they are lent; the wait ends when the attempt is cancelled, and on the budget at its
/// deadline.
async fn reserve(
    job: &PartitionJob,
    context: &PartitionContext,
    room: &Room,
    bytes: u64,
) -> Result<Reservation, Error> {
    let too_large = |large: TooLarge| {
        let row = RowTooLarge {
            expanded: large.asked,
            limit: large.limit,
        };
        row_too_large(job, &row)
    };
    let Room::Lent(lent) = room else {
        return reserving(job, context, bytes, too_large).await;
    };
    if let Some(reserved) = room.try_reserve(&context.budget, bytes) {
        return Ok(reserved);
    }
    taking(&context.cancel, lent, bytes, too_large).await
}

/// Takes `bytes` of `lent`, waiting for its parts to be written unless `cancel` fires first:
/// `too_large` is the error for more than is lent.
async fn taking(
    cancel: &CancellationToken,
    lent: &Lent,
    bytes: u64,
    too_large: impl FnOnce(TooLarge) -> Error,
) -> Result<Reservation, Error> {
    let taken = tokio::select! {
        biased;
        () = cancel.cancelled() => {
            return Err(Error::cancelled("the attempt was cancelled"));
        }
        taken = lent.take(bytes) => taken,
    };
    taken.map_err(too_large)
}

/// Reserves `bytes` of what lowering holds, as [`reserve`] does: `too_large` is the error for
/// more than one request may take.
async fn reserving(
    job: &PartitionJob,
    context: &PartitionContext,
    bytes: u64,
    too_large: impl FnOnce(TooLarge) -> Error,
) -> Result<Reservation, Error> {
    let reserved = tokio::select! {
        biased;
        () = context.cancel.cancelled() => {
            return Err(Error::cancelled("the attempt was cancelled"));
        }
        reserved = context.budget.acquire_working(bytes) => reserved,
    };
    reserved.map_err(|denied| match denied {
        Denied::Exhausted(exhausted) => Error::memory(exhausted).with_stream(&job.stream),
        Denied::TooLarge(large) => too_large(large),
    })
}

/// Lowers `units`, each some batches of one schema and the memory they hold, written from pushes
/// of `kind`, into the partition's table and queues them on its lane, in order.
///
/// - Each unit's plan is found in order, since finding it may change the table.
/// - The unit is then cut into pieces by what lowering each holds at once as its table stores
///   it: its columns converted to the table's types and rendered as the destination stores
///   them, the columns it holds nothing in, and the metadata lowering adds. A row that alone
///   takes more than one request of the units' room may fails the write.
/// - Each piece reserves that from the room before it is lowered. The partition waits for a
///   piece only while it holds none it has not handed to its lane; pieces the room has space
///   for at once are lowered together, [`window::LOWERING_WINDOW`] at most.
/// - A cut that reads no value, and a window of pieces whose lowering copies none, run on the
///   partition's task: their steps grow with the columns alone, fewer than a trip to the
///   compute pool takes. Every other cut and window runs on the pool.
///
/// The units are one flush, which chunks and pieces cut however their sizes fall, so its integers
/// are judged together: where it was cut decides no column's type.
async fn write(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    schemas: &mut Schemas,
    units: Units,
    kind: PushKind,
) -> Result<(), Error> {
    if let Some(shape) = context.tables.shape(job.table) {
        let parts = &mut schemas.parts;
        return normalized::write_normalized(job, context, open, parts, units, (&shape, kind))
            .await;
    }
    let Units { units, room } = units;
    let shares = room.shares(&context.budget);
    let planned = planned(job, context, open, schemas, units, shares).await?;
    lowered(job, context, open, planned, room).await
}

/// A unit whose plan is found, to be cut into pieces as its table stores it.
struct Planned {
    plan: Arc<LoweringPlan>,
    pieces: Pieces,
    /// Whether the plan hands the unit's columns over as they arrive, which its pieces, slices
    /// of batches of one schema, share.
    handed: bool,
}

/// `units` judged and their plans found in order, since finding one may change the table, each to
/// be cut by what lowering it takes as its table stores it within `shares`.
async fn planned(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    schemas: &mut Schemas,
    units: Vec<Held>,
    shares: Shares,
) -> Result<Vec<Planned>, Error> {
    let judged = judged(job, open, schemas, units)?;
    let rounding: BTreeSet<ColumnPath> = judged
        .iter()
        .flat_map(|unit| unit.incoming.rounding.iter().cloned())
        .collect();
    let (max, limit) = piece_bounds(shares);
    let mut planned = Vec::with_capacity(judged.len());
    for mut unit in judged {
        unit.incoming.rounding.clone_from(&rounding);
        let plan = context.tables.plan(job.table, unit.incoming).await?;
        let batch = &unit.held.batches[0];
        let lowered = measured(&plan, &context.rendering, batch, job.changes, (max, limit));
        let handed = handed(&plan, batch, job.changes);
        let pieces = Pieces::new(unit.held, lowered);
        planned.push(Planned {
            plan,
            pieces,
            handed,
        });
    }
    Ok(planned)
}

/// How the batches of a unit like `batch`, read as `mode` says, are measured as `plan` lowers
/// them for a destination rendering as `rendering` says, cut into pieces of `max` bytes at most,
/// each row within `limit`.
fn measured(
    plan: &LoweringPlan,
    rendering: &Rendering,
    batch: &RecordBatch,
    mode: Option<ChangeMode>,
    (max, limit): (u64, u64),
) -> Lowered {
    let (stored, changes) = match mode {
        Some(_) => (plan.aligned(batch), CHANGE_ROW),
        None => (plan.stored(), 0),
    };
    Lowered {
        rendering: rendering.clone(),
        stored,
        row: plan.row_bytes().saturating_add(changes),
        item: 0,
        piece: plan.piece_bytes(),
        part: arrays(batch.schema_ref().fields()),
        max,
        limit,
    }
}

/// Lowers the pieces of `planned`, each reserving what it takes from `room`, and queues them on
/// the partition's lane, in order.
async fn lowered(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    planned: Vec<Planned>,
    room: Room,
) -> Result<(), Error> {
    let mut window = Window::new(room);
    for Planned {
        plan,
        mut pieces,
        handed,
    } in planned
    {
        while let Some(piece) = next(job, context, &mut pieces).await? {
            let reserved = window.reserved(job, context, open, piece.bytes).await?;
            let ignored = job.changes.map_or_else(Ignored::default, |mode| {
                changes::ignored(mode, &piece.parts)
            });
            open.deletes_ignored += ignored.deletes;
            open.truncates_ignored += ignored.truncates;
            let rows = u64::try_from(piece.rows).unwrap_or(u64::MAX);
            let stamp = stamp(context, open, rows.saturating_sub(ignored.rows()));
            let lowering = Lowering {
                reserved,
                unit: pieces.share(),
            };
            let copies = !copies_nothing(&piece, &plan, &stamp, handed);
            let (plan, stream, mode) = (Arc::clone(&plan), job.stream.clone(), job.changes);
            let run = move || {
                let prepared = lower(&stream, mode, &piece.parts, &plan, &stamp)?;
                let grown = prepared.growth(&piece.parts);
                Ok((prepared, grown))
            };
            window.push(Box::new(run), lowering, copies);
            if full(window.len()) {
                window.lower(job, context, open).await?;
            }
        }
    }
    // Every piece has its room: what each keeps once lowered goes back to the budget as it is
    // written.
    window.room.close();
    window.lower(job, context, open).await
}

/// Bytes: the most a piece's rows take to lower, and a row's alone: a piece's and a request's
/// share of `shares`, within what a text array's offsets reach.
fn piece_bounds(shares: Shares) -> (u64, u64) {
    (
        shares.piece.min(MAX_PIECE_BYTES),
        shares.request.min(MAX_PIECE_BYTES),
    )
}

/// Whether `plan` hands the columns of a unit like `batch`, read as `mode` says, over as they
/// arrive: not a change stream's, whose change columns are split off first.
fn handed(plan: &LoweringPlan, batch: &RecordBatch, mode: Option<ChangeMode>) -> bool {
    mode.is_none() && plan.arrives_stored(batch.schema_ref())
}

/// Whether lowering `piece` as `plan` lowers it for the load `stamp` names copies no value: a
/// piece of one part, which needs no concatenating, of a unit whose columns the plan hands over
/// as they arrive, as `handed` says.
fn copies_nothing(piece: &Piece, plan: &LoweringPlan, stamp: &Stamp, handed: bool) -> bool {
    handed && piece.parts.len() == 1 && plan.constants_built(stamp, piece.rows)
}

/// The next piece of `pieces`: one cut ahead, or else cut on the partition's task where cutting
/// it reads no value, and on the compute pool otherwise.
async fn next(
    job: &PartitionJob,
    context: &PartitionContext,
    pieces: &mut Pieces,
) -> Result<Option<Piece>, Error> {
    if pieces.done() {
        return Ok(None);
    }
    if let Some(piece) = pieces.ready() {
        return Ok(Some(piece));
    }
    if !pieces.reads_values() {
        return pieces.next().map_err(|row| row_too_large(job, &row));
    }
    let mut cutting = std::mem::replace(pieces, Pieces::none());
    // The piece is returned first: where the caller stopped waiting, its slices of the unit go
    // before what holds the unit.
    let cut = move || {
        let piece = cutting.next();
        (piece, cutting)
    };
    let (piece, cutting) = context
        .pool
        .run_all([cut])
        .await
        .pop()
        .ok_or_else(|| Error::internal("the pool returned no piece"))?;
    *pieces = cutting;
    piece.map_err(|row| row_too_large(job, &row))
}

/// A unit with rows and its columns as they arrive.
struct Judged {
    incoming: Incoming,
    /// The unit's batches as they were pushed, a change stream's with their change columns.
    held: Held,
}

/// `units` with rows, each with its columns as they arrive, its own integers judged; a change
/// stream's by its data columns, none of them copied, and without the units whose every row the
/// stream ignores, which `open` counts.
fn judged(
    job: &PartitionJob,
    open: &mut OpenSegment,
    schemas: &mut Schemas,
    units: Vec<Held>,
) -> Result<Vec<Judged>, Error> {
    let mut judged = Vec::with_capacity(units.len());
    for held in units {
        let parts = &held.batches;
        let rows: usize = parts.iter().map(RecordBatch::num_rows).sum();
        let ignored = job
            .changes
            .map_or_else(Ignored::default, |mode| changes::ignored(mode, parts));
        if u64::try_from(rows).unwrap_or(u64::MAX) == ignored.rows() {
            open.deletes_ignored += ignored.deletes;
            open.truncates_ignored += ignored.truncates;
            continue;
        }
        let data = match job.changes {
            Some(_) => parts.iter().map(changes::data).collect::<Result<_, _>>()?,
            None => parts.clone(),
        };
        let data: Vec<RecordBatch> = data;
        let (schema, paths) = schemas
            .of(data[0].schema_ref())
            .map_err(|error| schema_invalid(job, &error))?;
        let incoming = Incoming::of(schema, paths, &data);
        judged.push(Judged { incoming, held });
    }
    Ok(judged)
}

/// The stamp of the next `received` rows written to `open`, which counts them.
fn stamp(context: &PartitionContext, open: &mut OpenSegment, received: u64) -> Stamp {
    let stamp = Stamp {
        epoch: context.epoch,
        loaded_at: context.clock.started(),
        received_at: context.clock.received(context.env.as_ref()),
        segment: open.id,
        first_row: open.received,
    };
    open.received += received;
    stamp
}

/// `parts`, a piece of a unit, one batch once concatenated, as `plan` lowers it; a change
/// stream's split into its data and its change columns first, read as `mode` says.
fn lower(
    stream: &rdlt_connector::StreamName,
    mode: Option<ChangeMode>,
    parts: &[RecordBatch],
    plan: &LoweringPlan,
    stamp: &Stamp,
) -> Result<Prepared, Error> {
    if let Some(mode) = mode {
        let (data, changes) = split_changes(stream, mode, parts)?;
        return plan.prepare(data, None, stamp, Some(changes));
    }
    let batch =
        joined(parts).map_err(|error| Error::internal(format!("coalescing batches: {error}")))?;
    plan.prepare(batch, None, stamp, None)
}

/// `parts`, batches of one schema, as one batch: a lone batch as it is, and several
/// concatenated.
fn joined(parts: &[RecordBatch]) -> Result<RecordBatch, arrow_schema::ArrowError> {
    match parts {
        [part] => Ok(part.clone()),
        _ => arrow_select::concat::concat_batches(&parts[0].schema(), parts),
    }
}
