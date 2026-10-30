//! Writing what a partition gathered: shredding JSON, fitting each batch to its table, lowering
//! it a piece at a time and queueing each piece on its lane with the memory it holds.

mod allowance;
mod changes;
mod held;
mod json;
mod normalized;
mod pieces;
mod queue;
mod schemas;
#[cfg(test)]
mod tests;
mod units;

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::cost::{Rendering, arrays};
use rdlt_connector::{ColumnPath, Permit, SchemaError, TableSchema};
use tokio_util::sync::CancellationToken;

use self::changes::{Ignored, split_changes};
use self::held::{Held, Share};
use self::pieces::{Lowered, Piece, Pieces, RowTooLarge};
use self::queue::queue;
pub(super) use self::schemas::Schemas;
use self::units::Units;
use super::coalesce::{Flushed, Unit};
use super::{ChangeMode, OpenSegment, PartitionContext, PartitionJob};
use crate::budget::{Denied, Lent, MemoryBudget, Reservation, Shares, TooLarge};
use crate::compute::Pool;
use crate::cost::CHANGE_ROW;
use crate::error::{Error, ErrorKind, codes};
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
    /// lent, and a piece no more.
    fn shares(&self, budget: &MemoryBudget) -> Shares {
        let shares = budget.shares();
        match self {
            Self::Budget => shares,
            Self::Lent(lent) => Shares {
                request: lent.bytes(),
                piece: shares.piece.min(lent.bytes()),
                ..shares
            },
        }
    }

    /// Lends no more: what is lent and no piece holds goes back to the budget, and each piece's
    /// part as it is written.
    fn close(&self) {
        if let Self::Lent(lent) = self {
            lent.close();
        }
    }

    /// The error for a row that alone takes more to lower than a request of the room may.
    fn refusal(&self, job: &PartitionJob, budget: &MemoryBudget, row: &RowTooLarge) -> Error {
        match self {
            Self::Budget => row_too_large(job, row),
            Self::Lent(lent) => {
                let request = budget.shares().request;
                let built = request.saturating_sub(lent.bytes());
                json::row_beside_built(job, row, built, request)
            }
        }
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
///   for at once are lowered together, [`LOWERING_WINDOW`] at most.
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
        return normalized::write_normalized(job, context, open, units, &shape, kind).await;
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

/// What holds a piece while it is lowered and until it is written: dropped, what it reserved is
/// released before its unit, whose batches go before their permits.
struct Lowering {
    /// What lowering the piece holds, reserved before it is lowered.
    reserved: Reservation,
    /// A share of what holds its unit, so the unit's batches and permits hold until all of it is
    /// written.
    unit: Share,
}

/// A piece's lowering, and the bytes of what it keeps alive beyond the parts it was lowered from.
type Run = Box<dyn FnOnce() -> Result<(Prepared, u64), Error> + Send>;

/// Pieces reserved from a room and not yet lowered, in order: the runs are declared before what
/// holds their pieces, so they go first.
struct Window {
    room: Room,
    runs: Vec<Run>,
    held: Vec<Lowering>,
    /// Whether lowering a piece of the window copies values.
    copies: bool,
}

impl Window {
    fn new(room: Room) -> Self {
        Self {
            room,
            runs: Vec::new(),
            held: Vec::new(),
            copies: false,
        }
    }

    /// Adds a piece, lowered by `run`, held by `lowering`, whose lowering `copies` values or not.
    fn push(&mut self, run: Run, lowering: Lowering, copies: bool) {
        self.runs.push(run);
        self.held.push(lowering);
        self.copies |= copies;
    }

    fn len(&self) -> usize {
        self.runs.len()
    }

    /// Reserves `bytes` for a piece to lower with those the window holds, where the budget has
    /// them at once; nothing where the window is empty, or the piece must wait: a partition
    /// waits only while it holds no piece it has not handed over.
    fn reserve(&self, budget: &MemoryBudget, bytes: u64) -> Option<Reservation> {
        if self.held.is_empty() {
            return None;
        }
        self.room.try_reserve(budget, bytes)
    }

    /// Reserves `bytes` for a piece: with those the window holds where the budget has them at
    /// once, or after the window's pieces are lowered and handed to their lane, so the wait is
    /// for bytes a write releases and this partition holds none of them.
    async fn reserved(
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
    async fn lowered(&mut self, pool: &Pool) -> Vec<(Result<(Prepared, u64), Error>, Lowering)> {
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
    async fn lower(
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

/// The table schema of `batch`'s columns.
fn schema_of(job: &PartitionJob, batch: &RecordBatch) -> Result<TableSchema, Error> {
    TableSchema::from_arrow(&batch.schema()).map_err(|error| schema_invalid(job, &error))
}

/// The error for a batch of `job`'s stream holding a column with no table schema.
pub(super) fn schema_invalid(job: &PartitionJob, error: &SchemaError) -> Error {
    Error::schema(format!(
        "stream {}: a batch has no table schema: {error}",
        job.stream
    ))
    .with_code(codes::BATCH_SCHEMA_INVALID)
    .with_stream(&job.stream)
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

/// Pieces lowered on the pool at once, at most: what bounds the memory they hold together is
/// what each reserved before it was lowered.
const LOWERING_WINDOW: usize = 8;

/// Whether a window of `pieces` holds as many as are lowered together.
fn full(pieces: usize) -> bool {
    pieces >= LOWERING_WINDOW
}

/// The error for a row that alone takes more to lower than a request may take of the budget:
/// no piece of it can be reserved.
fn row_too_large(job: &PartitionJob, row: &RowTooLarge) -> Error {
    Error::new(
        ErrorKind::Source,
        format!(
            "stream {}: lowering one row takes more than {} bytes, beyond the {} one request may \
             take of the memory budget",
            job.stream, row.expanded, row.limit
        ),
    )
    .with_code(codes::ROW_EXCEEDS_BUDGET)
    .with_stream(&job.stream)
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
