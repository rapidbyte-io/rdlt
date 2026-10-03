//! Writing what a partition gathered: shredding JSON, fitting each batch to its table, lowering
//! it a piece at a time and queueing each piece on its lane with the memory it holds.

mod allowance;
mod changes;
mod held;
mod normalized;
mod pieces;
mod queue;
#[cfg(test)]
mod tests;
mod units;

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
use parking_lot::Mutex;
use rdlt_connector::cost::Allocations;
use rdlt_connector::{ColumnPath, Permit, TableSchema};

use self::changes::{CHANGE_ROW, Ignored, split_changes};
use self::held::Held;
use self::pieces::{Lowered, Piece, Pieces};
use self::queue::queue;
use super::coalesce::Flushed;
use super::{ChangeMode, OpenSegment, PartitionContext, PartitionJob};
use crate::budget::{Denied, MemoryBudget, Reservation, Shares, TooLarge};
use crate::compute::run_all;
use crate::error::{Error, ErrorKind};
use crate::limits::{MAX_PIECE_BYTES, ROW_EXCEEDS_BUDGET};
use crate::table::{Incoming, LoweringPlan, Prepared, Stamp};

/// Writes pushes gathered together: Arrow batches as one batch, JSON shredded into batches.
pub(super) async fn write_flushed(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    flushed: Flushed,
) -> Result<(), Error> {
    let units = units::of(job, context, flushed).await?;
    write(job, context, open, units).await
}

/// Reserves `bytes` for the lowering this partition does next, all of them in one request, so
/// it waits only for what other lowerings hold; the wait ends when the attempt is cancelled, and
/// at the budget's deadline.
async fn reserve(
    job: &PartitionJob,
    context: &PartitionContext,
    bytes: u64,
) -> Result<Reservation, Error> {
    let too_large = |large: TooLarge| {
        let row = pieces::RowTooLarge {
            expanded: large.asked,
            limit: large.limit,
        };
        row_too_large(job, &row)
    };
    reserving(job, context, bytes, too_large).await
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

/// How many times what lowering a piece takes is reserved for it: once, and once more where the
/// load keeps a log, for the frame the lowered batch is logged as.
fn reserved_times(context: &PartitionContext) -> u64 {
    1 + u64::from(context.wal.is_some())
}

/// Lowers `units`, each some batches of one schema and the memory they hold, into the partition's
/// table and queues them on its lane, in order.
///
/// - Each unit's plan is found in order, since finding it may change the table.
/// - The unit is then cut into pieces by what lowering each holds at once as its table stores
///   it: its columns converted to the table's types and rendered as the destination stores
///   them, the columns it holds nothing in, and the metadata lowering adds. A row that alone
///   takes more than one request may fails the write.
/// - Each piece reserves that from the budget before it is lowered. The partition waits for a
///   piece only while it holds none it has not handed to its lane; pieces the budget has room
///   for at once are lowered together on the compute pool, [`LOWERING_WINDOW`] at most.
///
/// The units are one flush, which chunks and pieces cut however their sizes fall, so its integers
/// are judged together: where it was cut decides no column's type.
async fn write(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    units: Vec<(Vec<RecordBatch>, Held)>,
) -> Result<(), Error> {
    if let Some(shape) = context.tables.shape(job.table) {
        return normalized::write_normalized(job, context, open, units, &shape).await;
    }
    let judged = judged(job, open, units)?;
    let rounding: BTreeSet<ColumnPath> = judged
        .iter()
        .flat_map(|unit| unit.incoming.rounding.iter().cloned())
        .collect();
    let times = reserved_times(context);
    let shares = context.budget.shares();
    let mut window = Window::default();
    for mut unit in judged {
        unit.incoming.rounding.clone_from(&rounding);
        let plan = context.tables.plan(job.table, unit.incoming).await?;
        let (stored, changes) = match job.changes {
            Some(_) => (changes::aligned(&unit.parts[0], &plan.stored()), CHANGE_ROW),
            None => (plan.stored(), 0),
        };
        let (max, limit) = piece_bounds(shares, times);
        let lowered = Lowered {
            rendering: context.rendering.as_ref().clone(),
            stored,
            row: plan.row_bytes().saturating_add(changes),
            item: 0,
            max,
            limit,
        };
        let mut pieces = Pieces::new(unit.parts, lowered);
        let mut held = Some(unit.held);
        while let Some(piece) = next(job, context, &mut pieces).await? {
            let bytes = piece.bytes.saturating_mul(times);
            let reserved = window.reserved(job, context, open, bytes).await?;
            let ignored = job.changes.map_or_else(Ignored::default, |mode| {
                changes::ignored(mode, &piece.parts)
            });
            open.deletes_ignored += ignored.deletes;
            open.truncates_ignored += ignored.truncates;
            let rows = u64::try_from(piece.rows).unwrap_or(u64::MAX);
            let stamp = stamp(context, open, rows.saturating_sub(ignored.rows()));
            let Some(unit_held) = &held else {
                return Err(Error::internal("a unit was cut after its last piece"));
            };
            let lowering = Lowering {
                allocations: Arc::clone(&unit_held.allocations),
                reserved,
                // The unit's permits stay with its last piece, so they hold until all of it is
                // written.
                unit: if pieces.done() { held.take() } else { None },
            };
            let (plan, stream, mode) = (Arc::clone(&plan), job.stream.clone(), job.changes);
            let run = move || lower(&stream, mode, &piece.parts, &plan, &stamp);
            window.push(Box::new(run), lowering);
            if full(window.len()) {
                window.lower(job, context, open).await?;
            }
        }
    }
    window.lower(job, context, open).await
}

/// Bytes: the most a piece's rows take to lower, and a row's alone, where what lowering takes is
/// reserved `times` over: a piece's and a request's share of `shares`, within what a text array's
/// offsets reach.
fn piece_bounds(shares: Shares, times: u64) -> (u64, u64) {
    let bound = |bytes: u64| (bytes / times).min(MAX_PIECE_BYTES);
    (bound(shares.piece), bound(shares.request))
}

/// The next piece of `pieces`, cut on the compute pool.
async fn next(
    job: &PartitionJob,
    context: &PartitionContext,
    pieces: &mut Pieces,
) -> Result<Option<Piece>, Error> {
    if pieces.done() {
        return Ok(None);
    }
    let mut cutting = std::mem::replace(pieces, Pieces::none());
    let cut = move || {
        let piece = cutting.next();
        (cutting, piece)
    };
    let (cutting, piece) = run_all(context.env.compute(), [cut])
        .await
        .pop()
        .ok_or_else(|| Error::internal("the pool returned no piece"))?;
    *pieces = cutting;
    piece.map_err(|row| row_too_large(job, &row))
}

/// What holds a piece while it is lowered and until it is written.
struct Lowering {
    /// The allocations its unit holds, which what it is lowered to may share.
    allocations: Arc<Mutex<Allocations>>,
    /// What lowering the piece holds, reserved before it is lowered.
    reserved: Reservation,
    /// For a unit's last piece, what holds the unit.
    unit: Option<Held>,
}

/// A piece's lowering, to run on the compute pool.
type Run = Box<dyn FnOnce() -> Result<Prepared, Error> + Send>;

/// Pieces reserved and not yet lowered, in order.
#[derive(Default)]
struct Window {
    runs: Vec<Run>,
    held: Vec<Lowering>,
}

impl Window {
    fn push(&mut self, run: Run, lowering: Lowering) {
        self.runs.push(run);
        self.held.push(lowering);
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
        budget.try_acquire_working(bytes)
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
        reserve(job, context, bytes).await
    }

    /// Lowers the window's pieces on the compute pool and queues them, in order: each then holds
    /// what it was lowered to and its unit did not hold already, where that is less than was
    /// reserved for lowering it, and its frame in the log what the frame takes.
    async fn lower(
        &mut self,
        job: &PartitionJob,
        context: &PartitionContext,
        open: &mut OpenSegment,
    ) -> Result<(), Error> {
        let (runs, held) = (
            std::mem::take(&mut self.runs),
            std::mem::take(&mut self.held),
        );
        let times = reserved_times(context);
        let lowered: Vec<_> = run_all(context.env.compute(), runs)
            .await
            .into_iter()
            .zip(held)
            .map(|(prepared, mut lowering)| {
                prepared.map(|prepared| {
                    let frame = frame_part(&mut lowering.reserved, times);
                    let frame = frame.map(|frame| Box::new(frame) as Permit);
                    let fresh = prepared.growth(&mut lowering.allocations.lock());
                    lowering.reserved.shrink(fresh);
                    (prepared, lowering, frame)
                })
            })
            .collect();
        for lowered in lowered {
            let (prepared, lowering, frame) = lowered?;
            let unit = lowering.unit.map(|unit| unit.permits);
            let reservation: Permit = Box::new((lowering.reserved, unit));
            queue(
                job,
                context,
                open,
                job.table,
                prepared,
                (reservation, frame),
            )
            .await?;
        }
        Ok(())
    }
}

/// The part of `reserved`, what a piece reserved `times` times over, that holds its frame in the
/// log: all but one of them, where it was reserved more than once.
fn frame_part(reserved: &mut Reservation, times: u64) -> Option<Reservation> {
    let logged = reserved.bytes() - reserved.bytes() / times.max(1);
    (times > 1).then(|| reserved.split(logged))
}

/// A unit with rows and its columns as they arrive.
struct Judged {
    /// The unit's batches as they were pushed, a change stream's with their change columns.
    parts: Vec<RecordBatch>,
    incoming: Incoming,
    held: Held,
}

/// `units` with rows, each with its columns as they arrive, its own integers judged; a change
/// stream's by its data columns, none of them copied, and without the units whose every row the
/// stream ignores, which `open` counts.
fn judged(
    job: &PartitionJob,
    open: &mut OpenSegment,
    units: Vec<(Vec<RecordBatch>, Held)>,
) -> Result<Vec<Judged>, Error> {
    let mut judged = Vec::with_capacity(units.len());
    for (parts, held) in units {
        let rows: usize = parts.iter().map(RecordBatch::num_rows).sum();
        let ignored = job
            .changes
            .map_or_else(Ignored::default, |mode| changes::ignored(mode, &parts));
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
        let schema = schema_of(job, &data[0])?;
        let paths = schema
            .fields()
            .iter()
            .map(|field| ColumnPath::from(field.name()))
            .collect();
        let incoming = Incoming::of(schema, paths, &data);
        judged.push(Judged {
            parts,
            incoming,
            held,
        });
    }
    Ok(judged)
}

/// The table schema of `batch`'s columns.
fn schema_of(job: &PartitionJob, batch: &RecordBatch) -> Result<TableSchema, Error> {
    TableSchema::from_arrow(&batch.schema()).map_err(|error| {
        Error::schema(format!(
            "stream {}: a batch has no table schema: {error}",
            job.stream
        ))
        .with_code("batch_schema_invalid")
        .with_stream(&job.stream)
    })
}

/// The stamp of the next `received` rows written to `open`, which counts them.
fn stamp(context: &PartitionContext, open: &mut OpenSegment, received: u64) -> Stamp {
    let stamp = Stamp {
        load_id: context.load_id,
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
fn row_too_large(job: &PartitionJob, row: &pieces::RowTooLarge) -> Error {
    Error::new(
        ErrorKind::Source,
        format!(
            "stream {}: lowering one row takes more than {} bytes, beyond the {} one request may \
             take of the memory budget",
            job.stream, row.expanded, row.limit
        ),
    )
    .with_code(ROW_EXCEEDS_BUDGET)
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
        return plan.prepare(&data, None, stamp, Some(&changes));
    }
    // A lone batch concatenates to itself without a copy.
    let batch = arrow_select::concat::concat_batches(&parts[0].schema(), parts)
        .map_err(|error| Error::internal(format!("coalescing batches: {error}")))?;
    plan.prepare(&batch, None, stamp, None)
}
