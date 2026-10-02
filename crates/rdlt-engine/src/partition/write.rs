//! Writing what a partition gathered: shredding JSON, fitting each batch to its table, lowering
//! it a piece at a time and queueing each piece on its lane with the memory it holds.

mod changes;
mod held;
mod normalized;
mod pieces;
mod queue;
#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::RecordBatch;
use rdlt_connector::cost::Rendering;
use rdlt_connector::{ColumnPath, Permit, TableSchema};

use self::changes::split_changes;
use self::held::Held;
use self::pieces::{Lowered, Piece, Pieces};
use self::queue::queue;
use super::coalesce::{Flushed, Unit};
use super::{OpenSegment, PartitionContext, PartitionJob};
use crate::budget::{MemoryBudget, Reservation};
use crate::compute::run_all;
use crate::error::{Error, ErrorKind};
use crate::shred::{self, ShredError};
use crate::table::{ChangeRows, Incoming, LoweringPlan, Prepared, Stamp};

/// Bytes a chunk of JSON is charged before it is parsed, for each byte of its text.
///
/// It is what the chunk's values take once built, but for the nulls of sparse records, which
/// the shredder's limit on cells bounds. What the chunk's batch costs is charged in its place
/// once it is built.
const JSON_BUILT: u64 = 2;

/// Writes pushes gathered together: Arrow batches as one batch, JSON shredded into batches.
pub(super) async fn write_flushed(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    flushed: Flushed,
) -> Result<(), Error> {
    let permits = flushed.permits;
    let units = match flushed.unit {
        Unit::Arrow(batches) => {
            let held = Held::of(permits, flushed.bytes, &batches);
            vec![(batches, held)]
        }
        Unit::Json(pushes) => {
            let failed = |error: ShredError| shred_failed(job, &error);
            let compute = context.env.compute();
            let chunk_bytes = context.batch.chunk_bytes().get();
            let chunks = shred::scan(compute, &pushes, chunk_bytes)
                .await
                .map_err(failed)?;
            // Each chunk is charged before any is parsed.
            let mut reserved = Vec::with_capacity(chunks.len());
            for chunk in &chunks {
                let bytes = chunk.bytes().saturating_mul(JSON_BUILT);
                reserved.push(reserve(context, bytes, false).await?);
            }
            let batches = shred::shred_chunks(compute, chunks).await.map_err(failed)?;
            drop(pushes);
            let held = hold(
                &context.budget,
                &context.rendering,
                &batches,
                reserved,
                permits,
            );
            batches
                .into_iter()
                .zip(held)
                .map(|(batch, held)| (vec![batch], held))
                .collect()
        }
    };
    write(job, context, open, units).await
}

/// Reserves `bytes` for work this partition has begun, `large` for one row that takes more than
/// a piece: the wait ends when the attempt is cancelled, and at the budget's deadline.
async fn reserve(
    context: &PartitionContext,
    bytes: u64,
    large: bool,
) -> Result<Reservation, Error> {
    tokio::select! {
        biased;
        () = context.cancel.cancelled() => Err(Error::cancelled("the attempt was cancelled")),
        reserved = context.budget.acquire_working(bytes, large) => reserved.map_err(Error::memory),
    }
}

/// The error for a JSON push the shredder refused.
fn shred_failed(job: &PartitionJob, error: &ShredError) -> Error {
    let message = format!(
        "stream {}: a JSON push cannot be loaded: {error}",
        job.stream
    );
    let failed = match error {
        ShredError::Internal(_) => Error::internal(message),
        _ => Error::new(ErrorKind::Source, message),
    };
    failed.with_code(error.code()).with_stream(&job.stream)
}

/// What holds each of `batches`, shredded from chunks `reserved` was charged for, each charged
/// as `rendering` costs it before `permits`, which held the pushes they were shredded from, are
/// released: the budget always accounts for one or the other.
fn hold(
    budget: &MemoryBudget,
    rendering: &Rendering,
    batches: &[RecordBatch],
    reserved: Vec<Reservation>,
    permits: Vec<Permit>,
) -> Vec<Held> {
    let held = batches
        .iter()
        .zip(reserved)
        .map(|(batch, mut reserved)| {
            // What the batch keeps alive, or what it becomes where that is more, as far as the
            // budget goes: it is lowered a piece at a time.
            let cost = rendering.cost(batch, budget.capacity());
            let bytes = cost.held.max(cost.expanded.min(budget.capacity()));
            reserved.resize(bytes);
            let permit: Permit = Box::new(reserved);
            Held::of(vec![permit], bytes, std::slice::from_ref(batch))
        })
        .collect();
    drop(permits);
    held
}

/// Lowers `units`, each some batches of one schema and the memory they hold, into the partition's
/// table and queues them on its lane, in order.
///
/// - Each unit's plan is found in order, since finding it may change the table.
/// - The unit is then cut into pieces by what lowering each holds at once as its table stores
///   it: its columns converted to the table's types and rendered as the destination stores
///   them, and the columns it holds nothing in. A row that alone takes more than the budget
///   fails the write.
/// - Each piece reserves that from the budget before it is lowered, waiting for what a write
///   releases, never for what a unit being lowered holds. Pieces whose bytes were there at
///   once are lowered together on the compute pool, [`LOWERING_WINDOW`] at most.
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
    let mut window = Window::default();
    for mut unit in judged {
        unit.incoming.rounding.clone_from(&rounding);
        let plan = context.tables.plan(job.table, unit.incoming).await?;
        // The unit's pieces reserve what lowering takes: the unit holds its source alone.
        unit.held.settle();
        let max = pieces::piece_bytes(&context.budget);
        let lowered = Lowered {
            rendering: context.rendering.as_ref().clone(),
            stored: plan.stored(),
            row: plan.null_fill(1),
            max,
            budget: context.budget.capacity(),
        };
        let mut pieces = Pieces::new(unit.parts, lowered);
        let (mut held, mut first) = (Some(unit.held), 0);
        while let Some(piece) = next(job, context, &mut pieces).await? {
            let large = piece.bytes > max;
            let reserved =
                if let Some(reserved) = context.budget.try_acquire_working(piece.bytes, large) {
                    reserved
                } else {
                    // What was reserved is lowered and queued first, so a write releases it.
                    window.lower(job, context, open).await?;
                    reserve(context, piece.bytes, large).await?
                };
            let changes = unit
                .changes
                .as_ref()
                .map(|rows| rows.slice(first, piece.rows));
            first += piece.rows;
            let stamp = stamp(context, open, u64::try_from(piece.rows).unwrap_or(u64::MAX));
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
            let plan = Arc::clone(&plan);
            let run = move || lower(&piece.parts, &plan, &stamp, changes.as_ref());
            window.push(Box::new(run), lowering);
            if window.len() == LOWERING_WINDOW {
                window.lower(job, context, open).await?;
            }
        }
    }
    window.lower(job, context, open).await
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
    allocations: Arc<parking_lot::Mutex<rdlt_connector::cost::Allocations>>,
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

    /// Lowers the window's pieces on the compute pool and queues them, in order: each then holds
    /// what it was lowered to and its unit did not hold already, in place of what was reserved
    /// for lowering it.
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
        let lowered: Vec<_> = run_all(context.env.compute(), runs)
            .await
            .into_iter()
            .zip(held)
            .map(|(prepared, mut lowering)| {
                prepared.map(|prepared| {
                    let fresh = prepared.growth(&mut lowering.allocations.lock());
                    lowering.reserved.resize(fresh);
                    (prepared, lowering)
                })
            })
            .collect();
        for lowered in lowered {
            let (prepared, mut lowering) = lowered?;
            // Queued, the piece is released by a write, with its unit where it is the last.
            lowering.reserved.stage();
            let unit = lowering.unit.map(Held::staged);
            let reservation: Permit = Box::new((lowering.reserved, unit));
            queue(job, context, open, job.table, prepared, reservation).await?;
        }
        Ok(())
    }
}

/// A unit with rows, its change columns and its columns as they arrive.
struct Judged {
    parts: Vec<RecordBatch>,
    changes: Option<ChangeRows>,
    incoming: Incoming,
    held: Held,
}

/// `units` with rows, a change stream's split into data and change columns, each with its
/// columns as they arrive, its own integers judged.
fn judged(
    job: &PartitionJob,
    open: &mut OpenSegment,
    units: Vec<(Vec<RecordBatch>, Held)>,
) -> Result<Vec<Judged>, Error> {
    let mut judged = Vec::with_capacity(units.len());
    for (parts, held) in units {
        let (parts, changes) = match job.changes {
            Some(mode) => {
                let (data, changes) = split_changes(job, mode, open, &parts)?;
                (vec![data], Some(changes))
            }
            None => (parts, None),
        };
        if parts.iter().all(|batch| batch.num_rows() == 0) {
            continue;
        }
        let schema = schema_of(job, &parts[0])?;
        let paths = schema
            .fields()
            .iter()
            .map(|field| ColumnPath::from(field.name()))
            .collect();
        let incoming = Incoming::of(schema, paths, &parts);
        judged.push(Judged {
            parts,
            changes,
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
        loaded_at: context.loaded_at,
        received_at: context.env.now(),
        segment: open.id,
        first_row: open.received,
    };
    open.received += received;
    stamp
}

/// Pieces lowered on the pool at once, at most: what bounds the memory they hold together is
/// what each reserved before it was lowered.
const LOWERING_WINDOW: usize = 8;

/// `items` in order, in windows of at most [`LOWERING_WINDOW`].
fn windows<T>(items: Vec<T>) -> Vec<Vec<T>> {
    let mut windows = Vec::with_capacity(items.len().div_ceil(LOWERING_WINDOW));
    let mut items = items.into_iter().peekable();
    while items.peek().is_some() {
        windows.push(items.by_ref().take(LOWERING_WINDOW).collect());
    }
    windows
}

/// The error for a row that alone expands beyond the whole budget: no slice of it can be lowered
/// within the budget.
fn row_too_large(job: &PartitionJob, row: &pieces::RowTooLarge) -> Error {
    Error::new(
        ErrorKind::Source,
        format!(
            "stream {}: one row expands to more than {} bytes, beyond the memory budget of {}",
            job.stream, row.expanded, row.budget
        ),
    )
    .with_code("row_exceeds_budget")
    .with_stream(&job.stream)
}

/// `parts`, one batch once concatenated, as `plan` lowers it, with a change stream's `changes`.
fn lower(
    parts: &[RecordBatch],
    plan: &LoweringPlan,
    stamp: &Stamp,
    changes: Option<&ChangeRows>,
) -> Result<Prepared, Error> {
    // A lone batch concatenates to itself without a copy.
    let batch = arrow_select::concat::concat_batches(&parts[0].schema(), parts)
        .map_err(|error| Error::internal(format!("coalescing batches: {error}")))?;
    plan.prepare(&batch, None, stamp, changes)
}
