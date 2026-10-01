//! Writing what a partition gathered: shredding JSON, fitting each batch to its table, preparing
//! it and queueing it on its lane with the memory it holds.

mod normalized;
mod slices;
#[cfg(test)]
mod tests;

use std::collections::BTreeSet;

use std::sync::Arc;

use arrow_array::{BooleanArray, RecordBatch};
use parking_lot::Mutex;
use rdlt_connector::ChangeOp;
use rdlt_connector::cost::{Allocations, Rendering};
use rdlt_connector::{ColumnPath, Permit, TableSchema};

use super::coalesce::{Flushed, Unit};
use super::{ChangeMode, OpenSegment, PartitionContext, PartitionJob, Progress};
use crate::budget::MemoryBudget;
use crate::compute::run_all;
use crate::error::{Error, ErrorKind};
use crate::lane::Write;
use crate::plan::{DeleteMode, OnTruncate};
use crate::shred::{self, ShredError};
use crate::table::{ChangeRows, Incoming, LoweringPlan, Prepared, Stamp};

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
            let chunk_bytes = context.batch.chunk_bytes().get();
            let batches = shred::shred(context.env.compute(), &pushes, chunk_bytes)
                .await
                .map_err(|error| shred_failed(job, &error))?;
            drop(pushes);
            let held = hold(&context.budget, &context.rendering, &batches, permits);
            batches
                .into_iter()
                .zip(held)
                .map(|(batch, held)| (vec![batch], held))
                .collect()
        }
    };
    write(job, context, open, units).await
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

/// What holds each of `batches`, charged as `rendering` costs it before `permits`, which held
/// the pushes they were shredded from, are released: the budget always accounts for one or the
/// other.
fn hold(
    budget: &MemoryBudget,
    rendering: &Rendering,
    batches: &[RecordBatch],
    permits: Vec<Permit>,
) -> Vec<Held> {
    let held = batches
        .iter()
        .map(|batch| {
            // What the batch keeps alive, or what it becomes where that is more, as far as the
            // budget goes: it is lowered a slice at a time.
            let cost = rendering.cost(batch, budget.capacity());
            let bytes = cost.held.max(cost.expanded.min(budget.capacity()));
            let permit: Permit = Box::new(budget.charge(bytes));
            Held::of(vec![permit], bytes, std::slice::from_ref(batch))
        })
        .collect();
    drop(permits);
    held
}

/// What holds a unit's memory: the permits charged for it and the allocations they cover.
struct Held {
    permits: Vec<Permit>,
    /// Bytes the permits hold beyond what the allocations take: what the unit may still grow by
    /// before more is charged.
    spare: u64,
    /// The allocations charged so far, the unit's own first; the pieces of a unit share them,
    /// so each is charged once however many pieces keep it alive.
    allocations: Arc<Mutex<Allocations>>,
}

impl Held {
    /// What holds `batches`, a unit `permits` hold `bytes` for.
    fn of(permits: Vec<Permit>, bytes: u64, batches: &[RecordBatch]) -> Self {
        let mut allocations = Allocations::default();
        for batch in batches {
            allocations.add(batch);
        }
        Self {
            permits,
            spare: bytes.saturating_sub(allocations.bytes()),
            allocations: Arc::new(Mutex::new(allocations)),
        }
    }

    /// What holds another piece of the same unit: no permits of its own, the unit's allocations.
    fn piece(&self) -> Self {
        Self {
            permits: Vec::new(),
            spare: 0,
            allocations: Arc::clone(&self.allocations),
        }
    }

    /// Charges `budget` `bytes` the unit is about to grow by, before it does: its permits then
    /// spare them for the growth that follows.
    fn reserve(&mut self, budget: &MemoryBudget, bytes: u64) {
        self.permits.push(Box::new(budget.charge(bytes)));
        self.spare = self.spare.saturating_add(bytes);
    }

    /// Charges `budget` the `fresh` bytes the unit grew by, beyond what its permits spare.
    fn grow(&mut self, budget: &MemoryBudget, fresh: u64) {
        self.permits
            .push(Box::new(budget.charge(fresh.saturating_sub(self.spare))));
        self.spare = self.spare.saturating_sub(fresh);
    }
}

/// Lowers `units`, each some batches of one schema and the memory they hold, into the partition's
/// table and queues them on its lane, in order.
///
/// Each unit's plan is found in order, since finding it may change the table. Then the units are
/// concatenated and lowered on the compute pool a window at a time, and queued in order.
///
/// The units are one flush, which chunks and slices cut however their sizes fall, so its integers
/// are judged together: where it was cut decides no column's type.
async fn write(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    units: Vec<(Vec<RecordBatch>, Held)>,
) -> Result<(), Error> {
    // A few bytes of encoded columns may decode to far more, so large units lower in slices.
    let slice = slices::slice_bytes(&context.budget);
    let units = slices::sliced(units, &context.rendering, slice, context.budget.capacity())
        .map_err(|row| row_too_large(job, &row))?;
    if let Some(shape) = context.tables.shape(job.table) {
        return normalized::write_normalized(job, context, open, units, &shape).await;
    }
    let judged = judged(job, open, units)?;
    let rounding: BTreeSet<ColumnPath> = judged
        .iter()
        .flat_map(|unit| unit.incoming.rounding.iter().cloned())
        .collect();
    let mut planned = Vec::with_capacity(judged.len());
    for Judged {
        parts,
        changes,
        mut incoming,
        mut held,
    } in judged
    {
        incoming.rounding.clone_from(&rounding);
        let plan = context.tables.plan(job.table, incoming).await?;
        let rows: usize = parts.iter().map(RecordBatch::num_rows).sum();
        // The columns the unit holds nothing in are charged before lowering fills them.
        held.reserve(&context.budget, plan.null_fill(rows));
        let received = u64::try_from(rows).unwrap_or(u64::MAX);
        let stamp = stamp(context, open, received);
        planned.push((move || lower(&parts, &plan, &stamp, changes.as_ref()), held));
    }
    for window in windows(planned) {
        let (jobs, reservations): (Vec<_>, Vec<_>) = window.into_iter().unzip();
        // A window's lowered batches are charged before the first waits on its lane, so at most
        // one window's growth is ever uncharged.
        let lowered: Vec<_> = run_all(context.env.compute(), jobs)
            .await
            .into_iter()
            .zip(reservations)
            .map(|(prepared, held)| {
                prepared.map(|prepared| {
                    let held = charge_growth(&context.budget, &prepared, held);
                    (prepared, held)
                })
            })
            .collect();
        for lowered in lowered {
            let (prepared, held) = lowered?;
            let reservation: Permit = Box::new(held.permits);
            queue(job, context, open, job.table, prepared, reservation).await?;
        }
    }
    Ok(())
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

/// Units lowered on the pool at once: a flush of the default size fits in one window, and a
/// larger one never holds more than this many lowered batches uncharged.
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

/// `held` with the growth of `prepared` beyond it charged: the permits then hold the allocations
/// the lowered batch keeps alive that the unit did not hold already.
///
/// A lowered piece of a larger batch may share the batch's buffers, which the unit's permits
/// hold until its last piece is written.
fn charge_growth(budget: &MemoryBudget, prepared: &Prepared, mut held: Held) -> Held {
    let fresh = prepared.growth(&mut held.allocations.lock());
    held.grow(budget, fresh);
    held
}

/// The error for a row that alone expands beyond the whole budget: no slice of it can be lowered
/// within the budget.
fn row_too_large(job: &PartitionJob, row: &slices::RowTooLarge) -> Error {
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

/// The bytes the rows of `batch` take, as reports and the commit policy count what was written:
/// a slice counts its own rows, whatever it keeps alive, which is the budget's to charge.
fn written_bytes(batch: &RecordBatch) -> u64 {
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

/// `parts`, a change stream's batches of one schema, as one batch of data and its change
/// columns, without the deletes and truncates the stream ignores, which `open` counts.
///
/// A merge's rows flagging columns unchanged are refused where the destination cannot keep a
/// column's value.
fn split_changes(
    job: &PartitionJob,
    mode: ChangeMode,
    open: &mut OpenSegment,
    parts: &[RecordBatch],
) -> Result<(RecordBatch, ChangeRows), Error> {
    let failed =
        |error: arrow_schema::ArrowError| Error::internal(format!("splitting changes: {error}"));
    let batch = arrow_select::concat::concat_batches(&parts[0].schema(), parts).map_err(failed)?;
    let (data, changes) = ChangeRows::split(&batch).map_err(failed)?;
    // A log stores each row's flags as data; only a merge keeps a column's value.
    let flagged = if mode.merge && !mode.partial_updates {
        changes.flagged()
    } else {
        Vec::new()
    };
    if !flagged.is_empty() {
        let schema = data.schema();
        let names: Vec<&str> = flagged
            .iter()
            .filter_map(|ordinal| {
                schema
                    .fields()
                    .get(*ordinal)
                    .map(|field| field.name().as_str())
            })
            .collect();
        return Err(Error::config(format!(
            "stream {}: updates leave columns {} unchanged, which the destination cannot keep",
            job.stream,
            names.join(", ")
        ))
        .with_code("partial_updates_unsupported")
        .with_stream(&job.stream));
    }
    let ignores = |op| match op {
        Some(ChangeOp::Delete) => mode.merge && mode.deletes == DeleteMode::Ignore,
        Some(ChangeOp::Truncate) => mode.merge && mode.truncates == OnTruncate::Ignore,
        _ => false,
    };
    let keep: BooleanArray = (0..changes.op.len())
        .map(|row| Some(!ignores(changes.op(row))))
        .collect();
    if keep.true_count() == keep.len() {
        return Ok((data, changes));
    }
    for row in (0..changes.op.len()).filter(|row| !keep.value(*row)) {
        match changes.op(row) {
            Some(ChangeOp::Delete) => open.deletes_ignored += 1,
            _ => open.truncates_ignored += 1,
        }
    }
    let data = arrow_select::filter::filter_record_batch(&data, &keep).map_err(failed)?;
    Ok((data, changes.filter(&keep).map_err(failed)?))
}

/// Queues `prepared` on `table`'s lane with `reservation`, the permits holding its bytes, which
/// travel with it.
///
/// Growth beyond what a push held was charged at once; later pushes pay it back by waiting.
async fn queue(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    table: usize,
    prepared: Prepared,
    reservation: Permit,
) -> Result<(), Error> {
    open.discarded_rows += prepared.discarded_rows;
    open.discarded_values += prepared.discarded_values;
    let rows = u64::try_from(prepared.batch.num_rows()).unwrap_or(u64::MAX);
    if rows == 0 {
        return Ok(());
    }
    let bytes = written_bytes(&prepared.batch);
    if let Some(log) = &context.wal {
        // Queued for the log before the partition can seal the segment, so the frame of the
        // commit that takes the segment, queued after the seal, follows this batch's.
        let compute = context.env.compute();
        log.batch(
            compute,
            &context.budget,
            table,
            &prepared.view,
            open.id,
            &prepared.batch,
        )
        .await?;
    }
    let lane = context.lanes.route(table, job.partition.id());
    context
        .lanes
        .write(
            lane,
            Write {
                table,
                version: prepared.view.table.version,
                segment: open.id,
                batch: prepared.batch,
                reservation,
            },
        )
        .await?;
    open.rows += rows;
    open.bytes += bytes;
    context.report(Progress::Written { rows, bytes })
}
