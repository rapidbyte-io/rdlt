//! Writing what a partition gathered: shredding JSON, fitting each batch to its table, preparing
//! it and queueing it on its lane with the memory it holds.

mod normalized;
mod slices;
#[cfg(test)]
mod tests;

use std::collections::BTreeSet;

use arrow_array::{BooleanArray, RecordBatch};
use rdlt_connector::ChangeOp;
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
            let held = Held {
                permits,
                bytes: flushed.bytes,
            };
            vec![(batches, held)]
        }
        Unit::Json(pushes) => {
            let chunk_bytes = context.batch.chunk_bytes().get();
            let batches = shred::shred(context.env.compute(), &pushes, chunk_bytes)
                .await
                .map_err(|error| shred_failed(job, &error))?;
            drop(pushes);
            let held = hold(&context.budget, &batches, permits);
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

/// Reservations holding each of `batches`' bytes, charged before `permits`, which held the pushes
/// they were shredded from, are released: the budget always accounts for one or the other.
fn hold(budget: &MemoryBudget, batches: &[RecordBatch], permits: Vec<Permit>) -> Vec<Held> {
    let held = batches
        .iter()
        .map(|batch| {
            let bytes = u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX);
            let permit: Permit = Box::new(budget.charge(bytes));
            Held {
                permits: vec![permit],
                bytes,
            }
        })
        .collect();
    drop(permits);
    held
}

/// Permits holding `bytes` of a batch's memory.
struct Held {
    permits: Vec<Permit>,
    bytes: u64,
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
    let units = slices::sliced(units, slices::slice_bytes(&context.budget));
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
        held,
    } in judged
    {
        incoming.rounding.clone_from(&rounding);
        let plan = context.tables.plan(job.table, incoming).await?;
        let received = parts
            .iter()
            .map(|batch| u64::try_from(batch.num_rows()).unwrap_or(u64::MAX))
            .sum::<u64>();
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

/// `held` with the growth of `prepared` beyond it charged: the permits then hold the lowered
/// batch's bytes, or the shredded batch's where lowering shrank it.
///
/// A lowered piece of a larger batch may share its buffers, so it is charged for its own rows.
fn charge_growth(budget: &MemoryBudget, prepared: &Prepared, mut held: Held) -> Held {
    let bytes = slices::held_bytes(&prepared.batch);
    held.permits
        .push(Box::new(budget.charge(bytes.saturating_sub(held.bytes))));
    held.bytes = held.bytes.max(bytes);
    held
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
    let bytes = slices::held_bytes(&prepared.batch);
    let lane = context.lanes.route(table, job.partition.id());
    context
        .lanes
        .write(
            lane,
            Write {
                table,
                version: prepared.version,
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
