//! Writing what a partition gathered: shredding JSON, fitting each batch to its table, preparing
//! it and queueing it on its lane with the memory it holds.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::ArrowError;
use parking_lot::Mutex;
use rdlt_connector::{Permit, StreamName, TableSchema};

use super::coalesce::{Flushed, Unit};
use super::{OpenSegment, PartitionContext, PartitionJob, Progress};
use crate::budget::MemoryBudget;
use crate::compute::run_all;
use crate::error::{Error, ErrorKind};
use crate::lane::Write;
use crate::normalize::{self, Part, Shape};
use crate::shred::{self, ShredError};
use crate::table::{Admission, Incoming, LoweringPlan, Prepared, Stamp};

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
async fn write(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    units: Vec<(Vec<RecordBatch>, Held)>,
) -> Result<(), Error> {
    if let Some(shape) = context.tables.shape(job.table) {
        return write_normalized(job, context, open, units, &shape).await;
    }
    let mut planned = Vec::with_capacity(units.len());
    for (parts, held) in units {
        let received = parts
            .iter()
            .map(|batch| u64::try_from(batch.num_rows()).unwrap_or(u64::MAX))
            .sum::<u64>();
        if received == 0 {
            continue;
        }
        let incoming = schema_of(job, &parts[0])?;
        let plan = context.tables.plan(job.table, incoming).await?;
        let stamp = stamp(context, open, received);
        planned.push((move || lower(&parts, &plan, &stamp), held));
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

/// Lowers `units` of a stream that normalizes as `shape` into its table and child tables, and
/// queues them on their lanes, a window of units at a time.
///
/// Each unit is concatenated and normalized on the compute pool. Its parts' tables and plans are
/// then found in order, since finding them may add child tables or change tables, and the parts
/// are lowered on the pool. A unit's parts share the permits holding its memory, charged with its
/// growth before any part waits on its lane.
async fn write_normalized(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    units: Vec<(Vec<RecordBatch>, Held)>,
    shape: &Arc<Shape>,
) -> Result<(), Error> {
    for window in windows(units) {
        let (batches, reservations): (Vec<_>, Vec<_>) = window.into_iter().unzip();
        let jobs = batches.into_iter().map(|parts| {
            let (shape, stream) = (Arc::clone(shape), job.stream.clone());
            move || split(&stream, &parts, &shape)
        });
        let split = run_all(context.env.compute(), jobs).await;
        let mut planned = Vec::with_capacity(split.len());
        for (parts, held) in split.into_iter().zip(reservations) {
            let parts = parts?;
            let received = parts.first().map_or(0, |part| part.batch.num_rows() as u64);
            if received == 0 {
                continue;
            }
            let stamp = stamp(context, open, received);
            let (unit, discarded) = plan_parts(job, context, parts).await?;
            open.discarded_values += discarded;
            let lower_unit = move || {
                unit.into_iter()
                    .map(|(table, part, plan)| {
                        let prepared = plan.prepare(&part.batch, Some(&part.lineage), &stamp)?;
                        Ok((table, prepared))
                    })
                    .collect::<Result<Vec<_>, Error>>()
            };
            planned.push((lower_unit, held));
        }
        let (jobs, reservations): (Vec<_>, Vec<_>) = planned.into_iter().unzip();
        let lowered: Vec<_> = run_all(context.env.compute(), jobs)
            .await
            .into_iter()
            .zip(reservations)
            .map(|(prepared, held)| {
                prepared.map(|prepared| {
                    let shared = share_growth(&context.budget, &prepared, held);
                    (prepared, shared)
                })
            })
            .collect();
        for lowered in lowered {
            let (prepared, shared) = lowered?;
            for (table, prepared) in prepared {
                let reservation: Permit = Box::new(Arc::clone(&shared));
                queue(job, context, open, table, prepared, reservation).await?;
            }
        }
    }
    Ok(())
}

/// The table and plan of each of `parts`, a unit's, found in order, and the values dropped: a
/// part below the stream's table goes to its child table, added the first time unless the
/// stream's policy refuses or discards a new one.
async fn plan_parts(
    job: &PartitionJob,
    context: &PartitionContext,
    parts: Vec<Part>,
) -> Result<(Vec<(usize, Part, Arc<LoweringPlan>)>, u64), Error> {
    let mut planned = Vec::with_capacity(parts.len());
    let mut discarded = 0;
    for part in parts {
        let table = if part.path.is_empty() {
            job.table
        } else {
            match context.tables.admit_child(job.table, &part.path) {
                Admission::Add => context.tables.child(job.table, &part.path).await?,
                Admission::Discard => {
                    discarded += part.batch.num_rows() as u64;
                    continue;
                }
                Admission::Refuse => return Err(frozen(job, &part.path)),
            }
        };
        let incoming = Incoming {
            schema: schema_of(job, &part.batch)?,
            paths: part.columns.clone(),
        };
        let plan = context.tables.plan(table, incoming).await?;
        planned.push((table, part, plan));
    }
    Ok((planned, discarded))
}

/// The error for rows of a new array in a stream whose schema is frozen.
fn frozen(job: &PartitionJob, path: &[Arc<str>]) -> Error {
    let array = path.join(".");
    Error::schema(format!(
        "stream {}: array {array}: a new array would add a child table to a frozen schema",
        job.stream
    ))
    .with_code("schema_frozen")
    .with_stream(&job.stream)
}

/// `parts`, one batch once concatenated, normalized as `shape`.
fn split(stream: &StreamName, parts: &[RecordBatch], shape: &Shape) -> Result<Vec<Part>, Error> {
    let failed = |error: ArrowError| {
        Error::internal(format!("stream {stream}: normalizing a batch: {error}"))
    };
    let batch = arrow_select::concat::concat_batches(&parts[0].schema(), parts).map_err(failed)?;
    normalize::normalize(&batch, shape).map_err(failed)
}

/// The permits of `held` with the growth of `prepared`, a unit's lowered parts, beyond them
/// charged, shared by the parts: the unit's memory is held until the last part is staged.
fn share_growth(
    budget: &MemoryBudget,
    prepared: &[(usize, Prepared)],
    mut held: Held,
) -> Arc<Mutex<Vec<Permit>>> {
    let bytes: u64 = prepared
        .iter()
        .map(|(_, prepared)| {
            u64::try_from(prepared.batch.get_array_memory_size()).unwrap_or(u64::MAX)
        })
        .sum();
    held.permits
        .push(Box::new(budget.charge(bytes.saturating_sub(held.bytes))));
    Arc::new(Mutex::new(held.permits))
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
fn charge_growth(budget: &MemoryBudget, prepared: &Prepared, mut held: Held) -> Held {
    let bytes = u64::try_from(prepared.batch.get_array_memory_size()).unwrap_or(u64::MAX);
    held.permits
        .push(Box::new(budget.charge(bytes.saturating_sub(held.bytes))));
    held.bytes = held.bytes.max(bytes);
    held
}

/// `parts`, one batch once concatenated, as `plan` lowers it.
fn lower(parts: &[RecordBatch], plan: &LoweringPlan, stamp: &Stamp) -> Result<Prepared, Error> {
    // A lone batch concatenates to itself without a copy.
    let batch = arrow_select::concat::concat_batches(&parts[0].schema(), parts)
        .map_err(|error| Error::internal(format!("coalescing batches: {error}")))?;
    plan.prepare(&batch, None, stamp)
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
    let bytes = u64::try_from(prepared.batch.get_array_memory_size()).unwrap_or(u64::MAX);
    let lane = context.lanes.route(table, job.partition.id());
    context
        .lanes
        .write(
            lane,
            Write {
                table,
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
