//! Writing what a partition gathered: shredding JSON, fitting each batch to its table, preparing
//! it and queueing it on its lane with the memory it holds.

#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;
use rdlt_connector::{Permit, TableSchema};

use super::coalesce::{Flushed, Unit};
use super::{OpenSegment, PartitionContext, PartitionJob, Progress};
use crate::budget::MemoryBudget;
use crate::compute::run_all;
use crate::error::{Error, ErrorKind};
use crate::lane::Write;
use crate::shred::{self, ShredError};
use crate::table::{LoweringPlan, Prepared, Stamp};

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
/// Each unit's plan is found in order, since finding it may change the table. Then every unit is
/// concatenated and lowered on the compute pool at once, and queued as its turn comes.
async fn write(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    units: Vec<(Vec<RecordBatch>, Held)>,
) -> Result<(), Error> {
    let mut jobs = Vec::with_capacity(units.len());
    let mut reservations = Vec::with_capacity(units.len());
    for (parts, held) in units {
        let received = parts
            .iter()
            .map(|batch| u64::try_from(batch.num_rows()).unwrap_or(u64::MAX))
            .sum::<u64>();
        if received == 0 {
            continue;
        }
        let incoming = TableSchema::from_arrow(&parts[0].schema()).map_err(|error| {
            Error::schema(format!(
                "stream {}: a batch has no table schema: {error}",
                job.stream
            ))
            .with_code("batch_schema_invalid")
            .with_stream(&job.stream)
        })?;
        let plan = context.tables.plan(job.table, incoming).await?;
        let stamp = Stamp {
            load_id: context.load_id,
            loaded_at: context.loaded_at,
            segment: open.id,
            first_row: open.received,
        };
        open.received += received;
        jobs.push(move || lower(&parts, &plan, &stamp));
        reservations.push(held);
    }
    let prepared = run_all(context.env.compute(), jobs).await;
    for (prepared, held) in prepared.into_iter().zip(reservations) {
        queue(job, context, open, prepared?, held).await?;
    }
    Ok(())
}

/// `parts`, one batch once concatenated, as `plan` lowers it.
fn lower(parts: &[RecordBatch], plan: &LoweringPlan, stamp: &Stamp) -> Result<Prepared, Error> {
    // A lone batch concatenates to itself without a copy.
    let batch = arrow_select::concat::concat_batches(&parts[0].schema(), parts)
        .map_err(|error| Error::internal(format!("coalescing batches: {error}")))?;
    plan.prepare(&batch, stamp)
}

/// Queues `prepared` on its lane, with `held` and the growth beyond it.
///
/// The permits holding the batch's bytes travel with it. Growth beyond what they hold is charged
/// at once; later pushes pay it back by waiting.
async fn queue(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    prepared: Prepared,
    held: Held,
) -> Result<(), Error> {
    open.discarded_rows += prepared.discarded_rows;
    open.discarded_values += prepared.discarded_values;
    let rows = u64::try_from(prepared.batch.num_rows()).unwrap_or(u64::MAX);
    if rows == 0 {
        return Ok(());
    }
    let bytes = u64::try_from(prepared.batch.get_array_memory_size()).unwrap_or(u64::MAX);
    let growth = context.budget.charge(bytes.saturating_sub(held.bytes));
    let lane = context.lanes.route(job.table, job.partition.id());
    context
        .lanes
        .write(
            lane,
            Write {
                table: job.table,
                segment: open.id,
                batch: prepared.batch,
                reservation: Box::new((held.permits, growth)),
            },
        )
        .await?;
    open.rows += rows;
    open.bytes += bytes;
    context.report(Progress::Written { rows, bytes })
}
