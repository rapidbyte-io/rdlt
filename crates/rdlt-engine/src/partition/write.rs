//! Writing what a partition gathered: shredding JSON, fitting each batch to its table, preparing
//! it and queueing it on its lane with the memory it holds.

#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;
use rdlt_connector::{Permit, TableSchema};

use super::coalesce::{Flushed, Unit};
use super::{OpenSegment, PartitionContext, PartitionJob, Progress};
use crate::budget::MemoryBudget;
use crate::error::{Error, ErrorKind};
use crate::lane::Write;
use crate::shred::{self, ShredError};
use crate::table::{Stamp, prepare};

/// Writes pushes gathered together: Arrow batches as one batch, JSON shredded into batches.
pub(super) async fn write_flushed(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    flushed: Flushed,
) -> Result<(), Error> {
    let permits = flushed.permits;
    match flushed.unit {
        Unit::Arrow(batches) => {
            // A lone batch concatenates to itself without a copy.
            let batch = arrow_select::concat::concat_batches(&batches[0].schema(), &batches)
                .map_err(|error| Error::internal(format!("coalescing batches: {error}")))?;
            // The pushes' permits now hold the copy.
            drop(batches);
            let held = Held {
                permits,
                bytes: flushed.bytes,
            };
            write(job, context, open, &batch, held).await
        }
        Unit::Json(pushes) => {
            let chunk_bytes = context.batch.chunk_bytes().get();
            let batches = shred::shred(context.env.compute(), &pushes, chunk_bytes)
                .await
                .map_err(|error| shred_failed(job, &error))?;
            drop(pushes);
            for (batch, held) in batches.iter().zip(hold(&context.budget, &batches, permits)) {
                write(job, context, open, batch, held).await?;
            }
            Ok(())
        }
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

/// Fits `batch` to its table, prepares it, and queues it on its lane.
///
/// The permits holding the batch's bytes travel with it. Growth beyond what they hold is charged
/// at once; later pushes pay it back by waiting.
async fn write(
    job: &PartitionJob,
    context: &PartitionContext,
    open: &mut OpenSegment,
    batch: &RecordBatch,
    held: Held,
) -> Result<(), Error> {
    let received = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
    if received == 0 {
        return Ok(());
    }
    let incoming = TableSchema::from_arrow(&batch.schema()).map_err(|error| {
        Error::schema(format!(
            "stream {}: a batch has no table schema: {error}",
            job.stream
        ))
        .with_code("batch_schema_invalid")
        .with_stream(&job.stream)
    })?;
    let (view, routes) = context.tables.fit(job.table, &incoming).await?;
    let stamp = Stamp {
        load_id: context.load_id,
        loaded_at: context.loaded_at,
        segment: open.id,
        first_row: open.received,
    };
    let prepared = prepare(&job.stream, &view, &incoming, batch, &routes, &stamp)?;
    open.received += received;
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
