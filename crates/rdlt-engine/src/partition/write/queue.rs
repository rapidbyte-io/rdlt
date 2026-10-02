//! Queueing a lowered batch on its lane, with what holds its memory.

use arrow_array::RecordBatch;
use rdlt_connector::Permit;
use rdlt_wire::{Weigher, Weight};

use super::super::{OpenSegment, PartitionContext, PartitionJob, Progress};
use crate::error::Error;
use crate::lane::Write;
use crate::table::Prepared;

/// The bytes the rows of `batch` take, as reports and the commit policy count what was written.
///
/// They are what a frame holding only those rows holds, as the wire weighs it, with each
/// dictionary's values: a slice counts its own rows, whatever it keeps alive, which is the
/// budget's to charge.
pub(super) fn written_bytes(batch: &RecordBatch) -> u64 {
    let mut weigher = Weigher::new(batch);
    let mut weight = Weight::default();
    for dictionary in weigher.dictionaries() {
        weight += dictionary;
    }
    weight += weigher.weigh_rows(0..batch.num_rows());
    weight.frame_bytes()
}

/// Queues `prepared` on `table`'s lane with `reservation`, the permits holding its bytes, which
/// travel with it.
///
/// Growth beyond what a push held was charged at once; later pushes pay it back by waiting.
pub(super) async fn queue(
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
