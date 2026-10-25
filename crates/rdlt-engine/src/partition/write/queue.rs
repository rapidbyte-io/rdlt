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
    let bytes = written_bytes(&prepared.batch);
    // The batch is held with what holds it from here on, however the queueing ends.
    let write = Write {
        table,
        version: prepared.view.table.version,
        segment: open.id,
        batch: prepared.batch,
        reservation,
    };
    if rows == 0 {
        return Ok(());
    }
    let lane = context.lanes.route(table, job.index);
    context.lanes.write(lane, write).await?;
    open.rows += rows;
    open.bytes += bytes;
    match written(job, rows, bytes) {
        Some(progress) => context.report(progress),
        None => Ok(()),
    }
}

/// What a batch of `rows` of `bytes` that `job` queued tells the coordinator: the rows of a
/// partition that seals when a barrier asks count toward a commit from when they are written,
/// another's only once sealed, so it tells nothing.
pub(super) fn written(job: &PartitionJob, rows: u64, bytes: u64) -> Option<Progress> {
    job.on_demand.then_some(Progress::Written {
        partition: job.index,
        rows,
        bytes,
    })
}
