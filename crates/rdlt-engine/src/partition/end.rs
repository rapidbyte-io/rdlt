//! How a partition's read ends: where the partition resumes, and the refusal of a read that
//! sealed none of the rows it pushed.

use rdlt_connector::PartitionState;

use super::{Ingested, PartitionJob};
use crate::error::{Error, ErrorKind, codes};

/// Where a partition that read to its end resumes, if anywhere new.
///
/// Rows read after the last checkpoint have no cursor that resumes past them, so committing them
/// marks the partition `Done`, whether they were written, its policy discarded them all, its
/// filters dropped them or it ignores them, and the seal carries what was done to them.
/// Otherwise the partition resumes from its last cursor, so an incremental read picks up rows the
/// source adds later. A partition that read nothing and never checkpointed records no position,
/// so its next read starts from the beginning again.
///
/// Where the read `reads_again`, as a read of an unbounded partition that is not full does, the
/// rows after its last checkpoint are not sealed: its next read, from that checkpoint, reads them
/// again.
pub(super) fn end_state(ingested: &Ingested, reads_again: bool) -> Option<PartitionState> {
    match (&ingested.last_cursor, ingested.open.read()) {
        (Some(cursor), 0) => Some(PartitionState::Cursor(cursor.clone())),
        (None, 0) => None,
        _ if reads_again => None,
        _ => Some(PartitionState::Done),
    }
}

/// The error for a read of `job` that pushed rows and no checkpoint after any, so none of them
/// could land: its source broke the read's contract, which no retry mends.
pub(super) fn checkpoint_missing(job: &PartitionJob) -> Error {
    Error::new(
        ErrorKind::Source,
        format!(
            "stream {}: partition {} pushed rows and no checkpoint after any, and read \
             incrementally a partition that never ends commits only the rows before a \
             checkpoint: checkpoint after the last row",
            job.stream,
            job.partition.id()
        ),
    )
    .with_code(codes::CHECKPOINT_MISSING)
    .with_stream(&job.stream)
}
