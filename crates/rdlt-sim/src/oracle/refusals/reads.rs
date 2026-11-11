//! The refusals of a stream's reads that no schema decides: of a phase that reads incrementally
//! partitions that never end, which a full read before it finished, and of an incremental read of
//! such partitions that seals none of the rows it pushes.

use rdlt_connector::ReadMode;
use rdlt_engine::{Code, codes};

use crate::workload::{Relaxed, SimStream};

/// The refusal every run reading `stream` in `phase` meets, its source as `relaxed` leaves it.
///
/// Only a stream whose partitions never end, and whose reads end on rows, meets one, in a phase
/// that reads it incrementally. Where the phase before read it in full, its reads finished each
/// partition holding rows, which no cursor then resumes: `cursor_missing`, as each run plans it.
/// Otherwise its source checkpoints nowhere, so a read that pushes rows seals none of them:
/// `checkpoint_missing`, until an operator mends the source.
pub(super) fn refusal(stream: &SimStream, relaxed: Relaxed, phase: usize) -> Option<Code> {
    let ends_on_rows = !stream.final_checkpoint && !relaxed.mended;
    if !stream.unbounded || !ends_on_rows || stream.reads[phase] != ReadMode::Incremental {
        return None;
    }
    let partitions = 0..stream.partitions.len();
    let finished = phase.checked_sub(1).is_some_and(|last| {
        stream.reads[last] == ReadMode::Full
            && partitions
                .clone()
                .any(|partition| !stream.rows(partition, last).is_empty())
    });
    if finished {
        return Some(codes::CURSOR_MISSING);
    }
    partitions
        .clone()
        .any(|partition| !stream.read(partition, phase).is_empty())
        .then_some(codes::CHECKPOINT_MISSING)
}

#[cfg(test)]
mod tests;
