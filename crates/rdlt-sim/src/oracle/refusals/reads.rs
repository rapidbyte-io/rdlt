//! The refusals of a stream's reads that no schema decides: of a phase that reads incrementally
//! partitions that never end, which a full read before it finished, and of an incremental read of
//! such partitions that seals none of the rows it pushes.

use rdlt_connector::ReadMode;
use rdlt_engine::{Code, codes};

use crate::workload::{Relaxed, SimStream};

/// A refusal of a stream's reads a phase's runs may meet.
#[derive(Debug, PartialEq, Eq)]
pub(super) struct Refusal {
    /// Its code.
    pub(super) code: Code,
    /// Whether every run reading the stream meets it.
    pub(super) must: bool,
}

/// The refusal runs reading `stream` in `phase` may meet, its source and its state as `relaxed`
/// leaves them.
///
/// Only a stream whose partitions never end, and whose reads end on rows, meets one, in a phase
/// that reads it incrementally. Where the phase before read it in full, its reads finished each
/// partition holding rows, which no position then resumes: `cursor_missing` as each run plans it,
/// until the stream is reset with its tables. Otherwise a read that pushes rows and checkpoints
/// after none of them is refused `checkpoint_missing`, until an operator mends the source: every
/// such read of a source that checkpoints on demand there, as it then checkpoints nowhere, and of
/// one that checkpoints naturally a read from where its partition stands of one checkpoint group
/// or less, as the next read of the rows a read leaves after its last checkpoint is.
pub(super) fn refusal(stream: &SimStream, relaxed: Relaxed, phase: usize) -> Option<Refusal> {
    let ends_on_rows = !stream.final_checkpoint && relaxed.mended.is_none();
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
    if finished && relaxed.reset != Some(phase) {
        let code = codes::CURSOR_MISSING;
        return Some(Refusal { code, must: true });
    }
    // The batches each partition's read in the phase pushes, from where the partition stands.
    let size = usize::try_from(stream.batch_rows).unwrap_or(1);
    let batches: Vec<usize> = partitions
        .map(|partition| {
            let rows = stream.rows(partition, phase).len();
            let begins = stream.begins(partition, phase, relaxed);
            rows.saturating_sub(begins).div_ceil(size)
        })
        .collect();
    let every = usize::try_from(stream.checkpoint_every).unwrap_or(1);
    let unsealed = |count: &usize| *count > 0 && (!stream.tails() || *count <= every);
    let code = codes::CHECKPOINT_MISSING;
    if batches.iter().any(unsealed) {
        Some(Refusal { code, must: true })
    } else if batches.iter().any(|count| *count > 0) {
        Some(Refusal { code, must: false })
    } else {
        None
    }
}

#[cfg(test)]
mod tests;
