//! What the first phase's reads of each stream ended on, as the engine took them.

#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::mem;
use std::ops::Range;

use parking_lot::Mutex;

use crate::workload::End;

/// What the first phase's reads of each stream, by index, ended on: those the running attempt's
/// reads noted, and those kept once their attempt ended.
#[derive(Debug, Default)]
pub(crate) struct Ends {
    running: Mutex<BTreeSet<(usize, End)>>,
    kept: Mutex<BTreeSet<(usize, End)>>,
}

impl Ends {
    /// Notes that a read of stream `stream` ended on `end`.
    pub(crate) fn note(&self, stream: usize, end: End) {
        self.running.lock().insert((stream, end));
    }

    /// Keeps what the reads of the attempt that ended noted, but rows after a checkpoint where
    /// the engine refused a read of the stream, by its index among `refused`, for want of a
    /// checkpoint after its rows: an engine that refuses such a read is wrong, and the reads that
    /// it refused reached nothing.
    pub(crate) fn keep(&self, refused: &BTreeSet<usize>) {
        let running = mem::take(&mut *self.running.lock());
        let taken = |(stream, end): &(usize, End)| *end != End::Tail || !refused.contains(stream);
        self.kept.lock().extend(running.into_iter().filter(taken));
    }

    /// What the kept reads of stream `stream` ended on.
    pub(crate) fn of(&self, stream: usize) -> Vec<End> {
        let kept = self.kept.lock();
        let ends = kept.range((stream, End::Checkpoint)..=(stream, End::Unmarked));
        ends.map(|(_, end)| *end).collect()
    }
}

/// What a read that sent the rows at the offsets `sent` ended on, where the engine took its last
/// checkpoint before the row at `checkpointed`, if anywhere: a checkpoint after its last row, rows
/// after a checkpoint after one of its rows, or rows with no checkpoint after any, as a
/// checkpoint at its start comes after none of them; nothing where it sent no rows.
pub(crate) fn end(checkpointed: Option<u64>, sent: Range<usize>) -> Option<End> {
    let (start, last) = (sent.start as u64, sent.end as u64);
    match checkpointed {
        _ if sent.is_empty() => None,
        Some(at) if at == last => Some(End::Checkpoint),
        Some(at) if at > start => Some(End::Tail),
        _ => Some(End::Unmarked),
    }
}
