//! Relieving a full log between commits: a chunk published to let go of what no replay needs,
//! and what each chunk holds of segments still open.

use std::sync::atomic::Ordering;

use super::super::frame::Frame;
use super::{Log, Settled, Written};
use crate::error::Error;

impl Log {
    /// Publishes chunks between commits while each lets the log hold less.
    ///
    /// The chunks no replay needs go, those whose receipts no chunk recorded yet among them, and
    /// so do the chunks holding frames of settled segments once the frames of their open segments
    /// are carried, which the room one relief frees may let the next copy. Nothing is published
    /// while the chunk staged holds seals, which go with the commit that follows them.
    pub(super) async fn relieve(&mut self) -> Result<(), Error> {
        self.failure()?;
        // Each relief that goes on frees some of what the log holds, so this ends.
        while !self.sealing && self.relieved().await? > 0 {}
        Ok(())
    }

    /// Publishes the chunk staged between commits where that lets anything go: the bytes the log
    /// no longer holds.
    async fn relieved(&mut self) -> Result<u64, Error> {
        // A chunk holding no batch or commit is no larger than the chunk publishing would add.
        let unneeded = self.written.iter().any(|(number, written)| {
            *number < self.chunk
                && !(written.batches.is_empty() && written.commits.is_empty())
                && !self.needed(written)
        });
        let before = self.shared.held.load(Ordering::Relaxed);
        let carried = self.carry_to_free().await?;
        if !(unneeded || carried) {
            return Ok(0);
        }
        self.append(Frame::Relieved.encode()?).await?;
        self.publish().await?;
        Ok(before.saturating_sub(self.shared.held.load(Ordering::Relaxed)))
    }

    /// Counts again what each chunk holds of open segments, and the most a carry of a chunk
    /// published copies.
    pub(super) fn refresh_copies(&mut self) {
        let settled = &self.settled;
        for written in self.written.values_mut() {
            written.open = open(written, settled);
        }
        self.copied = self
            .written
            .range(..self.chunk)
            .map(|(_, written)| copied(written))
            .max()
            .unwrap_or(0);
        self.note_copies();
    }

    /// Notes the most a carry of a chunk published or of the chunk staged copies.
    pub(super) fn note_copies(&self) {
        let staged = self.written.get(&self.chunk).map_or(0, copied);
        let copied = self.copied.max(staged);
        self.shared.copied.store(copied, Ordering::Relaxed);
    }
}

/// Bytes: what the batch frames of `written`'s segments not `settled` take.
fn open(written: &Written, settled: &Settled) -> u64 {
    written
        .batches
        .iter()
        .filter(|logged| !settled.contains(logged.segment))
        .map(|logged| logged.span.len)
        .fold(0, u64::saturating_add)
}

/// Bytes: what a carry of `written` copies before it frees the frames of other segments beside
/// them: the batch frames of its open segments, where it was not carried and holds frames of
/// another segment too.
fn copied(written: &Written) -> u64 {
    if written.carried || written.segments.len() < 2 {
        return 0;
    }
    written.open
}
