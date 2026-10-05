//! Relieving a full log between commits, and the room the writer keeps for its own frames.

use std::sync::atomic::Ordering;

use rdlt_connector::CommitSeq;

use super::super::frame::{self, End, Frame};
use super::{Log, Settled, Written, count};
use crate::error::Error;

impl Log {
    /// Publishes chunks between commits while each lets the log hold less.
    ///
    /// The chunks no replay needs go, those whose receipts no chunk recorded yet among them, and
    /// so do the chunks whose open frames are carried, which gathers small chunks into one and
    /// frees what others hold of settled segments; the room one relief frees may let the next
    /// copy more. Nothing is published while the chunk staged holds seals, which go with the
    /// commit that follows them.
    pub(super) async fn relieve(&mut self) -> Result<(), Error> {
        self.failure()?;
        // Each relief that goes on frees some of what the log holds, so this ends.
        while !self.sealing && self.relieved().await? > 0 {}
        Ok(())
    }

    /// Publishes the chunk staged between commits where that frees more than it writes: the
    /// bytes it freed, less those it wrote.
    async fn relieved(&mut self) -> Result<u64, Error> {
        let unneeded = self
            .written
            .range(..self.chunk)
            .filter(|(_, written)| !self.needed(written))
            .map(|(_, written)| written.len)
            .fold(0, u64::saturating_add);
        let staged = self
            .written
            .get(&self.chunk)
            .map_or(0, |written| written.len);
        if !self.carry_to_free(unneeded).await? {
            return Ok(0);
        }
        self.append(Frame::Relieved.encode()?).await?;
        let freed = self.publish().await?;
        let published = self.written.get(&(self.chunk - 1));
        let wrote = published.map_or(0, |written| written.len.saturating_sub(staged));
        Ok(freed.saturating_sub(wrote))
    }

    /// Counts again what each chunk holds of open segments, and notes the room the writer keeps
    /// to end the chunk staged and one more after it.
    pub(super) fn note_room(&mut self) {
        let settled = &self.settled;
        for written in self.written.values_mut() {
            written.open = open(written, settled);
        }
        let closing = self
            .ending()
            .map_or(u64::MAX, |(ending, next)| ending.saturating_add(next));
        self.shared.closing.store(closing, Ordering::SeqCst);
    }

    /// Bytes: what ending the chunk staged writes at most, and what staging and ending one more
    /// after it writes: a preamble and header where the chunk is not staged yet, a closing frame,
    /// and an end naming every chunk and commit the log then holds.
    pub(super) fn ending(&self) -> Result<(u64, u64), Error> {
        let closing = count(Frame::Relieved.encode()?.len());
        let ended = |live: &[u64], received: &[CommitSeq]| {
            let end = Frame::End(End {
                live: live.to_vec(),
                received: received.to_vec(),
            });
            Ok::<u64, Error>(count(end.encode()?.len()).saturating_add(closing))
        };
        let staged =
            |chunk: u64| Ok::<u64, Error>(count(frame::PREAMBLE + self.header(chunk)?.len()));
        let mut live: Vec<u64> = self.written.keys().copied().collect();
        let mut received: Vec<CommitSeq> = self
            .written
            .values()
            .flat_map(|written| written.commits.iter().copied())
            .collect();
        let staging = if self.staged.is_some() {
            0
        } else {
            staged(self.chunk)?
        };
        let ending = staging.saturating_add(ended(&live, &received)?);
        // The chunk after names this one too, and the commit that may end this one.
        live.push(self.chunk);
        received.push(self.last.map_or(CommitSeq::FIRST, CommitSeq::next));
        let next = staged(self.chunk.saturating_add(1))?.saturating_add(ended(&live, &received)?);
        Ok((ending, next))
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
