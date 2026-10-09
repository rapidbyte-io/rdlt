//! Relieving a full log between commits, and the room the writer keeps for its own frames.

use std::sync::atomic::Ordering;

use rdlt_connector::CommitSeq;

use super::super::frame::{self, Frame};
use super::{Log, count};
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
        // Each relief that goes on frees more of what the log holds than it writes, so this
        // ends.
        while !self.sealing && self.relieved().await? {}
        Ok(())
    }

    /// Publishes the chunk staged between commits where that frees more than it writes: whether
    /// it did.
    async fn relieved(&mut self) -> Result<bool, Error> {
        let unneeded = self
            .written
            .range(..self.chunk)
            .filter(|(_, written)| !self.needed(written))
            .map(|(_, written)| written.len)
            .fold(0, u64::saturating_add);
        if !self.carry_to_free(unneeded).await? {
            return Ok(false);
        }
        self.append(Frame::Relieved.encode()?).await?;
        self.publish().await?;
        Ok(true)
    }

    /// Notes the room the writer keeps for its own frames: what ends the chunk staged and one
    /// more after it.
    pub(super) fn note_room(&mut self) {
        let closing = self
            .ending()
            .map_or(u64::MAX, |(ending, next)| ending.saturating_add(next));
        self.shared.closing.store(closing, Ordering::SeqCst);
    }

    /// Bytes: what ending the chunk staged writes at most, and what staging and ending one more
    /// after it writes: a preamble and header where the chunk is not staged yet, a closing frame,
    /// and an end naming every chunk and commit the log then holds.
    pub(super) fn ending(&self) -> Result<(u64, u64), Error> {
        let closing = count(frame::HEAD);
        let staged = |chunk: u64| -> Result<u64, Error> {
            let header = frame::header_len(&self.chunk_header(chunk))?;
            Ok(count(frame::PREAMBLE).saturating_add(header))
        };
        let live = || self.written.keys().copied();
        let received = || {
            self.written
                .values()
                .flat_map(|written| written.commits.iter().copied().map(CommitSeq::get))
        };
        let staging = if self.staged.is_some() {
            0
        } else {
            staged(self.chunk)?
        };
        let ending = staging
            .saturating_add(frame::end_len(live(), received()))
            .saturating_add(closing);
        // The chunk after names this one too, and the commit that may end this one.
        let after = self.last.map_or(CommitSeq::FIRST, CommitSeq::next).get();
        let next = staged(self.chunk.saturating_add(1))?
            .saturating_add(frame::end_len(
                live().chain([self.chunk]),
                received().chain([after]),
            ))
            .saturating_add(closing);
        Ok((ending, next))
    }
}
