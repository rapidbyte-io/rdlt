//! Carrying the frames of segments still open out of old chunks, so a segment its partition
//! keeps open keeps no settled frame of another segment.
//!
//! The copies go to the chunk staged, and the old chunk is needed no more once that chunk is
//! published: its deletion follows the publish, so the log always holds one copy a replay reads.
//! Replay stages only the segments of commits without receipts, and the old chunk's commits all
//! have theirs.

use std::sync::atomic::Ordering;

use rdlt_connector::SegmentId;

use super::super::frame::Frame;
use super::super::store::Chunk;
use super::chunk::Logged;
use super::{Kept, Log};
use crate::error::Error;

/// The old chunks a relief carries, what it copies of them, and what deleting them frees.
#[derive(Default)]
struct Chosen {
    numbers: Vec<u64>,
    copied: u64,
    gained: u64,
}

impl Log {
    /// Carries the frames of the open segments of each old chunk that holds at least as many
    /// bytes beside them as they take, its frames of settled segments, its header, seals, commit
    /// and end: what a carry copies is never more than what it frees beside the copy.
    ///
    /// A carry the log has no room for, beside the frames that end the chunk staged, or that
    /// would take the chunk staged past what a chunk holds, is left undone: the old chunk stays,
    /// and a later carry may free it.
    pub(super) async fn carry(&mut self) -> Result<(), Error> {
        for number in self.carriable() {
            let written = &self.written[&number];
            let copies = written.copies();
            if copies == 0 || !worth(written.len, copies) || !self.takes(copies) {
                continue;
            }
            if !self.shared.reserve(copies, self.shared.kept(Kept::Closing)) {
                break;
            }
            let wrote = self.carry_chunk(number).await?;
            self.shared.release(copies.saturating_sub(wrote));
        }
        Ok(())
    }

    /// Whether the chunk staged takes `copies` bytes of carried frames: they keep it within what
    /// a chunk holds, or it holds no batch frame yet.
    fn takes(&self, copies: u64) -> bool {
        self.written.get(&self.chunk).is_none_or(|written| {
            written.batches.is_empty() || written.len.saturating_add(copies) <= self.most()
        })
    }

    /// Carries the frames of the open segments of the old chunks, the oldest first, each the log
    /// has room to copy beside those before it and the frames that end the chunks it writes, and
    /// each that keeps the chunk it goes to within what a chunk holds: whether publishing the
    /// chunk then frees more than the relief writes, with `unneeded` bytes of chunks no replay
    /// needs freed however the carry goes.
    ///
    /// The copies go to the chunk staged where it takes them; where it holds batch frames and
    /// takes none, it is published first and they go to the next.
    pub(super) async fn carry_to_free(&mut self, unneeded: u64) -> Result<bool, Error> {
        let limit = self.shared.limit.load(Ordering::SeqCst);
        let held = self.shared.held.load(Ordering::SeqCst);
        // The copies keep the room every frame keeps, for ending the chunk staged and one more.
        let (ending, next) = self.ending()?;
        let closing = ending.saturating_add(next);
        let room = limit.saturating_sub(held).saturating_sub(closing);
        let most = self.most();
        let staged = self.written.get(&self.chunk);
        let len = staged.map_or(0, |written| written.len);
        let batched = staged.is_some_and(|written| !written.batches.is_empty());
        let mut chosen = self.chosen(room, |copied, copy| {
            copied == 0 && !batched || len.saturating_add(copied).saturating_add(copy) <= most
        });
        let mut first = false;
        if chosen.numbers.is_empty() && batched {
            let fresh = self.chosen(room, |copied, copy| {
                copied == 0 || next.saturating_add(copied).saturating_add(copy) <= most
            });
            first = !fresh.numbers.is_empty();
            if first {
                chosen = fresh;
            }
        }
        let ends = if first { closing } else { ending };
        let gained = chosen.gained.saturating_add(unneeded);
        // With nothing to copy it needs no room, where a commit larger than any before took the
        // log past what it may hold.
        if gained <= chosen.copied.saturating_add(ends)
            || chosen.copied > 0 && !self.shared.reserve(chosen.copied, closing)
        {
            return Ok(false);
        }
        if first {
            self.append(Frame::Relieved.encode()?).await?;
            self.publish().await?;
        }
        let mut wrote = 0_u64;
        for number in chosen.numbers {
            wrote = wrote.saturating_add(self.carry_chunk(number).await?);
        }
        self.shared.release(chosen.copied.saturating_sub(wrote));
        Ok(true)
    }

    /// The old chunks to carry, the oldest first, each whose copy fits `room` beside those before
    /// it and `fits` the chunk it goes to, given what those before it copied.
    fn chosen(&self, mut room: u64, fits: impl Fn(u64, u64) -> bool) -> Chosen {
        let mut chosen = Chosen::default();
        for number in self.carriable() {
            let written = &self.written[&number];
            let copy = written.copies();
            if copy == 0 || copy > room || !fits(chosen.copied, copy) {
                continue;
            }
            room -= copy;
            chosen.copied = chosen.copied.saturating_add(copy);
            chosen.gained = chosen.gained.saturating_add(written.len);
            chosen.numbers.push(number);
        }
        chosen
    }

    /// The old chunks not carried yet whose commits all have receipts and whose segments are
    /// each settled or open: no commit waiting for its receipt takes one.
    fn carriable(&self) -> Vec<u64> {
        self.written
            .iter()
            .filter(|(number, written)| **number < self.chunk && !written.carried)
            .filter(|(_, written)| {
                written
                    .commits
                    .iter()
                    .all(|seq| !self.pending.contains_key(seq))
                    && written.segments.iter().all(|segment| !self.taken(*segment))
            })
            .map(|(number, _)| *number)
            .collect()
    }

    /// Whether a commit waiting for its receipt takes `segment`.
    pub(super) fn taken(&self, segment: SegmentId) -> bool {
        self.pending
            .values()
            .any(|segments| segments.contains(segment))
    }

    /// Copies the frames of chunk `number`'s open segments to the chunk staged, in the order
    /// they were logged and each table's schema before its first, counted already, a piece at a
    /// time, and marks the chunk carried: the bytes written.
    async fn carry_chunk(&mut self, number: u64) -> Result<u64, Error> {
        let Some(written) = self.written.get(&number) else {
            return Ok(0);
        };
        let open: Vec<Logged> = written
            .batches
            .iter()
            .filter(|logged| !self.settled.contains(logged.segment))
            .copied()
            .collect();
        let schemas = written.schemas.clone();
        let from = Chunk {
            load: self.owner.load,
            number,
        };
        let mut wrote = 0_u64;
        for logged in open {
            if !self.current().schemas.contains_key(&logged.table) {
                let span = schemas.get(&logged.table).copied().ok_or_else(|| {
                    Error::internal(format!(
                        "a logged batch of table {}, whose schema its chunk lacks",
                        logged.table
                    ))
                })?;
                let span = self.copy(from, span).await?;
                self.current().schemas.insert(logged.table, span);
                wrote = wrote.saturating_add(span.len);
            }
            let span = self.copy(from, logged.span).await?;
            wrote = wrote.saturating_add(span.len);
            self.logged(Logged { span, ..logged });
        }
        if let Some(written) = self.written.get_mut(&number) {
            written.carried = true;
        }
        Ok(wrote)
    }
}

/// Whether a carry of `copies` bytes out of a chunk of `len` frees at least as much beside them.
fn worth(len: u64, copies: u64) -> bool {
    len.saturating_sub(copies) >= copies
}
