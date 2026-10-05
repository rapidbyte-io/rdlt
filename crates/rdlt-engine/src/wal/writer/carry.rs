//! Carrying the frames of segments still open out of old chunks, so a segment its partition
//! keeps open keeps no settled frame of another segment.
//!
//! The copies go to the chunk staged, and the old chunk is needed no more once that chunk is
//! published: its deletion follows the publish, so the log always holds one copy a replay reads.
//! Replay stages only the segments of commits without receipts, and the old chunk's commits all
//! have theirs.

use bytes::Bytes;
use rdlt_connector::SegmentId;

use super::{Kept, Log, Logged, Span, Written};
use crate::error::Error;

impl Log {
    /// Carries the frames of the open segments of each old chunk that holds at least as many
    /// bytes beside them as they take, its frames of settled segments, its header, seals, commit
    /// and end: what a carry copies is never more than what it frees beside the copy.
    ///
    /// A carry the log has no room for, beside the frames that end the chunk staged, is left
    /// undone: the old chunk stays, and a later carry may free it.
    pub(super) async fn carry(&mut self) -> Result<(), Error> {
        for number in self.carriable() {
            let written = &self.written[&number];
            let copies = copies(written);
            if copies == 0 || !worth(written.len, copies) {
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

    /// Carries the frames of the open segments of the old chunks into the chunk staged, the
    /// oldest first, each the log has room to copy beside those before it and the frames that
    /// end the chunk staged: whether publishing the chunk then frees more than it writes, with
    /// `unneeded` bytes of chunks no replay needs freed however the carry goes.
    pub(super) async fn carry_to_free(&mut self, unneeded: u64) -> Result<bool, Error> {
        let limit = self.shared.limit.load(std::sync::atomic::Ordering::SeqCst);
        let held = self.shared.held.load(std::sync::atomic::Ordering::SeqCst);
        // The relief ends the chunk staged alone: it frees more than it writes, so the room
        // the next chunk needs is there after it as before.
        let (closing, _) = self.ending()?;
        let mut room = limit.saturating_sub(held).saturating_sub(closing);
        let (mut copied, mut gained, mut chosen) = (0_u64, unneeded, Vec::new());
        for number in self.carriable() {
            let written = &self.written[&number];
            let copy = copies(written);
            let Some(left) = room.checked_sub(copy).filter(|_| copy > 0) else {
                continue;
            };
            room = left;
            copied = copied.saturating_add(copy);
            gained = gained.saturating_add(written.len);
            chosen.push(number);
        }
        // What the publish writes beside the copies is at most what ends the chunk staged, which
        // it frees more than: with nothing to copy it needs no room, where a commit larger than
        // any before took the log past what it may hold.
        if gained <= copied.saturating_add(closing)
            || copied > 0 && !self.shared.reserve(copied, closing)
        {
            return Ok(false);
        }
        let mut wrote = 0_u64;
        for number in chosen {
            wrote = wrote.saturating_add(self.carry_chunk(number).await?);
        }
        self.shared.release(copied.saturating_sub(wrote));
        Ok(true)
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
    fn taken(&self, segment: SegmentId) -> bool {
        self.pending
            .values()
            .any(|segments| segments.contains(segment))
    }

    /// Writes the frames of chunk `number`'s open segments to the chunk staged, in the order
    /// they were logged and each table's schema before its first, counted already, and marks the
    /// chunk carried: the bytes written.
    ///
    /// Each frame is read into memory the log's share of the budget holds for the largest of
    /// them; a chunk the share has no room for at once is not carried.
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
        let largest = open
            .iter()
            .map(|logged| logged.span)
            .chain(schemas.values().copied())
            .map(|span| span.len)
            .max()
            .unwrap_or(0);
        let Some(_reading) = self.budget.try_acquire_log(largest) else {
            return Ok(0);
        };
        let from = super::Chunk {
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
                let schema = self.read(from, span).await?;
                self.describe(logged.table, schema).await?;
                wrote = wrote.saturating_add(span.len);
            }
            let frame = self.read(from, logged.span).await?;
            let span = self.written_out(frame).await?;
            wrote = wrote.saturating_add(span.len);
            let current = self.current();
            current.open += span.len;
            current.segments.insert(logged.segment);
            current.batches.push(Logged { span, ..logged });
        }
        if let Some(written) = self.written.get_mut(&number) {
            written.carried = true;
        }
        self.note_room();
        Ok(wrote)
    }

    /// The frame at `span` of chunk `chunk`.
    async fn read(&mut self, chunk: super::Chunk, span: Span) -> Result<Bytes, Error> {
        self.store
            .read(&self.owner.pipeline, chunk, span.offset, span.len)
            .await
            .map_err(|error| self.lost(error))
    }
}

/// Bytes: what a carry of `written` copies at most: the batch frames of its open segments and
/// every schema frame it holds; none where it holds no open frame.
fn copies(written: &Written) -> u64 {
    if written.open == 0 {
        return 0;
    }
    written
        .schemas
        .values()
        .map(|span| span.len)
        .fold(written.open, u64::saturating_add)
}

/// Whether a carry of `copies` bytes out of a chunk of `len` frees at least as much beside them.
fn worth(len: u64, copies: u64) -> bool {
    len.saturating_sub(copies) >= copies
}
