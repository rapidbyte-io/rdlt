//! Carrying the frames of segments still open out of old chunks in a relief, so the settled frames
//! of other segments those chunks hold go when the log is short of room.
//!
//! The copies go to the chunk the relief publishes, and the old chunk is needed no more once that
//! chunk is published: its deletion follows the publish, so the log always holds one copy a
//! replay reads.
//! Replay stages only the segments of commits without receipts, and the old chunk's commits all
//! have theirs.

use std::cmp::Reverse;
use std::collections::BTreeSet;
use std::sync::atomic::Ordering;

use rdlt_connector::SegmentId;

use super::super::frame::Frame;
use super::super::store::Chunk;
use super::Log;
use super::chunk::{Logged, Written};
use crate::error::Error;

/// An old chunk a relief may carry: its number, and the tables its open frames are of.
struct Candidate {
    number: u64,
    tables: BTreeSet<u32>,
}

/// The old chunks a relief carries, what it copies of them, and what deleting them frees.
#[derive(Default)]
struct Chosen {
    numbers: Vec<u64>,
    copied: u64,
    gained: u64,
}

impl Log {
    /// Carries the frames of the open segments of the old chunks, those that free the most first,
    /// each the log has room to copy beside those before it and the frames that end the chunks
    /// it writes, and each that keeps the chunk it goes to within what a chunk holds: whether
    /// publishing the chunk then frees more than the relief writes, with `unneeded` bytes of
    /// chunks no replay needs freed however the carry goes.
    ///
    /// The copies go to the chunk staged where those it takes pay; where it holds batch frames and
    /// takes none that do, it is published first and they go to the next.
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
        // What the chunk staged holds, with its header where it is not staged yet, and what
        // ends it, beside the copies.
        let beside = len.saturating_add(ending);
        let described = staged.map(|written| written.schemas.keys().copied().collect());
        let candidates = self.candidates();
        let mut chosen = self.chosen(
            &candidates,
            room,
            described.unwrap_or_default(),
            |copied, copy| {
                copied == 0 && !batched
                    || beside.saturating_add(copied).saturating_add(copy) <= most
            },
        );
        let mut first = false;
        // Where what fits beside the chunk staged's batch frames does not pay, what fits a chunk
        // of its own may.
        if batched && chosen.gained.saturating_add(unneeded) <= chosen.copied.saturating_add(ending)
        {
            let fresh = self.chosen(&candidates, room, BTreeSet::new(), |copied, copy| {
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
            self.remove_unneeded().await?;
        }
        let mut wrote = 0_u64;
        for number in chosen.numbers {
            wrote = wrote.saturating_add(self.carry_chunk(number).await?);
        }
        self.shared.release(chosen.copied.saturating_sub(wrote));
        Ok(true)
    }

    /// The old chunks a relief may carry, in the order it takes them: first those holding
    /// settled frames, those that free the most beside their copy first, then those holding open
    /// frames alone, those with the fewest open bytes first, the oldest first among equals.
    ///
    /// A chunk that frees little beside a large copy, as one a relief gathered open frames into,
    /// comes after the chunks that free more, and takes only the room they leave; and where any
    /// two chunks of open frames alone fit a chunk beside every schema frame, the two with the
    /// fewest open bytes do, so a gathered chunk taken first leaves none of them apart.
    fn candidates(&self) -> Vec<Candidate> {
        let mut candidates: Vec<Candidate> = self
            .carriable()
            .into_iter()
            .map(|number| {
                let tables = self.written[&number]
                    .batches
                    .iter()
                    .filter(|logged| !self.settled.contains(logged.segment))
                    .map(|logged| logged.table)
                    .collect();
                Candidate { number, tables }
            })
            .collect();
        candidates.sort_by_key(|candidate| {
            let written = &self.written[&candidate.number];
            if self.settles(written) {
                let frees = written.len.saturating_sub(written.copies());
                (false, Reverse(frees), 0)
            } else {
                (true, Reverse(0), written.open)
            }
        });
        candidates
    }

    /// Of `candidates`, in turn, those to carry: each whose copy fits `room` beside those before
    /// it and `fits` the chunk it goes to, given what those before it copied, into a chunk
    /// holding the schema frames of the tables `described` names.
    fn chosen(
        &self,
        candidates: &[Candidate],
        mut room: u64,
        mut described: BTreeSet<u32>,
        fits: impl Fn(u64, u64) -> bool,
    ) -> Chosen {
        let mut chosen = Chosen::default();
        for candidate in candidates {
            let written = &self.written[&candidate.number];
            let copy = copy_len(written, &candidate.tables, &described);
            if copy == 0 || copy > room || !fits(chosen.copied, copy) {
                continue;
            }
            described.extend(&candidate.tables);
            room -= copy;
            chosen.copied = chosen.copied.saturating_add(copy);
            chosen.gained = chosen.gained.saturating_add(written.len);
            chosen.numbers.push(candidate.number);
        }
        chosen
    }

    /// Whether `written` holds frames a carry frees beside what holds its open frames: a settled
    /// segment's, or a commit's.
    fn settles(&self, written: &Written) -> bool {
        !written.commits.is_empty()
            || written
                .segments
                .iter()
                .any(|segment| self.settled.contains(*segment))
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
        self.tally
            .add(|counters| counters.log.carried = counters.log.carried.saturating_add(wrote));
        Ok(wrote)
    }
}

/// Bytes: what a carry of `written`, whose open frames are of `tables`, writes to a chunk
/// holding the schema frames of the tables `described` names: its open frames, and the schema
/// frame of each of their tables the chunk lacks.
fn copy_len(written: &Written, tables: &BTreeSet<u32>, described: &BTreeSet<u32>) -> u64 {
    tables
        .difference(described)
        .filter_map(|table| written.schemas.get(table))
        .map(|span| span.len)
        .fold(written.open, u64::saturating_add)
}
