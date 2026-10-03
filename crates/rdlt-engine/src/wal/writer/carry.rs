//! Carrying the frames of segments still open out of old chunks, so a segment its partition
//! keeps open keeps no settled frame of another segment.
//!
//! The copies go to the chunk staged, and the old chunk is needed no more once that chunk is
//! published: its deletion follows the publish, so the log always holds one copy a replay reads.
//! Replay stages only the segments of commits without receipts, and the old chunk's commits all
//! have theirs.

use bytes::Bytes;
use rdlt_connector::SegmentId;

use super::{Log, Logged, Span};
use crate::error::Error;

impl Log {
    /// Carries the frames of the open segments of the old chunks that hold nothing else a replay
    /// needs into the chunk staged, where they hold at least as many bytes of settled segments as
    /// of open ones: what is copied is never more than what is freed.
    pub(super) async fn carry(&mut self) -> Result<(), Error> {
        let carried = self.carriable();
        let (open, settled) = carried
            .iter()
            .fold((0_u64, 0_u64), |(open, settled), number| {
                let batches = &self.written[number].batches;
                batches
                    .iter()
                    .fold((open, settled), |(open, settled), logged| {
                        if self.settled.contains(logged.segment) {
                            (open, settled.saturating_add(logged.span.len))
                        } else {
                            (open.saturating_add(logged.span.len), settled)
                        }
                    })
            });
        if open == 0 || settled < open {
            return Ok(());
        }
        for number in carried {
            self.carry_chunk(number).await?;
        }
        Ok(())
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
    /// they were logged and each table's schema before its first, and marks the chunk carried.
    async fn carry_chunk(&mut self, number: u64) -> Result<(), Error> {
        let Some(written) = self.written.get(&number) else {
            return Ok(());
        };
        let open: Vec<Logged> = written
            .batches
            .iter()
            .filter(|logged| !self.settled.contains(logged.segment))
            .copied()
            .collect();
        let schemas = written.schemas.clone();
        let from = super::Chunk {
            load: self.owner.load,
            number,
        };
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
            }
            let frame = self.read(from, logged.span).await?;
            let span = self.append(frame).await?;
            let current = self.current();
            current.segments.insert(logged.segment);
            current.batches.push(Logged { span, ..logged });
        }
        if let Some(written) = self.written.get_mut(&number) {
            written.carried = true;
        }
        Ok(())
    }

    /// The frame at `span` of chunk `chunk`.
    async fn read(&mut self, chunk: super::Chunk, span: Span) -> Result<Bytes, Error> {
        self.store
            .read(&self.owner.pipeline, chunk, span.offset, span.len)
            .await
            .map_err(Error::from_wal)
    }
}
