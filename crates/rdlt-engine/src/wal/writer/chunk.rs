//! What each chunk of a load's log holds, and writing frames to the chunk staged: a chunk is
//! published between commits before a batch frame would take it past what a carry may copy.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::Ordering;

use bytes::Bytes;
use rdlt_connector::{CommitSeq, SegmentId};

use super::super::frame::{self, Frame, Header};
use super::super::store::Chunk;
use super::{Log, count};
use crate::crash::crash_point;
use crate::error::Error;
use crate::limits::{LOG_COPY_BYTES, LOG_PARTS};

/// The segments whose frames no replay needs: committed, or abandoned by their partition.
///
/// A settled segment is forgotten once no chunk holds its frames, so a load that commits for ever
/// keeps only those of the chunks it has not deleted.
#[derive(Default)]
pub(super) struct Settled(BTreeSet<SegmentId>);

impl Settled {
    pub(super) fn contains(&self, segment: SegmentId) -> bool {
        self.0.contains(&segment)
    }
}

/// What one chunk of the log holds.
#[derive(Default)]
pub(super) struct Written {
    /// Bytes: what was written to it.
    pub(super) len: u64,
    /// Bytes: what its batch frames of segments not settled take.
    pub(super) open: u64,
    /// Bytes: what its batch frames take, by segment.
    pub(super) by_segment: BTreeMap<SegmentId, u64>,
    /// Where each table's schema frame lies in it.
    pub(super) schemas: BTreeMap<u32, Span>,
    /// The segments with frames in it.
    pub(super) segments: BTreeSet<SegmentId>,
    /// Its batch frames, in order.
    pub(super) batches: Vec<Logged>,
    /// The commits whose frames it holds.
    pub(super) commits: BTreeSet<CommitSeq>,
    /// Whether the frames of its open segments were carried to a later chunk: it is needed no
    /// more once that chunk is published.
    pub(super) carried: bool,
}

impl Written {
    /// Bytes: what a carry of the chunk copies at most: the batch frames of its open segments
    /// and every schema frame it holds; none where it holds no open frame.
    pub(super) fn copies(&self) -> u64 {
        if self.open == 0 {
            return 0;
        }
        self.schemas
            .values()
            .map(|span| span.len)
            .fold(self.open, u64::saturating_add)
    }
}

/// Where a frame lies in its chunk: its offset and its length.
#[derive(Clone, Copy, Debug)]
pub(super) struct Span {
    pub(super) offset: u64,
    pub(super) len: u64,
}

/// A batch frame in a chunk: its segment, its table, and where it lies.
#[derive(Clone, Copy, Debug)]
pub(super) struct Logged {
    pub(super) segment: SegmentId,
    pub(super) table: u32,
    pub(super) span: Span,
}

impl Log {
    /// Bytes: the most a chunk holds, but a chunk of a single frame or carried from a single
    /// chunk, and the frames of a commit beside it.
    pub(super) fn most(&self) -> u64 {
        self.shared.limit.load(Ordering::SeqCst) / LOG_PARTS
    }

    pub(super) fn current(&mut self) -> &mut Written {
        self.written.entry(self.chunk).or_default()
    }

    /// Notes `segment` has frames in the chunk staged.
    pub(super) fn holds(&mut self, segment: SegmentId) {
        self.current().segments.insert(segment);
        self.holders.entry(segment).or_default().insert(self.chunk);
    }

    /// Settles `segments`: their frames are needed no more, and no chunk counts them open; one
    /// no chunk holds is not remembered.
    pub(super) fn settle(&mut self, segments: impl IntoIterator<Item = SegmentId>) {
        for segment in segments {
            let Some(holders) = self.holders.get(&segment) else {
                continue;
            };
            if !self.settled.0.insert(segment) {
                continue;
            }
            for number in holders {
                if let Some(written) = self.written.get_mut(number) {
                    let bytes = written.by_segment.get(&segment).copied().unwrap_or(0);
                    written.open = written.open.saturating_sub(bytes);
                }
            }
        }
    }

    /// Forgets chunk `number`, deleted: a settled segment no chunk holds is forgotten too.
    pub(super) fn forgotten(&mut self, number: u64) -> Option<Written> {
        let written = self.written.remove(&number)?;
        for segment in &written.segments {
            let holders = self.holders.entry(*segment).or_default();
            holders.remove(&number);
            if holders.is_empty() {
                self.holders.remove(segment);
                self.settled.0.remove(segment);
            }
        }
        Some(written)
    }

    /// Writes `frame`, one of the writer's own, to the chunk staged, as [`Log::written_out`]
    /// does, counting it first: it comes out of the room every other frame keeps for it.
    pub(super) async fn append(&mut self, frame: Bytes) -> Result<Span, Error> {
        self.take(count(frame.len()));
        self.written_out(frame).await
    }

    /// Counts `bytes` of the writer's own frames: out of what a batch counted for them where it
    /// did, and out of the room every other frame keeps otherwise.
    pub(super) fn take(&mut self, bytes: u64) {
        let spent = bytes.min(self.spare);
        self.spare -= spent;
        self.shared.held.fetch_add(bytes - spent, Ordering::SeqCst);
    }

    /// Writes `frame`, whose bytes were counted already, to the chunk staged: where it lies.
    pub(super) async fn written_out(&mut self, frame: Bytes) -> Result<Span, Error> {
        self.stage().await?;
        let offset = self.current().len;
        self.put(frame).await?;
        Ok(Span {
            offset,
            len: self.current().len - offset,
        })
    }

    /// Stages the chunk, with its preamble and header, where none is staged.
    async fn stage(&mut self) -> Result<(), Error> {
        self.failure()?;
        if self.staged.is_some() {
            return Ok(());
        }
        let chunk = Chunk {
            load: self.owner.load,
            number: self.chunk,
        };
        let staged = self
            .store
            .stage(&self.owner.pipeline, chunk)
            .await
            .map_err(|error| self.lost(error))?;
        let mut head = frame::preamble().to_vec();
        head.extend_from_slice(&self.header(self.chunk)?);
        self.take(count(head.len()));
        self.staged = Some(staged);
        self.put(Bytes::from(head)).await
    }

    /// Appends `bytes` to the chunk staged.
    async fn put(&mut self, bytes: Bytes) -> Result<(), Error> {
        let staged = self
            .staged
            .as_mut()
            .ok_or_else(|| Error::internal("a chunk staged is gone"))?;
        crash_point!("engine.wal.append");
        let len = count(bytes.len());
        if let Err(error) = staged.append(bytes).await {
            self.discard().await;
            return Err(Error::from_wal(error));
        }
        self.current().len += len;
        Ok(())
    }

    /// Copies the frame at `span` of chunk `from` to the chunk staged, a piece at a time, so it
    /// holds no more of it in memory than a piece: where it lies there.
    pub(super) async fn copy(&mut self, from: Chunk, span: Span) -> Result<Span, Error> {
        self.stage().await?;
        let offset = self.current().len;
        let mut copied = 0;
        while copied < span.len {
            let piece = (span.len - copied).min(LOG_COPY_BYTES);
            let bytes = self
                .store
                .read(&self.owner.pipeline, from, span.offset + copied, piece)
                .await
                .map_err(|error| self.lost(error))?;
            if count(bytes.len()) != piece {
                return Err(Error::wal(format!(
                    "chunk {} of the log ended within a frame it was read for",
                    from.number
                )));
            }
            self.put(bytes).await?;
            copied += piece;
        }
        Ok(Span {
            offset,
            len: span.len,
        })
    }

    /// The error for `error` of the store: a log found removed, or a chunk name found taken, was
    /// taken over by a replay.
    pub(super) fn lost(&self, error: std::io::Error) -> Error {
        match error.kind() {
            std::io::ErrorKind::NotFound | std::io::ErrorKind::AlreadyExists => {
                Error::wal_fenced(self.owner.load)
            }
            _ => Error::from_wal(error),
        }
    }

    /// The header of chunk `chunk`.
    pub(super) fn chunk_header(&self, chunk: u64) -> Header {
        Header {
            pipeline: self.owner.pipeline.clone(),
            load: self.owner.load,
            chunk,
            epoch: self.owner.epoch,
            opened: self.owner.opened,
            origin: self.owner.origin,
        }
    }

    /// The header frame of chunk `chunk`.
    pub(super) fn header(&self, chunk: u64) -> Result<Bytes, Error> {
        Frame::Header(self.chunk_header(chunk)).encode()
    }

    /// Writes `frame`, a batch frame of `segment` for the table at `table`, counted with
    /// `schema` bytes beside it for the table's schema frame, written first where the chunk
    /// staged lacks it and given back otherwise.
    ///
    /// A chunk holding batch frames is published first where this one would take it past what
    /// a carry may copy, so a chunk holds more only where it holds a single frame.
    pub(super) async fn batch(
        &mut self,
        segment: SegmentId,
        table: u32,
        frame: Bytes,
        schema: u64,
    ) -> Result<(), Error> {
        let most = self.most();
        let staged = self.written.get(&self.chunk);
        let len = staged.map_or(0, |written| written.len);
        let full = staged.is_some_and(|written| !written.batches.is_empty())
            && len
                .saturating_add(schema)
                .saturating_add(count(frame.len()))
                > most;
        if full {
            self.append(Frame::Relieved.encode()?).await?;
            self.publish().await?;
        }
        if self.current().schemas.contains_key(&table) {
            self.shared.release(schema);
        } else {
            let frame = self.tables.get(&table).cloned().ok_or_else(|| {
                Error::internal(format!(
                    "a batch of table {table}, whose schema was never sent"
                ))
            })?;
            self.describe(table, frame).await?;
            // Written once, the frame is the writer's to keep for the chunks after.
            drop(self.describing.remove(&table));
        }
        let span = self.written_out(frame).await?;
        self.logged(Logged {
            segment,
            table,
            span,
        });
        Ok(())
    }

    /// Notes `logged`, a batch frame of a segment not settled, written to the chunk staged.
    pub(super) fn logged(&mut self, logged: Logged) {
        self.holds(logged.segment);
        let current = self.current();
        current.open += logged.span.len;
        *current.by_segment.entry(logged.segment).or_default() += logged.span.len;
        current.batches.push(logged);
    }

    /// Writes `frame`, the schema frame of `table`, counted already, to the chunk staged.
    pub(super) async fn describe(&mut self, table: u32, frame: Bytes) -> Result<(), Error> {
        let span = self.written_out(frame).await?;
        self.current().schemas.insert(table, span);
        Ok(())
    }
}
