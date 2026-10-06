//! A commit as a load's log writes it: its seals, then the phases it begins and its frame, made
//! durable before the destination sees it.

use bytes::Bytes;
use rdlt_connector::{CommitMeta, PartitionState, SegmentSet};
use tokio::sync::oneshot;

use super::super::frame::{self, Frame};
use super::super::writer::Command;
use super::{FRAMED, LoadLog, Sealed, count, reserved, settled};
use crate::budget::MemoryBudget;
use crate::error::Error;
use crate::limits::RECORDED;

/// A commit's seals as sent: the segments it settles, how many seals it took, and the bytes of
/// its frames so far.
pub(super) struct Seals {
    segments: SegmentSet,
    count: u32,
    whole: u64,
}

impl LoadLog {
    /// Logs `sealed`, then the phases `begun` that `meta`'s commit begins with it and the commit,
    /// and returns once the commit's frame is durable; `budget` holds each frame's bytes until
    /// it is appended.
    ///
    /// The phase frames go in the commit's chunk with it, which is written whole.
    pub(crate) async fn commit(
        &self,
        budget: &MemoryBudget,
        sealed: Vec<Sealed>,
        begun: Vec<frame::BegunPhase>,
        meta: &CommitMeta,
        prepaid: u64,
    ) -> Result<(), Error> {
        let seals = self.seals(budget, sealed, meta).await?;
        self.finish(budget, seals, begun, meta, prepaid).await
    }

    /// Sends the seal frames of `sealed`, the first part of `meta`'s commit.
    ///
    /// Each frame is counted on disk, then sent, so it holds its memory no longer than its write;
    /// the commit counts as large as all its frames together.
    pub(super) async fn seals(
        &self,
        budget: &MemoryBudget,
        sealed: Vec<Sealed>,
        meta: &CommitMeta,
    ) -> Result<Seals, Error> {
        self.retire().await?;
        // The commit settles every segment it sealed, those it publishes nothing of included, so
        // its receipt lets their chunks go.
        let mut segments = meta.segments.clone();
        for seal in &sealed {
            segments.insert(seal.segment);
        }
        let count = u32::try_from(sealed.len())
            .map_err(|_| Error::internal("a commit takes more seals than a frame counts"))?;
        let mut whole = 0_u64;
        for seal in sealed {
            let (command, frame) = self.seal(budget, seal).await?;
            whole = whole.saturating_add(frame);
            self.admit_commit(frame, whole).await?;
            self.writer.send(command).await?;
        }
        Ok(Seals {
            segments,
            count,
            whole,
        })
    }

    /// Sends the frames of the phases `begun` and of `meta`'s commit after its `seals`, and
    /// returns once the commit's frame is durable.
    pub(super) async fn finish(
        &self,
        budget: &MemoryBudget,
        seals: Seals,
        begun: Vec<frame::BegunPhase>,
        meta: &CommitMeta,
        prepaid: u64,
    ) -> Result<(), Error> {
        let phases = u32::try_from(begun.len())
            .map_err(|_| Error::internal("a commit begins more phases than a frame counts"))?;
        // Reserved before they are encoded for what they take at most, but what they record of
        // tables, which each table's change reserved.
        let held = reserved(budget, commit_bytes(&begun, meta).saturating_sub(prepaid)).await?;
        let mut frames = Vec::new();
        for begun in begun {
            frames.extend_from_slice(&Frame::Begun(begun).encode()?);
        }
        frames.extend_from_slice(&frame::commit(meta, seals.count, phases)?);
        let frame = frames
            .len()
            .saturating_sub(usize::try_from(prepaid).unwrap_or(usize::MAX));
        let held = settled(budget, held, frame).await?;
        let held = Box::new(held);
        let frame = count(frames.len());
        self.admit_commit(frame, seals.whole.saturating_add(frame))
            .await?;
        let (durable, answer) = oneshot::channel();
        self.writer
            .send(Command::Commit {
                seq: meta.commit_seq,
                segments: seals.segments,
                frame: Bytes::from(frames),
                held,
                durable,
            })
            .await?;
        answer
            .await
            .map_err(|_| Error::wal("the write-ahead log's writer stopped"))?
    }

    /// The command logging `seal` with the batch frames and rows logged of its segment, and the
    /// bytes of its frame; `budget` holds the frame's bytes until it is written.
    async fn seal(&self, budget: &MemoryBudget, seal: Sealed) -> Result<(Command, u64), Error> {
        let segment = seal.segment;
        let counted = self.counts.lock().remove(&segment).unwrap_or_default();
        // Reserved before it is encoded, for the cursors it records as base64 text, within
        // `RECORDED` bytes for each of their bytes, and for the frame as it is once it exists.
        let cursors = [seal.from.as_ref(), Some(&seal.state)];
        let cursors = cursors.into_iter().flatten().map(recorded);
        let bytes = cursors.fold(FRAMED, u64::saturating_add);
        let held = reserved(budget, bytes).await?;
        let frame = Frame::Seal(frame::Seal {
            segment,
            stream: seal.stream,
            partition: seal.partition,
            replayable: seal.replayable,
            phase: seal.phase,
            from: seal.from,
            state: seal.state,
            batches: counted.batches,
            rows: counted.rows,
        })
        .encode()?;
        let held = settled(budget, held, frame.len()).await?;
        let bytes = count(frame.len());
        let command = Command::Seal {
            segment,
            frame,
            held: Box::new(held),
        };
        Ok((command, bytes))
    }
}

/// Bytes: about what `state` takes in a frame that records it.
fn recorded(state: &PartitionState) -> u64 {
    match state {
        PartitionState::Cursor(cursor) => RECORDED.saturating_mul(count(cursor.bytes().len())),
        PartitionState::Done => 0,
    }
}

/// Bytes: what a record of state takes in a frame beside its key and value, at most.
const PER_RECORD: u64 = 64;

/// Bytes: the most the frames of the commit `meta` and of the phases `begun` with it take: each
/// record's key and value written twice over, what a record and a frame take beside, and the
/// segments, generations and tables the commit names.
pub(super) fn commit_bytes(begun: &[frame::BegunPhase], meta: &CommitMeta) -> u64 {
    let changes = begun.iter().flat_map(|begun| &begun.changes);
    let record = |change: &rdlt_connector::StateChange| match change {
        rdlt_connector::StateChange::Put(record) => {
            count(record.key.len().saturating_add(record.value.len()))
        }
        rdlt_connector::StateChange::Delete(key) => count(key.len()),
    };
    let records = changes.chain(&meta.state_delta).map(|change| {
        RECORDED
            .saturating_mul(record(change))
            .saturating_add(PER_RECORD)
    });
    let named = [
        meta.finish_generations.len(),
        meta.child_tables.len(),
        meta.drop_tables.len(),
    ];
    let named = named.into_iter().map(count);
    let named = named.fold(meta.segments.len(), u64::saturating_add);
    let named = named.saturating_mul(NAMED);
    let frames = count(begun.len()).saturating_add(1).saturating_mul(FRAMED);
    records.fold(named.saturating_add(frames), u64::saturating_add)
}

/// Bytes: the most a segment, a generation or a table a commit names takes in its frame.
const NAMED: u64 = 1 << 10;
