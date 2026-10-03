//! Reading a load's log back, for replay: which tables, batches, seals and commits it holds, one
//! frame in memory at a time.
//!
//! The highest chunk is the log's last word: its end names the earlier chunks still needed and
//! the commits in them that were received. Exactly those chunks are read, and each must be
//! whole; one missing, damaged, of another log or another format, or holding a commit not all
//! of which is there, is refused.

mod checked;
mod chunk;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use arrow_array::RecordBatch;
use rdlt_connector::{CommitMeta, CommitSeq, LoadId, PipelineId, SegmentId};

use self::chunk::{First, Opened, Reading};
use super::frame::{self, BegunPhase, End, Frame, Header, Seal, Skimmed, Table};
use super::store::{Chunk, WalStore};
use crate::error::Error;
use crate::limits::{WAL_FOREIGN, WAL_UNREADABLE};

/// Where a batch frame is, and the table it names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Located {
    pub(crate) chunk: Chunk,
    pub(crate) offset: u64,
    pub(crate) len: u64,
    pub(crate) table: u32,
    /// Where the batch stands among the load's logged batches.
    pub(crate) ordinal: u64,
    /// The rows its header says it holds.
    pub(crate) rows: u64,
}

/// A commit frame, and the seal and phase frames its load logged just before it: every partition
/// it moves, empty segments' included, and every phase it begins.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Logged {
    pub(crate) meta: CommitMeta,
    pub(crate) seals: Vec<Seal>,
    pub(crate) begun: Vec<BegunPhase>,
}

/// What a log's highest chunk says of the log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Tail {
    /// The chunk's number.
    pub(crate) number: u64,
    /// Whether it holds a commit, which is then needed too.
    pub(crate) commits: bool,
    pub(crate) end: End,
}

impl Tail {
    /// The chunks a replay reads: those the end names, and this one where it holds a commit.
    pub(crate) fn needed(&self) -> Vec<u64> {
        let mut needed = self.end.live.clone();
        if self.commits {
            needed.push(self.number);
        }
        needed
    }
}

/// What a log holds, in the chunks its highest one says are needed.
#[derive(Debug, Default)]
pub(crate) struct Scanned {
    /// The header its load wrote each chunk with.
    pub(crate) header: Option<Header>,
    /// The tables its batch frames name, by index.
    pub(crate) tables: BTreeMap<u32, Table>,
    /// Each segment's batch frames, in the order they were logged.
    pub(crate) batches: BTreeMap<SegmentId, Vec<Located>>,
    /// Its commit frames with their seals, in order.
    pub(crate) commits: Vec<Logged>,
    /// The commits whose receipts it holds.
    pub(crate) received: BTreeSet<CommitSeq>,
}

impl Scanned {
    /// The commits without receipts, in order.
    pub(crate) fn pending(&self) -> impl Iterator<Item = &Logged> {
        self.commits
            .iter()
            .filter(|logged| !self.received.contains(&logged.meta.commit_seq))
    }
}

/// The log a scan reads, which its refusals name.
#[derive(Clone, Copy)]
pub(super) struct Of<'a> {
    pipeline: &'a PipelineId,
    load: LoadId,
}

impl Of<'_> {
    /// The error refusing the log as one this engine cannot read, as `detail` says.
    fn unreadable(self, detail: impl std::fmt::Display) -> Error {
        unreadable(self.pipeline, self.load, &detail)
    }
}

/// The error for a log this engine cannot read: an operator removes it.
fn unreadable(pipeline: &PipelineId, load: LoadId, detail: &dyn std::fmt::Display) -> Error {
    Error::wal(format!(
        "the write-ahead log of load {load} of pipeline {pipeline} cannot be read: {detail}"
    ))
    .with_code(WAL_UNREADABLE)
}

/// The error for a log of load `load` of `pipeline` whose chunk says it is `found`'s, of `of`.
fn foreign(pipeline: &PipelineId, load: LoadId, of: &PipelineId, found: LoadId) -> Error {
    Error::wal(format!(
        "the write-ahead log of load {load} of pipeline {pipeline} holds a chunk of load \
         {found} of pipeline {of}"
    ))
    .with_code(WAL_FOREIGN)
}

/// What the highest chunk of `load`'s log of `pipeline` in `store` says, reading it whole and
/// refusing frames beyond `frame_bytes`; none where the log has no chunk.
pub(crate) async fn tail(
    store: &dyn WalStore,
    pipeline: &PipelineId,
    load: LoadId,
    frame_bytes: u64,
) -> Result<Option<Tail>, Error> {
    let reading = Reading {
        store,
        pipeline,
        load,
        frame_bytes,
    };
    let chunks = store
        .chunks(pipeline, load)
        .await
        .map_err(Error::from_wal)?;
    let Some(&(number, len)) = chunks.last() else {
        return Ok(None);
    };
    let opened = reading.chunk(number, len).await?;
    Ok(Some(tail_of(number, &opened)))
}

fn tail_of(number: u64, opened: &Opened) -> Tail {
    let commits = opened.frames.last().is_some_and(
        |read| matches!(&read.frame, Skimmed::Other(frame) if matches!(**frame, Frame::Commit(_))),
    );
    Tail {
        number,
        commits: matches!(opened.first, First::Header(_)) && commits,
        end: opened.end.clone(),
    }
}

/// Reads `load`'s log of `pipeline` in `store`: its highest chunk, then the chunks it names,
/// each whole, refusing frames beyond `frame_bytes`.
pub(crate) async fn scan(
    store: &dyn WalStore,
    pipeline: &PipelineId,
    load: LoadId,
    frame_bytes: u64,
) -> Result<Scanned, Error> {
    let reading = Reading {
        store,
        pipeline,
        load,
        frame_bytes,
    };
    let chunks: BTreeMap<u64, u64> = store
        .chunks(pipeline, load)
        .await
        .map_err(Error::from_wal)?
        .into_iter()
        .collect();
    let Some((&last, &len)) = chunks.last_key_value() else {
        return Ok(Scanned::default());
    };
    let tail = reading.chunk(last, len).await?;
    let needed = tail_of(last, &tail).needed();
    let of = Of { pipeline, load };
    let mut scanned = Scanned {
        received: tail.end.received.iter().copied().collect(),
        ..Scanned::default()
    };
    let mut tail = Some(tail);
    for number in needed {
        let opened = if let Some(opened) = tail.take_if(|_| number == last) {
            opened
        } else {
            let missing = || {
                unreadable(
                    pipeline,
                    load,
                    &format!("chunk {number} it needs is missing"),
                )
            };
            let len = chunks.get(&number).ok_or_else(missing)?;
            reading.chunk(number, *len).await?
        };
        note(&mut scanned, opened, Chunk { load, number }, of)?;
    }
    checked::commits(&scanned, of)?;
    // A segment's batches replay in the order they were logged, though a carry moved some of
    // them after others.
    for batches in scanned.batches.values_mut() {
        batches.sort_by_key(|located| located.ordinal);
    }
    Ok(scanned)
}

/// Adds what `opened`, chunk `chunk`, holds to `scanned`; what makes it disagree otherwise.
fn note(scanned: &mut Scanned, opened: Opened, chunk: Chunk, of: Of<'_>) -> Result<(), Error> {
    let number = chunk.number;
    headed(scanned, opened.first, number, of)?;
    let (mut sealing, mut beginning) = (Vec::new(), Vec::new());
    for read in opened.frames {
        let frame = match read.frame {
            Skimmed::Batch(header) => {
                let located = Located {
                    chunk,
                    offset: read.offset,
                    len: read.len,
                    table: header.table,
                    ordinal: header.ordinal,
                    rows: header.rows,
                };
                scanned
                    .batches
                    .entry(header.segment)
                    .or_default()
                    .push(located);
                continue;
            }
            Skimmed::Other(frame) => *frame,
        };
        match frame {
            Frame::Schema(table) => match scanned.tables.get(&table.index) {
                Some(known) if *known != table => {
                    return Err(of.unreadable(format!(
                        "chunk {number} describes table {} anew",
                        table.index
                    )));
                }
                Some(_) => {}
                None => {
                    scanned.tables.insert(table.index, table);
                }
            },
            Frame::Seal(seal) => sealing.push(seal),
            Frame::Begun(begun) => beginning.push(begun),
            Frame::Commit(commit) => {
                checked::counted(of, &commit, &sealing, &beginning)?;
                scanned.commits.push(Logged {
                    meta: commit.meta,
                    seals: std::mem::take(&mut sealing),
                    begun: std::mem::take(&mut beginning),
                });
            }
            // A chunk's shape was checked as it was read: nothing else is here.
            _ => {}
        }
    }
    Ok(())
}

/// Notes `first`, the first frame of chunk `number`, in `scanned`: a header like the chunks'
/// before it.
fn headed(scanned: &mut Scanned, first: First, number: u64, of: Of<'_>) -> Result<(), Error> {
    match first {
        First::Header(header) => match &scanned.header {
            Some(known) if (known.epoch, known.opened) != (header.epoch, header.opened) => Err(of
                .unreadable(format!(
                    "chunk {number} was written by another session than the chunks before it"
                ))),
            Some(_) => Ok(()),
            None => {
                scanned.header = Some(header);
                Ok(())
            }
        },
        First::Fence(_) => {
            Err(of.unreadable(format!("chunk {number} is a fence, which no chunk needs")))
        }
    }
}

/// The batch at `located` in `load`'s log of `pipeline`, decoded within `limits`.
pub(crate) async fn batch(
    store: &dyn WalStore,
    pipeline: &PipelineId,
    located: Located,
    limits: rdlt_wire::Limits,
) -> Result<RecordBatch, Error> {
    let load = located.chunk.load;
    let bytes = store
        .read(pipeline, located.chunk, located.offset, located.len)
        .await
        .map_err(Error::from_wal)?;
    let frame =
        frame::decode(&bytes, limits).map_err(|error| unreadable(pipeline, load, &error))?;
    // Exactly the frame the scan found.
    match frame {
        Frame::Batch(batch) if batch.ordinal == located.ordinal && batch.table == located.table => {
            Ok(batch.batch)
        }
        _ => Err(unreadable(
            pipeline,
            load,
            &"a batch frame read once no longer reads back",
        )),
    }
}
