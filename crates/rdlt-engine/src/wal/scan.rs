//! Reading a load's log back, for replay: which tables, batches, seals and commits it holds, one
//! frame in memory at a time.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};

use arrow_array::RecordBatch;
use rdlt_connector::{CommitMeta, CommitSeq, LoadId, PipelineId, SegmentId};

use super::frame::{self, Frame, Frames, HEAD, Header, Seal, Table};
use super::store::{Chunk, WalStore};
use crate::error::Error;

/// Where a batch frame is, and the table it names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Located {
    pub(crate) chunk: Chunk,
    pub(crate) offset: u64,
    pub(crate) len: u64,
    pub(crate) table: u32,
}

/// A commit frame, and the seal frames its load logged just before it: every partition it moves,
/// empty segments' included.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Logged {
    pub(crate) meta: CommitMeta,
    pub(crate) seals: Vec<Seal>,
}

/// What a log holds, up to where each of its chunks ends or a crash tore it.
#[derive(Debug, Default)]
pub(crate) struct Scanned {
    pub(crate) header: Option<Header>,
    /// The tables its batch frames name, by index.
    pub(crate) tables: BTreeMap<u32, Table>,
    /// Each segment's batch frames, in order.
    pub(crate) batches: BTreeMap<SegmentId, Vec<Located>>,
    /// Its commit frames with their seals, in order.
    pub(crate) commits: Vec<Logged>,
    /// Seal frames no commit frame has followed yet.
    sealing: Vec<Seal>,
    /// The commits whose receipts it holds.
    pub(crate) received: BTreeSet<CommitSeq>,
    /// Whether its load closed it.
    pub(crate) closed: bool,
}

impl Scanned {
    /// The commits without receipts, in order.
    pub(crate) fn pending(&self) -> impl Iterator<Item = &Logged> {
        self.commits
            .iter()
            .filter(|logged| !self.received.contains(&logged.meta.commit_seq))
    }
}

/// The error for a log this engine cannot read: an operator removes it.
fn unreadable(pipeline: &PipelineId, load: LoadId, detail: &dyn std::fmt::Display) -> Error {
    Error::wal(format!(
        "the write-ahead log of load {load} of pipeline {pipeline} cannot be read: {detail}"
    ))
    .with_code("wal_unreadable")
}

/// Reads `load`'s log of `pipeline` in `store`, chunk by chunk, each up to its first torn frame.
pub(crate) async fn scan(
    store: &dyn WalStore,
    pipeline: &PipelineId,
    load: LoadId,
) -> Result<Scanned, Error> {
    let mut scanned = Scanned::default();
    let chunks = store
        .chunks(pipeline, load)
        .await
        .map_err(Error::from_wal)?;
    for (number, len) in chunks {
        let chunk = Chunk { load, number };
        let mut offset = 0;
        while let Some((frame, next)) = read_frame(store, pipeline, chunk, offset, len).await? {
            let frame = frame.map_err(|error| unreadable(pipeline, load, &error))?;
            note(
                &mut scanned,
                frame,
                chunk,
                offset,
                next - offset,
                pipeline,
                load,
            )?;
            offset = next;
        }
    }
    Ok(scanned)
}

/// The frame of `chunk` at `offset`, of a chunk `len` bytes long, and where the next starts;
/// none where the chunk ends or a crash tore it there.
async fn read_frame(
    store: &dyn WalStore,
    pipeline: &PipelineId,
    chunk: Chunk,
    offset: u64,
    len: u64,
) -> Result<Option<(Result<Frame, Error>, u64)>, Error> {
    let head_len = HEAD as u64;
    if offset + head_len > len {
        return Ok(None);
    }
    let head = store
        .read(pipeline, chunk, offset, head_len)
        .await
        .map_err(Error::from_wal)?;
    let Some(payload) = frame::payload_len(&head) else {
        return Ok(None);
    };
    let end = offset + head_len + payload;
    if end > len {
        return Ok(None);
    }
    let bytes = store
        .read(pipeline, chunk, offset, end - offset)
        .await
        .map_err(Error::from_wal)?;
    Ok(Frames::new(&bytes)
        .next()
        .map(|frame| (frame.map(|(_, frame)| frame), end)))
}

/// Adds `frame`, `len` bytes at `offset` of `chunk`, to what `scanned` holds.
fn note(
    scanned: &mut Scanned,
    frame: Frame,
    chunk: Chunk,
    offset: u64,
    len: u64,
    pipeline: &PipelineId,
    load: LoadId,
) -> Result<(), Error> {
    match frame {
        Frame::Header(header) => {
            if header.version != frame::VERSION || header.load != load {
                let detail = format!("its header is {header:?}");
                return Err(unreadable(pipeline, load, &detail));
            }
            scanned.header.get_or_insert(header);
        }
        Frame::Schema(table) => {
            scanned.tables.entry(table.index).or_insert(table);
        }
        Frame::Batch(batch) => {
            let located = Located {
                chunk,
                offset,
                len,
                table: batch.table,
            };
            scanned
                .batches
                .entry(batch.segment)
                .or_default()
                .push(located);
        }
        Frame::Seal(seal) => scanned.sealing.push(seal),
        Frame::Commit(meta) => {
            let seals = std::mem::take(&mut scanned.sealing);
            scanned.commits.push(Logged { meta: *meta, seals });
        }
        Frame::Committed(receipt) => {
            scanned.received.insert(receipt.commit_seq);
        }
        Frame::Closed => scanned.closed = true,
    }
    Ok(())
}

/// The batch at `located` in `load`'s log of `pipeline`.
pub(crate) async fn batch(
    store: &dyn WalStore,
    pipeline: &PipelineId,
    located: Located,
) -> Result<RecordBatch, Error> {
    let load = located.chunk.load;
    let bytes = store
        .read(pipeline, located.chunk, located.offset, located.len)
        .await
        .map_err(Error::from_wal)?;
    match Frames::new(&bytes).next() {
        Some(Ok((_, Frame::Batch(batch)))) => Ok(batch.batch),
        Some(Err(error)) => Err(unreadable(pipeline, load, &error)),
        _ => Err(unreadable(
            pipeline,
            load,
            &"a batch frame read once no longer reads back",
        )),
    }
}
