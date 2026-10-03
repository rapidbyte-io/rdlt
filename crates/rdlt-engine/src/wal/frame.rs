//! The write-ahead log's frames, as spec §15.6 lays them out: `[kind u8][len u32 LE][crc32c u32
//! LE][payload]`.
//!
//! Metadata frames carry their payload as JSON. A batch frame carries its segment and table as a
//! JSON header, then the batch in the wire's Arrow framing, which names its own schema. A frame
//! that ends early, or whose checksum does not match, ends the log: a crash tore it.

mod arrow;
#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;
use bytes::{BufMut, Bytes, BytesMut};
use rdlt_connector::{
    CommitMeta, CommitSeq, LoadId, PartitionId, PartitionState, PipelineId, Receipt, SegmentId,
    StateChange, StreamName, TableRef, TableSchema,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::Error;

/// The version of the frames this engine writes.
pub(crate) const VERSION: u16 = 2;

/// The bytes before a frame's payload: its kind, length and checksum.
pub(crate) const HEAD: usize = 9;

/// One frame of a load's log.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Frame {
    /// The log's first frame.
    Header(Header),
    /// A table the load writes, by the index its batch frames name it with.
    Schema(Table),
    /// A batch the load wrote for a table, in a segment.
    Batch(Batch),
    /// A segment sealed with a partition's position.
    Seal(Seal),
    /// A phase the next commit frame begins.
    Begun(BegunPhase),
    /// A commit about to be made, whole.
    Commit(Box<CommitMeta>),
    /// A commit's receipt.
    Committed(Receipt),
    /// The load stopped appending: no frame follows.
    Closed,
}

/// Whose log it is, and the last commit the destination had received when the load opened it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Header {
    pub(crate) version: u16,
    pub(crate) pipeline: PipelineId,
    pub(crate) load: LoadId,
    pub(crate) opened: Option<(LoadId, CommitSeq)>,
}

/// A table the load writes: the index batch frames name it by, and how to create it again.
#[expect(
    clippy::struct_field_names,
    reason = "a table frame names the table it describes"
)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Table {
    pub(crate) index: u32,
    pub(crate) table: TableRef,
    pub(crate) schema: TableSchema,
}

/// A batch written for the table at `table` in `segment`.
#[expect(
    clippy::struct_field_names,
    reason = "a batch frame holds the batch it logs"
)]
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Batch {
    pub(crate) segment: SegmentId,
    pub(crate) table: u32,
    /// Where the batch stands among the load's logged batches: a segment's batches replay in
    /// this order, wherever in the log a carry left them.
    pub(crate) ordinal: u64,
    pub(crate) batch: RecordBatch,
}

/// The header a batch frame's payload starts with.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchHeader {
    segment: SegmentId,
    table: u32,
    ordinal: u64,
}

/// A segment sealed with its partition's position: `from`, where the destination held the
/// partition just before the segment's commit, and `state`, where the segment leaves it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Seal {
    pub(crate) segment: SegmentId,
    pub(crate) stream: StreamName,
    pub(crate) partition: PartitionId,
    /// Whether the stream's source can read the segment again.
    pub(crate) replayable: bool,
    /// The phase of the stream the segment belongs to.
    pub(crate) phase: u16,
    pub(crate) from: Option<PartitionState>,
    pub(crate) state: PartitionState,
}

/// A stream's phase a commit begins: the changes that begin it, as the commit's state delta
/// leads with them — the previous phase's entries deleted, where its partitions start, the phase.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BegunPhase {
    pub(crate) stream: StreamName,
    pub(crate) phase: u16,
    pub(crate) changes: Vec<StateChange>,
}

impl Frame {
    fn kind(&self) -> u8 {
        match self {
            Self::Header(_) => 1,
            Self::Schema(_) => 2,
            Self::Batch(_) => 3,
            Self::Seal(_) => 4,
            Self::Commit(_) => COMMIT,
            Self::Committed(_) => 6,
            Self::Closed => 7,
            Self::Begun(_) => 8,
        }
    }

    /// The frame's bytes.
    pub(crate) fn encode(&self) -> Result<Bytes, Error> {
        let payload = match self {
            Self::Header(header) => json(header)?,
            Self::Schema(table) => json(table)?,
            Self::Batch(batch) => batch_payload(batch)?,
            Self::Seal(seal) => json(seal)?,
            Self::Begun(begun) => json(begun)?,
            Self::Commit(meta) => return commit(meta),
            Self::Committed(receipt) => json(receipt)?,
            Self::Closed => Vec::new(),
        };
        framed(self.kind(), &payload)
    }
}

/// The kind a commit frame is marked with.
const COMMIT: u8 = 5;

/// The bytes of the frame of the commit `meta` describes, encoded from where it lies.
pub(crate) fn commit(meta: &CommitMeta) -> Result<Bytes, Error> {
    framed(COMMIT, &json(meta)?)
}

/// A frame of `kind` holding `payload`.
fn framed(kind: u8, payload: &[u8]) -> Result<Bytes, Error> {
    let len = u32::try_from(payload.len())
        .map_err(|_| Error::internal("a write-ahead log frame beyond 4 GiB"))?;
    let mut frame = BytesMut::with_capacity(HEAD + payload.len());
    frame.put_u8(kind);
    frame.put_u32_le(len);
    frame.put_u32_le(crc32c::crc32c(payload));
    frame.put_slice(payload);
    Ok(frame.freeze())
}

fn json(value: &impl Serialize) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(value)
        .map_err(|error| Error::internal(format!("encoding a write-ahead log frame: {error}")))
}

fn batch_payload(batch: &Batch) -> Result<Vec<u8>, Error> {
    let header = json(&BatchHeader {
        segment: batch.segment,
        table: batch.table,
        ordinal: batch.ordinal,
    })?;
    let ipc = arrow::encode(&batch.batch)?;
    let len = u32::try_from(header.len()).unwrap_or(u32::MAX);
    let mut payload = Vec::with_capacity(4 + header.len() + ipc.len());
    payload.extend_from_slice(&len.to_le_bytes());
    payload.extend_from_slice(&header);
    payload.extend_from_slice(&ipc);
    Ok(payload)
}

/// The length of the payload a frame's head, its first [`HEAD`] bytes, announces.
pub(crate) fn payload_len(head: &[u8]) -> Option<u64> {
    let len = head.get(1..5)?;
    Some(u64::from(u32::from_le_bytes([
        len[0], len[1], len[2], len[3],
    ])))
}

/// The frames of a log's bytes, each with the offset it starts at, up to the first a crash tore.
///
/// A torn frame ends early or its checksum does not match. A frame whose checksum matches but
/// that does not decode is an error: the log is not one this engine wrote.
pub(crate) struct Frames<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Frames<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    /// Where the frames read so far end: past it, the log is torn or ends.
    pub(crate) fn end(&self) -> usize {
        self.offset
    }
}

impl Iterator for Frames<'_> {
    type Item = Result<(usize, Frame), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        let rest = &self.bytes[self.offset..];
        let head = rest.get(..HEAD)?;
        let kind = head[0];
        let len = u32::from_le_bytes([head[1], head[2], head[3], head[4]]);
        let crc = u32::from_le_bytes([head[5], head[6], head[7], head[8]]);
        let end = HEAD.checked_add(usize::try_from(len).ok()?)?;
        let payload = rest.get(HEAD..end)?;
        if crc32c::crc32c(payload) != crc {
            return None;
        }
        let offset = self.offset;
        self.offset += end;
        Some(decode(kind, payload).map(|frame| (offset, frame)))
    }
}

/// A frame as a scan reads it: a batch's segment and table, without its batch, or any other
/// frame whole.
pub(crate) enum Skimmed {
    Batch {
        segment: SegmentId,
        table: u32,
        ordinal: u64,
    },
    Other(Box<Frame>),
}

/// The frame `bytes` holds whole, as [`Frames`] reads it but for a batch's data, which is left
/// undecoded; none where it is torn.
pub(crate) fn skim(bytes: &[u8]) -> Option<Result<Skimmed, Error>> {
    let head = bytes.get(..HEAD)?;
    let payload = bytes.get(HEAD..)?;
    if crc32c::crc32c(payload) != u32::from_le_bytes([head[5], head[6], head[7], head[8]]) {
        return None;
    }
    Some(match head[0] {
        3 => batch_head(payload).map(|head| Skimmed::Batch {
            segment: head.segment,
            table: head.table,
            ordinal: head.ordinal,
        }),
        kind => decode(kind, payload).map(|frame| Skimmed::Other(Box::new(frame))),
    })
}

/// The header a batch frame's payload starts with, and where its batch begins.
fn batch_header(payload: &[u8]) -> Result<(BatchHeader, usize), Error> {
    let corrupt = |what: &dyn std::fmt::Display| {
        Error::internal(format!("a write-ahead log batch does not decode: {what}"))
    };
    let len = payload
        .get(..4)
        .map(|len| u32::from_le_bytes([len[0], len[1], len[2], len[3]]))
        .ok_or_else(|| corrupt(&"it has no header"))?;
    let end = 4 + usize::try_from(len).unwrap_or(usize::MAX);
    let header = payload
        .get(4..end)
        .ok_or_else(|| corrupt(&"its header ends early"))?;
    let header = serde_json::from_slice(header).map_err(|error| corrupt(&error))?;
    Ok((header, end))
}

fn batch_head(payload: &[u8]) -> Result<BatchHeader, Error> {
    batch_header(payload).map(|(header, _)| header)
}

fn decode(kind: u8, payload: &[u8]) -> Result<Frame, Error> {
    match kind {
        1 => parse(payload).map(Frame::Header),
        2 => parse(payload).map(Frame::Schema),
        3 => decode_batch(payload).map(Frame::Batch),
        4 => parse(payload).map(Frame::Seal),
        5 => parse(payload).map(|meta| Frame::Commit(Box::new(meta))),
        6 => parse(payload).map(Frame::Committed),
        7 if payload.is_empty() => Ok(Frame::Closed),
        8 => parse(payload).map(Frame::Begun),
        other => Err(Error::internal(format!(
            "a write-ahead log frame of kind {other} does not decode"
        ))),
    }
}

fn parse<T: DeserializeOwned>(payload: &[u8]) -> Result<T, Error> {
    serde_json::from_slice(payload).map_err(|error| {
        Error::internal(format!("a write-ahead log frame does not decode: {error}"))
    })
}

fn decode_batch(payload: &[u8]) -> Result<Batch, Error> {
    let (header, end) = batch_header(payload)?;
    let batch = arrow::decode(&payload[end..])?;
    Ok(Batch {
        segment: header.segment,
        table: header.table,
        ordinal: header.ordinal,
        batch,
    })
}
