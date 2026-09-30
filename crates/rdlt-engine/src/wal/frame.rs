//! The write-ahead log's frames, as spec §15.6 lays them out: `[kind u8][len u32 LE][crc32c u32
//! LE][payload]`.
//!
//! Metadata frames carry their payload as JSON. A batch frame carries its segment and table as a
//! JSON header, then the batch as an Arrow IPC stream, which names its own schema. A frame that
//! ends early, or whose checksum does not match, ends the log: a crash tore it.

#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;
use bytes::{BufMut, Bytes, BytesMut};
use rdlt_connector::{
    CommitMeta, CommitSeq, LoadId, PartitionId, PartitionState, PipelineId, Receipt, SegmentId,
    StreamName, TableRef, TableSchema,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::Error;

/// The version of the frames this engine writes.
pub(crate) const VERSION: u16 = 1;

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
    /// A commit about to be made, whole.
    Commit(Box<CommitMeta>),
    /// A commit's receipt.
    Committed(Receipt),
    /// The load stopped appending: no frame follows.
    Closed,
}

/// Whose log it is, and the last commit the destination had received when the load opened it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
    pub(crate) batch: RecordBatch,
}

/// The header a batch frame's payload starts with.
#[derive(Serialize, Deserialize)]
struct BatchHeader {
    segment: SegmentId,
    table: u32,
}

/// A segment sealed with its partition's position: `from`, where the destination held the
/// partition just before the segment's commit, and `state`, where the segment leaves it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Seal {
    pub(crate) segment: SegmentId,
    pub(crate) stream: StreamName,
    pub(crate) partition: PartitionId,
    /// Whether the stream's source can read the segment again.
    pub(crate) replayable: bool,
    pub(crate) from: Option<PartitionState>,
    pub(crate) state: PartitionState,
}

impl Frame {
    fn kind(&self) -> u8 {
        match self {
            Self::Header(_) => 1,
            Self::Schema(_) => 2,
            Self::Batch(_) => 3,
            Self::Seal(_) => 4,
            Self::Commit(_) => 5,
            Self::Committed(_) => 6,
            Self::Closed => 7,
        }
    }

    /// The frame's bytes.
    pub(crate) fn encode(&self) -> Result<Bytes, Error> {
        let payload = match self {
            Self::Header(header) => json(header)?,
            Self::Schema(table) => json(table)?,
            Self::Batch(batch) => batch_payload(batch)?,
            Self::Seal(seal) => json(seal)?,
            Self::Commit(meta) => json(meta)?,
            Self::Committed(receipt) => json(receipt)?,
            Self::Closed => Vec::new(),
        };
        let len = u32::try_from(payload.len())
            .map_err(|_| Error::internal("a write-ahead log frame beyond 4 GiB"))?;
        let mut frame = BytesMut::with_capacity(HEAD + payload.len());
        frame.put_u8(self.kind());
        frame.put_u32_le(len);
        frame.put_u32_le(crc32c::crc32c(&payload));
        frame.put_slice(&payload);
        Ok(frame.freeze())
    }
}

fn json(value: &impl Serialize) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(value)
        .map_err(|error| Error::internal(format!("encoding a write-ahead log frame: {error}")))
}

fn batch_payload(batch: &Batch) -> Result<Vec<u8>, Error> {
    let header = json(&BatchHeader {
        segment: batch.segment,
        table: batch.table,
    })?;
    let failed = |error: arrow_schema::ArrowError| {
        Error::internal(format!("encoding a write-ahead log batch: {error}"))
    };
    let mut ipc = Vec::new();
    {
        let mut writer =
            arrow_ipc::writer::StreamWriter::try_new(&mut ipc, batch.batch.schema_ref())
                .map_err(failed)?;
        writer.write(&batch.batch).map_err(failed)?;
        writer.finish().map_err(failed)?;
    }
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
    #[cfg(test)]
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

fn decode(kind: u8, payload: &[u8]) -> Result<Frame, Error> {
    match kind {
        1 => parse(payload).map(Frame::Header),
        2 => parse(payload).map(Frame::Schema),
        3 => decode_batch(payload).map(Frame::Batch),
        4 => parse(payload).map(Frame::Seal),
        5 => parse(payload).map(|meta| Frame::Commit(Box::new(meta))),
        6 => parse(payload).map(Frame::Committed),
        7 if payload.is_empty() => Ok(Frame::Closed),
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
    let header: BatchHeader = serde_json::from_slice(header).map_err(|error| corrupt(&error))?;
    let mut reader = arrow_ipc::reader::StreamReader::try_new(&payload[end..], None)
        .map_err(|error| corrupt(&error))?;
    let batch = reader
        .next()
        .ok_or_else(|| corrupt(&"it holds no batch"))?
        .map_err(|error| corrupt(&error))?;
    Ok(Batch {
        segment: header.segment,
        table: header.table,
        batch,
    })
}
