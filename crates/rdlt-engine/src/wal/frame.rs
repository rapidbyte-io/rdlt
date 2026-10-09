//! The write-ahead log's chunks and frames.
//!
//! A chunk starts with a preamble, `rdltwal\0`, the format as a `u16` LE and a CRC32C of both,
//! so a chunk of another format is told from damage. Frames follow, each
//! `[kind u8][len u32 LE][crc32c u32 LE][payload]`, the checksum covering the kind and the length
//! too. Metadata frames carry their payload as JSON; a batch frame carries its segment, table,
//! ordinal and rows as a JSON header, then the batch in the wire's Arrow framing, which names its
//! own schema.

mod arrow;
mod len;
#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;
use bytes::{BufMut, Bytes, BytesMut};
use rdlt_connector::{
    CommitMeta, CommitSeq, Epoch, LoadId, PartitionId, PartitionState, PipelineId, SegmentId,
    StateChange, StreamName, TableRef, TableSchema,
};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::Error;

pub(crate) use self::arrow::limits;
pub(crate) use self::len::{end_len, header_len};

/// The format of the chunks this engine writes.
pub(crate) const VERSION: u16 = 4;

/// What every chunk starts with.
const MAGIC: [u8; 8] = *b"rdltwal\0";

/// The bytes of a chunk's preamble: its magic, its format and their checksum.
pub(crate) const PREAMBLE: usize = 14;

/// The bytes before a frame's payload: its kind, length and checksum.
pub(crate) const HEAD: usize = 9;

/// One frame of a load's log.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Frame {
    /// The first frame of each chunk its load writes.
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
    Commit(Box<Committing>),
    /// The load stopped appending: no chunk follows.
    Closed,
    /// The chunk was published between commits to let the log hold less: it holds batch frames
    /// carried into it, and its end the receipts that arrived, but no commit.
    Relieved,
    /// The last frame of every chunk: what of the log is still needed.
    End(End),
    /// The first frame of a chunk a replay published to take the log over.
    Fence(Fence),
}

/// Whose log a chunk belongs to, where it stands in it, and what its load opened on: the epoch
/// of its session and the last commit the destination had received.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Header {
    pub(crate) pipeline: PipelineId,
    pub(crate) load: LoadId,
    pub(crate) chunk: u64,
    pub(crate) epoch: Epoch,
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) opened: Option<(LoadId, CommitSeq)>,
    /// The destination the log is written for: the first load whose commit reached the pipeline
    /// there, or, where none had when the load opened, the load itself.
    pub(crate) origin: LoadId,
}

/// A chunk a replay published as the next of a log, so its load publishes nothing more.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Fence {
    pub(crate) pipeline: PipelineId,
    pub(crate) load: LoadId,
    pub(crate) chunk: u64,
}

/// What of the log a replay needs once the chunk ending with this is published: the chunks
/// before it still needed, and the commits in them that were received.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct End {
    pub(crate) live: Vec<u64>,
    pub(crate) received: Vec<CommitSeq>,
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

/// A batch of `rows` rows written for the table at `table` in `segment`.
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
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BatchHeader {
    pub(crate) segment: SegmentId,
    pub(crate) table: u32,
    pub(crate) ordinal: u64,
    /// The rows of the batch, which its data must hold.
    pub(crate) rows: u64,
}

/// A segment sealed with its partition's position: `from`, where the destination held the
/// partition just before the segment's commit, and `state`, where the segment leaves it; and the
/// batch frames and rows the load logged of it.
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
    #[serde(deserialize_with = "Option::deserialize")]
    pub(crate) from: Option<PartitionState>,
    pub(crate) state: PartitionState,
    pub(crate) batches: u64,
    pub(crate) rows: u64,
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

/// A commit as its frame holds it: the commit, and how many seal and phase frames precede it.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Committing {
    pub(crate) meta: CommitMeta,
    pub(crate) seals: u32,
    pub(crate) phases: u32,
}

/// A commit's frame as it is written, from where the commit lies.
#[derive(Serialize)]
struct Written<'a> {
    meta: &'a CommitMeta,
    seals: u32,
    phases: u32,
}

/// The kinds frames are marked with.
const HEADER: u8 = 1;
const SCHEMA: u8 = 2;
pub(crate) const BATCH: u8 = 3;
const SEAL: u8 = 4;
const COMMIT: u8 = 5;
const CLOSED: u8 = 7;
const BEGUN: u8 = 8;
const END: u8 = 9;
const FENCE: u8 = 10;
const RELIEVED: u8 = 11;

impl Frame {
    fn kind(&self) -> u8 {
        match self {
            Self::Header(_) => HEADER,
            Self::Schema(_) => SCHEMA,
            Self::Batch(_) => BATCH,
            Self::Seal(_) => SEAL,
            Self::Commit(_) => COMMIT,
            Self::Closed => CLOSED,
            Self::Begun(_) => BEGUN,
            Self::End(_) => END,
            Self::Fence(_) => FENCE,
            Self::Relieved => RELIEVED,
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
            Self::Commit(commit) => {
                return self::commit(&commit.meta, commit.seals, commit.phases);
            }
            Self::Closed | Self::Relieved => Vec::new(),
            Self::End(end) => json(end)?,
            Self::Fence(fence) => json(fence)?,
        };
        framed(self.kind(), &payload)
    }
}

/// The bytes of the frame of the commit `meta` describes, after `seals` seal frames and `phases`
/// phase frames, encoded from where it lies.
pub(crate) fn commit(meta: &CommitMeta, seals: u32, phases: u32) -> Result<Bytes, Error> {
    framed(
        COMMIT,
        &json(&Written {
            meta,
            seals,
            phases,
        })?,
    )
}

/// The preamble every chunk starts with.
pub(crate) fn preamble() -> [u8; PREAMBLE] {
    let mut preamble = [0; PREAMBLE];
    preamble[..8].copy_from_slice(&MAGIC);
    preamble[8..10].copy_from_slice(&VERSION.to_le_bytes());
    let check = crc32c::crc32c(&preamble[..10]);
    preamble[10..].copy_from_slice(&check.to_le_bytes());
    preamble
}

/// Checks `bytes`, a chunk's first [`PREAMBLE`] bytes, are this format's preamble.
///
/// # Errors
///
/// Saying whether the chunk is damaged, of another format, or no chunk of a log at all.
pub(crate) fn check_preamble(bytes: &[u8]) -> Result<(), Error> {
    let refused = |what: &str| Error::internal(format!("a write-ahead log chunk {what}"));
    let Some(preamble) = bytes.get(..PREAMBLE) else {
        return Err(refused("ends before its preamble"));
    };
    let check = u32::from_le_bytes([preamble[10], preamble[11], preamble[12], preamble[13]]);
    if crc32c::crc32c(&preamble[..10]) != check {
        return Err(refused("has a damaged preamble"));
    }
    if preamble[..8] != MAGIC {
        return Err(refused("is no chunk of a write-ahead log"));
    }
    let version = u16::from_le_bytes([preamble[8], preamble[9]]);
    if version != VERSION {
        let detail = format!("is of format {version}, and this build reads {VERSION}");
        return Err(refused(&detail));
    }
    Ok(())
}

/// The checksum of a frame of `kind` holding `payload`.
fn checksum(kind: u8, payload: &[u8]) -> Result<u32, Error> {
    let len = u32::try_from(payload.len())
        .map_err(|_| Error::internal("a write-ahead log frame beyond 4 GiB"))?;
    let mut head = [0; 5];
    head[0] = kind;
    head[1..].copy_from_slice(&len.to_le_bytes());
    Ok(crc32c::crc32c_append(crc32c::crc32c(&head), payload))
}

/// A frame of `kind` holding `payload`.
fn framed(kind: u8, payload: &[u8]) -> Result<Bytes, Error> {
    let check = checksum(kind, payload)?;
    let mut frame = BytesMut::with_capacity(HEAD + payload.len());
    frame.put_u8(kind);
    frame.put_u32_le(u32::try_from(payload.len()).unwrap_or(u32::MAX));
    frame.put_u32_le(check);
    frame.put_slice(payload);
    Ok(frame.freeze())
}

fn json(value: &impl Serialize) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(value).map_err(|error| unencoded(&error))
}

/// The error of a frame whose payload does not encode.
fn unencoded(error: &serde_json::Error) -> Error {
    Error::internal(format!("encoding a write-ahead log frame: {error}"))
}

fn batch_payload(batch: &Batch) -> Result<Vec<u8>, Error> {
    let header = json(&BatchHeader {
        segment: batch.segment,
        table: batch.table,
        ordinal: batch.ordinal,
        rows: u64::try_from(batch.batch.num_rows()).unwrap_or(u64::MAX),
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

/// The error for a frame that does not read, as `what` says.
fn garbled(what: &dyn std::fmt::Display) -> Error {
    Error::internal(format!("a write-ahead log frame does not read: {what}"))
}

/// The kind and payload of the frame `bytes` holds whole, its checksum checked.
fn opened(bytes: &[u8]) -> Result<(u8, &[u8]), Error> {
    let head = bytes
        .get(..HEAD)
        .ok_or_else(|| garbled(&"it ends before its head"))?;
    let payload = &bytes[HEAD..];
    let announced = payload_len(head).and_then(|len| usize::try_from(len).ok());
    if announced != Some(payload.len()) {
        return Err(garbled(&"its length is not what it holds"));
    }
    let check = u32::from_le_bytes([head[5], head[6], head[7], head[8]]);
    if checksum(head[0], payload)? != check {
        return Err(garbled(&"its checksum does not match"));
    }
    Ok((head[0], payload))
}

/// A frame's checksum, summed over its head and its payload as the payload is read a piece at a
/// time.
pub(crate) struct Summing {
    expected: u32,
    sum: u32,
}

impl Summing {
    /// The checksum the frame whose head, its first [`HEAD`] bytes, is `head` says it has.
    pub(crate) fn of(head: &[u8]) -> Option<Self> {
        let (counted, expected) = (head.get(..5)?, head.get(5..HEAD)?);
        Some(Self {
            expected: u32::from_le_bytes([expected[0], expected[1], expected[2], expected[3]]),
            sum: crc32c::crc32c(counted),
        })
    }

    /// Adds the next `piece` of the payload.
    pub(crate) fn add(&mut self, piece: &[u8]) {
        self.sum = crc32c::crc32c_append(self.sum, piece);
    }

    /// Checks the sum of the whole payload against the checksum the head says.
    pub(crate) fn check(&self) -> Result<(), Error> {
        if self.sum != self.expected {
            return Err(garbled(&"its checksum does not match"));
        }
        Ok(())
    }
}

/// The header a batch frame's payload, starting with `start`, opens with.
///
/// # Errors
///
/// Where `start` does not hold the whole header, or it does not read.
pub(crate) fn batch_header_in(start: &[u8]) -> Result<BatchHeader, Error> {
    batch_header(start).map(|(header, _)| header)
}

/// A frame as a scan reads it: a batch's header, without its batch, or any other frame whole.
pub(crate) enum Skimmed {
    Batch(BatchHeader),
    Other(Box<Frame>),
}

/// The frame `bytes` holds whole, as [`decode`] reads it but for a batch's data, which is left
/// undecoded.
///
/// # Errors
///
/// Where the frame is not whole, its checksum does not match, or it does not decode.
pub(crate) fn skim(bytes: &[u8]) -> Result<Skimmed, Error> {
    let (kind, payload) = opened(bytes)?;
    match kind {
        BATCH => batch_header(payload).map(|(header, _)| Skimmed::Batch(header)),
        kind => parsed(kind, payload).map(|frame| Skimmed::Other(Box::new(frame))),
    }
}

/// The frame `bytes` holds whole, its batch decoded within `limits`.
///
/// # Errors
///
/// As [`skim`], and where a batch's data does not hold together, passes `limits`, or holds other
/// than the rows its header says.
#[cfg(test)]
pub(crate) fn decode(bytes: &Bytes, limits: rdlt_wire::Limits) -> Result<Frame, Error> {
    let (kind, payload) = opened(bytes)?;
    if kind != BATCH {
        return parsed(kind, payload);
    }
    pending(bytes, limits)?.decode().map(Frame::Batch)
}

/// A batch frame read and measured, its batch not decoded yet.
pub(crate) struct Pending {
    pub(crate) header: BatchHeader,
    batch: arrow::Measured,
}

impl Pending {
    /// Bytes: what decoding the batch allocates.
    pub(crate) fn held(&self) -> u64 {
        self.batch.held()
    }

    /// The batch, which must hold the rows its header says.
    pub(crate) fn decode(self) -> Result<Batch, Error> {
        let batch = self.batch.decode()?;
        if u64::try_from(batch.num_rows()).ok() != Some(self.header.rows) {
            return Err(garbled(&format!(
                "its batch holds {} rows, and its header says {}",
                batch.num_rows(),
                self.header.rows
            )));
        }
        Ok(Batch {
            segment: self.header.segment,
            table: self.header.table,
            ordinal: self.header.ordinal,
            batch,
        })
    }
}

/// The batch frame `bytes` holds whole, checked and measured within `limits`, not decoded.
///
/// # Errors
///
/// Where the frame is not whole, its checksum does not match, it is no batch, or its data does
/// not hold together or passes `limits`.
pub(crate) fn pending(bytes: &Bytes, limits: rdlt_wire::Limits) -> Result<Pending, Error> {
    let (kind, payload) = opened(bytes)?;
    if kind != BATCH {
        return Err(garbled(&format!("a frame of kind {kind} is no batch")));
    }
    let (header, end) = batch_header(payload)?;
    let batch = arrow::measured(bytes.slice(HEAD + end..), limits)?;
    Ok(Pending { header, batch })
}

/// The header a batch frame's payload starts with, and where its batch begins.
fn batch_header(payload: &[u8]) -> Result<(BatchHeader, usize), Error> {
    let len = payload
        .get(..4)
        .map(|len| u32::from_le_bytes([len[0], len[1], len[2], len[3]]))
        .ok_or_else(|| garbled(&"its batch has no header"))?;
    let end = 4 + usize::try_from(len).unwrap_or(usize::MAX);
    let header = payload
        .get(4..end)
        .ok_or_else(|| garbled(&"its batch's header ends early"))?;
    let header = serde_json::from_slice(header).map_err(|error| garbled(&error))?;
    Ok((header, end))
}

/// The frame of `kind` that is no batch, holding `payload`.
fn parsed(kind: u8, payload: &[u8]) -> Result<Frame, Error> {
    match kind {
        HEADER => parse(payload).map(Frame::Header),
        SCHEMA => parse(payload).map(Frame::Schema),
        SEAL => parse(payload).map(Frame::Seal),
        COMMIT => parse(payload).map(|commit| Frame::Commit(Box::new(commit))),
        CLOSED if payload.is_empty() => Ok(Frame::Closed),
        BEGUN => parse(payload).map(Frame::Begun),
        END => parse(payload).map(Frame::End),
        FENCE => parse(payload).map(Frame::Fence),
        RELIEVED if payload.is_empty() => Ok(Frame::Relieved),
        other => Err(garbled(&format!("no frame is of kind {other}"))),
    }
}

fn parse<T: DeserializeOwned>(payload: &[u8]) -> Result<T, Error> {
    serde_json::from_slice(payload).map_err(|error| garbled(&error))
}

/// The frames of `chunk`, a whole chunk's bytes, each decoded within `limits`.
///
/// # Errors
///
/// Where its preamble is not this format's, or any frame does not read.
#[cfg(test)]
pub(crate) fn frames(chunk: &[u8], limits: rdlt_wire::Limits) -> Result<Vec<Frame>, Error> {
    check_preamble(chunk)?;
    let shared = Bytes::copy_from_slice(chunk);
    let mut frames = Vec::new();
    let mut offset = PREAMBLE;
    while offset < chunk.len() {
        let len = payload_len(&chunk[offset..])
            .and_then(|len| usize::try_from(len).ok())
            .and_then(|len| len.checked_add(HEAD))
            .ok_or_else(|| garbled(&"it ends before a frame's head"))?;
        let end = offset
            .checked_add(len)
            .filter(|end| *end <= chunk.len())
            .ok_or_else(|| garbled(&"it ends inside a frame"))?;
        frames.push(decode(&shared.slice(offset..end), limits)?);
        offset = end;
    }
    Ok(frames)
}
