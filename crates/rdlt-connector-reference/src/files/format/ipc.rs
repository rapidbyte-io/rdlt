//! Arrow IPC files read block by block, each block checked against the file and the frame limit
//! before a byte of it is held, and decoded by the wire's decoder under the wire's limits.

use std::fs::File;
use std::io::{Read as _, Seek as _, SeekFrom};

use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use bytes::Bytes;
use rdlt_connector::{ConnectorError, LimitExceeded, Result};
use rdlt_wire::{Decoder, IpcFrame, Limits, WireError};

/// What an Arrow IPC file starts with, padded to eight bytes, and ends with.
const MAGIC: &[u8; 6] = b"ARROW1";

/// Bytes: what a file holds before its first message, and after its footer: the footer's length
/// and the closing magic.
const HEAD: u64 = 8;
const TAIL: u64 = 10;

/// The marker a message's length follows, since Arrow 0.15.
const CONTINUATION: [u8; 4] = [0xff; 4];

/// One message of the file: where it starts, and how long its metadata and its body are.
#[derive(Clone, Copy, Debug)]
struct Block {
    offset: u64,
    metadata: usize,
    body: usize,
}

/// An Arrow IPC file, its record batches read in order.
#[derive(Debug)]
pub(super) struct IpcFile {
    file: File,
    decoder: Decoder,
    schema: SchemaRef,
    batches: std::vec::IntoIter<Block>,
}

/// A file that is not what an Arrow IPC writer writes.
fn malformed(what: impl std::fmt::Display) -> ConnectorError {
    ConnectorError::data(format!("the Arrow file is malformed: {what}"))
}

/// A frame the wire's decoder refused, as a data error: a limit's refusal keeps its limit.
fn refused(error: WireError) -> ConnectorError {
    match error {
        WireError::Refused(refusal) => ConnectorError::exceeds(LimitExceeded {
            name: refusal.field,
            limit: refusal.limit,
            actual: refusal.actual,
        }),
        other => {
            ConnectorError::data(format!("the Arrow file is malformed: {other}")).with_source(other)
        }
    }
}

fn read(error: std::io::Error) -> ConnectorError {
    malformed(format_args!("it ends before what it declares: {error}")).with_source(error)
}

impl IpcFile {
    /// Opens `file`, a regular file, checking its footer and every block the footer lists
    /// against the file's size and `limits`, and reading its schema and dictionaries.
    pub(super) fn open(mut file: File, limits: Limits) -> Result<Self> {
        let size = file.metadata().map_err(read)?.len();
        let (dictionaries, batches) = blocks(&mut file, size, &limits)?;
        let mut decoder = Decoder::new(limits);
        let schema = schema(&mut file, size, &limits, &mut decoder)?;
        let mut opened = Self {
            file,
            decoder,
            schema,
            batches: Vec::new().into_iter(),
        };
        for block in dictionaries {
            if opened.decode(block)?.is_some() {
                return Err(malformed("a dictionary block holds a record batch"));
            }
        }
        opened.batches = batches.into_iter();
        Ok(opened)
    }

    /// The schema of the file's batches.
    pub(super) fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// Skips the next `batches` record batches.
    pub(super) fn skip(&mut self, batches: u64) {
        let batches = usize::try_from(batches).unwrap_or(usize::MAX);
        if batches > 0 {
            self.batches.nth(batches - 1);
        }
    }

    /// The next record batch, none once every batch is read.
    pub(super) fn next(&mut self) -> Result<Option<RecordBatch>> {
        let Some(block) = self.batches.next() else {
            return Ok(None);
        };
        match self.decode(block)? {
            Some(batch) => Ok(Some(batch)),
            None => Err(malformed("a record batch block holds a dictionary")),
        }
    }

    /// Reads and decodes the message at `block`.
    fn decode(&mut self, block: Block) -> Result<Option<RecordBatch>> {
        let mut bytes = vec![0; block.metadata + block.body];
        self.file
            .seek(SeekFrom::Start(block.offset))
            .and_then(|_| self.file.read_exact(&mut bytes))
            .map_err(read)?;
        let bytes = Bytes::from(bytes);
        let header = message(&bytes.slice(..block.metadata))?;
        let frame = IpcFrame {
            header,
            body: bytes.slice(block.metadata..),
        };
        self.decoder.frame(&frame).map_err(refused)
    }
}

/// The message flatbuffer in `metadata`: a continuation marker, the message's length, the
/// message, and padding.
fn message(metadata: &Bytes) -> Result<Bytes> {
    let (Some(marker), Some(length)) = (metadata.get(..4), metadata.get(4..8)) else {
        return Err(malformed("a message does not start as one"));
    };
    if marker != CONTINUATION {
        return Err(malformed("a message does not start as one"));
    }
    let length = i32::from_le_bytes([length[0], length[1], length[2], length[3]]);
    usize::try_from(length)
        .ok()
        .and_then(|length| length.checked_add(8))
        .filter(|end| metadata.get(..*end).is_some())
        .map(|end| metadata.slice(8..end))
        .ok_or_else(|| malformed("a message is longer than its block"))
}

/// The dictionary and record batch blocks the footer of `file`, `size` bytes long, lists, each
/// lying between the file's head and its footer and within the frame limit.
fn blocks(file: &mut File, size: u64, limits: &Limits) -> Result<(Vec<Block>, Vec<Block>)> {
    let Some(tail) = size
        .checked_sub(TAIL)
        .filter(|tail| tail.checked_sub(HEAD).is_some())
    else {
        return Err(malformed("it is shorter than an empty file"));
    };
    let mut ending = [0; 10];
    file.seek(SeekFrom::Start(tail))
        .and_then(|_| file.read_exact(&mut ending))
        .map_err(read)?;
    if &ending[4..] != MAGIC {
        return Err(malformed("it does not end as one"));
    }
    let length = i32::from_le_bytes([ending[0], ending[1], ending[2], ending[3]]);
    // The footer lies between the head and the tail.
    let (length, start) = u64::try_from(length)
        .ok()
        .and_then(|length| Some((length, (tail - HEAD).checked_sub(length)? + HEAD)))
        .ok_or_else(|| malformed("its footer lies outside it"))?;
    let admitted = usize::try_from(length).unwrap_or(usize::MAX);
    limits
        .admit_frame(admitted)
        .map_err(WireError::from)
        .map_err(refused)?;
    let mut footer = vec![0; admitted];
    file.seek(SeekFrom::Start(start))
        .and_then(|_| file.read_exact(&mut footer))
        .map_err(read)?;
    let footer = arrow_ipc::root_as_footer_with_opts(&verifier(limits), &footer)
        .map_err(|error| malformed(format_args!("its footer is no footer: {error}")))?;
    let listed = |blocks: Option<flatbuffers::Vector<'_, arrow_ipc::Block>>| {
        // Each block follows the block listed before it: no two share a byte of the file.
        let mut from = HEAD;
        blocks
            .into_iter()
            .flatten()
            .map(|block| {
                let block = checked(block, from, start, limits)?;
                from = block.offset + frame(&block);
                Ok(block)
            })
            .collect::<Result<Vec<Block>>>()
    };
    Ok((
        listed(footer.dictionaries())?,
        listed(footer.recordBatches())?,
    ))
}

/// The flatbuffers verifier a schema nested to the limit passes, as the wire's decoder sets it:
/// a schema deeper than the limit is refused by the limit, not as no footer.
fn verifier(limits: &Limits) -> flatbuffers::VerifierOptions {
    let depth = usize::try_from(limits.nesting_depth).unwrap_or(usize::MAX);
    flatbuffers::VerifierOptions {
        max_depth: depth.saturating_mul(4).saturating_add(64),
        ..flatbuffers::VerifierOptions::default()
    }
}

/// Bytes: the frame `block` holds, its metadata and its body.
fn frame(block: &Block) -> u64 {
    u64::try_from(block.metadata + block.body).unwrap_or(u64::MAX)
}

/// Checks that `block` lies between `from` and the file's footer at `footer`, and that the frame
/// it holds is within the frame limit.
fn checked(block: &arrow_ipc::Block, from: u64, footer: u64, limits: &Limits) -> Result<Block> {
    let outside = || malformed("a block lies outside the file, or within another block");
    let offset = u64::try_from(block.offset()).map_err(|_| outside())?;
    let metadata = u64::try_from(block.metaDataLength()).map_err(|_| outside())?;
    let body = u64::try_from(block.bodyLength()).map_err(|_| outside())?;
    let frame = metadata.checked_add(body).ok_or_else(outside)?;
    let end = offset.checked_add(frame).ok_or_else(outside)?;
    // It starts no earlier than `from`, and ends no later than the footer starts.
    if offset.checked_sub(from).is_none() || footer.checked_sub(end).is_none() {
        return Err(outside());
    }
    Limits::admit("frame bytes", limits.frame_bytes, frame)
        .map_err(WireError::from)
        .map_err(refused)?;
    let size = |bytes: u64| usize::try_from(bytes).map_err(|_| outside());
    Ok(Block {
        offset,
        metadata: size(metadata)?,
        body: size(body)?,
    })
}

/// Bytes: the furthest a file's first message starts, where its writer aligns to 64 bytes.
const FIRST_MESSAGE: usize = 64;

/// Reads the schema message `file` starts with into `decoder`, which checks it against its
/// limits.
///
/// The message follows the magic and its padding, to eight bytes or to its writer's alignment.
fn schema(file: &mut File, size: u64, limits: &Limits, decoder: &mut Decoder) -> Result<SchemaRef> {
    let mut head = [0; FIRST_MESSAGE + 8];
    let known = usize::try_from(size).map_or(head.len(), |size| size.min(head.len()));
    file.seek(SeekFrom::Start(0))
        .and_then(|_| file.read_exact(&mut head[..known]))
        .map_err(read)?;
    let starts = (8..=known.saturating_sub(8))
        .step_by(8)
        .find(|at| head[*at..*at + 8] != [0; 8])
        .filter(|at| head[..6] == *MAGIC && head[*at..*at + 4] == CONTINUATION);
    let Some(at) = starts else {
        return Err(malformed("it does not start as one"));
    };
    let length = i32::from_le_bytes([head[at + 4], head[at + 5], head[at + 6], head[at + 7]]);
    let length = u64::try_from(length)
        .ok()
        .filter(|length| size.checked_sub(*length).is_some())
        .ok_or_else(|| malformed("its schema lies outside it"))?;
    let admitted = usize::try_from(length).unwrap_or(usize::MAX);
    limits
        .admit_frame(admitted)
        .map_err(WireError::from)
        .map_err(refused)?;
    let mut message = vec![0; admitted];
    let after = u64::try_from(at + 8).unwrap_or(u64::MAX);
    file.seek(SeekFrom::Start(after))
        .and_then(|_| file.read_exact(&mut message))
        .map_err(read)?;
    decoder.schema(&Bytes::from(message)).map_err(refused)
}

/// Checks that `schema` is one a reader of the file accepts: within the limits on columns and
/// nesting the wire's decoder enforces.
pub(super) fn admitted(schema: &arrow_schema::Schema, limits: Limits) -> Result<()> {
    let message = rdlt_wire::Encoder::default().schema(schema);
    Decoder::new(limits)
        .schema(&message)
        .map(|_| ())
        .map_err(refused)
}
