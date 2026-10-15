//! A batch frame's Arrow data, in the wire's framing (`rdlt_wire::codec`): the IPC schema
//! message, the count of messages, then each dictionary and record batch message's header and
//! body, each after its length.
//!
//! A header is padded with zeros so the body after it lies at a multiple of 64 bytes from the
//! frame's first byte, as Arrow's IPC format pads metadata.
//!
//! The wire's decoder checks every message's shape against its schema and its body before Arrow
//! reads it, so data a checksum passes but a crash or a bug garbled is refused, never a panic.

use arrow_array::RecordBatch;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use rdlt_wire::aligned::ALIGNMENT;
use rdlt_wire::{Decoder, Encoder, IpcFrame, Limits};

use crate::error::Error;

/// What a log's batches may hold in an engine of `memory` bytes: what one request for lowering
/// may take, a quarter of the budget, in bytes of a frame, its schema and its strings, and in
/// rows and values, since each row of a piece costs at least a byte of what the piece reserved;
/// nested no deeper than the wire allows any connector's.
///
/// The engine writes no batch frame larger: a piece of lowering, and its frame, are within a
/// request.
pub(crate) fn limits(memory: u64) -> Limits {
    let request = memory / crate::limits::REQUEST_SHARE;
    Limits {
        frame_bytes: request,
        batch_rows: request,
        batch_values: request,
        schema_columns: request,
        schema_bytes: request,
        control_string_bytes: request,
        dictionary_bytes: request,
        ..Limits::default()
    }
}

/// A batch's Arrow data as the wire's encoder made it: its schema message, then its messages.
pub(super) struct Encoded {
    schema: Bytes,
    frames: Vec<IpcFrame>,
}

/// Zero bytes after a message's header of `header` bytes, whose first byte lies `at` bytes into
/// its frame, that put its body, after the body's length, at a multiple of [`ALIGNMENT`] from
/// the frame's first byte.
pub(super) fn padding(at: usize, header: usize) -> usize {
    let body = at + header + 4;
    (ALIGNMENT - body % ALIGNMENT) % ALIGNMENT
}

impl Encoded {
    /// Bytes: what the data takes framed from `at` bytes into its frame: each message after its
    /// length, each header padded so its body lies at a multiple of [`ALIGNMENT`].
    pub(super) fn len(&self, at: usize) -> usize {
        let mut end = at + 4 + self.schema.len() + 4;
        for frame in &self.frames {
            let header = frame.header.len() + padding(end + 4, frame.header.len());
            end += 4 + header + 4 + frame.body.len();
        }
        end - at
    }

    /// Writes the data to `out`, whose frame starts at `frame`: the schema message, the count of
    /// messages, then each message's header, padded with zeros to place its body, and its body,
    /// each after its length.
    pub(super) fn write(&self, out: &mut BytesMut, frame: usize) -> Result<(), Error> {
        prefixed(out, &self.schema)?;
        out.put_u32_le(length(self.frames.len())?);
        for message in &self.frames {
            let pad = padding(out.len() + 4 - frame, message.header.len());
            out.put_u32_le(length(message.header.len() + pad)?);
            out.put_slice(&message.header);
            out.put_bytes(0, pad);
            prefixed(out, &message.body)?;
        }
        Ok(())
    }
}

/// `batch`'s Arrow data, its messages kept as the encoder made them.
pub(super) fn encode(batch: &RecordBatch) -> Result<Encoded, Error> {
    let mut encoder = Encoder::default();
    let unencoded = |error: rdlt_wire::WireError| {
        Error::internal("encoding a write-ahead log batch").with_source(error)
    };
    let schema = encoder.schema(batch.schema_ref()).map_err(unencoded)?;
    let frames = encoder.batch(batch).map_err(unencoded)?;
    Ok(Encoded { schema, frames })
}

/// The error for a batch whose data does not decode, as `what` says.
fn garbled(what: &dyn std::fmt::Display) -> Error {
    Error::internal(format!("a write-ahead log batch does not decode: {what}"))
}

/// A batch's messages, checked and measured but not decoded: what decoding them holds is known
/// before it is made.
pub(crate) struct Measured {
    decoder: Decoder,
    frames: Vec<IpcFrame>,
    held: u64,
}

impl Measured {
    /// The batch's messages, as read.
    #[cfg(test)]
    pub(super) fn frames(&self) -> &[IpcFrame] {
        &self.frames
    }

    /// Bytes: what decoding the batch allocates, its dictionaries included.
    pub(crate) fn held(&self) -> u64 {
        self.held
    }

    /// The batch.
    pub(crate) fn decode(mut self) -> Result<RecordBatch, Error> {
        let mut batch = None;
        for frame in &self.frames {
            batch = self.decoder.frame(frame).map_err(|error| garbled(&error))?;
        }
        batch.ok_or_else(|| garbled(&"it holds no batch"))
    }
}

/// The batch `data` holds, within `limits`, measured: its messages are `data`'s bytes, never a
/// copy of them.
pub(super) fn measured(mut data: Bytes, limits: Limits) -> Result<Measured, Error> {
    let mut decoder = Decoder::new(limits);
    decoder
        .schema(&take(&mut data)?)
        .map_err(|error| garbled(&error))?;
    let count = count(&mut data)?;
    let (mut frames, mut held) = (Vec::new(), 0_u64);
    for _ in 0..count {
        let frame = IpcFrame::new(take(&mut data)?, take(&mut data)?);
        let bytes = decoder.held(&frame).map_err(|error| garbled(&error))?;
        held = held.saturating_add(bytes);
        frames.push(frame);
    }
    if data.has_remaining() {
        return Err(garbled(&"bytes follow its last message"));
    }
    Ok(Measured {
        decoder,
        frames,
        held,
    })
}

/// The batch `data` holds, within `limits`.
#[cfg(test)]
pub(super) fn decode(data: Bytes, limits: Limits) -> Result<RecordBatch, Error> {
    measured(data, limits)?.decode()
}

pub(super) fn length(len: usize) -> Result<u32, Error> {
    u32::try_from(len).map_err(|_| Error::internal("a write-ahead log batch message beyond 4 GiB"))
}

/// Writes `bytes` to `out` after their length.
fn prefixed(out: &mut BytesMut, bytes: &[u8]) -> Result<(), Error> {
    out.put_u32_le(length(bytes.len())?);
    out.put_slice(bytes);
    Ok(())
}

fn count(data: &mut Bytes) -> Result<u32, Error> {
    if data.remaining() < 4 {
        return Err(garbled(&"it ends before a length"));
    }
    Ok(data.get_u32_le())
}

/// The next length-prefixed message of `data`.
fn take(data: &mut Bytes) -> Result<Bytes, Error> {
    let len = usize::try_from(count(data)?).unwrap_or(usize::MAX);
    if data.remaining() < len {
        return Err(garbled(&"a message ends early"));
    }
    Ok(data.split_to(len))
}
