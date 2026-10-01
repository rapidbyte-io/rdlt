//! A batch frame's Arrow data, in the wire's framing (`rdlt_wire::codec`): the IPC schema
//! message, then the dictionary and record batch messages, each length-prefixed.
//!
//! The wire's decoder checks every message's shape against its schema and its body before Arrow
//! reads it, so data a checksum passes but a crash or a bug garbled is refused, never a panic.

use arrow_array::RecordBatch;
use bytes::{Buf, BufMut, Bytes};
use rdlt_wire::{Decoder, Encoder, IpcFrame, Limits};

use crate::error::Error;

/// What a log's batches may hold: whatever the engine wrote, of any size, but nested no deeper
/// than the wire allows any connector's.
fn limits() -> Limits {
    Limits {
        frame_bytes: u64::MAX,
        batch_rows: u64::MAX,
        batch_values: u64::MAX,
        schema_columns: u64::MAX,
        schema_bytes: u64::MAX,
        control_string_bytes: u64::MAX,
        ..Limits::default()
    }
}

/// `batch`'s Arrow data.
pub(super) fn encode(batch: &RecordBatch) -> Result<Vec<u8>, Error> {
    let mut encoder = Encoder::default();
    let schema = encoder.schema(batch.schema_ref());
    let frames = encoder
        .batch(batch)
        .map_err(|error| Error::internal(format!("encoding a write-ahead log batch: {error}")))?;
    let mut out = Vec::new();
    put(&mut out, &schema)?;
    out.put_u32_le(length(frames.len())?);
    for frame in &frames {
        put(&mut out, &frame.header)?;
        put(&mut out, &frame.body)?;
    }
    Ok(out)
}

/// The error for a batch whose data does not decode, as `what` says.
fn garbled(what: &dyn std::fmt::Display) -> Error {
    Error::internal(format!("a write-ahead log batch does not decode: {what}"))
}

/// The batch `data` holds.
pub(super) fn decode(data: &[u8]) -> Result<RecordBatch, Error> {
    let mut data = Bytes::copy_from_slice(data);
    let mut decoder = Decoder::new(limits());
    decoder
        .schema(&take(&mut data)?)
        .map_err(|error| garbled(&error))?;
    let frames = count(&mut data)?;
    let mut batch = None;
    for _ in 0..frames {
        let frame = IpcFrame {
            header: take(&mut data)?,
            body: take(&mut data)?,
        };
        batch = decoder.frame(&frame).map_err(|error| garbled(&error))?;
    }
    if data.has_remaining() {
        return Err(garbled(&"bytes follow its last message"));
    }
    batch.ok_or_else(|| garbled(&"it holds no batch"))
}

fn length(len: usize) -> Result<u32, Error> {
    u32::try_from(len).map_err(|_| Error::internal("a write-ahead log batch message beyond 4 GiB"))
}

fn put(out: &mut Vec<u8>, bytes: &[u8]) -> Result<(), Error> {
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
