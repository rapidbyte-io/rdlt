//! The batches an Arrow file is written as: each one a reader accepts.

#[cfg(test)]
mod tests;

use arrow_array::RecordBatch;
use arrow_ipc::writer::{
    DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteContext, IpcWriteOptions,
};
use bytes::Bytes;
use rdlt_connector::{ConnectorError, LimitExceeded, Result};
use rdlt_wire::{Decoder, IpcFrame, Limit, Limits, WireError};

use crate::limits::CHUNK_BYTES;

/// `batch` as batches of about [`CHUNK_BYTES`] each, by its rows' average size, and of at most
/// the rows a reader under `limits` accepts in one batch; [`fitted`] halves those whose rows
/// are far from their average.
pub(super) fn chunks<'a>(
    batch: &'a RecordBatch,
    limits: &Limits,
) -> impl Iterator<Item = RecordBatch> + 'a {
    let rows = batch.num_rows();
    let bytes = u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX);
    let row = (bytes / u64::try_from(rows).unwrap_or(u64::MAX).max(1)).max(1);
    let step = usize::try_from((CHUNK_BYTES / row).min(limits.batch_rows))
        .unwrap_or(usize::MAX)
        .max(1);
    (0..rows)
        .step_by(step)
        .map(move |from| batch.slice(from, step.min(rows - from)))
}

/// Writes `batch` with `write` as batches a reader under `limits` accepts, in order.
///
/// A batch beyond a limit, as one cut by its rows' average size is when a few rows are far
/// larger than the rest, is halved by its rows until its parts fit: as many steps as its rows
/// halve, each part encoded once to measure it.
///
/// # Errors
///
/// The limit one row alone is beyond, or a dictionary is, which no halving changes; or what
/// `write` fails with.
pub(super) fn fitted(
    batch: RecordBatch,
    limits: &Limits,
    write: &mut dyn FnMut(&RecordBatch) -> Result<()>,
) -> Result<()> {
    let mut pending = vec![batch];
    while let Some(part) = pending.pop() {
        let rows = part.num_rows();
        match unfit(&part, limits)? {
            None => write(&part)?,
            Some(limit) if rows <= 1 => return Err(limit),
            Some(_) => {
                // The later half waits while the earlier one is written.
                let half = rows / 2;
                pending.push(part.slice(half, rows - half));
                pending.push(part.slice(0, half));
            }
        }
    }
    Ok(())
}

/// The limit a reader under `limits` would refuse `batch` for, where fewer of its rows might
/// pass; none for a batch a reader accepts.
///
/// The batch is encoded as a writer writes it, and each frame is checked by the wire's decoder
/// as a reader checks it before decoding it.
///
/// # Errors
///
/// A limit fewer rows would not pass: a dictionary's, which every batch of the file shares.
fn unfit(batch: &RecordBatch, limits: &Limits) -> Result<Option<ConnectorError>> {
    let (generator, options) = (IpcDataGenerator::default(), IpcWriteOptions::default());
    // The tracker numbers the schema's dictionaries, as a writer's does when it starts a file.
    let mut tracker = DictionaryTracker::new(false);
    let schema = generator.schema_to_bytes_with_dictionary_tracker(
        batch.schema_ref(),
        &mut tracker,
        &options,
    );
    let mut decoder = Decoder::new(*limits);
    decoder
        .schema(&Bytes::from(schema.ipc_message))
        .map_err(refused)?;
    let (dictionaries, encoded) = generator
        .encode(
            batch,
            &mut tracker,
            &options,
            &mut IpcWriteContext::default(),
        )
        .map_err(|error| ConnectorError::data(format!("encoding a batch: {error}")))?;
    // A reader takes each dictionary as a frame of its own.
    for dictionary in dictionaries {
        if let Some(limit) = beyond(&decoder, dictionary, limits)? {
            return Err(limit);
        }
    }
    beyond(&decoder, encoded, limits)
}

/// Bytes: what a frame holds besides its message and its body, their prefix and padding.
const FRAMING: u64 = 128;

/// The limit a reader under `limits` refuses the frame `encoded` for: by its bytes in the file,
/// or as `decoder` measures it.
///
/// # Errors
///
/// A frame the decoder does not take as one, which Arrow's writer never writes.
fn beyond(
    decoder: &Decoder,
    encoded: EncodedData,
    limits: &Limits,
) -> Result<Option<ConnectorError>> {
    let bytes = encoded
        .ipc_message
        .len()
        .saturating_add(encoded.arrow_data.len());
    let bytes = u64::try_from(bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(FRAMING);
    if bytes > limits.frame_bytes {
        return Ok(Some(exceeds(
            Limit::FrameBytes.name(),
            limits.frame_bytes,
            bytes,
        )));
    }
    let frame = IpcFrame {
        header: Bytes::from(encoded.ipc_message),
        body: Bytes::from(encoded.arrow_data),
    };
    match decoder.held(&frame) {
        Ok(_) => Ok(None),
        Err(WireError::Refused(refusal)) => Ok(Some(ConnectorError::from(refusal))),
        Err(error) => Err(refused(error)),
    }
}

/// What the wire's decoder refused, as a data error: a limit's refusal keeps its limit.
fn refused(error: WireError) -> ConnectorError {
    match error {
        WireError::Refused(refusal) => ConnectorError::from(refusal),
        other => ConnectorError::data(format!("encoding a batch: {other}")).with_source(other),
    }
}

fn exceeds(name: &'static str, limit: u64, actual: u64) -> ConnectorError {
    ConnectorError::exceeds(LimitExceeded {
        name,
        limit,
        actual,
    })
}
