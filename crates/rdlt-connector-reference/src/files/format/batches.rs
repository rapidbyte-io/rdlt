//! The batches an Arrow file is written as: each one a reader accepts.

#[cfg(test)]
mod tests;

use arrow_array::types::{Int16Type, Int32Type, Int64Type, RunEndIndexType};
use arrow_array::{Array as _, OffsetSizeTrait, RecordBatch, RunArray};
use arrow_data::ArrayData;
use arrow_ipc::writer::{
    DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteContext, IpcWriteOptions,
};
use arrow_schema::{DataType, UnionMode};
use bytes::Bytes;
use rdlt_connector::{ConnectorError, LimitExceeded, Result};
use rdlt_wire::{Decoder, IpcFrame, Limit, Limits, WireError};

use crate::limits::CHUNK_BYTES;

/// The code of a write refused for a union Arrow's writer writes from other rows than its own.
const UNION_UNWRITABLE: &str = "union_unwritable";

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
/// A limit fewer rows would not pass: a dictionary's, or the dictionaries' together, which every
/// batch of the file shares; or a union Arrow's writer would write from other rows than its own.
fn unfit(batch: &RecordBatch, limits: &Limits) -> Result<Option<ConnectorError>> {
    placed(batch)?;
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
    // A reader takes each dictionary as a frame of its own, and holds every one of the file.
    let mut held: u64 = 0;
    for dictionary in dictionaries {
        held = held.saturating_add(measured(&decoder, dictionary, limits)??);
    }
    limits
        .admit_dictionaries(held)
        .map_err(ConnectorError::from)?;
    Ok(measured(&decoder, encoded, limits)?.err())
}

/// Bytes: what a frame holds besides its message and its body, their prefix and padding.
const FRAMING: u64 = 128;

/// The bytes a reader under `limits` holds the frame `encoded` in, or the limit it refuses it for:
/// by its bytes in the file, or as `decoder` measures it.
///
/// # Errors
///
/// A frame the decoder does not take as one, which Arrow's writer never writes.
fn measured(decoder: &Decoder, encoded: EncodedData, limits: &Limits) -> Result<Result<u64>> {
    let bytes = encoded
        .ipc_message
        .len()
        .saturating_add(encoded.arrow_data.len());
    let bytes = u64::try_from(bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(FRAMING);
    if bytes > limits.frame_bytes {
        return Ok(Err(exceeds(
            Limit::FrameBytes.name(),
            limits.frame_bytes,
            bytes,
        )));
    }
    let frame = IpcFrame::new(
        Bytes::from(encoded.ipc_message),
        Bytes::from(encoded.arrow_data),
    );
    match decoder.held(&frame) {
        Ok(held) => Ok(Ok(held)),
        Err(WireError::Refused(refusal)) => Ok(Err(ConnectorError::from(refusal))),
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

/// Checks that Arrow's writer writes each union `batch` holds as the rows it holds.
///
/// It writes a union's type ids and offsets, and a sparse union's children, whole, where its
/// reader reads as many as the union has rows. A list's items are cut where they are written,
/// so a union inside a list or a map that holds only some of its rows is written from its first
/// row: its reader then refuses the file, or reads a dense union's other rows.
///
/// # Errors
///
/// A data error naming the first column that holds such a union.
fn placed(batch: &RecordBatch) -> Result<()> {
    let columns = batch.schema_ref().fields().iter().zip(batch.columns());
    for (field, column) in columns {
        if misplaced(&column.to_data()) {
            let message = format!(
                "column \"{}\": a union inside a list or a map that holds only some of its rows \
                 is not written, since Arrow writes it from its first row and its reader then \
                 refuses the file or reads other rows",
                field.name()
            );
            return Err(ConnectorError::data(message).with_code(UNION_UNWRITABLE));
        }
    }
    Ok(())
}

/// Whether Arrow's writer writes a union in `data` from other rows than the union's own, as it
/// reaches each column nested in it.
fn misplaced(data: &ArrayData) -> bool {
    match data.data_type() {
        DataType::Union(_, mode) => {
            let cut = data
                .child_data()
                .iter()
                .any(|child| child.len() != data.len());
            data.offset() != 0
                || (*mode == UnionMode::Sparse && cut)
                || data.child_data().iter().any(misplaced)
        }
        DataType::List(_) | DataType::Map(..) => listed(data, data.buffer::<i32>(0)),
        DataType::LargeList(_) => listed(data, data.buffer::<i64>(0)),
        DataType::FixedSizeList(_, size) => {
            let size = usize::try_from(*size).unwrap_or(usize::MAX);
            let items = data.offset().saturating_mul(size);
            misplaced(&data.child_data()[0].slice(items, data.len().saturating_mul(size)))
        }
        DataType::RunEndEncoded(ends, _) => match ends.data_type() {
            DataType::Int16 => spanned::<Int16Type>(data),
            DataType::Int32 => spanned::<Int32Type>(data),
            _ => spanned::<Int64Type>(data),
        },
        _ => data.child_data().iter().any(misplaced),
    }
}

/// Whether the items of the list `data`, of `offsets` from its first row, are written
/// misplaced: the writer cuts its child to the items its rows hold.
fn listed<O: OffsetSizeTrait>(data: &ArrayData, offsets: &[O]) -> bool {
    let child = &data.child_data()[0];
    if data.is_empty() {
        return misplaced(&child.slice(0, 0));
    }
    let (first, last) = (offsets[0].as_usize(), offsets[data.len()].as_usize());
    misplaced(&child.slice(first, last - first))
}

/// Whether the values of the run-end column `data` are written misplaced: unless its rows span
/// every run, the writer cuts its values, as an array, to the runs they span.
fn spanned<R: RunEndIndexType>(data: &ArrayData) -> bool {
    let runs = RunArray::<R>::from(data.clone());
    let ends = runs.run_ends();
    if ends.offset() == 0 && ends.max_value() == ends.len() {
        return data.child_data().iter().any(misplaced);
    }
    let (start, end) = (
        ends.get_start_physical_index(),
        ends.get_end_physical_index(),
    );
    misplaced(&runs.values().slice(start, end - start + 1).to_data())
}

fn exceeds(name: &'static str, limit: u64, actual: u64) -> ConnectorError {
    ConnectorError::exceeds(LimitExceeded {
        name,
        limit,
        actual,
    })
}
