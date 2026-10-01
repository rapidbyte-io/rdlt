//! The batches an Arrow file is written as: each one a reader accepts.

#[cfg(test)]
mod tests;

use arrow_array::cast::AsArray as _;
use arrow_array::{Array, ArrayRef, OffsetSizeTrait, RecordBatch, make_array};
use arrow_ipc::writer::{
    DictionaryTracker, EncodedData, IpcDataGenerator, IpcWriteContext, IpcWriteOptions,
};
use arrow_schema::DataType;
use rdlt_connector::{ConnectorError, LimitExceeded, Result};
use rdlt_wire::Limits;

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

/// Checks that a reader under `limits` accepts `batch`, which holds at most a batch's rows,
/// whatever bytes it is written as: its dictionaries and the values its columns nest.
///
/// # Errors
///
/// The limit a reader would refuse the batch for.
pub(super) fn admitted(batch: &RecordBatch, limits: &Limits) -> Result<()> {
    batch
        .columns()
        .iter()
        .try_for_each(|column| node(column.as_ref(), limits))
}

/// Checks `array` and what it nests as [`admitted`] does.
fn node(array: &dyn Array, limits: &Limits) -> Result<()> {
    let length = u64::try_from(array.len()).unwrap_or(u64::MAX);
    let dictionary = matches!(array.data_type(), DataType::Dictionary(..));
    // A reader bounds a node's values by a batch's rows, or by the bits of the batch's body
    // where those are more: values that take no bytes are bounded by the rows alone.
    if length > limits.batch_rows && !weighs(array.data_type()) {
        return Err(exceeds(VALUES, limits.batch_rows, length));
    }
    for child in children(array) {
        let values = u64::try_from(child.len()).unwrap_or(u64::MAX);
        // A reader takes a dictionary as a batch of its own, of at most a batch's rows.
        if dictionary && values > limits.batch_rows {
            return Err(exceeds("batch rows", limits.batch_rows, values));
        }
        node(child.as_ref(), limits)?;
    }
    Ok(())
}

/// What `array` nests, as it is written: of a list, a map or a struct that is part of a
/// larger one, only the values its rows hold.
fn children(array: &dyn Array) -> Vec<ArrayRef> {
    fn held<O: OffsetSizeTrait>(offsets: &[O], values: &dyn Array) -> Vec<ArrayRef> {
        let first = offsets.first().map_or(0, |first| first.as_usize());
        let last = offsets.last().map_or(first, |last| last.as_usize());
        vec![values.slice(first, last.saturating_sub(first))]
    }
    match array.data_type() {
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            held(list.value_offsets(), list.values().as_ref())
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            held(list.value_offsets(), list.values().as_ref())
        }
        DataType::Map(..) => {
            let map = array.as_map();
            held(map.value_offsets(), map.entries())
        }
        DataType::Struct(_) => array.as_struct().columns().to_vec(),
        _ => {
            let data = array.to_data();
            data.child_data().iter().cloned().map(make_array).collect()
        }
    }
}

/// Whether every value of a column of `data_type` takes at least a bit of the batch it is
/// written in.
fn weighs(data_type: &DataType) -> bool {
    match data_type {
        DataType::Null | DataType::RunEndEncoded(..) | DataType::FixedSizeBinary(0) => false,
        DataType::Struct(fields) => fields.iter().any(|field| weighs(field.data_type())),
        DataType::FixedSizeList(item, size) => *size > 0 && weighs(item.data_type()),
        _ => true,
    }
}

/// Writes `batch` with `write` as batches a reader under `limits` accepts, in order.
///
/// A batch whose encoding no frame holds, as one cut by its rows' average size is when a few
/// rows are far larger than the rest, is halved by its rows until its parts fit: as many
/// steps as its rows halve, each part encoded once to measure it.
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

/// Bytes: what a frame holds besides its message and its body, their prefix and padding.
const FRAMING: u64 = 128;

/// The limit a reader under `limits` would refuse `batch` for, where fewer of its rows might
/// pass; none for a batch a reader accepts.
///
/// # Errors
///
/// A limit fewer rows would not pass: a dictionary's, which every batch of the file shares.
fn unfit(batch: &RecordBatch, limits: &Limits) -> Result<Option<ConnectorError>> {
    let rows = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
    if rows > limits.batch_rows {
        return Ok(Some(exceeds("batch rows", limits.batch_rows, rows)));
    }
    if let Err(refused) = admitted(batch, limits) {
        let nested = refused.limit().is_some_and(|limit| limit.name == VALUES);
        return if nested {
            Ok(Some(refused))
        } else {
            Err(refused)
        };
    }
    // What a batch holds in memory is more than what it is written as: one far within a frame
    // needs no measuring.
    let held = u64::try_from(batch.get_array_memory_size()).unwrap_or(u64::MAX);
    if held.saturating_add(FRAMING) <= limits.frame_bytes / 2 {
        return Ok(None);
    }
    let (generator, options) = (IpcDataGenerator::default(), IpcWriteOptions::default());
    // The tracker numbers the schema's dictionaries, as a writer's does when it starts a file.
    let mut tracker = DictionaryTracker::new(false);
    generator.schema_to_bytes_with_dictionary_tracker(batch.schema_ref(), &mut tracker, &options);
    let (dictionaries, encoded) = generator
        .encode(
            batch,
            &mut tracker,
            &options,
            &mut IpcWriteContext::default(),
        )
        .map_err(|error| ConnectorError::data(format!("encoding a batch: {error}")))?;
    let frame = |encoded: &EncodedData| {
        let bytes = encoded
            .ipc_message
            .len()
            .saturating_add(encoded.arrow_data.len());
        u64::try_from(bytes)
            .unwrap_or(u64::MAX)
            .saturating_add(FRAMING)
    };
    // A reader takes each dictionary as a frame of its own.
    if let Some(bytes) = dictionaries
        .iter()
        .map(&frame)
        .find(|bytes| *bytes > limits.frame_bytes)
    {
        return Err(exceeds("frame bytes", limits.frame_bytes, bytes));
    }
    let bytes = frame(&encoded);
    Ok((bytes > limits.frame_bytes).then(|| exceeds("frame bytes", limits.frame_bytes, bytes)))
}

/// The name of the limit on the values a column nests.
const VALUES: &str = "values per node";

fn exceeds(name: &'static str, limit: u64, actual: u64) -> ConnectorError {
    ConnectorError::exceeds(LimitExceeded {
        name,
        limit,
        actual,
    })
}
