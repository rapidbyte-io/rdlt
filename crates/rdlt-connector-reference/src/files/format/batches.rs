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
use rdlt_wire::{Limit, Limits};

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

/// The values a reader counts in the frame `batch` is written as: every value of every column,
/// nested ones included, and every item a list view names; checks that a reader under `limits`
/// accepts each dictionary's values, which are a frame of their own.
///
/// # Errors
///
/// The limit a reader would refuse a dictionary for.
fn counted(batch: &RecordBatch, limits: &Limits) -> Result<u64> {
    batch.columns().iter().try_fold(0_u64, |values, column| {
        Ok(values.saturating_add(values_of(column.as_ref(), limits)?))
    })
}

/// The values `array` and what it nests count in its frame, as [`counted`] counts them.
fn values_of(array: &dyn Array, limits: &Limits) -> Result<u64> {
    let mut values = count(array.len()).saturating_add(named(array));
    let dictionary = matches!(array.data_type(), DataType::Dictionary(..));
    for child in children(array) {
        let nested = values_of(child.as_ref(), limits)?;
        if dictionary {
            if nested > limits.batch_values {
                return Err(exceeds(
                    Limit::BatchValues.name(),
                    limits.batch_values,
                    nested,
                ));
            }
        } else {
            values = values.saturating_add(nested);
        }
    }
    Ok(values)
}

/// The items the list views of `array` name, which a reader counts beside its child's values.
fn named(array: &dyn Array) -> u64 {
    fn sizes<O: OffsetSizeTrait>(sizes: &[O]) -> u64 {
        sizes
            .iter()
            .map(|size| count(size.as_usize()))
            .fold(0, u64::saturating_add)
    }
    match array.data_type() {
        DataType::ListView(_) => sizes(array.as_list_view::<i32>().value_sizes()),
        DataType::LargeListView(_) => sizes(array.as_list_view::<i64>().value_sizes()),
        _ => 0,
    }
}

fn count(values: usize) -> u64 {
    u64::try_from(values).unwrap_or(u64::MAX)
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
        return Ok(Some(exceeds(
            Limit::BatchRows.name(),
            limits.batch_rows,
            rows,
        )));
    }
    let values = counted(batch, limits)?;
    if values > limits.batch_values {
        return Ok(Some(exceeds(
            Limit::BatchValues.name(),
            limits.batch_values,
            values,
        )));
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
        return Err(exceeds(Limit::FrameBytes.name(), limits.frame_bytes, bytes));
    }
    let bytes = frame(&encoded);
    Ok((bytes > limits.frame_bytes)
        .then(|| exceeds(Limit::FrameBytes.name(), limits.frame_bytes, bytes)))
}

fn exceeds(name: &'static str, limit: u64, actual: u64) -> ConnectorError {
    ConnectorError::exceeds(LimitExceeded {
        name,
        limit,
        actual,
    })
}
