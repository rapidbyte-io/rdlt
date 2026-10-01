//! The batches an Arrow file is written as: each one a reader accepts.

#[cfg(test)]
mod tests;

use arrow_array::{Array, RecordBatch, make_array};
use arrow_schema::DataType;
use rdlt_connector::{ConnectorError, LimitExceeded, Result};
use rdlt_wire::Limits;

use crate::limits::CHUNK_BYTES;

/// `batch` as batches of about [`CHUNK_BYTES`] each, by its rows' average size, and of at most
/// the rows a reader under `limits` accepts in one batch.
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
        return Err(exceeds("values per node", limits.batch_rows, length));
    }
    for child in array.to_data().child_data() {
        let values = u64::try_from(child.len()).unwrap_or(u64::MAX);
        // A reader takes a dictionary as a batch of its own, of at most a batch's rows.
        if dictionary && values > limits.batch_rows {
            return Err(exceeds("batch rows", limits.batch_rows, values));
        }
        node(make_array(child.clone()).as_ref(), limits)?;
    }
    Ok(())
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

/// Checks that a batch written as `bytes` is a frame a reader under `limits` accepts.
pub(super) fn framed(bytes: u64, limits: &Limits) -> Result<()> {
    if bytes > limits.frame_bytes {
        return Err(exceeds("frame bytes", limits.frame_bytes, bytes));
    }
    Ok(())
}

fn exceeds(name: &'static str, limit: u64, actual: u64) -> ConnectorError {
    ConnectorError::exceeds(LimitExceeded {
        name,
        limit,
        actual,
    })
}
