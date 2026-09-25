//! The parts of a lowering plan only merge tables use: key checks, sequences and compaction.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::builder::BinaryBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::UInt32Type;
use arrow_array::{ArrayRef, BooleanArray, RecordBatch, UInt32Array};
use arrow_row::{RowConverter, SortField};
use rdlt_connector::{LogicalType, StreamName};

use super::{Source, Stamp, lower_array};
use crate::error::Error;
use crate::table::TableView;

/// The positions of the rows `kept` keeps.
pub(super) fn positions(kept: &BooleanArray) -> ArrayRef {
    let kept = (0..kept.len()).filter(|row| kept.value(*row));
    Arc::new(UInt32Array::from_iter_values(
        kept.map(|row| u32::try_from(row).unwrap_or(u32::MAX)),
    ))
}

/// Refuses a merge batch that lacks a key column or holds a null key.
pub(super) fn check_key(
    stream: &StreamName,
    view: &TableView,
    batch: &RecordBatch,
    sources: &[Source],
) -> Result<(), Error> {
    if view.table.merge.is_none() {
        return Ok(());
    }
    let refuse = |code: &str, detail: String| {
        Err(Error::schema(format!("stream {stream}: {detail}"))
            .with_code(code)
            .with_stream(stream))
    };
    if view.key.len() < view.key_len {
        return refuse(
            "merge_key_missing",
            "the table has no column for part of the merge key".to_owned(),
        );
    }
    for column in &view.key {
        let name = view.model.columns[*column].name();
        match &sources[*column] {
            Source::Nulls => {
                return refuse(
                    "merge_key_missing",
                    format!("a batch has no key column {name}"),
                );
            }
            Source::Incoming(index, _) if batch.column(*index).logical_null_count() > 0 => {
                return refuse("merge_key_null", format!("key column {name} holds a null"));
            }
            Source::Incoming(..) => {}
        }
    }
    Ok(())
}

/// Each row's sequence in a merge table: its segment, then its position among the segment's rows,
/// the row's own unless `positions` gives each row's.
pub(super) fn sequence(
    view: &TableView,
    stamp: &Stamp,
    rows: usize,
    positions: Option<&ArrayRef>,
) -> Result<ArrayRef, arrow_schema::ArrowError> {
    let lowered = view.physical[view.model.columns.len() + 2].logical_type();
    let positions = positions.map(AsArray::as_primitive::<UInt32Type>);
    let mut seq = BinaryBuilder::with_capacity(rows, rows * 16);
    for row in 0..rows {
        let offset = positions.map_or(row as u64, |positions| u64::from(positions.value(row)));
        let mut bytes = [0_u8; 16];
        bytes[..8].copy_from_slice(&stamp.segment.0.to_be_bytes());
        bytes[8..].copy_from_slice(&(stamp.first_row + offset).to_be_bytes());
        seq.append_value(bytes);
    }
    let seq: ArrayRef = Arc::new(seq.finish());
    lower_array(&seq, &LogicalType::Binary, lowered)
}

/// `batch` with only the last row of each key, in their order: the rows a merge keeps.
pub(super) fn compact(
    batch: &RecordBatch,
    key: &[usize],
) -> Result<RecordBatch, arrow_schema::ArrowError> {
    let columns: Vec<ArrayRef> = key
        .iter()
        .map(|index| Arc::clone(batch.column(*index)))
        .collect();
    let fields = columns
        .iter()
        .map(|column| SortField::new(column.data_type().clone()))
        .collect();
    let rows = RowConverter::new(fields)?.convert_columns(&columns)?;
    let mut last: BTreeMap<&[u8], u32> = BTreeMap::new();
    for row in 0..batch.num_rows() {
        last.insert(rows.row(row).data(), u32::try_from(row).unwrap_or(u32::MAX));
    }
    if last.len() == batch.num_rows() {
        return Ok(batch.clone());
    }
    let mut kept: Vec<u32> = last.into_values().collect();
    kept.sort_unstable();
    arrow_select::take::take_record_batch(batch, &UInt32Array::from(kept))
}
