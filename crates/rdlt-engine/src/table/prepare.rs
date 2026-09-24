//! Preparing a batch for its table: discards, exact conversions, lowering, metadata columns and,
//! for merge tables, the sequence column and compaction.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow_array::builder::{BinaryBuilder, FixedSizeBinaryBuilder};
use arrow_array::{
    Array, ArrayRef, BooleanArray, RecordBatch, TimestampMicrosecondArray, UInt32Array,
    new_null_array,
};
use arrow_row::{RowConverter, SortField};
use rdlt_connector::{Field, LoadId, LogicalType, SegmentId, StreamName, TableSchema};

use super::TableView;
use super::convert::{convert, text};
use super::lower::{LOAD_ID_TYPE, loaded_at_type};
use super::resolve::Route;
use crate::error::Error;

/// What the metadata columns of a batch hold.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Stamp {
    pub(crate) load_id: LoadId,
    /// When the load started.
    pub(crate) loaded_at: SystemTime,
    /// The batch's segment, and the position of its first row among the rows written to it.
    pub(crate) segment: SegmentId,
    pub(crate) first_row: u64,
}

/// A batch ready for its table, and what the schema policy discarded from it.
#[derive(Debug)]
pub(crate) struct Prepared {
    pub(crate) batch: RecordBatch,
    /// Rows dropped because they carried a discarded change.
    pub(crate) discarded_rows: u64,
    /// Values nulled because they carried a discarded change.
    pub(crate) discarded_values: u64,
}

/// `batch`, whose columns are `incoming`, as rows of `view`'s table.
///
/// Every column goes where `routes` sends it, converted exactly and lowered to how the
/// destination stores it, and the metadata columns follow. A merge table's batch keeps only the
/// last row of each key.
pub(crate) fn prepare(
    stream: &StreamName,
    view: &TableView,
    incoming: &TableSchema,
    batch: &RecordBatch,
    routes: &[Route],
    stamp: &Stamp,
) -> Result<Prepared, Error> {
    let failed = |error: arrow_schema::ArrowError| {
        Error::internal(format!("stream {stream}: preparing a batch: {error}"))
    };
    let (batch, discarded_rows) = discard_rows(batch, routes).map_err(failed)?;
    if batch.num_rows() == 0 {
        return Ok(Prepared {
            batch: RecordBatch::new_empty(Arc::clone(&view.schema)),
            discarded_rows,
            discarded_values: 0,
        });
    }
    let mut discarded_values = 0;
    let mut sources: BTreeMap<usize, usize> = BTreeMap::new();
    for (index, route) in routes.iter().enumerate() {
        match route {
            Route::Column(column) => {
                sources.insert(*column, index);
            }
            Route::DiscardValues => {
                let column = batch.column(index);
                discarded_values += (column.len() - column.null_count()) as u64;
            }
            Route::DiscardRows | Route::Skip => {}
        }
    }
    check_key(stream, view, &batch, &sources)?;
    let rows = batch.num_rows();
    let mut columns = Vec::with_capacity(view.physical.len());
    for (column, lowered) in view.model.columns.iter().zip(&view.lowered) {
        let position = columns.len();
        let array = match sources.get(&position) {
            Some(index) => {
                let from = incoming
                    .fields()
                    .iter()
                    .nth(*index)
                    .expect("routes follow the incoming columns")
                    .logical_type();
                store(stream, batch.column(*index), from, column, lowered)?
            }
            None => new_null_array(&lowered.to_arrow(), rows),
        };
        columns.push(array);
    }
    columns.extend(meta_columns(view, stamp, rows).map_err(failed)?);
    let prepared = RecordBatch::try_new(Arc::clone(&view.schema), columns).map_err(failed)?;
    let prepared = if view.table.merge.is_some() {
        compact(&prepared, &view.key).map_err(failed)?
    } else {
        prepared
    };
    Ok(Prepared {
        batch: prepared,
        discarded_rows,
        discarded_values,
    })
}

/// `array`, of type `from`, as `column` holds it and `lowered` stores it; a value the column
/// cannot hold fails the batch.
fn store(
    stream: &StreamName,
    array: &ArrayRef,
    from: &LogicalType,
    column: &Field,
    lowered: &LogicalType,
) -> Result<ArrayRef, Error> {
    convert(array, from, column.logical_type())
        .and_then(|array| lower_array(&array, column.logical_type(), lowered))
        .map_err(|error| {
            let detail = format!(
                "stream {stream}: column {} cannot hold a value of the batch: {error}",
                column.name()
            );
            Error::schema(detail)
                .with_code("value_unrepresentable")
                .with_stream(stream)
        })
}

/// `batch` without the rows holding a value in a column routed to [`Route::DiscardRows`], and
/// how many rows were dropped.
fn discard_rows(
    batch: &RecordBatch,
    routes: &[Route],
) -> Result<(RecordBatch, u64), arrow_schema::ArrowError> {
    let discarding: Vec<&ArrayRef> = routes
        .iter()
        .enumerate()
        .filter(|(_, route)| **route == Route::DiscardRows)
        .map(|(index, _)| batch.column(index))
        .collect();
    if discarding
        .iter()
        .all(|column| column.null_count() == column.len())
    {
        return Ok((batch.clone(), 0));
    }
    let keep: BooleanArray = (0..batch.num_rows())
        .map(|row| Some(discarding.iter().all(|column| column.is_null(row))))
        .collect();
    let kept = arrow_select::filter::filter_record_batch(batch, &keep)?;
    Ok((kept.clone(), (batch.num_rows() - kept.num_rows()) as u64))
}

/// Refuses a merge batch that lacks a key column or holds a null key.
fn check_key(
    stream: &StreamName,
    view: &TableView,
    batch: &RecordBatch,
    sources: &BTreeMap<usize, usize>,
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
        match sources.get(column) {
            None => {
                return refuse(
                    "merge_key_missing",
                    format!("a batch has no key column {name}"),
                );
            }
            Some(index) if batch.column(*index).null_count() > 0 => {
                return refuse("merge_key_null", format!("key column {name} holds a null"));
            }
            Some(_) => {}
        }
    }
    Ok(())
}

/// `array` of `logical` as the destination stores it: as it is, or as its text, which for
/// nested values and JSON is JSON (spec §8.7).
fn lower_array(
    array: &ArrayRef,
    logical: &LogicalType,
    lowered: &LogicalType,
) -> Result<ArrayRef, arrow_schema::ArrowError> {
    if lowered == logical {
        Ok(Arc::clone(array))
    } else {
        text(array, logical)
    }
}

/// The load id, the load's start and, for merge tables, each row's sequence.
fn meta_columns(
    view: &TableView,
    stamp: &Stamp,
    rows: usize,
) -> Result<Vec<ArrayRef>, arrow_schema::ArrowError> {
    let lowered = |index: usize| view.physical[view.model.columns.len() + index].logical_type();
    let mut load_ids = FixedSizeBinaryBuilder::with_capacity(rows, 16);
    for _ in 0..rows {
        load_ids.append_value(stamp.load_id.as_bytes())?;
    }
    let load_id: ArrayRef = Arc::new(load_ids.finish());
    let micros = stamp
        .loaded_at
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_micros()).unwrap_or(i64::MAX)
        });
    let loaded_at: ArrayRef =
        Arc::new(TimestampMicrosecondArray::from(vec![micros; rows]).with_timezone("UTC"));
    let mut columns = vec![
        lower_array(&load_id, &LOAD_ID_TYPE, lowered(0))?,
        lower_array(&loaded_at, &loaded_at_type(), lowered(1))?,
    ];
    if view.meta.seq.is_some() {
        let mut seq = BinaryBuilder::with_capacity(rows, rows * 16);
        for row in 0..rows as u64 {
            let mut bytes = [0_u8; 16];
            bytes[..8].copy_from_slice(&stamp.segment.0.to_be_bytes());
            bytes[8..].copy_from_slice(&(stamp.first_row + row).to_be_bytes());
            seq.append_value(bytes);
        }
        let seq: ArrayRef = Arc::new(seq.finish());
        columns.push(lower_array(&seq, &LogicalType::Binary, lowered(2))?);
    }
    Ok(columns)
}

/// `batch` with only the last row of each key, in their order: the rows a merge keeps.
fn compact(batch: &RecordBatch, key: &[usize]) -> Result<RecordBatch, arrow_schema::ArrowError> {
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
