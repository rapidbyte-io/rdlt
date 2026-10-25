//! The parts of a lowering plan only merge tables use: sequences, and the rows a merge keeps.

use std::sync::Arc;

use arrow_array::builder::BinaryBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::UInt32Type;
use arrow_array::{Array, ArrayRef, BooleanArray, UInt32Array};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use arrow_row::{RowConverter, Rows, SortField};
use arrow_schema::ArrowError;
use arrow_select::filter::FilterBuilder;
use rdlt_connector::LogicalType;

use super::{ChangeRows, LoweringPlan, Stamp, lower_array};
use crate::table::TableView;

/// The positions of the rows `kept` keeps.
pub(super) fn positions(kept: &BooleanArray) -> ArrayRef {
    let kept = (0..kept.len()).filter(|row| kept.value(*row));
    Arc::new(UInt32Array::from_iter_values(
        kept.map(|row| u32::try_from(row).unwrap_or(u32::MAX)),
    ))
}

/// Each row's sequence in a merge table: its segment, then its position among the segment's rows,
/// the row's own unless `positions` gives each row's.
pub(super) fn sequence(
    view: &TableView,
    stamp: &Stamp,
    rows: usize,
    positions: Option<&ArrayRef>,
) -> Result<ArrayRef, ArrowError> {
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

/// Which rows of a batch whose key columns are `keys` are the last of their key: the rows a
/// merge keeps; none where every row is.
pub(super) fn last_of_each_key(keys: &[ArrayRef]) -> Result<Option<BooleanArray>, ArrowError> {
    let (rows, order) = sorted(keys)?;
    let mut kept = BooleanBufferBuilder::new(order.len());
    kept.append_n(order.len(), false);
    for run in order.chunk_by(|left, right| rows.row(*left) == rows.row(*right)) {
        if let Some(last) = run.last() {
            kept.set_bit(*last, true);
        }
    }
    Ok(kept_or_none(kept.finish()))
}

/// The keys of a batch whose key columns are `keys`, as rows, and its rows' positions ordered by
/// key, then by position.
pub(super) fn sorted(keys: &[ArrayRef]) -> Result<(Rows, Vec<usize>), ArrowError> {
    let fields = keys
        .iter()
        .map(|key| SortField::new(key.data_type().clone()))
        .collect();
    let rows = RowConverter::new(fields)?.convert_columns(keys)?;
    let mut order: Vec<usize> = (0..rows.num_rows()).collect();
    order.sort_unstable_by(|left, right| {
        rows.row(*left).cmp(&rows.row(*right)).then(left.cmp(right))
    });
    Ok((rows, order))
}

/// `kept` as a mask of the rows kept, or none where it keeps every row.
pub(super) fn kept_or_none(kept: BooleanBuffer) -> Option<BooleanArray> {
    (kept.count_set_bits() < kept.len()).then(|| BooleanArray::new(kept, None))
}

/// The rows of a batch that `outer` keeps and `inner`, which says which of those rows stay,
/// keeps too; `inner` where `outer` keeps every row.
pub(super) fn within(outer: Option<&BooleanArray>, inner: &BooleanArray) -> BooleanArray {
    let Some(outer) = outer else {
        return inner.clone();
    };
    let mut kept = BooleanBufferBuilder::new(outer.len());
    kept.append_n(outer.len(), false);
    for (row, stays) in outer.values().set_indices().zip(inner.values().iter()) {
        if stays {
            kept.set_bit(row, true);
        }
    }
    BooleanArray::new(kept.finish(), None)
}

impl LoweringPlan {
    /// Cuts `columns`, a batch's model columns as its table stores them, `kept`, which of the
    /// rows received they are where the schema policy dropped some, and `changes`, their change
    /// rows, to the rows the table's merges keep.
    ///
    /// Each column is let go as its rows are copied, so columns nothing else holds are not held
    /// twice over.
    pub(super) fn merged(
        &self,
        columns: &mut [ArrayRef],
        kept: &mut Option<BooleanArray>,
        changes: &mut Option<ChangeRows>,
    ) -> Result<(), ArrowError> {
        let Some(keep) = self.compaction(columns, changes.as_ref())? else {
            return Ok(());
        };
        let predicate = FilterBuilder::new(&keep).optimize().build();
        for column in columns.iter_mut() {
            *column = predicate.filter(column.as_ref())?;
        }
        *changes = changes.take().map(|rows| rows.filter(&keep)).transpose()?;
        *kept = Some(within(kept.as_ref(), &keep));
        Ok(())
    }

    /// Which rows of a batch whose model columns, as stored, are `columns` its table's merges
    /// keep: the last row of each key, or for a change stream's, the rows no later row of their
    /// key supersedes; none where the table keeps every row, as history and child tables do.
    fn compaction(
        &self,
        columns: &[ArrayRef],
        changes: Option<&ChangeRows>,
    ) -> Result<Option<BooleanArray>, ArrowError> {
        let view = &self.view;
        if !view.compacts() {
            return Ok(None);
        }
        let keys: Vec<ArrayRef> = view
            .key
            .iter()
            .map(|column| Arc::clone(&columns[*column]))
            .collect();
        match (&view.meta.changes, changes) {
            (None, _) => last_of_each_key(&keys),
            (Some(names), Some(changes)) => {
                changes.unsuperseded(&keys, &self.written, names.deleted_at.is_none())
            }
            (Some(_), None) => Err(ArrowError::SchemaError(
                "a change stream's batch carries no change rows".to_owned(),
            )),
        }
    }
}

#[cfg(test)]
mod tests;
