//! The parts of a lowering plan only merge tables use: sequences, and the rows a merge keeps.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::builder::BinaryBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::UInt32Type;
use arrow_array::{Array, ArrayRef, BooleanArray, UInt32Array};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use arrow_row::{RowConverter, Rows, SortField};
use arrow_schema::{ArrowError, DataType};
use arrow_select::filter::FilterBuilder;
use rdlt_connector::LogicalType;

use super::{ChangeRows, LoweringPlan, Stamp, lower_array};
use crate::cost::{KEY_BATCH, KEY_FLUSH, KEY_ROW, KEY_VALUE};
use crate::normalize::Lineage;
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

/// Which rows of `batches`, each a batch's key columns, every batch holding one at least, are the
/// last of their key among the rows of every batch in order: a bit for each, set where no later
/// row holds the same key.
///
/// Keys of different types are never one key: where a key column's type differs between batches,
/// the rows of each type are compared apart, which leaves more rows the last of their key than a
/// merge by the values the types convert to would, never fewer. What it holds is within what
/// [`comparing`] says, where that says.
pub(crate) fn last_across(batches: &[Vec<ArrayRef>]) -> Result<BooleanBuffer, ArrowError> {
    let mut alike: BTreeMap<Vec<DataType>, Vec<usize>> = BTreeMap::new();
    let mut firsts = Vec::with_capacity(batches.len());
    let mut total = 0;
    for (index, keys) in batches.iter().enumerate() {
        let types = keys.iter().map(|key| key.data_type().clone()).collect();
        alike.entry(types).or_default().push(index);
        firsts.push(total);
        total += keys.first().map_or(0, Array::len);
    }
    let mut last = BooleanBufferBuilder::new(total);
    last.append_n(total, false);
    for (types, members) in alike {
        let converter = RowConverter::new(types.into_iter().map(SortField::new).collect())?;
        let count = members.iter().map(|index| batches[*index][0].len()).sum();
        let bytes = members.iter().map(|index| {
            let keys = &batches[*index];
            encoded(keys, keys[0].len()).unwrap_or(0)
        });
        // Sized to the most its rows take, so it is never grown: growing holds it twice over.
        let mut rows = converter.empty_rows(count, bytes.sum());
        for index in &members {
            converter.append(&mut rows, &batches[*index])?;
        }
        // The rows of a type are its batches' in order: a row's place among every batch's rows is
        // its batch's first place and its place among its batch's.
        let mut places = Vec::with_capacity(rows.num_rows());
        for index in members {
            places.extend(firsts[index]..firsts[index] + batches[index][0].len());
        }
        let mut order: Vec<usize> = (0..rows.num_rows()).collect();
        order.sort_unstable_by(|left, right| {
            rows.row(*left).cmp(&rows.row(*right)).then(left.cmp(right))
        });
        for run in order.chunk_by(|left, right| rows.row(*left) == rows.row(*right)) {
            if let Some(row) = run.last() {
                last.set_bit(places[*row], true);
            }
        }
    }
    Ok(last.finish())
}

/// Bytes: the most finding the last row of each key of `batches`' rows holds, as [`last_across`]
/// finds them: their keys as comparable rows, their offsets and order, and a bit for each; none
/// where a key column is of a type whose comparable row its own bytes do not bound, as a
/// dictionary's or a nested value's.
pub(crate) fn comparing(batches: &[Vec<ArrayRef>]) -> Option<u64> {
    let mut held = KEY_FLUSH;
    for keys in batches {
        let rows = keys.first().map_or(0, Array::len);
        let encoded = u64::try_from(encoded(keys, rows)?).ok()?;
        let rows = u64::try_from(rows).ok()?;
        held = held
            .saturating_add(KEY_BATCH)
            .saturating_add(encoded)
            .saturating_add(rows.saturating_mul(KEY_ROW))
            .saturating_add(rows.div_ceil(8));
    }
    Some(held)
}

/// Bytes: the most `keys`, the key columns of `rows` rows, take as comparable rows: each value a
/// sentinel and the padding of a block at most beside twice its own bytes; none where a column's
/// type does not bound its comparable row so.
fn encoded(keys: &[ArrayRef], rows: usize) -> Option<usize> {
    let mut bytes = 0_usize;
    for key in keys {
        let flat = key.data_type().is_primitive()
            || matches!(
                key.data_type(),
                DataType::Boolean
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Utf8View
                    | DataType::Binary
                    | DataType::LargeBinary
                    | DataType::BinaryView
                    | DataType::FixedSizeBinary(_)
            );
        if !flat {
            return None;
        }
        let values = key.to_data().get_slice_memory_size().ok()?;
        let padding = usize::try_from(KEY_VALUE).ok()?.saturating_mul(rows);
        bytes = bytes.saturating_add(padding.saturating_add(values.saturating_mul(2)));
    }
    Some(bytes)
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

/// Which of the rows `kept` keeps, where the schema policy dropped some, descend from a root row
/// `staged` sets, by the positions of their roots `roots` gives: none where every such row does.
fn descending(
    staged: &BooleanBuffer,
    roots: &ArrayRef,
    kept: Option<&BooleanArray>,
) -> Option<BooleanArray> {
    let roots = roots.as_primitive::<UInt32Type>().values();
    let root = |row: usize| staged.value(at(roots[row]));
    let descending: BooleanBuffer = match kept {
        Some(kept) => kept.values().set_indices().map(root).collect(),
        None => (0..roots.len()).map(root).collect(),
    };
    kept_or_none(descending)
}

/// `row`, a position among a batch's rows, as an index.
fn at(row: u32) -> usize {
    usize::try_from(row).expect("a 32-bit position fits in an index")
}

impl LoweringPlan {
    /// Cuts `columns`, a batch's model columns as its table stores them, `kept`, which of the
    /// rows received they are where the schema policy dropped some, and `changes`, their change
    /// rows, to the rows the table's merges keep: where `lineage` says which of a normalized
    /// merge's root rows its flush stages, the rows descending from those, in every table of the
    /// stream.
    ///
    /// Each column is let go as its rows are copied, so columns nothing else holds are not held
    /// twice over. A column of nulls is sliced, not copied: the columns of a type the batch holds
    /// nothing in share one array, and keep sharing it.
    pub(super) fn merged(
        &self,
        columns: &mut [ArrayRef],
        kept: &mut Option<BooleanArray>,
        changes: &mut Option<ChangeRows>,
        lineage: Option<&Lineage>,
    ) -> Result<(), ArrowError> {
        let staged = lineage.and_then(|lineage| Some((lineage.last.as_ref()?, lineage.root_row())));
        let compaction = match staged {
            Some((staged, roots)) => descending(staged, roots, kept.as_ref()),
            None => self.compaction(columns, changes.as_ref())?,
        };
        let Some(keep) = compaction else {
            return Ok(());
        };
        let predicate = FilterBuilder::new(&keep).optimize().build();
        let rows = keep.true_count();
        let mut nulls: BTreeMap<DataType, ArrayRef> = BTreeMap::new();
        for (column, source) in columns.iter_mut().zip(&self.sources) {
            *column = if source.is_null() {
                // Any rows of a column of nulls hold the same nulls as its first rows.
                let sliced = nulls
                    .entry(column.data_type().clone())
                    .or_insert_with(|| column.slice(0, rows));
                Arc::clone(sliced)
            } else {
                predicate.filter(column.as_ref())?
            };
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
