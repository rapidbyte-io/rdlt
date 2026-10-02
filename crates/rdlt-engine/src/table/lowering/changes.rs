//! The parts of a lowering plan only change streams use: their op, sequence and unchanged
//! columns, split from a pushed batch and carried to its table, and their compaction.

#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use arrow_array::builder::BinaryBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{Int8Type, TimestampMicrosecondType};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int8Array, PrimitiveArray, RecordBatch, UInt32Array,
};
use arrow_row::{RowConverter, SortField};
use arrow_schema::{ArrowError, DataType};
use rdlt_connector::{ChangeOp, LogicalType, OP_COLUMN, SEQ_COLUMN, UNCHANGED_COLUMN};

use super::merge::compact;
use super::{LoweringPlan, Source, Stamp, lower_array};
use crate::table::lower::loaded_at_type;

/// The change columns of a change stream's batch, split from its data.
#[derive(Clone, Debug)]
pub(crate) struct ChangeRows {
    /// Each row's op code.
    pub(crate) op: Int8Array,
    /// Each row's source position, 16 bytes of `Binary`.
    pub(crate) seq: BinaryArray,
    /// Each row's unchanged flags, a bitmap over the data batch's field ordinals, where the
    /// batch flags any.
    pub(crate) unchanged: Option<BinaryArray>,
}

/// For each field of `batch`, a change batch, its place among the data's fields; none for a
/// change column.
pub(crate) fn data_ordinals(batch: &RecordBatch) -> Vec<Option<usize>> {
    let mut data = 0;
    let schema = batch.schema();
    let fields = schema.fields().iter();
    fields
        .map(|field| {
            let change = matches!(
                field.name().as_str(),
                OP_COLUMN | SEQ_COLUMN | UNCHANGED_COLUMN
            );
            (!change).then(|| {
                data += 1;
                data - 1
            })
        })
        .collect()
}

impl ChangeRows {
    /// `batch`, a valid change batch, as its data and its change columns: the flags of
    /// `_rdlt_unchanged` move from the batch's field ordinals to the data's.
    pub(crate) fn split(batch: &RecordBatch) -> Result<(RecordBatch, Self), ArrowError> {
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .ok_or_else(|| ArrowError::SchemaError(format!("no {name} column")))
        };
        let op = column(OP_COLUMN)?.as_primitive::<Int8Type>().clone();
        let seq = arrow_cast::cast(column(SEQ_COLUMN)?, &DataType::Binary)?;
        let seq = seq.as_binary::<i32>().clone();
        let data_ordinal = data_ordinals(batch);
        let unchanged = batch
            .column_by_name(UNCHANGED_COLUMN)
            .map(|flags| remap(flags.as_binary::<i32>(), &data_ordinal));
        let data = Self::data(batch)?;
        Ok((data, Self { op, seq, unchanged }))
    }

    /// The data columns of `batch`, a change batch, as they are: none is copied.
    pub(crate) fn data(batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
        let ordinals = data_ordinals(batch);
        let kept: Vec<usize> = (0..ordinals.len())
            .filter(|ordinal| ordinals[*ordinal].is_some())
            .collect();
        batch.project(&kept)
    }

    /// The rows `keep` keeps.
    pub(crate) fn filter(&self, keep: &BooleanArray) -> Result<Self, ArrowError> {
        let filter = |array: &dyn Array| arrow_select::filter::filter(array, keep);
        Ok(Self {
            op: filter(&self.op)?.as_primitive::<Int8Type>().clone(),
            seq: filter(&self.seq)?.as_binary::<i32>().clone(),
            unchanged: match &self.unchanged {
                Some(flags) => Some(filter(flags)?.as_binary::<i32>().clone()),
                None => None,
            },
        })
    }

    /// The op of row `row`.
    pub(crate) fn op(&self, row: usize) -> Option<ChangeOp> {
        ChangeOp::from_code(self.op.value(row))
    }

    /// The data fields, by ordinal, some row flags unchanged.
    pub(crate) fn flagged(&self) -> Vec<usize> {
        let Some(flags) = &self.unchanged else {
            return Vec::new();
        };
        let mut flagged = BTreeSet::new();
        for bitmap in flags.iter().flatten() {
            for (byte, bits) in bitmap.iter().enumerate() {
                for bit in 0..8 {
                    if bits & (1 << bit) != 0 {
                        flagged.insert(byte * 8 + bit);
                    }
                }
            }
        }
        flagged.into_iter().collect()
    }

    /// The unchanged flags over a table's written fields, where data field `i` is written as
    /// field `written[i]`, if it is written.
    pub(crate) fn unchanged_over(&self, written: &[Option<usize>]) -> ArrayRef {
        let rows = self.op.len();
        match &self.unchanged {
            Some(flags) => Arc::new(remap(flags, written)),
            None => Arc::new(BinaryArray::new_null(rows)),
        }
    }

    /// When each row was deleted: `at` for deletes and truncates, null for other rows.
    pub(crate) fn deleted_at(&self, at: i64) -> ArrayRef {
        let deleted: PrimitiveArray<TimestampMicrosecondType> = (0..self.op.len())
            .map(|row| {
                matches!(self.op(row), Some(ChangeOp::Delete | ChangeOp::Truncate)).then_some(at)
            })
            .collect();
        Arc::new(deleted.with_timezone("UTC"))
    }

    /// Whether row `row` truncates.
    pub(crate) fn truncates(&self, row: usize) -> bool {
        self.op(row) == Some(ChangeOp::Truncate)
    }
}

/// `flags`, bitmaps over some fields' ordinals, over other ordinals: field `i` becomes field
/// `to[i]`, or is dropped where that is `None`.
fn remap(flags: &BinaryArray, to: &[Option<usize>]) -> BinaryArray {
    let mut remapped = BinaryBuilder::with_capacity(flags.len(), flags.len());
    for bitmap in flags {
        let Some(bitmap) = bitmap else {
            remapped.append_null();
            continue;
        };
        let mut out: Vec<u8> = Vec::new();
        for (ordinal, target) in to.iter().enumerate() {
            let set = bitmap
                .get(ordinal / 8)
                .is_some_and(|byte| byte & (1 << (ordinal % 8)) != 0);
            if let (true, Some(target)) = (set, target) {
                out.resize(out.len().max(target / 8 + 1), 0);
                out[target / 8] |= 1 << (target % 8);
            }
        }
        remapped.append_value(out);
    }
    remapped.finish()
}

/// `batch`, a change stream's merge batch, without the rows a later row of their key supersedes.
///
/// An insert or update that flags nothing unchanged supersedes, and, where deletes remove rows
/// outright, a delete. A batch holding a truncate keeps every row, since rows before it must meet
/// it.
///
/// `key` are the key columns' positions; `op`, `seq` and `unchanged` the change columns'.
pub(crate) fn compact_changes(
    batch: &RecordBatch,
    key: &[usize],
    [op, seq, unchanged]: [usize; 3],
    hard: bool,
) -> Result<RecordBatch, ArrowError> {
    let ops = batch.column(op).as_primitive::<Int8Type>();
    let truncated = (0..batch.num_rows())
        .any(|row| ChangeOp::from_code(ops.value(row)) == Some(ChangeOp::Truncate));
    if truncated || batch.num_rows() < 2 {
        return Ok(batch.clone());
    }
    let seqs = arrow_cast::cast(batch.column(seq), &DataType::Binary)?;
    let seqs = seqs.as_binary::<i32>();
    let flags = batch.column(unchanged).as_binary::<i32>();
    let columns: Vec<ArrayRef> = key
        .iter()
        .map(|index| Arc::clone(batch.column(*index)))
        .collect();
    let fields = columns
        .iter()
        .map(|column| SortField::new(column.data_type().clone()))
        .collect();
    let keys = RowConverter::new(fields)?.convert_columns(&columns)?;
    let supersedes = |row: usize| match ChangeOp::from_code(ops.value(row)) {
        Some(ChangeOp::Insert | ChangeOp::Update) => {
            flags.is_null(row) || flags.value(row).iter().all(|byte| *byte == 0)
        }
        Some(ChangeOp::Delete) => hard,
        _ => false,
    };
    // The greatest sequence of each key's superseding rows.
    let mut latest: BTreeMap<&[u8], &[u8]> = BTreeMap::new();
    for row in (0..batch.num_rows()).filter(|row| supersedes(*row)) {
        let seq = seqs.value(row);
        let entry = latest.entry(keys.row(row).data()).or_insert(seq);
        *entry = (*entry).max(seq);
    }
    let kept: Vec<u32> = (0..batch.num_rows())
        .filter(|row| {
            latest
                .get(keys.row(*row).data())
                .is_none_or(|latest| seqs.value(*row) >= *latest)
        })
        .map(|row| u32::try_from(row).unwrap_or(u32::MAX))
        .collect();
    if kept.len() == batch.num_rows() {
        return Ok(batch.clone());
    }
    arrow_select::take::take_record_batch(batch, &UInt32Array::from(kept))
}

impl LoweringPlan {
    /// A change stream's sequence column, the source's positions, lowered as the table's column
    /// at `index` stores it.
    pub(super) fn source_sequence(
        &self,
        changes: &ChangeRows,
        index: usize,
    ) -> Result<ArrayRef, ArrowError> {
        let seq: ArrayRef = Arc::new(changes.seq.clone());
        let lowered = self.view.physical[index].logical_type();
        lower_array(&seq, &LogicalType::Binary, lowered)
    }

    /// Pushes to `columns` the change columns a change stream's table stores: a log's op and
    /// unchanged flags, and a soft-deleting table's deletion time, each lowered.
    pub(super) fn stored_changes(
        &self,
        changes: &ChangeRows,
        stamp: &Stamp,
        columns: &mut Vec<ArrayRef>,
    ) -> Result<(), ArrowError> {
        let Some(names) = &self.view.meta.changes else {
            return Ok(());
        };
        let mut stored: Vec<(ArrayRef, LogicalType)> = Vec::new();
        if names.stored {
            stored.push((Arc::new(changes.op.clone()), LogicalType::Int8));
            let written = self.written_ordinals();
            stored.push((changes.unchanged_over(&written), LogicalType::Binary));
        }
        if names.deleted_at.is_some() {
            stored.push((changes.deleted_at(micros(stamp)), loaded_at_type()));
        }
        for (array, logical) in stored {
            let lowered = self.view.physical[columns.len()].logical_type();
            columns.push(lower_array(&array, &logical, lowered)?);
        }
        Ok(())
    }

    /// `prepared` compacted as its table's merges keep rows: the last row of each key, or for a
    /// change stream's, the rows no later row of their key supersedes.
    pub(super) fn compacted(&self, prepared: RecordBatch) -> Result<RecordBatch, ArrowError> {
        let view = &self.view;
        match (view.compacts(), &view.meta.changes) {
            (false, _) => Ok(prepared),
            (true, None) => compact(&prepared, &view.key),
            (true, Some(names)) => {
                let schema = prepared.schema();
                let seq = view.meta.seq.as_deref().unwrap_or_default();
                let columns = [
                    schema.index_of(&names.op)?,
                    schema.index_of(seq)?,
                    schema.index_of(&names.unchanged)?,
                ];
                compact_changes(&prepared, &view.key, columns, names.deleted_at.is_none())
            }
        }
    }

    /// Where each incoming column is written: the position of the table's column it goes to, if
    /// any, by the incoming column's ordinal.
    pub(super) fn written_ordinals(&self) -> Vec<Option<usize>> {
        let mut written = vec![None; self.incoming.schema.fields().len()];
        for (column, source) in self.sources.iter().enumerate() {
            if let Source::Incoming(index, _) = source {
                written[*index] = Some(column);
            }
        }
        written
    }
}

/// When the load `stamp` names started, in microseconds since the epoch.
fn micros(stamp: &Stamp) -> i64 {
    stamp
        .loaded_at
        .duration_since(UNIX_EPOCH)
        .map_or(0, |since| {
            i64::try_from(since.as_micros()).unwrap_or(i64::MAX)
        })
}
