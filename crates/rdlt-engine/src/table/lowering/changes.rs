//! The parts of a lowering plan only change streams use: their op, sequence and unchanged
//! columns, split from a pushed batch and carried to its table, the flags remapped once, and
//! their compaction.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int8Type, TimestampMicrosecondType};
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int8Array, PrimitiveArray, RecordBatch,
};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder};
use arrow_schema::{ArrowError, DataType};
use arrow_select::filter::FilterBuilder;
use rdlt_connector::cost::Stored;
use rdlt_connector::{
    ChangeOp, LogicalType, OP_COLUMN, SEQ_COLUMN, UNCHANGED_COLUMN, UnchangedFlags, remap_unchanged,
};

use super::merge::{kept_or_none, sorted};
use super::{LoweringPlan, Source, lower_array};
use crate::table::lower::loaded_at_type;

/// The change columns of a change stream's batch, split from its data.
#[derive(Clone, Debug)]
pub(crate) struct ChangeRows {
    /// Each row's op code.
    pub(crate) op: Int8Array,
    /// Each row's source position, 16 bytes of `Binary`.
    pub(crate) seq: BinaryArray,
    /// Each row's unchanged flags as pushed, a bitmap over the pushed batch's field ordinals,
    /// where the batch flags any.
    pub(crate) unchanged: Option<BinaryArray>,
    /// For each field of the pushed batch, its place among the data's fields; none for a change
    /// column.
    pub(crate) ordinals: Arc<[Option<usize>]>,
}

/// For each field of `batch`, a change batch, its place among the data's fields; none for a
/// change column.
pub(crate) fn data_ordinals(batch: &RecordBatch) -> Vec<Option<usize>> {
    let mut data = 0;
    let schema = batch.schema();
    let fields = schema.fields().iter();
    fields
        .map(|field| {
            (!change_column(field.name())).then(|| {
                data += 1;
                data - 1
            })
        })
        .collect()
}

/// Where the change columns of `batch`, a change batch, sit among its fields.
pub(super) fn places(batch: &RecordBatch) -> Vec<usize> {
    let fields = batch.schema_ref().fields().iter().enumerate();
    let changes = fields.filter(|(_, field)| change_column(field.name()));
    changes.map(|(place, _)| place).collect()
}

/// Whether a field named `name` is a change column.
fn change_column(name: &str) -> bool {
    matches!(name, OP_COLUMN | SEQ_COLUMN | UNCHANGED_COLUMN)
}

/// `stored`, how a table stores each data column of `batch`, a change batch, by the batch's own
/// columns: nothing for its change columns.
pub(crate) fn aligned(batch: &RecordBatch, stored: &[Option<Stored>]) -> Vec<Option<Stored>> {
    data_ordinals(batch)
        .into_iter()
        .map(|data| data.and_then(|data| stored.get(data).cloned().flatten()))
        .collect()
}

impl ChangeRows {
    /// `batch`, a valid change batch, as its data and its change columns.
    pub(crate) fn split(batch: &RecordBatch) -> Result<(RecordBatch, Self), ArrowError> {
        let column = |name: &str| {
            batch
                .column_by_name(name)
                .ok_or_else(|| ArrowError::SchemaError(format!("no {name} column")))
        };
        let op = column(OP_COLUMN)?.as_primitive::<Int8Type>().clone();
        let seq = arrow_cast::cast(column(SEQ_COLUMN)?, &DataType::Binary)?;
        let seq = seq.as_binary::<i32>().clone();
        let unchanged = batch
            .column_by_name(UNCHANGED_COLUMN)
            .map(|flags| flags.as_binary::<i32>().clone());
        let ordinals: Arc<[Option<usize>]> = data_ordinals(batch).into();
        let data = projected(batch, &ordinals)?;
        Ok((
            data,
            Self {
                op,
                seq,
                unchanged,
                ordinals,
            },
        ))
    }

    /// The data columns of `batch`, a change batch, as they are: none is copied.
    pub(crate) fn data(batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
        projected(batch, &data_ordinals(batch))
    }

    /// The rows `keep` keeps, each column let go as its rows are copied.
    pub(crate) fn filter(self, keep: &BooleanArray) -> Result<Self, ArrowError> {
        let predicate = FilterBuilder::new(keep).optimize().build();
        let Self {
            op,
            seq,
            unchanged,
            ordinals,
        } = self;
        let taken = |array: ArrayRef| predicate.filter(array.as_ref());
        let op = taken(Arc::new(op))?.as_primitive::<Int8Type>().clone();
        let seq = taken(Arc::new(seq))?.as_binary::<i32>().clone();
        let unchanged = match unchanged {
            Some(flags) => Some(taken(Arc::new(flags))?.as_binary::<i32>().clone()),
            None => None,
        };
        Ok(Self {
            op,
            seq,
            unchanged,
            ordinals,
        })
    }

    /// The op of row `row`.
    pub(crate) fn op(&self, row: usize) -> Option<ChangeOp> {
        ChangeOp::from_code(self.op.value(row))
    }

    /// The data fields, by ordinal, some row flags unchanged, in ascending order.
    ///
    /// Only the bytes holding the pushed batch's fields are read, however long a bitmap is.
    pub(crate) fn flagged(&self) -> Vec<usize> {
        let Some(flags) = &self.unchanged else {
            return Vec::new();
        };
        let mut any = vec![0_u8; self.ordinals.len().div_ceil(8)];
        for bitmap in flags.iter().flatten() {
            any.iter_mut()
                .zip(bitmap)
                .for_each(|(any, byte)| *any |= byte);
        }
        UnchangedFlags::new(&any)
            .ordinals_below(self.ordinals.len())
            .filter_map(|pushed| self.ordinals[pushed])
            .collect()
    }

    /// The unchanged flags over a table's written fields, where data field `i` is written as
    /// field `written[i]`, if it is written: one remap from the pushed batch's ordinals.
    pub(crate) fn unchanged_over(&self, written: &[Option<usize>]) -> ArrayRef {
        let Some(flags) = &self.unchanged else {
            return Arc::new(BinaryArray::new_null(self.op.len()));
        };
        Arc::new(remap_unchanged(flags, &self.pushed_to(written)))
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

    /// The rows of a merge batch, whose key columns as its table stores them are `keys`, that no
    /// later row of their key supersedes; none where every row stays.
    ///
    /// An insert or update that flags none of the fields its table writes unchanged supersedes,
    /// where data field `i` is written as `written[i]`, and, where deletes remove rows outright
    /// (`hard`), a delete. A row stays where its sequence is at least the greatest of its key's
    /// superseding rows'. A batch holding a truncate keeps every row, since rows before it must
    /// meet it.
    pub(crate) fn unsuperseded(
        &self,
        keys: &[ArrayRef],
        written: &[Option<usize>],
        hard: bool,
    ) -> Result<Option<BooleanArray>, ArrowError> {
        let rows = self.op.len();
        if rows < 2 || (0..rows).any(|row| self.truncates(row)) {
            return Ok(None);
        }
        let to = self.pushed_to(written);
        let supersedes = BooleanBuffer::collect_bool(rows, |row| match self.op(row) {
            Some(ChangeOp::Insert | ChangeOp::Update) => self.flags_nothing(row, &to),
            Some(ChangeOp::Delete) => hard,
            _ => false,
        });
        // Where no row supersedes, every row stays, and the keys need no sort.
        if supersedes.count_set_bits() == 0 {
            return Ok(None);
        }
        let (keyed, order) = sorted(keys)?;
        let mut kept = BooleanBufferBuilder::new(rows);
        kept.append_n(rows, false);
        for run in order.chunk_by(|left, right| keyed.row(*left) == keyed.row(*right)) {
            let latest = run
                .iter()
                .filter(|row| supersedes.value(**row))
                .map(|row| self.seq.value(*row))
                .max();
            for row in run {
                if latest.is_none_or(|latest| self.seq.value(*row) >= latest) {
                    kept.set_bit(*row, true);
                }
            }
        }
        Ok(kept_or_none(kept.finish()))
    }

    /// For each field of the pushed batch, the table's field it is written as, where data field
    /// `i` is written as `written[i]`; none for a change column or a field not written.
    fn pushed_to(&self, written: &[Option<usize>]) -> Vec<Option<usize>> {
        self.ordinals
            .iter()
            .map(|data| data.and_then(|data| written.get(data).copied().flatten()))
            .collect()
    }

    /// Whether row `row` flags none of the fields `to` maps to a table's field unchanged.
    ///
    /// Only the bytes holding the pushed batch's fields are read, however long a bitmap is.
    fn flags_nothing(&self, row: usize, to: &[Option<usize>]) -> bool {
        let Some(flags) = &self.unchanged else {
            return true;
        };
        flags.is_null(row)
            || UnchangedFlags::new(flags.value(row))
                .ordinals_below(to.len())
                .all(|pushed| to[pushed].is_none())
    }
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

    /// Pushes to `columns` the change columns a change stream's table stores, each lowered: a
    /// log's op and unchanged flags, and a soft-deleting table's deletion time, `loaded_at`, the
    /// load's start in microseconds since the epoch.
    pub(super) fn stored_changes(
        &self,
        changes: &ChangeRows,
        loaded_at: i64,
        columns: &mut Vec<ArrayRef>,
    ) -> Result<(), ArrowError> {
        let Some(names) = &self.view.meta.changes else {
            return Ok(());
        };
        let mut stored: Vec<(ArrayRef, LogicalType)> = Vec::new();
        if names.stored {
            stored.push((Arc::new(changes.op.clone()), LogicalType::Int8));
            stored.push((changes.unchanged_over(&self.written), LogicalType::Binary));
        }
        if names.deleted_at.is_some() {
            stored.push((changes.deleted_at(loaded_at), loaded_at_type()));
        }
        for (array, logical) in stored {
            let lowered = self.view.physical[columns.len()].logical_type();
            columns.push(lower_array(&array, &logical, lowered)?);
        }
        Ok(())
    }
}

/// Where each of `fields` incoming columns is written, by its ordinal: the position of the
/// table's column it goes to, if any, where `sources` says where each column takes its values.
pub(super) fn written(sources: &[Source], fields: usize) -> Vec<Option<usize>> {
    let mut written = vec![None; fields];
    for (column, source) in sources.iter().enumerate() {
        if let Source::Incoming(index, _) = source {
            written[*index] = Some(column);
        }
    }
    written
}

/// The fields of `batch` that `ordinals` gives a data ordinal, as they are.
fn projected(batch: &RecordBatch, ordinals: &[Option<usize>]) -> Result<RecordBatch, ArrowError> {
    let kept: Vec<usize> = (0..ordinals.len())
        .filter(|ordinal| ordinals[*ordinal].is_some())
        .collect();
    batch.project(&kept)
}
