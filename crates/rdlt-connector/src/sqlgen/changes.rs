//! A change stream's tables beside its target: the staging columns that direct its merge, and the
//! tombstones of the rows it removed outright.
//!
//! A tombstone is a key a hard delete removed, with the delete's sequence; the latest hard
//! truncate's sequence is a row naming no key, the bound. A change sequenced at or before its
//! key's tombstone, or before the bound, never applies, so a change sent again after a delete or
//! truncate committed never brings a row back.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use arrow_array::builder::BinaryBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::Int8Type;
use arrow_array::{Array, ArrayRef, BinaryArray, Int8Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

use super::publish::keyless;
use super::{Column, Owned, SqlDialect, SqlPlanner, Statement};
use crate::change::{ChangeOp, UnchangedFlags};
use crate::destination::{ChangeColumns, Deletion, MergeKey, TableRef};
use crate::error::{ConnectorError, Result};

impl<D: SqlDialect> SqlPlanner<D> {
    /// The table holding the tombstones of the table `name`.
    pub fn tombstone_table(&self, name: &str) -> String {
        self.fitted(format!("_rdlt_tombstones__{name}"))
    }

    /// The statement forgetting the tombstones of the table `owned` names, whose rows were
    /// replaced whole.
    ///
    /// # Errors
    ///
    /// The witness is of a table yet to be created.
    pub fn forget_tombstones(&self, owned: &Owned<'_>) -> Result<Statement> {
        owned.is(owned.name())?;
        let tombstones = self.tombstone_table(owned.name());
        Ok(Statement {
            sql: format!("DELETE FROM {}", self.quote(&tombstones)),
            params: Vec::new(),
        })
    }

    /// The statements readying a change stream's tables before it stages rows for `table`, which
    /// `owned` names, given the columns its target, staging and tombstones tables have now: the
    /// staging table gains the op and unchanged columns, and the tombstones table is created with
    /// the target's key and sequence columns; nothing for a table that merges no change stream.
    ///
    /// They run where the stream stages its rows, so a commit changes no table's columns;
    /// [`SqlPlanner::key_indexes`] then indexes the three by the key.
    pub fn change_tables(
        &self,
        owned: &Owned<'_>,
        table: &TableRef,
        [target, staging, tombstones]: [&[Column]; 3],
    ) -> Result<Vec<Statement>> {
        owned.is(&table.name)?;
        let Some((key, changes)) = changed(table) else {
            return Ok(Vec::new());
        };
        let mut plan = Vec::new();
        let missing = |name: &str| !staging.iter().any(|column| column.name == name);
        let staging_name = self.quote(&self.staging_table(&table.name));
        let mut add = |name: &str, declared: &str| {
            plan.push(Statement {
                sql: format!(
                    "ALTER TABLE {staging_name} ADD COLUMN {} {declared}",
                    self.quote(name)
                ),
                params: Vec::new(),
            });
        };
        if missing(&changes.op) {
            add(&changes.op, &self.integer);
        }
        if let Some(unchanged) = changes.unchanged.as_deref().filter(|name| missing(name)) {
            add(unchanged, &self.blob);
        }
        if tombstones.is_empty() {
            let columns = key
                .columns
                .iter()
                .chain([&key.seq])
                .map(|name| {
                    let column = target
                        .iter()
                        .find(|column| *column.name == **name)
                        .ok_or_else(|| {
                            ConnectorError::data(format!(
                                "table {} has no column {name} to merge changes by",
                                table.name
                            ))
                        })?;
                    let declared = self.rendered(&table.name, column)?;
                    Ok(format!("{} {declared}", self.quote(name)))
                })
                .collect::<Result<Vec<_>>>()?;
            plan.push(Statement {
                sql: format!(
                    "CREATE TABLE IF NOT EXISTS {} ({})",
                    self.quote(&self.tombstone_table(&table.name)),
                    columns.join(", ")
                ),
                params: Vec::new(),
            });
        }
        Ok(plan)
    }

    /// The statements indexing `table`, which `owned` names, a merge table, and its staging by
    /// its key's columns where they are not, and its tombstones too where it merges a change
    /// stream; nothing for a table that does not merge, or a child table, which
    /// [`SqlPlanner::root_index`] indexes.
    ///
    /// They run where the table's rows are staged, so a commit finds each staged key's rows by
    /// them and changes no index.
    pub fn key_indexes(&self, owned: &Owned<'_>, table: &TableRef) -> Result<Vec<Statement>> {
        owned.is(&table.name)?;
        let Some(key) = table.merge.as_ref().filter(|key| key.root.is_none()) else {
            return Ok(Vec::new());
        };
        if key.columns.is_empty() {
            return Err(keyless(&table.name));
        }
        let columns: Vec<String> = key
            .columns
            .iter()
            .map(|column| self.quote(column))
            .collect();
        let columns = columns.join(", ");
        let mut tables = vec![self.target(table), self.staging_table(&table.name)];
        if key.changes.is_some() {
            tables.push(self.tombstone_table(&table.name));
        }
        Ok(tables
            .into_iter()
            .map(|indexed| Statement {
                sql: self.dialect.create_index(
                    &self.quote(&self.key_index_name(&indexed)),
                    &self.quote(&indexed),
                    &columns,
                ),
                params: Vec::new(),
            })
            .collect())
    }

    /// The name of the index of the table `table` by its key.
    pub(super) fn key_index_name(&self, table: &str) -> String {
        self.fitted(format!("_rdlt_key__{table}"))
    }
}

/// The merge key and change columns of `table`, where it merges a change stream.
fn changed(table: &TableRef) -> Option<(&MergeKey, &ChangeColumns)> {
    let key = table.merge.as_ref()?;
    Some((key, key.changes.as_ref()?))
}

/// `batch`, written for `table`, as its staging table holds it: where it merges a change stream,
/// each row's unchanged flags, a bitmap over the batch's fields, become bytes over the `target`
/// columns, the byte at a column's ordinal 1 where the row flags it, or null for none: what a
/// commit reads a column's flag from at one position, whatever the table's width.
///
/// Each refusal is a `Data` error under its code, raised before any row is staged: a row
/// without an op a change stream has (`op_invalid`) or without a sequence (`sequence_missing`);
/// flags that are no bitmap (`flags_invalid`), or naming a column the target lacks
/// (`flag_on_missing_column`) or a key or sequence column, which a change always sets
/// (`flag_on_key`); and for a history table whose deletes are soft, a delete or a truncate that
/// says no deletion time (`deletion_untimed`).
pub fn staged_changes(
    batch: &RecordBatch,
    table: &TableRef,
    target: &[Column],
) -> Result<RecordBatch> {
    let Some((key, changes)) = changed(table) else {
        return Ok(batch.clone());
    };
    let ops = ops(batch, changes)?;
    sequenced(batch, key)?;
    timed(batch, key, changes, ops)?;
    let Some((index, _)) = changes
        .unchanged
        .as_deref()
        .and_then(|name| batch.schema_ref().column_with_name(name))
    else {
        return Ok(batch.clone());
    };
    let flags = batch.column(index).as_binary_opt::<i32>().ok_or_else(|| {
        ConnectorError::data("a change stream's unchanged flags are not a bitmap of bytes")
            .with_code("flags_invalid")
    })?;
    let schema = batch.schema();
    // Where each of the batch's fields is among the table's columns, worked out once for every
    // row: its ordinal there, or why a row may not flag it.
    let ordinals: Vec<std::result::Result<usize, Unflaggable>> = schema
        .fields()
        .iter()
        .map(|field| ordinal_of(field.name(), key, target))
        .collect();
    let staged = flag_bytes(flags, &ordinals, &schema)?;
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields[index] = Field::new(schema.field(index).name(), DataType::Binary, true);
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns[index] = Arc::new(staged);
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| ConnectorError::internal(format!("restaging unchanged flags: {error}")))
}

/// Each row's `flags`, a bitmap over the fields of `schema`, as bytes over the table's
/// columns: the byte at a column's ordinal 1 where the row flags the field `ordinals` places
/// there, null where it flags none; a flag on a field a row may not flag is refused.
fn flag_bytes(
    flags: &BinaryArray,
    ordinals: &[std::result::Result<usize, Unflaggable>],
    schema: &Schema,
) -> Result<BinaryArray> {
    let mut staged = BinaryBuilder::new();
    let mut bytes: Vec<u8> = Vec::new();
    for row in 0..flags.len() {
        bytes.clear();
        if flags.is_valid(row) {
            for ordinal in UnchangedFlags::new(flags.value(row)).ordinals_below(ordinals.len()) {
                match &ordinals[ordinal] {
                    Ok(position) => {
                        if bytes.len() <= *position {
                            bytes.resize(position + 1, 0);
                        }
                        bytes[*position] = 1;
                    }
                    Err(why) => return Err(why.refused(schema.field(ordinal).name())),
                }
            }
        }
        if bytes.is_empty() {
            staged.append_null();
        } else {
            staged.append_value(&bytes);
        }
    }
    Ok(staged.finish())
}

/// The ops of `batch`'s rows, refused unless each row holds a change stream's op in the column
/// `changes` names: the codes past them are those a commit computes its rows under.
fn ops<'a>(batch: &'a RecordBatch, changes: &ChangeColumns) -> Result<&'a Int8Array> {
    let invalid = |message: String| ConnectorError::data(message).with_code("op_invalid");
    let ops = batch
        .column_by_name(&changes.op)
        .and_then(|ops| ops.as_primitive_opt::<Int8Type>())
        .ok_or_else(|| invalid("a change stream's batch has no op column of bytes".to_owned()))?;
    match ops
        .iter()
        .find(|op| op.and_then(ChangeOp::from_code).is_none())
    {
        Some(op) => Err(invalid(format!("{op:?} is no change stream's op"))),
        None => Ok(ops),
    }
}

/// Refuses `batch` unless each of its rows has a sequence, in the column `key` names.
fn sequenced(batch: &RecordBatch, key: &MergeKey) -> Result<()> {
    match batch.column_by_name(&key.seq) {
        Some(seqs) if seqs.null_count() == 0 => Ok(()),
        _ => Err(ConnectorError::data("a change has no sequence").with_code("sequence_missing")),
    }
}

/// Refuses `batch`, written for a history table whose deletes are soft, where a delete or a
/// truncate says no deletion time: when its version was deleted is what tells it from a live
/// one.
fn timed(
    batch: &RecordBatch,
    key: &MergeKey,
    changes: &ChangeColumns,
    ops: &Int8Array,
) -> Result<()> {
    let (Some(_), Deletion::Soft { at }) = (&key.history, &changes.deletion) else {
        return Ok(());
    };
    let times = batch.column_by_name(at);
    let removes = [ChangeOp::Delete.code(), ChangeOp::Truncate.code()];
    let untimed = ops.iter().enumerate().any(|(row, op)| {
        op.is_some_and(|op| removes.contains(&op)) && times.is_none_or(|times| times.is_null(row))
    });
    if untimed {
        let message = "a delete or a truncate of a history table says no deletion time";
        return Err(ConnectorError::data(message).with_code("deletion_untimed"));
    }
    Ok(())
}

/// Why a row may not flag a field unchanged.
#[derive(Clone, Copy)]
enum Unflaggable {
    /// It is a key column or the sequence, which a change always sets.
    SetAlways,
    /// The table has no such column.
    Missing,
}

impl Unflaggable {
    /// The `Data` error for a row flagging the field `name`.
    fn refused(self, name: &str) -> ConnectorError {
        match self {
            Self::SetAlways => ConnectorError::data(format!(
                "a change flags its key or sequence column {name} unchanged"
            ))
            .with_code("flag_on_key"),
            Self::Missing => ConnectorError::data(format!(
                "a change flags column {name} unchanged, which the table lacks"
            ))
            .with_code("flag_on_missing_column"),
        }
    }
}

/// The ordinal of the `target` column `name`, which a row may flag unchanged.
fn ordinal_of(
    name: &str,
    key: &MergeKey,
    target: &[Column],
) -> std::result::Result<usize, Unflaggable> {
    if *key.seq == *name || key.columns.iter().any(|column| **column == *name) {
        return Err(Unflaggable::SetAlways);
    }
    target
        .iter()
        .position(|column| column.name == name)
        .ok_or(Unflaggable::Missing)
}
