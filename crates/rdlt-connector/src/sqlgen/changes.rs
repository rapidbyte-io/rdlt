//! A change stream's tables beside its target: the staging columns that direct its merge, and the
//! tombstones of the rows it removed outright.
//!
//! A tombstone is a key a hard delete removed, with the delete's sequence; the latest hard
//! truncate's sequence is a row naming no key, the bound. A change sequenced at or before its
//! key's tombstone, or before the bound, never applies, so a change sent again after a delete or
//! truncate committed never brings a row back.

#[cfg(test)]
mod tests;

use std::fmt::Write as _;
use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::Int8Type;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Field, Schema};

use super::{Column, SqlDialect, SqlPlanner, Statement};
use crate::change::ChangeOp;
use crate::destination::{ChangeColumns, MergeKey, TableRef};
use crate::error::{ConnectorError, Result};

impl<D: SqlDialect> SqlPlanner<D> {
    /// The table holding the tombstones of the table `name`.
    pub fn tombstone_table(&self, name: &str) -> String {
        self.fitted(format!("_rdlt_tombstones__{name}"))
    }

    /// The statement forgetting the tombstones of the table `name`, whose rows were replaced whole.
    pub fn forget_tombstones(&self, name: &str) -> Statement {
        Statement {
            sql: format!("DELETE FROM {}", self.quote(&self.tombstone_table(name))),
            params: Vec::new(),
        }
    }

    /// The statements readying a change stream's tables before it stages rows for `table`, given
    /// the columns its target, staging and tombstones tables have now: the staging table gains the
    /// op and unchanged columns, the tombstones table is created with the target's key and
    /// sequence columns, and each of the three is indexed by the key where it is not; nothing for
    /// a table that merges no change stream.
    ///
    /// They run where the stream stages its rows, so a commit changes no table's columns.
    pub fn change_tables(
        &self,
        table: &TableRef,
        [target, staging, tombstones]: [&[Column]; 3],
    ) -> Result<Vec<Statement>> {
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
            add(unchanged, &self.text);
        }
        if tombstones.is_empty() {
            let columns = key
                .columns
                .iter()
                .chain([&key.seq])
                .map(|name| {
                    let column = target.iter().find(|column| *column.name == **name);
                    column
                        .map(|column| format!("{} {}", self.quote(name), column.declared))
                        .ok_or_else(|| {
                            ConnectorError::data(format!(
                                "table {} has no column {name} to merge changes by",
                                table.name
                            ))
                        })
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
        plan.extend(self.key_indexes(&table.name, key));
        Ok(plan)
    }

    /// The statements indexing the table `name`, its staging and its tombstones by `key`'s
    /// columns, where they are not: a commit finds each changed key's rows by them.
    fn key_indexes(&self, name: &str, key: &MergeKey) -> Vec<Statement> {
        let columns: Vec<String> = key
            .columns
            .iter()
            .map(|column| self.quote(column))
            .collect();
        let columns = columns.join(", ");
        [
            name.to_owned(),
            self.staging_table(name),
            self.tombstone_table(name),
        ]
        .into_iter()
        .map(|table| Statement {
            sql: self.dialect.create_index(
                &self.quote(&self.key_index_name(&table)),
                &self.quote(&table),
                &columns,
            ),
            params: Vec::new(),
        })
        .collect()
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
/// each row's unchanged flags, a bitmap over the batch's fields, become the text `,i,j,` of the
/// ordinals of the `target` columns they name, or null for none.
///
/// A flag naming a column the target lacks, or one of its key or sequence columns, which a change
/// always sets, is a `Data` error.
pub fn staged_changes(
    batch: &RecordBatch,
    table: &TableRef,
    target: &[Column],
) -> Result<RecordBatch> {
    let Some((key, changes)) = changed(table) else {
        return Ok(batch.clone());
    };
    ops(batch, changes)?;
    let Some((index, _)) = changes
        .unchanged
        .as_deref()
        .and_then(|name| batch.schema_ref().column_with_name(name))
    else {
        return Ok(batch.clone());
    };
    let flags = batch.column(index).as_binary_opt::<i32>().ok_or_else(|| {
        ConnectorError::data("a change stream's unchanged flags are not a bitmap of bytes")
    })?;
    let schema = batch.schema();
    let mut texts = StringBuilder::new();
    for row in 0..batch.num_rows() {
        let bitmap = if flags.is_null(row) {
            &[][..]
        } else {
            flags.value(row)
        };
        let mut text = String::from(",");
        for (ordinal, field) in schema.fields().iter().enumerate() {
            let flagged = bitmap
                .get(ordinal / 8)
                .is_some_and(|byte| byte & (1 << (ordinal % 8)) != 0);
            if flagged {
                let name = field.name().as_str();
                let position = ordinal_of(name, key, target)?;
                write!(text, "{position},").expect("writing to a string succeeds");
            }
        }
        if text.len() > 1 {
            texts.append_value(text);
        } else {
            texts.append_null();
        }
    }
    let mut fields: Vec<Field> = schema.fields().iter().map(|f| f.as_ref().clone()).collect();
    fields[index] = Field::new(schema.field(index).name(), DataType::Utf8, true);
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    columns[index] = Arc::new(texts.finish());
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .map_err(|error| ConnectorError::internal(format!("restaging unchanged flags: {error}")))
}

/// Refuses `batch` unless each of its rows holds a change stream's op in the column `changes`
/// names: the codes past them are those a commit computes its rows under.
fn ops(batch: &RecordBatch, changes: &ChangeColumns) -> Result<()> {
    let ops = batch
        .column_by_name(&changes.op)
        .and_then(|ops| ops.as_primitive_opt::<Int8Type>())
        .ok_or_else(|| ConnectorError::data("a change stream's batch has no op column of bytes"))?;
    match ops
        .iter()
        .find(|op| op.and_then(ChangeOp::from_code).is_none())
    {
        Some(op) => Err(ConnectorError::data(format!(
            "{op:?} is no change stream's op"
        ))),
        None => Ok(()),
    }
}

/// The ordinal of the `target` column `name`, which a row flags unchanged.
fn ordinal_of(name: &str, key: &MergeKey, target: &[Column]) -> Result<usize> {
    if *key.seq == *name || key.columns.iter().any(|column| **column == *name) {
        return Err(ConnectorError::data(format!(
            "a change flags its key or sequence column {name} unchanged"
        )));
    }
    target
        .iter()
        .position(|column| column.name == name)
        .ok_or_else(|| {
            ConnectorError::data(format!(
                "a change flags column {name} unchanged, which the table lacks"
            ))
        })
}
