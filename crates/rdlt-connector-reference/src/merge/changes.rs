//! Merging a change stream's rows: each row applies in sequence order, only when its sequence is
//! greater than the published row's, as an insert, update, delete or truncate.
//!
//! The work is in proportion to the rows given: a row no change touches is one reference, a
//! truncate is recorded and met by each row once, and a row's unchanged flags are read through a
//! map made once for its batch.

mod table;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int8Type;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_row::Rows;
use arrow_schema::{ArrowError, DataType, Schema, SchemaRef};
use rdlt_connector::{ChangeColumns, ChangeOp, Deletion, MergeKey};

use super::aligned::{Nulls, aligned, concat};
use super::retype::retyped;
use super::tombstones::{self, Tombstones};
use super::{binary, converter, held, key_columns};
use table::{Applied, At, Change, Columns, Table};

/// The schema a change stream's table stores: `schema` without the columns `changes` names that
/// only written batches carry.
pub(crate) fn stored(schema: &SchemaRef, changes: &ChangeColumns) -> SchemaRef {
    let directive = |name: &str| {
        name == &*changes.op
            || changes
                .unchanged
                .as_deref()
                .is_some_and(|flags| flags == name)
    };
    let fields: Vec<_> = schema
        .fields()
        .iter()
        .filter(|field| !directive(field.name()))
        .cloned()
        .collect();
    Arc::new(Schema::new(fields))
}

/// The published rows of a change stream's table, and its tombstones, once `incoming` applies,
/// row by row in sequence order, to `published` and `buried`, its tombstones.
pub(crate) fn merge_changes(
    schema: &SchemaRef,
    published: &[RecordBatch],
    buried: &[RecordBatch],
    incoming: &[RecordBatch],
    key: &MergeKey,
    changes: &ChangeColumns,
) -> Result<(Vec<RecordBatch>, Vec<RecordBatch>), ArrowError> {
    let schema = stored(schema, changes);
    let converter = converter(&schema, key)?;
    let mut nulls = Nulls::default();
    let published = concat(published, &schema, &mut nulls)?;
    let nullable = nullable(&schema);
    let aligned = incoming
        .iter()
        .map(|batch| aligned(batch, &nullable, &mut nulls))
        .collect::<Result<Vec<_>, _>>()?;
    let keys = aligned
        .iter()
        .map(|batch| converter.convert_columns(&key_columns(batch, key)?))
        .collect::<Result<Vec<Rows>, _>>()?;
    let flags = incoming
        .iter()
        .map(|batch| Flags::of(batch, &schema, key, changes))
        .collect::<Result<Vec<_>, _>>()?;
    let mut rows = changed_rows(incoming, &aligned, key, changes)?;
    // A stable sort keeps rows of one sequence in the order they were written.
    rows.sort_by(|left, right| left.seq.cmp(&right.seq));
    let columns = Columns {
        seq: schema.index_of(&key.seq)?,
        at: match &changes.deletion {
            Deletion::Hard => None,
            Deletion::Soft { at } => Some(schema.index_of(at)?),
        },
        count: schema.fields().len(),
    };
    let hard = columns.at.is_none();
    let mut table = Table::load(&published, &aligned, &converter, key, columns)?;
    let tombstone_schema = tombstones::schema(&schema, key)?;
    let mut tombstones = Tombstones::load(buried, &tombstone_schema, &converter, key)?;
    for change in rows {
        if change.op == ChangeOp::Truncate {
            if tombstones.admits(None, &change.seq) {
                table.truncate(&change);
                if hard {
                    tombstones.raise(change.seq);
                }
            }
            continue;
        }
        let At { source, row } = change.at;
        let row_key = keys[source - 1].row(row).as_ref().to_vec();
        if !tombstones.admits(Some(&row_key), &change.seq) {
            continue;
        }
        let unchanged = match &flags[source - 1] {
            Some(flags) => flags.mask(row)?,
            None => None,
        };
        let seq = change.seq.clone();
        match table.apply(row_key.clone(), change, unchanged.as_deref()) {
            Applied::Removed => tombstones.bury(row_key, seq, source - 1, row),
            Applied::Held => tombstones.lift(&row_key),
            Applied::Nothing => {}
        }
    }
    let merged = table.assemble(&schema, &mut nulls)?;
    let buried = tombstones.assemble(&tombstone_schema, &aligned, key)?;
    Ok((held([merged]), buried))
}

/// `schema` with every column nullable, as incoming rows align to it: a truncate names no key.
pub(super) fn nullable(schema: &SchemaRef) -> SchemaRef {
    Arc::new(Schema::new(
        schema
            .fields()
            .iter()
            .map(|field| field.as_ref().clone().with_nullable(true))
            .collect::<Vec<_>>(),
    ))
}

/// The op of every row of `batch`, a written batch, as the column `changes` names holds it.
pub(super) fn ops(
    batch: &RecordBatch,
    changes: &ChangeColumns,
) -> Result<Vec<ChangeOp>, ArrowError> {
    let ops = batch
        .column_by_name(&changes.op)
        .ok_or_else(|| ArrowError::SchemaError(format!("no op column {}", changes.op)))?;
    let ops = retyped(ops, &DataType::Int8)?;
    ops.as_primitive::<Int8Type>()
        .iter()
        .map(|op| {
            op.and_then(ChangeOp::from_code)
                .ok_or_else(|| ArrowError::InvalidArgumentError(format!("{op:?} is no op")))
        })
        .collect()
}

/// Every row of `incoming`, with its op and sequence.
fn changed_rows(
    incoming: &[RecordBatch],
    aligned: &[RecordBatch],
    key: &MergeKey,
    changes: &ChangeColumns,
) -> Result<Vec<Change>, ArrowError> {
    let mut rows = Vec::new();
    for (index, (raw, batch)) in incoming.iter().zip(aligned).enumerate() {
        let seqs = binary(batch, &key.seq)?;
        let seqs = seqs.as_binary::<i32>();
        for (row, op) in ops(raw, changes)?.into_iter().enumerate() {
            if seqs.is_null(row) {
                return Err(ArrowError::InvalidArgumentError(
                    "a change has no sequence".to_owned(),
                ));
            }
            rows.push(Change {
                at: At {
                    source: index + 1,
                    row,
                },
                op,
                seq: seqs.value(row).to_vec(),
            });
        }
    }
    Ok(rows)
}

/// The unchanged flags of one written batch: each row's bitmap over the batch's fields, and the
/// stored column each field is, worked out once for the batch.
struct Flags {
    bitmaps: ArrayRef,
    /// The stored column of each of the batch's fields, none for a field the table does not
    /// store.
    stored: Vec<Option<usize>>,
    /// Whether each stored column is a key column or the sequence, which every change sets.
    set_always: Vec<bool>,
}

impl Flags {
    /// The flags of `raw`, a written batch of a table of `schema` merged by `key`, where its rows
    /// carry any.
    fn of(
        raw: &RecordBatch,
        schema: &SchemaRef,
        key: &MergeKey,
        changes: &ChangeColumns,
    ) -> Result<Option<Self>, ArrowError> {
        let Some(bitmaps) = changes
            .unchanged
            .as_deref()
            .and_then(|column| raw.column_by_name(column))
        else {
            return Ok(None);
        };
        let stored = raw
            .schema_ref()
            .fields()
            .iter()
            .map(|field| schema.index_of(field.name()).ok())
            .collect();
        let set_always = schema
            .fields()
            .iter()
            .map(|field| {
                let name = field.name().as_str();
                *key.seq == *name || key.columns.iter().any(|column| **column == *name)
            })
            .collect();
        Ok(Some(Self {
            bitmaps: retyped(bitmaps, &DataType::Binary)?,
            stored,
            set_always,
        }))
    }

    /// Which stored columns row `row` flags unchanged; none where it flags none.
    ///
    /// A flag on a key column or the sequence, which a change always sets, is an error.
    fn mask(&self, row: usize) -> Result<Option<Vec<bool>>, ArrowError> {
        let bitmaps = self.bitmaps.as_binary::<i32>();
        if bitmaps.is_null(row) {
            return Ok(None);
        }
        let bitmap = bitmaps.value(row);
        let mut mask = vec![false; self.set_always.len()];
        let mut flagged = false;
        for (ordinal, stored) in self.stored.iter().enumerate() {
            let set = bitmap
                .get(ordinal / 8)
                .is_some_and(|byte| byte & (1 << (ordinal % 8)) != 0);
            let (true, Some(column)) = (set, stored) else {
                continue;
            };
            if self.set_always[*column] {
                return Err(ArrowError::InvalidArgumentError(
                    "a change flags its key or sequence column unchanged".to_owned(),
                ));
            }
            mask[*column] = true;
            flagged = true;
        }
        Ok(flagged.then_some(mask))
    }
}
