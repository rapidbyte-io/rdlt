//! Merging published rows by key, as the memory and files destinations publish a merge table.

mod aligned;
mod changes;
mod history;
mod retype;
#[cfg(test)]
mod tests;
mod tombstones;

pub(crate) use tombstones::schema as tombstone_schema;

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, BooleanArray, RecordBatch};
use arrow_row::{RowConverter, SortField};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use rdlt_connector::{ChangeColumns, ConnectorError, MergeKey, RootKey, TableRef};

use aligned::{Nulls, concat, filtered, taken};

/// Refuses a writer of `table` where it is a replace generation of a history table, which merges
/// into its table only.
pub(crate) fn refuse_history_generation(table: &TableRef) -> rdlt_connector::Result<()> {
    let history = table
        .merge
        .as_ref()
        .is_some_and(|key| key.history.is_some());
    if history && table.generation.is_some() {
        return Err(ConnectorError::internal(
            "a history table merges into its table, never a generation",
        ));
    }
    Ok(())
}

/// The schema a change stream's written batches have: `stored`, every column nullable since a
/// truncate names no key, then the columns `changes` directs the merge with.
pub(crate) fn written_schema(stored: &SchemaRef, changes: &ChangeColumns) -> SchemaRef {
    let mut fields: Vec<Field> = stored
        .fields()
        .iter()
        .map(|field| field.as_ref().clone().with_nullable(true))
        .collect();
    fields.push(Field::new(changes.op.as_ref(), DataType::Int8, false));
    if let Some(unchanged) = &changes.unchanged {
        fields.push(Field::new(unchanged.as_ref(), DataType::Binary, true));
    }
    Arc::new(Schema::new(fields))
}

/// The published rows once `incoming` is merged into `published` by `key`: for a history table,
/// each row versions its key in sequence order; for a change stream's table, each row applies in
/// sequence order as its op says; otherwise an incoming row replaces the published row with its
/// key, and among incoming rows of one key the greatest sequence wins.
///
/// A key `schema` cannot be merged by, one of no column or naming a column it lacks, is an error,
/// and so is a value a column's type no longer holds: nothing merges then.
pub(crate) fn merge(
    schema: &SchemaRef,
    published: &[RecordBatch],
    buried: &[RecordBatch],
    incoming: &[RecordBatch],
    key: &MergeKey,
) -> Result<Merged, ArrowError> {
    let (rows, tombstones) = match (&key.history, &key.changes) {
        (Some(history), _) => {
            history::merge_history(schema, published, buried, incoming, key, history)?
        }
        (None, Some(changes)) => {
            changes::merge_changes(schema, published, buried, incoming, key, changes)?
        }
        (None, None) => {
            return Ok(Merged {
                rows: upsert(schema, published, incoming, key)?,
                tombstones: Vec::new(),
            });
        }
    };
    Ok(Merged { rows, tombstones })
}

/// A merge table's rows once merged, and for a change stream's, the tombstones of the rows it
/// removed outright.
#[derive(Debug)]
pub(crate) struct Merged {
    pub(crate) rows: Vec<RecordBatch>,
    pub(crate) tombstones: Vec<RecordBatch>,
}

/// The batches of `batches` that hold a row.
fn held(batches: impl IntoIterator<Item = RecordBatch>) -> Vec<RecordBatch> {
    batches
        .into_iter()
        .filter(|batch| batch.num_rows() != 0)
        .collect()
}

/// The published rows once `incoming` upserts into `published` by `key`, the greatest sequence
/// winning among incoming rows of one key.
fn upsert(
    schema: &SchemaRef,
    published: &[RecordBatch],
    incoming: &[RecordBatch],
    key: &MergeKey,
) -> Result<Vec<RecordBatch>, ArrowError> {
    let converter = converter(schema, key)?;
    let mut nulls = Nulls::default();
    let incoming = concat(incoming, schema, &mut nulls)?;
    let incoming_keys = converter.convert_columns(&key_columns(&incoming, key)?)?;
    let seq = binary(&incoming, &key.seq)?;
    let seq = seq.as_binary::<i32>();
    let mut winners: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
    for row in 0..incoming.num_rows() {
        let row_key = incoming_keys.row(row).as_ref().to_vec();
        match winners.get(&row_key) {
            Some(&best) if seq.value(best) >= seq.value(row) => {}
            _ => {
                winners.insert(row_key, row);
            }
        }
    }
    let mut rows: Vec<usize> = winners.values().copied().collect();
    rows.sort_unstable();
    let incoming = taken(&incoming, &rows, &mut nulls)?;
    let published = concat(published, schema, &mut nulls)?;
    let published_keys = converter.convert_columns(&key_columns(&published, key)?)?;
    let kept: BooleanArray = (0..published.num_rows())
        .map(|row| Some(!winners.contains_key(published_keys.row(row).as_ref())))
        .collect();
    let published = filtered(&published, &kept, &mut nulls)?;
    Ok(held([published, incoming]))
}

/// The published rows of a child table once the roots `roots` publish replace their children:
/// published rows of those roots go, and of `incoming`, the rows of each root's winning row, by
/// root id and sequence, are added.
pub(crate) fn merge_children(
    schema: &SchemaRef,
    published: &[RecordBatch],
    incoming: &[RecordBatch],
    key: &MergeKey,
    root: &RootKey,
    roots: &[RecordBatch],
) -> Result<Vec<RecordBatch>, ArrowError> {
    let column = key.columns.first().ok_or_else(keyless)?;
    let mut winners: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for batch in roots {
        let (ids, seqs) = (binary(batch, &root.id)?, binary(batch, &root.seq)?);
        let (ids, seqs) = (ids.as_binary::<i32>(), seqs.as_binary::<i32>());
        for row in 0..batch.num_rows() {
            let (id, seq) = (ids.value(row), seqs.value(row));
            let best = winners.entry(id.to_vec()).or_default();
            *best = std::cmp::max(std::mem::take(best), seq.to_vec());
        }
    }
    let mut nulls = Nulls::default();
    let published = concat(published, schema, &mut nulls)?;
    let owners = binary(&published, column)?;
    let owners = owners.as_binary::<i32>();
    let kept: BooleanArray = (0..published.num_rows())
        .map(|row| Some(!winners.contains_key(owners.value(row))))
        .collect();
    let published = filtered(&published, &kept, &mut nulls)?;
    let incoming = concat(incoming, schema, &mut nulls)?;
    let (owners, seqs) = (binary(&incoming, column)?, binary(&incoming, &key.seq)?);
    let (owners, seqs) = (owners.as_binary::<i32>(), seqs.as_binary::<i32>());
    let winning: BooleanArray = (0..incoming.num_rows())
        .map(|row| Some(winners.get(owners.value(row)).map(Vec::as_slice) == Some(seqs.value(row))))
        .collect();
    let incoming = filtered(&incoming, &winning, &mut nulls)?;
    Ok(held([published, incoming]))
}

/// `batch`'s column `name` as `Binary`, which an id or a sequence is.
fn binary(batch: &RecordBatch, name: &str) -> Result<ArrayRef, ArrowError> {
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| ArrowError::SchemaError(format!("no column {name}")))?;
    retype::compared(column)
}

/// The error for a merge key naming no column.
fn keyless() -> ArrowError {
    ArrowError::InvalidArgumentError("the table is merged by a key of no column".to_owned())
}

/// The converter of `key`'s columns of `schema` to comparable rows; a key of no column, or of a
/// column or a sequence `schema` lacks, merges nothing.
fn converter(schema: &SchemaRef, key: &MergeKey) -> Result<RowConverter, ArrowError> {
    if key.columns.is_empty() {
        return Err(keyless());
    }
    schema
        .field_with_name(&key.seq)
        .map_err(|_| ArrowError::SchemaError(format!("no sequence column {}", key.seq)))?;
    let fields = key
        .columns
        .iter()
        .map(|column| {
            let field = schema
                .field_with_name(column)
                .map_err(|_| ArrowError::SchemaError(format!("no key column {column}")))?;
            Ok(SortField::new(field.data_type().clone()))
        })
        .collect::<Result<Vec<_>, ArrowError>>()?;
    RowConverter::new(fields)
}

fn key_columns(batch: &RecordBatch, key: &MergeKey) -> Result<Vec<ArrayRef>, ArrowError> {
    key.columns
        .iter()
        .map(|column| {
            batch
                .column_by_name(column)
                .cloned()
                .ok_or_else(|| ArrowError::SchemaError(format!("no key column {column}")))
        })
        .collect()
}
