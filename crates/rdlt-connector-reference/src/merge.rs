//! Merging published rows by key, as the memory and files destinations publish a merge table.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, BooleanArray, RecordBatch, UInt32Array, new_null_array};
use arrow_row::{RowConverter, SortField};
use arrow_schema::{ArrowError, DataType, SchemaRef};
use rdlt_connector::{MergeKey, RootKey};

/// `batch` under `schema`: columns found by name and cast to the schema's types, missing columns
/// null.
pub(crate) fn align(batch: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, ArrowError> {
    let columns = schema
        .fields()
        .iter()
        .map(|field| match batch.column_by_name(field.name()) {
            Some(column) if column.data_type() == field.data_type() => Ok(Arc::clone(column)),
            Some(column) => arrow_cast::cast(column, field.data_type()),
            None => Ok(new_null_array(field.data_type(), batch.num_rows())),
        })
        .collect::<Result<Vec<ArrayRef>, _>>()?;
    RecordBatch::try_new(Arc::clone(schema), columns)
}

/// One batch holding `batches` under `schema`.
fn concat(batches: &[RecordBatch], schema: &SchemaRef) -> Result<RecordBatch, ArrowError> {
    let aligned = batches
        .iter()
        .map(|batch| align(batch, schema))
        .collect::<Result<Vec<_>, _>>()?;
    arrow_select::concat::concat_batches(schema, &aligned)
}

/// The published rows once `incoming` is merged into `published` by `key`: an incoming row
/// replaces the published row with its key, and among incoming rows of one key the greatest
/// sequence wins.
pub(crate) fn merge(
    schema: &SchemaRef,
    published: &[RecordBatch],
    incoming: &[RecordBatch],
    key: &MergeKey,
) -> Result<Vec<RecordBatch>, ArrowError> {
    let incoming = concat(incoming, schema)?;
    let converter = converter(schema, key)?;
    let incoming_keys = converter.convert_columns(&key_columns(&incoming, key)?)?;
    let seq = incoming
        .column_by_name(&key.seq)
        .ok_or_else(|| ArrowError::SchemaError(format!("no sequence column {}", key.seq)))?;
    let seq = arrow_cast::cast(seq, &DataType::Binary)?;
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
    let mut rows: Vec<u32> = winners
        .values()
        .map(|row| u32::try_from(*row).unwrap_or(u32::MAX))
        .collect();
    rows.sort_unstable();
    let incoming = arrow_select::take::take_record_batch(&incoming, &UInt32Array::from(rows))?;
    let published = concat(published, schema)?;
    let published_keys = converter.convert_columns(&key_columns(&published, key)?)?;
    let kept: BooleanArray = (0..published.num_rows())
        .map(|row| Some(!winners.contains_key(published_keys.row(row).as_ref())))
        .collect();
    let published = arrow_select::filter::filter_record_batch(&published, &kept)?;
    Ok([published, incoming]
        .into_iter()
        .filter(|batch| batch.num_rows() > 0)
        .collect())
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
    let mut winners: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for batch in roots {
        let (ids, seqs) = (binary(batch, &root.id)?, binary(batch, &root.seq)?);
        let (ids, seqs) = (ids.as_binary::<i32>(), seqs.as_binary::<i32>());
        for row in 0..batch.num_rows() {
            let (id, seq) = (ids.value(row), seqs.value(row));
            if winners.get(id).is_none_or(|best| best.as_slice() < seq) {
                winners.insert(id.to_vec(), seq.to_vec());
            }
        }
    }
    let column = key
        .columns
        .first()
        .ok_or_else(|| ArrowError::SchemaError("a child table's key names its root id".into()))?;
    let published = concat(published, schema)?;
    let owners = binary(&published, column)?;
    let owners = owners.as_binary::<i32>();
    let kept: BooleanArray = (0..published.num_rows())
        .map(|row| Some(!winners.contains_key(owners.value(row))))
        .collect();
    let published = arrow_select::filter::filter_record_batch(&published, &kept)?;
    let incoming = concat(incoming, schema)?;
    let (owners, seqs) = (binary(&incoming, column)?, binary(&incoming, &key.seq)?);
    let (owners, seqs) = (owners.as_binary::<i32>(), seqs.as_binary::<i32>());
    let winning: BooleanArray = (0..incoming.num_rows())
        .map(|row| Some(winners.get(owners.value(row)).map(Vec::as_slice) == Some(seqs.value(row))))
        .collect();
    let incoming = arrow_select::filter::filter_record_batch(&incoming, &winning)?;
    Ok([published, incoming]
        .into_iter()
        .filter(|batch| batch.num_rows() > 0)
        .collect())
}

/// `batch`'s column `name` as `Binary`.
fn binary(batch: &RecordBatch, name: &str) -> Result<ArrayRef, ArrowError> {
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| ArrowError::SchemaError(format!("no column {name}")))?;
    arrow_cast::cast(column, &DataType::Binary)
}

fn converter(schema: &SchemaRef, key: &MergeKey) -> Result<RowConverter, ArrowError> {
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
