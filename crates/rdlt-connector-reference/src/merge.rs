//! Merging published rows by key, as the memory and files destinations publish a merge table.

mod changes;
mod folded;
mod history;
mod refused;
mod retype;
mod sparse;
#[cfg(test)]
pub(crate) mod tests;
mod tombstones;
mod written;

pub(crate) use folded::folded;
pub(crate) use refused::failed;
pub(crate) use retype::holds;
pub(crate) use tombstones::schema as tombstone_schema;
pub(crate) use written::admitted;

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_row::{RowConverter, Rows, SortField};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use rdlt_connector::{ChangeColumns, ConnectorError, MergeKey, RootKey, TableRef};

use sparse::{At, Base, Nulls, Pick, Sources, assemble};

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
/// The rows are given back as batches of the columns they hold, as [`sparse`] has them: a
/// column a row never had costs the row nothing, here or in what a destination keeps of them.
/// It is one batch for each set of columns rows hold, however many: [`folded`] joins them for a
/// destination that pays for each.
///
/// A key `schema` cannot be merged by, one of no column or naming a column it lacks, is an error,
/// and so is a value a column's type no longer holds: nothing merges then.
pub(crate) fn merge_sparse(
    schema: &SchemaRef,
    published: &[RecordBatch],
    buried: &[RecordBatch],
    incoming: &[RecordBatch],
    key: &MergeKey,
) -> Result<Merged, ArrowError> {
    let stored = match &key.changes {
        Some(changes) => changes::stored(schema, changes),
        None => Arc::clone(schema),
    };
    for batch in incoming {
        admitted(batch, Some(&stored), key)?;
    }
    let (rows, tombstones) = match (&key.history, &key.changes) {
        (Some(history), _) => {
            history::merge_history(schema, published, buried, incoming, key, history)?
        }
        (None, Some(changes)) => {
            changes::merge_changes(schema, published, buried, incoming, key, changes)?
        }
        (None, None) => (upsert(schema, published, incoming, key)?, Vec::new()),
    };
    Ok(Merged { rows, tombstones })
}

/// `rows`, batches of the columns they hold, as a reader is given them: batches of every column
/// of `schema`, a column a batch does not hold being nulls its rows share with every other such
/// column, which costs a reader a reference a column, whatever the rows.
pub(crate) fn read_back(
    schema: &SchemaRef,
    rows: &[RecordBatch],
) -> Result<Vec<RecordBatch>, ArrowError> {
    sparse::every_column(schema, rows, &mut Nulls::default())
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

/// The sources of a merge under `schema`: `published`, then `incoming`, each batch a source;
/// returns them with how many are published.
fn sources(
    schema: &SchemaRef,
    published: &[RecordBatch],
    incoming: &[RecordBatch],
) -> Result<(Sources, usize), ArrowError> {
    let mut sources = Sources::new(schema);
    for batch in published.iter().chain(incoming) {
        sources.add(batch)?;
    }
    Ok((sources, published.len()))
}

/// The keys of every row of each source, as `converter` encodes `key`'s columns.
fn source_keys(
    sources: &Sources,
    converter: &RowConverter,
    key: &MergeKey,
    nulls: &mut Nulls,
) -> Result<Vec<Rows>, ArrowError> {
    let columns = key
        .columns
        .iter()
        .map(|column| sources.schema().index_of(column))
        .collect::<Result<Vec<usize>, _>>()?;
    (0..sources.len())
        .map(|source| {
            let values: Vec<ArrayRef> = columns
                .iter()
                .map(|column| sources.dense(source, *column, nulls))
                .collect();
            converter.convert_columns(&values)
        })
        .collect()
}

/// The sequence of every row of each source, as the bytes it compares by.
fn source_seqs(
    sources: &Sources,
    key: &MergeKey,
    nulls: &mut Nulls,
) -> Result<Vec<ArrayRef>, ArrowError> {
    source_bytes(sources, &key.seq, nulls)
}

/// The column `name` of each source as the bytes an id or a sequence compares by.
fn source_bytes(
    sources: &Sources,
    name: &str,
    nulls: &mut Nulls,
) -> Result<Vec<ArrayRef>, ArrowError> {
    let column = sources
        .schema()
        .index_of(name)
        .map_err(|_| unkeyed(format!("the table has no column {name}")))?;
    (0..sources.len())
        .map(|source| retype::compared(&sources.dense(source, column, nulls)))
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
    let (sources, held) = sources(schema, published, incoming)?;
    let keys = source_keys(&sources, &converter, key, &mut nulls)?;
    let seqs = source_seqs(&sources, key, &mut nulls)?;
    let seq = |at: At| seqs[at.source].as_binary::<i32>().value(at.row);
    let rows_in = |range: std::ops::Range<usize>| -> Vec<At> {
        range
            .flat_map(|source| (0..sources.rows(source)).map(move |row| At { source, row }))
            .collect()
    };
    let mut winners: BTreeMap<Vec<u8>, At> = BTreeMap::new();
    for at in rows_in(held..sources.len()) {
        let row_key = keys[at.source].row(at.row).as_ref().to_vec();
        match winners.get(&row_key) {
            Some(best) if seq(*best) >= seq(at) => {}
            _ => {
                winners.insert(row_key, at);
            }
        }
    }
    let kept = rows_in(0..held)
        .into_iter()
        .filter(|at| !winners.contains_key(keys[at.source].row(at.row).as_ref()));
    let mut won: Vec<At> = winners.values().copied().collect();
    won.sort_unstable();
    let picks = kept.chain(won).map(|at| Pick {
        base: Base::Row(at),
        over: &[],
    });
    assemble(&sources, picks)
}

/// The published rows of a child table once the roots `roots` publish replace their children:
/// published rows of those roots go, and of `incoming`, the rows of each root's winning row, by
/// root id and sequence, are added.
pub(crate) fn merge_children_sparse(
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
        alike(batch, &root.id, schema, column)?;
        alike(batch, &root.seq, schema, &key.seq)?;
        let (ids, seqs) = (binary(batch, &root.id)?, binary(batch, &root.seq)?);
        let (ids, seqs) = (ids.as_binary::<i32>(), seqs.as_binary::<i32>());
        for row in 0..batch.num_rows() {
            let (id, seq) = (ids.value(row), seqs.value(row));
            let best = winners.entry(id.to_vec()).or_default();
            *best = std::cmp::max(std::mem::take(best), seq.to_vec());
        }
    }
    let mut nulls = Nulls::default();
    let (sources, held) = sources(schema, published, incoming)?;
    let owners = source_bytes(&sources, column, &mut nulls)?;
    let seqs = source_seqs(&sources, key, &mut nulls)?;
    let mut picked = Vec::new();
    for source in 0..sources.len() {
        let (owners, seqs) = (
            owners[source].as_binary::<i32>(),
            seqs[source].as_binary::<i32>(),
        );
        for row in 0..sources.rows(source) {
            let winner = winners.get(owners.value(row)).map(Vec::as_slice);
            // A published row stays unless its root is published again; an incoming row is
            // added where it is of its root's winning row.
            let stays = if source < held {
                winner.is_none()
            } else {
                winner == Some(seqs.value(row))
            };
            if stays {
                picked.push(At { source, row });
            }
        }
    }
    let picks = picked.into_iter().map(|at| Pick {
        base: Base::Row(at),
        over: &[],
    });
    assemble(&sources, picks)
}

/// Refuses a root's column `of_root` of `roots` and its children's column `of_children` of
/// `schema` where one compares as a number and the other as bytes: no value of one is a value
/// of the other, so every child would be taken for a root's that was not published.
fn alike(
    roots: &RecordBatch,
    of_root: &str,
    schema: &SchemaRef,
    of_children: &str,
) -> Result<(), ArrowError> {
    let (Some(root), Ok(child)) = (
        roots.column_by_name(of_root),
        schema.field_with_name(of_children),
    ) else {
        // A column that is missing is refused where it is read.
        return Ok(());
    };
    if retype::numbered(root.data_type()) == retype::numbered(child.data_type()) {
        return Ok(());
    }
    Err(unkeyed(format!(
        "the roots' {of_root} is {} and their children's {of_children} is {}",
        root.data_type(),
        child.data_type()
    )))
}

/// `batch`'s column `name` as `Binary`, which an id or a sequence is.
fn binary(batch: &RecordBatch, name: &str) -> Result<ArrayRef, ArrowError> {
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| unkeyed(format!("the rows have no column {name}")))?;
    retype::compared(column)
}

/// The error for a merge key naming no column.
fn keyless() -> ArrowError {
    unkeyed("the table is merged by a key of no column")
}

/// The refusal of a merge key the rows cannot be merged by.
fn unkeyed(message: impl Into<String>) -> ArrowError {
    refused::refused(refused::MERGE_KEY_INVALID, message)
}

/// The converter of `key`'s columns of `schema` to comparable rows; a key of no column, or of a
/// column or a sequence `schema` lacks, merges nothing.
fn converter(schema: &SchemaRef, key: &MergeKey) -> Result<RowConverter, ArrowError> {
    if key.columns.is_empty() {
        return Err(keyless());
    }
    schema
        .field_with_name(&key.seq)
        .map_err(|_| unkeyed(format!("the table has no sequence column {}", key.seq)))?;
    let fields = key
        .columns
        .iter()
        .map(|column| {
            let field = schema
                .field_with_name(column)
                .map_err(|_| unkeyed(format!("the table has no key column {column}")))?;
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
                .ok_or_else(|| unkeyed(format!("the rows have no key column {column}")))
        })
        .collect()
}
