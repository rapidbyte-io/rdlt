//! Stored rows: each row's Arrow data, for the oracle to read back as the types it was written with
//! say, and its cells as what they mean, for the destination's own merges and checks.

use std::collections::BTreeMap;

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::Int8Type;
use arrow_array::{Array, ArrayRef, RecordBatch, new_null_array};
use arrow_schema::{DataType, Field as ArrowField, Schema};
use rdlt_connector::{
    ChangeColumns, ChangeOp, ColumnKey, ColumnPath, ConnectorError, Deletion, MergeKey, NameMap,
    Result, RootKey, TableSchema,
};
use rdlt_testkit::canon::Canon;
use rdlt_testkit::decode;

/// One stored row's cells by identifier, each read as the type its column is stored as.
pub type Cells = BTreeMap<String, Canon>;

/// One stored row.
#[derive(Clone, Debug)]
pub struct Stored {
    /// Its cells.
    pub cells: Cells,
    /// The row itself: a batch of one row, with the types it was stored as, each naming the
    /// logical type it holds where that differs.
    pub row: RecordBatch,
}

/// The rows of `batch`.
pub(crate) fn rows(batch: &RecordBatch) -> Result<Vec<Stored>> {
    let schema = TableSchema::from_arrow(&batch.schema())
        .map_err(|error| ConnectorError::data(format!("reading a batch: {error}")))?;
    Ok((0..batch.num_rows())
        .map(|row| {
            let cells = schema
                .fields()
                .iter()
                .zip(batch.columns())
                .map(|(field, column)| {
                    let stored = field.logical_type();
                    let value = decode::cell(column.as_ref(), row, stored, stored, stored);
                    (field.name().to_owned(), value)
                })
                .collect();
            Stored {
                cells,
                row: batch.slice(row, 1),
            }
        })
        .collect())
}

/// The text a cell compares by: its meaning, printed.
fn text(row: &Stored, column: &str) -> String {
    format!("{:?}", row.cells.get(column).unwrap_or(&Canon::Null))
}

/// Merges `incoming` into `published` by `key`: an incoming row replaces the published row with
/// its key, and among incoming rows of one key the greatest sequence wins.
pub(crate) fn merge(published: &mut Vec<Stored>, incoming: Vec<Stored>, key: &MergeKey) {
    let key_of = |row: &Stored| {
        let values: Vec<String> = key.columns.iter().map(|column| text(row, column)).collect();
        values.join("\u{1}")
    };
    let mut winners: BTreeMap<String, Stored> = BTreeMap::new();
    for row in incoming {
        let row_key = key_of(&row);
        match winners.get(&row_key) {
            Some(best) if text(best, &key.seq) >= text(&row, &key.seq) => {}
            _ => {
                winners.insert(row_key, row);
            }
        }
    }
    published.retain(|row| !winners.contains_key(&key_of(row)));
    published.extend(winners.into_values());
}

/// Merges a change stream's `incoming` rows into `published` by `key`, as `changes` directs:
/// each row applies in sequence order, only past the published row's sequence, as an insert,
/// update, delete or truncate; the columns that direct the merge are never stored.
pub(crate) fn merge_changes(
    published: &mut Vec<Stored>,
    mut incoming: Vec<Stored>,
    key: &MergeKey,
    changes: &ChangeColumns,
) {
    let key_of = |row: &Stored| {
        let values: Vec<String> = key.columns.iter().map(|column| text(row, column)).collect();
        values.join("\u{1}")
    };
    // Sequences are 16 bytes, so their texts order as the bytes do.
    incoming.sort_by_key(|row| text(row, &key.seq));
    let at = match &changes.deletion {
        Deletion::Soft { at } => Some(at.as_ref()),
        Deletion::Hard => None,
    };
    for row in incoming {
        let op = row
            .row
            .column_by_name(&changes.op)
            .and_then(|ops| arrow_cast::cast(ops, &DataType::Int8).ok())
            .and_then(|ops| ChangeOp::from_code(ops.as_primitive::<Int8Type>().value(0)));
        let seq = text(&row, &key.seq);
        if op == Some(ChangeOp::Truncate) {
            match at {
                None => published.retain(|stored| text(stored, &key.seq) >= seq),
                Some(at) => {
                    for stored in published.iter_mut() {
                        if text(stored, &key.seq) < seq {
                            *stored = deleted(stored, &row, &key.seq, at, changes);
                        }
                    }
                }
            }
            continue;
        }
        let row_key = key_of(&row);
        let current = published
            .iter()
            .position(|stored| key_of(stored) == row_key);
        if current.is_some_and(|index| text(&published[index], &key.seq) >= seq) {
            continue;
        }
        match (op, at, current) {
            (Some(ChangeOp::Delete), None, Some(index)) => {
                published.remove(index);
            }
            (Some(ChangeOp::Delete), Some(at), Some(index)) => {
                published[index] = deleted(&published[index], &row, &key.seq, at, changes);
            }
            (Some(ChangeOp::Delete), _, None) => {}
            _ => {
                let merged = upserted(current.map(|index| &published[index]), &row, changes);
                match current {
                    Some(index) => published[index] = merged,
                    None => published.push(merged),
                }
            }
        }
    }
}

/// `row`, an insert or update, as it replaces `current`: every column but those it flags
/// unchanged, which keep `current`'s values, or are null where there is none.
fn upserted(current: Option<&Stored>, row: &Stored, changes: &ChangeColumns) -> Stored {
    let flagged = unchanged(row, changes);
    let Some(current) = current else {
        return nulled(row, &flagged, changes);
    };
    let schema = row.row.schema();
    let kept: Vec<&str> = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .filter(|name| !flagged.iter().any(|flag| flag == name))
        .collect();
    with(current, row, &kept, changes)
}

/// The names of the columns `row` flags unchanged.
fn unchanged(row: &Stored, changes: &ChangeColumns) -> Vec<String> {
    let Some(column) = &changes.unchanged else {
        return Vec::new();
    };
    let Some(flags) = row.row.column_by_name(column) else {
        return Vec::new();
    };
    let Ok(flags) = arrow_cast::cast(flags, &DataType::Binary) else {
        return Vec::new();
    };
    let flags = flags.as_binary::<i32>();
    if flags.is_null(0) {
        return Vec::new();
    }
    let bitmap = flags.value(0);
    row.row
        .schema()
        .fields()
        .iter()
        .enumerate()
        .filter(|(ordinal, _)| {
            bitmap
                .get(ordinal / 8)
                .is_some_and(|byte| byte & (1 << (ordinal % 8)) != 0)
        })
        .map(|(_, field)| field.name().clone())
        .collect()
}

/// `stored` marked deleted by `row`: it takes the row's sequence in `seq`, and its deletion time
/// in `at` unless it was deleted already, so it keeps when that was.
fn deleted(stored: &Stored, row: &Stored, seq: &str, at: &str, changes: &ChangeColumns) -> Stored {
    let before = !matches!(stored.cells.get(at), None | Some(Canon::Null));
    let columns: &[&str] = if before { &[seq] } else { &[seq, at] };
    with(stored, row, columns, changes)
}

/// `stored` with the cells of `columns` taken from `row`, without the columns that direct a
/// merge.
fn with(stored: &Stored, row: &Stored, columns: &[&str], changes: &ChangeColumns) -> Stored {
    let fields = row.row.schema();
    let mut parts = Vec::new();
    for field in fields.fields() {
        let name = field.name().as_str();
        if directs(name, changes) {
            continue;
        }
        let (source, other) = if columns.contains(&name) {
            (row, stored)
        } else {
            (stored, row)
        };
        let column = source
            .row
            .column_by_name(name)
            .or_else(|| other.row.column_by_name(name))
            .cloned()
            .map_or_else(|| new_null_array(field.data_type(), 1), |column| column);
        let column = arrow_cast::cast(&column, field.data_type()).unwrap_or(column);
        let cell = source
            .cells
            .get(name)
            .or_else(|| other.cells.get(name))
            .cloned()
            .unwrap_or(Canon::Null);
        parts.push((field.as_ref().clone(), column, cell));
    }
    compose(parts)
}

/// `row` with the columns `flagged` null, and without the columns that direct a merge.
fn nulled(row: &Stored, flagged: &[String], changes: &ChangeColumns) -> Stored {
    let fields = row.row.schema();
    let mut parts = Vec::new();
    for (field, column) in fields.fields().iter().zip(row.row.columns()) {
        let name = field.name().as_str();
        if directs(name, changes) {
            continue;
        }
        let (column, cell) = if flagged.iter().any(|flag| flag == name) {
            (new_null_array(field.data_type(), 1), Canon::Null)
        } else {
            (
                Arc::clone(column),
                row.cells.get(name).cloned().unwrap_or(Canon::Null),
            )
        };
        parts.push((field.as_ref().clone(), column, cell));
    }
    compose(parts)
}

/// Whether `name` is a column that only directs a merge.
fn directs(name: &str, changes: &ChangeColumns) -> bool {
    name == &*changes.op || changes.unchanged.as_deref() == Some(name)
}

/// One stored row of `parts`: each column's field, one-row array and cell.
fn compose(parts: Vec<(ArrowField, ArrayRef, Canon)>) -> Stored {
    let fields: Vec<ArrowField> = parts
        .iter()
        .map(|(field, _, _)| field.clone().with_nullable(true))
        .collect();
    let columns: Vec<ArrayRef> = parts
        .iter()
        .map(|(_, column, _)| Arc::clone(column))
        .collect();
    let cells = parts
        .into_iter()
        .map(|(field, _, cell)| (field.name().clone(), cell))
        .collect();
    let row = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns)
        .expect("one row of every column");
    Stored { cells, row }
}

/// Merges `incoming` into `published`, the rows of a child table merging by `key` below `root`,
/// as the root table merges `roots`: every published row of a root among `roots` goes, and the
/// incoming rows of each root's winning row, whose sequence is greatest, take their place.
pub(crate) fn merge_children(
    published: &mut Vec<Stored>,
    incoming: Vec<Stored>,
    key: &MergeKey,
    root: &RootKey,
    roots: &[Stored],
) {
    let mut winners: BTreeMap<String, String> = BTreeMap::new();
    for row in roots {
        let (id, seq) = (text(row, &root.id), text(row, &root.seq));
        if winners.get(&id).is_none_or(|best| *best < seq) {
            winners.insert(id, seq);
        }
    }
    let owner = key.columns.first().map_or("", AsRef::as_ref);
    published.retain(|row| !winners.contains_key(&text(row, owner)));
    published.extend(
        incoming
            .into_iter()
            .filter(|row| winners.get(&text(row, owner)) == Some(&text(row, &key.seq))),
    );
}

/// The whole number `row` holds in the column `names` gives the source column `column`, stored
/// natively or, where the destination stores integers as text, as text.
pub(crate) fn number(row: &Stored, names: &NameMap, column: &str) -> Option<u64> {
    let physical = names.get(&ColumnKey::Source(ColumnPath::from(column)))?;
    match row.cells.get(physical)? {
        Canon::Number(text) | Canon::Text(text) => text.parse().ok(),
        _ => None,
    }
}
