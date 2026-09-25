//! Stored rows: each row's Arrow data, for the oracle to read back as the types it was written with
//! say, and its cells as what they mean, for the destination's own merges and checks.

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use rdlt_connector::{
    ColumnKey, ColumnPath, ConnectorError, MergeKey, NameMap, Result, RootKey, TableSchema,
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
