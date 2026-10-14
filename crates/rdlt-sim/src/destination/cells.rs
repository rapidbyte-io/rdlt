//! Stored rows: each row's Arrow data, for the oracle to read back as the types it was written with
//! say, and its cells as what they mean, for the oracle's read-back and the cursor checks; the
//! merge of a merge table's rows is the SDK's.

use std::collections::BTreeMap;

use arrow_array::RecordBatch;
use rdlt_connector::{ColumnKey, ColumnPath, ConnectorError, NameMap, Result, TableSchema};
use rdlt_testkit::canon::Canon;
use rdlt_testkit::decode;

/// The field metadata naming the type a stored value was written as, where the merge holds it at
/// a type its column widened to since: the value is read at the type it is held at, and judged
/// as it was written.
pub(crate) const WRITTEN: &str = "rdlt-sim:written";

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

/// The whole number `row` holds in the column `names` gives the source column `column`, stored
/// natively or, where the destination stores integers as text, as text.
pub(crate) fn number(row: &Stored, names: &NameMap, column: &str) -> Option<u64> {
    let physical = names.get(&ColumnKey::Source(ColumnPath::from(column)))?;
    match row.cells.get(physical)? {
        Canon::Number(text) | Canon::Text(text) => text.parse().ok(),
        _ => None,
    }
}
