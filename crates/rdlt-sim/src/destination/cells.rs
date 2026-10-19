//! Stored rows: each row's Arrow data, for the oracle to read back as the types it was written with
//! say, and its cells as what they mean, for the oracle's read-back and the cursor checks; the
//! merge of a merge table's rows is the SDK's.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{RecordBatch, RecordBatchOptions, StringArray, UInt32Array};
use arrow_schema::DataType;
use arrow_select::take::take_record_batch;
use rdlt_connector::{ColumnKey, ColumnPath, ConnectorError, NameMap, Result, TableSchema};
use rdlt_testkit::canon::Canon;
use rdlt_testkit::decode;

use crate::oracle::{Kept, aside, freed};

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
    /// What its cells hold, the world's and not the engine's, dropped after them; a copy's
    /// cells are made where it is copied, which counts them.
    cells_kept: Kept,
    /// What the row holds, the world's: copies share it, so it goes with the last of them.
    row_kept: Arc<Kept>,
}

/// The rows of `batch`, each holding buffers of its own rather than the batch's.
///
/// Making them is the world's work, and what they hold is the world's: each counts as kept for
/// as long as it lives, as much as freeing a first making of it gave back.
pub(crate) fn rows(batch: &RecordBatch) -> Result<Vec<Stored>> {
    aside(|| {
        let gauge = made(batch)?;
        let mut rows = made(batch)?;
        for (row, gauged) in rows.iter_mut().zip(gauge) {
            let Stored {
                cells, row: shared, ..
            } = gauged;
            row.cells_kept = Kept::held(freed(cells));
            row.row_kept = Arc::new(Kept::held(freed(shared)));
        }
        Ok(rows)
    })
}

/// The rows of `batch`, none yet carrying what it holds.
fn made(batch: &RecordBatch) -> Result<Vec<Stored>> {
    let batch = &condensed(batch)?;
    let schema = TableSchema::from_arrow(&batch.schema())
        .map_err(|error| ConnectorError::data(format!("reading a batch: {error}")))?;
    (0..batch.num_rows())
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
            Ok(Stored {
                cells,
                row: apart(batch, row)?,
                cells_kept: Kept::held(0),
                row_kept: Arc::new(Kept::held(0)),
            })
        })
        .collect()
}

/// Row `row` of `batch` as a batch of one row, copied into buffers of its own.
///
/// A slice keeps every buffer of the batch it was cut from, and the world keeps its rows for as
/// long as it lives. A take copies the row's values, a list's and a struct's children among them,
/// but of a dictionary only its key: the row keeps the dictionary's values whole. The take is of
/// the whole batch, not of a slice of it, since a list's take sizes its copy by the proportion of
/// the column it takes.
fn apart(batch: &RecordBatch, row: usize) -> Result<RecordBatch> {
    let index = UInt32Array::from(vec![u32::try_from(row).map_err(|_| {
        ConnectorError::internal("a batch of more rows than a row index holds")
    })?]);
    take_record_batch(batch, &index)
        .map_err(|error| ConnectorError::internal(format!("storing a row: {error}")))
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

/// `batch` with each fat value of its column of them in the short form the destination keeps,
/// and every other value as it is: the world keeps every row it stores for as long as it lives.
fn condensed(batch: &RecordBatch) -> Result<RecordBatch> {
    let schema = batch.schema();
    let fat = schema
        .fields()
        .iter()
        .position(|field| field.name().eq_ignore_ascii_case(crate::fat::COLUMN));
    let Some(index) = fat else {
        return Ok(batch.clone());
    };
    let failed = |error: arrow_schema::ArrowError| {
        ConnectorError::data(format!("keeping a fat value: {error}"))
    };
    let column = batch.column(index);
    let texts = arrow_cast::cast(column, &DataType::Utf8).map_err(failed)?;
    let short: StringArray = texts
        .as_string::<i32>()
        .iter()
        .map(|text| text.map(|text| crate::fat::condense(text).unwrap_or_else(|| text.to_owned())))
        .collect();
    let mut columns = batch.columns().to_vec();
    columns[index] = arrow_cast::cast(&short, column.data_type()).map_err(failed)?;
    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema, columns, &options).map_err(failed)
}
