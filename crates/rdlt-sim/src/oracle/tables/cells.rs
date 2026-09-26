//! Reading a stored row's cells back: each as the logical type the row was written with.

use rdlt_connector::{Field, LogicalType, TableSchema};
use rdlt_testkit::canon::Canon;
use rdlt_testkit::decode;

use crate::destination::Stored;

/// A cell's meaning as text: a number's or a hex id's digits.
pub(super) fn text(value: &Canon) -> String {
    match value {
        Canon::Number(text) | Canon::Bytes(text) | Canon::Text(text) => text.clone(),
        other => format!("{other:?}"),
    }
}

/// The field of `schema` called `name`.
fn field<'a>(schema: &'a TableSchema, name: &str) -> Option<&'a Field> {
    schema.fields().iter().find(|field| field.name() == name)
}

/// One cell of a row, read.
pub(super) struct Cell<'a> {
    pub(super) physical: &'a str,
    /// The logical type it was written with.
    pub(super) logical: LogicalType,
    /// The type the row stores it as.
    pub(super) lowered: LogicalType,
    pub(super) value: Canon,
}

/// The cell of `physical` in `row`, read as the logical type the row was written with, with
/// JSON in it read as `source` says; `None` where the row lacks the column.
pub(super) fn read<'a>(
    row: &Stored,
    written: &TableSchema,
    physical: &'a str,
    source: Option<&LogicalType>,
) -> Option<Cell<'a>> {
    let array = row.row.column_by_name(physical)?;
    let lowered = field(written, physical)?.logical_type().clone();
    let arrow = row.row.schema();
    let named = Field::lowered_from(arrow.field_with_name(physical).ok()?);
    let logical = named.unwrap_or_else(|| lowered.clone());
    let hint = decode::hint(&logical, source);
    let value = decode::cell(array.as_ref(), 0, &logical, &lowered, &hint);
    Some(Cell {
        physical,
        logical,
        lowered,
        value,
    })
}

/// The names of `row`'s columns, in order.
pub(super) fn fields(row: &Stored) -> Vec<String> {
    let schema = row.row.schema();
    schema
        .fields()
        .iter()
        .map(|field| field.name().clone())
        .collect()
}
