//! Reading a stored row's cells back: each as the logical type the row was written with.

use rdlt_connector::{Field, LogicalType, TableSchema};
use rdlt_testkit::canon::Canon;
use rdlt_testkit::decode;

use crate::destination::{Stored, WRITTEN};

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

/// The cell of `physical` in `row`, read as the logical type the row was written with, with JSON
/// in it read as `source` says; `None` where the row lacks the column.
///
/// A value its column widened past since it was written is read at the type it is held at, and
/// the cell is the type it was written as.
pub(super) fn read<'a>(
    row: &Stored,
    written: &TableSchema,
    physical: &'a str,
    source: Option<&LogicalType>,
) -> Option<Cell<'a>> {
    let array = row.row.column_by_name(physical)?;
    let held = field(written, physical)?.logical_type().clone();
    let arrow = row.row.schema();
    let arrow_field = arrow.field_with_name(physical).ok()?;
    let as_written = arrow_field.metadata().get(WRITTEN).map(|written| {
        serde_json::from_str::<LogicalType>(written).expect("a stored row names a logical type")
    });
    let lowered = as_written.clone().unwrap_or_else(|| held.clone());
    let logical = Field::lowered_from(arrow_field).unwrap_or_else(|| lowered.clone());
    // A value held at the type its column widened to since is read at that type.
    let value = if as_written.is_some() {
        let hint = decode::hint(&held, source);
        decode::cell(array.as_ref(), 0, &held, &held, &hint)
    } else {
        let hint = decode::hint(&logical, source);
        decode::cell(array.as_ref(), 0, &logical, &lowered, &hint)
    };
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
