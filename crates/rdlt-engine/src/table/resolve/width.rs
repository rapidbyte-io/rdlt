//! How wide a table's model is: its columns with every field nested in them, as a schema's width
//! is counted.

use rdlt_connector::{Field, LogicalType};

use super::super::model::Model;
use crate::error::Error;
use crate::limits::TABLE_COLUMNS_EXCEEDED;

/// Columns: `model`'s columns and the fields nested in them, counted without recursion; the
/// metadata columns lowering adds are the engine's own, and not counted.
pub(super) fn width(model: &Model) -> u64 {
    let mut count = 0_u64;
    let mut pending: Vec<&Field> = model.columns.iter().collect();
    while let Some(field) = pending.pop() {
        count = count.saturating_add(1);
        match field.logical_type() {
            LogicalType::Struct(fields) => pending.extend(fields.iter()),
            LogicalType::List(item) => pending.push(item),
            _ => {}
        }
    }
    count
}

/// Admits `model` if it is no wider than `limit` columns.
///
/// # Errors
///
/// A `Schema` error coded `table_columns_exceeded` where it is wider.
pub(super) fn admit(model: &Model, limit: u64) -> Result<(), Error> {
    let width = width(model);
    if width <= limit {
        return Ok(());
    }
    Err(Error::schema(format!(
        "the table would hold {width} columns, nested fields counted, more than the limit of \
         {limit}"
    ))
    .with_code(TABLE_COLUMNS_EXCEEDED))
}
