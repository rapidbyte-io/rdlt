//! The merge keys a normalized stream cannot keep: those it takes apart.

use arrow_schema::{DataType, Schema};

use crate::normalize::Shape;

/// The column of the key that normalizing a batch of `schema` as `shape` takes apart, if any.
///
/// An object is flattened into columns of their own, and an array moved to a table of its own,
/// which leaves the table no column for its key.
pub(crate) fn taken_apart<'a>(schema: &Schema, shape: &'a Shape) -> Option<&'a str> {
    if shape.max_depth == 0 {
        return None;
    }
    shape.key.iter().map(AsRef::as_ref).find(|key| {
        let Ok(field) = schema.field_with_name(key) else {
            return false;
        };
        let stored = match field.data_type() {
            DataType::Dictionary(_, values) => values.as_ref(),
            DataType::RunEndEncoded(_, values) => values.data_type(),
            other => other,
        };
        let container = matches!(
            stored,
            DataType::Struct(_)
                | DataType::List(_)
                | DataType::LargeList(_)
                | DataType::ListView(_)
                | DataType::LargeListView(_)
                | DataType::FixedSizeList(..)
                | DataType::Map(..)
        );
        container && !shape.whole.contains(*key)
    })
}
