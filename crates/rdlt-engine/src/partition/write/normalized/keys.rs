//! The merge keys a normalized stream cannot keep: those it takes apart.

use arrow_schema::Schema;

use crate::normalize::Shape;
use crate::normalize::placement::{Container, Placement, placement};

/// The column of the key that normalizing a batch of `schema` as `shape` takes apart, if any.
///
/// An object is flattened into columns of their own, and an array moved to a table of its own,
/// which leaves the table no column for its key; one kept whole, or held in a dictionary or runs,
/// stays a column.
pub(crate) fn taken_apart<'a>(schema: &Schema, shape: &'a Shape) -> Option<&'a str> {
    shape.key.iter().map(AsRef::as_ref).find(|key| {
        !shape.whole.contains(*key)
            && schema.field_with_name(key).is_ok_and(|field| {
                let container = Container::of_arrow(field.data_type());
                placement(container, 1, shape.max_depth) != Placement::Column
            })
    })
}

#[cfg(test)]
mod tests;
