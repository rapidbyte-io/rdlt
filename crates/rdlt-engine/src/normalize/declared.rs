//! The tables a normalized stream's declared schema makes before anything is read: its own
//! table's columns, and each child table with the columns its rows bring.

#[cfg(test)]
mod tests;

use std::sync::Arc;

use rdlt_connector::{ColumnPath, Field, LogicalType, TableSchema};

use super::{Shape, VALUE, name};
use crate::error::Error;
use crate::table::Incoming;

/// A child table's path below the stream's table and the columns a declared schema gives it.
pub(crate) type Child = (Vec<Arc<str>>, Incoming);

/// The columns of one table a declared schema makes, by their path within it, and the arrays
/// within depth its rows hold: each one's path, item and depth.
#[derive(Default)]
struct Table {
    columns: Vec<(Vec<Arc<str>>, Field)>,
    arrays: Vec<(Vec<Arc<str>>, Field, u8)>,
}

impl Table {
    /// Places `field`, at `path` with values at `depth`, as normalizing places a column of its
    /// type: an object within depth flattens into its fields, an array within depth waits to
    /// become a child table, and anything else is a column.
    fn place(&mut self, path: Vec<Arc<str>>, field: &Field, depth: u8, max_depth: u8) {
        match field.logical_type() {
            _ if depth > max_depth => self.columns.push((path, field.clone())),
            LogicalType::Struct(fields) => {
                for inner in fields.iter() {
                    let mut inner_path = path.clone();
                    inner_path.push(Arc::from(inner.name()));
                    let inner = Field::new(inner.name(), inner.logical_type().clone(), true);
                    self.place(inner_path, &inner, depth.saturating_add(1), max_depth);
                }
            }
            LogicalType::List(item) => self.arrays.push((path, item.as_ref().clone(), depth)),
            _ => self.columns.push((path, field.clone())),
        }
    }

    /// The table's columns, each named so no two paths share a name.
    fn incoming(self) -> Result<Incoming, Error> {
        let paths: Vec<ColumnPath> = self
            .columns
            .iter()
            .map(|(path, _)| {
                ColumnPath::new(path.clone()).map_err(|error| Error::internal(error.to_string()))
            })
            .collect::<Result<_, _>>()?;
        let fields = self
            .columns
            .into_iter()
            .zip(&paths)
            .map(|((_, field), path)| {
                Field::new(
                    name(path),
                    field.logical_type().clone(),
                    field.is_nullable(),
                )
            })
            .collect();
        let schema =
            TableSchema::new(fields).map_err(|error| Error::internal(error.to_string()))?;
        Ok(Incoming::of(schema, paths, &[]))
    }
}

/// The table holding a declared `schema`'s rows, normalized as `shape`, before its arrays are
/// taken out.
fn rows(schema: &TableSchema, shape: &Shape) -> Table {
    let mut table = Table::default();
    for field in schema.fields().iter() {
        let path = vec![Arc::from(field.name())];
        if shape.whole.contains(field.name()) {
            table.columns.push((path, field.clone()));
        } else {
            table.place(path, field, 1, shape.max_depth);
        }
    }
    table
}

/// The columns of a normalized stream's own table that a declared `schema` holds: objects within
/// depth flatten into their fields, and arrays within depth, which are child tables, are left
/// out.
pub(crate) fn root_columns(schema: &TableSchema, shape: &Shape) -> Result<Incoming, Error> {
    rows(schema, shape).incoming()
}

/// The child tables a declared `schema`'s arrays within depth make, parents first, each with the
/// columns its rows bring.
pub(crate) fn children(schema: &TableSchema, shape: &Shape) -> Result<Vec<Child>, Error> {
    let mut children = Vec::new();
    for (path, item, depth) in rows(schema, shape).arrays {
        items(&path, &item, depth, shape.max_depth, &mut children)?;
    }
    Ok(children)
}

/// Adds the child table at `path` of the array at `depth` whose items are `item`, then its own
/// child tables: an object within depth flattens into columns, an array within depth is a
/// grandchild table under `value`, and anything else is the column `value`.
fn items(
    path: &[Arc<str>],
    item: &Field,
    depth: u8,
    max_depth: u8,
    children: &mut Vec<Child>,
) -> Result<(), Error> {
    let depth = depth.saturating_add(1);
    let mut table = Table::default();
    let value = || vec![Arc::from(VALUE)];
    match item.logical_type() {
        LogicalType::Struct(fields) if depth <= max_depth => {
            for field in fields.iter() {
                let inner = Field::new(field.name(), field.logical_type().clone(), true);
                let inner_path = vec![Arc::from(field.name())];
                table.place(inner_path, &inner, depth.saturating_add(1), max_depth);
            }
        }
        LogicalType::List(inner) if depth <= max_depth => {
            table.arrays.push((value(), inner.as_ref().clone(), depth));
        }
        logical => {
            let column = Field::new(VALUE, logical.clone(), true);
            table.columns.push((value(), column));
        }
    }
    let arrays = std::mem::take(&mut table.arrays);
    children.push((path.to_vec(), table.incoming()?));
    for (child, inner, child_depth) in arrays {
        let child_path = [path, &child].concat();
        items(&child_path, &inner, child_depth, max_depth, children)?;
    }
    Ok(())
}
