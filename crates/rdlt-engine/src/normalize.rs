//! Normalizing nested data: arrays become child tables at any depth, objects flatten into one
//! column per field, and every row carries its lineage.
//!
//! Normalizing works on Arrow batches, after JSON is shredded, so Arrow and JSON pushes normalize
//! alike. A container (an object or an array) nested deeper than the stream's `max_depth` stays
//! whole, and its table stores it as `Json`.

mod cascade;
pub(crate) mod declared;
#[cfg(test)]
mod encodings;
pub(crate) mod identity;
pub(crate) mod placement;
#[cfg(test)]
mod reference;
pub(crate) mod rounding;
#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::UInt32Type;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Int64Array, ListArray, RecordBatch,
    RecordBatchOptions, UInt32Array,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{ArrowError, DataType, Field as ArrowField, FieldRef, Schema};
use arrow_select::nullif::nullif;
use placement::{Container, Placement, placement};
use rdlt_connector::ColumnPath;

use crate::table::convert::decoded;

pub(crate) use cascade::{Dropped, Pruned};

/// How a stream's batches normalize.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Shape {
    /// Containers nested deeper than this stay whole.
    pub(crate) max_depth: u8,
    /// Top-level columns kept whole, which the plan stores natively or as JSON instead.
    pub(crate) whole: BTreeSet<Arc<str>>,
    /// The top-level columns that identify a root row, in order; with none, the whole row does.
    pub(crate) key: Vec<Arc<str>>,
}

/// One table's rows from a normalized batch.
#[derive(Clone, Debug)]
pub(crate) struct Part {
    /// The table's path below the stream's table: empty for the stream's table itself, the path of
    /// the array its rows come from for a child table.
    pub(crate) path: Vec<Arc<str>>,
    /// The path of each of the batch's columns within its table.
    pub(crate) columns: Vec<ColumnPath>,
    /// The table's data columns, named so no two paths share a name.
    pub(crate) batch: RecordBatch,
    pub(crate) lineage: Lineage,
}

/// Where the rows of a part come from.
#[derive(Clone, Debug)]
pub(crate) struct Lineage {
    /// Each row's id.
    pub(crate) id: ArrayRef,
    /// The position of each row's root row, the row itself for a stream's own table, among the
    /// stream's rows the batch holds: a merge table's rows take their sequence from it.
    pub(crate) root_row: ArrayRef,
    /// For a child table, each row's parent.
    pub(crate) parent: Option<Parent>,
}

/// The parents of a child table's rows.
#[derive(Clone, Debug)]
pub(crate) struct Parent {
    /// The id of each row's parent row.
    pub(crate) id: ArrayRef,
    /// The id of each row's root row.
    pub(crate) root: ArrayRef,
    /// Each row's position in its parent's array.
    pub(crate) idx: ArrayRef,
    /// The path of the parent rows' part.
    pub(crate) path: Vec<Arc<str>>,
    /// The position of each row's parent row among its part's rows.
    pub(crate) row: ArrayRef,
}

/// The rows of a table whose arrays become child tables: their part's path, each row's id, its
/// root's id and the position of its root row.
#[derive(Clone, Copy)]
struct Rows<'a> {
    path: &'a [Arc<str>],
    ids: &'a BinaryArray,
    roots: &'a BinaryArray,
    positions: &'a UInt32Array,
}

/// The columns and arrays one table's rows hold while a batch normalizes.
#[derive(Default)]
struct Table {
    /// Each column's path, its Arrow field (whose name the part replaces) and its values.
    columns: Vec<(ColumnPath, FieldRef, ArrayRef)>,
    /// Arrays within depth, which become child tables: their path in this table, their values
    /// and their depth.
    arrays: Vec<(Vec<Arc<str>>, ArrayRef, u8)>,
}

/// `batch`'s rows, of a stream normalized as `shape`, as rows of the stream's table and of its
/// child tables, the parents' parts before their children's.
pub(crate) fn normalize(batch: &RecordBatch, shape: &Shape) -> Result<Vec<Part>, ArrowError> {
    let ids = identity::root_ids(batch, &shape.key)?;
    let mut root = Table::default();
    for (field, column) in batch.schema().fields().iter().zip(batch.columns()) {
        let path = vec![Arc::from(field.name().as_str())];
        if shape.whole.contains(field.name().as_str()) {
            root.column(path, field, Arc::clone(column))?;
        } else {
            root.place(path, field, column, 1, shape.max_depth)?;
        }
    }
    let mut parts = Vec::new();
    let arrays = std::mem::take(&mut root.arrays);
    let root_rows =
        UInt32Array::from_iter_values(0..u32::try_from(batch.num_rows()).unwrap_or(u32::MAX));
    let lineage = Lineage {
        id: Arc::new(ids.clone()),
        root_row: Arc::new(root_rows.clone()),
        parent: None,
    };
    parts.push(root.part(Vec::new(), batch.num_rows(), lineage)?);
    let rows = Rows {
        path: &[],
        ids: &ids,
        roots: &ids,
        positions: &root_rows,
    };
    for (path, array, depth) in arrays {
        expand(&path, &array, depth, rows, shape.max_depth, &mut parts)?;
    }
    Ok(parts)
}

impl Table {
    fn column(
        &mut self,
        path: Vec<Arc<str>>,
        field: &FieldRef,
        array: ArrayRef,
    ) -> Result<(), ArrowError> {
        let path =
            ColumnPath::new(path).map_err(|error| ArrowError::SchemaError(error.to_string()))?;
        self.columns.push((path, Arc::clone(field), array));
        Ok(())
    }

    /// Places `array`, of `field`, the column at `path` whose values sit at `depth`, as
    /// [`placement`] says of `field`'s type.
    ///
    /// The field's type, not the array's, decides: a field nulled under an object with nulls
    /// may hold its runs decoded, and an object or array in runs stays a column all the same.
    fn place(
        &mut self,
        path: Vec<Arc<str>>,
        field: &FieldRef,
        array: &ArrayRef,
        depth: u8,
        max_depth: u8,
    ) -> Result<(), ArrowError> {
        match placement(Container::of_arrow(field.data_type()), depth, max_depth) {
            Placement::Fields => {
                let object = array.as_struct();
                for (inner, values) in object.fields().iter().zip(object.columns()) {
                    let mut inner_path = path.clone();
                    inner_path.push(Arc::from(inner.name().as_str()));
                    let values = within(values, object.nulls())?;
                    self.place(inner_path, inner, &values, depth + 1, max_depth)?;
                }
                Ok(())
            }
            Placement::Items => {
                self.arrays.push((path, Arc::clone(array), depth));
                Ok(())
            }
            Placement::Column => self.column(path, field, Arc::clone(array)),
        }
    }

    /// The part of `rows` rows at `path` this table holds.
    fn part(self, path: Vec<Arc<str>>, rows: usize, lineage: Lineage) -> Result<Part, ArrowError> {
        let mut columns = Vec::with_capacity(self.columns.len());
        let mut fields = Vec::with_capacity(self.columns.len());
        let mut arrays = Vec::with_capacity(self.columns.len());
        for (path, field, array) in self.columns {
            // The field keeps its metadata, such as the JSON and UUID extension names.
            let field = field
                .as_ref()
                .clone()
                .with_name(name(&path))
                .with_data_type(array.data_type().clone())
                .with_nullable(true);
            columns.push(path);
            fields.push(field);
            arrays.push(array);
        }
        let options = RecordBatchOptions::new().with_row_count(Some(rows));
        let batch =
            RecordBatch::try_new_with_options(Arc::new(Schema::new(fields)), arrays, &options)?;
        Ok(Part {
            path,
            columns,
            batch,
            lineage,
        })
    }
}

/// A unique name for a column at `path`: each segment's length and the segment.
fn name(path: &ColumnPath) -> String {
    let mut name = String::new();
    for segment in path.segments() {
        name.push_str(&segment.len().to_string());
        name.push(':');
        name.push_str(segment);
    }
    name
}

/// `values`, a field of an object, null wherever the object is.
///
/// A run-end encoded field holds no nulls of its own, so its values are decoded, then nulled, and
/// a union, which no table holds, is refused.
fn within(values: &ArrayRef, object: Option<&NullBuffer>) -> Result<ArrayRef, ArrowError> {
    let Some(object) = object else {
        return Ok(Arc::clone(values));
    };
    match values.data_type() {
        DataType::Union(..) => Err(ArrowError::InvalidArgumentError(
            "a union cannot be a column of a table".to_owned(),
        )),
        DataType::RunEndEncoded(..) => within(&decoded(values)?, Some(object)),
        _ => nullif(values.as_ref(), &BooleanArray::new(!object.inner(), None)),
    }
}

/// The item field of an array type, if `data_type` is one.
fn item_field(data_type: &DataType) -> Option<&Arc<ArrowField>> {
    match data_type {
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::FixedSizeList(item, _)
        | DataType::ListView(item)
        | DataType::LargeListView(item)
        | DataType::Map(item, _) => Some(item),
        _ => None,
    }
}

/// `array` as a `ListArray`: maps as arrays of their entries, other arrays cast.
pub(crate) fn as_list(array: &ArrayRef) -> Result<ListArray, ArrowError> {
    match array.data_type() {
        DataType::List(_) => Ok(array.as_list::<i32>().clone()),
        DataType::Map(entries, _) => {
            let map = array.as_map();
            ListArray::try_new(
                Arc::clone(entries),
                map.offsets().clone(),
                Arc::new(map.entries().clone()),
                map.nulls().cloned(),
            )
        }
        data_type => {
            let item = item_field(data_type).cloned().ok_or_else(|| {
                ArrowError::InvalidArgumentError(format!("{data_type} is not an array type"))
            })?;
            let cast = arrow_cast::cast(array, &DataType::List(item))?;
            Ok(cast.as_list::<i32>().clone())
        }
    }
}

/// Adds the child table at `path` holding the items of `array`, whose rows are `parents`, then its
/// own child tables.
///
/// Null and empty arrays hold no rows; a null item is a row.
fn expand(
    path: &[Arc<str>],
    array: &ArrayRef,
    depth: u8,
    parents: Rows<'_>,
    max_depth: u8,
    parts: &mut Vec<Part>,
) -> Result<(), ArrowError> {
    let list = as_list(array)?;
    let Some(items) = Items::of(&list) else {
        return Ok(());
    };
    let values = arrow_select::take::take(list.values(), &items.values, None)?;
    let parent_ids = arrow_select::take::take(parents.ids, &items.parents, None)?;
    let root_ids = arrow_select::take::take(parents.roots, &items.parents, None)?;
    let root_ids = root_ids.as_binary::<i32>().clone();
    let root_rows = arrow_select::take::take(parents.positions, &items.parents, None)?;
    let root_rows = root_rows.as_primitive::<UInt32Type>().clone();
    let ids = identity::child_ids(parent_ids.as_binary::<i32>(), &items.idx);
    let item = match list.data_type() {
        DataType::List(item) => Arc::clone(item),
        other => Arc::new(ArrowField::new(VALUE, other.clone(), true)),
    };
    let mut table = items_table(&item, &values, depth + 1, max_depth)?;
    let arrays = std::mem::take(&mut table.arrays);
    let lineage = Lineage {
        id: Arc::new(ids.clone()),
        root_row: Arc::new(root_rows.clone()),
        parent: Some(Parent {
            id: parent_ids,
            root: Arc::new(root_ids.clone()),
            idx: Arc::new(items.idx),
            path: parents.path.to_vec(),
            row: Arc::new(items.parents),
        }),
    };
    parts.push(table.part(path.to_vec(), values.len(), lineage)?);
    let rows = Rows {
        path,
        ids: &ids,
        roots: &root_ids,
        positions: &root_rows,
    };
    for (child, array, child_depth) in arrays {
        let child_path = [path, &child].concat();
        expand(&child_path, &array, child_depth, rows, max_depth, parts)?;
    }
    Ok(())
}

/// The items of an array column's non-null arrays: where each sits among the values, its row
/// and its position in its array.
struct Items {
    values: UInt32Array,
    parents: UInt32Array,
    idx: Int64Array,
}

impl Items {
    /// The items of `list`, or `None` where it holds none.
    fn of(list: &ListArray) -> Option<Self> {
        let offsets = list.value_offsets();
        let (mut parents, mut values, mut idx) = (Vec::new(), Vec::new(), Vec::new());
        for row in 0..list.len() {
            if list.is_null(row) {
                continue;
            }
            for (position, item) in (offsets[row]..offsets[row + 1]).enumerate() {
                parents.push(u32::try_from(row).unwrap_or(u32::MAX));
                values.push(u32::try_from(item).unwrap_or(u32::MAX));
                idx.push(i64::try_from(position).unwrap_or(i64::MAX));
            }
        }
        (!values.is_empty()).then(|| Self {
            values: UInt32Array::from(values),
            parents: UInt32Array::from(parents),
            idx: Int64Array::from(idx),
        })
    }
}

/// The child table of `values`, items of `item` at `depth`: placed as [`placement`] says of
/// `item`'s type, an array within depth a grandchild table under `value`, and anything else the
/// column `value`.
fn items_table(
    item: &FieldRef,
    values: &ArrayRef,
    depth: u8,
    max_depth: u8,
) -> Result<Table, ArrowError> {
    let mut table = Table::default();
    match placement(Container::of_arrow(item.data_type()), depth, max_depth) {
        Placement::Fields => {
            let object = values.as_struct();
            for (field, column) in object.fields().iter().zip(object.columns()) {
                let column = within(column, object.nulls())?;
                let path = vec![Arc::from(field.name().as_str())];
                table.place(path, field, &column, depth + 1, max_depth)?;
            }
        }
        Placement::Items => {
            table
                .arrays
                .push((vec![Arc::from(VALUE)], Arc::clone(values), depth));
        }
        Placement::Column => table.column(vec![Arc::from(VALUE)], item, Arc::clone(values))?,
    }
    Ok(table)
}

/// The column holding the items of an array of values that are not objects.
pub(crate) const VALUE: &str = "value";
