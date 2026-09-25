//! Normalizing nested data (spec §8.7): arrays become child tables at any depth, objects flatten
//! into one column per field, and every row carries its lineage.
//!
//! Normalizing works on Arrow batches, after JSON is shredded, so Arrow and JSON pushes normalize
//! alike. A container (an object or an array) nested deeper than the stream's `max_depth` stays
//! whole, and its table stores it as `Json`.

mod identity;
#[cfg(test)]
mod reference;
#[cfg(test)]
mod tests;

use std::collections::BTreeSet;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{
    Array, ArrayRef, BinaryArray, Int64Array, ListArray, RecordBatch, RecordBatchOptions,
    UInt32Array, make_array,
};
use arrow_buffer::NullBuffer;
use arrow_schema::{ArrowError, DataType, Field as ArrowField, Schema};
use rdlt_connector::ColumnPath;

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
}

/// The columns and arrays one table's rows hold while a batch normalizes.
#[derive(Default)]
struct Table {
    columns: Vec<(ColumnPath, ArrayRef)>,
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
            root.column(path, Arc::clone(column))?;
        } else {
            root.place(path, column, 1, shape.max_depth)?;
        }
    }
    let mut parts = Vec::new();
    let arrays = std::mem::take(&mut root.arrays);
    let lineage = Lineage {
        id: Arc::new(ids.clone()),
        parent: None,
    };
    parts.push(root.part(Vec::new(), batch.num_rows(), lineage)?);
    for (path, array, depth) in arrays {
        expand(
            &path,
            &array,
            depth,
            &ids,
            &ids,
            shape.max_depth,
            &mut parts,
        )?;
    }
    Ok(parts)
}

impl Table {
    fn column(&mut self, path: Vec<Arc<str>>, array: ArrayRef) -> Result<(), ArrowError> {
        let path =
            ColumnPath::new(path).map_err(|error| ArrowError::SchemaError(error.to_string()))?;
        self.columns.push((path, array));
        Ok(())
    }

    /// Places `array`, the column at `path` whose values sit at `depth`: an object within depth
    /// flattens into its fields, an array within depth waits to become a child table, and
    /// anything else is a column.
    fn place(
        &mut self,
        path: Vec<Arc<str>>,
        array: &ArrayRef,
        depth: u8,
        max_depth: u8,
    ) -> Result<(), ArrowError> {
        if depth > max_depth {
            return self.column(path, Arc::clone(array));
        }
        match array.data_type() {
            DataType::Struct(_) => {
                let object = array.as_struct();
                for (field, values) in object.fields().iter().zip(object.columns()) {
                    let mut field_path = path.clone();
                    field_path.push(Arc::from(field.name().as_str()));
                    let values = within(values, object.nulls())?;
                    self.place(field_path, &values, depth + 1, max_depth)?;
                }
                Ok(())
            }
            data_type if item_field(data_type).is_some() => {
                self.arrays.push((path, Arc::clone(array), depth));
                Ok(())
            }
            _ => self.column(path, Arc::clone(array)),
        }
    }

    /// The part of `rows` rows at `path` this table holds.
    fn part(self, path: Vec<Arc<str>>, rows: usize, lineage: Lineage) -> Result<Part, ArrowError> {
        let (columns, arrays): (Vec<ColumnPath>, Vec<ArrayRef>) = self.columns.into_iter().unzip();
        let fields: Vec<ArrowField> = columns
            .iter()
            .zip(&arrays)
            .map(|(path, array)| ArrowField::new(name(path), array.data_type().clone(), true))
            .collect();
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
fn within(values: &ArrayRef, object: Option<&NullBuffer>) -> Result<ArrayRef, ArrowError> {
    let Some(object) = object else {
        return Ok(Arc::clone(values));
    };
    let nulls = NullBuffer::union(Some(object), values.nulls());
    let data = values.to_data().into_builder().nulls(nulls).build()?;
    Ok(make_array(data))
}

/// The item field of an array type, if `data_type` is one.
fn item_field(data_type: &DataType) -> Option<&Arc<ArrowField>> {
    match data_type {
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::FixedSizeList(item, _)
        | DataType::Map(item, _) => Some(item),
        _ => None,
    }
}

/// `array` as a `ListArray`: maps as arrays of their entries, other arrays cast.
fn as_list(array: &ArrayRef) -> Result<ListArray, ArrowError> {
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

/// Adds the child table at `path` holding the items of `array`, whose rows' ids are `parents` and
/// whose roots' ids are `roots`, then its own child tables.
///
/// Null and empty arrays hold no rows; a null item is a row.
fn expand(
    path: &[Arc<str>],
    array: &ArrayRef,
    depth: u8,
    parents: &BinaryArray,
    roots: &BinaryArray,
    max_depth: u8,
    parts: &mut Vec<Part>,
) -> Result<(), ArrowError> {
    let list = as_list(array)?;
    let Some(items) = Items::of(&list) else {
        return Ok(());
    };
    let values = arrow_select::take::take(list.values(), &items.values, None)?;
    let parent_ids = arrow_select::take::take(parents, &items.parents, None)?;
    let root_ids = arrow_select::take::take(roots, &items.parents, None)?;
    let root_ids = root_ids.as_binary::<i32>().clone();
    let ids = identity::child_ids(parent_ids.as_binary::<i32>(), &items.idx);
    let mut table = items_table(&values, depth + 1, max_depth)?;
    let arrays = std::mem::take(&mut table.arrays);
    let lineage = Lineage {
        id: Arc::new(ids.clone()),
        parent: Some(Parent {
            id: parent_ids,
            root: Arc::new(root_ids.clone()),
            idx: Arc::new(items.idx),
        }),
    };
    parts.push(table.part(path.to_vec(), values.len(), lineage)?);
    for (child, array, child_depth) in arrays {
        let child_path = [path, &child].concat();
        let (ids, roots) = (&ids, &root_ids);
        expand(
            &child_path,
            &array,
            child_depth,
            ids,
            roots,
            max_depth,
            parts,
        )?;
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

/// The child table of `values`, items at `depth`: an object within depth flattens into columns,
/// an array within depth becomes a grandchild table under `value`, and anything else is the column
/// `value`.
fn items_table(values: &ArrayRef, depth: u8, max_depth: u8) -> Result<Table, ArrowError> {
    let mut table = Table::default();
    match values.data_type() {
        DataType::Struct(_) if depth <= max_depth => {
            let object = values.as_struct();
            for (field, column) in object.fields().iter().zip(object.columns()) {
                let column = within(column, object.nulls())?;
                let path = vec![Arc::from(field.name().as_str())];
                table.place(path, &column, depth + 1, max_depth)?;
            }
        }
        data_type if item_field(data_type).is_some() && depth <= max_depth => {
            table
                .arrays
                .push((vec![Arc::from(VALUE)], Arc::clone(values), depth));
        }
        _ => table.column(vec![Arc::from(VALUE)], Arc::clone(values))?,
    }
    Ok(table)
}

/// The column holding the items of an array of values that are not objects.
pub(crate) const VALUE: &str = "value";
