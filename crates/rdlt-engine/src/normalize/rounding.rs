//! The columns of 64-bit integers that hold, in each table a normalized flush loads, a value a
//! 64-bit float would round, judged where the flush's batches hold its rows: each column walked
//! as normalizing places it, through the rows each object and array holds, with nothing split,
//! decoded or held a row.

use std::collections::{BTreeMap, BTreeSet};
use std::rc::Rc;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::{Array, RecordBatch};
use arrow_buffer::{ArrowNativeType, NullBuffer};
use arrow_schema::{DataType, FieldRef};
use rdlt_connector::{ColumnPath, Field, LogicalType, SchemaError, TableSchema};

use super::placement::{Container, Placement, placement};
use super::{Shape, VALUE};
use crate::named::{Offsets, Rows, Views};
use crate::table::rounds_at;

/// The columns of 64-bit integers holding a value a float would round, by the path of their
/// table below the stream's.
pub(crate) type Rounding = BTreeMap<Vec<Arc<str>>, BTreeSet<ColumnPath>>;

/// Adds to `rounding` the columns of 64-bit integers that hold, in `batches` normalized as
/// `shape`, a value a 64-bit float would round.
///
/// # Errors
///
/// Where a column normalizing puts in a table has no table schema: its type has no logical type,
/// or it nests deeper than a table's columns may.
pub(crate) fn judge(
    batches: &[RecordBatch],
    shape: &Shape,
    rounding: &mut Rounding,
) -> Result<(), SchemaError> {
    let mut walk = Walk {
        table: &[],
        max_depth: shape.max_depth,
        rounding,
        checked: false,
    };
    for batch in batches {
        let rows = Rows::All(batch.num_rows());
        let schema = batch.schema();
        for (field, column) in schema.fields().iter().zip(batch.columns()) {
            let path = vec![Arc::from(field.name().as_str())];
            if shape.whole.contains(field.name().as_str()) {
                walk.column(path, field, column.as_ref(), &rows)?;
            } else {
                walk.place(path, field, column.as_ref(), &rows, 1)?;
            }
        }
    }
    Ok(())
}

/// One table's columns, walked as normalizing places them.
struct Walk<'a> {
    /// The table's path below the stream's table.
    table: &'a [Arc<str>],
    /// Containers nested deeper than this stay whole.
    max_depth: u8,
    /// The columns found rounding so far, in every table.
    rounding: &'a mut Rounding,
    /// Whether the table schema of every column below is checked already, so only values are
    /// read.
    checked: bool,
}

impl Walk<'_> {
    /// The same walk, in the table at `table`.
    fn at<'b>(&'b mut self, table: &'b [Arc<str>]) -> Walk<'b> {
        Walk {
            table,
            max_depth: self.max_depth,
            rounding: self.rounding,
            checked: self.checked,
        }
    }

    /// The same walk, reading only values: the table schemas below are checked.
    fn reading(&mut self) -> Walk<'_> {
        Walk {
            table: self.table,
            max_depth: self.max_depth,
            rounding: self.rounding,
            checked: true,
        }
    }

    /// Judges `array`, of `field`, the column at `path` whose values sit at `depth`, at the rows
    /// `rows` names.
    fn place(
        &mut self,
        path: Vec<Arc<str>>,
        field: &FieldRef,
        array: &dyn Array,
        rows: &Rows<'_>,
        depth: u8,
    ) -> Result<(), SchemaError> {
        match placement(
            Container::of_arrow(array.data_type()),
            depth,
            self.max_depth,
        ) {
            Placement::Fields => {
                let object = array.as_struct();
                let valid = valid(rows, object.nulls());
                for (inner, values) in object.fields().iter().zip(object.columns()) {
                    let mut inner_path = path.clone();
                    inner_path.push(Arc::from(inner.name().as_str()));
                    self.place(inner_path, inner, values.as_ref(), &valid, depth + 1)?;
                }
                Ok(())
            }
            Placement::Items => {
                let table = [self.table, path.as_slice()].concat();
                self.at(&table).items(array, rows, depth + 1)
            }
            Placement::Column => self.column(path, field, array, rows),
        }
    }

    /// Judges the column at `path` holding `array`, of `field`, at the rows `rows` names.
    fn column(
        &mut self,
        path: Vec<Arc<str>>,
        field: &FieldRef,
        array: &dyn Array,
        rows: &Rows<'_>,
    ) -> Result<(), SchemaError> {
        if !self.checked {
            // A part holding the column must have a table schema, whether or not its rows load.
            TableSchema::new(vec![Field::from_arrow(field)?])?;
        }
        // Only a column of 64-bit integers holding a value a float rounds is typed: typing every
        // column again for each row a view reads alone would read its field each time.
        if integers(array.data_type())
            && rounds_at(array, rows)
            && *Field::from_arrow(field)?.logical_type() == LogicalType::Int64
        {
            let path = ColumnPath::new(path).expect("a column's path holds its own name");
            self.rounding
                .entry(self.table.to_vec())
                .or_default()
                .insert(path);
        }
        Ok(())
    }

    /// Judges this child table, holding the items, at `depth`, of the rows `rows` names of
    /// `array`, an array of any layout or a map.
    fn items(&mut self, array: &dyn Array, rows: &Rows<'_>, depth: u8) -> Result<(), SchemaError> {
        let valid = Rc::new(valid(rows, array.nulls()));
        let (item, values, named) = match array.data_type() {
            DataType::List(item) => {
                let list = array.as_list::<i32>();
                let named = Rows::Items(valid, Offsets::Small(list.offsets()));
                (item, list.values().as_ref(), named)
            }
            DataType::LargeList(item) => {
                let list = array.as_list::<i64>();
                let named = Rows::Items(valid, Offsets::Large(list.offsets()));
                (item, list.values().as_ref(), named)
            }
            DataType::Map(item, _) => {
                let map = array.as_map();
                let named = Rows::Items(valid, Offsets::Small(map.offsets()));
                (item, map.entries() as &dyn Array, named)
            }
            DataType::FixedSizeList(item, _) => {
                let list = array.as_fixed_size_list();
                let named = Rows::Fixed(valid, list.value_length().as_usize());
                (item, list.values().as_ref(), named)
            }
            DataType::ListView(item) => {
                let views = array.as_list_view::<i32>();
                let spans = Views::Small(views.offsets(), views.sizes());
                let viewed = Viewed {
                    item,
                    values: views.values().as_ref(),
                    views: spans,
                };
                return viewed.judge(self, &valid, depth);
            }
            DataType::LargeListView(item) => {
                let views = array.as_list_view::<i64>();
                let spans = Views::Large(views.offsets(), views.sizes());
                let viewed = Viewed {
                    item,
                    values: views.values().as_ref(),
                    views: spans,
                };
                return viewed.judge(self, &valid, depth);
            }
            // Normalizing makes child tables of no other type.
            _ => return Ok(()),
        };
        let named = Rows::Clamped(Rc::new(named), values.len());
        self.item_table(item, values, &named, depth)
    }

    /// Judges this child table, of the items `rows` names of `values`, of `item`, at `depth`:
    /// an object within depth flattens into its fields, an array within depth is a grandchild
    /// table under `value`, and anything else is the column `value`.
    fn item_table(
        &mut self,
        item: &FieldRef,
        values: &dyn Array,
        rows: &Rows<'_>,
        depth: u8,
    ) -> Result<(), SchemaError> {
        match placement(
            Container::of_arrow(values.data_type()),
            depth,
            self.max_depth,
        ) {
            Placement::Fields => {
                let object = values.as_struct();
                let valid = valid(rows, object.nulls());
                for (field, column) in object.fields().iter().zip(object.columns()) {
                    let path = vec![Arc::from(field.name().as_str())];
                    self.place(path, field, column.as_ref(), &valid, depth + 1)?;
                }
                Ok(())
            }
            Placement::Items => {
                let grandchild: Vec<Arc<str>> = self
                    .table
                    .iter()
                    .cloned()
                    .chain([Arc::from(VALUE)])
                    .collect();
                self.at(&grandchild).items(values, rows, depth + 1)
            }
            Placement::Column => self.column(vec![Arc::from(VALUE)], item, values, rows),
        }
    }
}

/// A list view's items, and the spans its rows name.
struct Viewed<'a> {
    item: &'a FieldRef,
    values: &'a dyn Array,
    views: Views<'a>,
}

impl Viewed<'_> {
    /// Judges, as `walk`'s child table, the items the `valid` rows name.
    ///
    /// Spans in row order are read as a list's are; others a row at a time, so the rows each
    /// level maps stay in order. Order is judged over the rows named alone, so a row read alone
    /// reads only the rows below that it names.
    fn judge(&self, walk: &mut Walk<'_>, valid: &Rows<'_>, depth: u8) -> Result<(), SchemaError> {
        let values = self.values;
        if self.views.ordered(valid) {
            let viewed = Rows::Viewed(Rc::new(valid.clone()), self.views);
            let named = Rows::Clamped(Rc::new(viewed), values.len());
            return walk.item_table(self.item, values, &named, depth);
        }
        // The columns the items make follow from their type alone: they are checked once, and
        // each row's items only read.
        if !walk.checked {
            walk.item_table(self.item, values, &Rows::All(0), depth)?;
        }
        let mut reading = walk.reading();
        for row in valid.ranges().flatten() {
            let span = self.views.span(row);
            if span.is_empty() {
                continue;
            }
            let named = Rows::Clamped(Rc::new(Rows::Ranges(Rc::new(vec![span]))), values.len());
            reading.item_table(self.item, values, &named, depth)?;
        }
        Ok(())
    }
}

/// Whether an array of `data_type` holds 64-bit integers, in any encoding.
fn integers(data_type: &DataType) -> bool {
    match data_type {
        DataType::Int64 => true,
        DataType::Dictionary(_, values) => integers(values),
        DataType::RunEndEncoded(_, values) => integers(values.data_type()),
        _ => false,
    }
}

/// The rows of `rows` an array whose null buffer is `nulls` holds valid.
fn valid<'a>(rows: &Rows<'a>, nulls: Option<&NullBuffer>) -> Rows<'a> {
    match nulls {
        Some(nulls) => Rows::Valid(Rc::new(rows.clone()), nulls.clone()),
        None => rows.clone(),
    }
}

#[cfg(test)]
mod tests;
