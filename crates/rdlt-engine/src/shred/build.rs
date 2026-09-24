//! Arrow columns built as a chunk is parsed, typed by the values as they arrive.
//!
//! A column starts as nulls and takes the type of its first value. A later value of a wider type
//! the column can take without loss (a float after exact integers, a large unsigned integer after
//! integers) converts it. Any other makes the column `Json`, which it cannot build without the
//! values it already took, so the column stops building and the chunk is built again once the
//! push's shape is known.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::builder::{
    BooleanBuilder, Decimal128Builder, Float64Builder, Int64Builder, StringBuilder,
};
use arrow_array::{ArrayRef, ListArray, NullArray, StructArray};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};
use arrow_schema::Fields;

use super::ShredError;
use super::observe::{Observed, Shape};

/// A scalar JSON value.
#[derive(Clone, Copy, Debug)]
pub(crate) enum Scalar<'a> {
    Bool(bool),
    Int(i64),
    /// An integer above the signed 64-bit range.
    Wide(u64),
    Float(f64),
    Text(&'a str),
}

impl Scalar<'_> {
    /// What the value is observed as.
    pub(crate) fn observed(self) -> Observed {
        match self {
            Self::Bool(_) => Observed::Bool,
            Self::Int(value) => Observed::integer(value),
            Self::Wide(_) => Observed::Wide,
            Self::Float(_) => Observed::Float,
            Self::Text(_) => Observed::Text,
        }
    }
}

/// A column being built, typed by the values it has held.
pub(crate) enum Column {
    /// Only nulls so far: this many.
    Null(usize),
    Bool(BooleanBuilder),
    Int {
        builder: Int64Builder,
        /// Whether every integer is exact as a 64-bit float.
        exact: bool,
    },
    Wide(Decimal128Builder),
    Float(Float64Builder),
    Text(StringBuilder),
    Json(StringBuilder),
    Struct(Box<Record>),
    List(Box<List>),
    /// A column whose values joined to `Json` after it built others: its values are only checked
    /// from then on.
    Spoiled,
}

/// The columns of objects: one per field seen, in the order first seen.
pub(crate) struct Record {
    names: Vec<Arc<str>>,
    index: BTreeMap<Arc<str>, usize>,
    columns: Vec<Column>,
    /// The row that last wrote each field, so a row's missing fields are nulled and repeated keys
    /// are caught.
    written: Vec<usize>,
    rows: usize,
    nulls: NullBufferBuilder,
    /// How many rows the builders are sized for.
    capacity: usize,
    /// How many keys were searched for rather than found at their hint.
    #[cfg(test)]
    searches: usize,
}

/// The column of arrays: offsets into one item column.
pub(crate) struct List {
    offsets: Vec<i32>,
    nulls: NullBufferBuilder,
    item: Column,
}

impl Column {
    /// A column for values observed as `observed`, holding `nulls` nulls, sized for `capacity`.
    pub(crate) fn new(observed: &Observed, nulls: usize, capacity: usize) -> Self {
        let capacity = capacity.max(nulls);
        let mut column = match observed {
            Observed::Null => return Self::Null(nulls),
            Observed::Bool => Self::Bool(BooleanBuilder::with_capacity(capacity)),
            Observed::Int { exact } => Self::Int {
                builder: Int64Builder::with_capacity(capacity),
                exact: *exact,
            },
            Observed::Wide => Self::Wide(wide(capacity)),
            Observed::Float => Self::Float(Float64Builder::with_capacity(capacity)),
            Observed::Text => Self::Text(StringBuilder::with_capacity(capacity, capacity * 8)),
            Observed::Json => Self::Json(StringBuilder::with_capacity(capacity, capacity * 16)),
            Observed::Object(shape) => Self::Struct(Box::new(Record::new(shape, capacity))),
            Observed::Array(item) => Self::List(Box::new(List {
                offsets: Vec::with_capacity(capacity + 1),
                nulls: NullBufferBuilder::new(capacity),
                item: Self::new(item, 0, capacity),
            })),
        };
        if let Self::List(list) = &mut column {
            list.offsets.push(0);
        }
        for _ in 0..nulls {
            column.null();
        }
        column
    }

    /// What the column's values are observed as.
    pub(crate) fn observed(&self) -> Observed {
        match self {
            Self::Null(_) => Observed::Null,
            Self::Bool(_) => Observed::Bool,
            Self::Int { exact, .. } => Observed::Int { exact: *exact },
            Self::Wide(_) => Observed::Wide,
            Self::Float(_) => Observed::Float,
            Self::Text(_) => Observed::Text,
            Self::Struct(record) => Observed::Object(record.shape()),
            Self::List(list) => Observed::Array(Box::new(list.item.observed())),
            Self::Json(_) | Self::Spoiled => Observed::Json,
        }
    }

    /// Appends a null.
    pub(crate) fn null(&mut self) {
        match self {
            Self::Null(rows) => *rows += 1,
            Self::Bool(builder) => builder.append_null(),
            Self::Int { builder, .. } => builder.append_null(),
            Self::Wide(builder) => builder.append_null(),
            Self::Float(builder) => builder.append_null(),
            Self::Text(builder) | Self::Json(builder) => builder.append_null(),
            Self::Struct(record) => record.null(),
            Self::List(list) => list.null(),
            Self::Spoiled => {}
        }
    }

    /// Appends `value`, converting the column when it widens it without loss; returns whether it
    /// fitted.
    pub(crate) fn scalar(&mut self, value: Scalar<'_>, capacity: usize) -> bool {
        match (&*self, value) {
            (Self::Null(nulls), _) => *self = Self::new(&value.observed(), *nulls, capacity),
            (Self::Int { exact: true, .. }, Scalar::Float(_)) => self.widen(&Observed::Float),
            (Self::Int { .. }, Scalar::Wide(_)) => self.widen(&Observed::Wide),
            _ => {}
        }
        match (self, value) {
            (Self::Bool(builder), Scalar::Bool(value)) => builder.append_value(value),
            (Self::Int { builder, exact }, Scalar::Int(value)) => {
                *exact &= Observed::integer(value) == Observed::Int { exact: true };
                builder.append_value(value);
            }
            (Self::Wide(builder), Scalar::Int(value)) => builder.append_value(i128::from(value)),
            (Self::Wide(builder), Scalar::Wide(value)) => builder.append_value(i128::from(value)),
            (Self::Float(builder), Scalar::Float(value)) => builder.append_value(value),
            #[expect(
                clippy::cast_precision_loss,
                reason = "the integer is exact as a float"
            )]
            (Self::Float(builder), Scalar::Int(value))
                if Observed::integer(value) == (Observed::Int { exact: true }) =>
            {
                builder.append_value(value as f64);
            }
            (Self::Text(builder), Scalar::Text(value)) => builder.append_value(value),
            _ => return false,
        }
        true
    }

    /// Stops building: a value joined the column's type to `Json`.
    pub(crate) fn spoil(&mut self) {
        *self = Self::Spoiled;
    }

    /// Converts the integers built so far to the wider `observed`, a float or a wide integer.
    #[expect(
        clippy::cast_precision_loss,
        reason = "the integers are exact as floats"
    )]
    fn widen(&mut self, observed: &Observed) {
        let Self::Int { builder, .. } = self else {
            return;
        };
        let integers = builder.finish();
        let capacity = builder.capacity().max(integers.len());
        *self = if *observed == Observed::Float {
            let mut floats = Float64Builder::with_capacity(capacity);
            integers
                .iter()
                .for_each(|value| floats.append_option(value.map(|v| v as f64)));
            Self::Float(floats)
        } else {
            let mut wide = wide(capacity);
            integers
                .iter()
                .for_each(|value| wide.append_option(value.map(i128::from)));
            Self::Wide(wide)
        };
    }

    /// The column built.
    pub(crate) fn finish(self) -> Result<ArrayRef, ShredError> {
        let array: ArrayRef = match self {
            Self::Null(rows) => Arc::new(NullArray::new(rows)),
            Self::Bool(mut builder) => Arc::new(builder.finish()),
            Self::Int { mut builder, .. } => Arc::new(builder.finish()),
            Self::Wide(mut builder) => Arc::new(builder.finish()),
            Self::Float(mut builder) => Arc::new(builder.finish()),
            Self::Text(mut builder) | Self::Json(mut builder) => Arc::new(builder.finish()),
            Self::Struct(record) => Arc::new(record.finish_struct()?),
            Self::List(list) => list.finish()?,
            Self::Spoiled => {
                return Err(ShredError::Internal(
                    "finishing a column never built".to_owned(),
                ));
            }
        };
        Ok(array)
    }
}

/// A builder of 20-digit integers, the unsigned 64-bit range.
fn wide(capacity: usize) -> Decimal128Builder {
    Decimal128Builder::with_capacity(capacity)
        .with_precision_and_scale(20, 0)
        .expect("20 digits fit a 128-bit decimal")
}

impl Record {
    /// Columns for objects of `shape`, sized for `capacity` rows.
    pub(crate) fn new(shape: &Shape, capacity: usize) -> Self {
        let mut record = Self::empty(capacity);
        for (name, observed) in shape.fields() {
            record.index.insert(Arc::clone(name), record.names.len());
            record.names.push(Arc::clone(name));
            record.columns.push(Column::new(observed, 0, capacity));
            record.written.push(usize::MAX);
        }
        record
    }

    /// No columns yet, sized for `capacity` rows.
    pub(crate) fn empty(capacity: usize) -> Self {
        Self {
            names: Vec::new(),
            index: BTreeMap::new(),
            columns: Vec::new(),
            written: Vec::new(),
            rows: 0,
            nulls: NullBufferBuilder::new(capacity),
            capacity,
            #[cfg(test)]
            searches: 0,
        }
    }

    /// How many keys were searched for rather than found at their hint.
    #[cfg(test)]
    pub(crate) fn searches(&self) -> usize {
        self.searches
    }

    /// How many rows the builders are sized for.
    pub(crate) fn capacity(&self) -> usize {
        self.capacity
    }

    /// The position of the field `name`, trying `hint` first, adding the field when new: objects
    /// usually repeat their keys' order, so the field after the last one found is most often next.
    pub(crate) fn position(&mut self, name: &str, hint: usize) -> usize {
        if self
            .names
            .get(hint)
            .is_some_and(|field| field.as_ref() == name)
        {
            return hint;
        }
        #[cfg(test)]
        {
            self.searches += 1;
        }
        if let Some(&position) = self.index.get(name) {
            return position;
        }
        let name: Arc<str> = name.into();
        self.index.insert(Arc::clone(&name), self.names.len());
        self.names.push(name);
        self.columns.push(Column::Null(self.rows));
        self.written.push(usize::MAX);
        self.names.len() - 1
    }

    /// The column of the field at `position`, for the row being appended; a field the row already
    /// wrote is a repeated key.
    pub(crate) fn field(&mut self, position: usize) -> Result<&mut Column, ShredError> {
        if self.written[position] == self.rows {
            return Err(ShredError::DuplicateKey(self.names[position].to_string()));
        }
        self.written[position] = self.rows;
        Ok(&mut self.columns[position])
    }

    /// Ends the row being appended, which wrote `fields` fields: the fields it lacked get nulls.
    pub(crate) fn end_row(&mut self, fields: usize) {
        if fields != self.columns.len() {
            for (column, written) in self.columns.iter_mut().zip(&self.written) {
                if *written != self.rows {
                    column.null();
                }
            }
        }
        self.rows += 1;
        self.nulls.append_non_null();
    }

    fn null(&mut self) {
        for column in &mut self.columns {
            column.null();
        }
        self.rows += 1;
        self.nulls.append_null();
    }

    /// What the objects appended are observed as.
    pub(crate) fn shape(&self) -> Shape {
        let mut shape = Shape::default();
        for (name, column) in self.names.iter().zip(&self.columns) {
            shape.push(Arc::clone(name), column.observed());
        }
        shape
    }

    /// The columns built, in the order first seen.
    pub(crate) fn finish_columns(self) -> Result<Vec<ArrayRef>, ShredError> {
        self.columns.into_iter().map(Column::finish).collect()
    }

    fn finish_struct(mut self) -> Result<StructArray, ShredError> {
        let fields: Fields = self
            .shape()
            .logical_fields()
            .iter()
            .map(rdlt_connector::Field::to_arrow)
            .collect();
        let rows = self.rows;
        let nulls = self.nulls.finish();
        if fields.is_empty() {
            return Ok(StructArray::new_empty_fields(rows, nulls));
        }
        let columns = self.finish_columns()?;
        StructArray::try_new(fields, columns, nulls)
            .map_err(|error| ShredError::Internal(format!("building a struct column: {error}")))
    }
}

impl List {
    /// The column the items are appended to.
    pub(crate) fn item(&mut self) -> &mut Column {
        &mut self.item
    }

    /// Ends an array of `items` items.
    pub(crate) fn end_row(&mut self, items: usize) -> Result<(), ShredError> {
        let end = i32::try_from(items)
            .ok()
            .and_then(|items| self.offsets.last().and_then(|last| last.checked_add(items)))
            .ok_or(ShredError::TooLarge)?;
        self.offsets.push(end);
        self.nulls.append_non_null();
        Ok(())
    }

    fn null(&mut self) {
        self.offsets.push(*self.offsets.last().unwrap_or(&0));
        self.nulls.append_null();
    }

    fn finish(mut self) -> Result<ArrayRef, ShredError> {
        let field = Arc::new(
            rdlt_connector::Field::new("item", self.item.observed().logical_type(), true)
                .to_arrow(),
        );
        let values = self.item.finish()?;
        let offsets = OffsetBuffer::new(self.offsets.into());
        ListArray::try_new(field, offsets, values, self.nulls.finish())
            .map(|array| Arc::new(array) as ArrayRef)
            .map_err(|error| ShredError::Internal(format!("building a list column: {error}")))
    }
}
