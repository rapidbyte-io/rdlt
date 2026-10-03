//! The columns of objects: one per field seen, in the order first seen, growing with the rows,
//! each growth charged before it.

use std::collections::BTreeMap;
use std::sync::Arc;

use arrow_array::{ArrayRef, StructArray};
use arrow_buffer::NullBufferBuilder;
use arrow_schema::Fields;

use super::{Column, count, rows_of};
use crate::limits::QUOTED_BYTES;
use crate::shred::ShredError;
use crate::shred::meter::{Columns, Meter, Over};
use crate::shred::observe::Shape;

/// The columns of objects: one per field seen, in the order first seen.
pub(crate) struct Record {
    names: Vec<Arc<str>>,
    index: BTreeMap<Arc<str>, usize>,
    pub(super) columns: Vec<Column>,
    /// The row that last wrote each field, so a row's missing fields are nulled and repeated keys
    /// are caught.
    written: Vec<usize>,
    rows: usize,
    nulls: NullBufferBuilder,
    /// How many rows the builders are sized and charged for.
    room: usize,
    /// How many keys were searched for rather than found at their hint.
    #[cfg(test)]
    searches: usize,
}

impl Record {
    /// Columns for objects of `shape`, sized for `capacity` rows and charged to `meter`.
    ///
    /// # Errors
    ///
    /// [`Over`] where the meter has no room for the builders.
    pub(crate) fn new(shape: &Shape, capacity: usize, meter: &Meter) -> Result<Self, Over> {
        let mut record = Self::empty(capacity);
        for (name, observed) in shape.fields() {
            record.index.insert(Arc::clone(name), record.names.len());
            record.names.push(Arc::clone(name));
            record
                .columns
                .push(Column::new(observed, 0, capacity, meter)?);
            record.written.push(usize::MAX);
        }
        Ok(record)
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
            room: capacity,
            #[cfg(test)]
            searches: 0,
        }
    }

    /// How many keys were searched for rather than found at their hint.
    #[cfg(test)]
    pub(crate) fn searches(&self) -> usize {
        self.searches
    }

    /// How many rows the builders are sized for: what a column made now is sized for.
    pub(crate) fn capacity(&self) -> usize {
        self.room
    }

    /// The position of the field `name`, trying `hint` first, adding the field when new and
    /// counting it among `columns`: objects usually repeat their keys' order, so the field after
    /// the last one found is most often next.
    ///
    /// # Errors
    ///
    /// [`ShredError::TooManyColumns`] for a field past the limit, before the rest of the chunk is
    /// read.
    pub(crate) fn position(
        &mut self,
        name: &str,
        hint: usize,
        columns: &Columns,
    ) -> Result<usize, ShredError> {
        if self
            .names
            .get(hint)
            .is_some_and(|field| field.as_ref() == name)
        {
            return Ok(hint);
        }
        #[cfg(test)]
        {
            self.searches += 1;
        }
        if let Some(&position) = self.index.get(name) {
            return Ok(position);
        }
        columns.add()?;
        let name: Arc<str> = name.into();
        self.index.insert(Arc::clone(&name), self.names.len());
        self.names.push(name);
        self.columns.push(Column::Null(self.rows));
        self.written.push(usize::MAX);
        Ok(self.names.len() - 1)
    }

    /// The column of the field at `position`, for the row being appended; a field the row already
    /// wrote is a repeated key.
    pub(crate) fn field(&mut self, position: usize) -> Result<&mut Column, ShredError> {
        if self.written[position] == self.rows {
            let key = rdlt_connector::text::shown(&self.names[position], QUOTED_BYTES);
            return Err(ShredError::DuplicateKey(key));
        }
        self.written[position] = self.rows;
        Ok(&mut self.columns[position])
    }

    /// Makes room for one more row: past the rows the builders were sized for, they grow as
    /// builders do, doubling, charged to `meter` first.
    fn grow(&mut self, meter: &Meter) -> Result<(), Over> {
        if self.rows < self.room {
            return Ok(());
        }
        // The builders double, charged for the rows they grow by; the copy they grow from is
        // a builder's spare capacity while it grows.
        let room = self.room.saturating_mul(2).max(1);
        let width = self
            .columns
            .iter()
            .map(Column::width)
            .fold(0, u64::saturating_add);
        let bits = self
            .columns
            .iter()
            .map(Column::bits)
            .fold(1, u64::saturating_add);
        meter.charge(rows_of(count(room - self.room), width, bits))?;
        self.room = room;
        Ok(())
    }

    /// Ends the row being appended, which wrote `fields` fields: the fields it lacked get nulls.
    ///
    /// # Errors
    ///
    /// [`Over`] where the meter has no room for the row.
    pub(crate) fn end_row(&mut self, fields: usize, meter: &Meter) -> Result<(), Over> {
        self.grow(meter)?;
        if fields != self.columns.len() {
            for (column, written) in self.columns.iter_mut().zip(&self.written) {
                if *written != self.rows {
                    column.null(meter)?;
                }
            }
        }
        self.rows += 1;
        self.nulls.append_non_null();
        Ok(())
    }

    /// Appends a null object.
    ///
    /// # Errors
    ///
    /// [`Over`] where the meter has no room for the row.
    pub(super) fn null(&mut self, meter: &Meter) -> Result<(), Over> {
        self.grow(meter)?;
        for column in &mut self.columns {
            column.null(meter)?;
        }
        self.rows += 1;
        self.nulls.append_null();
        Ok(())
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

    /// The objects appended, as a struct column.
    pub(super) fn finish_struct(mut self) -> Result<StructArray, ShredError> {
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
