//! The column of arrays: offsets into one item column, whose builders grow with the items, each
//! growth charged before it.

use std::sync::Arc;

use arrow_array::{ArrayRef, ListArray};
use arrow_buffer::{NullBufferBuilder, OffsetBuffer};

use super::{Column, count, rows_of};
use crate::shred::ShredError;
use crate::shred::meter::{Meter, Over};
use crate::shred::observe::Observed;

/// The column of arrays: offsets into one item column.
pub(crate) struct List {
    offsets: Vec<i32>,
    nulls: NullBufferBuilder,
    item: Column,
    /// How many items the arrays hold so far.
    items: usize,
    /// How many items the item column's builders are sized and charged for.
    room: usize,
}

impl List {
    /// A column of arrays of `item`, sized for `capacity` arrays and `items` items, charged to
    /// `meter`; the item column, which holds nulls only while no item was seen, is sized for at
    /// least `capacity` items once it holds one.
    pub(super) fn new(
        item: &Observed,
        capacity: usize,
        items: usize,
        meter: &Meter,
    ) -> Result<Self, Over> {
        let mut offsets = Vec::with_capacity(capacity + 1);
        offsets.push(0);
        Ok(Self {
            offsets,
            nulls: NullBufferBuilder::new(capacity),
            item: Column::new(item, 0, items, meter)?,
            items: 0,
            room: if *item == Observed::Null {
                capacity.max(items)
            } else {
                items
            },
        })
    }

    /// What the arrays are observed as: the join of their items, and how many there are.
    pub(super) fn observed(&self) -> Observed {
        Observed::Array(Box::new(self.item.observed()), count(self.items))
    }

    /// The column the next item is appended to, made room for, and how many items a column it
    /// makes is sized for: past the items its builders were sized for, they grow as builders
    /// do, doubling, charged to `meter` first.
    ///
    /// # Errors
    ///
    /// [`Over`] where the meter has no room for the item.
    pub(crate) fn item(&mut self, meter: &Meter) -> Result<(&mut Column, usize), Over> {
        if self.items >= self.room {
            // The item column doubles, charged for the items it grows by; one holding nulls only
            // takes nothing yet, and is charged for what it is sized for once made.
            let room = self.room.saturating_mul(2).max(1);
            let (width, bits) = (self.item.width(), self.item.bits());
            meter.charge(rows_of(count(room - self.room), width, bits))?;
            self.room = room;
        }
        self.items += 1;
        Ok((&mut self.item, self.room))
    }

    /// Ends an array of `items` items.
    ///
    /// # Errors
    ///
    /// [`ShredError::TooLarge`] where the items pass what a list's offsets hold.
    pub(crate) fn end_row(&mut self, items: usize) -> Result<(), ShredError> {
        let end = i32::try_from(items)
            .ok()
            .and_then(|items| self.offsets.last().and_then(|last| last.checked_add(items)))
            .ok_or(ShredError::TooLarge)?;
        self.offsets.push(end);
        self.nulls.append_non_null();
        Ok(())
    }

    pub(super) fn null(&mut self) {
        self.offsets.push(*self.offsets.last().unwrap_or(&0));
        self.nulls.append_null();
    }

    pub(super) fn finish(mut self) -> Result<ArrayRef, ShredError> {
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
