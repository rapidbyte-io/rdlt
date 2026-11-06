//! What a transform allocates at most, and how the engine measures what it did allocate.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use arrow_array::{Array, RecordBatch, make_array};
use arrow_schema::DataType;
use rdlt_connector::cost::Allocations;

/// Bytes a transform allocates at most for a batch, linear in it: a fixed part, a part for each
/// row, and a part for each byte each named input column holds.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Bound {
    fixed: u64,
    per_row: u64,
    /// Bytes for each byte the input column at each index holds.
    columns: BTreeMap<usize, u64>,
}

impl Bound {
    /// A bound of `fixed` bytes, whatever the batch.
    #[must_use]
    pub const fn new(fixed: u64) -> Self {
        Self {
            fixed,
            per_row: 0,
            columns: BTreeMap::new(),
        }
    }

    /// The bound with `per_row` more bytes for each row.
    #[must_use]
    pub fn with_rows(mut self, per_row: u64) -> Self {
        self.per_row = self.per_row.saturating_add(per_row);
        self
    }

    /// The bound with `per_byte` more bytes for each byte input column `index` holds.
    #[must_use]
    pub fn with_column(mut self, index: usize, per_byte: u64) -> Self {
        let held = self.columns.entry(index).or_default();
        *held = held.saturating_add(per_byte);
        self
    }

    /// This bound and `other` together.
    #[must_use]
    pub fn plus(mut self, other: &Self) -> Self {
        self.fixed = self.fixed.saturating_add(other.fixed);
        self.per_row = self.per_row.saturating_add(other.per_row);
        for (index, per_byte) in &other.columns {
            self = self.with_column(*index, *per_byte);
        }
        self
    }

    /// This bound `factor` times over.
    #[must_use]
    pub fn times(mut self, factor: u64) -> Self {
        self.fixed = self.fixed.saturating_mul(factor);
        self.per_row = self.per_row.saturating_mul(factor);
        for per_byte in self.columns.values_mut() {
            *per_byte = per_byte.saturating_mul(factor);
        }
        self
    }

    /// Bytes: the bound for `batch`; a column the batch lacks holds nothing.
    pub fn of(&self, batch: &RecordBatch) -> u64 {
        let rows = u64::try_from(batch.num_rows()).unwrap_or(u64::MAX);
        let mut bytes = self.fixed.saturating_add(self.per_row.saturating_mul(rows));
        for (index, per_byte) in &self.columns {
            let held = batch
                .columns()
                .get(*index)
                .map_or(0, |column| column_bytes(column.as_ref()));
            bytes = bytes.saturating_add(per_byte.saturating_mul(held));
        }
        bytes
    }
}

/// Bytes: what the rows of `array` hold in its buffers, a slice's own rows alone where its
/// layout says which bytes they are, and for a view layout the bytes its views reference.
pub fn column_bytes(array: &dyn Array) -> u64 {
    let data = array.to_data();
    let bytes = data
        .get_slice_memory_size()
        .unwrap_or_else(|_| array.get_buffer_memory_size());
    u64::try_from(bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(viewed_bytes(array))
}

/// The longest value, in bytes, a view holds inline.
const INLINE: u32 = 12;

/// Bytes: the text and bytes stored outside the views of `array` and its children, which are
/// the views longer than they hold inline.
fn viewed_bytes(array: &dyn Array) -> u64 {
    let data = array.to_data();
    let own = if matches!(data.data_type(), DataType::Utf8View | DataType::BinaryView) {
        data.buffers()
            .first()
            .and_then(|views| {
                views
                    .typed_data::<u128>()
                    .get(data.offset()..data.offset() + data.len())
            })
            .map_or(0, |views| {
                views
                    .iter()
                    .map(|view| u32::try_from(view & u128::from(u32::MAX)).unwrap_or(0))
                    .filter(|length| *length > INLINE)
                    .map(u64::from)
                    .sum()
            })
    } else {
        0
    };
    data.child_data().iter().fold(own, |bytes, child| {
        bytes.saturating_add(viewed_bytes(make_array(child.clone()).as_ref()))
    })
}

/// Bytes: the buffers `output` keeps alive that `input` does not, each counted once; the schema
/// of neither counts.
pub fn allocated_beyond(output: &RecordBatch, input: &RecordBatch) -> u64 {
    let mut held = Allocations::of(input);
    held.add(&RecordBatch::new_empty(output.schema()));
    Allocations::default().add_beyond(output, &held)
}
