//! Weighs a batch row by row, without copying it: what each row adds to a frame holding only
//! what its rows name, and what it takes once its dictionary keys and runs are replaced by the
//! values they name.

mod column;
#[cfg(test)]
mod tests;

use std::ops::AddAssign;

use arrow_array::RecordBatch;

use self::column::{Column, Counts};
use super::compact::plain;

/// What rows weigh.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Weight {
    /// Values: what the rows add to the values of a frame holding only what they name, as its
    /// receiver counts them: a value for each row of each column, nested columns included, for
    /// each item a list view names, and for each run a run-end column begins in the frame.
    pub values: u64,
    /// Bytes: what the rows' views name in their data buffers in such a frame, counted once a
    /// view, as its receiver counts them.
    pub view_bytes: u64,
    /// Bits: the buffers such a frame holds for the rows, before the padding of each buffer and
    /// the frame's header: a dictionary column's keys, a run's value once for the run.
    pub frame_bits: u64,
    /// Bits: the rows' values once every dictionary key and every run is replaced by the value
    /// it names, a value named by several rows counted for each.
    pub expanded_bits: u64,
}

impl Weight {
    /// Bytes: [`Weight::frame_bits`], rounded up.
    pub fn frame_bytes(&self) -> u64 {
        self.frame_bits.div_ceil(8)
    }

    /// Bytes: [`Weight::expanded_bits`], rounded up.
    pub fn expanded_bytes(&self) -> u64 {
        self.expanded_bits.div_ceil(8)
    }
}

impl AddAssign for Weight {
    fn add_assign(&mut self, other: Self) {
        self.values = self.values.saturating_add(other.values);
        self.view_bytes = self.view_bytes.saturating_add(other.view_bytes);
        self.frame_bits = self.frame_bits.saturating_add(other.frame_bits);
        self.expanded_bits = self.expanded_bits.saturating_add(other.expanded_bits);
    }
}

/// Weighs the rows of a batch, in order, as the rows of consecutive pieces of it.
///
/// A row's weight follows what it names, through offsets, views, list views, unions, runs and
/// dictionary keys, whatever buffers its batch shares or was sliced from; nothing is copied, and
/// what the weigher keeps beside the batch's own buffers does not grow with its rows. Rows of
/// one piece are weighed in order, since a run-end column's run is weighed with the first row of
/// the piece that names it.
///
/// ```
/// use std::sync::Arc;
///
/// use arrow_array::{ArrayRef, Int32Array, RecordBatch};
/// use rdlt_wire::Weigher;
///
/// let ids: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3]));
/// let batch = RecordBatch::try_from_iter([("id", ids)])?;
/// let mut weigher = Weigher::new(&batch);
/// weigher.begin();
/// let row = weigher.weigh(0);
/// assert_eq!((row.values, row.frame_bytes()), (1, 5));
/// # Ok::<(), arrow_schema::ArrowError>(())
/// ```
#[derive(Debug)]
pub struct Weigher {
    columns: Vec<Column>,
    /// The run each run-end column last weighed in the piece begun.
    runs: Vec<Option<usize>>,
    counts: Counts,
    rows: usize,
}

impl Weigher {
    /// A weigher of `batch`'s rows.
    pub fn new(batch: &RecordBatch) -> Self {
        let mut counts = Counts::default();
        let columns = batch.columns().iter();
        let columns = columns.map(|column| {
            let rebuilt = !plain(column.data_type());
            Column::of(column.as_ref(), rebuilt, &mut counts)
        });
        let columns: Vec<_> = columns.collect();
        Self {
            columns,
            runs: vec![None; counts.runs],
            counts,
            rows: batch.num_rows(),
        }
    }

    /// How many rows the batch holds.
    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Begins a piece: the next row weighed is its first.
    pub fn begin(&mut self) {
        self.runs.fill(None);
    }

    /// What `row` adds to the piece begun, weighed after the rows of the piece before it; a row
    /// the batch does not hold weighs nothing.
    pub fn weigh(&mut self, row: usize) -> Weight {
        let mut weight = Weight::default();
        if row < self.rows {
            for column in &self.columns {
                column.weigh(row, true, &mut self.runs, &mut weight);
            }
        }
        weight
    }

    /// Bytes: the most a frame of this batch's schema takes beside what its rows weigh: each
    /// buffer's padding, an offset more than rows in each buffer of offsets, and its header.
    pub fn overhead(&self) -> u64 {
        let wide = |count: usize| u64::try_from(count).unwrap_or(u64::MAX);
        let (nodes, buffers) = (wide(self.counts.nodes), wide(self.counts.buffers));
        // A buffer is padded to 64 bytes and described in 16; a node is described in 16.
        (PADDING + 16 + 8) * buffers + 16 * nodes + HEADER
    }
}

/// Bytes: the padding Arrow's writer may add to a buffer.
const PADDING: u64 = 64;

/// Bytes: a record batch message beside what describes its nodes and buffers.
const HEADER: u64 = 512;
