//! Weighs the rows of a batch without copying it: what they add to a frame holding only what
//! its rows name.

mod build;
mod column;
mod span;
#[cfg(test)]
mod tests;

use std::ops::{AddAssign, Range};

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
}

impl Weight {
    /// Bytes: [`Weight::frame_bits`], rounded up.
    pub fn frame_bytes(&self) -> u64 {
        self.frame_bits.div_ceil(8)
    }
}

impl AddAssign for Weight {
    fn add_assign(&mut self, other: Self) {
        self.values = self.values.saturating_add(other.values);
        self.view_bytes = self.view_bytes.saturating_add(other.view_bytes);
        self.frame_bits = self.frame_bits.saturating_add(other.frame_bits);
    }
}

/// What weighing keeps between rows.
#[derive(Clone, Debug)]
struct State {
    /// The run each run-end column last weighed in the piece begun.
    runs: Vec<Option<usize>>,
    /// The values beyond which a stretch of rows is weighed no further.
    most: u64,
    /// How many columns, rows and runs were looked at, for tests of what weighing costs.
    #[cfg(test)]
    visits: u64,
}

impl State {
    /// Counts one column, row or run looked at.
    #[cfg_attr(
        not(test),
        expect(clippy::unused_self, reason = "the count is kept for tests alone")
    )]
    fn visit(&mut self) {
        #[cfg(test)]
        {
            self.visits += 1;
        }
    }

    /// Whether `weight` already holds more values than rows are weighed for.
    fn over(&self, weight: &Weight) -> bool {
        weight.values > self.most
    }
}

/// Weighs the rows of a batch, in order, as the rows of consecutive pieces of it.
///
/// A row's weight follows what it names, through offsets, views, list views, unions, runs and
/// dictionary keys, whatever buffers its batch shares or was sliced from, and nothing is copied.
///
/// - Rows of one piece are weighed in order, since a run-end column's run is weighed with the
///   first row of the piece that names it.
/// - Weighing is linear in the rows and the items they name. A stretch of fixed-width values,
///   of bytes by offsets, of dictionary keys, or of structs and lists of those is weighed by
///   arithmetic, however long it is.
/// - A dictionary's values are in a frame of their own and in no row's weight:
///   [`Weigher::dictionaries`] weighs each.
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
/// let rest = weigher.weigh_rows(1..3);
/// assert_eq!((rest.values, rest.frame_bytes()), (2, 9));
/// # Ok::<(), arrow_schema::ArrowError>(())
/// ```
#[derive(Debug)]
pub struct Weigher {
    columns: Vec<Column>,
    state: State,
    /// The runs last weighed when the weigher was marked.
    marked: Vec<Option<usize>>,
    counts: Counts,
    rows: usize,
}

impl Weigher {
    /// A weigher of `batch`'s rows.
    pub fn new(batch: &RecordBatch) -> Self {
        Self::within(batch, u64::MAX)
    }

    /// A weigher of `batch`'s rows that weighs a stretch of rows no further once it holds more
    /// than `most` values: what it then returns is beyond `most`, and no more is known of it.
    pub(super) fn within(batch: &RecordBatch, most: u64) -> Self {
        let mut counts = Counts::default();
        let columns = batch.columns().iter();
        let columns = columns.map(|column| {
            let rebuilt = !plain(column.data_type());
            Column::of(column.as_ref(), rebuilt, &mut counts)
        });
        let columns: Vec<_> = columns.collect();
        Self {
            columns,
            state: State {
                runs: vec![None; counts.runs],
                most,
                #[cfg(test)]
                visits: 0,
            },
            marked: vec![None; counts.runs],
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
        self.state.runs.fill(None);
    }

    /// What `row` adds to the piece begun, weighed after the rows of the piece before it; a row
    /// the batch does not hold weighs nothing.
    pub fn weigh(&mut self, row: usize) -> Weight {
        self.weigh_rows(row..row.saturating_add(1))
    }

    /// What `rows` add to the piece begun, weighed after the rows of the piece before them;
    /// rows the batch does not hold weigh nothing.
    pub fn weigh_rows(&mut self, rows: Range<usize>) -> Weight {
        let mut weight = Weight::default();
        let end = rows.end.min(self.rows);
        for column in &self.columns {
            column.span(rows.start, end, &mut self.state, &mut weight);
        }
        weight
    }

    /// What the values of each dictionary in the batch weigh as the frame of their own they go
    /// in, a dictionary among another's values too; this begins a piece anew.
    pub fn dictionaries(&mut self) -> Vec<Weight> {
        let mut weights = Vec::new();
        for column in &self.columns {
            column.dictionaries(&mut self.state, &mut weights);
        }
        self.begin();
        weights
    }

    /// Remembers which runs the piece begun has weighed, for [`Weigher::rewind`].
    pub(super) fn mark(&mut self) {
        self.marked.clone_from(&self.state.runs);
    }

    /// Forgets the rows weighed since the last [`Weigher::mark`].
    pub(super) fn rewind(&mut self) {
        self.state.runs.clone_from(&self.marked);
    }

    /// How many columns, rows and runs weighing has looked at.
    #[cfg(test)]
    pub(super) fn visits(&self) -> u64 {
        self.state.visits
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
