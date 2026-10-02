//! What a batch costs whoever holds it: the memory it keeps alive, and the memory it becomes
//! once its encodings are decoded and its values rendered as the destination stores them.
//!
//! A batch is charged the larger of the two. What a frame of its rows would hold on the wire is
//! not measured here: `rdlt_wire::Weigher` weighs that.

mod expanded;
mod held;
#[cfg(test)]
pub(crate) mod tests;
mod widths;

use std::collections::BTreeSet;
use std::fmt;
use std::ops::Range;

use arrow_array::{Array, RecordBatch};
use arrow_schema::DataType;

pub use held::Allocations;

use crate::sink::Push;
use crate::types::TypeKind;
use expanded::Meter;

/// What a batch costs whoever holds it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Cost {
    /// Bytes: every allocation the batch keeps alive, each counted once.
    pub held: u64,
    /// Bytes: what the batch's rows become once decoded and rendered, up to the limit they were
    /// measured against.
    pub expanded: u64,
}

impl Cost {
    /// The bytes to charge: the larger of what is held and what it becomes.
    pub fn charge(&self) -> u64 {
        self.held.max(self.expanded)
    }
}

/// Which values a destination stores as text, so what they cost is what they are rendered to.
///
/// Nested values always cost their JSON text, field names included: whether a table stores them
/// as they are is its stream's choice, made after a push is admitted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Rendering {
    /// The kinds stored as they are; `None` where every kind is.
    native: Option<BTreeSet<TypeKind>>,
}

impl Rendering {
    /// A destination storing values of the kinds `native` as they are, and any other as text.
    pub fn new(native: impl IntoIterator<Item = TypeKind>) -> Self {
        Self {
            native: Some(native.into_iter().collect()),
        }
    }

    /// A destination storing every value as text: what costs most.
    pub fn text() -> Self {
        Self::new([])
    }

    /// Whoever holds every value outside a nested one as it is: what decoding alone costs.
    pub fn native() -> Self {
        Self { native: None }
    }

    /// Whether a value of `kind` outside any nested value is rendered as text.
    fn renders(&self, kind: TypeKind) -> bool {
        self.native
            .as_ref()
            .is_some_and(|native| !native.contains(&kind))
    }

    /// What `batch` costs, its expansion measured up to `limit`.
    ///
    /// An expansion beyond `limit` is reported as some value beyond it: measuring stops there, so
    /// the work is bounded by `limit` however an encoding multiplies its values.
    pub fn cost(&self, batch: &RecordBatch, limit: u64) -> Cost {
        Cost {
            held: Allocations::of(batch).bytes(),
            expanded: self.expanded(batch, 0..batch.num_rows(), limit),
        }
    }

    /// The bytes whoever holds `push` charges for it, its expansion measured up to `limit`: a
    /// batch the larger of what it keeps alive and what it becomes, and JSON its text, whose
    /// records are charged as they are parsed.
    pub fn charge(&self, push: &Push, limit: u64) -> u64 {
        match push {
            Push::Arrow(batch) | Push::Changes(batch) => self.cost(batch, limit).charge(),
            Push::Json(text) => widths::count(text.len()),
        }
    }

    /// What `rows` of `batch` expand to, measured up to `limit`.
    pub fn expanded(&self, batch: &RecordBatch, rows: Range<usize>, limit: u64) -> u64 {
        self.measure(batch, limit).expanded(rows)
    }

    /// What `rows` of `array`, a column of its own, expand to, measured up to `limit`.
    pub fn expanded_array(&self, array: &dyn Array, rows: Range<usize>, limit: u64) -> u64 {
        let mut meter = Meter::new(self, limit);
        meter.column(array, rows);
        meter.spent()
    }

    /// A measure of what rows of `batch` expand to, up to `limit`, for measuring many stretches
    /// of them: a value many rows name is measured once for all of them.
    pub fn measure<'a>(&'a self, batch: &'a RecordBatch, limit: u64) -> Measure<'a> {
        Measure {
            batch,
            meter: Meter::new(self, limit),
        }
    }
}

/// Measures what stretches of one batch's rows expand to, up to a limit.
pub struct Measure<'a> {
    batch: &'a RecordBatch,
    meter: Meter<'a>,
}

impl fmt::Debug for Measure<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Measure").finish_non_exhaustive()
    }
}

impl Measure<'_> {
    /// What `rows` expand to: beyond the limit, some value beyond it, where measuring stopped.
    pub fn expanded(&mut self, rows: Range<usize>) -> u64 {
        self.meter.restart();
        for column in self.batch.columns() {
            if self.meter.over() {
                break;
            }
            self.meter.column(column.as_ref(), rows.clone());
        }
        self.meter.spent()
    }

    /// Whether `rows` expand to no more than the limit.
    fn fits(&mut self, rows: Range<usize>) -> bool {
        self.expanded(rows);
        !self.meter.over()
    }

    /// Where to cut the batch so each piece expands to at most the limit: the end of each piece,
    /// in order, the last being the batch's row count.
    ///
    /// A row that alone expands beyond the limit is a piece of its own. The pieces are found by
    /// searching the rows' running cost, which only grows, so no cost a row is kept.
    pub fn cuts(&mut self) -> Vec<usize> {
        let rows = self.batch.num_rows();
        let mut cuts = Vec::new();
        let mut first = 0;
        while first < rows && !self.fits(first..rows) {
            // Doubles the piece while it fits, then searches between the last that fit and the
            // first that did not; the first row is a piece whether or not it fits.
            let (mut fitting, mut step) = (first + 1, 1_usize);
            let mut beyond = rows;
            while fitting < rows {
                let next = fitting.saturating_add(step).min(rows);
                if !self.fits(first..next) {
                    beyond = next;
                    break;
                }
                fitting = next;
                step = step.saturating_mul(2);
            }
            while beyond - fitting > 1 {
                let middle = fitting + (beyond - fitting) / 2;
                if self.fits(first..middle) {
                    fitting = middle;
                } else {
                    beyond = middle;
                }
            }
            cuts.push(fitting);
            first = fitting;
        }
        if cuts.last() != Some(&rows) {
            cuts.push(rows);
        }
        cuts
    }

    /// The measure, remembering no value.
    #[cfg(test)]
    pub(crate) fn forgetful(self) -> Self {
        Self {
            batch: self.batch,
            meter: self.meter.forgetful(),
        }
    }

    /// How many rows, items and stretches measuring has looked at, and how many values it
    /// remembers.
    #[cfg(test)]
    pub(crate) fn work(&self) -> (u64, usize) {
        (self.meter.steps(), self.meter.remembered())
    }
}

/// The bytes `rows` nulls of `data_type` take once built as an array of it.
pub fn nulls(data_type: &DataType, rows: usize) -> u64 {
    let rows = widths::count(rows);
    rows.saturating_mul(widths::null_slot(data_type))
        .saturating_add(rows.div_ceil(8))
}
