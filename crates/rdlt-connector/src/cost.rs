//! What a batch costs whoever holds it: the memory it keeps alive, and the memory it becomes
//! once its encodings are decoded and its values rendered as the destination stores them.
//!
//! A push is charged what it keeps alive ([`push_charge`]); what it becomes bounds what is
//! lowered, read back or compared at once. What a frame of its rows would hold on the wire is not
//! measured here: `rdlt_wire::Weigher` weighs that.

mod expanded;
mod held;
#[cfg(test)]
pub(crate) mod tests;
mod widths;

use std::collections::BTreeSet;
use std::fmt;
use std::ops::Range;

use arrow_array::cast::AsArray as _;
use arrow_array::{Array, RecordBatch};
use arrow_schema::DataType;
use rdlt_wire::limits::count;

pub use held::{Allocations, schema_bytes};

use crate::sink::Push;
use crate::types::{LogicalType, TypeKind};
use expanded::Meter;

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

    /// What `rows` of `batch` expand to, measured up to `limit`.
    pub fn expanded(&self, batch: &RecordBatch, rows: Range<usize>, limit: u64) -> u64 {
        self.measure(batch, limit).expanded(rows)
    }

    /// A measure of what rows of `batch` expand to, up to `limit`, for measuring many stretches
    /// of them: a value many rows name is measured once for all of them.
    pub fn measure(&self, batch: &RecordBatch, limit: u64) -> Measure {
        self.lowering(batch, Vec::new(), 0, limit)
    }

    /// A measure of what lowering rows of `batch` into their table holds at once, up to `limit`.
    ///
    /// `stored` says, for each column of the batch in order, how its table stores it; a column
    /// it says nothing of is measured as [`Rendering::measure`] measures it. Every row costs
    /// `row` bytes beside its columns: the columns of the table the batch holds nothing in.
    pub fn lowering(
        &self,
        batch: &RecordBatch,
        stored: Vec<Option<Stored>>,
        row: u64,
        limit: u64,
    ) -> Measure {
        Measure {
            batch: batch.clone(),
            stored,
            row,
            meter: Meter::new(self, limit),
        }
    }
}

/// Bytes a JSON push is charged for each byte of its text: the text, and twice it for the
/// batches the engine shreds it into, which are paid for before they are built.
///
/// What sparse records become beyond that, a null in every column for every row, the shredder's
/// limit on cells bounds, not the budget.
pub const JSON_CHARGE: u64 = 3;

/// The bytes holding `push` is charged, by the engine's admission and by certification alike: a
/// batch every allocation it keeps alive, each counted once, and JSON [`JSON_CHARGE`] bytes for
/// each byte of its text.
pub fn push_charge(push: &Push) -> u64 {
    match push {
        Push::Arrow(batch) | Push::Changes(batch) => Allocations::of(batch).bytes(),
        Push::Json(text) => count(text.len()).saturating_mul(JSON_CHARGE),
    }
}

impl Measure {
    /// The measure, each item a list names, at any depth, costing `bytes` beside itself: for
    /// whoever makes a row of each item, as normalizing does.
    #[must_use]
    pub fn with_items(self, bytes: u64) -> Self {
        Self {
            meter: self.meter.with_items(bytes),
            ..self
        }
    }
}

/// How a table stores one column of a batch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stored {
    /// The type of the table's column, which the batch's values are converted to.
    pub column: LogicalType,
    /// Whether the destination stores the column's values as text.
    pub text: bool,
    /// Whether the batch's column is JSON text whose values the column, of another type, holds
    /// in part: those it holds are read into its type, and the others copied as text.
    pub read: bool,
}

/// A run of rows cut from a batch, and what it was measured to expand to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Piece {
    /// The row the piece ends before.
    pub end: usize,
    /// Bytes: what the piece expands to at most, the sum of the stretches it was measured in;
    /// for one row beyond the limit, some value beyond it.
    pub bytes: u64,
}

/// Measures what stretches of one batch's rows expand to, up to a limit.
pub struct Measure {
    batch: RecordBatch,
    stored: Vec<Option<Stored>>,
    /// Bytes: what every row costs beside its columns.
    row: u64,
    meter: Meter,
}

impl fmt::Debug for Measure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Measure").finish_non_exhaustive()
    }
}

impl Measure {
    /// What `rows` expand to: beyond the limit, some value beyond it, where measuring stopped.
    pub fn expanded(&mut self, rows: Range<usize>) -> u64 {
        let limit = self.meter.limit();
        self.stretch(rows, limit)
    }

    /// What `rows` expand to, measured no further than `stop` bytes.
    fn stretch(&mut self, rows: Range<usize>, stop: u64) -> u64 {
        self.meter.restart(stop);
        let within = rows.start.min(self.batch.num_rows())..rows.end.min(self.batch.num_rows());
        self.meter.rows(within.len(), self.row);
        for (index, column) in self.batch.columns().iter().enumerate() {
            if self.meter.over() {
                break;
            }
            let stored = self.stored.get(index).and_then(Option::as_ref);
            self.meter.column(column.as_ref(), rows.clone(), stored);
        }
        self.meter.spent()
    }

    /// The next piece of the batch from row `first`: the longest run of rows whose stretches,
    /// as they were measured, expand to no more than the limit together, and one row at least.
    ///
    /// Stretches are measured each twice as long as the last while they fit, and half as long
    /// once one did not, each no further than what the piece has left: a piece costs about one
    /// measuring of its rows and of half as many again. The stretches' sum is never less than
    /// what the rows expand to measured together.
    pub fn piece(&mut self, first: usize) -> Piece {
        let (rows, limit) = (self.batch.num_rows(), self.meter.limit());
        let mut end = first.saturating_add(1).min(rows);
        let mut bytes = self.stretch(first..end, limit);
        // How many rows are tried next while every stretch fitted, and once one did not, how
        // many are known not to fit.
        let (mut step, mut unfit) = (1_usize, None);
        while end < rows && bytes <= limit {
            let tried = match unfit {
                None => step.min(rows - end),
                Some(unfit) if unfit > 1 => unfit / 2,
                Some(_) => break,
            };
            let more = self.stretch(end..end + tried, limit - bytes);
            if more <= limit - bytes {
                (end, bytes) = (end + tried, bytes + more);
                step = step.saturating_mul(2);
                unfit = unfit.map(|unfit| unfit - tried);
            } else {
                unfit = Some(tried);
            }
        }
        Piece { end, bytes }
    }

    /// Where to cut the batch so each piece expands to at most the limit: each piece in order,
    /// the last ending at the batch's row count.
    ///
    /// A row that alone expands beyond the limit is a piece of its own.
    pub fn cuts(&mut self) -> Vec<Piece> {
        let rows = self.batch.num_rows();
        let mut cuts = Vec::new();
        let mut first = 0;
        while first < rows {
            let piece = self.piece(first);
            first = piece.end;
            cuts.push(piece);
        }
        if cuts.is_empty() {
            cuts.push(Piece { end: 0, bytes: 0 });
        }
        cuts
    }

    /// The measure, remembering no value.
    #[cfg(test)]
    pub(crate) fn forgetful(self) -> Self {
        Self {
            meter: self.meter.forgetful(),
            ..self
        }
    }

    /// How many rows, items and stretches measuring has looked at, and how many values it
    /// remembers.
    #[cfg(test)]
    pub(crate) fn work(&self) -> (u64, usize) {
        (self.meter.steps(), self.meter.remembered())
    }
}

/// Bytes: an upper bound on the text the values of `array` render to together, for whoever
/// builds it to hold no more: their JSON text where `json`, else their text as a destination
/// storing text takes it, bytes in hex.
///
/// Strings and bytes are measured exactly, a value of a fixed width at the longest text its
/// type renders to, and a nested value as the cost model measures its JSON text.
pub fn text_bytes(array: &dyn Array, json: bool) -> u64 {
    let rows = count(array.len());
    let quotes = if json {
        rows.saturating_mul(widths::BRACKETS)
    } else {
        0
    };
    if let Some(scalar) = widths::scalar(array.data_type()) {
        return rows.saturating_mul(scalar.text);
    }
    match array.data_type() {
        DataType::Utf8 => {
            let text = array.as_string::<i32>();
            let (first, last) = (text.value_offsets()[0], text.value_offsets()[array.len()]);
            let range = usize::try_from(first).unwrap_or(0)..usize::try_from(last).unwrap_or(0);
            let bytes = text.value_data().get(range).unwrap_or_default();
            let escapes = if json { widths::escapes(bytes) } else { 0 };
            count(bytes.len())
                .saturating_add(escapes)
                .saturating_add(quotes)
        }
        DataType::Binary => {
            let bytes = array.as_binary::<i32>();
            let (first, last) = (bytes.value_offsets()[0], bytes.value_offsets()[array.len()]);
            let length = u64::try_from(last.saturating_sub(first)).unwrap_or(0);
            length.saturating_mul(2).saturating_add(quotes)
        }
        _ => {
            let mut meter = Meter::new(&Rendering::native(), u64::MAX);
            meter.inside(array);
            meter.spent()
        }
    }
}

/// The bytes `rows` nulls of `data_type` take once built as an array of it.
pub fn nulls(data_type: &DataType, rows: usize) -> u64 {
    let rows = count(rows);
    rows.saturating_mul(widths::null_slot(data_type))
        .saturating_add(rows.div_ceil(8))
}
