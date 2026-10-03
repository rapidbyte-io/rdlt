//! The rows of an array that a batch's rows name, read as sorted, disjoint ranges as they are
//! needed: each level maps its parent's ranges, so naming the items of a long list or the
//! values of a long run takes no memory a row.

use std::ops::Range;
use std::rc::Rc;

use arrow_buffer::bit_iterator::BitSliceIterator;
use arrow_buffer::{ArrowNativeType, BooleanBuffer, NullBuffer, RunEndBuffer};

/// The ends of a run-end encoded array's runs, of any width.
#[derive(Clone, Copy)]
pub(super) enum Ends<'a> {
    I16(&'a RunEndBuffer<i16>),
    I32(&'a RunEndBuffer<i32>),
    I64(&'a RunEndBuffer<i64>),
}

impl Ends<'_> {
    /// The run holding the array's row `row`.
    fn physical(self, row: usize) -> usize {
        match self {
            Self::I16(ends) => ends.get_physical_index(row),
            Self::I32(ends) => ends.get_physical_index(row),
            Self::I64(ends) => ends.get_physical_index(row),
        }
    }
}

/// A list's offsets, of either width.
#[derive(Clone, Copy)]
pub(super) enum Offsets<'a> {
    Small(&'a [i32]),
    Large(&'a [i64]),
}

impl Offsets<'_> {
    fn get(self, row: usize) -> usize {
        match self {
            Self::Small(offsets) => offsets[row].as_usize(),
            Self::Large(offsets) => offsets[row].as_usize(),
        }
    }
}

/// A list view's offsets and sizes, of either width.
#[derive(Clone, Copy)]
pub(super) enum Views<'a> {
    Small(&'a [i32], &'a [i32]),
    Large(&'a [i64], &'a [i64]),
}

impl Views<'_> {
    /// The items row `row` names.
    pub(super) fn span(self, row: usize) -> Range<usize> {
        let (first, size) = match self {
            Self::Small(offsets, sizes) => (offsets[row].as_usize(), sizes[row].as_usize()),
            Self::Large(offsets, sizes) => (offsets[row].as_usize(), sizes[row].as_usize()),
        };
        first..first.saturating_add(size)
    }

    /// Whether the items its `rows` rows name lie in row order, none named twice: its rows then
    /// name its items as a list's do.
    pub(super) fn ordered(self, rows: usize) -> bool {
        let mut end = 0;
        (0..rows).map(|row| self.span(row)).all(|span| {
            let ordered = span.is_empty() || span.start >= end;
            if !span.is_empty() {
                end = span.end;
            }
            ordered
        })
    }
}

/// The rows of an array that something names.
#[derive(Clone)]
pub(super) enum Rows<'a> {
    /// Every row, below the length.
    All(usize),
    /// The rows of others valid in a null buffer of the same array.
    Valid(Rc<Rows<'a>>, NullBuffer),
    /// The items of a list's rows, between its offsets.
    Items(Rc<Rows<'a>>, Offsets<'a>),
    /// The items of a list view's rows, which name them in row order.
    Viewed(Rc<Rows<'a>>, Views<'a>),
    /// The items of a fixed-size list's rows: a row's from its first, so many a row.
    Fixed(Rc<Rows<'a>>, usize, usize),
    /// The values of a run-end encoded array's rows.
    Runs(Rc<Rows<'a>>, Ends<'a>),
    /// The rows set in a bitmap.
    Bits(Rc<BooleanBuffer>),
    /// The rows listed, in order, each once.
    Listed(Rc<Vec<usize>>),
    /// Sorted, disjoint ranges.
    Ranges(Rc<Vec<Range<usize>>>),
    /// The rows of others below a length: those of an array that has them.
    Clamped(Rc<Rows<'a>>, usize),
}

impl Rows<'_> {
    /// The rows, as sorted, disjoint, non-empty ranges.
    pub(super) fn ranges(&self) -> Box<dyn Iterator<Item = Range<usize>> + '_> {
        match self {
            Self::All(len) => Box::new((*len > 0).then_some(0..*len).into_iter()),
            Self::Valid(rows, nulls) => Box::new(rows.ranges().flat_map(move |range| {
                let bits = nulls.inner();
                let slices =
                    BitSliceIterator::new(bits.values(), bits.offset() + range.start, range.len());
                slices.map(move |(start, end)| range.start + start..range.start + end)
            })),
            Self::Items(rows, offsets) => Box::new(
                rows.ranges()
                    .map(|range| offsets.get(range.start)..offsets.get(range.end))
                    .filter(|range| !range.is_empty()),
            ),
            Self::Viewed(rows, views) => Box::new(
                rows.ranges()
                    .flatten()
                    .map(|row| views.span(row))
                    .filter(|span| !span.is_empty()),
            ),
            Self::Fixed(rows, first, size) => Box::new(
                rows.ranges()
                    .map(move |range| first + range.start * size..first + range.end * size)
                    .filter(|range| !range.is_empty()),
            ),
            Self::Runs(rows, ends) => {
                let mut done = 0;
                Box::new(rows.ranges().filter_map(move |range| {
                    let start = ends.physical(range.start).max(done);
                    let end = ends.physical(range.end - 1) + 1;
                    done = done.max(end);
                    (start < end).then_some(start..end)
                }))
            }
            Self::Bits(bits) => Box::new(bits.set_slices().map(|(start, end)| start..end)),
            Self::Listed(rows) => Box::new(merged(rows.iter().copied())),
            Self::Ranges(ranges) => Box::new(ranges.iter().cloned()),
            Self::Clamped(rows, len) => Box::new(
                rows.ranges()
                    .map(|range| range.start.min(*len)..range.end.min(*len))
                    .filter(|range| !range.is_empty()),
            ),
        }
    }
}

/// Rows in order, each once, as ranges of the consecutive ones.
fn merged(rows: impl Iterator<Item = usize>) -> impl Iterator<Item = Range<usize>> {
    let mut rows = rows.peekable();
    std::iter::from_fn(move || {
        let start = rows.next()?;
        let mut end = start + 1;
        while rows.next_if_eq(&end).is_some() {
            end += 1;
        }
        Some(start..end)
    })
}
