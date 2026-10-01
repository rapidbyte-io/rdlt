//! Copies the ranges of a column that nests nothing, and of validity masks.

use arrow_array::{Array as _, ArrayRef, UInt64Array};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer};
use arrow_schema::ArrowError;
use arrow_select::concat::concat;
use arrow_select::take::take;

use super::{Ranges, count, sized};

/// How many indices are held at once to copy what ranges name: the items of a piece are
/// copied a run of this many at a time, whatever the piece holds.
pub(super) const INDICES: usize = 1 << 16;

#[cfg(test)]
thread_local! {
    /// The most indices this thread held at once.
    pub(super) static HELD: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// The items of `array`, which nests no column, that `ranges` name.
pub(super) fn leaf(array: &ArrayRef, ranges: &Ranges) -> Result<ArrayRef, ArrowError> {
    match ranges {
        [] => return Ok(array.slice(0, 0)),
        [(start, end)] => return Ok(array.slice(*start, end.saturating_sub(*start))),
        _ => {}
    }
    let mut parts = Vec::new();
    let mut indices = Vec::with_capacity(INDICES.min(count(ranges)));
    for (start, end) in ranges {
        for index in *start..*end {
            indices.push(sized::<u64>(index)?);
            if indices.len() == INDICES {
                parts.push(taken(array, &mut indices)?);
            }
        }
    }
    if !indices.is_empty() {
        parts.push(taken(array, &mut indices)?);
    }
    if let [part] = &parts[..] {
        return Ok(ArrayRef::clone(part));
    }
    let parts: Vec<_> = parts.iter().map(AsRef::as_ref).collect();
    concat(&parts)
}

/// The items of `array` at `indices`, which are left empty.
fn taken(array: &ArrayRef, indices: &mut Vec<u64>) -> Result<ArrayRef, ArrowError> {
    #[cfg(test)]
    HELD.with(|held| held.set(held.get().max(indices.len())));
    let held = UInt64Array::from(std::mem::take(indices));
    take(array, &held, None)
}

/// The validity of the rows `ranges` name, of a column whose validity is `nulls`.
pub(super) fn validity(nulls: Option<&NullBuffer>, ranges: &Ranges) -> Option<NullBuffer> {
    let nulls = nulls?;
    if let [(start, end)] = ranges {
        return Some(nulls.slice(*start, end.saturating_sub(*start)));
    }
    let mut valid = BooleanBufferBuilder::new(count(ranges));
    for (start, end) in ranges {
        valid.append_buffer(&nulls.inner().slice(*start, end.saturating_sub(*start)));
    }
    Some(NullBuffer::new(valid.finish()))
}
