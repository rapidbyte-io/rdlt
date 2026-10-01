//! Weighs a stretch of a column's items: by arithmetic on widths and offsets where a layout
//! allows, and item by item only where each names something of its own.

use super::build::{VALID, wide};
use super::column::{Column, Expanded};
use super::{State, Weight};

/// Adds a column's own `values` and `bits` to `weight`'s frame where `framed`, and `expanded`
/// bits to what it takes expanded.
fn own(weight: &mut Weight, framed: bool, values: u64, bits: u64, expanded: u64) {
    if framed {
        weight.values = weight.values.saturating_add(values);
        weight.frame_bits = weight.frame_bits.saturating_add(bits);
    }
    weight.expanded_bits = weight.expanded_bits.saturating_add(expanded);
}

/// Adds `count` items of `bits` bits each, in the frame and expanded alike.
fn each(weight: &mut Weight, framed: bool, count: u64, bits: u64) {
    let bits = count.saturating_mul(bits);
    own(weight, framed, count, bits, bits);
}

impl Column {
    /// Adds what the items from `start` to before `end` weigh to `weight`: to all of it where
    /// `framed`, else, for values a dictionary key or a run already weighed names, to its
    /// expanded bits only.
    pub(super) fn span(
        &mut self,
        start: usize,
        end: usize,
        framed: bool,
        state: &mut State,
        weight: &mut Weight,
    ) {
        if start >= end {
            return;
        }
        state.visit();
        let count = wide(end - start);
        match self {
            Self::Fixed { bits } => each(weight, framed, count, *bits),
            Self::Bytes { bits, offset } => {
                let bytes = wide(offset(end).saturating_sub(offset(start)));
                let bits = count.saturating_mul(*bits);
                let bits = bits.saturating_add(bytes.saturating_mul(8));
                own(weight, framed, count, bits, bits);
            }
            Self::Views { views } => {
                for index in start..end {
                    if state.over(weight) {
                        return;
                    }
                    view(views, index, framed, weight);
                    state.visit();
                }
            }
            Self::Sized { size, item } => {
                each(weight, framed, count, VALID);
                let (start, end) = (start.saturating_mul(*size), end.saturating_mul(*size));
                item.span(start, end, framed, state, weight);
            }
            Self::Each { bits, children } => {
                each(weight, framed, count, *bits);
                for child in children {
                    child.span(start, end, framed, state, weight);
                }
            }
            _ => self.named(start, end, framed, state, weight),
        }
    }

    /// As [`Column::span`], for the layouts whose items name others.
    fn named(
        &mut self,
        start: usize,
        end: usize,
        framed: bool,
        state: &mut State,
        weight: &mut Weight,
    ) {
        let count = wide(end - start);
        match self {
            Self::List {
                bits,
                offset,
                nulls: None,
                item,
            } => {
                each(weight, framed, count, *bits);
                item.span(offset(start), offset(end), framed, state, weight);
            }
            Self::List {
                bits,
                offset,
                nulls: Some(nulls),
                item,
            } => {
                each(weight, framed, count, *bits);
                // The rows that are not null, a stretch at a time.
                let valid = nulls.inner().slice(start, end - start);
                for (from, to) in valid.set_slices() {
                    let (from, to) = (start + from, start + to);
                    item.span(offset(from), offset(to), framed, state, weight);
                    state.visit();
                }
            }
            Self::ListView { bits, range, item } => {
                for row in start..end {
                    if state.over(weight) {
                        return;
                    }
                    let (from, to) = range(row);
                    let named = wide(to.saturating_sub(from));
                    own(weight, framed, 1 + named, *bits, *bits);
                    item.span(from, to, framed, state, weight);
                    state.visit();
                }
            }
            Self::Dense { named, children } => {
                for row in start..end {
                    if state.over(weight) {
                        return;
                    }
                    own(weight, framed, 1, 8 + 32, 8 + 32);
                    let (child, item) = named(row);
                    if let Some(child) = children.get_mut(child) {
                        child.span(item, item.saturating_add(1), framed, state, weight);
                    }
                }
            }
            _ => self.encoded(start, end, framed, state, weight),
        }
    }

    /// As [`Column::span`], for run-end columns and dictionaries.
    fn encoded(
        &mut self,
        start: usize,
        end: usize,
        framed: bool,
        state: &mut State,
        weight: &mut Weight,
    ) {
        match self {
            Self::Runs {
                bits,
                reach,
                place,
                values,
                expanded,
            } => {
                let mut at = start;
                while at < end && !state.over(weight) {
                    let (run, reached) = reach(at);
                    let upto = reached.min(end).max(at + 1);
                    let mut rows = wide(upto - at);
                    // A run's end and value are in the frame once, with the first row of the
                    // piece in the run; every row takes the value once runs are replaced.
                    let last = state.runs.get_mut(*place).filter(|_| framed);
                    let begins = last.is_some_and(|last| last.replace(run) != Some(run));
                    own(weight, framed, rows, 0, 0);
                    if begins {
                        own(weight, true, 1, *bits, 0);
                        values.span(run, run + 1, true, state, weight);
                        rows -= 1;
                    }
                    let value = worth(expanded, values, run, rows, state);
                    weight.expanded_bits = weight.expanded_bits.saturating_add(value);
                    at = upto;
                    state.visit();
                }
            }
            Self::Keyed {
                bits,
                key,
                values,
                expanded,
            } => {
                each_key(weight, framed, wide(end - start), *bits);
                for row in start..end {
                    if let Some(key) = key(row) {
                        let value = worth(expanded, values, key, 1, state);
                        weight.expanded_bits = weight.expanded_bits.saturating_add(value);
                    }
                    state.visit();
                }
            }
            _ => {}
        }
    }
}

/// Adds `count` keys of `bits` bits each to the frame: their values take nothing there.
fn each_key(weight: &mut Weight, framed: bool, count: u64, bits: u64) {
    own(weight, framed, count, count.saturating_mul(bits), 0);
}

/// Bits: what `rows` rows naming value `index` of `values` take once it replaces them, the
/// value weighed once however many rows name it.
fn worth(
    expanded: &mut Expanded,
    values: &mut Column,
    index: usize,
    rows: u64,
    state: &mut State,
) -> u64 {
    if rows == 0 {
        return 0;
    }
    let value = expanded.of(index, || {
        let mut weight = Weight::default();
        values.span(index, index.saturating_add(1), false, state, &mut weight);
        weight.expanded_bits
    });
    value.saturating_mul(rows)
}

/// Adds what view `index` of `views` weighs: itself, and the bytes it names beyond those it
/// holds.
fn view(views: &[u128], index: usize, framed: bool, weight: &mut Weight) {
    let length = views
        .get(index)
        .map_or(0, |view| *view & u128::from(u32::MAX));
    let named = u64::try_from(length).ok().filter(|length| *length > 12);
    let (named, bits) = (named.unwrap_or(0), 128 + VALID + 8 * named.unwrap_or(0));
    own(weight, framed, 1, bits, bits);
    if framed {
        weight.view_bytes = weight.view_bytes.saturating_add(named);
    }
}
