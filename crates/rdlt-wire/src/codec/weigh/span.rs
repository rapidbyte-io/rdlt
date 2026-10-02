//! Weighs a stretch of a column's items: by arithmetic on widths and offsets where a layout
//! allows, and item by item only where each names something of its own.

use super::build::{VALID, wide};
use super::column::Column;
use super::{State, Weight};

/// Adds a column's own `values` and `bits` to `weight`.
fn own(weight: &mut Weight, values: u64, bits: u64) {
    weight.values = weight.values.saturating_add(values);
    weight.frame_bits = weight.frame_bits.saturating_add(bits);
}

/// Adds `count` items of `bits` bits each.
fn each(weight: &mut Weight, count: u64, bits: u64) {
    own(weight, count, count.saturating_mul(bits));
}

impl Column {
    /// Adds what the items from `start` to before `end` weigh to `weight`.
    pub(super) fn span(&self, start: usize, end: usize, state: &mut State, weight: &mut Weight) {
        if start >= end {
            return;
        }
        state.visit();
        let count = wide(end - start);
        match self {
            Self::Fixed { bits } | Self::Keyed { bits, .. } => each(weight, count, *bits),
            Self::Bytes { bits, offset } => {
                let bytes = wide(offset(end).saturating_sub(offset(start)));
                let bits = count.saturating_mul(*bits);
                own(weight, count, bits.saturating_add(bytes.saturating_mul(8)));
            }
            Self::Views { views } => {
                for index in start..end {
                    if state.over(weight) {
                        return;
                    }
                    view(views, index, weight);
                    state.visit();
                }
            }
            Self::Sized { size, item } => {
                each(weight, count, VALID);
                let (start, end) = (start.saturating_mul(*size), end.saturating_mul(*size));
                item.span(start, end, state, weight);
            }
            Self::Each { bits, children } => {
                each(weight, count, *bits);
                for child in children {
                    child.span(start, end, state, weight);
                }
            }
            _ => self.named(start, end, state, weight),
        }
    }

    /// As [`Column::span`], for the layouts whose items name others.
    fn named(&self, start: usize, end: usize, state: &mut State, weight: &mut Weight) {
        let count = wide(end - start);
        match self {
            Self::List {
                bits,
                offset,
                nulls: None,
                item,
            } => {
                each(weight, count, *bits);
                item.span(offset(start), offset(end), state, weight);
            }
            Self::List {
                bits,
                offset,
                nulls: Some(nulls),
                item,
            } => {
                each(weight, count, *bits);
                // The rows that are not null, a stretch at a time.
                let valid = nulls.inner().slice(start, end - start);
                for (from, to) in valid.set_slices() {
                    let (from, to) = (start + from, start + to);
                    item.span(offset(from), offset(to), state, weight);
                    state.visit();
                }
            }
            Self::ListView { bits, range, item } => {
                for row in start..end {
                    if state.over(weight) {
                        return;
                    }
                    let (from, to) = range(row);
                    own(weight, 1 + wide(to.saturating_sub(from)), *bits);
                    item.span(from, to, state, weight);
                    state.visit();
                }
            }
            Self::Dense { named, children } => {
                for row in start..end {
                    if state.over(weight) {
                        return;
                    }
                    own(weight, 1, 8 + 32);
                    let (child, item) = named(row);
                    if let Some(child) = children.get(child) {
                        child.span(item, item.saturating_add(1), state, weight);
                    }
                }
            }
            _ => self.runs(start, end, state, weight),
        }
    }

    /// As [`Column::span`], for run-end columns.
    fn runs(&self, start: usize, end: usize, state: &mut State, weight: &mut Weight) {
        let Self::Runs {
            bits,
            reach,
            place,
            values,
        } = self
        else {
            return;
        };
        let mut at = start;
        while at < end && !state.over(weight) {
            let (run, reached) = reach(at);
            // A run that does not reach past the row it is in is no run: nothing more is
            // weighed of the column.
            let rows = reached.min(end).saturating_sub(at);
            if rows == 0 {
                return;
            }
            let upto = at.saturating_add(rows);
            own(weight, wide(rows), 0);
            // A run's end and value are in the frame once, with the first row of the piece in
            // the run.
            let last = state.runs.get_mut(*place);
            if last.is_some_and(|last| last.replace(run) != Some(run)) {
                own(weight, 1, *bits);
                values.span(run, run + 1, state, weight);
            }
            at = upto;
            state.visit();
        }
    }

    /// Adds to `weights` what the values of each dictionary in the column weigh as the frame
    /// of their own they go in, those of a dictionary among another's values too.
    pub(super) fn dictionaries(&self, state: &mut State, weights: &mut Vec<Weight>) {
        match self {
            Self::Keyed { length, values, .. } => {
                let mut weight = Weight::default();
                state.runs.fill(None);
                values.span(0, *length, state, &mut weight);
                weights.push(weight);
                values.dictionaries(state, weights);
            }
            Self::List { item, .. } | Self::Sized { item, .. } | Self::ListView { item, .. } => {
                item.dictionaries(state, weights);
            }
            Self::Runs { values, .. } => values.dictionaries(state, weights),
            Self::Each { children, .. } | Self::Dense { children, .. } => {
                for child in children {
                    child.dictionaries(state, weights);
                }
            }
            Self::Fixed { .. } | Self::Bytes { .. } | Self::Views { .. } => {}
        }
    }
}

/// Adds what view `index` of `views` weighs: itself, and the bytes it names beyond those it
/// holds.
fn view(views: &[u128], index: usize, weight: &mut Weight) {
    let length = views
        .get(index)
        .map_or(0, |view| *view & u128::from(u32::MAX));
    let named = u64::try_from(length).ok().filter(|length| *length > 12);
    let named = named.unwrap_or(0);
    own(weight, 1, 128 + VALID + 8 * named);
    weight.view_bytes = weight.view_bytes.saturating_add(named);
}
