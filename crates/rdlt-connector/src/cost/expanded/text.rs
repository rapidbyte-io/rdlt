//! What strings, bytes and views become: as they are, as JSON strings, or as hex.

use std::ops::Range;

use arrow_array::cast::AsArray;
use arrow_array::{Array, OffsetSizeTrait};
use arrow_buffer::ArrowNativeType;

use super::{Meter, SCANNED, Within};
use crate::cost::widths::{BRACKETS, OFFSET, VIEW, count, escapes};
use crate::types::TypeKind;

/// How a string or bytes value is rendered.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Text {
    /// As it is.
    Plain,
    /// As a JSON string: quoted, its quotes and control characters escaped.
    Escaped,
    /// As hex, quoted.
    Hex,
}

impl Meter {
    /// Whether bytes measured `within` are rendered as hex: inside a nested value, in a column
    /// stored as text, or, where no table is known yet, for a destination that stores no bytes
    /// as they are.
    fn hex(&self, within: Within<'_>) -> bool {
        if within.planned {
            within.nested || within.text
        } else {
            within.nested || self.rendering.renders(TypeKind::Binary)
        }
    }

    pub(super) fn strings<O: OffsetSizeTrait>(
        &mut self,
        array: &dyn Array,
        rows: Range<usize>,
        within: Within<'_>,
    ) {
        let strings = array.as_string::<O>();
        let (offsets, data) = (strings.value_offsets(), strings.value_data());
        // A string outside a nested value is stored as it is, whatever stores it.
        let text = if within.nested {
            Text::Escaped
        } else {
            Text::Plain
        };
        if within.planned && within.text && !within.nested {
            // A column stored as text holds its strings rendered beside them.
            self.spanned(offsets, data, rows.clone(), Text::Plain);
        }
        self.spanned(offsets, data, rows, text);
    }

    pub(super) fn bytes<O: OffsetSizeTrait>(
        &mut self,
        array: &dyn Array,
        rows: Range<usize>,
        within: Within<'_>,
    ) {
        let bytes = array.as_binary::<O>();
        let (offsets, data) = (bytes.value_offsets(), bytes.value_data());
        let hex = self.hex(within);
        if within.planned && hex && !within.nested {
            // Lowering holds the bytes decoded beside their text.
            self.spanned(offsets, data, rows.clone(), Text::Plain);
        }
        let text = if hex { Text::Hex } else { Text::Plain };
        self.spanned(offsets, data, rows, text);
    }

    /// Measures the `rows` values lying in `data` between consecutive `offsets`.
    fn spanned<O: ArrowNativeType>(
        &mut self,
        offsets: &[O],
        data: &[u8],
        rows: Range<usize>,
        text: Text,
    ) {
        let offset = |row: usize| offsets.get(row).map_or(0, |offset| offset.as_usize());
        let (start, end) = (offset(rows.start), offset(rows.end));
        let bytes = count(end.saturating_sub(start));
        self.times(rows.len() + 1, OFFSET);
        self.add(bytes);
        match text {
            Text::Plain => {}
            Text::Hex => {
                self.add(bytes);
                self.times(rows.len(), BRACKETS);
            }
            Text::Escaped => {
                self.times(rows.len(), BRACKETS);
                if !self.over() {
                    self.steps = self.steps.saturating_add(bytes / SCANNED);
                    self.add(escapes(data.get(start..end).unwrap_or_default()));
                }
            }
        }
    }

    pub(super) fn string_views(
        &mut self,
        array: &dyn Array,
        rows: Range<usize>,
        within: Within<'_>,
    ) {
        let strings = array.as_string_view();
        self.times(rows.len(), VIEW + OFFSET);
        let nested = within.nested;
        // A column stored as text holds its strings rendered beside them.
        let copies = if within.planned && within.text && !nested {
            self.times(rows.len(), OFFSET);
            2
        } else {
            1
        };
        for row in rows {
            if self.over() {
                return;
            }
            self.step();
            let length = u64::from(length(strings.views()[row]));
            self.add(copies * length);
            if nested {
                self.add(BRACKETS);
                if !self.over() {
                    self.steps = self.steps.saturating_add(length / SCANNED);
                    self.add(escapes(strings.value(row).as_bytes()));
                }
            }
        }
    }

    pub(super) fn binary_views(
        &mut self,
        array: &dyn Array,
        rows: Range<usize>,
        within: Within<'_>,
    ) {
        let hex = self.hex(within);
        let views = array.as_binary_view().views();
        self.times(rows.len(), VIEW + OFFSET);
        // Lowering holds the bytes decoded beside their text.
        let decoded = u64::from(hex && within.planned && !within.nested);
        for row in rows {
            if self.over() {
                return;
            }
            self.step();
            let length = u64::from(length(views[row]));
            self.add(decoded * length);
            self.add(if hex { 2 * length + BRACKETS } else { length });
        }
    }
}

/// The bytes the value a view names is long.
fn length(view: u128) -> u32 {
    u32::try_from(view & u128::from(u32::MAX)).unwrap_or(u32::MAX)
}
