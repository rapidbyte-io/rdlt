//! What one value of each type takes: stored in its plain Arrow type, and rendered as text.

use arrow_schema::{DataType, Fields, IntervalUnit, UnionFields};

use crate::types::TypeKind;

/// Bytes: an offset, as the widest list or string type keeps one a value.
pub(super) const OFFSET: u64 = 8;

/// Bytes: a view.
pub(super) const VIEW: u64 = 16;

/// Bytes: the text of a null in JSON.
pub(super) const NULL_TEXT: u64 = 4;

/// Bytes: the brackets or braces around a nested value's JSON text.
pub(super) const BRACKETS: u64 = 2;

/// Bytes: the longest text of a date, time, timestamp or duration, zone and far years included.
const TEMPORAL_TEXT: u64 = 64;

/// A value of a type whose values all take the same bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Scalar {
    /// Bytes: the value in its type, or in the plain type its logical type names where wider.
    pub(super) slot: u64,
    /// Bytes: the longest text the value renders to, quoted as JSON quotes it, in its type or
    /// in the plain type its logical type names where that renders longer.
    pub(super) text: u64,
    /// The kinds a destination must store natively for the value to stay as it is.
    pub(super) kinds: &'static [TypeKind],
}

/// What a value of `data_type` takes where every value takes the same, and `None` for a type
/// whose values vary: strings, bytes, views and every nested or encoded type.
pub(super) fn scalar(data_type: &DataType) -> Option<Scalar> {
    use TypeKind as K;
    let scalar = |slot, text, kinds| Some(Scalar { slot, text, kinds });
    let decimal = |slot, precision: u8, scale: i8| {
        let digits = u64::from(precision) + u64::from(scale.unsigned_abs());
        scalar(slot, digits + 6, &[K::Decimal])
    };
    match data_type {
        DataType::Null => scalar(1, NULL_TEXT, &[K::Null]),
        DataType::Boolean => scalar(1, 5, &[K::Bool]),
        DataType::Int8 => scalar(1, 4, &[K::Int8]),
        // An unsigned integer is stored in, and rendered from, the next wider signed type.
        DataType::Int16 | DataType::UInt8 => scalar(2, 6, &[K::Int16]),
        DataType::Int32 | DataType::UInt16 => scalar(4, 11, &[K::Int32]),
        DataType::Int64 | DataType::UInt32 => scalar(8, 20, &[K::Int64]),
        DataType::UInt64 => scalar(16, 26, &[K::Decimal]),
        DataType::Float16 | DataType::Float32 => scalar(4, 16, &[K::Float32]),
        DataType::Float64 => scalar(8, 25, &[K::Float64]),
        DataType::Decimal32(precision, scale)
        | DataType::Decimal64(precision, scale)
        | DataType::Decimal128(precision, scale) => decimal(16, *precision, *scale),
        DataType::Decimal256(precision, scale) => decimal(32, *precision, *scale),
        DataType::Date32 | DataType::Date64 => scalar(8, TEMPORAL_TEXT, &[K::Date]),
        DataType::Time32(_) | DataType::Time64(_) => scalar(8, TEMPORAL_TEXT, &[K::Time]),
        DataType::Timestamp(..) => scalar(8, TEMPORAL_TEXT, &[K::Timestamp]),
        DataType::Duration(_) => scalar(8, TEMPORAL_TEXT, &[K::Duration]),
        DataType::Interval(IntervalUnit::YearMonth) => scalar(4, 0, &[]),
        DataType::Interval(IntervalUnit::DayTime) => scalar(8, 0, &[]),
        DataType::Interval(IntervalUnit::MonthDayNano) => scalar(16, 0, &[]),
        DataType::FixedSizeBinary(width) => {
            let width = u64::from(width.unsigned_abs());
            // Bytes render as hex, and sixteen of them as a UUID where the column is one.
            let text = width.saturating_mul(2).saturating_add(BRACKETS).max(38);
            scalar(width.saturating_add(OFFSET), text, &[K::Binary, K::Uuid])
        }
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(..)
        | DataType::Struct(_)
        | DataType::Union(..)
        | DataType::Dictionary(..)
        | DataType::Map(..)
        | DataType::RunEndEncoded(..) => None,
    }
}

/// The bytes a null of `data_type` takes once decoded out of whatever encoding named it: a slot
/// of its type, whose width the schema alone decides.
pub(super) fn null_slot(data_type: &DataType) -> u64 {
    if let Some(scalar) = scalar(data_type) {
        // Its slot, and a byte for its validity.
        return scalar.slot.saturating_add(1);
    }
    let fields = |fields: &Fields| {
        fields
            .iter()
            .map(|field| null_slot(field.data_type()))
            .fold(1, u64::saturating_add)
    };
    let members = |fields: &UnionFields| {
        fields
            .iter()
            .map(|(_, field)| null_slot(field.data_type()))
            .fold(OFFSET, u64::saturating_add)
    };
    match data_type {
        DataType::Utf8View | DataType::BinaryView => VIEW,
        DataType::ListView(_) | DataType::LargeListView(_) => 2 * OFFSET + BRACKETS,
        DataType::FixedSizeList(item, size) => u64::from(size.unsigned_abs())
            .saturating_mul(null_slot(item.data_type()))
            .saturating_add(OFFSET + BRACKETS),
        DataType::Struct(members) => fields(members),
        DataType::Union(fields, _) => members(fields),
        DataType::Dictionary(_, values) => null_slot(values),
        DataType::RunEndEncoded(_, values) => null_slot(values.data_type()).saturating_add(4),
        // A null string, bytes value or list is an offset, and its text `null`.
        _ => OFFSET + BRACKETS,
    }
}

/// Bytes: at most what the text of a null of `data_type` is measured at, a row, whose array
/// the cost model measures as JSON text: the larger of a scalar's slot and its longest text, a
/// string's offsets and quotes, a struct's names and its fields', a list's offsets and brackets.
///
/// What a builder of a column's text reserves for a null converted to the column's type is no
/// more, so a value typed null, or a field a struct lacks, is charged it.
pub(super) fn null_text(data_type: &DataType) -> u64 {
    // A row's validity, in a byte of its own at the most.
    let validity = 1;
    let text = if let Some(scalar) = scalar(data_type) {
        scalar.slot.max(scalar.text + OFFSET).max(1)
    } else {
        match data_type {
            DataType::Utf8View | DataType::BinaryView => VIEW + OFFSET + BRACKETS,
            DataType::Struct(fields) => fields
                .iter()
                .map(|field| null_text(field.data_type()))
                .fold(keys(fields), u64::saturating_add),
            DataType::FixedSizeList(item, size) => {
                let size = u64::from(size.unsigned_abs());
                size.saturating_mul(1 + null_text(item.data_type()))
                    .saturating_add(OFFSET + BRACKETS)
            }
            DataType::Union(fields, _) => fields
                .iter()
                .map(|(_, field)| null_text(field.data_type()))
                .fold(2 * OFFSET, u64::saturating_add),
            DataType::Dictionary(_, values) => null_text(values).saturating_add(OFFSET),
            DataType::RunEndEncoded(_, values) => {
                null_text(values.data_type()).saturating_add(OFFSET)
            }
            // Strings, bytes and lists of every layout: their offsets and their brackets.
            _ => 2 * OFFSET + BRACKETS,
        }
    };
    text.saturating_add(validity)
}

/// Bytes: the widest null slot of the items of any list `data_type` holds, at any depth; none
/// where it holds no list.
pub(super) fn item_slot(data_type: &DataType) -> u64 {
    match data_type {
        DataType::List(item)
        | DataType::LargeList(item)
        | DataType::ListView(item)
        | DataType::LargeListView(item)
        | DataType::FixedSizeList(item, _)
        | DataType::Map(item, _) => null_slot(item.data_type()).max(item_slot(item.data_type())),
        DataType::Struct(fields) => fields
            .iter()
            .map(|field| item_slot(field.data_type()))
            .max()
            .unwrap_or(0),
        _ => 0,
    }
}

/// The bytes the names of a struct's `fields` add to every row of its JSON text: each quoted and
/// escaped, with its colon and comma, between the braces.
pub(super) fn keys(fields: &Fields) -> u64 {
    fields
        .iter()
        .map(|field| key(field.name()))
        .fold(BRACKETS, u64::saturating_add)
}

/// The bytes a field named `name` adds to a row of its struct's JSON text beside its value: the
/// name quoted and escaped, its colon and its comma.
pub(super) fn key(name: &str) -> u64 {
    let name = name.as_bytes();
    count(name.len())
        .saturating_add(escapes(name))
        .saturating_add(4)
}

/// The bytes escaping adds to `text` in a JSON string: a quote, a backslash and the short
/// control escapes double, and any other control character becomes six bytes.
pub(super) fn escapes(text: &[u8]) -> u64 {
    let extra: usize = text
        .iter()
        .map(|byte| match byte {
            b'"' | b'\\' | 0x08 | 0x09 | 0x0a | 0x0c | 0x0d => 1,
            0x00..=0x1f => 5,
            _ => 0,
        })
        .sum();
    count(extra)
}

/// A count as the `u64` costs are measured in.
pub(super) fn count(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}
