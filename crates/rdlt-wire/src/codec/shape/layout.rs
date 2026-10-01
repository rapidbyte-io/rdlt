//! How each Arrow type lays a column out in a record batch message: its node, its buffers and
//! the columns nested in it, as Arrow's reader consumes them.

use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano, i256};
use arrow_schema::{DataType, Fields, IntervalUnit, UnionFields, UnionMode};

/// A column's layout after its node.
pub(super) enum Layout<'a> {
    /// No buffer.
    Null,
    /// Validity, then values of `width` bytes each.
    Fixed {
        /// Bytes in one value.
        width: u64,
        /// The alignment Arrow needs of the values' buffer.
        alignment: usize,
    },
    /// Validity, then a bit a value.
    Bits,
    /// Validity, offsets of `offset` bytes each, then the bytes they index.
    Bytes {
        /// Bytes in one offset.
        offset: u64,
    },
    /// Validity, views, then as many data buffers as the message counts for the column.
    Views,
    /// Validity and offsets of `offset` bytes each, then the item's column.
    List {
        /// Bytes in one offset.
        offset: u64,
        /// The item's type.
        item: &'a DataType,
    },
    /// Validity, offsets and sizes, then the item's column.
    ListView {
        /// Whether an offset or a size takes eight bytes, not four.
        large: bool,
        /// The item's type.
        item: &'a DataType,
    },
    /// Validity, then the item's column.
    FixedSizeList(&'a DataType),
    /// Validity, then each field's column.
    Struct(&'a Fields),
    /// A type id a value, an offset a value when `dense`, then each field's column.
    Union {
        /// The fields a value may be of.
        fields: &'a UnionFields,
        /// Whether each value has an offset into its field's column.
        dense: bool,
    },
    /// No buffer; the run ends' column, then the values'.
    RunEnd {
        /// The run ends' type.
        ends: &'a DataType,
        /// The values' type.
        values: &'a DataType,
    },
}

/// The layout of a column of `data_type`; a dictionary column's is its keys'.
pub(super) fn layout(data_type: &DataType) -> Layout<'_> {
    use DataType as T;
    match data_type {
        T::Null => Layout::Null,
        T::Boolean => Layout::Bits,
        T::Int8 | T::UInt8 => fixed::<u8>(),
        T::Int16 | T::UInt16 | T::Float16 => fixed::<u16>(),
        T::Int32
        | T::UInt32
        | T::Float32
        | T::Date32
        | T::Time32(_)
        | T::Decimal32(..)
        | T::Interval(IntervalUnit::YearMonth) => fixed::<u32>(),
        T::Int64
        | T::UInt64
        | T::Float64
        | T::Date64
        | T::Time64(_)
        | T::Timestamp(..)
        | T::Duration(_)
        | T::Decimal64(..) => fixed::<u64>(),
        T::Interval(IntervalUnit::DayTime) => fixed::<IntervalDayTime>(),
        T::Interval(IntervalUnit::MonthDayNano) => fixed::<IntervalMonthDayNano>(),
        T::Decimal128(..) => fixed::<i128>(),
        T::Decimal256(..) => fixed::<i256>(),
        T::FixedSizeBinary(width) => bytes(u64::try_from(*width).unwrap_or(0)),
        T::Utf8 | T::Binary => Layout::Bytes { offset: 4 },
        T::LargeUtf8 | T::LargeBinary => Layout::Bytes { offset: 8 },
        T::Utf8View | T::BinaryView => Layout::Views,
        T::List(item) | T::Map(item, _) => list(4, item.data_type()),
        T::LargeList(item) => list(8, item.data_type()),
        T::ListView(item) => list_view(false, item.data_type()),
        T::LargeListView(item) => list_view(true, item.data_type()),
        T::FixedSizeList(item, _) => Layout::FixedSizeList(item.data_type()),
        T::Struct(fields) => Layout::Struct(fields),
        T::Union(fields, mode) => Layout::Union {
            fields,
            dense: *mode == UnionMode::Dense,
        },
        T::RunEndEncoded(ends, values) => Layout::RunEnd {
            ends: ends.data_type(),
            values: values.data_type(),
        },
        T::Dictionary(keys, _) => layout(keys),
    }
}

/// The layout of a column of values held as `T`.
fn fixed<T>() -> Layout<'static> {
    Layout::Fixed {
        width: u64::try_from(size_of::<T>()).unwrap_or(u64::MAX),
        alignment: align_of::<T>(),
    }
}

/// The layout of a column of values of `width` bytes each, aligned as bytes are.
fn bytes(width: u64) -> Layout<'static> {
    Layout::Fixed {
        width,
        alignment: 1,
    }
}

fn list(offset: u64, item: &DataType) -> Layout<'_> {
    Layout::List { offset, item }
}

fn list_view(large: bool, item: &DataType) -> Layout<'_> {
    Layout::ListView { large, item }
}
