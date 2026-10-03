//! Values in the forms identity encodes them in, whatever type holds them: temporal values in
//! nanoseconds and decimals as their shortest text.

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_buffer::{ScalarBuffer, i256};
use arrow_schema::DataType;

use super::{ELAPSED, INSTANT, TIME_OF_DAY};

/// The tag of a temporal type's kind: dates and timestamps are instants.
pub(super) fn temporal_tag(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(..) => Some(INSTANT),
        DataType::Time32(_) | DataType::Time64(_) => Some(TIME_OF_DAY),
        DataType::Duration(_) => Some(ELAPSED),
        _ => None,
    }
}

/// How a temporal type's values count nanoseconds since their origin: the epoch for dates and
/// timestamps, midnight for times; durations count them elapsed.
#[derive(Clone, Copy, Debug)]
pub(super) struct Since {
    /// Nanoseconds in one unit of the type.
    unit: i128,
    /// Units in the least step a value takes: a day of milliseconds for a `Date64`, whose
    /// values name the day they are within; one for any other type.
    step: i128,
}

impl Since {
    /// How values of the temporal `data_type` count; `None` for another type.
    pub(super) fn of(data_type: &DataType) -> Option<Self> {
        use arrow_schema::TimeUnit;
        let per = |unit: &TimeUnit| match unit {
            TimeUnit::Second => 1_000_000_000,
            TimeUnit::Millisecond => 1_000_000,
            TimeUnit::Microsecond => 1_000,
            TimeUnit::Nanosecond => 1,
        };
        let (unit, step) = match data_type {
            DataType::Date32 => (86_400_000_000_000, 1),
            DataType::Date64 => (1_000_000, 86_400_000),
            DataType::Time32(unit)
            | DataType::Time64(unit)
            | DataType::Timestamp(unit, _)
            | DataType::Duration(unit) => (per(unit), 1),
            _ => return None,
        };
        Some(Self { unit, step })
    }

    /// The nanoseconds `value`, as its type stores it, is since its origin: the start of the step
    /// it is within, which a value of 64 bits or less always has in 128 bits.
    pub(super) fn nanoseconds(self, value: i128) -> i128 {
        value.div_euclid(self.step) * self.step * self.unit
    }
}

/// The values of an integer, temporal or decimal array, as its type stores them: the array's own
/// buffer, read a value at a time.
///
/// A copy of the array in one wide type would take several times the array for narrow values.
#[derive(Clone, Debug)]
pub(super) enum Stored {
    I8(ScalarBuffer<i8>),
    I16(ScalarBuffer<i16>),
    I32(ScalarBuffer<i32>),
    I64(ScalarBuffer<i64>),
    U8(ScalarBuffer<u8>),
    U16(ScalarBuffer<u16>),
    U32(ScalarBuffer<u32>),
    I128(ScalarBuffer<i128>),
    I256(ScalarBuffer<i256>),
}

impl Stored {
    /// The values of `array`; `None` for an array of any other type.
    pub(super) fn of(array: &dyn Array) -> Option<Self> {
        use arrow_array::types as t;
        use arrow_schema::TimeUnit as U;
        macro_rules! values {
            ($kind:ident, $type:ty) => {
                Some(Self::$kind(array.as_primitive::<$type>().values().clone()))
            };
        }
        match array.data_type() {
            DataType::Int8 => values!(I8, t::Int8Type),
            DataType::Int16 => values!(I16, t::Int16Type),
            DataType::Int32 => values!(I32, t::Int32Type),
            DataType::Int64 => values!(I64, t::Int64Type),
            DataType::UInt8 => values!(U8, t::UInt8Type),
            DataType::UInt16 => values!(U16, t::UInt16Type),
            DataType::UInt32 => values!(U32, t::UInt32Type),
            DataType::Date32 => values!(I32, t::Date32Type),
            DataType::Date64 => values!(I64, t::Date64Type),
            DataType::Time32(U::Second) => values!(I32, t::Time32SecondType),
            DataType::Time32(_) => values!(I32, t::Time32MillisecondType),
            DataType::Time64(U::Microsecond) => values!(I64, t::Time64MicrosecondType),
            DataType::Time64(_) => values!(I64, t::Time64NanosecondType),
            DataType::Timestamp(U::Second, _) => values!(I64, t::TimestampSecondType),
            DataType::Timestamp(U::Millisecond, _) => values!(I64, t::TimestampMillisecondType),
            DataType::Timestamp(U::Microsecond, _) => values!(I64, t::TimestampMicrosecondType),
            DataType::Timestamp(U::Nanosecond, _) => values!(I64, t::TimestampNanosecondType),
            DataType::Duration(U::Second) => values!(I64, t::DurationSecondType),
            DataType::Duration(U::Millisecond) => values!(I64, t::DurationMillisecondType),
            DataType::Duration(U::Microsecond) => values!(I64, t::DurationMicrosecondType),
            DataType::Duration(U::Nanosecond) => values!(I64, t::DurationNanosecondType),
            DataType::Decimal32(..) => values!(I32, t::Decimal32Type),
            DataType::Decimal64(..) => values!(I64, t::Decimal64Type),
            DataType::Decimal128(..) => values!(I128, t::Decimal128Type),
            DataType::Decimal256(..) => values!(I256, t::Decimal256Type),
            _ => None,
        }
    }

    /// The value at `index`, of an integer or temporal array; zero for a decimal beyond 128 bits.
    pub(super) fn value(&self, index: usize) -> i128 {
        match self {
            Self::I8(values) => values[index].into(),
            Self::I16(values) => values[index].into(),
            Self::I32(values) => values[index].into(),
            Self::I64(values) => values[index].into(),
            Self::U8(values) => values[index].into(),
            Self::U16(values) => values[index].into(),
            Self::U32(values) => values[index].into(),
            Self::I128(values) => values[index],
            Self::I256(values) => values[index].to_i128().unwrap_or(0),
        }
    }

    /// The value at `index`, of a decimal array of `scale`, as its text without trailing zeros
    /// after the point.
    pub(super) fn decimal(&self, scale: i8, index: usize) -> String {
        let digits = match self {
            Self::I256(values) => values[index].to_string(),
            narrower => narrower.value(index).to_string(),
        };
        scaled(&digits, scale)
    }

    /// Whether the values are `array`'s own, shared and not copied.
    #[cfg(test)]
    pub(super) fn shares(&self, array: &dyn Array) -> bool {
        let data = array.to_data();
        let held = match self {
            Self::I8(values) => values.inner(),
            Self::I16(values) => values.inner(),
            Self::I32(values) => values.inner(),
            Self::I64(values) => values.inner(),
            Self::U8(values) => values.inner(),
            Self::U16(values) => values.inner(),
            Self::U32(values) => values.inner(),
            Self::I128(values) => values.inner(),
            Self::I256(values) => values.inner(),
        };
        held.data_ptr() == data.buffers()[0].data_ptr()
    }
}

/// The scale of the decimal `data_type`.
pub(super) fn decimal_scale(data_type: &DataType) -> Option<i8> {
    match data_type {
        DataType::Decimal32(_, scale)
        | DataType::Decimal64(_, scale)
        | DataType::Decimal128(_, scale)
        | DataType::Decimal256(_, scale) => Some(*scale),
        _ => None,
    }
}

/// `digits`, an unscaled value, with `scale` digits after the point, without trailing zeros
/// after it.
fn scaled(digits: &str, scale: i8) -> String {
    let (sign, digits) = digits
        .strip_prefix('-')
        .map_or(("", digits), |rest| ("-", rest));
    let Ok(scale) = usize::try_from(scale) else {
        let zeros = "0".repeat(usize::from(scale.unsigned_abs()));
        return format!("{sign}{digits}{zeros}");
    };
    if scale == 0 {
        return format!("{sign}{digits}");
    }
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (whole, fraction) = padded.split_at(padded.len() - scale);
    let fraction = fraction.trim_end_matches('0');
    if fraction.is_empty() {
        format!("{sign}{whole}")
    } else {
        format!("{sign}{whole}.{fraction}")
    }
}
