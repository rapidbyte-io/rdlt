//! Values in the forms identity encodes them in, whatever type holds them: temporal values in
//! nanoseconds and decimals as their shortest text, written where they go.

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_buffer::{ScalarBuffer, i256};
use arrow_schema::DataType;

use super::{ELAPSED, INSTANT, TIME_OF_DAY};

/// Digits: the most an integer of 256 bits has, beside its sign.
pub(super) const DIGITS: usize = 77;

/// The tag of a temporal type's kind: dates and timestamps are instants.
pub(super) fn temporal_tag(data_type: &DataType) -> Option<u8> {
    match data_type {
        DataType::Date32 | DataType::Date64 | DataType::Timestamp(..) => Some(INSTANT),
        DataType::Time32(_) | DataType::Time64(_) => Some(TIME_OF_DAY),
        DataType::Duration(_) => Some(ELAPSED),
        _ => None,
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

    /// Appends the value at `index`, of a decimal array of `scale`, to `out` as its text without
    /// trailing zeros after the point.
    pub(super) fn write_decimal(&self, scale: i8, index: usize, out: &mut Vec<u8>) {
        let mut buffer = [0_u8; DIGITS];
        let (negative, digits) = match self {
            Self::I256(values) => match values[index].to_i128() {
                Some(value) => digits(value, &mut buffer),
                None => wide_digits(values[index], &mut buffer),
            },
            narrower => digits(narrower.value(index), &mut buffer),
        };
        scaled(out, negative, digits, scale);
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

/// Whether `value` is negative, and its magnitude's decimal digits, written at the end of
/// `buffer`.
pub(super) fn digits(value: i128, buffer: &mut [u8; DIGITS]) -> (bool, &[u8]) {
    let mut at = DIGITS;
    let mut rest = value.unsigned_abs();
    loop {
        at -= 1;
        buffer[at] = b'0' + u8::try_from(rest % 10).unwrap_or(0);
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    (value < 0, &buffer[at..])
}

/// As [`digits`], for a 256-bit integer beyond 128 bits.
fn wide_digits(value: i256, buffer: &mut [u8; DIGITS]) -> (bool, &[u8]) {
    let ten = i256::from_i128(10);
    let mut at = DIGITS;
    let mut rest = value;
    loop {
        at -= 1;
        // A remainder takes its dividend's sign, so its magnitude is the digit.
        let digit = rest.wrapping_rem(ten).as_i128().unsigned_abs();
        buffer[at] = b'0' + u8::try_from(digit).unwrap_or(0);
        rest = rest.wrapping_div(ten);
        if rest == i256::ZERO {
            break;
        }
    }
    (value.is_negative(), &buffer[at..])
}

/// Appends the decimal whose unscaled magnitude has `digits`, negative where `negative` says,
/// with `scale` digits after the point and no trailing zeros after it.
fn scaled(out: &mut Vec<u8>, negative: bool, digits: &[u8], scale: i8) {
    if negative {
        out.push(b'-');
    }
    let Ok(scale) = usize::try_from(scale) else {
        out.extend_from_slice(digits);
        out.extend(std::iter::repeat_n(b'0', usize::from(scale.unsigned_abs())));
        return;
    };
    // The digits after enough zeros that a whole digit leads the scale's.
    let zeros = (scale + 1).saturating_sub(digits.len());
    let digit = |at: usize| at.checked_sub(zeros).map_or(b'0', |at| digits[at]);
    let width = zeros + digits.len();
    let point = width - scale;
    out.extend((0..point).map(digit));
    let end = (point..width)
        .rfind(|at| digit(*at) != b'0')
        .map_or(point, |at| at + 1);
    if end > point {
        out.push(b'.');
        out.extend((point..end).map(digit));
    }
}
