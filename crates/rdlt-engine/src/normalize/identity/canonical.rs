//! Values in the forms identity encodes them in, whatever type holds them: temporal values in
//! nanoseconds and decimals as their shortest text.

use arrow_array::ArrayRef;
use arrow_array::cast::AsArray;
use arrow_schema::{ArrowError, DataType};

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

/// Each value of `array`, of a temporal type, in nanoseconds: since the epoch for dates and
/// timestamps, since midnight for times, and elapsed for durations.
pub(super) fn nanoseconds(array: &ArrayRef) -> Result<Vec<Option<i128>>, ArrowError> {
    use arrow_schema::TimeUnit;
    let per = |unit: &TimeUnit| match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    };
    let (values, per): (ArrayRef, i128) = match array.data_type() {
        DataType::Date32 => (
            arrow_cast::cast(array, &DataType::Int32)?,
            86_400_000_000_000,
        ),
        DataType::Date64 => (arrow_cast::cast(array, &DataType::Int64)?, 1_000_000),
        DataType::Time32(unit)
        | DataType::Time64(unit)
        | DataType::Timestamp(unit, _)
        | DataType::Duration(unit) => (arrow_cast::cast(array, &DataType::Int64)?, per(unit)),
        other => return Err(ArrowError::CastError(format!("{other} is not temporal"))),
    };
    let values = arrow_cast::cast(&values, &DataType::Int64)?;
    Ok(values
        .as_primitive::<arrow_array::types::Int64Type>()
        .iter()
        .map(|value| value.map(|value| i128::from(value) * per))
        .collect())
}

/// Each value of `array`, of a decimal type, as its text without trailing zeros after the point.
pub(super) fn decimals(array: &ArrayRef) -> Result<Vec<Option<String>>, ArrowError> {
    let scale = match array.data_type() {
        DataType::Decimal32(_, scale)
        | DataType::Decimal64(_, scale)
        | DataType::Decimal128(_, scale)
        | DataType::Decimal256(_, scale) => *scale,
        other => return Err(ArrowError::CastError(format!("{other} is not a decimal"))),
    };
    let wide = arrow_cast::cast(array, &DataType::Decimal256(76, scale))?;
    let wide = wide.as_primitive::<arrow_array::types::Decimal256Type>();
    Ok(wide
        .iter()
        .map(|value| value.map(|value| scaled(&value.to_string(), scale)))
        .collect())
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
