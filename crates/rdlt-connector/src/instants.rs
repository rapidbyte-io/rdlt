//! What a temporal value denotes, in nanoseconds, and a temporal column as the wider temporal
//! type its column takes.
//!
//! A date denotes its midnight in UTC, in whatever zone a column of timestamps holding it shows
//! its instants, as a change time does; a timestamp, zoned or not, denotes the instant its value
//! counts since the epoch; a `Date64` holding part of a day denotes the day it is within. A
//! widening converts every value to the same instant, time of day or duration in a finer unit,
//! so a value's identity, its history hash and what a destination holds of it never move. The
//! engine, its reference destinations and identity all convert through this module.

#[cfg(test)]
mod tests;
mod text;

pub use text::Renderer;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowPrimitiveType, Date32Type, Date64Type, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Int32Type, Int64Type, Time32MillisecondType,
    Time32SecondType, Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use arrow_array::{Array, ArrayRef};
use arrow_schema::{ArrowError, DataType, TimeUnit};

/// Nanoseconds in a day.
pub const DAY: i128 = 86_400_000_000_000;

/// Milliseconds in a day: a `Date64`'s step.
const DAY_MILLIS: i64 = 86_400_000;

/// Nanoseconds in one `unit`.
#[must_use]
pub fn unit_nanos(unit: TimeUnit) -> i128 {
    match unit {
        TimeUnit::Second => 1_000_000_000,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    }
}

/// The nanoseconds `value`, as a column of `data_type` stores it, denotes: since the epoch for
/// dates and timestamps (a `Date64` floored to its day), since midnight for times of day, and
/// elapsed for durations; `None` for a type that is not temporal.
#[must_use]
pub fn nanos(data_type: &DataType, value: i128) -> Option<i128> {
    Some(match data_type {
        DataType::Date32 => value * DAY,
        DataType::Date64 => value.div_euclid(i128::from(DAY_MILLIS)) * DAY,
        DataType::Timestamp(unit, _)
        | DataType::Time32(unit)
        | DataType::Time64(unit)
        | DataType::Duration(unit) => value * unit_nanos(*unit),
        _ => return None,
    })
}

/// The value at `row` of `array`, a plain temporal array, as its type stores it; `None` for an
/// array of another type.
#[must_use]
pub fn stored(array: &dyn Array, row: usize) -> Option<i64> {
    macro_rules! at {
        ($type:ty) => {
            i64::from(array.as_primitive::<$type>().value(row))
        };
    }
    Some(match array.data_type() {
        DataType::Date32 => at!(Date32Type),
        DataType::Date64 => at!(Date64Type),
        DataType::Time32(TimeUnit::Second) => at!(Time32SecondType),
        DataType::Time32(_) => at!(Time32MillisecondType),
        DataType::Time64(TimeUnit::Microsecond) => at!(Time64MicrosecondType),
        DataType::Time64(_) => at!(Time64NanosecondType),
        DataType::Timestamp(TimeUnit::Second, _) => at!(TimestampSecondType),
        DataType::Timestamp(TimeUnit::Millisecond, _) => at!(TimestampMillisecondType),
        DataType::Timestamp(TimeUnit::Microsecond, _) => at!(TimestampMicrosecondType),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => at!(TimestampNanosecondType),
        DataType::Duration(TimeUnit::Second) => at!(DurationSecondType),
        DataType::Duration(TimeUnit::Millisecond) => at!(DurationMillisecondType),
        DataType::Duration(TimeUnit::Microsecond) => at!(DurationMicrosecondType),
        DataType::Duration(TimeUnit::Nanosecond) => at!(DurationNanosecondType),
        _ => return None,
    })
}

/// `array`, a plain temporal array, as `to`: each value the same instant, time of day or
/// duration, exactly.
///
/// A widening multiplies each value by what one of `from`'s units counts in `to`'s, a whole
/// number since `to`'s unit is as fine or finer; a `Date64` is first the day it is within.
///
/// # Errors
///
/// A conversion that is no widening, such as an instant into a time of day or a zoned instant
/// into a wall-clock time, and a value `to` cannot hold.
pub fn widened(array: &ArrayRef, to: &DataType) -> Result<ArrayRef, ArrowError> {
    use DataType as T;
    let from = array.data_type();
    let unwidened = || ArrowError::CastError(format!("{from} does not widen to {to}"));
    if !widens(from, to) {
        return Err(unwidened());
    }
    let unit = match from {
        T::Date32 | T::Date64 => DAY,
        _ => nanos(from, 1).ok_or_else(unwidened)?,
    };
    let factor = nanos(to, 1)
        .and_then(|finer| i64::try_from(unit / finer).ok())
        .ok_or_else(unwidened)?;
    match from {
        // The same count in the same unit, under another zone.
        T::Timestamp(..) if factor == 1 => retyped(array, to),
        T::Date64 => scaled::<Int64Type>(array, to, |millis| {
            millis.div_euclid(DAY_MILLIS).checked_mul(factor)
        }),
        T::Date32 | T::Time32(_) => {
            scaled::<Int32Type>(array, to, |value| value.checked_mul(factor))
        }
        _ => scaled::<Int64Type>(array, to, |value| value.checked_mul(factor)),
    }
}

/// Whether `data_type` is a temporal type: a date, a time of day, a timestamp or a duration.
#[must_use]
pub fn is_temporal(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Date32
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(..)
            | DataType::Duration(_)
    )
}

/// Whether the temporal type `from` widens to `to`: a date to a timestamp or to the day it is
/// within, a timestamp to a finer one in the same zone or from no zone to one, and a time of day
/// or a duration to a finer one.
#[must_use]
pub fn widens(from: &DataType, to: &DataType) -> bool {
    use DataType as T;
    let finer = |from: &TimeUnit, to: &TimeUnit| unit_nanos(*to) <= unit_nanos(*from);
    match (from, to) {
        (T::Date32 | T::Date64, T::Timestamp(..)) | (T::Date64, T::Date32) => true,
        // A zone only shows an instant; a wall-clock value without one counts from the epoch
        // as a zoned one does. An instant is no wall-clock time, so a zone is never dropped.
        (T::Timestamp(from, from_zone), T::Timestamp(to, zone)) => {
            finer(from, to) && (zone.is_some() || from_zone.is_none())
        }
        (T::Time32(from) | T::Time64(from), T::Time32(to) | T::Time64(to))
        | (T::Duration(from), T::Duration(to)) => finer(from, to),
        _ => false,
    }
}

/// `array` as `to`, its buffers as they are.
fn retyped(array: &ArrayRef, to: &DataType) -> Result<ArrayRef, ArrowError> {
    let data = array
        .to_data()
        .into_builder()
        .data_type(to.clone())
        .build()?;
    Ok(arrow_array::make_array(data))
}

/// `array`, stored as `I`'s integers, as `to`, each value `widen`ed into `to`'s integer and
/// `array`'s nulls kept; a null's slot is never read.
fn scaled<I>(
    array: &ArrayRef,
    to: &DataType,
    widen: impl Fn(i64) -> Option<i64>,
) -> Result<ArrayRef, ArrowError>
where
    I: ArrowPrimitiveType,
    I::Native: Into<i64>,
{
    let stored = retyped(array, &I::DATA_TYPE)?;
    let values = stored.as_primitive::<I>();
    let beyond =
        |value: i64| ArrowError::CastError(format!("value {value} is beyond what a {to} holds"));
    let data = match to {
        DataType::Date32 | DataType::Time32(_) => values
            .try_unary::<_, Int32Type, _>(|value| {
                let value = value.into();
                widen(value)
                    .and_then(|widened| i32::try_from(widened).ok())
                    .ok_or_else(|| beyond(value))
            })?
            .into_data(),
        _ => values
            .try_unary::<_, Int64Type, _>(|value| {
                let value = value.into();
                widen(value).ok_or_else(|| beyond(value))
            })?
            .into_data(),
    };
    retyped(&arrow_array::make_array(data), to)
}
