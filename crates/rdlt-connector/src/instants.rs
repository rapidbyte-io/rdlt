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

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowPrimitiveType, Date32Type, Date64Type, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use arrow_array::{Array, ArrayRef, PrimitiveArray};
use arrow_schema::{ArrowError, DataType, TimeUnit};

/// Nanoseconds in a day.
pub const DAY: i128 = 86_400_000_000_000;

/// Milliseconds in a day: a `Date64`'s step.
const DAY_MILLIS: i128 = 86_400_000;

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
/// dates and timestamps, since midnight for times of day, and elapsed for durations; `None` for
/// a type that is not temporal.
#[must_use]
pub fn nanos(data_type: &DataType, value: i128) -> Option<i128> {
    Some(match data_type {
        DataType::Date32 => value * DAY,
        DataType::Date64 => value.div_euclid(DAY_MILLIS) * DAY,
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
/// # Errors
///
/// A conversion that is no widening, such as an instant into a time of day or a zoned instant
/// into a wall-clock time, and a value `to` cannot hold exactly.
pub fn widened(array: &ArrayRef, to: &DataType) -> Result<ArrayRef, ArrowError> {
    let from = array.data_type();
    let unwidened = || ArrowError::CastError(format!("{from} does not widen to {to}"));
    if !widens(from, to) {
        return Err(unwidened());
    }
    // A unit of `to`, in nanoseconds: what one of its values counts.
    let per = nanos(to, 1).ok_or_else(unwidened)?;
    if let (DataType::Timestamp(unit, _), DataType::Timestamp(wider, _)) = (from, to)
        && unit == wider
    {
        // The same count in the same unit, under another zone: the buffers as they are.
        let data = array
            .to_data()
            .into_builder()
            .data_type(to.clone())
            .build()?;
        return Ok(arrow_array::make_array(data));
    }
    let count = |row: usize| -> Result<i128, ArrowError> {
        let value = stored(array.as_ref(), row).map(i128::from);
        let nanos = value
            .and_then(|value| nanos(from, value))
            .ok_or_else(unwidened)?;
        // No widening ends in a `Date64`, whose unit counts no whole nanosecond: none is divided by.
        if nanos.checked_rem(per) != Some(0) {
            return Err(ArrowError::CastError(format!(
                "{nanos} ns is no whole number of a {to}'s units"
            )));
        }
        Ok(nanos / per)
    };
    built(array.as_ref(), to, count)
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

/// The array of `to` holding `count` of each row of `array` that is not null, with `array`'s
/// nulls; refused where a count is beyond what `to` stores.
fn built(
    array: &dyn Array,
    to: &DataType,
    count: impl Fn(usize) -> Result<i128, ArrowError>,
) -> Result<ArrayRef, ArrowError> {
    macro_rules! of {
        ($type:ty) => {{
            let mut values: Vec<<$type as ArrowPrimitiveType>::Native> =
                Vec::with_capacity(array.len());
            for row in 0..array.len() {
                if array.is_null(row) {
                    values.push(Default::default());
                    continue;
                }
                let value = count(row)?;
                values.push(value.try_into().map_err(|_| {
                    ArrowError::CastError(format!("value {value} is beyond what a {to} holds"))
                })?);
            }
            PrimitiveArray::<$type>::try_new(values.into(), array.nulls().cloned())?
        }};
    }
    Ok(match to {
        DataType::Date32 => Arc::new(of!(Date32Type)),
        DataType::Timestamp(unit, zone) => {
            let zone = zone.clone();
            match unit {
                TimeUnit::Second => Arc::new(of!(TimestampSecondType).with_timezone_opt(zone)),
                TimeUnit::Millisecond => {
                    Arc::new(of!(TimestampMillisecondType).with_timezone_opt(zone))
                }
                TimeUnit::Microsecond => {
                    Arc::new(of!(TimestampMicrosecondType).with_timezone_opt(zone))
                }
                TimeUnit::Nanosecond => {
                    Arc::new(of!(TimestampNanosecondType).with_timezone_opt(zone))
                }
            }
        }
        DataType::Time32(TimeUnit::Second) => Arc::new(of!(Time32SecondType)),
        DataType::Time32(_) => Arc::new(of!(Time32MillisecondType)),
        DataType::Time64(TimeUnit::Microsecond) => Arc::new(of!(Time64MicrosecondType)),
        DataType::Time64(_) => Arc::new(of!(Time64NanosecondType)),
        DataType::Duration(TimeUnit::Second) => Arc::new(of!(DurationSecondType)),
        DataType::Duration(TimeUnit::Millisecond) => Arc::new(of!(DurationMillisecondType)),
        DataType::Duration(TimeUnit::Microsecond) => Arc::new(of!(DurationMicrosecondType)),
        DataType::Duration(TimeUnit::Nanosecond) => Arc::new(of!(DurationNanosecondType)),
        other => {
            return Err(ArrowError::CastError(format!(
                "{other} is no temporal type a widening reaches"
            )));
        }
    })
}
