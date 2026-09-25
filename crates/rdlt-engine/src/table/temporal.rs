//! Temporal values across types and as text, exactly: a date or a wall-clock time placed in a
//! zone, and text as Arrow renders it wherever it can and beyond the years Arrow renders.
//!
//! Arrow multiplies dates into nanoseconds without checking, so a far date wraps silently; here
//! every product is checked and a value beyond the `i64` of its unit is refused. A wall-clock
//! value in a zone is the instant it names there; where the zone's clocks repeat it, the earlier
//! one, and where they skip it, the instant the offset in force then gives.
//!
//! Arrow renders dates, times and timestamps through `chrono`, which holds a few hundred thousand
//! years, and durations through `chrono`'s `Duration`, which holds fewer milliseconds than an
//! `i64` does; beyond them it fails, or writes `<invalid>`. Durations are always rendered here, as
//! Arrow renders them in range: `PT1.5S`. Other values Arrow cannot render are rendered here in
//! UTC, with as many year digits as they need.

#[cfg(test)]
mod tests;
mod text;

pub(crate) use text::{Renderer, text};

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::temporal_conversions::as_datetime;
use arrow_array::timezone::Tz;
use arrow_array::types::{
    Date32Type, Date64Type, DurationMicrosecondType, DurationMillisecondType,
    DurationNanosecondType, DurationSecondType, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
};
use arrow_array::{
    Array, ArrayRef, Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
    Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{ArrowError, DataType, TimeUnit};
use chrono::{LocalResult, NaiveDateTime, Offset as _, TimeDelta, TimeZone as _};

/// Nanoseconds in a day.
const DAY: i128 = 86_400 * NANOS_PER_SECOND;
const NANOS_PER_SECOND: i128 = 1_000_000_000;

/// `array`, dates, as timestamps of `unit` in `zone`: each date's midnight there.
pub(crate) fn midnights(
    array: &ArrayRef,
    unit: TimeUnit,
    zone: Option<&Arc<str>>,
) -> Result<ArrayRef, ArrowError> {
    // A `Date64` may hold more days than a `Date32`, so its days are read from it directly.
    let days: Vec<Option<i64>> = match array.data_type() {
        DataType::Date64 => array
            .as_primitive::<Date64Type>()
            .iter()
            .map(|millis| millis.map(|millis| millis / 86_400_000))
            .collect(),
        _ => arrow_cast::cast(array, &DataType::Date32)?
            .as_primitive::<Date32Type>()
            .iter()
            .map(|days| days.map(i64::from))
            .collect(),
    };
    let per_day = 86_400 * per_second(unit);
    let local: Vec<Option<i64>> = days
        .into_iter()
        .map(|days| {
            days.map(|days| days.checked_mul(per_day).ok_or_else(|| overflow(days)))
                .transpose()
        })
        .collect::<Result<_, _>>()?;
    placed(local, unit, zone)
}

/// `array`, wall-clock timestamps, as instants of `unit` in `zone`.
pub(crate) fn localized(
    array: &ArrayRef,
    unit: TimeUnit,
    zone: &Arc<str>,
) -> Result<ArrayRef, ArrowError> {
    let naive = arrow_cast::cast_with_options(
        array,
        &DataType::Timestamp(unit, None),
        &arrow_cast::CastOptions {
            safe: false,
            ..arrow_cast::CastOptions::default()
        },
    )?;
    let local: Vec<Option<i64>> = (0..naive.len())
        .map(|row| (!naive.is_null(row)).then(|| raw_timestamp(naive.as_ref(), row, unit)))
        .collect();
    placed(local, unit, Some(zone))
}

/// `local` wall-clock values of `unit` as the instants they name in `zone`, or as they are
/// without one.
fn placed(
    local: Vec<Option<i64>>,
    unit: TimeUnit,
    zone: Option<&Arc<str>>,
) -> Result<ArrayRef, ArrowError> {
    let values = match zone {
        None => local,
        Some(zone) => {
            let tz: Tz = zone.parse()?;
            local
                .into_iter()
                .map(|value| {
                    value
                        .map(|value| instant(value, unit, zone, tz))
                        .transpose()
                })
                .collect::<Result<_, _>>()?
        }
    };
    let zone = zone.cloned();
    Ok(match unit {
        TimeUnit::Second => Arc::new(TimestampSecondArray::from(values).with_timezone_opt(zone)),
        TimeUnit::Millisecond => {
            Arc::new(TimestampMillisecondArray::from(values).with_timezone_opt(zone))
        }
        TimeUnit::Microsecond => {
            Arc::new(TimestampMicrosecondArray::from(values).with_timezone_opt(zone))
        }
        TimeUnit::Nanosecond => {
            Arc::new(TimestampNanosecondArray::from(values).with_timezone_opt(zone))
        }
    })
}

/// The instant the wall-clock `value`, of `unit`, names in `zone`.
fn instant(value: i64, unit: TimeUnit, zone: &str, tz: Tz) -> Result<i64, ArrowError> {
    let offset = if let Some(offset) = fixed_offset(zone) {
        offset
    } else {
        let local = naive(value, unit).ok_or_else(|| {
            ArrowError::CastError(format!(
                "value {value} is beyond the years zone {zone}'s offsets are known for"
            ))
        })?;
        let offset = match tz.offset_from_local_datetime(&local) {
            LocalResult::Single(offset) | LocalResult::Ambiguous(offset, _) => offset,
            // Clocks skipped `local`: the offset in force before they did, a day earlier, moves
            // it forward by the gap.
            LocalResult::None => tz.offset_from_utc_datetime(
                &local
                    .checked_sub_signed(TimeDelta::days(1))
                    .expect("clocks skip only within the years a zone's offsets are known for"),
            ),
        };
        i64::from(offset.fix().local_minus_utc())
    };
    offset
        .checked_mul(per_second(unit))
        .and_then(|offset| value.checked_sub(offset))
        .ok_or_else(|| overflow(value))
}

/// `array`, times of day, in `unit`: each time exactly, refused where the type storing `unit`
/// cannot hold it, as Arrow's unchecked multiplication would wrap it.
pub(crate) fn times(array: &ArrayRef, unit: TimeUnit) -> Result<ArrayRef, ArrowError> {
    let (DataType::Time32(from) | DataType::Time64(from)) = *array.data_type() else {
        unreachable!("{} is not a time of day", array.data_type())
    };
    let per = nanos(1, unit);
    let values = (0..array.len())
        .map(|row| (!array.is_null(row)).then(|| nanos(raw(array.as_ref(), row), from) / per));
    let narrow = |value: i128| i32::try_from(value).map_err(|_| overflow(value));
    let wide = |value: i128| i64::try_from(value).map_err(|_| overflow(value));
    Ok(match unit {
        TimeUnit::Second => Arc::new(Time32SecondArray::from(
            values
                .map(|value| value.map(narrow).transpose())
                .collect::<Result<Vec<_>, _>>()?,
        )),
        TimeUnit::Millisecond => Arc::new(Time32MillisecondArray::from(
            values
                .map(|value| value.map(narrow).transpose())
                .collect::<Result<Vec<_>, _>>()?,
        )),
        TimeUnit::Microsecond => Arc::new(Time64MicrosecondArray::from(
            values
                .map(|value| value.map(wide).transpose())
                .collect::<Result<Vec<_>, _>>()?,
        )),
        TimeUnit::Nanosecond => Arc::new(Time64NanosecondArray::from(
            values
                .map(|value| value.map(wide).transpose())
                .collect::<Result<Vec<_>, _>>()?,
        )),
    })
}

/// Seconds a zone written as `UTC` or `±HH:MM` is ahead of UTC; `None` for a named zone.
fn fixed_offset(zone: &str) -> Option<i64> {
    if zone == "UTC" || zone == "Z" {
        return Some(0);
    }
    let sign = match zone.as_bytes().first()? {
        b'+' => 1,
        b'-' => -1,
        _ => return None,
    };
    let (hours, minutes) = zone[1..]
        .split_once(':')
        .unwrap_or((&zone[1..3], &zone[3..]));
    Some(sign * (hours.parse::<i64>().ok()? * 3_600 + minutes.parse::<i64>().ok()? * 60))
}

/// The wall-clock time `value`, of `unit`, where `chrono` can hold it.
fn naive(value: i64, unit: TimeUnit) -> Option<NaiveDateTime> {
    match unit {
        TimeUnit::Second => as_datetime::<TimestampSecondType>(value),
        TimeUnit::Millisecond => as_datetime::<TimestampMillisecondType>(value),
        TimeUnit::Microsecond => as_datetime::<TimestampMicrosecondType>(value),
        TimeUnit::Nanosecond => as_datetime::<TimestampNanosecondType>(value),
    }
}

/// Units of `unit` in a second.
fn per_second(unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => 1,
        TimeUnit::Millisecond => 1_000,
        TimeUnit::Microsecond => 1_000_000,
        TimeUnit::Nanosecond => 1_000_000_000,
    }
}

fn overflow(value: impl std::fmt::Display) -> ArrowError {
    ArrowError::ComputeError(format!("value {value} is beyond what its unit's i64 holds"))
}

/// The timestamp at `row` of `array`, of `unit`.
fn raw_timestamp(array: &dyn Array, row: usize, unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => array.as_primitive::<TimestampSecondType>().value(row),
        TimeUnit::Millisecond => array.as_primitive::<TimestampMillisecondType>().value(row),
        TimeUnit::Microsecond => array.as_primitive::<TimestampMicrosecondType>().value(row),
        TimeUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().value(row),
    }
}

/// Whether `data_type` is a temporal type this module renders.
pub(crate) fn is_temporal(data_type: &DataType) -> bool {
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

/// The value at `row` as its type stores it.
fn raw(array: &dyn Array, row: usize) -> i64 {
    match array.data_type() {
        DataType::Date32 => i64::from(array.as_primitive::<Date32Type>().value(row)),
        DataType::Date64 => array.as_primitive::<Date64Type>().value(row),
        DataType::Time32(TimeUnit::Second) => {
            i64::from(array.as_primitive::<Time32SecondType>().value(row))
        }
        DataType::Time32(_) => i64::from(array.as_primitive::<Time32MillisecondType>().value(row)),
        DataType::Time64(TimeUnit::Microsecond) => {
            array.as_primitive::<Time64MicrosecondType>().value(row)
        }
        DataType::Time64(_) => array.as_primitive::<Time64NanosecondType>().value(row),
        DataType::Timestamp(TimeUnit::Second, _) => {
            array.as_primitive::<TimestampSecondType>().value(row)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            array.as_primitive::<TimestampMillisecondType>().value(row)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            array.as_primitive::<TimestampMicrosecondType>().value(row)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            array.as_primitive::<TimestampNanosecondType>().value(row)
        }
        DataType::Duration(TimeUnit::Second) => {
            array.as_primitive::<DurationSecondType>().value(row)
        }
        DataType::Duration(TimeUnit::Millisecond) => {
            array.as_primitive::<DurationMillisecondType>().value(row)
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            array.as_primitive::<DurationMicrosecondType>().value(row)
        }
        DataType::Duration(TimeUnit::Nanosecond) => {
            array.as_primitive::<DurationNanosecondType>().value(row)
        }
        other => unreachable!("{other} is not temporal"),
    }
}

/// `value`, in `unit`, in nanoseconds.
fn nanos(value: i64, unit: TimeUnit) -> i128 {
    let per = match unit {
        TimeUnit::Second => NANOS_PER_SECOND,
        TimeUnit::Millisecond => 1_000_000,
        TimeUnit::Microsecond => 1_000,
        TimeUnit::Nanosecond => 1,
    };
    i128::from(value) * per
}
