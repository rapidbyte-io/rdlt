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

use std::fmt::Write as _;
use std::sync::Arc;

use arrow_array::builder::StringBuilder;
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
    Array, ArrayRef, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{ArrowError, DataType, TimeUnit};
use chrono::{LocalResult, NaiveDateTime, Offset as _, TimeZone as _};

/// Nanoseconds in a day.
const DAY: i128 = 86_400 * NANOS_PER_SECOND;
const NANOS_PER_SECOND: i128 = 1_000_000_000;

/// `array`, dates, as timestamps of `unit` in `zone`: each date's midnight there.
pub(crate) fn midnights(
    array: &ArrayRef,
    unit: TimeUnit,
    zone: Option<&Arc<str>>,
) -> Result<ArrayRef, ArrowError> {
    let dates = arrow_cast::cast(array, &DataType::Date32)?;
    let dates = dates.as_primitive::<Date32Type>();
    let per_day = 86_400 * per_second(unit);
    let local: Vec<Option<i64>> = dates
        .iter()
        .map(|days| {
            days.map(|days| {
                i64::from(days)
                    .checked_mul(per_day)
                    .ok_or_else(|| overflow(days))
            })
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
            LocalResult::None => tz.offset_from_utc_datetime(&local),
        };
        i64::from(offset.fix().local_minus_utc())
    };
    offset
        .checked_mul(per_second(unit))
        .and_then(|offset| value.checked_sub(offset))
        .ok_or_else(|| overflow(value))
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

/// Each value of `array`, a boolean, number or temporal array, as text; nulls stay null.
pub(crate) fn text(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let renderer = Renderer::new(array.as_ref())?;
    let nulls = array.logical_nulls();
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 24);
    for row in 0..array.len() {
        if nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)) {
            builder.append_null();
        } else {
            builder.append_value(renderer.render(row));
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Renders the values of one temporal array.
pub(crate) struct Renderer<'a> {
    array: &'a dyn Array,
    formatter: ArrayFormatter<'a>,
    /// For a named zone's timestamps, whether the zone's offset at an instant is whole minutes,
    /// which is all the text of an offset holds; other instants are rendered in UTC.
    whole_minutes: Option<Box<dyn Fn(i64) -> bool + 'a>>,
}

impl<'a> Renderer<'a> {
    /// A renderer of `array`'s values.
    pub(crate) fn new(array: &'a dyn Array) -> Result<Self, ArrowError> {
        let formatter = ArrayFormatter::try_new(array, &FormatOptions::default())?;
        let whole_minutes = match array.data_type() {
            DataType::Timestamp(unit, Some(zone)) if fixed_offset(zone).is_none() => {
                let (unit, tz): (TimeUnit, Tz) = (*unit, zone.parse()?);
                let whole = move |value: i64| {
                    naive(value, unit).is_none_or(|utc| {
                        tz.offset_from_utc_datetime(&utc).fix().local_minus_utc() % 60 == 0
                    })
                };
                Some(Box::new(whole) as Box<dyn Fn(i64) -> bool + 'a>)
            }
            _ => None,
        };
        Ok(Self {
            array,
            formatter,
            whole_minutes,
        })
    }

    /// The text of the value at `row`, which is not null.
    pub(crate) fn render(&self, row: usize) -> String {
        if let DataType::Duration(unit) = self.array.data_type() {
            return duration(nanos(self.raw(row), *unit));
        }
        if self
            .whole_minutes
            .as_ref()
            .is_some_and(|whole| !whole(self.raw(row)))
        {
            return self.beyond(row);
        }
        self.formatter
            .value(row)
            .try_to_string()
            .unwrap_or_else(|_| self.beyond(row))
    }

    /// The text of a value Arrow cannot render: exact, and for instants in UTC.
    fn beyond(&self, row: usize) -> String {
        let value = self.raw(row);
        match self.array.data_type() {
            DataType::Date32 => date(i128::from(value)),
            DataType::Date64 => date(i128::from(value).div_euclid(86_400_000)),
            DataType::Time32(unit) | DataType::Time64(unit) => clock(nanos(value, *unit)),
            DataType::Timestamp(unit, zone) => {
                let instant = nanos(value, *unit);
                let day = instant.div_euclid(DAY);
                let suffix = if zone.is_some() { "Z" } else { "" };
                format!("{}T{}{suffix}", date(day), clock(instant.rem_euclid(DAY)))
            }
            other => unreachable!("{other} is not temporal"),
        }
    }

    /// The value at `row` as its type stores it.
    fn raw(&self, row: usize) -> i64 {
        let array = self.array;
        match array.data_type() {
            DataType::Date32 => i64::from(array.as_primitive::<Date32Type>().value(row)),
            DataType::Date64 => array.as_primitive::<Date64Type>().value(row),
            DataType::Time32(TimeUnit::Second) => {
                i64::from(array.as_primitive::<Time32SecondType>().value(row))
            }
            DataType::Time32(_) => {
                i64::from(array.as_primitive::<Time32MillisecondType>().value(row))
            }
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

/// `nanos` as an ISO 8601 duration of seconds, as Arrow renders it: `PT1.5S`, `-PT0.5S`.
fn duration(nanos: i128) -> String {
    let sign = if nanos < 0 { "-" } else { "" };
    let nanos = nanos.unsigned_abs();
    let seconds = nanos / NANOS_PER_SECOND.unsigned_abs();
    let fraction = nanos % NANOS_PER_SECOND.unsigned_abs();
    if fraction == 0 {
        format!("{sign}PT{seconds}S")
    } else {
        let digits = format!("{fraction:09}");
        format!("{sign}PT{seconds}.{}S", digits.trim_end_matches('0'))
    }
}

/// The date `days` since the epoch, `YYYY-MM-DD`, its year signed beyond `0000`–`9999`.
fn date(days: i128) -> String {
    // Howard Hinnant's civil_from_days.
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let of_era = shifted.rem_euclid(146_097);
    let year_of_era = (of_era - of_era / 1_460 + of_era / 36_524 - of_era / 146_096) / 365;
    let of_year = of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * of_year + 2) / 153;
    let day = of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i128::from(month <= 2);
    let mut text = String::new();
    match year {
        0..=9_999 => write!(text, "{year:04}"),
        10_000.. => write!(text, "+{year}"),
        _ => write!(text, "-{:04}", year.unsigned_abs()),
    }
    .expect("writing to a string cannot fail");
    write!(text, "-{month:02}-{day:02}").expect("writing to a string cannot fail");
    text
}

/// A time of day `nanos` after midnight, `HH:MM:SS` and a fraction where there is one, as
/// `chrono` renders one: three, six or nine digits.
fn clock(nanos: i128) -> String {
    let sign = if nanos < 0 { "-" } else { "" };
    let nanos = nanos.unsigned_abs();
    let seconds = nanos / NANOS_PER_SECOND.unsigned_abs();
    let fraction = nanos % NANOS_PER_SECOND.unsigned_abs();
    let (hours, minutes, seconds) = (seconds / 3_600, seconds / 60 % 60, seconds % 60);
    let fraction = match fraction {
        0 => String::new(),
        _ if fraction.is_multiple_of(1_000_000) => format!(".{:03}", fraction / 1_000_000),
        _ if fraction.is_multiple_of(1_000) => format!(".{:06}", fraction / 1_000),
        _ => format!(".{fraction:09}"),
    };
    format!("{sign}{hours:02}:{minutes:02}:{seconds:02}{fraction}")
}
