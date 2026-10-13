//! Temporal values as text, exactly: every date, time of day, duration and timestamp, beyond the
//! years Arrow renders too.
//!
//! The text is Arrow's wherever Arrow renders the value, but for a `Date64`, its date where Arrow
//! adds a time, a zero duration, `PT0S` where Arrow writes `P0D`, and a zoned instant whose
//! offset has seconds, in UTC where Arrow rounds the offset to the minute and names another
//! instant.
//!
//! A zoned instant is rendered at its zone's offset where `chrono` holds the instant and its
//! time in the zone, and the offset then is whole minutes, which is all the text of an offset
//! holds; any other is rendered in UTC. A year beyond `0000`-`9999` takes its sign.

#[cfg(test)]
mod tests;

use std::cmp::Ordering;
use std::fmt::Write as _;

use arrow_array::Array;
use arrow_array::temporal_conversions::as_datetime;
use arrow_array::timezone::Tz;
use arrow_array::types::{
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow_schema::{ArrowError, DataType, TimeUnit};
use chrono::{NaiveDateTime, Offset as _, TimeDelta, TimeZone as _};

use super::{DAY, nanos, stored};

/// Nanoseconds in a second.
const NANOS_PER_SECOND: i128 = 1_000_000_000;

/// Renders the values of one array of dates, times of day, durations or timestamps as text.
#[derive(Debug)]
pub struct Renderer<'a> {
    array: &'a dyn Array,
    kind: Kind,
}

/// How a renderer renders its array's values.
#[derive(Debug)]
enum Kind {
    /// Durations, in ISO 8601 seconds.
    Duration,
    /// Times of day, as clocks.
    Clock,
    /// Dates, as the days they are within.
    Date,
    /// Timestamps without a zone, as their wall-clock time.
    Naive,
    /// Timestamps of a unit, in a zone.
    Zoned(TimeUnit, Tz),
}

impl<'a> Renderer<'a> {
    /// A renderer of `array`'s values, where they are temporal; a zone Arrow does not read is
    /// refused.
    pub fn new(array: &'a dyn Array) -> Result<Option<Self>, ArrowError> {
        let kind = match array.data_type() {
            DataType::Duration(_) => Kind::Duration,
            DataType::Time32(_) | DataType::Time64(_) => Kind::Clock,
            DataType::Date32 | DataType::Date64 => Kind::Date,
            DataType::Timestamp(_, None) => Kind::Naive,
            DataType::Timestamp(unit, Some(zone)) => Kind::Zoned(*unit, zone.parse()?),
            _ => return Ok(None),
        };
        Ok(Some(Self { array, kind }))
    }

    /// Appends the text of the value at `row`, which is not null, to `out`.
    pub fn write(&self, row: usize, out: &mut String) {
        let value = || stored(self.array, row).unwrap_or_default();
        let nanos =
            |count: i64| nanos(self.array.data_type(), i128::from(count)).unwrap_or_default();
        match &self.kind {
            Kind::Duration => duration(nanos(value()), out),
            Kind::Clock => clock(nanos(value()), out),
            Kind::Date => date(nanos(value()).div_euclid(DAY), out),
            Kind::Naive => instant(nanos(value()), out),
            Kind::Zoned(unit, zone) => {
                let value = value();
                if let Some(seconds) = offset(value, *unit, *zone) {
                    instant(nanos(value) + i128::from(seconds) * NANOS_PER_SECOND, out);
                    offset_text(seconds, out);
                } else {
                    instant(nanos(value), out);
                    out.push('Z');
                }
            }
        }
    }
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

/// The seconds `zone` is ahead of UTC at the instant `value` of `unit`, where its text there
/// names the instant: `chrono` holds the instant and its time in the zone, and the offset is
/// whole minutes.
fn offset(value: i64, unit: TimeUnit, zone: Tz) -> Option<i64> {
    let utc = naive(value, unit)?;
    let seconds = i64::from(zone.offset_from_utc_datetime(&utc).fix().local_minus_utc());
    let held = utc
        .checked_add_signed(TimeDelta::seconds(seconds))
        .is_some();
    (seconds % 60 == 0 && held).then_some(seconds)
}

/// Appends an offset of `seconds`, whole minutes, as RFC 3339 writes it: `Z` for none, else
/// `±HH:MM`.
fn offset_text(seconds: i64, out: &mut String) {
    let sign = match seconds.cmp(&0) {
        Ordering::Equal => {
            out.push('Z');
            return;
        }
        Ordering::Less => '-',
        Ordering::Greater => '+',
    };
    let minutes = seconds.unsigned_abs() / 60;
    write!(out, "{sign}{:02}:{:02}", minutes / 60, minutes % 60)
        .expect("writing to a string cannot fail");
}

/// Appends the date and time `nanos` after the epoch, `YYYY-MM-DDTHH:MM:SS` and a fraction where
/// there is one.
fn instant(nanos: i128, out: &mut String) {
    date(nanos.div_euclid(DAY), out);
    out.push('T');
    clock(nanos.rem_euclid(DAY), out);
}

/// Appends `nanos` as an ISO 8601 duration of seconds, as Arrow renders it but for zero, which is
/// `PT0S`: `PT1.5S`, `-PT0.5S`.
fn duration(nanos: i128, out: &mut String) {
    let sign = if nanos < 0 { "-" } else { "" };
    let nanos = nanos.unsigned_abs();
    let seconds = nanos / NANOS_PER_SECOND.unsigned_abs();
    let mut fraction = nanos % NANOS_PER_SECOND.unsigned_abs();
    if fraction == 0 {
        write!(out, "{sign}PT{seconds}S").expect("writing to a string cannot fail");
        return;
    }
    let mut digits: usize = 9;
    while fraction.is_multiple_of(10) {
        fraction /= 10;
        digits -= 1;
    }
    write!(out, "{sign}PT{seconds}.{fraction:0digits$}S").expect("writing to a string cannot fail");
}

/// Appends the date `days` since the epoch, `YYYY-MM-DD`, its year signed beyond `0000`-`9999`.
fn date(days: i128, out: &mut String) {
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
    match year {
        0..=9_999 => write!(out, "{year:04}"),
        10_000.. => write!(out, "+{year}"),
        _ => write!(out, "-{:04}", year.unsigned_abs()),
    }
    .expect("writing to a string cannot fail");
    write!(out, "-{month:02}-{day:02}").expect("writing to a string cannot fail");
}

/// Appends a time of day `nanos` after midnight, `HH:MM:SS` and a fraction where there is one,
/// as `chrono` renders one: three, six or nine digits.
fn clock(nanos: i128, out: &mut String) {
    let sign = if nanos < 0 { "-" } else { "" };
    let nanos = nanos.unsigned_abs();
    let seconds = nanos / NANOS_PER_SECOND.unsigned_abs();
    let fraction = nanos % NANOS_PER_SECOND.unsigned_abs();
    let (hours, minutes, seconds) = (seconds / 3_600, seconds / 60 % 60, seconds % 60);
    write!(out, "{sign}{hours:02}:{minutes:02}:{seconds:02}")
        .expect("writing to a string cannot fail");
    match fraction {
        0 => Ok(()),
        _ if fraction.is_multiple_of(1_000_000) => write!(out, ".{:03}", fraction / 1_000_000),
        _ if fraction.is_multiple_of(1_000) => write!(out, ".{:06}", fraction / 1_000),
        _ => write!(out, ".{fraction:09}"),
    }
    .expect("writing to a string cannot fail");
}
