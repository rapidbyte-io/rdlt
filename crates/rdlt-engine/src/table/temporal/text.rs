//! Temporal values, numbers and booleans as text: as Arrow renders them wherever it can, and
//! exactly, in UTC, where it cannot.
//!
//! Arrow is asked to render only what it holds: a zoned instant whose time in its zone `chrono`
//! holds, at an offset of whole minutes. Beyond that its formatter panics or writes a time of
//! another day, so times of day and durations, which it truncates past 2^32 seconds, are always
//! rendered here.

use std::fmt::Write as _;
use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::timezone::Tz;
use arrow_array::{Array, ArrayRef};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{ArrowError, DataType, TimeUnit};
use chrono::NaiveDateTime;
use chrono::{Offset as _, TimeDelta, TimeZone as _};

use rdlt_connector::instants::DAY;

use super::{NANOS_PER_SECOND, fixed_offset, naive, nanos, raw};

/// Each value of `array`, a boolean, number or temporal array, as text; nulls stay null.
pub(crate) fn text(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let renderer = Renderer::new(array.as_ref())?;
    let nulls = array.logical_nulls();
    let capacity = crate::table::convert::text_capacity(array, false);
    let mut builder = StringBuilder::with_capacity(array.len(), capacity);
    let mut value = String::new();
    for row in 0..array.len() {
        if nulls.as_ref().is_some_and(|nulls| nulls.is_null(row)) {
            builder.append_null();
        } else {
            value.clear();
            renderer.write(row, &mut value);
            builder.append_value(&value);
        }
    }
    Ok(Arc::new(builder.finish()))
}

/// Renders the values of one temporal array.
pub(crate) struct Renderer<'a> {
    array: &'a dyn Array,
    formatter: ArrayFormatter<'a>,
    /// For a zoned timestamp, whether Arrow renders an instant in the zone.
    ///
    /// It does where `chrono` holds the instant and its time there, and the zone's offset then is
    /// whole minutes, which is all the text of an offset holds; other instants are rendered in
    /// UTC.
    zoned: Option<Box<dyn Fn(i64) -> bool + 'a>>,
}

impl<'a> Renderer<'a> {
    /// A renderer of `array`'s values.
    pub(crate) fn new(array: &'a dyn Array) -> Result<Self, ArrowError> {
        // Arrow renders a `Date64` as a date and time; it is a date, as a `Date32` is.
        let options = FormatOptions::default().with_datetime_format(Some("%Y-%m-%d"));
        let formatter = ArrayFormatter::try_new(array, &options)?;
        let zoned = match array.data_type() {
            DataType::Timestamp(unit, Some(zone)) => Some(rendered_in(*unit, zone)?),
            _ => None,
        };
        Ok(Self {
            array,
            formatter,
            zoned,
        })
    }

    /// Appends the text of the value at `row`, which is not null, to `out`.
    pub(crate) fn write(&self, row: usize, out: &mut String) {
        match self.array.data_type() {
            DataType::Duration(unit) => {
                out.push_str(&duration(nanos(raw(self.array, row), *unit)));
                return;
            }
            DataType::Time32(unit) | DataType::Time64(unit) => {
                out.push_str(&clock(nanos(raw(self.array, row), *unit)));
                return;
            }
            _ => {}
        }
        if self
            .zoned
            .as_ref()
            .is_some_and(|renders| !renders(raw(self.array, row)))
        {
            out.push_str(&self.beyond(row));
            return;
        }
        let start = out.len();
        if self.formatter.value(row).write(out).is_err() {
            out.truncate(start);
            out.push_str(&self.beyond(row));
        }
    }

    /// The text of a value Arrow cannot render: exact, and for instants in UTC.
    fn beyond(&self, row: usize) -> String {
        let value = raw(self.array, row);
        match self.array.data_type() {
            DataType::Date32 => date(i128::from(value)),
            DataType::Date64 => date(i128::from(value).div_euclid(86_400_000)),
            DataType::Timestamp(unit, zone) => {
                let instant = nanos(value, *unit);
                let day = instant.div_euclid(DAY);
                let suffix = if zone.is_some() { "Z" } else { "" };
                format!("{}T{}{suffix}", date(day), clock(instant.rem_euclid(DAY)))
            }
            other => unreachable!("{other} is not temporal"),
        }
    }
}

/// Whether Arrow renders a timestamp of `unit` in `zone`: `chrono` holds the instant and its time
/// in the zone, and the zone's offset then is whole minutes.
fn rendered_in<'a>(
    unit: TimeUnit,
    zone: &str,
) -> Result<Box<dyn Fn(i64) -> bool + 'a>, ArrowError> {
    let offset = offset_in(zone)?;
    Ok(Box::new(move |value: i64| {
        naive(value, unit).is_some_and(|utc| {
            let offset = offset(&utc);
            offset % 60 == 0 && utc.checked_add_signed(TimeDelta::seconds(offset)).is_some()
        })
    }))
}

/// The seconds a zone is ahead of UTC at a UTC time.
type Offset = Box<dyn Fn(&NaiveDateTime) -> i64>;

/// The seconds `zone` is ahead of UTC at a UTC time.
fn offset_in(zone: &str) -> Result<Offset, ArrowError> {
    if let Some(seconds) = fixed_offset(zone) {
        return Ok(Box::new(move |_| seconds));
    }
    let tz: Tz = zone.parse()?;
    Ok(Box::new(move |utc| {
        i64::from(tz.offset_from_utc_datetime(utc).fix().local_minus_utc())
    }))
}

/// `nanos` as an ISO 8601 duration of seconds, as Arrow renders it: `PT1.5S`, `-PT0.5S`.
pub(super) fn duration(nanos: i128) -> String {
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
pub(super) fn date(days: i128) -> String {
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
pub(super) fn clock(nanos: i128) -> String {
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
