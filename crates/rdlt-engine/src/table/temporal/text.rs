//! Temporal values, numbers and booleans as text: as Arrow renders them wherever it can, and
//! exactly, in UTC, where it cannot.

use std::fmt::Write as _;
use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::timezone::Tz;
use arrow_array::{Array, ArrayRef};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::{ArrowError, DataType, TimeUnit};
use chrono::{Offset as _, TimeZone as _};

use super::{DAY, NANOS_PER_SECOND, fixed_offset, naive, nanos, raw};

/// Each value of `array`, a boolean, number or temporal array, as text; nulls stay null.
pub(crate) fn text(array: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let renderer = Renderer::new(array.as_ref())?;
    let nulls = array.logical_nulls();
    let mut builder = StringBuilder::with_capacity(array.len(), array.len() * 24);
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
    /// For a named zone's timestamps, whether the zone's offset at an instant is whole minutes,
    /// which is all the text of an offset holds; other instants are rendered in UTC.
    whole_minutes: Option<Box<dyn Fn(i64) -> bool + 'a>>,
}

impl<'a> Renderer<'a> {
    /// A renderer of `array`'s values.
    pub(crate) fn new(array: &'a dyn Array) -> Result<Self, ArrowError> {
        // Arrow renders a `Date64` as a date and time; it is a date, as a `Date32` is.
        let options = FormatOptions::default().with_datetime_format(Some("%Y-%m-%d"));
        let formatter = ArrayFormatter::try_new(array, &options)?;
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

    /// Appends the text of the value at `row`, which is not null, to `out`.
    pub(crate) fn write(&self, row: usize, out: &mut String) {
        if let DataType::Duration(unit) = self.array.data_type() {
            out.push_str(&duration(nanos(raw(self.array, row), *unit)));
            return;
        }
        if self
            .whole_minutes
            .as_ref()
            .is_some_and(|whole| !whole(raw(self.array, row)))
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
