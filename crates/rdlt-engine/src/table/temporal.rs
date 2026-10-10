//! Temporal values as text, exactly, and the days of dates: text as Arrow renders it wherever it
//! can and beyond the years Arrow renders, and a `Date64` holding part of a day as the day it is
//! within, never the day its milliseconds round toward zero to.
//!
//! Widenings between temporal types go through [`rdlt_connector::instants`], which keeps every
//! value the instant, time or duration it was.
//!
//! Arrow renders dates, times and timestamps through `chrono`, which holds a few hundred thousand
//! years, and durations through `chrono`'s `Duration`, which holds fewer milliseconds than an
//! `i64` does; beyond them it fails, or writes `<invalid>`. Durations are always rendered here, as
//! Arrow renders them in range: `PT1.5S`. Other values Arrow cannot render are rendered here in
//! UTC, with as many year digits as they need.

mod days;
#[cfg(test)]
mod tests;
mod text;

pub(crate) use days::{dated, micros_at};
pub(crate) use text::{Renderer, text};

use arrow_array::temporal_conversions::as_datetime;
use arrow_array::types::{
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType,
};
use arrow_schema::TimeUnit;
use chrono::NaiveDateTime;

/// Nanoseconds in a second.
const NANOS_PER_SECOND: i128 = 1_000_000_000;

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
