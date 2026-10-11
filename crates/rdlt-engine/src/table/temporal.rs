//! Temporal values as text, exactly, and the days of dates: text the engine renders for every
//! value, beyond the years Arrow renders too, and a `Date64` holding part of a day as the day it
//! is within, never the day its milliseconds round toward zero to.
//!
//! Widenings between temporal types go through [`rdlt_connector::instants`], which keeps every
//! value the instant, time or duration it was.
//!
//! Dates, times of day, durations and timestamps are rendered by the engine for every value:
//! durations as `PT1.5S`, a zoned instant at its zone's offset where chrono holds it there at
//! whole minutes and in UTC otherwise, and a year beyond `0000`-`9999` with its sign. The text is
//! Arrow's wherever Arrow renders the value, but for a `Date64`, its date where Arrow adds a
//! time, a zero duration, `PT0S` where Arrow writes `P0D`, and a zoned instant whose offset has
//! seconds, in UTC where Arrow rounds the offset to the minute and names another instant.

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

/// The wall-clock time `value`, of `unit`, where `chrono` can hold it.
fn naive(value: i64, unit: TimeUnit) -> Option<NaiveDateTime> {
    match unit {
        TimeUnit::Second => as_datetime::<TimestampSecondType>(value),
        TimeUnit::Millisecond => as_datetime::<TimestampMillisecondType>(value),
        TimeUnit::Microsecond => as_datetime::<TimestampMicrosecondType>(value),
        TimeUnit::Nanosecond => as_datetime::<TimestampNanosecondType>(value),
    }
}
