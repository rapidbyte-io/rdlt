//! Temporal values as text, exactly, and the days of dates: text the engine renders for every
//! value, beyond the years Arrow renders too, and a `Date64` holding part of a day as the day it
//! is within, never the day its milliseconds round toward zero to.
//!
//! Widenings between temporal types go through [`rdlt_connector::instants`], which keeps every
//! value the instant, time or duration it was.
//!
//! Dates, times of day, durations and timestamps are rendered by
//! [`rdlt_connector::instants::Renderer`] for every value: durations as `PT1.5S`, a zoned instant
//! at its zone's offset where chrono holds it there at whole minutes and in UTC otherwise, and a
//! year beyond `0000`-`9999` with its sign.

mod days;
#[cfg(test)]
mod tests;
mod text;

pub(crate) use days::{dated, micros_at};
pub(crate) use text::{Renderer, text};
