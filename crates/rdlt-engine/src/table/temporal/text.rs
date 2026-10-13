//! Temporal values, numbers and booleans as text: every date, time, duration and timestamp
//! exactly, as [`rdlt_connector::instants::Renderer`] renders it; numbers and booleans as Arrow
//! renders them.

use std::sync::Arc;

use arrow_array::builder::StringBuilder;
use arrow_array::{Array, ArrayRef};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::ArrowError;

use rdlt_connector::instants;

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

/// Renders the values of one array of booleans, numbers or temporal values.
pub(crate) enum Renderer<'a> {
    /// Dates, times of day, durations and timestamps.
    Temporal(instants::Renderer<'a>),
    /// Booleans and numbers, as Arrow renders them.
    Other(ArrayFormatter<'a>),
}

impl<'a> Renderer<'a> {
    /// A renderer of `array`'s values; a zone Arrow does not read is refused.
    pub(crate) fn new(array: &'a dyn Array) -> Result<Self, ArrowError> {
        Ok(match instants::Renderer::new(array)? {
            Some(temporal) => Self::Temporal(temporal),
            None => Self::Other(ArrayFormatter::try_new(array, &FormatOptions::default())?),
        })
    }

    /// Appends the text of the value at `row`, which is not null, to `out`.
    pub(crate) fn write(&self, row: usize, out: &mut String) {
        match self {
            Self::Temporal(temporal) => temporal.write(row, out),
            Self::Other(formatter) => formatter
                .value(row)
                .write(out)
                .expect("Arrow renders every boolean and number"),
        }
    }
}
