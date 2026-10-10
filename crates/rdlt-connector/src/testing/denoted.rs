//! What a value read back denotes, as text two reads of one value agree on whatever type holds
//! it: the oracle of every widening check.

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::{Decimal128Type, Decimal256Type, Float64Type};
use arrow_cast::display::{ArrayFormatter, FormatOptions};
use arrow_schema::DataType;

use crate::instants;

/// What the value at `row` of `array` denotes: an instant, a time of day or a duration in
/// nanoseconds, a number as the shortest decimal text of its exact value, `null`, and any other
/// value as Arrow shows it.
///
/// A float keeps the sign of its zero. A value that cannot be read denotes text saying so, never
/// a panic: what a destination sent back is not trusted.
pub fn denoted(array: &dyn Array, row: usize) -> String {
    if array.is_null(row) {
        return "null".to_owned();
    }
    let stored = instants::stored(array, row).map(i128::from);
    if let Some(nanos) = stored.and_then(|value| instants::nanos(array.data_type(), value)) {
        return format!("{nanos} ns");
    }
    let one = array.slice(row, 1);
    let number = match array.data_type() {
        DataType::Decimal128(_, scale) => shifted(
            &one.as_primitive::<Decimal128Type>().value(0).to_string(),
            *scale,
        ),
        DataType::Decimal256(_, scale) => shifted(
            &one.as_primitive::<Decimal256Type>().value(0).to_string(),
            *scale,
        ),
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            match arrow_cast::cast(&one, &DataType::Float64) {
                Ok(wide) => format!("{}", wide.as_primitive::<Float64Type>().value(0)),
                Err(error) => return format!("unreadable: {error}"),
            }
        }
        _ => {
            return match ArrayFormatter::try_new(one.as_ref(), &FormatOptions::default()) {
                Ok(shown) => shown.value(0).to_string(),
                Err(error) => format!("unreadable: {error}"),
            };
        }
    };
    trimmed(number)
}

/// `digits`, an integer's text, divided by ten to the `scale`, a negative scale multiplying.
fn shifted(digits: &str, scale: i8) -> String {
    let (sign, digits) = digits
        .strip_prefix('-')
        .map_or(("", digits), |digits| ("-", digits));
    let Ok(scale) = usize::try_from(scale) else {
        let zeros = "0".repeat(usize::from(scale.unsigned_abs()));
        return format!("{sign}{digits}{zeros}");
    };
    let padded = format!("{digits:0>width$}", width = scale + 1);
    let (whole, fraction) = padded.split_at(padded.len() - scale);
    format!("{sign}{whole}.{fraction}")
}

/// `text`, a number, without trailing zeros after its point, or the point itself.
fn trimmed(text: String) -> String {
    if !text.contains('.') {
        return text;
    }
    text.trim_end_matches('0').trim_end_matches('.').to_owned()
}

#[cfg(test)]
mod tests;
