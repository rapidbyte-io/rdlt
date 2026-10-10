//! JSON text of values whose text JSON writers disagree on.

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type};
use arrow_schema::DataType;

/// Writes the float at `row` of `array` as JSON: a finite float as the shortest text that reads
/// back as it, and one that is not finite as a JSON string of its name, `"NaN"`, `"Infinity"` or
/// `"-Infinity"`.
///
/// A 32-bit float is written as the 64-bit float it widens to, so its text is the text of the
/// column it widens to. A tie between two shortest texts goes to the even one, as JSON writers
/// break it.
///
/// # Panics
///
/// Where `array` holds no 32- or 64-bit floats or has no row `row`; writing itself never panics,
/// since `serde_json` writes every finite float to a `Vec`.
pub fn write_float(array: &dyn Array, row: usize, out: &mut Vec<u8>) {
    let value = match array.data_type() {
        DataType::Float32 => f64::from(array.as_primitive::<Float32Type>().value(row)),
        _ => array.as_primitive::<Float64Type>().value(row),
    };
    let name: &[u8] = match value {
        value if value.is_nan() => b"\"NaN\"",
        value if value == f64::INFINITY => b"\"Infinity\"",
        value if value == f64::NEG_INFINITY => b"\"-Infinity\"",
        _ => {
            let written = serde_json::to_writer(&mut *out, &value);
            return written.expect("serde_json writes every finite float to a vector");
        }
    };
    out.extend_from_slice(name);
}

#[cfg(test)]
mod tests;
