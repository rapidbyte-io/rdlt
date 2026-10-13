//! JSON text of the values JSON writers disagree on, floats and strings, and the marks of a JSON
//! text's structure.

use arrow_array::Array;
use arrow_array::cast::AsArray;
use arrow_array::types::{Float32Type, Float64Type};
use arrow_schema::DataType;

mod structure;

pub use structure::{Mark, Structure};

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

/// Writes `text` onto `out` as a JSON string, quoted, escaped as sonic-rs and `serde_json` escape
/// one: `"` and `\` behind a backslash, the control characters JSON names by a letter so, the
/// others as `\u00XX` in lower-case hex, and every other character as it is, `/`, DEL and
/// U+2028 among them.
#[inline]
pub fn write_string(text: &str, out: &mut String) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push('"');
    let mut plain = 0;
    for (at, byte) in text.bytes().enumerate() {
        let escaped = match byte {
            b'"' => "\\\"",
            b'\\' => "\\\\",
            b'\n' => "\\n",
            b'\r' => "\\r",
            b'\t' => "\\t",
            0x08 => "\\b",
            0x0c => "\\f",
            0..0x20 => "\\u00",
            _ => continue,
        };
        // An escaped byte is ASCII, so `at` is a character's boundary.
        out.push_str(text.get(plain..at).unwrap_or_default());
        out.push_str(escaped);
        if escaped == "\\u00" {
            out.push(char::from(HEX[usize::from(byte >> 4)]));
            out.push(char::from(HEX[usize::from(byte & 0xf)]));
        }
        plain = at + 1;
    }
    out.push_str(text.get(plain..).unwrap_or_default());
    out.push('"');
}

#[cfg(test)]
mod tests;
