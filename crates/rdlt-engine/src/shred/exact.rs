//! The exact parse: a record walked a token at a time, as the fast parse streams one, its
//! numbers kept as their text and each read as the narrowest type that holds it exactly.

#[cfg(test)]
mod tests;

use serde::de::{self, DeserializeSeed, Visitor};

use super::ShredError;
use super::visit::{Context, DECIMAL_LIMIT};
use crate::limits::QUOTED_BYTES;

/// Parses the record `bytes` into `seed`, its numbers exact: nothing of the record is built
/// beside what the seed builds.
pub(super) fn visit<S>(bytes: &[u8], seed: S, context: &Context) -> Result<(), sonic_rs::Error>
where
    S: for<'de> DeserializeSeed<'de, Value = ()>,
{
    let Ok(text) = std::str::from_utf8(bytes) else {
        return Err(de::Error::custom("the record is not valid UTF-8"));
    };
    super::tokens::visit(text, seed, context)
}

/// Visits the number written `text`, whose exponent the reader found within the limit, as the
/// narrowest of a 64-bit integer, a 38-digit integer and a float that holds it exactly, or else,
/// an integer, as its digits.
pub(super) fn number<'de, V: Visitor<'de>, E: de::Error>(
    text: &str,
    context: &Context,
    visitor: V,
) -> Result<V::Value, E> {
    // The fast parse reads `-0` as a float, and refuses floats beyond the finite ones.
    if text.contains(['.', 'e', 'E']) || text == "-0" {
        let float: f64 = text.parse().map_err(de::Error::custom)?;
        if !float.is_finite() {
            let shown = rdlt_connector::text::shown(text, QUOTED_BYTES);
            let refused =
                ShredError::Invalid(format!("the number {shown} is beyond a float's range"));
            return Err(context.fail(refused));
        }
        // A column of JSON renders the number as it was written.
        context.note_number(text);
        // Negative zero reads as zero, as the fast parse reads it.
        return visitor.visit_f64(if float == 0.0 { 0.0 } else { float });
    }
    if let Ok(integer) = text.parse::<i64>() {
        return visitor.visit_i64(integer);
    }
    if let Ok(integer) = text.parse::<u64>() {
        return visitor.visit_u64(integer);
    }
    // An integer beyond 38 digits visits as its digits, which no integer visit holds.
    match text.parse::<i128>() {
        Ok(integer) if integer.unsigned_abs() < DECIMAL_LIMIT => visitor.visit_i128(integer),
        _ => visitor.visit_bytes(text.as_bytes()),
    }
}
