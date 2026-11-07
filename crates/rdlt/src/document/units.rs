//! Sizes and durations as a document writes them: a whole number and a unit, `512MiB`, `10s`.

#[cfg(test)]
mod tests;

use std::borrow::Cow;
use std::fmt;
use std::time::Duration;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::de::{self, Unexpected, Visitor};

use super::read::{Text, refuse};

/// Each binary unit of a size and its bytes, largest first.
const SIZES: [(&str, u64); 5] = [
    ("TiB", 1 << 40),
    ("GiB", 1 << 30),
    ("MiB", 1 << 20),
    ("KiB", 1 << 10),
    ("B", 1),
];

/// Each unit of a duration and its nanoseconds, largest first.
const SPANS: [(&str, u128); 6] = [
    ("h", 3_600_000_000_000),
    ("m", 60_000_000_000),
    ("s", 1_000_000_000),
    ("ms", 1_000_000),
    ("us", 1_000),
    ("ns", 1),
];

/// `text`'s leading digits, none where it starts with no digit, and the unit after them.
fn split(text: &str) -> (&str, &str) {
    let digits = text.len() - text.trim_start_matches(|c: char| c.is_ascii_digit()).len();
    text.split_at(digits)
}

/// The bytes `text` says: digits and an optional binary unit.
pub(crate) fn size(text: &str) -> Option<u64> {
    let (digits, unit) = split(text);
    let scale = match unit {
        "" => 1,
        unit => SIZES.iter().find(|(name, _)| *name == unit)?.1,
    };
    digits.parse::<u64>().ok()?.checked_mul(scale)
}

/// `bytes` in the largest unit that holds it exactly.
pub(crate) fn sized(bytes: u64) -> String {
    let (name, scale) = SIZES
        .iter()
        .find(|(_, scale)| bytes != 0 && bytes.is_multiple_of(*scale))
        .copied()
        .unwrap_or(("B", 1));
    format!("{}{name}", bytes / scale)
}

/// The duration `text` says: digits and a unit, `ns` to `h`.
pub(crate) fn span(text: &str) -> Option<Duration> {
    let (digits, unit) = split(text);
    let scale = SPANS.iter().find(|(name, _)| *name == unit)?.1;
    let nanos = digits.parse::<u128>().ok()?.checked_mul(scale)?;
    let secs = u64::try_from(nanos / 1_000_000_000).ok()?;
    let rest = u32::try_from(nanos % 1_000_000_000).ok()?;
    Some(Duration::new(secs, rest))
}

/// `duration` in the largest unit that holds it exactly.
pub(crate) fn spanned(duration: Duration) -> String {
    let nanos = duration.as_nanos();
    let (name, scale) = SPANS
        .iter()
        .find(|(_, scale)| nanos != 0 && nanos.is_multiple_of(*scale))
        .copied()
        .unwrap_or(("s", 1_000_000_000));
    format!("{}{name}", nanos / scale)
}

/// A size's schema: a whole number of bytes, or digits and a binary unit.
pub(crate) struct SizeText;

impl JsonSchema for SizeText {
    fn schema_name() -> Cow<'static, str> {
        "Size".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "Bytes: a whole number, or digits and one of B, KiB, MiB, GiB, TiB.",
            "anyOf": [
                { "type": "integer", "minimum": 0 },
                { "type": "string", "pattern": "^[0-9]+(B|KiB|MiB|GiB|TiB)?$" }
            ]
        })
    }
}

/// A duration's schema: digits and a unit.
pub(crate) struct SpanText;

impl JsonSchema for SpanText {
    fn schema_name() -> Cow<'static, str> {
        "Duration".into()
    }

    fn json_schema(_: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "description": "A duration: digits and one of ns, us, ms, s, m, h.",
            "type": "string",
            "pattern": "^[0-9]+(ns|us|ms|s|m|h)$"
        })
    }
}

/// Reads a size, never quoting what it refuses.
struct Sized;

impl Visitor<'_> for Sized {
    type Value = u64;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a size such as 512MiB")
    }

    fn visit_u64<E: de::Error>(self, bytes: u64) -> Result<u64, E> {
        Ok(bytes)
    }

    fn visit_i64<E: de::Error>(self, bytes: i64) -> Result<u64, E> {
        u64::try_from(bytes)
            .map_err(|_| E::invalid_value(Unexpected::Other("a negative number"), &self))
    }

    fn visit_str<E: de::Error>(self, text: &str) -> Result<u64, E> {
        size(text).ok_or_else(|| E::invalid_value(Unexpected::Other("text"), &self))
    }

    refuse!(bool wide fraction);
}

/// The duration a document's text says.
fn spanned_text<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
    let expected = &"a duration such as 10s";
    let Text(text) = serde::Deserialize::deserialize(deserializer)?;
    span(&text).ok_or_else(|| de::Error::invalid_value(Unexpected::Other("text"), expected))
}

/// An optional size, as a document writes it.
pub(crate) mod bytes {
    use serde::{Deserializer, Serialize as _, Serializer};

    use super::{Sized, sized};

    #[expect(
        clippy::ref_option,
        reason = "serde's `with` hands a field of `Option` by reference"
    )]
    pub(crate) fn serialize<S: Serializer>(
        value: &Option<u64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value.map(sized).serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<u64>, D::Error> {
        deserializer.deserialize_any(Sized).map(Some)
    }
}

/// An optional duration, as a document writes it.
pub(crate) mod duration {
    use std::time::Duration;

    use serde::{Deserializer, Serialize as _, Serializer};

    use super::{spanned, spanned_text};

    #[expect(
        clippy::ref_option,
        reason = "serde's `with` hands a field of `Option` by reference"
    )]
    pub(crate) fn serialize<S: Serializer>(
        value: &Option<Duration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value.map(spanned).serialize(serializer)
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Duration>, D::Error> {
        spanned_text(deserializer).map(Some)
    }
}
