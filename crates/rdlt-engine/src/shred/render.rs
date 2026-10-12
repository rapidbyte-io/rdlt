//! Values of `Json` columns, rendered as compact JSON text as they are parsed.
//!
//! Only a chunk's second parse renders, over records its first parse already checked, so rendering
//! recurses no deeper than the nesting limit. Numbers keep the text they were written as: integers
//! read exactly render as their digits, and a float only as the exact parse noted its text.

#[cfg(test)]
mod tests;

use std::fmt::{self, Display, Write as _};

use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};

use super::ShredError;
use super::keys::OpenKeys;
use super::visit::{Context, nest};
use crate::limits::QUOTED_BYTES;

/// What a parse renders its values of JSON into, kept from value to value so that rendering one
/// allocates only to grow: the value's text and the keys of the objects open in it.
#[derive(Debug, Default)]
pub(crate) struct Rendering {
    pub(crate) text: String,
    pub(crate) keys: OpenKeys,
}

impl Rendering {
    /// Readies it for the next value.
    pub(crate) fn clear(&mut self) {
        self.text.clear();
        self.keys.clear();
    }
}

/// Writes `text` onto `out` as a JSON string, quoted, escaped as sonic-rs escapes one: `"` and `\`
/// behind a backslash, the control characters JSON names by a letter so, the others as `\u00XX`,
/// and every other character as it is.
pub(crate) fn escape(text: &str, out: &mut String) {
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

/// One value rendered onto `rendering`'s text; an object repeating a key fails.
pub(crate) struct Render<'a> {
    pub(crate) rendering: &'a mut Rendering,
    pub(crate) context: &'a Context,
}

impl Render<'_> {
    /// A value nested in this one.
    fn inner(&mut self) -> Render<'_> {
        Render {
            rendering: &mut *self.rendering,
            context: self.context,
        }
    }

    /// Writes the integer `value` as its digits.
    fn integer(self, value: impl Display) {
        // Writing into a `String` cannot fail.
        let _ = write!(self.rendering.text, "{value}");
    }
}

impl<'de> DeserializeSeed<'de> for Render<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Render<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_unit<E>(self) -> Result<(), E> {
        self.rendering.text.push_str("null");
        Ok(())
    }

    fn visit_bool<E>(self, value: bool) -> Result<(), E> {
        self.rendering
            .text
            .push_str(if value { "true" } else { "false" });
        Ok(())
    }

    fn visit_i64<E>(self, value: i64) -> Result<(), E> {
        self.integer(value);
        Ok(())
    }

    fn visit_u64<E>(self, value: u64) -> Result<(), E> {
        self.integer(value);
        Ok(())
    }

    /// A float, as it was written: a chunk whose column of JSON holds one is built with the
    /// exact parse, which notes each number's text.
    fn visit_f64<E: serde::de::Error>(self, value: f64) -> Result<(), E> {
        self.context.float(value);
        let text = &mut self.rendering.text;
        let written = self.context.noted_number(|written| {
            if let Some(written) = written {
                text.push_str(written);
            }
            written.is_some()
        });
        if !written {
            let missing = "a float of a column of JSON came without the text it was written as";
            return Err(self.context.fail(ShredError::Internal(missing.to_owned())));
        }
        Ok(())
    }

    fn visit_i128<E>(self, value: i128) -> Result<(), E> {
        self.integer(value);
        Ok(())
    }

    /// An integer beyond 38 digits, as its digits: JSON text holds any integer as they are.
    fn visit_bytes<E: serde::de::Error>(self, digits: &[u8]) -> Result<(), E> {
        let digits = std::str::from_utf8(digits).map_err(|error| {
            self.context.fail(ShredError::Internal(format!(
                "an integer's digits: {error}"
            )))
        })?;
        self.rendering.text.push_str(digits);
        Ok(())
    }

    fn visit_str<E>(self, value: &str) -> Result<(), E> {
        escape(value, &mut self.rendering.text);
        Ok(())
    }

    fn visit_map<A: MapAccess<'de>>(mut self, mut map: A) -> Result<(), A::Error> {
        nest(move || {
            self.rendering.text.push('{');
            self.rendering.keys.open();
            let mut first = true;
            while map
                .next_key_seed(Key {
                    rendering: &mut *self.rendering,
                    first,
                })?
                .is_some()
            {
                first = false;
                map.next_value_seed(self.inner())?;
            }
            let rendering = &mut *self.rendering;
            if let Some(key) = rendering.keys.close(&rendering.text) {
                let key = rendering.text.get(key).unwrap_or_default();
                return Err(self.context.fail(repeated(key)));
            }
            rendering.text.push('}');
            Ok(())
        })
    }

    fn visit_seq<A: SeqAccess<'de>>(mut self, mut seq: A) -> Result<(), A::Error> {
        nest(move || {
            self.rendering.text.push('[');
            let mut first = true;
            loop {
                // The separator goes before each item, and comes off again when none follows.
                let before = self.rendering.text.len();
                if !first {
                    self.rendering.text.push(',');
                }
                if seq.next_element_seed(self.inner())?.is_none() {
                    self.rendering.text.truncate(before);
                    break;
                }
                first = false;
            }
            self.rendering.text.push(']');
            Ok(())
        })
    }
}

/// The refusal of `key`, a key as JSON text renders it, which an object named again.
#[cold]
fn repeated(key: &str) -> ShredError {
    let key: String = sonic_rs::from_str(key).unwrap_or_else(|_| key.to_owned());
    ShredError::DuplicateKey(rdlt_connector::text::shown(&key, QUOTED_BYTES))
}

/// An object's next key, written onto the text after the separator where it is not the first,
/// and noted among the object's keys.
struct Key<'a> {
    rendering: &'a mut Rendering,
    first: bool,
}

impl<'de> DeserializeSeed<'de> for Key<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_str(self)
    }
}

impl Visitor<'_> for Key<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an object key")
    }

    fn visit_str<E>(self, key: &str) -> Result<(), E> {
        let text = &mut self.rendering.text;
        if !self.first {
            text.push(',');
        }
        let start = text.len();
        escape(key, text);
        self.rendering.keys.key(start..text.len());
        text.push(':');
        Ok(())
    }
}
