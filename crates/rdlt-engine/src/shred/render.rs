//! Values of `Json` columns, rendered as compact JSON text as they are parsed.
//!
//! A column of scalars that became JSON as a chunk was first parsed renders as that parse goes,
//! over records nothing checked yet, so each level of a value is held to the nesting limit as it
//! is rendered. Numbers keep the text they were written as: integers read exactly render as their
//! digits, and a float only as the exact parse noted its text.

#[cfg(test)]
mod tests;

use std::fmt::{self, Display, Write as _};

use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};

use arrow_array::builder::StringBuilder;
use arrow_array::{Array, ArrayRef};
use rdlt_connector::json::write_string;

use super::ShredError;
use super::build::{Scalar, scalar_at};
use super::keys::OpenKeys;
use super::visit::{Context, digits, nest, within_depth};
use crate::limits::QUOTED_BYTES;

/// What a parse renders its values of JSON into, kept from value to value so that rendering one
/// allocates only to grow: the value's text and the keys of the objects open in it.
#[derive(Debug, Default)]
pub(crate) struct Rendering {
    pub(crate) text: String,
    pub(crate) keys: OpenKeys,
    /// Whether the value held a float the parse kept no text of: the fast parse reads a float
    /// as its value alone, so its column cannot hold it as JSON text.
    pub(crate) unwritten: bool,
}

impl Rendering {
    /// Readies it for the next value; a value that stopped before closing its objects left keys
    /// only before any later object's own, which closing that object never reads.
    pub(crate) fn clear(&mut self) {
        self.text.clear();
        self.unwritten = false;
    }
}

/// Writes `value` onto `out` as JSON text, as it was read; a float or an integer beyond 76 digits,
/// whose text JSON text keeps as it was written, writes nothing.
pub(crate) fn scalar(value: Scalar<'_>, out: &mut String) {
    match value {
        Scalar::Bool(value) => out.push_str(if value { "true" } else { "false" }),
        Scalar::Int(value) => integer(value, out),
        Scalar::Wide(value) => integer(value, out),
        Scalar::Huge(value) => integer(value, out),
        Scalar::Vast(value) => integer(value, out),
        Scalar::Text(value) => write_string(value, out),
        Scalar::Float(_) | Scalar::Beyond => {}
    }
}

/// Writes the integer `value` onto `out` as its digits.
fn integer(value: impl Display, out: &mut String) {
    // Writing into a `String` cannot fail.
    let _ = write!(out, "{value}");
}

/// The values of `array`, a column of scalars that render as they were read, as a column of JSON
/// text: built in a buffer of the text's own size, which the values' text was no shorter than.
pub(crate) fn json_text(array: &dyn Array) -> ArrayRef {
    let mut value = String::new();
    let rendered = |row: usize, value: &mut String| {
        value.clear();
        scalar_at(array, row).map(|read| scalar(read, value))
    };
    let mut bytes = 0;
    for row in 0..array.len() {
        if rendered(row, &mut value).is_some() {
            bytes += value.len();
        }
    }
    let mut column = StringBuilder::with_capacity(array.len(), bytes);
    for row in 0..array.len() {
        match rendered(row, &mut value) {
            Some(()) => column.append_value(&value),
            None => column.append_null(),
        }
    }
    std::sync::Arc::new(column.finish())
}

/// One value, `depth` levels deep, rendered onto `rendering`'s text; an object repeating a key
/// fails, as does a value nested past the limit.
pub(crate) struct Render<'a> {
    pub(crate) rendering: &'a mut Rendering,
    pub(crate) context: &'a Context,
    pub(crate) depth: u64,
}

impl Render<'_> {
    /// A value nested in this one.
    fn inner(&mut self) -> Render<'_> {
        Render {
            rendering: &mut *self.rendering,
            context: self.context,
            depth: self.depth + 1,
        }
    }
}

impl<'de> DeserializeSeed<'de> for Render<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        within_depth(self.depth, self.context)?;
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
        integer(value, &mut self.rendering.text);
        Ok(())
    }

    fn visit_u64<E>(self, value: u64) -> Result<(), E> {
        integer(value, &mut self.rendering.text);
        Ok(())
    }

    /// A float, as it was written where the exact parse noted its text; the fast parse notes
    /// none, and the value is then left unwritten.
    fn visit_f64<E>(self, value: f64) -> Result<(), E> {
        self.context.float(value);
        let text = &mut self.rendering.text;
        let written = self.context.noted_number(|written| {
            if let Some(written) = written {
                text.push_str(written);
            }
            written.is_some()
        });
        self.rendering.unwritten |= !written;
        Ok(())
    }

    fn visit_i128<E>(self, value: i128) -> Result<(), E> {
        integer(value, &mut self.rendering.text);
        Ok(())
    }

    /// An integer beyond 38 digits, as its digits: JSON text holds any integer as they are.
    fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<(), E> {
        let digits = digits(bytes, self.context)?;
        self.rendering.text.push_str(digits);
        Ok(())
    }

    fn visit_str<E>(self, value: &str) -> Result<(), E> {
        write_string(value, &mut self.rendering.text);
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
        write_string(key, text);
        self.rendering.keys.key(start..text.len());
        text.push(':');
        Ok(())
    }
}
