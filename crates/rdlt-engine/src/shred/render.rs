//! Values of `Json` columns, rendered as compact JSON text as they are parsed.
//!
//! A column of scalars that became JSON as a chunk was first parsed renders as that parse goes,
//! over records nothing checked yet, so each level of a value is held to the nesting limit as it
//! is rendered. Numbers keep the text they were written as: integers read exactly render as their
//! digits, and a float only as the exact parse noted its text.

#[cfg(test)]
mod tests;

use std::fmt::{self, Display, Write as _};

use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};

use arrow_array::builder::StringBuilder;
use arrow_array::{Array, ArrayRef};
use rdlt_connector::json::write_string;
use rdlt_wire::limits::count;

use super::ShredError;
use super::build::{Scalar, scalar_at};
use super::keys::OpenKeys;
use super::meter::{Meter, Over};
use super::visit::{Context, digits, nest, over, within_depth};
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

    /// Makes room in the text for `more` bytes beside what it holds, charging `meter` first for
    /// what its capacity grows by: to twice what it was, or to what it must hold where that is
    /// more.
    ///
    /// # Errors
    ///
    /// [`Over`] where the meter has no room for that; the text is as it was.
    pub(crate) fn room(&mut self, more: usize, meter: &Meter) -> Result<(), Over> {
        let (len, capacity) = (self.text.len(), self.text.capacity());
        let needed = len.saturating_add(more);
        if needed <= capacity {
            return Ok(());
        }
        let grown = needed.max(capacity.saturating_mul(2));
        meter.charge(count(grown - capacity))?;
        self.text.reserve_exact(grown - len);
        Ok(())
    }

    /// Writes `text` onto the text, charged as [`Rendering::room`] charges.
    ///
    /// # Errors
    ///
    /// [`Over`] where the meter has no room for it.
    pub(crate) fn push(&mut self, text: &str, meter: &Meter) -> Result<(), Over> {
        self.room(text.len(), meter)?;
        self.text.push_str(text);
        Ok(())
    }

    /// Writes `text` onto the text as a JSON string, charged as [`Rendering::room`] charges:
    /// its escaped length is counted only where the text might have to grow for it.
    ///
    /// # Errors
    ///
    /// [`Over`] where the meter has no room for it.
    pub(crate) fn string(&mut self, text: &str, meter: &Meter) -> Result<(), Over> {
        let most = text.len().saturating_mul(6).saturating_add(2);
        if self.text.len().saturating_add(most) > self.text.capacity() {
            self.room(escaped_len(text), meter)?;
        }
        write_string(text, &mut self.text);
        Ok(())
    }

    /// Writes the integer `value` onto the text as its digits, charged as [`Rendering::room`]
    /// charges.
    ///
    /// # Errors
    ///
    /// [`Over`] where the meter has no room for them.
    pub(crate) fn integer(&mut self, value: impl Display, meter: &Meter) -> Result<(), Over> {
        self.room(DIGITS, meter)?;
        integer(value, &mut self.text);
        Ok(())
    }

    /// Writes `value` onto the text as [`scalar`] writes it, charged as [`Rendering::room`]
    /// charges.
    ///
    /// # Errors
    ///
    /// [`Over`] where the meter has no room for it.
    pub(crate) fn scalar(&mut self, value: Scalar<'_>, meter: &Meter) -> Result<(), Over> {
        if let Scalar::Text(text) = value {
            return self.string(text, meter);
        }
        self.room(DIGITS, meter)?;
        scalar(value, &mut self.text);
        Ok(())
    }
}

/// Bytes: the most an integer's digits take, its sign included, a 256-bit integer's; a
/// boolean's text is shorter.
const DIGITS: usize = 78;

/// Bytes `text` takes written as a JSON string, as `write_string` writes it.
fn escaped_len(text: &str) -> usize {
    text.bytes()
        .fold(text.len().saturating_add(2), |len, byte| match byte {
            b'"' | b'\\' | b'\n' | b'\r' | b'\t' | 0x08 | 0x0c => len + 1,
            0..0x20 => len + 5,
            _ => len,
        })
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

    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        self.rendering
            .push("null", &self.context.meter)
            .map_err(over)
    }

    fn visit_bool<E: de::Error>(self, value: bool) -> Result<(), E> {
        let text = if value { "true" } else { "false" };
        self.rendering.push(text, &self.context.meter).map_err(over)
    }

    fn visit_i64<E: de::Error>(self, value: i64) -> Result<(), E> {
        self.rendering
            .integer(value, &self.context.meter)
            .map_err(over)
    }

    fn visit_u64<E: de::Error>(self, value: u64) -> Result<(), E> {
        self.rendering
            .integer(value, &self.context.meter)
            .map_err(over)
    }

    /// A float, as it was written where the exact parse noted its text; the fast parse notes
    /// none, and the value is then left unwritten.
    fn visit_f64<E: de::Error>(self, value: f64) -> Result<(), E> {
        let context = self.context;
        context.float(value);
        let rendering = &mut *self.rendering;
        let written = context.noted_number(|written| match written {
            Some(written) => rendering.push(written, &context.meter).map(|()| true),
            None => Ok(false),
        });
        rendering.unwritten |= !written.map_err(over)?;
        Ok(())
    }

    fn visit_i128<E: de::Error>(self, value: i128) -> Result<(), E> {
        self.rendering
            .integer(value, &self.context.meter)
            .map_err(over)
    }

    /// An integer beyond 38 digits, as its digits: JSON text holds any integer as they are.
    fn visit_bytes<E: de::Error>(self, bytes: &[u8]) -> Result<(), E> {
        let digits = digits(bytes, self.context)?;
        self.rendering
            .push(digits, &self.context.meter)
            .map_err(over)
    }

    fn visit_str<E: de::Error>(self, value: &str) -> Result<(), E> {
        self.rendering
            .string(value, &self.context.meter)
            .map_err(over)
    }

    fn visit_map<A: MapAccess<'de>>(mut self, mut map: A) -> Result<(), A::Error> {
        nest(move || {
            let context = self.context;
            let meter = &context.meter;
            self.rendering.push("{", meter).map_err(over)?;
            self.rendering.keys.open(meter).map_err(over)?;
            let mut first = true;
            while map
                .next_key_seed(Key {
                    rendering: &mut *self.rendering,
                    first,
                    meter,
                })?
                .is_some()
            {
                first = false;
                map.next_value_seed(self.inner())?;
            }
            let rendering = &mut *self.rendering;
            if let Some(key) = rendering.keys.close(&rendering.text) {
                let key = rendering.text.get(key).unwrap_or_default();
                return Err(context.fail(repeated(key)));
            }
            rendering.push("}", meter).map_err(over)
        })
    }

    fn visit_seq<A: SeqAccess<'de>>(mut self, mut seq: A) -> Result<(), A::Error> {
        nest(move || {
            let context = self.context;
            let meter = &context.meter;
            self.rendering.push("[", meter).map_err(over)?;
            let mut first = true;
            loop {
                // The separator goes before each item, and comes off again when none follows.
                let before = self.rendering.text.len();
                if !first {
                    self.rendering.push(",", meter).map_err(over)?;
                }
                if seq.next_element_seed(self.inner())?.is_none() {
                    self.rendering.text.truncate(before);
                    break;
                }
                first = false;
            }
            self.rendering.push("]", meter).map_err(over)
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
/// and noted among the object's keys, each charged to `meter` before the text or keys grow.
struct Key<'a> {
    rendering: &'a mut Rendering,
    first: bool,
    meter: &'a Meter,
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

    fn visit_str<E: de::Error>(self, key: &str) -> Result<(), E> {
        let rendering = self.rendering;
        if !self.first {
            rendering.push(",", self.meter).map_err(over)?;
        }
        let start = rendering.text.len();
        rendering.string(key, self.meter).map_err(over)?;
        let end = rendering.text.len();
        rendering.keys.key(start..end, self.meter).map_err(over)?;
        rendering.push(":", self.meter).map_err(over)
    }
}
