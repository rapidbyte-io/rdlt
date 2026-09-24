//! Values of `Json` columns, rendered as compact JSON text as they are parsed.
//!
//! Only a chunk's second parse renders, over records its first parse already checked, so rendering
//! recurses no deeper than the nesting limit.

use std::collections::BTreeSet;
use std::fmt;

use serde::de::{DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};

use super::ShredError;
use super::visit::{Context, nest};

/// One value rendered onto `text`; an object repeating a key fails.
pub(crate) struct Render<'a> {
    pub(crate) text: &'a mut String,
    pub(crate) context: &'a Context,
}

impl Render<'_> {
    /// Renders `value` with sonic-rs, which escapes strings and writes floats in their shortest
    /// round-trip form.
    fn serialized<E: serde::de::Error, T: serde::Serialize + ?Sized>(
        &mut self,
        value: &T,
    ) -> Result<(), E> {
        let rendered = sonic_rs::to_string(value).map_err(|error| {
            self.context.fail(ShredError::Internal(format!(
                "rendering a JSON value: {error}"
            )))
        })?;
        self.text.push_str(&rendered);
        Ok(())
    }

    /// A value nested in this one.
    fn inner(&mut self) -> Render<'_> {
        Render {
            text: &mut *self.text,
            context: self.context,
        }
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
        self.text.push_str("null");
        Ok(())
    }

    fn visit_bool<E>(self, value: bool) -> Result<(), E> {
        self.text.push_str(if value { "true" } else { "false" });
        Ok(())
    }

    fn visit_i64<E>(self, value: i64) -> Result<(), E> {
        self.text.push_str(&value.to_string());
        Ok(())
    }

    fn visit_u64<E>(self, value: u64) -> Result<(), E> {
        self.text.push_str(&value.to_string());
        Ok(())
    }

    fn visit_f64<E: serde::de::Error>(mut self, value: f64) -> Result<(), E> {
        self.serialized(&value)
    }

    fn visit_str<E: serde::de::Error>(mut self, value: &str) -> Result<(), E> {
        self.serialized(value)
    }

    fn visit_map<A: MapAccess<'de>>(mut self, mut map: A) -> Result<(), A::Error> {
        nest(move || {
            let mut keys = BTreeSet::new();
            self.text.push('{');
            while let Some(key) = map.next_key::<String>()? {
                if keys.contains(&key) {
                    return Err(self.context.fail(ShredError::DuplicateKey(key)));
                }
                if !keys.is_empty() {
                    self.text.push(',');
                }
                self.serialized(key.as_str())?;
                self.text.push(':');
                keys.insert(key);
                map.next_value_seed(self.inner())?;
            }
            self.text.push('}');
            Ok(())
        })
    }

    fn visit_seq<A: SeqAccess<'de>>(mut self, mut seq: A) -> Result<(), A::Error> {
        nest(move || {
            self.text.push('[');
            let mut first = true;
            loop {
                // The separator goes before each item, and comes off again when none follows.
                let before = self.text.len();
                if !first {
                    self.text.push(',');
                }
                if seq.next_element_seed(self.inner())?.is_none() {
                    self.text.truncate(before);
                    break;
                }
                first = false;
            }
            self.text.push(']');
            Ok(())
        })
    }
}
