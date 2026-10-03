//! The exact parse: a record parsed with its numbers as their text, then walked as the fast parse
//! walks one, each number read as the narrowest type that holds it exactly.

use serde::de::{
    self, DeserializeSeed, Deserializer, IntoDeserializer, MapAccess, SeqAccess, Visitor,
};
use sonic_rs::{JsonContainerTrait, JsonType, JsonValueTrait, Value};

use super::ShredError;
use super::visit::{Context, DECIMAL_LIMIT};
use crate::limits::QUOTED_BYTES;

/// Parses the record `bytes` into `seed`, its numbers exact.
pub(super) fn visit<S>(bytes: &[u8], seed: S, context: &Context) -> Result<(), sonic_rs::Error>
where
    S: for<'de> DeserializeSeed<'de, Value = ()>,
{
    // Parsing numbers as their text leaves strings unchecked, so the record's text is checked first.
    let Ok(text) = std::str::from_utf8(bytes) else {
        return Err(de::Error::custom("the record is not valid UTF-8"));
    };
    // Parsing builds the record whole, recursing once per level, so depth is checked first.
    if crate::json::too_deep(text.as_bytes()) {
        return Err(context.fail(ShredError::TooDeep));
    }
    let mut deserializer = sonic_rs::Deserializer::from_str(text).use_rawnumber();
    let value: Value = serde::Deserialize::deserialize(&mut deserializer)?;
    deserializer.end()?;
    seed.deserialize(Exact {
        value: &value,
        context,
    })
}

/// A parsed value, walked with its numbers exact.
struct Exact<'a> {
    value: &'a Value,
    context: &'a Context,
}

impl<'de> Deserializer<'de> for Exact<'_> {
    type Error = sonic_rs::Error;

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let Self { value, context } = self;
        match value.get_type() {
            JsonType::Null => visitor.visit_unit(),
            JsonType::Boolean => visitor.visit_bool(value.as_bool().unwrap_or_default()),
            JsonType::Number => number(value, context, visitor),
            JsonType::String => visitor.visit_str(value.as_str().unwrap_or_default()),
            JsonType::Object => {
                let entries = value
                    .as_object()
                    .into_iter()
                    .flat_map(|object| object.iter());
                visitor.visit_map(Entries {
                    fields: entries.collect::<Vec<_>>().into_iter(),
                    value: None,
                    context,
                })
            }
            JsonType::Array => {
                let items = value.as_array().into_iter().flat_map(|array| array.iter());
                visitor.visit_seq(Items {
                    items: items.collect::<Vec<_>>().into_iter(),
                    context,
                })
            }
        }
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string bytes byte_buf
        option unit unit_struct newtype_struct seq tuple tuple_struct map struct enum identifier
        ignored_any
    }
}

/// Visits the number `value` as the narrowest of a 64-bit integer, a 38-digit integer and a
/// float that holds its text exactly, or else, an integer, as its digits.
fn number<'de, V: Visitor<'de>>(
    value: &Value,
    context: &Context,
    visitor: V,
) -> Result<V::Value, sonic_rs::Error> {
    let text = value
        .as_raw_number()
        .map(|number| number.as_str().to_owned())
        .unwrap_or_default();
    // The fast parse reads `-0` as a float, and refuses floats beyond the finite ones.
    if text.contains(['.', 'e', 'E']) || text == "-0" {
        if !crate::json::exponent_within(&text) {
            let shown = rdlt_connector::text::shown(&text, QUOTED_BYTES);
            return Err(context.fail(ShredError::Exponent(shown)));
        }
        let float: f64 = text.parse().map_err(de::Error::custom)?;
        if !float.is_finite() {
            let shown = rdlt_connector::text::shown(&text, QUOTED_BYTES);
            let refused =
                ShredError::Invalid(format!("the number {shown} is beyond a float's range"));
            return Err(context.fail(refused));
        }
        // A column of JSON renders the number as it was written.
        context.number(&text);
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

/// The fields of an object, walked in order.
struct Entries<'a> {
    fields: std::vec::IntoIter<(&'a str, &'a Value)>,
    value: Option<&'a Value>,
    context: &'a Context,
}

impl<'de> MapAccess<'de> for Entries<'_> {
    type Error = sonic_rs::Error;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Self::Error> {
        let Some((key, value)) = self.fields.next() else {
            return Ok(None);
        };
        self.value = Some(value);
        seed.deserialize(key.into_deserializer()).map(Some)
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, Self::Error> {
        let value = self
            .value
            .take()
            .ok_or_else(|| de::Error::custom("a value asked for before its key"))?;
        seed.deserialize(Exact {
            value,
            context: self.context,
        })
    }
}

/// The items of an array, walked in order.
struct Items<'a> {
    items: std::vec::IntoIter<&'a Value>,
    context: &'a Context,
}

impl<'de> SeqAccess<'de> for Items<'_> {
    type Error = sonic_rs::Error;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Self::Error> {
        let Some(value) = self.items.next() else {
            return Ok(None);
        };
        seed.deserialize(Exact {
            value,
            context: self.context,
        })
        .map(Some)
    }
}
