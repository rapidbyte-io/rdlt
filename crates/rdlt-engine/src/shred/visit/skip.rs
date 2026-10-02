//! Values of a column that stopped building: only their nesting is checked.

use std::fmt;

use serde::de::{DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor};

use super::{Context, MAX_DEPTH, nest};
use crate::shred::ShredError;

/// One value, `depth` levels deep, of a column that stopped building.
pub(crate) struct Skip<'a> {
    pub(crate) context: &'a Context,
    pub(crate) depth: u64,
}

impl<'de> DeserializeSeed<'de> for Skip<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        if self.depth > MAX_DEPTH {
            return Err(self.context.fail(ShredError::TooDeep));
        }
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for Skip<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a JSON value")
    }

    fn visit_unit<E>(self) -> Result<(), E> {
        Ok(())
    }

    fn visit_bool<E>(self, _: bool) -> Result<(), E> {
        Ok(())
    }

    fn visit_i64<E>(self, _: i64) -> Result<(), E> {
        Ok(())
    }

    fn visit_u64<E>(self, _: u64) -> Result<(), E> {
        Ok(())
    }

    fn visit_f64<E>(self, value: f64) -> Result<(), E> {
        // The column's values are rendered as JSON text when it is built again, exactly only
        // once the chunk is parsed exactly.
        self.context.float(value);
        Ok(())
    }

    fn visit_i128<E>(self, _: i128) -> Result<(), E> {
        Ok(())
    }

    fn visit_bytes<E>(self, _: &[u8]) -> Result<(), E> {
        Ok(())
    }

    fn visit_str<E>(self, _: &str) -> Result<(), E> {
        Ok(())
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        nest(move || {
            while map.next_key::<IgnoredAny>()?.is_some() {
                map.next_value_seed(Skip {
                    context: self.context,
                    depth: self.depth + 1,
                })?;
            }
            Ok(())
        })
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<(), A::Error> {
        nest(move || {
            let depth = self.depth + 1;
            while seq
                .next_element_seed(Skip {
                    context: self.context,
                    depth,
                })?
                .is_some()
            {}
            Ok(())
        })
    }
}
