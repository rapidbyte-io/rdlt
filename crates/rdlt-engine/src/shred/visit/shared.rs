//! What the visitor trees share: a record that must be an object, the keys whose values it skips,
//! a value's nesting limit and an integer's digits read as the narrowest scalar holding them.

use std::fmt;

use arrow_buffer::i256;
use rdlt_connector::Exclusion;
use rdlt_wire::limits::NESTING_DEPTH;
use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};

use super::Context;
use crate::shred::ShredError;
use crate::shred::build::Scalar;

/// The most digits of an integer a decimal holds.
const VAST_DIGITS: usize = 76;

/// The keys of a record's object whose values a parse skips, neither observing nor building them:
/// none, or the keys an exclusion names; a walk is made for each apart, so one that skips none
/// looks no key up.
pub(crate) trait Skipped: Copy {
    /// Whether the value of the key `name` is skipped.
    fn skips(self, name: &str) -> bool;
}

impl Skipped for () {
    fn skips(self, _: &str) -> bool {
        false
    }
}

impl Skipped for &Exclusion {
    fn skips(self, name: &str) -> bool {
        self.contains(name)
    }
}

/// What a record's object is walked into.
pub(crate) trait Fields {
    /// The parse the record is part of.
    fn context(&self) -> &Context;

    /// Walks the record's fields, `map`.
    ///
    /// # Errors
    ///
    /// Where a field's value is refused.
    fn fields<'de, A: MapAccess<'de>>(self, map: A) -> Result<(), A::Error>;
}

/// One record, which is an object walked into `F`; any other value is refused.
pub(crate) struct RecordSeed<F>(pub(crate) F);

impl<F: Fields> RecordSeed<F> {
    #[cold]
    fn not_object<E: de::Error>(&self) -> E {
        self.0.context().fail(ShredError::NotObject)
    }
}

impl<'de, F: Fields> DeserializeSeed<'de> for RecordSeed<F> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_any(self)
    }
}

impl<'de, F: Fields> Visitor<'de> for RecordSeed<F> {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a record")
    }

    fn visit_map<A: MapAccess<'de>>(self, map: A) -> Result<(), A::Error> {
        self.0.fields(map)
    }

    fn visit_unit<E: de::Error>(self) -> Result<(), E> {
        Err(self.not_object())
    }

    fn visit_bool<E: de::Error>(self, _: bool) -> Result<(), E> {
        Err(self.not_object())
    }

    fn visit_i64<E: de::Error>(self, _: i64) -> Result<(), E> {
        Err(self.not_object())
    }

    fn visit_u64<E: de::Error>(self, _: u64) -> Result<(), E> {
        Err(self.not_object())
    }

    fn visit_f64<E: de::Error>(self, _: f64) -> Result<(), E> {
        Err(self.not_object())
    }

    fn visit_str<E: de::Error>(self, _: &str) -> Result<(), E> {
        Err(self.not_object())
    }

    /// An integer beyond 38 digits, which the exact parse visits as its digits.
    fn visit_bytes<E: de::Error>(self, _: &[u8]) -> Result<(), E> {
        Err(self.not_object())
    }

    fn visit_seq<A: SeqAccess<'de>>(self, _: A) -> Result<(), A::Error> {
        Err(self.not_object())
    }
}

/// Refuses a value `depth` levels deep, past the nesting limit.
///
/// # Errors
///
/// [`ShredError::TooDeep`] past the limit.
#[inline]
pub(crate) fn within_depth<E: de::Error>(depth: u64, context: &Context) -> Result<(), E> {
    if depth > NESTING_DEPTH {
        return Err(context.fail(ShredError::TooDeep));
    }
    Ok(())
}

/// The digits of an integer beyond 38 digits, which the exact parse visits as bytes.
///
/// # Errors
///
/// [`ShredError::Internal`] where they are not text, which the exact parse never visits.
pub(crate) fn digits<'a, E: de::Error>(bytes: &'a [u8], context: &Context) -> Result<&'a str, E> {
    std::str::from_utf8(bytes).map_err(|error| {
        context.fail(ShredError::Internal(format!(
            "an integer's digits: {error}"
        )))
    })
}

/// The integer `digits`, beyond 38 digits, as a decimal of 76 where it holds them, or else as
/// digits only JSON text holds.
pub(crate) fn scalar_of_digits(digits: &str) -> Scalar<'_> {
    let vast = (digits.trim_start_matches('-').len() <= VAST_DIGITS)
        .then(|| i256::from_string(digits))
        .flatten();
    vast.map_or(Scalar::Beyond, Scalar::Vast)
}
