//! Configuration values that must never be printed.

#[cfg(test)]
mod tests;

use std::fmt;

use schemars::JsonSchema;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use subtle::ConstantTimeEq as _;
use zeroize::Zeroize;

/// A value that is redacted wherever it is displayed or serialized, and wiped from memory when
/// it is dropped; read it with [`Secret::expose`].
///
/// A secret is not `Clone`: a copy is made on purpose, with [`Secret::duplicate`]. Two
/// secrets of bytes or text compare in a time that depends on their lengths alone.
#[derive(JsonSchema)]
#[serde(transparent)]
pub struct Secret<T: Zeroize>(T);

impl<T: Zeroize> Secret<T> {
    /// Wraps `value`.
    pub fn new(value: T) -> Self {
        Self(value)
    }

    /// The secret value.
    pub fn expose(&self) -> &T {
        &self.0
    }

    /// A second copy of the secret, wiped when dropped as this one is.
    #[must_use]
    pub fn duplicate(&self) -> Self
    where
        T: Clone,
    {
        Self(self.0.clone())
    }
}

impl<T: Zeroize> Drop for Secret<T> {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl<T: Zeroize + AsRef<[u8]>> PartialEq for Secret<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.as_ref().ct_eq(other.0.as_ref()).into()
    }
}

impl<T: Zeroize + AsRef<[u8]>> Eq for Secret<T> {}

const REDACTED: &str = "***";

impl<T: Zeroize> fmt::Debug for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Secret({REDACTED})")
    }
}

impl<T: Zeroize> fmt::Display for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

/// Serializes the redaction, never the value: what is written is not read back as the secret.
impl<T: Zeroize> Serialize for Secret<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(REDACTED)
    }
}

/// Deserializes the inner value, replacing its error: serde's messages quote the rejected input.
impl<'de, T: Zeroize + Deserialize<'de>> Deserialize<'de> for Secret<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize(deserializer)
            .map(Self)
            .map_err(|_| D::Error::custom("the secret value is invalid (redacted)"))
    }
}
