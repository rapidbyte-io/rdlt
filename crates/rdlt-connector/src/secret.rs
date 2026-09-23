//! Configuration values that must never be printed.

#[cfg(test)]
mod tests;

use std::fmt;

use schemars::JsonSchema;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A value that is redacted wherever it is displayed or serialized; read it with [`Secret::expose`].
#[derive(Clone, PartialEq, Eq, JsonSchema)]
#[serde(transparent)]
pub struct Secret<T>(T);

impl<T> Secret<T> {
    /// Wraps `value`.
    pub fn new(value: T) -> Self {
        Self(value)
    }

    /// The secret value.
    pub fn expose(&self) -> &T {
        &self.0
    }
}

const REDACTED: &str = "***";

impl<T> fmt::Debug for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Secret({REDACTED})")
    }
}

impl<T> fmt::Display for Secret<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(REDACTED)
    }
}

impl<T> Serialize for Secret<T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(REDACTED)
    }
}

/// Deserializes the inner value, replacing its error: serde's messages quote the rejected input.
impl<'de, T: Deserialize<'de>> Deserialize<'de> for Secret<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        T::deserialize(deserializer)
            .map(Self)
            .map_err(|_| D::Error::custom("the secret value is invalid (redacted)"))
    }
}
