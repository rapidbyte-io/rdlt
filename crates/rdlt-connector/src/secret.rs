//! Configuration values that must never be printed.

#[cfg(test)]
mod tests;

use std::fmt;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize, Serializer};

/// A value that is redacted wherever it is displayed or serialized; read it with [`Secret::expose`].
#[derive(Clone, PartialEq, Eq, Deserialize, JsonSchema)]
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
