//! Source positions.

use serde::{Deserialize, Serialize};

/// An opaque, source-defined extraction position. The engine only stores it, compares
/// it for equality, and hands it back via `ReadRequest.since` — semantics belong
/// entirely to the source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Cursor(serde_json::Value);

impl Cursor {
    pub fn new(value: impl Into<serde_json::Value>) -> Self {
        Self(value.into())
    }

    pub fn as_value(&self) -> &serde_json::Value {
        &self.0
    }
}
