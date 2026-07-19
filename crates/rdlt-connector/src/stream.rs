//! Stream declarations (data-model.md §4).

use std::collections::BTreeMap;

use rdlt_core::{LogicalType, StreamName};
use serde::{Deserialize, Serialize};

/// A source's declaration of one stream. Consumed by engine planning; per-column type
/// hints override inference (design doc §5.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamSpec {
    pub name: StreamName,
    /// Declared key: `_rdlt_id` becomes a key hash and `Merge` merges on it.
    pub primary_key: Option<Vec<String>>,
    /// The field carrying the incremental cursor, if the stream supports one.
    pub cursor_field: Option<String>,
    /// Per-column logical-type hints; take precedence over inference.
    pub type_hints: BTreeMap<String, LogicalType>,
    /// This stream pushes already-structured Arrow batches (contract clause S7,
    /// feature 002). Structured streams carry run-level provenance only (no
    /// `_rdlt_id`), so `Merge` is rejected at plan time (clause B4). Additive field:
    /// serde-defaults to `false`.
    #[serde(default)]
    pub structured: bool,
}

impl StreamSpec {
    pub fn new(name: impl Into<StreamName>) -> Self {
        Self {
            name: name.into(),
            primary_key: None,
            cursor_field: None,
            type_hints: BTreeMap::new(),
            structured: false,
        }
    }

    /// Declare this stream as pushing already-structured Arrow batches (clause S7).
    pub fn structured(mut self) -> Self {
        self.structured = true;
        self
    }

    pub fn with_primary_key(mut self, key: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.primary_key = Some(key.into_iter().map(Into::into).collect());
        self
    }

    pub fn with_cursor_field(mut self, field: impl Into<String>) -> Self {
        self.cursor_field = Some(field.into());
        self
    }

    pub fn with_type_hint(mut self, column: impl Into<String>, ty: LogicalType) -> Self {
        self.type_hints.insert(column.into(), ty);
        self
    }
}
