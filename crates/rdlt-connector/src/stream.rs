//! Stream declarations: a source's static description of each stream it offers.

use std::collections::BTreeMap;

use rdlt_core::{LogicalType, StreamName};
use serde::{Deserialize, Serialize};

/// A source's declaration of one stream. Consumed by engine planning; per-column type
/// hints override inference.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamSpec {
    /// The stream's name, which is also the destination table name it owns.
    pub name: StreamName,
    /// Declared key: `_rdlt_id` becomes a key hash and `Merge` merges on it.
    ///
    /// Merge-key precedence: this and a stream's `WriteMode::Merge { key }` name
    /// one identity, not two. The engine requires them to AGREE (same columns,
    /// as a set) and rejects a mismatch typed at plan time — neither silently
    /// wins over the other.
    pub primary_key: Option<Vec<String>>,
    /// The field carrying the incremental cursor, if the stream supports one.
    pub cursor_field: Option<String>,
    /// Per-column logical-type hints; take precedence over inference.
    pub type_hints: BTreeMap<String, LogicalType>,
    /// This stream pushes already-structured Arrow batches (no per-row `_rdlt_id`).
    /// A structured stream may still `Merge`, but only on a declared `primary_key`
    /// that its `WriteMode::Merge { key }` names exactly; a keyless structured
    /// stream cannot Merge (rejected at plan time).
    /// Serde-defaults to `false` so older payloads deserialize unchanged.
    #[serde(default)]
    pub structured: bool,
}

impl StreamSpec {
    /// A stream with no key, no cursor, and no hints — the minimum declaration.
    pub fn new(name: impl Into<StreamName>) -> Self {
        Self {
            name: name.into(),
            primary_key: None,
            cursor_field: None,
            type_hints: BTreeMap::new(),
            structured: false,
        }
    }

    /// Declare this stream as pushing already-structured Arrow batches.
    pub fn with_structured(mut self) -> Self {
        self.structured = true;
        self
    }

    /// Declare the key that identifies a row. Under `Merge` this must name the
    /// same columns as the write mode's key — the engine rejects a mismatch at
    /// plan time rather than picking one.
    pub fn with_primary_key(mut self, key: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.primary_key = Some(key.into_iter().map(Into::into).collect());
        self
    }

    /// Declare the field carrying the incremental cursor, enabling resumable
    /// reads for this stream.
    pub fn with_cursor_field(mut self, field: impl Into<String>) -> Self {
        self.cursor_field = Some(field.into());
        self
    }

    /// Pin one column's logical type, overriding what inference would choose.
    /// Useful where the data alone is ambiguous — an ISO-8601 string that should
    /// be a timestamp rather than text.
    pub fn with_type_hint(mut self, column: impl Into<String>, ty: LogicalType) -> Self {
        self.type_hints.insert(column.into(), ty);
        self
    }
}
