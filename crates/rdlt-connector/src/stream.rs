//! Stream declarations: a source's static description of what it offers.

use std::collections::BTreeMap;

use crate::core::{LogicalType, StreamName};
use serde::{Deserialize, Serialize};

/// A source's declaration of one stream, consumed by host planning.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StreamSpec {
    /// The stream's name — also the destination table it owns.
    pub name: StreamName,
    /// Declared row identity: `_rdlt_id` becomes a key hash and `Merge`
    /// merges on it.
    ///
    /// Merge-key precedence: this and a run's `WriteMode::Merge { key }`
    /// name ONE identity, not two. The host requires them to agree (same
    /// columns, as a set) and rejects a mismatch typed at plan time —
    /// neither silently wins.
    pub primary_key: Option<Vec<String>>,
    /// The field carrying the incremental cursor, when the stream can
    /// resume.
    pub cursor_field: Option<String>,
    /// Per-column logical-type hints; they take precedence over
    /// inference.
    pub type_hints: BTreeMap<String, LogicalType>,
    /// This stream pushes already-structured Arrow batches (no per-row
    /// `_rdlt_id`). A structured stream may still `Merge`, but only on a
    /// declared `primary_key` that the write mode names exactly; a
    /// keyless structured stream cannot Merge (rejected at plan time).
    ///
    /// Serde-defaults to `false` so older serialized declarations
    /// deserialize unchanged.
    #[serde(default)]
    pub structured: bool,
}

impl StreamSpec {
    /// The minimum declaration: no key, no cursor, no hints.
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

    /// Declare the columns identifying a row. Under `Merge` these must
    /// name the same set as the write mode's key — a mismatch is rejected
    /// at plan time rather than arbitrated.
    pub fn with_primary_key(mut self, key: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.primary_key = Some(key.into_iter().map(Into::into).collect());
        self
    }

    /// Declare the field carrying the incremental cursor, enabling
    /// resumable reads.
    pub fn with_cursor_field(mut self, field: impl Into<String>) -> Self {
        self.cursor_field = Some(field.into());
        self
    }

    /// Pin one column's logical type over what inference would choose —
    /// for data that is ambiguous on its own, like an ISO-8601 string
    /// that should be a timestamp rather than text.
    pub fn with_type_hint(mut self, column: impl Into<String>, hint: LogicalType) -> Self {
        self.type_hints.insert(column.into(), hint);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builders compose onto the minimum declaration, and an older wire
    /// form without `structured` deserializes to `false`.
    #[test]
    fn builders_compose_and_the_wire_form_defaults_structured_off() {
        let declared = StreamSpec::new("orders")
            .with_primary_key(["id"])
            .with_cursor_field("updated_at")
            .with_type_hint("updated_at", LogicalType::TimestampTz);
        assert_eq!(declared.primary_key.as_deref(), Some(&["id".into()][..]));
        assert_eq!(declared.cursor_field.as_deref(), Some("updated_at"));
        assert!(!declared.structured);
        assert!(StreamSpec::new("raw").with_structured().structured);

        let wire = serde_json::json!({
            "name": "orders",
            "primary_key": null,
            "cursor_field": null,
            "type_hints": {},
        });
        let parsed: StreamSpec = serde_json::from_value(wire).expect("older form parses");
        assert!(!parsed.structured, "absent `structured` defaults to false");
    }
}
