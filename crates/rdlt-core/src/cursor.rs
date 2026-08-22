//! Source positions.

use serde::{Deserialize, Serialize};

/// An opaque, source-defined extraction position. The engine only stores it, compares
/// it for equality, and hands it back to the source on the next read — semantics
/// belong entirely to the source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct Cursor(serde_json::Value);

impl Cursor {
    /// Wrap an opaque cursor value. The engine never interprets it — only the
    /// source that produced it knows what it means.
    pub fn new(value: impl Into<serde_json::Value>) -> Self {
        Self(value.into())
    }

    /// The underlying value, for the source that owns its meaning.
    pub fn as_value(&self) -> &serde_json::Value {
        &self.0
    }

    /// What this cursor holds resident, charged conservatively: a
    /// node's own size plus every string's bytes, over the whole tree,
    /// iteratively. A checkpoint is queued, cloned into retained state
    /// and written to the WAL, and its byte ceiling bounds what arrived
    /// rather than what the arrival became — `[0,0,0,…]` spends two
    /// wire bytes per element and buys a node — so a queue that charged
    /// a cursor nothing would let a flood of them ride past the budget
    /// that bounds everything else in it.
    pub fn resident_footprint(&self) -> usize {
        const NODE: usize = std::mem::size_of::<serde_json::Value>();
        let mut pending = vec![&self.0];
        let mut total = 0usize;
        while let Some(node) = pending.pop() {
            total = total.saturating_add(NODE);
            match node {
                serde_json::Value::String(text) => {
                    total = total.saturating_add(text.len());
                }
                serde_json::Value::Array(items) => pending.extend(items.iter()),
                serde_json::Value::Object(fields) => {
                    for (key, value) in fields {
                        total = total.saturating_add(key.len());
                        pending.push(value);
                    }
                }
                _ => {}
            }
        }
        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A cursor is charged for what it holds: a scalar is one node, a
    /// dense array is a node per element, and strings add their bytes.
    #[test]
    fn the_resident_footprint_counts_nodes_and_string_bytes() {
        let node = std::mem::size_of::<serde_json::Value>();
        assert_eq!(Cursor::new(serde_json::json!(7)).resident_footprint(), node);
        assert_eq!(
            Cursor::new(serde_json::json!([0, 0, 0])).resident_footprint(),
            4 * node
        );
        assert_eq!(
            Cursor::new(serde_json::json!({"k": "abc"})).resident_footprint(),
            2 * node + 1 + 3
        );
    }
}
