//! The JSON view seam: one definition of shred semantics,
//! two representations.
//!
//! Everything semantics-bearing in the shredder — type observation, canonical
//! bytes, identity hashing, policy fit checks, Arrow building — is written ONCE,
//! generic over [`JsonView`]. Production backs it with an arena node borrowing
//! the input slab (`arena::Node`); `&serde_json::Value` keeps a view too — it
//! powers the unit tests and holds the seam honest. Correctness of the arena
//! view against serde_json semantics is pinned by the contract below plus the
//! canonical-agreement tests in `arena.rs`.
//!
//! ## View contract (what both implementations MUST guarantee)
//!
//! - `obj_entries()` yields entries in the map's NATIVE order: this workspace
//!   compiles serde_json with `preserve_order` (arrow-json requires it), so
//!   that is FIRST-OCCURRENCE position with duplicate keys collapsed to the
//!   LAST occurrence's value — IndexMap insert semantics. Field first-seen
//!   order (schema column order!) depends on this.
//! - Canonicalization does NOT rely on entry order — `canonical_json_bytes`
//!   sorts keys explicitly.
//! - Numbers decompose exactly like `serde_json::Number`: `as_i64` first, then
//!   `as_u64` (beyond `i64::MAX`), else finite `f64`.
//! - `Str` is the UNESCAPED string content.

use serde_json::Value;

/// One JSON value's shape + scalar payload, decomposed for observation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum ValueKind<'a> {
    Null,
    Bool(bool),
    Int(i64),
    /// Beyond `i64::MAX` only (mirrors `serde_json::Number::as_u64` fallback).
    UInt(u64),
    Float(f64),
    Str(&'a str),
    Object,
    Array,
}

pub(crate) trait JsonView<'a>: Copy {
    type ObjectIter: Iterator<Item = (&'a str, Self)>;
    type ArrayIter: Iterator<Item = Self>;

    fn kind(self) -> ValueKind<'a>;
    /// Entries in NATIVE order — first-occurrence position, last-occurrence
    /// value (see the view contract above); canonicalization sorts separately.
    fn obj_entries(self) -> Self::ObjectIter;
    fn arr_items(self) -> Self::ArrayIter;
    /// Top-level object lookup (last duplicate wins); `None` off objects.
    fn obj_get(self, key: &str) -> Option<Self>;

    fn is_null(self) -> bool {
        matches!(self.kind(), ValueKind::Null)
    }
    fn is_object(self) -> bool {
        matches!(self.kind(), ValueKind::Object)
    }
    fn is_array(self) -> bool {
        matches!(self.kind(), ValueKind::Array)
    }
}

// ---- the tree view: `&serde_json::Value` ----

impl<'a> JsonView<'a> for &'a Value {
    type ObjectIter = ValueObjectIter<'a>;
    type ArrayIter = std::slice::Iter<'a, Value>;

    fn kind(self) -> ValueKind<'a> {
        match self {
            Value::Null => ValueKind::Null,
            Value::Bool(b) => ValueKind::Bool(*b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    ValueKind::Int(i)
                } else if let Some(u) = n.as_u64() {
                    ValueKind::UInt(u)
                } else {
                    ValueKind::Float(n.as_f64().expect("JSON numbers are i64/u64/f64"))
                }
            }
            Value::String(s) => ValueKind::Str(s),
            Value::Object(_) => ValueKind::Object,
            Value::Array(_) => ValueKind::Array,
        }
    }

    fn obj_entries(self) -> Self::ObjectIter {
        match self {
            Value::Object(map) => ValueObjectIter(map.iter()),
            _ => ValueObjectIter(EMPTY_MAP.iter()),
        }
    }

    fn arr_items(self) -> Self::ArrayIter {
        match self {
            Value::Array(items) => items.iter(),
            _ => [].iter(),
        }
    }

    fn obj_get(self, key: &str) -> Option<Self> {
        self.get(key)
    }
}

static EMPTY_MAP: std::sync::LazyLock<serde_json::Map<String, Value>> =
    std::sync::LazyLock::new(serde_json::Map::new);

pub(crate) struct ValueObjectIter<'a>(serde_json::map::Iter<'a>);

impl<'a> Iterator for ValueObjectIter<'a> {
    type Item = (&'a str, &'a Value);
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| (k.as_str(), v))
    }
}
