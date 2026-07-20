//! The JSON view seam (feature 003 R24): one definition of shred semantics,
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
pub(crate) enum Kind<'a> {
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
    type ObjIter: Iterator<Item = (&'a str, Self)>;
    type ArrIter: Iterator<Item = Self>;

    fn kind(self) -> Kind<'a>;
    /// Entries in sorted-key, last-duplicate-wins order (see view contract).
    fn obj_entries(self) -> Self::ObjIter;
    fn arr_items(self) -> Self::ArrIter;
    /// Top-level object lookup (last duplicate wins); `None` off objects.
    fn obj_get(self, key: &str) -> Option<Self>;

    fn is_null(self) -> bool {
        matches!(self.kind(), Kind::Null)
    }
    fn is_object(self) -> bool {
        matches!(self.kind(), Kind::Object)
    }
    fn is_array(self) -> bool {
        matches!(self.kind(), Kind::Array)
    }
}

// ---- the tree view: `&serde_json::Value` ----

impl<'a> JsonView<'a> for &'a Value {
    type ObjIter = ValueObjIter<'a>;
    type ArrIter = std::slice::Iter<'a, Value>;

    fn kind(self) -> Kind<'a> {
        match self {
            Value::Null => Kind::Null,
            Value::Bool(b) => Kind::Bool(*b),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Kind::Int(i)
                } else if let Some(u) = n.as_u64() {
                    Kind::UInt(u)
                } else {
                    Kind::Float(n.as_f64().expect("JSON numbers are i64/u64/f64"))
                }
            }
            Value::String(s) => Kind::Str(s),
            Value::Object(_) => Kind::Object,
            Value::Array(_) => Kind::Array,
        }
    }

    fn obj_entries(self) -> Self::ObjIter {
        match self {
            Value::Object(map) => ValueObjIter(map.iter()),
            _ => ValueObjIter(EMPTY_MAP.iter()),
        }
    }

    fn arr_items(self) -> Self::ArrIter {
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

pub(crate) struct ValueObjIter<'a>(serde_json::map::Iter<'a>);

impl<'a> Iterator for ValueObjIter<'a> {
    type Item = (&'a str, &'a Value);
    fn next(&mut self) -> Option<Self::Item> {
        self.0.next().map(|(k, v)| (k.as_str(), v))
    }
}
