//! Slab arena: the streaming shred path's JSON representation (feature 003 R24).
//!
//! One arena per pushed slab. Parsing goes through serde_json's OWN parser via a
//! `DeserializeSeed` — every lexical edge case (escapes, surrogates, number
//! grammar, depth limits) behaves exactly as the tree path's `Value` parse —
//! but lands in three flat vectors instead of per-row allocated trees:
//! nodes, object entries, array items. Strings and keys borrow from the slab
//! whenever they contain no escapes.
//!
//! Object entries are stored deduplicated with IndexMap insert semantics
//! (first-occurrence position, last-occurrence value) at parse time, so
//! [`Node`]'s `JsonView` satisfies the view contract with plain slices.

use std::borrow::Cow;
use std::fmt;
use std::marker::PhantomData;

use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};

use super::view::{JsonView, Kind};

/// Index of a node within its arena.
pub(crate) type NodeId = u32;

/// Arena indices are u32 by design (half the footprint of usize on the hot
/// vectors). A single document dense enough to overflow them (~4.29e9 nodes,
/// which is ~100 GB of arena before the cast even matters) panics with a clear
/// message instead of silently wrapping into aliased ranges; the engine
/// surfaces test/prod panics from the shred stage as typed task errors.
#[inline]
fn checked_idx(len: usize) -> u32 {
    u32::try_from(len).expect("arena index overflow: a single JSON document exceeds 4.29e9 nodes")
}

#[derive(Debug)]
pub(crate) enum ANode<'s> {
    Null,
    Bool(bool),
    Int(i64),
    /// Beyond `i64::MAX` only (serde_json visits u64 for those).
    UInt(u64),
    Float(f64),
    Str(Cow<'s, str>),
    /// Range into `Arena::obj_entries`.
    Obj(u32, u32),
    /// Range into `Arena::arr_items`.
    Arr(u32, u32),
}

#[derive(Debug, Default)]
pub(crate) struct Arena<'s> {
    nodes: Vec<ANode<'s>>,
    obj_entries: Vec<(Cow<'s, str>, NodeId)>,
    arr_items: Vec<NodeId>,
}

impl<'s> Arena<'s> {
    /// Parse a raw-JSON push (NDJSON, a top-level array, or a single document)
    /// into this arena, returning the ROW nodes: top-level arrays flatten into
    /// their items, and non-object rows are wrapped as `{"value": …}` — the
    /// exact semantics of `table::parse_rows` + the traversal's wrapping.
    ///
    /// Documents are located zero-copy via `&RawValue` (serde_json's stream
    /// iterator cannot seed), then each document slice is seed-parsed into the
    /// arena — serde_json's parser both times, so lexical edge cases match the
    /// tree path exactly.
    pub(crate) fn parse_rows(&mut self, bytes: &'s [u8]) -> Result<Vec<NodeId>, serde_json::Error> {
        let mut rows = Vec::new();
        for raw in serde_json::Deserializer::from_slice(bytes)
            .into_iter::<&'s serde_json::value::RawValue>()
        {
            let raw = raw?;
            let mut de = serde_json::Deserializer::from_str(raw.get());
            let node = NodeSeed { arena: self }.deserialize(&mut de)?;
            de.end()?;
            match self.nodes[node as usize] {
                ANode::Arr(start, end) => {
                    for i in start..end {
                        rows.push(self.arr_items[i as usize]);
                    }
                }
                _ => rows.push(node),
            }
        }
        // Wrap AFTER parsing (arena mutation is safe on plain ids).
        for row in &mut rows {
            if !matches!(self.nodes[*row as usize], ANode::Obj(..)) {
                *row = self.wrap_in_value_obj(*row);
            }
        }
        Ok(rows)
    }

    /// `{"value": <node>}` — for bare-scalar/array rows and scalar child items.
    pub(crate) fn wrap_in_value_obj(&mut self, node: NodeId) -> NodeId {
        let start = checked_idx(self.obj_entries.len());
        self.obj_entries.push((Cow::Borrowed("value"), node));
        self.push_node(ANode::Obj(start, start + 1))
    }

    pub(crate) fn node(&self, id: NodeId) -> Node<'_, 's> {
        Node { arena: self, id }
    }

    fn push_node(&mut self, node: ANode<'s>) -> NodeId {
        let id = checked_idx(self.nodes.len());
        self.nodes.push(node);
        id
    }
}

/// A borrowed view of one arena node — the streaming path's `JsonView`.
#[derive(Clone, Copy)]
pub(crate) struct Node<'a, 's> {
    arena: &'a Arena<'s>,
    id: NodeId,
}

impl<'a, 's> Node<'a, 's> {
    pub(crate) fn id(&self) -> NodeId {
        self.id
    }
}

impl fmt::Debug for Node<'_, '_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Node({})", self.id)
    }
}

impl<'a, 's: 'a> JsonView<'a> for Node<'a, 's> {
    type ObjIter = ObjIter<'a, 's>;
    type ArrIter = ArrIter<'a, 's>;

    fn kind(self) -> Kind<'a> {
        match &self.arena.nodes[self.id as usize] {
            ANode::Null => Kind::Null,
            ANode::Bool(b) => Kind::Bool(*b),
            ANode::Int(i) => Kind::Int(*i),
            ANode::UInt(u) => Kind::UInt(*u),
            ANode::Float(f) => Kind::Float(*f),
            ANode::Str(s) => Kind::Str(s.as_ref()),
            ANode::Obj(..) => Kind::Object,
            ANode::Arr(..) => Kind::Array,
        }
    }

    fn obj_entries(self) -> Self::ObjIter {
        let (start, end) = match self.arena.nodes[self.id as usize] {
            ANode::Obj(start, end) => (start, end),
            _ => (0, 0),
        };
        ObjIter {
            arena: self.arena,
            next: start,
            end,
        }
    }

    fn arr_items(self) -> Self::ArrIter {
        let (start, end) = match self.arena.nodes[self.id as usize] {
            ANode::Arr(start, end) => (start, end),
            _ => (0, 0),
        };
        ArrIter {
            arena: self.arena,
            next: start,
            end,
        }
    }

    fn obj_get(self, key: &str) -> Option<Self> {
        let ANode::Obj(start, end) = self.arena.nodes[self.id as usize] else {
            return None;
        };
        self.arena.obj_entries[start as usize..end as usize]
            .iter()
            .find(|(k, _)| k.as_ref() == key)
            .map(|(_, id)| self.arena.node(*id))
    }
}

pub(crate) struct ObjIter<'a, 's> {
    arena: &'a Arena<'s>,
    next: u32,
    end: u32,
}

impl<'a, 's: 'a> Iterator for ObjIter<'a, 's> {
    type Item = (&'a str, Node<'a, 's>);
    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.end {
            return None;
        }
        let (key, id) = &self.arena.obj_entries[self.next as usize];
        self.next += 1;
        Some((key.as_ref(), self.arena.node(*id)))
    }
}

pub(crate) struct ArrIter<'a, 's> {
    arena: &'a Arena<'s>,
    next: u32,
    end: u32,
}

impl<'a, 's: 'a> Iterator for ArrIter<'a, 's> {
    type Item = Node<'a, 's>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.next >= self.end {
            return None;
        }
        let id = self.arena.arr_items[self.next as usize];
        self.next += 1;
        Some(self.arena.node(id))
    }
}

// ---- serde construction ----

struct NodeSeed<'a, 's> {
    arena: &'a mut Arena<'s>,
}

impl<'de, 's: 'de, 'a> DeserializeSeed<'de> for NodeSeed<'a, 's>
where
    'de: 's,
{
    type Value = NodeId;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(NodeVisitor { arena: self.arena })
    }
}

struct NodeVisitor<'a, 's> {
    arena: &'a mut Arena<'s>,
}

impl<'de, 's: 'de, 'a> Visitor<'de> for NodeVisitor<'a, 's>
where
    'de: 's,
{
    type Value = NodeId;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("any JSON value")
    }

    fn visit_unit<E>(self) -> Result<NodeId, E> {
        Ok(self.arena.push_node(ANode::Null))
    }

    fn visit_bool<E>(self, b: bool) -> Result<NodeId, E> {
        Ok(self.arena.push_node(ANode::Bool(b)))
    }

    fn visit_i64<E>(self, i: i64) -> Result<NodeId, E> {
        Ok(self.arena.push_node(ANode::Int(i)))
    }

    fn visit_u64<E>(self, u: u64) -> Result<NodeId, E> {
        // Mirror serde_json::Number: within i64 range it IS an i64.
        Ok(self.arena.push_node(if u <= i64::MAX as u64 {
            ANode::Int(u as i64)
        } else {
            ANode::UInt(u)
        }))
    }

    fn visit_f64<E>(self, f: f64) -> Result<NodeId, E> {
        Ok(self.arena.push_node(ANode::Float(f)))
    }

    fn visit_borrowed_str<E>(self, s: &'de str) -> Result<NodeId, E> {
        Ok(self.arena.push_node(ANode::Str(Cow::Borrowed(s))))
    }

    fn visit_str<E>(self, s: &str) -> Result<NodeId, E> {
        Ok(self.arena.push_node(ANode::Str(Cow::Owned(s.to_owned()))))
    }

    fn visit_string<E>(self, s: String) -> Result<NodeId, E> {
        Ok(self.arena.push_node(ANode::Str(Cow::Owned(s))))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<NodeId, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut items: Vec<NodeId> = Vec::new();
        while let Some(id) = seq.next_element_seed(NodeSeed { arena: self.arena })? {
            items.push(id);
        }
        let start = checked_idx(self.arena.arr_items.len());
        self.arena.arr_items.extend_from_slice(&items);
        let end = checked_idx(self.arena.arr_items.len());
        Ok(self.arena.push_node(ANode::Arr(start, end)))
    }

    fn visit_map<A>(self, mut map: A) -> Result<NodeId, A::Error>
    where
        A: MapAccess<'de>,
    {
        // IndexMap insert semantics AT PARSE TIME: first position, last value.
        let mut entries: Vec<(Cow<'s, str>, NodeId)> = Vec::new();
        while let Some(key) = map.next_key_seed(KeySeed(PhantomData))? {
            let value = map.next_value_seed(NodeSeed { arena: self.arena })?;
            match entries.iter_mut().find(|(k, _)| *k == key) {
                Some((_, slot)) => *slot = value,
                None => entries.push((key, value)),
            }
        }
        let start = checked_idx(self.arena.obj_entries.len());
        self.arena.obj_entries.extend(entries);
        let end = checked_idx(self.arena.obj_entries.len());
        Ok(self.arena.push_node(ANode::Obj(start, end)))
    }
}

struct KeySeed<'s>(PhantomData<&'s ()>);

impl<'de, 's: 'de> DeserializeSeed<'de> for KeySeed<'s>
where
    'de: 's,
{
    type Value = Cow<'s, str>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct V<'s>(PhantomData<&'s ()>);
        impl<'de, 's: 'de> Visitor<'de> for V<'s>
        where
            'de: 's,
        {
            type Value = Cow<'s, str>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("an object key")
            }
            fn visit_borrowed_str<E>(self, s: &'de str) -> Result<Self::Value, E> {
                Ok(Cow::Borrowed(s))
            }
            fn visit_str<E>(self, s: &str) -> Result<Self::Value, E> {
                Ok(Cow::Owned(s.to_owned()))
            }
            fn visit_string<E>(self, s: String) -> Result<Self::Value, E> {
                Ok(Cow::Owned(s))
            }
        }
        deserializer.deserialize_str(V(PhantomData))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shred::canon::canonical_json_bytes;
    use crate::shred::view::JsonView;
    use serde_json::Value;

    /// Duplicate keys keep FIRST-occurrence POSITION with LAST-occurrence value
    /// (IndexMap insert semantics — the view contract schema column order
    /// depends on). Canonicalization sorts and is structurally blind to
    /// position, so this asserts the entry ORDER directly, against both views.
    #[test]
    fn duplicate_keys_keep_first_position_last_value() {
        let input = br#"{"dup":1,"other":2,"dup":3}"#;
        let mut arena = Arena::default();
        let rows = arena.parse_rows(input).expect("parse");
        let node = arena.node(rows[0]);
        let arena_entries: Vec<(String, String)> = node
            .obj_entries()
            .map(|(k, v)| (k.to_owned(), format!("{:?}", v.kind())))
            .collect();
        assert_eq!(
            arena_entries,
            vec![
                ("dup".to_owned(), "Int(3)".to_owned()),
                ("other".to_owned(), "Int(2)".to_owned()),
            ],
            "first position, last value"
        );
        // The &Value view (serde_json preserve_order) must agree exactly.
        let value: Value = serde_json::from_slice(input).expect("value parse");
        let value_entries: Vec<(String, String)> = (&value)
            .obj_entries()
            .map(|(k, v)| (k.to_owned(), format!("{:?}", v.kind())))
            .collect();
        assert_eq!(arena_entries, value_entries, "views agree on dup-key order");
    }

    /// The two views must agree on canonical bytes for any input — parsing,
    /// dedup, ordering, numbers, and escapes all fold into this one check.
    #[test]
    fn arena_and_value_agree_on_canonical_bytes() {
        let cases: &[&str] = &[
            r#"{"b":1,"a":{"y":[1,2.5,null],"x":"s"}}"#,
            r#"{"dup":1,"other":2,"dup":3}"#,
            r#"[{"n":9007199254740993},{"n":18446744073709551615}]"#,
            r#"{"esc":"a\"b\\c\ndé","emoji":"日本語"}"#,
            r#"[1,"two",true,null,{"k":[]}]"#,
            "42",
            r#""bare string""#,
        ];
        for case in cases {
            let bytes = case.as_bytes();
            let mut arena = Arena::default();
            let arena_rows = arena.parse_rows(bytes).expect("arena parse");
            let value_rows = crate::shred::table::parse_rows(bytes).expect("value parse");
            assert_eq!(arena_rows.len(), value_rows.len(), "row count for {case}");
            for (node, value) in arena_rows.iter().zip(&value_rows) {
                // Wrap the Value side exactly like push_row does.
                let value = match value {
                    Value::Object(_) => value.clone(),
                    other => serde_json::json!({"value": other}),
                };
                let (mut a, mut b) = (Vec::new(), Vec::new());
                canonical_json_bytes(arena.node(*node), &mut a);
                canonical_json_bytes(&value, &mut b);
                assert_eq!(
                    String::from_utf8_lossy(&a),
                    String::from_utf8_lossy(&b),
                    "canonical divergence for {case}"
                );
            }
        }
    }
}
