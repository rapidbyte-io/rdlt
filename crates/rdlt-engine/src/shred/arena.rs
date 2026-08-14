//! Slab arena: the tape shred path's JSON representation.
//!
//! One arena per pushed slab. Parsing goes through serde_json's OWN parser via a
//! `DeserializeSeed` — every lexical edge case (escapes, surrogates, number
//! grammar) behaves exactly as a `serde_json::Value` parse — but lands in
//! three flat vectors instead of per-row allocated trees: nodes, object
//! entries, array items. Strings and keys borrow from the slab whenever they
//! contain no escapes.
//!
//! One deliberate DIVERGENCE from serde's defaults (047 round 2, owner
//! ruling): nesting is capped at [`rdlt_connector::channel::MAX_ARROW_DEPTH`]
//! rather than serde's 128, so the JSONL front door and every capped walk
//! behind it (shred inference, canonicalization, lowering) agree on ONE
//! bound — data deeper than the cap refuses AT INGEST with a typed parse
//! error, and no deeper structure is ever built.
//!
//! Object entries are stored deduplicated with IndexMap insert semantics
//! (first-occurrence position, last-occurrence value) at parse time, so
//! [`Node`]'s `JsonView` satisfies the view contract with plain slices.

use std::{borrow::Cow, fmt, marker::PhantomData};

use serde::de::{DeserializeSeed, MapAccess, SeqAccess, Visitor};

use super::view::{JsonView, ValueKind};

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
pub(crate) enum ArenaNode<'s> {
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
    nodes: Vec<ArenaNode<'s>>,
    obj_entries: Vec<(Cow<'s, str>, NodeId)>,
    arr_items: Vec<NodeId>,
}

impl<'s> Arena<'s> {
    /// Pre-size for a slab of `bytes` bytes.
    ///
    /// Growing three vectors from empty means repeated reallocate-and-copy as
    /// a slab parses, so a starting capacity is worth having. Picking it is
    /// the delicate part, and the first version of this got the direction
    /// wrong: it divided by the SMALLEST bytes-per-node (4, for `1,` in an
    /// array), which is an UPPER bound on node count, and then claimed to
    /// under-estimate. It over-estimated on everything.
    ///
    /// Typical JSON runs far sparser than its densest possible encoding — an
    /// object entry is `"key":value,` — so this divides by a realistic figure
    /// instead of a floor, and caps hard. Under-shooting costs a doubling or
    /// two; over-shooting costs resident memory on every push, and peak RSS is
    /// one of this feature's recorded metrics. Capacity only, never a
    /// pre-filled length.
    pub(crate) fn sized_for(bytes: usize) -> Self {
        const BYTES_PER_NODE: usize = 32;
        const MAX_PRESIZE: usize = 64 * 1024;
        let nodes = (bytes / BYTES_PER_NODE).min(MAX_PRESIZE);
        Self {
            nodes: Vec::with_capacity(nodes),
            obj_entries: Vec::with_capacity(nodes),
            arr_items: Vec::with_capacity(nodes / 4),
        }
    }

    pub(crate) fn parse_rows(&mut self, bytes: &'s [u8]) -> Result<Vec<NodeId>, serde_json::Error> {
        let mut rows = Vec::new();
        for raw in serde_json::Deserializer::from_slice(bytes)
            .into_iter::<&'s serde_json::value::RawValue>()
        {
            let raw = raw?;
            let mut de = serde_json::Deserializer::from_str(raw.get());
            let node = NodeSeed {
                arena: self,
                depth: 0,
            }
            .deserialize(&mut de)?;
            de.end()?;
            match self.nodes[node as usize] {
                ArenaNode::Arr(start, end) => {
                    for i in start..end {
                        rows.push(self.arr_items[i as usize]);
                    }
                }
                _ => rows.push(node),
            }
        }
        // Wrap AFTER parsing (arena mutation is safe on plain ids).
        for row in &mut rows {
            if !matches!(self.nodes[*row as usize], ArenaNode::Obj(..)) {
                *row = self.wrap_in_value_obj(*row);
            }
        }
        Ok(rows)
    }

    /// `{"value": <node>}` — for bare-scalar/array rows and scalar child items.
    pub(crate) fn wrap_in_value_obj(&mut self, node: NodeId) -> NodeId {
        let start = checked_idx(self.obj_entries.len());
        self.obj_entries.push((Cow::Borrowed("value"), node));
        self.push_node(ArenaNode::Obj(start, start + 1))
    }

    pub(crate) fn node(&self, id: NodeId) -> Node<'_, 's> {
        Node { arena: self, id }
    }

    fn push_node(&mut self, node: ArenaNode<'s>) -> NodeId {
        let id = checked_idx(self.nodes.len());
        self.nodes.push(node);
        id
    }
}

/// A borrowed view of one arena node — the tape path's `JsonView`.
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
    type ObjectIter = ObjectIter<'a, 's>;
    type ArrayIter = ArrayIter<'a, 's>;

    fn kind(self) -> ValueKind<'a> {
        match &self.arena.nodes[self.id as usize] {
            ArenaNode::Null => ValueKind::Null,
            ArenaNode::Bool(b) => ValueKind::Bool(*b),
            ArenaNode::Int(i) => ValueKind::Int(*i),
            ArenaNode::UInt(u) => ValueKind::UInt(*u),
            ArenaNode::Float(f) => ValueKind::Float(*f),
            ArenaNode::Str(s) => ValueKind::Str(s.as_ref()),
            ArenaNode::Obj(..) => ValueKind::Object,
            ArenaNode::Arr(..) => ValueKind::Array,
        }
    }

    fn obj_entries(self) -> Self::ObjectIter {
        let (start, end) = match self.arena.nodes[self.id as usize] {
            ArenaNode::Obj(start, end) => (start, end),
            _ => (0, 0),
        };
        ObjectIter {
            arena: self.arena,
            next: start,
            end,
        }
    }

    fn arr_items(self) -> Self::ArrayIter {
        let (start, end) = match self.arena.nodes[self.id as usize] {
            ArenaNode::Arr(start, end) => (start, end),
            _ => (0, 0),
        };
        ArrayIter {
            arena: self.arena,
            next: start,
            end,
        }
    }

    fn obj_get(self, key: &str) -> Option<Self> {
        let ArenaNode::Obj(start, end) = self.arena.nodes[self.id as usize] else {
            return None;
        };
        self.arena.obj_entries[start as usize..end as usize]
            .iter()
            .find(|(k, _)| k.as_ref() == key)
            .map(|(_, id)| self.arena.node(*id))
    }
}

pub(crate) struct ObjectIter<'a, 's> {
    arena: &'a Arena<'s>,
    next: u32,
    end: u32,
}

impl<'a, 's: 'a> Iterator for ObjectIter<'a, 's> {
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

pub(crate) struct ArrayIter<'a, 's> {
    arena: &'a Arena<'s>,
    next: u32,
    end: u32,
}

impl<'a, 's: 'a> Iterator for ArrayIter<'a, 's> {
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
    /// Nesting level of the value this seed deserializes (root = 0) — the
    /// ingest half of the shared depth cap; see the module doc.
    depth: usize,
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
        deserializer.deserialize_any(NodeVisitor {
            arena: self.arena,
            depth: self.depth,
        })
    }
}

struct NodeVisitor<'a, 's> {
    arena: &'a mut Arena<'s>,
    depth: usize,
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
        Ok(self.arena.push_node(ArenaNode::Null))
    }

    fn visit_bool<E>(self, b: bool) -> Result<NodeId, E> {
        Ok(self.arena.push_node(ArenaNode::Bool(b)))
    }

    fn visit_i64<E>(self, i: i64) -> Result<NodeId, E> {
        Ok(self.arena.push_node(ArenaNode::Int(i)))
    }

    fn visit_u64<E>(self, u: u64) -> Result<NodeId, E> {
        // Mirror serde_json::Number: within i64 range it IS an i64.
        Ok(self.arena.push_node(if u <= i64::MAX as u64 {
            ArenaNode::Int(u as i64)
        } else {
            ArenaNode::UInt(u)
        }))
    }

    fn visit_f64<E>(self, f: f64) -> Result<NodeId, E> {
        Ok(self.arena.push_node(ArenaNode::Float(f)))
    }

    fn visit_borrowed_str<E>(self, s: &'de str) -> Result<NodeId, E> {
        Ok(self.arena.push_node(ArenaNode::Str(Cow::Borrowed(s))))
    }

    fn visit_str<E>(self, s: &str) -> Result<NodeId, E> {
        Ok(self
            .arena
            .push_node(ArenaNode::Str(Cow::Owned(s.to_owned()))))
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<NodeId, A::Error>
    where
        A: SeqAccess<'de>,
    {
        refuse_past_depth_cap::<A::Error>(self.depth)?;
        let mut items: Vec<NodeId> = Vec::new();
        while let Some(id) = seq.next_element_seed(NodeSeed {
            arena: self.arena,
            depth: self.depth + 1,
        })? {
            items.push(id);
        }
        let start = checked_idx(self.arena.arr_items.len());
        self.arena.arr_items.extend_from_slice(&items);
        let end = checked_idx(self.arena.arr_items.len());
        Ok(self.arena.push_node(ArenaNode::Arr(start, end)))
    }

    fn visit_map<A>(self, mut map: A) -> Result<NodeId, A::Error>
    where
        A: MapAccess<'de>,
    {
        refuse_past_depth_cap::<A::Error>(self.depth)?;
        // IndexMap insert semantics AT PARSE TIME: first position, last value.
        let mut entries: Vec<(Cow<'s, str>, NodeId)> = Vec::new();
        while let Some(key) = map.next_key_seed(KeySeed(PhantomData))? {
            let value = map.next_value_seed(NodeSeed {
                arena: self.arena,
                depth: self.depth + 1,
            })?;
            match entries.iter_mut().find(|(k, _)| *k == key) {
                Some((_, slot)) => *slot = value,
                None => entries.push((key, value)),
            }
        }
        let start = checked_idx(self.arena.obj_entries.len());
        self.arena.obj_entries.extend(entries);
        let end = checked_idx(self.arena.obj_entries.len());
        Ok(self.arena.push_node(ArenaNode::Obj(start, end)))
    }
}

/// The ingest depth gate: a container OPENING at `depth` means the document
/// already nests that many levels above it, and past the shared cap the
/// parse refuses instead of building deeper structure. The error rides
/// serde's own channel, so the extract seam classifies it per stream as
/// source data — a data refusal, never an internal error.
fn refuse_past_depth_cap<E: serde::de::Error>(depth: usize) -> Result<(), E> {
    if depth >= rdlt_connector::channel::MAX_ARROW_DEPTH {
        return Err(E::custom(format!(
            "JSON nesting exceeds the {}-level cap — refused at ingest before \
             deeper structure is built",
            rdlt_connector::channel::MAX_ARROW_DEPTH
        )));
    }
    Ok(())
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
        struct KeyVisitor<'s>(PhantomData<&'s ()>);
        impl<'de, 's: 'de> Visitor<'de> for KeyVisitor<'s>
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
            /// The ESCAPED-key path: serde_json decodes escapes into a scratch
            /// buffer and hands it over transiently, so this must copy.
            ///
            /// There is deliberately no `visit_string` override here or on the
            /// value visitor. `from_str`/`from_slice` never produce an owned
            /// `String` — they borrow or use scratch — so an override is
            /// unreachable code, and serde's default already forwards
            /// `visit_string` to this method for any deserializer that does own
            /// its strings. Verified by probe: a `panic!` in the override never
            /// fired across the whole engine suite.
            fn visit_str<E>(self, s: &str) -> Result<Self::Value, E> {
                Ok(Cow::Owned(s.to_owned()))
            }
        }
        deserializer.deserialize_str(KeyVisitor(PhantomData))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shred::canon::canonical_json_bytes;
    use crate::shred::view::JsonView;
    use serde_json::Value;

    /// Independent `serde_json::Value`-based parse of a raw slab (NDJSON /
    /// top-level array / single doc) — the differential oracle the arena
    /// parser is checked against. Lives here, with its only consumer.
    fn oracle_rows(bytes: &[u8]) -> Result<Vec<Value>, serde_json::Error> {
        let mut rows = Vec::new();
        for doc in serde_json::Deserializer::from_slice(bytes).into_iter::<Value>() {
            match doc? {
                Value::Array(items) => rows.extend(items),
                value => rows.push(value),
            }
        }
        Ok(rows)
    }

    /// `sized_for` is a CAPACITY decision, and capacity is the point: the doc
    /// above records that an earlier version divided by the densest possible
    /// encoding and over-estimated on everything, and that over-shooting costs
    /// resident memory on every push — with peak RSS a recorded metric of this
    /// workspace. Nothing about correctness changes if the arithmetic drifts,
    /// which is exactly why no test noticed; the deliberate memory trade is
    /// what needs pinning.
    #[test]
    fn sized_for_presizes_by_the_recorded_formula() {
        // One node per 32 bytes, and arr_items at a quarter of that.
        let arena = Arena::sized_for(32 * 1000);
        assert_eq!(arena.nodes.capacity(), 1000);
        assert_eq!(arena.obj_entries.capacity(), 1000);
        assert_eq!(arena.arr_items.capacity(), 250);

        // The cap is a HARD ceiling: a huge slab must not presize proportionally,
        // or a single large input reserves memory the run never gets back.
        let huge = Arena::sized_for(32 * 1024 * 1024);
        assert_eq!(huge.nodes.capacity(), 64 * 1024, "capped at MAX_PRESIZE");
        assert_eq!(huge.arr_items.capacity(), (64 * 1024) / 4);

        // Small inputs presize small rather than paying the ceiling.
        let small = Arena::sized_for(320);
        assert_eq!(small.nodes.capacity(), 10);
        assert!(
            small.nodes.capacity() < 64 * 1024,
            "a 320-byte slab must not reserve the cap"
        );
    }

    /// The array iterator must ADVANCE. `self.next += 1` is the only thing
    /// moving it, and holding it still yields the first element forever — a
    /// hang rather than a wrong answer, which is why it needs an iterator that
    /// is actually driven to completion.
    #[test]
    fn array_iteration_advances_and_terminates() {
        // Inside an object: `parse_rows` unwraps a TOP-LEVEL array into one row
        // per element and wraps any non-object row in `{"value": …}`, so a bare
        // `[[…]]` would not leave an array node at the row.
        let input = br#"{"xs":[10,20,30]}"#;
        let mut arena = Arena::default();
        let rows = arena.parse_rows(input).expect("parse");
        let (_, xs) = arena
            .node(rows[0])
            .obj_entries()
            .find(|(k, _)| *k == "xs")
            .expect("xs entry");
        // BOUNDED on purpose. The defect this pins is "the cursor never
        // advances", which makes the iterator INFINITE rather than wrong — and
        // an unbounded `collect()` on an infinite iterator exhausts memory and
        // takes the machine down instead of failing the test. (Learned the hard
        // way: the first version of this pin OOM-killed the host.) `take` past
        // the expected length still proves termination, because a correct
        // iterator stops on its own well inside the bound.
        let items: Vec<i64> = xs
            .arr_items()
            .take(16)
            .map(|v| match v.kind() {
                ValueKind::Int(i) => i,
                other => panic!("expected Int, got {other:?}"),
            })
            .collect();
        assert_eq!(
            items,
            vec![10, 20, 30],
            "each element exactly once, in order"
        );
    }

    /// serde hands an OWNED `String` — reaching `visit_string`/`visit_str`
    /// rather than the borrowed path — only when the JSON contains escapes, so
    /// every test using plain strings exercises the borrowed arm alone. These
    /// inputs force the owned arms for both a VALUE and a KEY.
    #[test]
    fn escaped_strings_take_the_owned_visitor_arms() {
        // `\n` and `\t` here are JSON escape SEQUENCES in the raw byte string —
        // two characters each — which is exactly what makes serde produce an
        // owned String instead of borrowing from the slab.
        let input = br#"{"a\nb":"x\ty","plain":"z"}"#;
        let mut arena = Arena::default();
        let rows = arena.parse_rows(input).expect("parse");
        let node = arena.node(rows[0]);
        let entries: Vec<(String, String)> = node
            .obj_entries()
            .map(|(k, v)| {
                let value = match v.kind() {
                    ValueKind::Str(s) => s.to_owned(),
                    other => panic!("expected Str, got {other:?}"),
                };
                (k.to_owned(), value)
            })
            .collect();
        assert_eq!(
            entries,
            vec![
                ("a\nb".to_owned(), "x\ty".to_owned()),
                ("plain".to_owned(), "z".to_owned()),
            ],
            "the escaped key and value must survive the owned visitor arms"
        );
    }

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

    /// 047 round 2 (owner ruling on the 65–128 band): the front door agrees
    /// with the back half — JSON nested deeper than the shared
    /// `MAX_ARROW_DEPTH` refuses AT INGEST with a typed parse error (the
    /// extract seam classifies it per stream as source data, never
    /// internal), so no deeper structure is ever built and every
    /// downstream walk inherits the bound. serde's own 128 limit no
    /// longer decides.
    #[test]
    fn nesting_deeper_than_the_shared_cap_refuses_at_ingest() {
        let nested = |levels: usize| -> Vec<u8> {
            let mut doc = String::new();
            for _ in 0..levels {
                doc.push_str("{\"k\":");
            }
            doc.push('1');
            for _ in 0..levels {
                doc.push('}');
            }
            doc.into_bytes()
        };

        let at_cap = nested(rdlt_connector::channel::MAX_ARROW_DEPTH);
        let mut arena = Arena::default();
        arena
            .parse_rows(&at_cap)
            .expect("nesting AT the cap still parses");

        for levels in [rdlt_connector::channel::MAX_ARROW_DEPTH + 1, 100] {
            let deep = nested(levels);
            let mut arena = Arena::default();
            let err = arena
                .parse_rows(&deep)
                .expect_err("nesting past the cap must refuse at ingest");
            assert!(
                err.to_string().contains("nesting"),
                "the refusal names the nesting: {err}"
            );
        }
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
            let value_rows = oracle_rows(bytes).expect("value parse");
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
