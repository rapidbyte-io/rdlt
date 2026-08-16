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

/// Why a slab refused to parse into rows.
#[derive(Debug)]
pub(crate) enum ParseRowsError {
    /// Invalid JSON — classified per stream at the call site as source data.
    Json(serde_json::Error),
    /// More rows than the caller's cap. Raised progressively, at the row that
    /// exceeds the cap, so the caller renders its own typed refusal instead
    /// of a parse error.
    RowCap,
    /// More values (object entries + nested array elements) than the
    /// caller's parse budget (5M5): the arena's memory tracks values, not
    /// rows, so value count carries its own progressive bound.
    ValueCap,
}

/// One remaining-allowance counter, with the flag that lets the caller tell
/// a cap refusal apart from a serde error after the fact (inside a visitor
/// only serde's error type can travel).
#[derive(Default)]
struct ParseBudget {
    remaining: usize,
    exhausted: bool,
}

impl ParseBudget {
    /// Spend one unit, or mark the budget exhausted and refuse.
    fn take_one(&mut self) -> Result<(), ()> {
        if self.remaining == 0 {
            self.exhausted = true;
            return Err(());
        }
        self.remaining -= 1;
        Ok(())
    }
}

/// The two allowances one `parse_rows` call spends (5M4/5M5): ROWS (roots —
/// documents, or elements of a top-level array) bound lineage and per-row
/// output; VALUES (object entries and nested array elements, at any depth)
/// bound the arena itself — each costs its node at parse, whether or not it
/// ever becomes a row. The split is what lets an honest list-carrying push
/// pass (its elements spend values, not rows) while a dense frame still
/// refuses before materializing a ~22× arena.
#[derive(Default)]
struct ParseBudgets {
    rows: ParseBudget,
    values: ParseBudget,
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

    /// Parse a slab into row nodes, refusing PROGRESSIVELY past `max_rows`
    /// and `max_values`.
    ///
    /// The bounds are enforced where spending is first observed: ROWS per
    /// document and per top-level array element (each becomes a row);
    /// VALUES per object entry and per nested array element — each costs
    /// its arena node at parse whether or not it becomes a row, so a slab
    /// dense enough to carry tens of millions of values in a legal frame
    /// (~2 wire bytes per node against ~24 arena bytes for an array
    /// element, ~56 for an object entry — its node plus its `(key, id)`
    /// tuple on the pinned layout — with the per-object parse transients
    /// doubling the peak) would otherwise materialize a ~12-16× arena
    /// before any post-parse check could refuse (4H3 arrays; 5M5
    /// objects).
    pub(crate) fn parse_rows(
        &mut self,
        bytes: &'s [u8],
        max_rows: usize,
        max_values: usize,
    ) -> Result<Vec<NodeId>, ParseRowsError> {
        let mut rows = Vec::new();
        let mut budgets = ParseBudgets {
            rows: ParseBudget {
                remaining: max_rows,
                exhausted: false,
            },
            values: ParseBudget {
                remaining: max_values,
                exhausted: false,
            },
        };
        for raw in serde_json::Deserializer::from_slice(bytes)
            .into_iter::<&'s serde_json::value::RawValue>()
        {
            let raw = raw.map_err(ParseRowsError::Json)?;
            let mut de = serde_json::Deserializer::from_str(raw.get());
            let parsed = NodeSeed {
                arena: self,
                depth: 0,
                budgets: &mut budgets,
            }
            .deserialize(&mut de);
            // The budgets ride serde's error channel out of the visitor
            // loops (a visitor can return no other type), so the flags are
            // what distinguish "over a cap" from genuinely invalid JSON.
            let node = parsed.map_err(|e| {
                if budgets.rows.exhausted {
                    ParseRowsError::RowCap
                } else if budgets.values.exhausted {
                    ParseRowsError::ValueCap
                } else {
                    ParseRowsError::Json(e)
                }
            })?;
            de.end().map_err(ParseRowsError::Json)?;
            match self.nodes[node as usize] {
                ArenaNode::Arr(start, end) => {
                    // Elements were already counted as they parsed (the
                    // depth-0 seed spends ROWS in `visit_seq`).
                    for i in start..end {
                        rows.push(self.arr_items[i as usize]);
                    }
                }
                _ => {
                    if budgets.rows.take_one().is_err() {
                        return Err(ParseRowsError::RowCap);
                    }
                    rows.push(node);
                }
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
    /// Both per-push allowances, carried at every depth (see
    /// [`ParseBudgets`]): which one a visitor spends depends on the
    /// container — a top-level array's elements are rows; everything
    /// nested, and every object entry, is values.
    budgets: &'a mut ParseBudgets,
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
            budgets: self.budgets,
        })
    }
}

struct NodeVisitor<'a, 's> {
    arena: &'a mut Arena<'s>,
    depth: usize,
    budgets: &'a mut ParseBudgets,
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
            budgets: self.budgets,
        })? {
            // Which allowance an element spends depends on WHERE the array
            // sits (5M4): at depth 0 each element becomes a ROW (the
            // top-level-array form), so it spends the row allowance;
            // nested elements become child-table rows or list cells —
            // both materialize their arena node at parse, so they spend
            // the value allowance (the arena's own bound), never the
            // rows'. Either way the spend happens AS the element parses,
            // before the rest of a dense slab becomes arena nodes.
            let unit = if self.depth == 0 {
                &mut self.budgets.rows
            } else {
                &mut self.budgets.values
            };
            if unit.take_one().is_err() {
                return Err(serde::de::Error::custom(if self.depth == 0 {
                    "top-level array exceeds the caller's row cap"
                } else {
                    "array elements exceed the caller's per-push value budget"
                }));
            }
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
        //
        // The duplicate scan is O(1) amortized past a 16-entry linear
        // prelude (5H2): a flat mega-object's `Vec::find` per entry was
        // N²/2 string comparisons of pure CPU inside `spawn_blocking` —
        // uninterruptible for the duration, and hours of it from one
        // 64 MiB frame. Small objects stay linear (cheaper than hashing);
        // the index is built lazily the moment an object outgrows the
        // prelude.
        let mut entries: Vec<(Cow<'s, str>, NodeId)> = Vec::new();
        let mut index: Option<std::collections::HashMap<Cow<'s, str>, usize>> = None;
        while let Some(key) = map.next_key_seed(KeySeed(PhantomData))? {
            // Every entry spends from the value budget AS IT PARSES (5M5):
            // object entries are arena nodes the row budget never saw.
            if self.budgets.values.take_one().is_err() {
                return Err(serde::de::Error::custom(
                    "object entries exceed the caller's per-push value budget",
                ));
            }
            let value = map.next_value_seed(NodeSeed {
                arena: self.arena,
                depth: self.depth + 1,
                budgets: self.budgets,
            })?;
            let existing = match &index {
                Some(index) => index.get(key.as_ref()).copied(),
                None => entries.iter().position(|(k, _)| *k == key),
            };
            match existing {
                Some(i) => entries[i].1 = value,
                None => {
                    if index.is_none() && entries.len() >= 16 {
                        let mut built = std::collections::HashMap::with_capacity(entries.len() * 2);
                        for (i, (k, _)) in entries.iter().enumerate() {
                            built.insert(k.clone(), i);
                        }
                        index = Some(built);
                    }
                    if let Some(index) = &mut index {
                        index.insert(key.clone(), entries.len());
                    }
                    entries.push((key, value));
                }
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
    use rdlt_connector::channel::{MAX_JSON_VALUES_PER_PUSH, MAX_RECORD_BATCH_ROWS};
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
        let rows = arena
            .parse_rows(input, MAX_RECORD_BATCH_ROWS, MAX_JSON_VALUES_PER_PUSH)
            .expect("parse");
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
        let rows = arena
            .parse_rows(input, MAX_RECORD_BATCH_ROWS, MAX_JSON_VALUES_PER_PUSH)
            .expect("parse");
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
        let rows = arena
            .parse_rows(input, MAX_RECORD_BATCH_ROWS, MAX_JSON_VALUES_PER_PUSH)
            .expect("parse");
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
            .parse_rows(&at_cap, MAX_RECORD_BATCH_ROWS, MAX_JSON_VALUES_PER_PUSH)
            .expect("nesting AT the cap still parses");

        for levels in [rdlt_connector::channel::MAX_ARROW_DEPTH + 1, 100] {
            let deep = nested(levels);
            let mut arena = Arena::default();
            let err = arena
                .parse_rows(&deep, MAX_RECORD_BATCH_ROWS, MAX_JSON_VALUES_PER_PUSH)
                .expect_err("nesting past the cap must refuse at ingest");
            let ParseRowsError::Json(err) = err else {
                panic!("a depth refusal is a parse error, not a row-cap one");
            };
            assert!(
                err.to_string().contains("nesting"),
                "the refusal names the nesting: {err}"
            );
        }
    }

    /// 4H3's array half, under the wave-6 split (5M4): NESTED array
    /// elements spend the VALUE budget at parse time — never the rows' —
    /// so a dense nested list refuses with the arena still small, while
    /// the row cap is not charged for values that may become list cells
    /// rather than rows.
    #[test]
    fn nested_array_elements_count_against_the_value_budget_at_parse_time() {
        let mut slab = Vec::new();
        slab.extend_from_slice(b"[[");
        for i in 0..8 {
            if i > 0 {
                slab.push(b',');
            }
            slab.push(b'0');
        }
        slab.extend_from_slice(b"]]");
        let mut arena = Arena::default();
        let err = arena
            .parse_rows(&slab, usize::MAX, 3)
            .expect_err("nested elements past the value budget must refuse");
        assert!(
            matches!(err, ParseRowsError::ValueCap),
            "the typed value-budget arm, not a parse error or the row cap: {err:?}"
        );
        assert!(
            arena.nodes.len() <= 6,
            "the parse stopped at the cap instead of materializing the list: {} nodes",
            arena.nodes.len()
        );

        // …and the row budget is untouched by nested elements: the same
        // slab with a tiny ROW cap and value headroom parses — its ONE
        // inner array becomes exactly one row, its eight nested elements
        // charging nothing to the rows' allowance.
        let mut arena = Arena::default();
        let rows = arena
            .parse_rows(&slab, 2, usize::MAX)
            .expect("the outer array's single element is one row");
        assert_eq!(rows.len(), 1);
    }

    /// 5M5: object entries spend the value budget too — the object-only
    /// slab was the hole: a flat mega-object parsed its whole arena with
    /// the row budget seeing a single document.
    #[test]
    fn object_entries_count_against_the_value_budget_at_parse_time() {
        let mut slab = Vec::new();
        slab.extend_from_slice(b"{");
        for i in 0..8 {
            if i > 0 {
                slab.push(b',');
            }
            slab.extend_from_slice(format!("\"k{i}\":0").as_bytes());
        }
        slab.extend_from_slice(b"}");
        let mut arena = Arena::default();
        let err = arena
            .parse_rows(&slab, usize::MAX, 3)
            .expect_err("a flat object past the value budget must refuse");
        assert!(
            matches!(err, ParseRowsError::ValueCap),
            "the value-budget arm: {err:?}"
        );
        assert!(
            arena.nodes.len() <= 6,
            "the parse stopped at the budget: {} nodes",
            arena.nodes.len()
        );
    }

    /// 5H2's CPU half: the duplicate-key scan is O(N) past a 16-entry
    /// linear prelude. A 200K-entry object parses AND dedups correctly —
    /// without the index this test is ~2×10¹⁰ string comparisons.
    #[test]
    fn a_wide_object_dedups_in_linear_time_with_first_position_last_value() {
        let mut doc = String::from("{");
        for i in 0..200_000 {
            if i > 0 {
                doc.push(',');
            }
            doc.push_str(&format!("\"k{i}\":{i}"));
        }
        // Duplicates on BOTH sides of the prelude: first position kept,
        // last value wins.
        doc.push_str(",\"k0\":-1,\"k199999\":-2}");
        let mut arena = Arena::default();
        let rows = arena
            .parse_rows(doc.as_bytes(), usize::MAX, usize::MAX)
            .expect("the wide object parses");
        let node = arena.node(rows[0]);
        let entries: Vec<_> = node.obj_entries().collect();
        assert_eq!(entries.len(), 200_000, "duplicates collapse");
        let first = entries[0].1.kind();
        assert!(
            matches!(first, ValueKind::Int(-1)),
            "first position, last value: {first:?}"
        );
        let last = entries[entries.len() - 1].1.kind();
        assert!(matches!(last, ValueKind::Int(-2)), "{last:?}");
    }

    /// The row bound is PROGRESSIVE at both row-observation sites: per
    /// document for a stream of documents, per element inside a top-level
    /// array. A small cap makes both directions cheap to prove; production
    /// passes the real `MAX_RECORD_BATCH_ROWS`, and the tape suite trips
    /// that constant with full-size slabs.
    #[test]
    fn parse_refuses_progressively_past_the_row_cap() {
        // At the cap: fine, from either shape.
        for slab in [b"1\n2\n3".as_slice(), b"[1,2,3]".as_slice()] {
            let mut arena = Arena::default();
            let rows = arena
                .parse_rows(slab, 3, usize::MAX)
                .expect("at the cap parses");
            assert_eq!(rows.len(), 3);
        }
        // One over: the typed row-cap refusal, not a parse error — and the
        // arena stops near the cap instead of holding every remaining row
        // (the count is what proves the check ran DURING the parse).
        for slab in [
            b"1\n2\n3\n4\n5\n6\n7\n8".as_slice(),
            b"[1,2,3,4,5,6,7,8]".as_slice(),
        ] {
            let mut arena = Arena::default();
            let err = arena
                .parse_rows(slab, 3, usize::MAX)
                .expect_err("one row past the cap must refuse");
            assert!(
                matches!(err, ParseRowsError::RowCap),
                "expected the row-cap arm, got a parse error"
            );
            assert!(
                arena.nodes.len() <= 5,
                "the parse must stop at the cap, not materialize the slab: {} nodes",
                arena.nodes.len()
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
            let arena_rows = arena
                .parse_rows(bytes, MAX_RECORD_BATCH_ROWS, MAX_JSON_VALUES_PER_PUSH)
                .expect("arena parse");
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
