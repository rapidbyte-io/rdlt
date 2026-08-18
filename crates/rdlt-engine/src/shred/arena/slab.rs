//! The flat node store: one arena per pushed slab, three vectors (nodes,
//! object entries, array items) instead of per-row trees, strings and keys
//! borrowing the slab whenever they carry no escapes. [`Node`] is the
//! borrowed [`JsonView`] over it — the JSON path's production view.
//!
//! Object entries are stored deduplicated with IndexMap insert semantics
//! (first-occurrence position, last-occurrence value) at parse time, so
//! [`Node`]'s `JsonView` satisfies the view contract with plain slices.

use std::{borrow::Cow, fmt};

use crate::shred::view::{JsonView, ValueKind};

/// Index of a node within its arena.
pub(crate) type NodeId = u32;

/// Arena indices are u32 by design (half the footprint of usize on the hot
/// vectors). A single document dense enough to overflow them (~4.29e9 nodes,
/// which is ~100 GB of arena before the cast even matters) panics with a clear
/// message instead of silently wrapping into aliased ranges; the engine
/// surfaces test/prod panics from the shred stage as typed task errors.
#[inline]
pub(super) fn checked_idx(len: usize) -> u32 {
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
    pub(super) nodes: Vec<ArenaNode<'s>>,
    pub(super) obj_entries: Vec<(Cow<'s, str>, NodeId)>,
    pub(super) arr_items: Vec<NodeId>,
}

impl<'s> Arena<'s> {
    /// Pre-size for a slab of `bytes` bytes. Growing three vectors from empty
    /// means repeated reallocate-and-copy as a slab parses, so a starting
    /// capacity is worth having — but typical JSON runs far sparser than its
    /// densest possible encoding (an object entry is `"key":value,`), so this
    /// divides by a realistic bytes-per-node rather than the 4-byte floor
    /// (which is an UPPER bound on node count and over-estimates everything),
    /// and caps hard. Under-shooting costs a doubling or two; over-shooting
    /// costs resident memory on every push. Capacity only, never a pre-filled
    /// length.
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

    /// `{"value": <node>}` — for bare-scalar/array rows and scalar child items.
    pub(crate) fn wrap_in_value_obj(&mut self, node: NodeId) -> NodeId {
        let start = checked_idx(self.obj_entries.len());
        self.obj_entries.push((Cow::Borrowed("value"), node));
        self.push_node(ArenaNode::Obj(start, start + 1))
    }

    pub(crate) fn node(&self, id: NodeId) -> Node<'_, 's> {
        Node { arena: self, id }
    }

    pub(super) fn push_node(&mut self, node: ArenaNode<'s>) -> NodeId {
        let id = checked_idx(self.nodes.len());
        self.nodes.push(node);
        id
    }
}

/// A borrowed view of one arena node — the JSON path's `JsonView`.
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

#[cfg(test)]
mod tests {
    use super::*;
    use rdlt_connector::channel::{MAX_JSON_VALUES_PER_PUSH, MAX_RECORD_BATCH_ROWS};

    /// `sized_for` is a CAPACITY decision, and capacity is the point:
    /// over-shooting costs resident memory on every push. Nothing about
    /// correctness changes if the arithmetic drifts, which is exactly why no
    /// behavioral test would notice; the deliberate memory trade is what
    /// needs pinning.
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
        // takes the machine down instead of failing the test. `take` past the
        // expected length still proves termination, because a correct
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
}
