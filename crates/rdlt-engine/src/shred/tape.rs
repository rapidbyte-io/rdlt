//! The TAPE shred path — the production default:
//! slab → arena → drained Arrow batches, with NO per-row owned tree.
//!
//! Breadth-first traversal: observe every field,
//! extract child tables at any depth, assign lineage identities — rows are
//! arena node ids borrowing the slab. Everything downstream (observation,
//! canonicalization, identity, policy, build) is the generic `JsonView` core;
//! the behavioral invariants are pinned by the `shred_property` test BINARY (whose
//! one test is `shred_invariants_hold` — a distinction that mattered: the gate
//! selected it by test name for a while, matched nothing, and reported success)
//! and the
//! hazard cases in `arena.rs`/`tests/passthrough.rs`.

use std::collections::VecDeque;

use rdlt_connector::{DestinationCapabilities, StreamSpec};
use rdlt_core::identity::child_row_id;
use rdlt_core::naming::child_table_name;
use rdlt_core::{ParentLink, RdltError, RowId, TableName};

use super::arena::{Arena, NodeId};
use super::infer::{ColumnState, ScalarState};
use super::table::{TableBuffer, content_hash_with, row_identity};
use super::view::JsonView;
use super::{DrainRow, ShredContext, drain_tables};
use crate::load::LoadItem;

/// A shred-path error: JSON errors format at the call site exactly like the
/// tree path's `push_bytes` errors do.
pub(crate) enum PushError {
    Json(serde_json::Error),
    Engine(RdltError),
}

/// One buffered row awaiting the drain (tape path: arena node id).
struct TapeRow {
    node: NodeId,
    id: RowId,
    parent_id: Option<RowId>,
    root_id: Option<RowId>,
    pos: Option<u64>,
}

/// A node queued for breadth-first traversal, carrying the lineage its row will
/// inherit. `root_id` is always present here (the row build decides whether the
/// root's own row records it).
struct Queued {
    table_idx: usize,
    node: NodeId,
    id: RowId,
    parent_id: Option<RowId>,
    root_id: RowId,
    pos: Option<u64>,
}

/// Shreds a stream off the *tape*: the flat slab arena the parse lays every JSON
/// node into — one append-only buffer walked breadth-first, never a per-row owned
/// tree. Observation and drain read straight off that slab, so no intermediate
/// tree is ever materialized.
pub(crate) struct TapeShredder {
    spec: StreamSpec,
    caps: DestinationCapabilities,
    /// Root first, children after, in first-seen order (shared state type with
    /// the tree path).
    tables: Vec<TableBuffer>,
    /// Rollback point for Discard* policy enforcement.
    rollback_snapshot: Vec<Vec<(String, ColumnState)>>,
}

impl TapeShredder {
    pub(crate) fn new(
        spec: StreamSpec,
        caps: DestinationCapabilities,
        root_table: TableName,
    ) -> Self {
        let mut root = TableBuffer::new(root_table, None, caps.ident_rules);
        // Hints pin root-level scalar columns (they win over inference).
        for (column, ty) in &spec.type_hints {
            *root.state_mut(column) = ColumnState::Scalar(ScalarState::pinned(*ty));
        }
        Self {
            spec,
            caps,
            tables: vec![root],
            rollback_snapshot: Vec::new(),
        }
    }

    /// Shred one raw push END-TO-END: parse into a slab arena, traverse and
    /// observe, then run the shared drain. One call replaces the tree path's
    /// `push_bytes` + `drain_batch` pair (the arena cannot outlive the call).
    pub(crate) fn push_and_drain(
        &mut self,
        bytes: &[u8],
        ctx: ShredContext,
    ) -> Result<Vec<LoadItem>, PushError> {
        // Snapshot observation states: Discard* enforcement rolls offending
        // columns back to exactly this point.
        self.rollback_snapshot = self.tables.iter().map(|t| t.columns.clone()).collect();

        let mut arena = Arena::sized_for(bytes.len());
        let roots = arena.parse_rows(bytes).map_err(PushError::Json)?;

        // Buffered rows per table, index-aligned with `self.tables`.
        let mut rows: Vec<Vec<TapeRow>> = self.tables.iter().map(|_| Vec::new()).collect();

        let lists_as_columns = self.caps.scalar_lists;
        let mut hash_scratch = Vec::new();
        for root in roots {
            self.shred_root(
                &mut arena,
                root,
                lists_as_columns,
                &mut rows,
                &mut hash_scratch,
            );
        }

        // Lower into the shared drain representation and run the ONE pipeline.
        let mut drain_rows: Vec<Vec<DrainRow<super::arena::Node<'_, '_>>>> = rows
            .iter()
            .map(|table_rows| {
                table_rows
                    .iter()
                    .map(|row| DrainRow {
                        value: arena.node(row.node),
                        id: row.id,
                        parent_id: row.parent_id,
                        root_id: row.root_id,
                        pos: row.pos,
                        nulled: Vec::new(),
                    })
                    .collect()
            })
            .collect();
        drain_tables(
            &mut self.tables,
            &mut drain_rows,
            &self.rollback_snapshot,
            ctx.registry,
            ctx.load_id,
            ctx.mode,
            ctx.policy,
        )
        .map_err(PushError::Engine)
    }

    /// Breadth-first traversal of one root document: observe every field at every
    /// depth into the table buffers, discover child tables, and buffer one
    /// `TapeRow` per node with its lineage identity.
    fn shred_root(
        &mut self,
        arena: &mut Arena,
        root: NodeId,
        lists_as_columns: bool,
        rows: &mut Vec<Vec<TapeRow>>,
        hash_scratch: &mut Vec<u8>,
    ) {
        let root_id = row_identity(
            self.spec.primary_key.as_deref(),
            arena.node(root),
            hash_scratch,
        );
        let mut queue: VecDeque<Queued> = VecDeque::new();
        queue.push_back(Queued {
            table_idx: 0,
            node: root,
            id: root_id,
            parent_id: None,
            root_id,
            pos: None,
        });

        while let Some(entry) = queue.pop_front() {
            let is_root = entry.table_idx == 0;

            // Observe every field; discover child tables. Keys borrow the
            // arena; table state is disjoint, so both borrows coexist.
            let mut child_lists: Vec<(String, Vec<NodeId>)> = Vec::new();
            for (key, value) in arena.node(entry.node).obj_entries() {
                let state = self.tables[entry.table_idx].state_mut(key);
                state.observe(value, lists_as_columns);
                if state.is_child_table() && value.is_array() {
                    child_lists.push((key.to_owned(), value.arr_items().map(|n| n.id()).collect()));
                }
            }

            self.enqueue_children(child_lists, &entry, arena, rows, &mut queue, hash_scratch);

            rows[entry.table_idx].push(TapeRow {
                node: entry.node,
                id: entry.id,
                parent_id: entry.parent_id,
                root_id: if is_root { None } else { Some(entry.root_id) },
                pos: entry.pos,
            });
        }
    }

    /// Enqueue the child rows discovered under one node: each non-null list item
    /// becomes a queued row in its child table (scalar items wrapped as
    /// `{"value": …}`), position counting null slots like the tree path.
    fn enqueue_children(
        &mut self,
        child_lists: Vec<(String, Vec<NodeId>)>,
        entry: &Queued,
        arena: &mut Arena,
        rows: &mut Vec<Vec<TapeRow>>,
        queue: &mut VecDeque<Queued>,
        hash_scratch: &mut Vec<u8>,
    ) {
        for (key, items) in child_lists {
            let child_idx = self.child_table_idx(entry.table_idx, &key, rows);
            for (i, item) in items.into_iter().enumerate() {
                if arena.node(item).is_null() {
                    continue;
                }
                // Scalar list items in a child table become {"value": …} rows.
                let child_node = if arena.node(item).is_object() {
                    item
                } else {
                    arena.wrap_in_value_obj(item)
                };
                let content = content_hash_with(arena.node(child_node), hash_scratch);
                let child_id = child_row_id(&entry.id, i as u64, &content);
                queue.push_back(Queued {
                    table_idx: child_idx,
                    node: child_node,
                    id: child_id,
                    parent_id: Some(entry.id),
                    root_id: entry.root_id,
                    pos: Some(i as u64),
                });
            }
        }
    }

    fn child_table_idx(
        &mut self,
        parent_idx: usize,
        source_key: &str,
        rows: &mut Vec<Vec<TapeRow>>,
    ) -> usize {
        // Memo hit: this parent has resolved this exact source key before.
        // Every document in a push repeats its keys, so after the first one
        // this replaces a normalized-name construction (which formats, and may
        // hash and truncate) plus a linear scan of every known table.
        if let Some(idx) = self.tables[parent_idx]
            .child_tables
            .iter()
            .find(|(key, _)| key == source_key)
            .map(|(_, idx)| *idx)
        {
            return idx;
        }
        let parent_table = self.tables[parent_idx].table.clone();
        let parent_depth = self.tables[parent_idx]
            .parent
            .as_ref()
            .map_or(0, |p| p.depth);
        let name = child_table_name(parent_table.as_str(), source_key, self.caps.ident_rules);
        let table = TableName::new(name);
        // On a miss the scan still runs: distinct source keys can normalize to
        // one table, so "not in the memo" does not mean "not yet created".
        let idx = match self.tables.iter().position(|t| t.table == table) {
            Some(idx) => idx,
            None => {
                self.tables.push(TableBuffer::new(
                    table,
                    Some(ParentLink {
                        parent: parent_table,
                        depth: parent_depth + 1,
                    }),
                    self.caps.ident_rules,
                ));
                rows.push(Vec::new());
                self.tables.len() - 1
            }
        };
        self.tables[parent_idx]
            .child_tables
            .push((source_key.to_owned(), idx));
        idx
    }
}
