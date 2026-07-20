//! The TAPE shred path (feature 003 R24, FR-006) — the production default:
//! slab → arena → drained Arrow batches, with NO per-row owned tree.
//!
//! Mirrors `nest::TreeShredder`'s traversal EXACTLY — same breadth-first
//! order, same wrapping, same identity assignment, same child-position rule —
//! but rows are arena node ids borrowing the slab. Everything downstream of the
//! traversal (observation, canonicalization, identity, policy, build) is the
//! SAME generic code both paths share; the equivalence proptest
//! (`tests/shred_equivalence.rs`) pins the whole relation.

use std::collections::VecDeque;

use rdlt_connector::{DestCapabilities, StreamSpec};
use rdlt_core::identity::child_row_id;
use rdlt_core::naming::child_table_name;
use rdlt_core::{ParentLink, RdltError, RowId, TableName};

use super::arena::{Arena, NodeId};
use super::infer::{ColState, ScalarState};
use super::nest::{TableBuffer, content_hash_with, row_identity};
use super::view::JsonView;
use super::{DrainRow, drain_tables};
use crate::load::LoadItem;
use crate::schema::registry::SchemaRegistry;

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

pub(crate) struct TapeShredder {
    spec: StreamSpec,
    caps: DestCapabilities,
    /// Root first, children after, in first-seen order (shared state type with
    /// the tree path).
    tables: Vec<TableBuffer>,
    /// Rollback point for Discard* policy enforcement.
    pre_batch: Vec<Vec<(String, ColState)>>,
}

impl TapeShredder {
    pub(crate) fn new(spec: StreamSpec, caps: DestCapabilities, root_table: TableName) -> Self {
        let mut root = TableBuffer::new(root_table, None, caps.ident_rules);
        // Hints pin root-level scalar columns (they win over inference).
        for (column, ty) in &spec.type_hints {
            *root.state_mut(column) = ColState::Scalar(ScalarState::pinned(*ty));
        }
        Self {
            spec,
            caps,
            tables: vec![root],
            pre_batch: Vec::new(),
        }
    }

    /// Shred one raw push END-TO-END: parse into a slab arena, traverse and
    /// observe, then run the shared drain. One call replaces the tree path's
    /// `push_bytes` + `drain_batch` pair (the arena cannot outlive the call).
    pub(crate) fn push_and_drain(
        &mut self,
        bytes: &[u8],
        registry: &mut SchemaRegistry,
        load_id: &rdlt_core::LoadId,
        mode: &rdlt_core::WriteMode,
        policy: &rdlt_core::SchemaPolicy,
    ) -> Result<Vec<LoadItem>, PushError> {
        // Snapshot observation states: Discard* enforcement rolls offending
        // columns back to exactly this point.
        self.pre_batch = self.tables.iter().map(|t| t.columns.clone()).collect();

        let mut arena = Arena::default();
        let roots = arena.parse_rows(bytes).map_err(PushError::Json)?;

        // Buffered rows per table, index-aligned with `self.tables`.
        let mut rows: Vec<Vec<TapeRow>> = self.tables.iter().map(|_| Vec::new()).collect();

        struct Queued {
            table_idx: usize,
            node: NodeId,
            id: RowId,
            parent_id: Option<RowId>,
            root_id: RowId,
            pos: Option<u64>,
        }
        let lists_as_columns = self.caps.scalar_lists;
        let mut hash_scratch = Vec::new();
        for root in roots {
            let root_id = row_identity(self.spec.primary_key.as_deref(), arena.node(root));
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
                        child_lists
                            .push((key.to_owned(), value.arr_items().map(|n| n.id()).collect()));
                    }
                }

                // Enqueue child rows (position counts null slots, like the tree path).
                for (key, items) in child_lists {
                    let child_idx = self.child_table_idx(entry.table_idx, &key, &mut rows);
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
                        let content = content_hash_with(arena.node(child_node), &mut hash_scratch);
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

                rows[entry.table_idx].push(TapeRow {
                    node: entry.node,
                    id: entry.id,
                    parent_id: entry.parent_id,
                    root_id: if is_root { None } else { Some(entry.root_id) },
                    pos: entry.pos,
                });
            }
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
            &self.pre_batch,
            registry,
            load_id,
            mode,
            policy,
        )
        .map_err(PushError::Engine)
    }

    fn child_table_idx(
        &mut self,
        parent_idx: usize,
        source_key: &str,
        rows: &mut Vec<Vec<TapeRow>>,
    ) -> usize {
        let parent_table = self.tables[parent_idx].table.clone();
        let parent_depth = self.tables[parent_idx]
            .parent
            .as_ref()
            .map_or(0, |p| p.depth);
        let name = child_table_name(parent_table.as_str(), source_key, self.caps.ident_rules);
        let table = TableName::new(name);
        if let Some(idx) = self.tables.iter().position(|t| t.table == table) {
            return idx;
        }
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
}
