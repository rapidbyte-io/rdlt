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
//! hazard cases in `arena.rs`/`tests/cases/test_passthrough.rs`.

use std::collections::VecDeque;

use rdlt_connector::{DestinationCapabilities, StreamSpec, channel::MAX_RECORD_BATCH_ROWS};
use rdlt_core::{
    ParentLink, RdltError, RowId, TableName, identity::child_row_id, naming::child_table_name,
};

use super::{
    DrainRow, MAX_CHILD_TABLES_PER_PARENT, ShredContext,
    arena::{Arena, NodeId},
    drain_tables,
    infer::{ColumnState, ScalarState},
    table::{TableBuffer, content_hash_with, row_identity},
    view::JsonView,
};
use crate::load::LoadItem;

/// A shred-path error: invalid JSON from the source (classified per stream at
/// the call site), or an engine error passing through unchanged.
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
    capabilities: DestinationCapabilities,
    /// Root first, children after, in first-seen order.
    tables: Vec<TableBuffer>,
}

impl TapeShredder {
    pub(crate) fn new(
        spec: StreamSpec,
        capabilities: DestinationCapabilities,
        root_table: TableName,
    ) -> Result<Self, RdltError> {
        let mut root = TableBuffer::new(root_table, None, capabilities.ident_rules);
        // Hints pin root-level scalar columns (they win over inference).
        for (column, ty) in &spec.type_hints {
            *root.state_mut(column)? = ColumnState::Scalar(ScalarState::pinned(*ty));
        }
        Ok(Self {
            spec,
            capabilities,
            tables: vec![root],
        })
    }

    /// Shred one raw push END-TO-END: parse into a slab arena, traverse and
    /// observe, then run the shared drain (the arena cannot outlive the call).
    pub(crate) fn push_and_drain(
        &mut self,
        bytes: &[u8],
        ctx: ShredContext,
    ) -> Result<Vec<LoadItem>, PushError> {
        // Snapshot observation states: Discard* enforcement rolls offending
        // columns back to exactly this point.
        let rollback_snapshot: Vec<Vec<(String, ColumnState)>> =
            self.tables.iter().map(|t| t.columns.clone()).collect();

        let mut arena = Arena::sized_for(bytes.len());
        let roots = arena.parse_rows(bytes).map_err(PushError::Json)?;
        if roots.len() > MAX_RECORD_BATCH_ROWS {
            return Err(PushError::Engine(RdltError::config(format!(
                "JSON push carries {} rows, over the {MAX_RECORD_BATCH_ROWS}-row cap — \
                 row count is bounded separately from encoded bytes to prevent per-row \
                 lineage and load-id amplification",
                roots.len()
            ))));
        }

        // Buffered rows per table, index-aligned with `self.tables`.
        let mut rows: Vec<Vec<TapeRow>> = self.tables.iter().map(|_| Vec::new()).collect();

        let lists_as_columns = self.capabilities.scalar_lists;
        let mut hash_scratch = Vec::new();
        for root in roots {
            self.shred_root(
                &mut arena,
                root,
                lists_as_columns,
                &mut rows,
                &mut hash_scratch,
            )
            .map_err(PushError::Engine)?;
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
        drain_tables(&mut self.tables, &mut drain_rows, &rollback_snapshot, ctx)
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
    ) -> Result<(), RdltError> {
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
                let state = self.tables[entry.table_idx].state_mut(key)?;
                state.observe(value, lists_as_columns);
                if state.is_child_table() && value.is_array() {
                    child_lists.push((key.to_owned(), value.arr_items().map(|n| n.id()).collect()));
                }
            }

            self.enqueue_children(child_lists, &entry, arena, rows, &mut queue, hash_scratch)?;

            rows[entry.table_idx].push(TapeRow {
                node: entry.node,
                id: entry.id,
                parent_id: entry.parent_id,
                root_id: if is_root { None } else { Some(entry.root_id) },
                pos: entry.pos,
            });
        }
        Ok(())
    }

    /// Enqueue the child rows discovered under one node: each non-null list item
    /// becomes a queued row in its child table (scalar items wrapped as
    /// `{"value": …}`), position counting null slots.
    fn enqueue_children(
        &mut self,
        child_lists: Vec<(String, Vec<NodeId>)>,
        entry: &Queued,
        arena: &mut Arena,
        rows: &mut Vec<Vec<TapeRow>>,
        queue: &mut VecDeque<Queued>,
        hash_scratch: &mut Vec<u8>,
    ) -> Result<(), RdltError> {
        for (key, items) in child_lists {
            let child_idx = self.child_table_idx(entry.table_idx, &key, rows)?;
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
        Ok(())
    }

    fn child_table_idx(
        &mut self,
        parent_idx: usize,
        source_key: &str,
        rows: &mut Vec<Vec<TapeRow>>,
    ) -> Result<usize, RdltError> {
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
            return Ok(idx);
        }
        if self.tables[parent_idx].child_tables.len() >= MAX_CHILD_TABLES_PER_PARENT {
            return Err(RdltError::config(format!(
                "table `{}` exceeds the {MAX_CHILD_TABLES_PER_PARENT}-child-table cap \
                 while observing key {source_key:?}",
                self.tables[parent_idx].table
            )));
        }
        let parent_table = self.tables[parent_idx].table.clone();
        let parent_depth = self.tables[parent_idx]
            .parent
            .as_ref()
            .map_or(0, |p| p.depth);
        let name = child_table_name(
            parent_table.as_str(),
            source_key,
            self.capabilities.ident_rules,
        );
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
                    self.capabilities.ident_rules,
                ));
                rows.push(Vec::new());
                self.tables.len() - 1
            }
        };
        self.tables[parent_idx]
            .child_tables
            .push((source_key.to_owned(), idx));
        Ok(idx)
    }
}

#[cfg(test)]
mod cardinality_tests {
    use super::*;

    #[test]
    fn a_parent_refuses_a_new_child_key_at_the_child_table_cap() {
        let mut shredder = TapeShredder::new(
            StreamSpec::new("events"),
            DestinationCapabilities::default(),
            TableName::new("events"),
        )
        .expect("shredder");
        shredder.tables[0].child_tables = (0..MAX_CHILD_TABLES_PER_PARENT)
            .map(|index| (format!("child-{index}"), 0))
            .collect();
        let error = shredder
            .child_table_idx(0, "one-too-many", &mut vec![Vec::new()])
            .expect_err("the child-table cap must refuse");
        assert!(error.to_string().contains("child-table cap"));
    }
}
