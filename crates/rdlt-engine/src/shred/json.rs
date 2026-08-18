//! The JSON path: slab → arena → resolved Arrow batches, with NO per-row
//! owned tree. A breadth-first traversal observes every field, extracts
//! child tables at any depth and assigns lineage identities — rows are arena
//! node ids borrowing the slab — and everything downstream (observation,
//! canonicalization, identity, policy, build) is the generic `JsonView` core
//! shared with the `&serde_json::Value` test view. The behavioral invariants
//! are pinned by the `shred_property` test binary (its one test is
//! `shred_invariants_hold`) and the hazard cases beside the arena parser and
//! among the crate's integration cases.

use std::collections::{BTreeMap, VecDeque};

use rdlt_connector::channel::MAX_RECORD_BATCH_ROWS;
use rdlt_connector::destination::Capabilities;
use rdlt_connector::source::StreamSpec;
use rdlt_core::error::Error;
use rdlt_core::id::TableName;
use rdlt_core::schema::ParentLink;

use super::arena::{Arena, NodeId, ParseRowsError};
use super::infer::{ColumnState, ScalarState};
use super::limits::{MAX_CHILD_TABLES_PER_PARENT, MAX_TABLES_PER_STREAM};
use super::resolve::{self, ShredContext};
use super::table::{TableBuffer, content_hash_with, row_identity};
use super::view::JsonView;
use crate::identity::{RowId, child_row_id};
use crate::load::LoadItem;
use crate::naming::child_table_name;

/// A shred-path error: invalid JSON from the source (classified per stream at
/// the call site), or an engine error passing through unchanged.
pub(crate) enum PushError {
    Json(serde_json::Error),
    Engine(Error),
}

/// The one refusal both halves of the per-push row bound share: the
/// parse-time root count and the traversal-time total (roots plus the child
/// rows their lists fan out into) spend from the same cap and speak with
/// one voice.
fn row_cap_refusal() -> Error {
    Error::config(format!(
        "JSON push exceeds the {MAX_RECORD_BATCH_ROWS}-row cap across root and child rows — \
         row count is bounded separately from encoded bytes to prevent per-row lineage \
         and load-id amplification"
    ))
}

/// The value-budget refusal: object entries and nested array elements each
/// cost an arena node at parse, so their count is bounded separately from
/// rows — a dense slab refuses typed instead of materializing a ~22× arena
/// before any traversal check could run.
fn value_cap_refusal() -> Error {
    Error::config(format!(
        "JSON push exceeds the {}-value parse budget across object fields and array \
         elements — value count bounds the parse arena separately from row count; split \
         the push into smaller slabs",
        rdlt_connector::channel::MAX_JSON_VALUES_PER_PUSH
    ))
}

/// Spend one row from the per-push budget, refusing typed at exhaustion.
fn take_row(remaining: &mut usize) -> Result<(), Error> {
    if *remaining == 0 {
        return Err(row_cap_refusal());
    }
    *remaining -= 1;
    Ok(())
}

/// One buffered row awaiting the resolve: an arena node id plus its lineage.
struct Row {
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

/// Shreds a stream off the flat slab arena the parse lays every JSON node
/// into — one append-only buffer walked breadth-first, never a per-row owned
/// tree. Observation and resolve read straight off that slab, so no
/// intermediate tree is ever materialized.
pub(crate) struct Shredder {
    spec: StreamSpec,
    capabilities: Capabilities,
    /// Root first, children after, in first-seen order.
    tables: Vec<TableBuffer>,
    /// Normalized table name → index into `tables`, maintained beside the
    /// vector (which stays the resolve-order truth — the map is lookup only).
    /// This is what resolves a per-parent memo MISS: distinct source keys can
    /// normalize to ONE table, so the miss path must go through the
    /// normalized name — but a linear scan of every table there turned a
    /// stream minting many tables into quadratic work per push.
    index_by_name: BTreeMap<TableName, usize>,
}

impl Shredder {
    pub(crate) fn new(
        spec: StreamSpec,
        capabilities: Capabilities,
        root_table: TableName,
    ) -> Result<Self, Error> {
        let mut root = TableBuffer::new(root_table.clone(), None, capabilities.ident_rules);
        // Hints pin root-level scalar columns (they win over inference).
        for (column, ty) in &spec.type_hints {
            *root.state_mut(column)? = ColumnState::Scalar(ScalarState::pinned(*ty));
        }
        Ok(Self {
            spec,
            capabilities,
            tables: vec![root],
            index_by_name: BTreeMap::from([(root_table, 0)]),
        })
    }

    /// Shred one raw push END-TO-END: parse into a slab arena, traverse and
    /// observe, then run the shared resolve (the arena cannot outlive the call).
    pub(crate) fn push_and_resolve(
        &mut self,
        bytes: &[u8],
        ctx: ShredContext,
    ) -> Result<Vec<LoadItem>, PushError> {
        // Rollback snapshots arm LAZILY: each table captures its pre-push
        // column states on this push's FIRST mutation of it, so a push that
        // touches few tables pays no whole-stream clone — an eager snapshot
        // would clone every table's every column state per push, an
        // O(total-state) tax no byte budget sees.
        for table in &mut self.tables {
            table.begin_push();
        }

        let mut arena = Arena::sized_for(bytes.len());
        // The parse enforces both allowances PROGRESSIVELY — the row cap at
        // roots and the value budget at every object entry and nested array
        // element — so a dense slab stops parsing at the cap instead of
        // materializing the whole arena first.
        let roots = arena
            .parse_rows(
                bytes,
                MAX_RECORD_BATCH_ROWS,
                rdlt_connector::channel::MAX_JSON_VALUES_PER_PUSH,
            )
            .map_err(|e| match e {
                ParseRowsError::Json(e) => PushError::Json(e),
                ParseRowsError::RowCap => PushError::Engine(row_cap_refusal()),
                ParseRowsError::ValueCap => PushError::Engine(value_cap_refusal()),
            })?;

        // Buffered rows per table, index-aligned with `self.tables`.
        let mut rows: Vec<Vec<Row>> = self.tables.iter().map(|_| Vec::new()).collect();

        let lists_as_columns = self.capabilities.scalar_lists;
        let mut hash_scratch = Vec::new();
        // The whole-push half of the row cap: EVERY buffered row spends from
        // this — roots and the child rows their lists fan out into — because
        // each one materializes lineage identities and per-table state. Roots
        // alone were bounded at parse; one root carrying millions of children
        // is what this budget refuses.
        let mut row_budget = MAX_RECORD_BATCH_ROWS;
        for root in roots {
            self.shred_root(
                &mut arena,
                root,
                lists_as_columns,
                &mut rows,
                &mut hash_scratch,
                &mut row_budget,
            )
            .map_err(PushError::Engine)?;
        }

        // Lower into the shared resolve representation and run the ONE pipeline.
        let mut resolve_rows: Vec<Vec<resolve::Row<super::arena::Node<'_, '_>>>> = rows
            .iter()
            .map(|table_rows| {
                table_rows
                    .iter()
                    .map(|row| resolve::Row {
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
        resolve::resolve_tables(&mut self.tables, &mut resolve_rows, ctx).map_err(PushError::Engine)
    }

    /// Breadth-first traversal of one root document: observe every field at every
    /// depth into the table buffers, discover child tables, and buffer one
    /// `Row` per node with its lineage identity.
    fn shred_root(
        &mut self,
        arena: &mut Arena,
        root: NodeId,
        lists_as_columns: bool,
        rows: &mut Vec<Vec<Row>>,
        hash_scratch: &mut Vec<u8>,
        row_budget: &mut usize,
    ) -> Result<(), Error> {
        let root_id = row_identity(
            self.spec.primary_key.as_deref(),
            arena.node(root),
            hash_scratch,
        );
        take_row(row_budget)?;
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
                let state =
                    self.tables[entry.table_idx].observe_value(key, value, lists_as_columns)?;
                if state.is_child_table() && value.is_array() {
                    child_lists.push((key.to_owned(), value.arr_items().map(|n| n.id()).collect()));
                }
            }

            self.enqueue_children(
                child_lists,
                &entry,
                arena,
                rows,
                &mut queue,
                hash_scratch,
                row_budget,
            )?;

            rows[entry.table_idx].push(Row {
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
    #[allow(clippy::too_many_arguments)]
    fn enqueue_children(
        &mut self,
        child_lists: Vec<(String, Vec<NodeId>)>,
        entry: &Queued,
        arena: &mut Arena,
        rows: &mut Vec<Vec<Row>>,
        queue: &mut VecDeque<Queued>,
        hash_scratch: &mut Vec<u8>,
        row_budget: &mut usize,
    ) -> Result<(), Error> {
        for (key, items) in child_lists {
            let child_idx = self.child_table_idx(entry.table_idx, &key, rows)?;
            for (i, item) in items.into_iter().enumerate() {
                if arena.node(item).is_null() {
                    continue;
                }
                // Every enqueued child becomes a buffered row with its own
                // lineage identity, so it spends from the same per-push row
                // budget as its root.
                take_row(row_budget)?;
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
        rows: &mut Vec<Vec<Row>>,
    ) -> Result<usize, Error> {
        // Memo hit: this parent has resolved this exact source key before.
        // Every document in a push repeats its keys, so after the first one
        // this replaces a normalized-name construction (which formats, and may
        // hash and truncate) plus the by-name index lookup.
        if let Some(idx) = self.tables[parent_idx]
            .child_tables
            .iter()
            .find(|(key, _)| key == source_key)
            .map(|(_, idx)| *idx)
        {
            return Ok(idx);
        }
        if self.tables[parent_idx].child_tables.len() >= MAX_CHILD_TABLES_PER_PARENT {
            return Err(Error::config(format!(
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
        // On a miss the by-name lookup still runs: distinct source keys can
        // normalize to one table, so "not in the memo" does not mean "not yet
        // created" — but the lookup is the index, never a scan of every table.
        let idx = match self.index_by_name.get(&table) {
            Some(idx) => *idx,
            None => {
                // Total-table bound, refused BEFORE the new buffer exists.
                // The per-parent cap alone cannot bound this: nesting
                // multiplies parents, and every push pays per-table
                // bookkeeping, so an unbounded total turns one crafted frame
                // into unbounded memory and quadratic work.
                if self.tables.len() >= MAX_TABLES_PER_STREAM {
                    return Err(Error::config(format!(
                        "stream `{}` exceeds the {MAX_TABLES_PER_STREAM}-table cap \
                         while observing key {source_key:?} under table `{parent_table}`",
                        self.spec.name
                    )));
                }
                self.tables.push(TableBuffer::new(
                    table.clone(),
                    Some(ParentLink {
                        parent: parent_table,
                        depth: parent_depth + 1,
                    }),
                    self.capabilities.ident_rules,
                ));
                rows.push(Vec::new());
                let idx = self.tables.len() - 1;
                self.index_by_name.insert(table, idx);
                idx
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
    use crate::policy::SchemaPolicy;
    use crate::schema::registry::SchemaRegistry;
    use rdlt_core::commit::WriteMode;
    use rdlt_core::id::LoadId;

    /// Drive one slab through the full push path with a throwaway context.
    fn push(shredder: &mut Shredder, bytes: &[u8]) -> Result<Vec<LoadItem>, PushError> {
        let mut registry = SchemaRegistry::default();
        let (load_id, mode, policy) = (
            LoadId::new("test-load"),
            WriteMode::Append,
            SchemaPolicy::evolve(),
        );
        shredder.push_and_resolve(
            bytes,
            ShredContext {
                registry: &mut registry,
                load_id: &load_id,
                mode: &mode,
                policy: &policy,
                max_batch_cells: super::super::limits::MAX_BATCH_CELLS,
            },
        )
    }

    fn shredder() -> Shredder {
        Shredder::new(
            StreamSpec::new("events"),
            Capabilities::default(),
            TableName::new("events"),
        )
        .expect("shredder")
    }

    fn expect_row_cap(result: Result<Vec<LoadItem>, PushError>) {
        match result {
            Err(PushError::Engine(e)) => assert!(
                e.to_string().contains("row cap"),
                "the refusal names the row cap: {e}"
            ),
            Err(PushError::Json(e)) => {
                panic!("row-cap refusal must be typed, not a parse error: {e}")
            }
            Ok(_) => panic!("a push over the row cap must refuse"),
        }
    }

    /// The idle-table skip contract: a push that leaves a table entirely
    /// untouched emits NOTHING for it — no re-resolve, no phantom delta.
    /// (The skip itself is a CPU optimization, but its observable contract
    /// is "an idle table is inert", which this pins against regressions
    /// that would emit from unchanged state.)
    #[test]
    fn an_idle_push_emits_nothing_for_an_untouched_table() {
        let mut shredder = shredder();
        push(&mut shredder, br#"{"x":0,"a":[{"y":2}]}"#)
            .unwrap_or_else(|_| panic!("the establishing push shreds"));
        assert_eq!(shredder.tables.len(), 2, "root plus the `a` child table");

        // The idle push observes only an already-known root column. (The
        // test harness's per-push registry is fresh, so a root CreateTable
        // delta is expected noise; the contract is that the CHILD table —
        // untouched this push — emits nothing at all.)
        let items =
            push(&mut shredder, br#"{"x":1}"#).unwrap_or_else(|_| panic!("the idle push shreds"));
        assert!(
            items.iter().all(|item| match item {
                LoadItem::Batch { table, .. } => table.as_str() == "events",
                LoadItem::Delta { schema, .. } => schema.table.as_str() == "events",
                _ => true,
            }),
            "the untouched child table emits nothing: {items:?}"
        );
        assert!(
            items.iter().any(|item| matches!(
                item,
                LoadItem::Batch { table, batch, .. } if table.as_str() == "events" && batch.num_rows() == 1
            )),
            "the root's one-row batch is still emitted: {items:?}"
        );
    }

    /// More root documents than the row cap refuse with the typed row-cap
    /// error — and the refusal is progressive: the parse stops at the cap
    /// instead of materializing every remaining root into the arena first.
    #[test]
    fn a_push_with_more_roots_than_the_row_cap_refuses_typed() {
        let mut slab = Vec::with_capacity((MAX_RECORD_BATCH_ROWS + 1) * 2);
        for _ in 0..=MAX_RECORD_BATCH_ROWS {
            slab.extend_from_slice(b"0\n");
        }
        expect_row_cap(push(&mut shredder(), &slab));
    }

    /// One legal root can carry an unbounded child list, and every child
    /// becomes a buffered row with lineage identities — so child rows count
    /// toward the SAME per-push row cap as roots. One root with cap children
    /// is cap+1 rows observed: refused, before any batch is built.
    #[test]
    fn child_rows_count_toward_the_row_cap() {
        let mut slab = Vec::with_capacity(MAX_RECORD_BATCH_ROWS * 3 + 16);
        slab.extend_from_slice(b"{\"a\":[");
        for i in 0..MAX_RECORD_BATCH_ROWS {
            if i > 0 {
                slab.push(b',');
            }
            slab.extend_from_slice(b"{}");
        }
        slab.extend_from_slice(b"]}");
        expect_row_cap(push(&mut shredder(), &slab));
    }

    /// Nested-object fields accumulate in a struct column OUTSIDE the
    /// top-level column bookkeeping, so without counting them a single
    /// column smuggles unbounded breadth past the source-column cap — and
    /// the registry then retains and re-clones it every batch. Struct fields
    /// spend from the same per-table budget as columns.
    #[test]
    fn nested_struct_fields_count_toward_the_source_column_cap() {
        let mut slab = Vec::new();
        slab.extend_from_slice(b"{\"s\":{");
        for index in 0..super::super::limits::MAX_SOURCE_COLUMNS_PER_TABLE {
            if index > 0 {
                slab.push(b',');
            }
            slab.extend_from_slice(format!("\"f{index}\":1").as_bytes());
        }
        slab.extend_from_slice(b"}}");
        match push(&mut shredder(), &slab) {
            Err(PushError::Engine(e)) => assert!(
                e.to_string().contains("source-column cap"),
                "the refusal names the cap: {e}"
            ),
            Err(PushError::Json(e)) => panic!("must refuse typed, not as a parse error: {e}"),
            Ok(_) => panic!("cap struct fields beside one column must refuse"),
        }
    }

    /// The cell budget pinned through the resolve seat (`limits`' own pin
    /// proves the arithmetic; this one proves the seat consults it): a
    /// maximal-width table times enough rows refuses before assembly.
    #[test]
    fn a_wide_table_times_many_rows_refuses_at_the_cell_budget() {
        let mut shredder = shredder();
        // Establish a 4,096-column root table with one wide document.
        let mut wide = String::from("{");
        for index in 0..super::super::limits::MAX_SOURCE_COLUMNS_PER_TABLE {
            if index > 0 {
                wide.push(',');
            }
            wide.push_str(&format!("\"f{index}\":1"));
        }
        wide.push('}');
        push(&mut shredder, wide.as_bytes())
            .unwrap_or_else(|_| panic!("the establishing push shreds"));

        // Rows needed to trip the product at 4,098 columns (4,096 source +
        // 2 root system): floor(budget / width) + 1 is the first refusal.
        let width = super::super::limits::MAX_SOURCE_COLUMNS_PER_TABLE + 2;
        let rows_needed = super::super::limits::MAX_BATCH_CELLS / width + 1;
        let mut slab = Vec::with_capacity(rows_needed * 9);
        for _ in 0..rows_needed {
            slab.extend_from_slice(b"{\"f0\":1}\n");
        }
        match push(&mut shredder, &slab) {
            Err(PushError::Engine(e)) => assert!(
                e.to_string().contains("cell"),
                "the refusal names the cell budget: {e}"
            ),
            Err(PushError::Json(e)) => panic!("must refuse typed, not as a parse error: {e}"),
            Ok(_) => panic!("a columns × rows product past the budget must refuse"),
        }
    }

    /// The per-parent child cap leaves the TOTAL unbounded (nesting multiplies
    /// parents), and every push pays per-table bookkeeping — so the stream
    /// carries a total-table cap of its own. Seeded directly, like the sibling
    /// cap pins: constructing 64Ki tables from JSON would dwarf the assertion.
    #[test]
    fn the_stream_refuses_a_new_table_at_the_total_table_cap() {
        let mut shredder = shredder();
        for index in 1..MAX_TABLES_PER_STREAM {
            shredder.tables.push(TableBuffer::new(
                TableName::new(format!("t{index}")),
                None,
                Capabilities::default().ident_rules,
            ));
        }
        let mut rows: Vec<Vec<Row>> = shredder.tables.iter().map(|_| Vec::new()).collect();
        let error = shredder
            .child_table_idx(0, "one-too-many", &mut rows)
            .expect_err("the total-table cap must refuse");
        assert!(
            error.to_string().contains("table cap"),
            "the refusal names the cap: {error}"
        );
    }

    /// Two DISTINCT source keys can normalize to ONE destination table
    /// (`a-b` and `a b` both become `a_b`), and they must share one buffer —
    /// the invariant that forbids resolving a memo miss by name construction
    /// alone. Guards the by-name index against regressing into per-key tables.
    #[test]
    fn distinct_source_keys_normalizing_alike_share_one_table() {
        let mut shredder = shredder();
        let items = push(&mut shredder, br#"{"a-b":[{"x":1}],"a b":[{"y":2}]}"#)
            .unwrap_or_else(|_| panic!("a small two-key push must shred"));
        assert_eq!(
            shredder.tables.len(),
            2,
            "root plus ONE shared child table, not one per source key"
        );
        let child_batch = items
            .iter()
            .find_map(|item| match item {
                LoadItem::Batch { table, batch, .. } if table.as_str() == "events__a_b" => {
                    Some(batch)
                }
                _ => None,
            })
            .expect("the shared child table emits one batch");
        assert_eq!(child_batch.num_rows(), 2, "both keys' rows land in it");
    }

    #[test]
    fn a_parent_refuses_a_new_child_key_at_the_child_table_cap() {
        let mut shredder = shredder();
        shredder.tables[0].child_tables = (0..MAX_CHILD_TABLES_PER_PARENT)
            .map(|index| (format!("child-{index}"), 0))
            .collect();
        let error = shredder
            .child_table_idx(0, "one-too-many", &mut vec![Vec::new()])
            .expect_err("the child-table cap must refuse");
        assert!(error.to_string().contains("child-table cap"));
    }
}
