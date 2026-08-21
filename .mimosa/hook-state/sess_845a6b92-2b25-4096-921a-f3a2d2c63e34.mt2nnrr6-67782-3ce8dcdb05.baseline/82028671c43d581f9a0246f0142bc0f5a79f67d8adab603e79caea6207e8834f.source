//! Shared shredding state: per-table naming, shape observation, lineage
//! identity, and schema resolution — everything the JSON traversal and the
//! resolve pipeline build on.

use std::collections::{BTreeMap, BTreeSet};

use rdlt_core::id::TableName;
use rdlt_core::schema::{self, Column, IdentRules, ParentLink, Provenance, TableSchema};

use super::canonical::{canonical_json_bytes, render_scalar};
use super::infer::ColumnState;
use super::limits::MAX_SOURCE_COLUMNS_PER_TABLE;
use super::view::JsonView;
use crate::identity::{FieldValue, RowId, RowIdBuilder};
use crate::naming::UniqueNamer;

/// One table's persistent shredding state: naming, shape observation, lineage —
/// everything EXCEPT the buffered rows (those are per-batch and path-specific).
#[derive(Debug)]
pub(crate) struct TableBuffer {
    pub(crate) table: TableName,
    pub(crate) parent: Option<ParentLink>,
    /// Column observation states in first-seen order; source key → state.
    pub(crate) columns: Vec<(String, ColumnState)>,
    /// The key→slot lookup beside `columns` ([`super::slots::SlotIndex`]):
    /// observation resolves a column per key per ROW, so a linear find
    /// priced a wide table's push quadratically. Kept coherent by
    /// `column_index` (append) and `revert_column` (rebuild after
    /// removal); deliberately NOT in the pre-push snapshot — the
    /// rollback path re-points it itself.
    column_slots: super::slots::SlotIndex,
    /// Source key → normalized column/child name mapping (collision-safe).
    namer: UniqueNamer,
    /// The memoized name pairings, BOTH directions: schema resolution maps
    /// source → normalized per column and the batch builder maps normalized →
    /// source per column, so a linear find in either direction would make
    /// every resolve O(columns²) of string compares — per push, on every
    /// table, row-less or not.
    to_normalized: std::collections::HashMap<String, String>,
    normalized_to_source: std::collections::HashMap<String, String>,
    /// Source key → index of the child table it resolves to, memoized.
    ///
    /// A CACHE IN FRONT OF the name-build-then-lookup in `child_table_idx`,
    /// never a replacement for it: a miss must still build the normalized name
    /// and consult the shredder's by-name index, because two different source
    /// keys can normalize to the SAME child table (`"a-b"` and `"a b"`) and a
    /// key at one depth can alias a table created at another. Skipping that
    /// lookup on a miss would create a duplicate `TableName`.
    ///
    /// Deliberately NOT in the pre-push snapshot and NOT cleared by
    /// `revert_column`: it maps keys to positions in an append-only table
    /// vector, so it stays valid across a column rollback. Adding it to the
    /// snapshot would change that struct's shape and its content.
    pub(crate) child_tables: Vec<(String, usize)>,
    /// The key→slot lookup beside `child_tables` — the memo answers per
    /// child key per ROW, so a linear find priced a many-child parent's
    /// push at O(keys × children) compares. Same append-only validity as
    /// the memo itself: kept coherent by [`Self::record_child`], never
    /// snapshotted, never reverted.
    child_slots: super::slots::SlotIndex,
    /// Nested struct fields retained across all columns — they spend from the
    /// SAME per-table budget as top-level columns, because a single struct
    /// column can otherwise smuggle unbounded breadth past the column cap
    /// (and the registry re-clones whatever is retained, every batch).
    nested_fields: usize,
    /// Whether any column state changed since this table's resolved schema
    /// last reached the registry: the resolve pipeline skips resolve+diff for
    /// a row-less, un-dirty table — a maximal stream's per-push bookkeeping
    /// otherwise costs O(total table state) of pure CPU even for an empty
    /// push. Born dirty (a new table must establish its schema).
    dirty: bool,
    /// The pre-push column states, captured LAZILY on this push's first
    /// mutation (an eager whole-stream snapshot would clone every table's
    /// every column state per push). `None` means "not mutated this push"
    /// — including "did not exist before this push", which is exactly the
    /// no-rollback case the resolve pipeline distinguishes.
    pre_push_snapshot: Option<Vec<(String, ColumnState)>>,
}

impl TableBuffer {
    pub(crate) fn new(table: TableName, parent: Option<ParentLink>, rules: IdentRules) -> Self {
        let mut namer = UniqueNamer::new(rules);
        // System columns RESERVE their names: a source field literally named
        // `_rdlt_id` gets suffixed rather than aliasing the lineage column.
        for sys in [
            schema::system::LOAD_ID,
            schema::system::ID,
            schema::system::PARENT_ID,
            schema::system::POS,
            schema::system::ROOT_ID,
        ] {
            namer.reserve(sys);
        }
        Self {
            table,
            parent,
            columns: Vec::new(),
            column_slots: super::slots::SlotIndex::default(),
            namer,
            to_normalized: std::collections::HashMap::new(),
            normalized_to_source: std::collections::HashMap::new(),
            child_tables: Vec::new(),
            child_slots: super::slots::SlotIndex::default(),
            nested_fields: 0,
            dirty: true,
            pre_push_snapshot: None,
        }
    }

    /// The dirty flag: set by every column mutation, cleared once the
    /// table's resolved schema has reached the registry.
    pub(crate) fn is_dirty(&self) -> bool {
        self.dirty
    }

    /// See [`Self::is_dirty`].
    pub(crate) fn mark_clean(&mut self) {
        self.dirty = false;
    }

    /// Per-push housekeeping: forget the previous push's snapshot so this
    /// push's first mutation captures the state IT started from.
    pub(crate) fn begin_push(&mut self) {
        self.pre_push_snapshot = None;
    }

    /// Capture the pre-push column states if this push has not mutated yet.
    /// Must run BEFORE the mutation it protects against a Discard* rollback.
    fn snapshot_on_first_mutation(&mut self) {
        if self.pre_push_snapshot.is_none() {
            self.pre_push_snapshot = Some(self.columns.clone());
        }
    }

    /// Hand the resolve pipeline this push's snapshot (taken, not borrowed —
    /// it mutates the columns the snapshot describes). `None` for a table
    /// not mutated this push.
    pub(crate) fn take_rollback_snapshot(&mut self) -> Option<Vec<(String, ColumnState)>> {
        self.pre_push_snapshot.take()
    }

    /// Observe one value under a source key: ensure the column exists (bounded
    /// by the column cap), then feed the value to its state with the table's
    /// REMAINING struct-field allowance — top-level columns and nested struct
    /// fields spend from one budget, so breadth refuses typed wherever it
    /// hides. Returns the observed state so the caller can inspect its shape.
    pub(crate) fn observe_value<'a, V: JsonView<'a>>(
        &mut self,
        source_key: &str,
        value: V,
        lists_as_columns: bool,
    ) -> Result<&ColumnState, rdlt_core::error::Error> {
        self.snapshot_on_first_mutation();
        self.dirty = true;
        let idx = self.column_index(source_key)?;
        let mut budget = MAX_SOURCE_COLUMNS_PER_TABLE
            .saturating_sub(self.columns.len())
            .saturating_sub(self.nested_fields);
        let initial = budget;
        let observed = self.columns[idx]
            .1
            .observe(value, lists_as_columns, &mut budget);
        // Fields retained before the refusal are already in the state, so the
        // running count charges them either way — the next observation starts
        // from an honest total.
        self.nested_fields += initial - budget;
        if observed.is_err() {
            return Err(rdlt_core::error::Error::config(format!(
                "table `{}` exceeds the {MAX_SOURCE_COLUMNS_PER_TABLE}-source-column cap \
                 while observing key {source_key:?} — nested struct fields count toward \
                 the same bound as columns",
                self.table
            )));
        }
        Ok(&self.columns[idx].1)
    }

    /// The memoized name pairings, normalized → source — what the batch
    /// builder consumes (it walks schema columns, which speak normalized).
    pub(crate) fn normalized_to_source(&self) -> &std::collections::HashMap<String, String> {
        &self.normalized_to_source
    }

    /// Reverse lookup: normalized column name → source key.
    pub(crate) fn source_key_for(&self, normalized: &str) -> Option<&str> {
        self.normalized_to_source
            .get(normalized)
            .map(String::as_str)
    }

    /// Undo a batch of column changes the policy discarded: each key is
    /// restored to the state the snapshot holds for it, or removed when
    /// the snapshot has none (the column was born in this push).
    ///
    /// BATCHED on purpose, and this is the whole reason the method takes
    /// a slice. Every removal shifts the slots after it, so the lookup
    /// index must be re-derived — and a re-derive clones and re-hashes
    /// every remaining key. Doing that per discarded column costs the
    /// table's width once per discard, which is quadratic in a width the
    /// wire chooses: a legal push of four thousand columns, every one of
    /// them discarded, would spend minutes inside one blocking call. One
    /// pass over the table answers all of them: restores land in place
    /// (they shift nothing), the removals go in a single `retain`, and
    /// the index and the nested-field count are each re-derived once.
    pub(crate) fn revert_columns(
        &mut self,
        source_keys: &[String],
        rollback_snapshot: Option<&[(String, ColumnState)]>,
    ) {
        if source_keys.is_empty() {
            return;
        }
        // A rollback IS a mutation: the resolved state no longer matches the
        // registry until the re-resolve lands.
        self.dirty = true;

        // Restores first, while every slot still means what the index
        // says: each is an in-place assignment that moves nothing.
        let mut removals: BTreeSet<&str> = BTreeSet::new();
        // The snapshot is indexed once rather than searched per key: a
        // scan per reverted column costs the snapshot's width times the
        // number reverted, and a batch rollback reverts as many as the
        // policy discarded.
        let prior_states: BTreeMap<&str, &ColumnState> = rollback_snapshot
            .unwrap_or(&[])
            .iter()
            .map(|(key, state)| (key.as_str(), state))
            .collect();
        for source_key in source_keys {
            let prior = prior_states
                .get(source_key.as_str())
                .map(|state| (*state).clone());
            match prior {
                Some(state) => {
                    if let Some(idx) = self.column_slots.slot_of(&self.columns, source_key) {
                        self.columns[idx].1 = state;
                    }
                }
                None => {
                    removals.insert(source_key.as_str());
                }
            }
        }

        // Then the removals, in one pass, and one re-derive behind them.
        if !removals.is_empty() {
            self.columns.retain(|(k, _)| !removals.contains(k.as_str()));
            self.column_slots.rebuilt(&self.columns);
        }

        // A rollback can remove or shrink a struct column, so the running
        // nested-field count is re-derived from what actually remains.
        self.nested_fields = self
            .columns
            .iter()
            .map(|(_, state)| state.nested_field_count())
            .sum();
    }

    /// The memoized child-table index for a source key, if this parent has
    /// resolved that exact key before.
    pub(crate) fn child_idx_of(&mut self, source_key: &str) -> Option<usize> {
        self.child_slots
            .slot_of(&self.child_tables, source_key)
            .map(|slot| self.child_tables[slot].1)
    }

    /// Memoize `source_key` → child table `idx` (see `child_tables`).
    pub(crate) fn record_child(&mut self, source_key: String, idx: usize) {
        self.child_tables.push((source_key, idx));
        self.child_slots.grew(&self.child_tables);
    }

    /// Normalized column name for a source key, memoizing the pairing on first
    /// sight — may allocate and insert into the maps.
    pub(crate) fn normalized_name_for(&mut self, source_key: &str) -> String {
        if let Some(normalized) = self.to_normalized.get(source_key) {
            return normalized.clone();
        }
        let normalized = self.namer.name_for(source_key);
        self.to_normalized
            .insert(source_key.to_owned(), normalized.clone());
        self.normalized_to_source
            .insert(normalized.clone(), source_key.to_owned());
        normalized
    }

    pub(crate) fn state_mut(
        &mut self,
        source_key: &str,
    ) -> Result<&mut ColumnState, rdlt_core::error::Error> {
        self.snapshot_on_first_mutation();
        self.dirty = true;
        let idx = self.column_index(source_key)?;
        Ok(&mut self.columns[idx].1)
    }

    /// Index of the column for a source key, creating it (as `Unknown`) under
    /// the column cap. Nested struct fields count toward the same cap — the
    /// budget arithmetic lives in [`Self::observe_value`], which is why the
    /// creation check here subtracts the running nested-field total too.
    fn column_index(&mut self, source_key: &str) -> Result<usize, rdlt_core::error::Error> {
        if let Some(idx) = self.column_slots.slot_of(&self.columns, source_key) {
            return Ok(idx);
        }
        if self.columns.len() + self.nested_fields >= MAX_SOURCE_COLUMNS_PER_TABLE {
            return Err(rdlt_core::error::Error::config(format!(
                "table `{}` exceeds the {MAX_SOURCE_COLUMNS_PER_TABLE}-source-column cap \
                 while observing key {source_key:?}",
                self.table
            )));
        }
        self.columns
            .push((source_key.to_owned(), ColumnState::Unknown));
        self.column_slots.grew(&self.columns);
        Ok(self.columns.len() - 1)
    }

    /// Wholesale index rebuilds this table's columns have paid for —
    /// what the rollback's complexity pin reads.
    #[cfg(test)]
    pub(crate) fn column_rebuilds(&self) -> u64 {
        self.column_slots.rebuilds()
    }

    /// Total key comparisons the column lookup has cost — the meter the
    /// complexity pin reads.
    #[cfg(test)]
    pub(crate) fn column_probes(&self) -> u64 {
        self.column_slots.probes()
    }

    /// Total key comparisons the child memo has cost — the meter the
    /// complexity pin reads.
    #[cfg(test)]
    pub(crate) fn child_probes(&self) -> u64 {
        self.child_slots.probes()
    }
}

/// `_rdlt_id` for a root row: key hash when the stream declares a primary key,
/// content hash otherwise.
///
/// `scratch` is the caller's reusable buffer, threaded through so the keyless
/// arm does not allocate per row. It is safe to share because
/// [`content_hash_with`] clears it on entry and the hasher is built AFTER
/// canonicalization finishes — nothing left in the buffer from a previous row
/// can reach the digest, and reuse cannot reorder what does.
pub(crate) fn row_identity<'a, V: JsonView<'a>>(
    primary_key: Option<&[String]>,
    row: V,
    scratch: &mut Vec<u8>,
) -> RowId {
    match primary_key {
        Some(key_fields) if !key_fields.is_empty() => {
            let mut builder = RowIdBuilder::keyed();
            for field in key_fields {
                // The keyed arm deliberately does NOT use `scratch`. Sharing
                // one buffer across key fields would need a clear between each
                // one, and getting that wrong concatenates field two onto
                // field one — `{"a":1,"b":2}` would hash as `{"a":12}` does.
                // `render_scalar`'s allocation is the price of that not being
                // possible; the pinned composite-key cases exist to keep it so.
                let rendered = row.obj_get(field).and_then(render_scalar);
                match &rendered {
                    Some(text) => builder.field(field, FieldValue::Bytes(text.as_bytes())),
                    None => builder.field(field, FieldValue::Null),
                };
            }
            builder.finish()
        }
        _ => content_hash_with(row, scratch),
    }
}

/// Content hash into a caller-owned scratch buffer — traversals hash every
/// row and child, and per-call Vec churn was visible in the shred profile.
///
/// The buffer is an output area, never an input: `clear` at entry, append-only
/// canonicalization, then a fresh hasher over exactly `scratch[..len]`. Its
/// residual capacity is unobservable because the length prefix is taken from
/// `len()`, not `capacity()`.
pub(crate) fn content_hash_with<'a, V: JsonView<'a>>(row: V, scratch: &mut Vec<u8>) -> RowId {
    scratch.clear();
    canonical_json_bytes(row, scratch);
    let mut builder = RowIdBuilder::keyless();
    builder.field("", FieldValue::Bytes(scratch));
    builder.finish()
}

/// Resolve one table's observation state into schema columns: system/lineage
/// columns first, then source columns in first-seen order.
pub(crate) fn resolve_schema(buffer: &mut TableBuffer) -> TableSchema {
    let mut columns: Vec<Column> = Vec::new();
    let system = |name: &str, ty| Column {
        name: name.to_owned(),
        column_type: rdlt_core::schema::ColumnType::scalar(ty),
        nullable: false,
        provenance: Provenance::System,
    };
    columns.push(system(
        schema::system::LOAD_ID,
        rdlt_core::types::LogicalType::Utf8,
    ));
    columns.push(system(
        schema::system::ID,
        rdlt_core::types::LogicalType::Utf8,
    ));
    if buffer.parent.is_some() {
        columns.push(system(
            schema::system::PARENT_ID,
            rdlt_core::types::LogicalType::Utf8,
        ));
        columns.push(system(
            schema::system::POS,
            rdlt_core::types::LogicalType::Int64,
        ));
        columns.push(system(
            schema::system::ROOT_ID,
            rdlt_core::types::LogicalType::Utf8,
        ));
    }

    let sources: Vec<(String, Option<rdlt_core::schema::ColumnType>)> = buffer
        .columns
        .iter()
        .map(|(key, state)| (key.clone(), state.resolve()))
        .collect();
    for (source_key, resolved) in sources {
        if let Some(ty) = resolved {
            let name = buffer.normalized_name_for(&source_key);
            columns.push(Column {
                name,
                column_type: ty,
                nullable: true,
                provenance: Provenance::Inferred,
            });
        }
    }

    TableSchema {
        table: buffer.table.clone(),
        parent: buffer.parent.clone(),
        columns,
    }
}

#[cfg(test)]
mod cardinality_tests {
    use super::*;

    #[test]
    fn cumulative_distinct_keys_stop_at_the_source_column_cap() {
        let mut table = TableBuffer::new(
            TableName::new("events"),
            None,
            rdlt_core::schema::IdentRules::default(),
        );
        for index in 0..MAX_SOURCE_COLUMNS_PER_TABLE {
            table
                .state_mut(&format!("field-{index}"))
                .expect("within the cap");
        }
        let error = table
            .state_mut("one-too-many")
            .expect_err("the cumulative cap must refuse");
        assert!(error.to_string().contains("source-column cap"));
    }

    /// Pinned directly because a regression here is invisible to every
    /// behavioral test (the failure mode is pure CPU): a table is born
    /// dirty, a successful resolve cleans it, and the rollback snapshot arms
    /// ONLY on a push's first mutation — never for a table the push left
    /// alone.
    #[test]
    fn the_dirty_flag_and_lazy_snapshot_arm_and_disarm_per_push() {
        let mut table = TableBuffer::new(
            TableName::new("events"),
            None,
            rdlt_core::schema::IdentRules::default(),
        );
        assert!(
            table.is_dirty(),
            "born dirty: a new table must establish its schema"
        );

        // The resolve handshake: applied to the registry → clean.
        table.mark_clean();
        assert!(!table.is_dirty());

        // A push begins; nothing observed yet → no snapshot exists.
        table.begin_push();
        assert!(
            table.take_rollback_snapshot().is_none(),
            "an unmutated table has nothing to roll back to"
        );

        // The push's first mutation arms the snapshot AND re-dirties.
        table
            .observe_value("k", &serde_json::json!(1), false)
            .expect("observe");
        assert!(table.is_dirty(), "a mutation re-dirties");
        let snapshot = table
            .take_rollback_snapshot()
            .expect("the first mutation of the push armed the snapshot");
        assert!(
            snapshot.is_empty(),
            "the pre-push state had no columns: {snapshot:?}"
        );
        assert!(
            table.take_rollback_snapshot().is_none(),
            "the resolve consumes the snapshot exactly once"
        );

        // A mutation-free push after that leaves both mechanisms idle.
        table.mark_clean();
        table.begin_push();
        assert!(!table.is_dirty());
        assert!(table.take_rollback_snapshot().is_none());
    }
}

#[cfg(test)]
mod identity_cross_view {
    //! The identity of a document must not depend on which JSON view carried
    //! it. The engine shreds raw byte slabs through the ARENA view and
    //! already-parsed rows through the `&serde_json::Value` TREE view; the
    //! same document must hash identically either way.
    //!
    //! This is not a restatement of "both call the same function". They walk
    //! different data structures — the arena stores borrowed `Cow<str>` keys
    //! in flat side-tables, the tree stores an owned `Map` — and either could
    //! diverge in entry order, in how a duplicate key resolves, or in how a
    //! non-object root is wrapped. The byte-exact corpus in
    //! `tests/cases/test_shred_identity_pin.rs` pins the VALUES; this pins their
    //! AGREEMENT across the two views, over arbitrary documents.

    use proptest::prelude::*;
    use rdlt_connector::channel::MAX_RECORD_BATCH_ROWS;
    use serde_json::Value;

    use super::{content_hash_with, row_identity};
    use crate::shred::arena::Arena;

    /// Arbitrary JSON, depth-limited, biased toward the shapes that differ
    /// between the two views: duplicate-prone short keys, nulls, nested
    /// objects and arrays.
    fn any_json(depth: u32) -> BoxedStrategy<Value> {
        let scalar = prop_oneof![
            Just(Value::Null),
            any::<bool>().prop_map(Value::from),
            any::<i64>().prop_map(Value::from),
            any::<u64>().prop_map(Value::from),
            (-1.0e6f64..1.0e6).prop_map(Value::from),
            "[a-z]{0,6}".prop_map(Value::from),
        ];
        if depth == 0 {
            return scalar.boxed();
        }
        prop_oneof![
            4 => scalar,
            1 => proptest::collection::vec(any_json(depth - 1), 0..3).prop_map(Value::from),
            2 => proptest::collection::btree_map("[a-z]{1,3}", any_json(depth - 1), 0..4)
                .prop_map(|m| Value::Object(m.into_iter().collect())),
        ]
        .boxed()
    }

    fn docs() -> impl Strategy<Value = Vec<Value>> {
        proptest::collection::vec(
            proptest::collection::btree_map("[a-z]{1,3}", any_json(2), 1..5)
                .prop_map(|m| Value::Object(m.into_iter().collect())),
            1..4,
        )
    }

    /// Serialize the generated documents, then hand BOTH views the same text.
    ///
    /// The re-parse is load-bearing, not ceremony. A `Value` built from Rust
    /// float literals and a `Value` parsed from their decimal text are not
    /// always the same `f64`: serde_json's number parser is not bit-identical
    /// to rustc's literal parser (`92634.32078095645` parses to the neighbour
    /// of the literal). Comparing a literal-built tree against a text-parsed
    /// arena would therefore fail on a difference that has nothing to do with
    /// the views — and in production BOTH sides come from a parser anyway.
    fn views(values: &[Value]) -> (String, Vec<Value>) {
        let slab = values
            .iter()
            .map(|v| serde_json::to_string(v).expect("json"))
            .collect::<Vec<_>>()
            .join("\n");
        let parsed = slab
            .lines()
            .map(|l| serde_json::from_str(l).expect("round-trip"))
            .collect();
        (slab, parsed)
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// Keyless arm: the content hash over canonical JSON bytes.
        #[test]
        fn content_hash_agrees_across_views(values in docs()) {
            let (slab, values) = views(&values);
            let bytes = slab.as_bytes();
            let mut arena = Arena::default();
            let rows = arena.parse_rows(bytes, MAX_RECORD_BATCH_ROWS, rdlt_connector::channel::MAX_JSON_VALUES_PER_PUSH).expect("the slab we just serialized must parse");
            prop_assert_eq!(rows.len(), values.len());
            for (node, value) in rows.into_iter().zip(&values) {
                let (mut a, mut b) = (Vec::new(), Vec::new());
                prop_assert_eq!(
                    content_hash_with(arena.node(node), &mut a),
                    content_hash_with(value, &mut b),
                    "arena and tree views disagree on the content hash of {}",
                    value
                );
            }
        }

        /// Keyed arm: per-field rendered text. Every top-level key is tried as
        /// a single-field key, and the first two together as a composite —
        /// composites are what catch a shared scratch buffer cleared once per
        /// row instead of once per field.
        #[test]
        fn keyed_identity_agrees_across_views(values in docs()) {
            let (slab, values) = views(&values);
            let bytes = slab.as_bytes();
            let mut arena = Arena::default();
            let rows = arena.parse_rows(bytes, MAX_RECORD_BATCH_ROWS, rdlt_connector::channel::MAX_JSON_VALUES_PER_PUSH).expect("the slab we just serialized must parse");
            for (node, value) in rows.into_iter().zip(&values) {
                let keys: Vec<String> = value
                    .as_object()
                    .map(|m| m.keys().cloned().collect())
                    .unwrap_or_default();
                let mut trials: Vec<Vec<String>> = keys.iter().map(|k| vec![k.clone()]).collect();
                if keys.len() >= 2 {
                    trials.push(vec![keys[0].clone(), keys[1].clone()]);
                    // Reversed too: declared key ORDER is load-bearing, so the
                    // views must agree on it and not merely on the key set.
                    trials.push(vec![keys[1].clone(), keys[0].clone()]);
                }
                // A key no document has: absent must render identically in
                // both views.
                trials.push(vec!["\u{1F600}missing".to_string()]);
                for key in trials {
                    let (mut a, mut b) = (Vec::new(), Vec::new());
                    prop_assert_eq!(
                        row_identity(Some(&key), arena.node(node), &mut a),
                        row_identity(Some(&key), value, &mut b),
                        "views disagree on the keyed identity of {} under key {:?}",
                        value,
                        key
                    );
                }
            }
        }
    }
}
