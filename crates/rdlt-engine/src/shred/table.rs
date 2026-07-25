//! Shared shredding state: per-table naming, shape observation, lineage
//! identity, and schema resolution — everything the tape traversal (`tape.rs`)
//! and the drain (`mod.rs`) build on.

use rdlt_core::identity::{FieldValue, RowIdBuilder};
use rdlt_core::naming::{IdentRules, UniqueNamer};
use rdlt_core::schema::system_columns;
use rdlt_core::{ColumnDef, ParentLink, Provenance, RowId, TableName, TableSchema};

use super::canon::{canonical_json_bytes, render_scalar};
use super::infer::ColState;
use super::view::JsonView;

/// One table's persistent shredding state: naming, shape observation, lineage —
/// everything EXCEPT the buffered rows (those are per-batch and path-specific).
#[derive(Debug)]
pub(crate) struct TableBuffer {
    pub(crate) table: TableName,
    pub(crate) parent: Option<ParentLink>,
    /// Column observation states in first-seen order; source key → state.
    pub(crate) columns: Vec<(String, ColState)>,
    /// Source key → normalized column/child name mapping (collision-safe).
    namer: UniqueNamer,
    names: Vec<(String, String)>,
    /// Source key → index of the child table it resolves to, memoized.
    ///
    /// A CACHE IN FRONT OF the name-build-then-scan in `child_table_idx`,
    /// never a replacement for it: a miss must still build the normalized name
    /// and scan, because two different source keys can normalize to the SAME
    /// child table (`"a-b"` and `"a b"`) and a key at one depth can alias a
    /// table created at another. Skipping the scan on a miss would create a
    /// duplicate `TableName`.
    ///
    /// Deliberately NOT in `rollback_snapshot` and NOT cleared by
    /// `revert_column`: it maps keys to positions in an append-only table
    /// vector, so it stays valid across a column rollback. Adding it to the
    /// snapshot would change that struct's shape and its positional alignment
    /// with `self.tables`.
    pub(crate) child_tables: Vec<(String, usize)>,
}

impl TableBuffer {
    pub(crate) fn new(table: TableName, parent: Option<ParentLink>, rules: IdentRules) -> Self {
        let mut namer = UniqueNamer::new(rules);
        // System columns RESERVE their names: a source field literally named
        // `_rdlt_id` gets suffixed rather than aliasing the lineage column.
        for sys in [
            system_columns::LOAD_ID,
            system_columns::ID,
            system_columns::PARENT_ID,
            system_columns::POS,
            system_columns::ROOT_ID,
        ] {
            namer.reserve(sys);
        }
        Self {
            table,
            parent,
            columns: Vec::new(),
            namer,
            names: Vec::new(),
            child_tables: Vec::new(),
        }
    }

    /// Source key → normalized column name pairs accumulated so far.
    pub(crate) fn source_to_normalized(&self) -> &[(String, String)] {
        &self.names
    }

    /// Reverse lookup: normalized column name → source key.
    pub(crate) fn source_key_for(&self, normalized: &str) -> Option<&str> {
        self.names
            .iter()
            .find(|(_, n)| n == normalized)
            .map(|(source, _)| source.as_str())
    }

    /// Policy enforcement: revert one column's observation state to its pre-batch
    /// snapshot (or remove it if the column first appeared this batch).
    pub(crate) fn revert_column(
        &mut self,
        source_key: &str,
        rollback_snapshot: Option<&[(String, ColState)]>,
    ) {
        let prior = rollback_snapshot.and_then(|columns| {
            columns
                .iter()
                .find(|(key, _)| key == source_key)
                .map(|(_, state)| state.clone())
        });
        match prior {
            Some(state) => {
                if let Some(idx) = self.columns.iter().position(|(k, _)| k == source_key) {
                    self.columns[idx].1 = state;
                }
            }
            None => self.columns.retain(|(k, _)| k != source_key),
        }
    }

    /// Normalized column name for a source key, memoizing the pairing on first
    /// sight — may allocate and insert into the source→normalized map.
    pub(crate) fn normalized_name_for(&mut self, source_key: &str) -> String {
        if let Some((_, normalized)) = self.names.iter().find(|(k, _)| k == source_key) {
            return normalized.clone();
        }
        let normalized = self.namer.name_for(source_key);
        self.names.push((source_key.to_owned(), normalized.clone()));
        normalized
    }

    pub(crate) fn state_mut(&mut self, source_key: &str) -> &mut ColState {
        if let Some(idx) = self.columns.iter().position(|(k, _)| k == source_key) {
            &mut self.columns[idx].1
        } else {
            self.columns
                .push((source_key.to_owned(), ColState::Unknown));
            &mut self.columns.last_mut().expect("just pushed").1
        }
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
    let mut columns: Vec<ColumnDef> = Vec::new();
    let system = |name: &str, ty| ColumnDef {
        name: name.to_owned(),
        column_type: rdlt_core::ColumnType::scalar(ty),
        nullable: false,
        provenance: Provenance::System,
    };
    columns.push(system(
        system_columns::LOAD_ID,
        rdlt_core::LogicalType::Utf8,
    ));
    columns.push(system(system_columns::ID, rdlt_core::LogicalType::Utf8));
    if buffer.parent.is_some() {
        columns.push(system(
            system_columns::PARENT_ID,
            rdlt_core::LogicalType::Utf8,
        ));
        columns.push(system(system_columns::POS, rdlt_core::LogicalType::Int64));
        columns.push(system(
            system_columns::ROOT_ID,
            rdlt_core::LogicalType::Utf8,
        ));
    }

    let sources: Vec<(String, Option<rdlt_core::ColumnType>)> = buffer
        .columns
        .iter()
        .map(|(key, state)| (key.clone(), state.resolve()))
        .collect();
    for (source_key, resolved) in sources {
        if let Some(ty) = resolved {
            let name = buffer.normalized_name_for(&source_key);
            columns.push(ColumnDef {
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
    //! `tests/shred_identity_pin.rs` pins the VALUES; this pins their
    //! AGREEMENT across the two views, over arbitrary documents.

    use proptest::prelude::*;
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
            let rows = arena.parse_rows(bytes).expect("the slab we just serialized must parse");
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
            let rows = arena.parse_rows(bytes).expect("the slab we just serialized must parse");
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
