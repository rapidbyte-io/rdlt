//! End-to-end shredder property test (feature 003 US1, research R23).
//!
//! Arbitrary nested documents through the FULL engine path, asserting the
//! invariants no example test can pin down exhaustively:
//!   1. Row conservation — every input row lands exactly once; child-table rows
//!      match a recursive oracle of the list-of-objects rule.
//!   2. Lineage integrity — every `_rdlt_parent_id` resolves to a real parent
//!      row, every `_rdlt_root_id` to a real root row.
//!   3. Schema monotonicity — the schema after batch 1 alone is a widen-only
//!      prefix of the schema after batches 1+2.
//!   4. Naming safety — no duplicate destination column names in any table.
//!
//! The key pool deliberately includes `_rdlt_*` system names (the feature-002
//! review bug), keys that collide after normalization, and 2^53-boundary ints.
//! Default 256 cases; scheduled CI runs PROPTEST_CASES=4096 (`TARGET=prop make test`).

use std::collections::BTreeSet;

use proptest::prelude::*;
use rdlt_connector::source::StreamSpec;
use rdlt_core::schema;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryBatch, MemoryDestination, MemorySource, MemoryStream};
use serde_json::{Map, Value, json};

// ---------- generation ----------
//
// Keys come from SHAPE-DISJOINT pools: a scalar key never holds a list, a
// child-list key never holds a scalar. Shape CONFLICTS (same key, different
// shapes across batches) are deliberately excluded — their child-extraction
// outcome is order-dependent by design (the shredder degrades the column to a
// JSON value from the conflict onward), so no exact row oracle exists for them;
// dedicated example tests cover that path. Within these rules the oracle below
// is exact.

/// Scalar-position keys: every naming hazard we know, including `_rdlt_*`
/// system names (the feature-002 review bug) and normalization collisions.
fn arb_scalar_key() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("id".to_owned()),
        Just("value".to_owned()),
        Just("_rdlt_id".to_owned()),
        Just("_rdlt_load_id".to_owned()),
        Just("user_name".to_owned()),
        Just("user-name".to_owned()),
        Just("User Name".to_owned()),
        Just("naïve".to_owned()),
        Just("0lead".to_owned()),
        "[a-c]{1,3}",
    ]
}

fn arb_child_list_key() -> impl Strategy<Value = String> {
    prop_oneof![
        Just("tags".to_owned()),
        Just("items".to_owned()),
        Just("events".to_owned())
    ]
}

fn arb_scalar_list_key() -> impl Strategy<Value = String> {
    prop_oneof![Just("codes".to_owned()), Just("flags".to_owned())]
}

fn arb_nested_key() -> impl Strategy<Value = String> {
    prop_oneof![Just("profile".to_owned()), Just("meta".to_owned())]
}

fn arb_scalar() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        // Include the ±2^53 widening boundary and its neighbors.
        prop_oneof![
            any::<i64>(),
            Just(1i64 << 53),
            Just((1i64 << 53) + 1),
            Just(-(1i64 << 53) - 1),
        ]
        .prop_map(Value::from),
        any::<f64>()
            .prop_filter("finite", |f| f.is_finite())
            .prop_map(Value::from),
        "[ -~]{0,12}".prop_map(Value::from),
        Just(Value::from("日本語テキスト")),
    ]
}

/// One generated object: scalar fields, plus (below the depth floor) optional
/// scalar-list, nested-object, and child-list fields under their pool keys.
fn arb_object(depth: u32) -> BoxedStrategy<Value> {
    let scalars = prop::collection::btree_map(arb_scalar_key(), arb_scalar(), 1..5);
    if depth == 0 {
        return scalars
            .prop_map(|m| Value::Object(m.into_iter().collect::<Map<_, _>>()))
            .boxed();
    }
    let scalar_lists = prop::collection::btree_map(
        arb_scalar_list_key(),
        prop::collection::vec(arb_scalar(), 0..3).prop_map(Value::Array),
        0..2,
    );
    let nested = prop::collection::btree_map(arb_nested_key(), arb_object(depth - 1), 0..2);
    let children = prop::collection::btree_map(
        arb_child_list_key(),
        prop::collection::vec(
            prop_oneof![4 => arb_object(depth - 1), 1 => Just(Value::Null)],
            0..3,
        )
        .prop_map(Value::Array),
        0..2,
    );
    (scalars, scalar_lists, nested, children)
        .prop_map(|(a, b, c, d)| {
            let mut map = Map::new();
            map.extend(a);
            map.extend(b);
            map.extend(c);
            map.extend(d);
            Value::Object(map)
        })
        .boxed()
}

fn arb_batch() -> impl Strategy<Value = Vec<Value>> {
    prop::collection::vec(arb_object(3), 1..8)
}

// ---------- oracle ----------

/// Recursive count of expected rows: 1 for the row itself + every non-null item
/// of every all-objects array, at any depth (mirrors the shredder's child rule).
fn expected_rows(row: &Value) -> u64 {
    let mut total = 1;
    if let Value::Object(map) = row {
        for value in map.values() {
            if let Value::Array(items) = value
                && !items.is_empty()
                && items.iter().all(|i| i.is_object() || i.is_null())
            {
                for item in items.iter().filter(|i| !i.is_null()) {
                    total += expected_rows(item);
                }
            }
        }
    }
    total
}

// ---------- harness ----------

fn run_engine(batches: Vec<Vec<Value>>) -> MemoryDestination {
    let dest = MemoryDestination::new();
    let stream = MemoryStream::new(
        StreamSpec::new("s"),
        batches
            .into_iter()
            .enumerate()
            .map(|(i, rows)| MemoryBatch::new(rows).with_checkpoint(json!({"b": i})))
            .collect(),
    );
    let engine = Engine::new(
        EngineConfig::new("prop"),
        MemorySource::new(vec![stream]),
        dest.clone(),
    );
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(engine.run())
        .expect("run succeeds");
    dest
}

fn scalar_of(ty: &rdlt_core::schema::ColumnType) -> Option<rdlt_core::types::LogicalType> {
    match ty {
        rdlt_core::schema::ColumnType::Scalar { scalar } => Some(*scalar),
        _ => None,
    }
}

proptest! {
    #[test]
    fn shred_invariants_hold(batch1 in arb_batch(), batch2 in arb_batch()) {
        let total_expected: u64 = batch1.iter().chain(&batch2).map(expected_rows).sum();
        let root_expected = (batch1.len() + batch2.len()) as u64;

        let dest = run_engine(vec![batch1.clone(), batch2.clone()]);
        let tables = dest.committed_tables();

        // (1) Row conservation.
        let root_rows = dest.committed_rows("s");
        prop_assert_eq!(root_rows.len() as u64, root_expected, "root row count");
        let total: u64 = tables
            .iter()
            .map(|t| dest.committed_rows(t.as_str()).len() as u64)
            .sum();
        prop_assert_eq!(total, total_expected, "total rows across table family");

        // (2) Lineage integrity + (4) naming safety.
        let root_ids: BTreeSet<String> = root_rows
            .iter()
            .map(|r| r[schema::system::ID].as_str().expect("root id").to_owned())
            .collect();
        for table in &tables {
            let schema = dest.schema(table.as_str()).expect("schema");
            let mut names = BTreeSet::new();
            for column in &schema.columns {
                prop_assert!(names.insert(column.name.clone()),
                    "duplicate column `{}` in `{}`", column.name, table);
            }
            let Some(parent) = schema.parent.clone() else { continue };
            let parent_ids: BTreeSet<String> = dest
                .committed_rows(parent.parent.as_str())
                .iter()
                .map(|r| r[schema::system::ID].as_str().expect("parent id").to_owned())
                .collect();
            for row in dest.committed_rows(table.as_str()) {
                let pid = row[schema::system::PARENT_ID].as_str().expect("parent id present");
                prop_assert!(parent_ids.contains(pid),
                    "orphan `_rdlt_parent_id` in `{table}`");
                let rid = row[schema::system::ROOT_ID].as_str().expect("root id present");
                prop_assert!(root_ids.contains(rid),
                    "dangling `_rdlt_root_id` in `{table}`");
            }
        }

        // (3) Schema monotonicity: batch1-only schemas are widen-only prefixes.
        let first = run_engine(vec![batch1]);
        for table in first.committed_tables() {
            let s1 = first.schema(table.as_str()).expect("schema 1");
            let Some(s2) = dest.schema(table.as_str()) else {
                // A table produced by batch1 must still exist after batch1+batch2.
                prop_assert!(false, "table `{table}` vanished with more data");
                continue;
            };
            for c1 in &s1.columns {
                let Some(c2) = s2.columns.iter().find(|c| c.name == c1.name) else {
                    prop_assert!(false, "column `{}` of `{table}` vanished", c1.name);
                    continue;
                };
                if let (Some(t1), Some(t2)) = (scalar_of(&c1.column_type), scalar_of(&c2.column_type)) {
                    // t2 must be t1 widened (possibly unchanged): joining them
                    // lands on t2, never back on t1's side of the lattice.
                    prop_assert!(
                        rdlt_core::types::widen(t1, t2) == t2,
                        "column `{}` of `{table}` narrowed: {t1:?} -> {t2:?}",
                        c1.name
                    );
                }
            }
        }
    }
}
