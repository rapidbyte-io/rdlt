//! Old-path ≡ new-path equivalence (feature 003 R24, gate G5.3; data-model §7).
//!
//! For any input byte sequence, the tree shredder and the tape shredder must
//! produce IDENTICAL LoadItem sequences: same schemas, same deltas, same
//! batches (bit-for-bit arrays — ids, values, nulls), same discard counts, in
//! the same order. Unit-level (LoadItem is crate-private); dup-key and escape
//! lexical cases are pinned in `arena` tests and the explicit cases below.

use proptest::prelude::*;
use rdlt_connector::{DestCapabilities, StreamSpec};
use rdlt_core::{LoadId, SchemaPolicy, TableName, WriteMode};
use serde_json::Value;

use super::tape::{PushError, TapeShredder};
use super::{LoadItem, TreeShredder};
use crate::schema::registry::SchemaRegistry;

fn run_tree(pushes: &[Vec<u8>], spec: &StreamSpec) -> Result<Vec<LoadItem>, String> {
    let caps = DestCapabilities::default();
    let mut shredder = TreeShredder::new(spec.clone(), caps, TableName::new("t"));
    let mut registry = SchemaRegistry::default();
    let mut items = Vec::new();
    for push in pushes {
        shredder.push_bytes(push).map_err(|e| e.to_string())?;
        items.extend(
            shredder
                .drain_batch(
                    &mut registry,
                    &LoadId::new("load"),
                    &WriteMode::Append,
                    &SchemaPolicy::evolve(),
                )
                .map_err(|e| e.to_string())?,
        );
    }
    Ok(items)
}

fn run_tape(pushes: &[Vec<u8>], spec: &StreamSpec) -> Result<Vec<LoadItem>, String> {
    let caps = DestCapabilities::default();
    let mut shredder = TapeShredder::new(spec.clone(), caps, TableName::new("t"));
    let mut registry = SchemaRegistry::default();
    let mut items = Vec::new();
    for push in pushes {
        let batch = shredder
            .push_and_drain(
                push,
                &mut registry,
                &LoadId::new("load"),
                &WriteMode::Append,
                &SchemaPolicy::evolve(),
            )
            .map_err(|e| match e {
                PushError::Json(e) => e.to_string(),
                PushError::Engine(e) => e.to_string(),
            })?;
        items.extend(batch);
    }
    Ok(items)
}

fn assert_equivalent(pushes: &[Vec<u8>], spec: &StreamSpec) {
    let tree = run_tree(pushes, spec);
    let tape = run_tape(pushes, spec);
    match (tree, tape) {
        (Ok(tree), Ok(tape)) => {
            assert_eq!(tree.len(), tape.len(), "item count");
            for (i, (a, b)) in tree.iter().zip(&tape).enumerate() {
                match (a, b) {
                    (
                        LoadItem::Delta {
                            schema: sa,
                            delta: da,
                            mode: ma,
                        },
                        LoadItem::Delta {
                            schema: sb,
                            delta: db,
                            mode: mb,
                        },
                    ) => {
                        assert_eq!(sa, sb, "schema at item {i}");
                        assert_eq!(da, db, "delta at item {i}");
                        assert_eq!(ma, mb, "mode at item {i}");
                    }
                    (
                        LoadItem::Batch {
                            table: ta,
                            batch: ba,
                        },
                        LoadItem::Batch {
                            table: tb,
                            batch: bb,
                        },
                    ) => {
                        assert_eq!(ta, tb, "table at item {i}");
                        assert_eq!(ba, bb, "batch at item {i} (table {ta})");
                    }
                    (
                        LoadItem::Discarded {
                            table: ta,
                            rows: ra,
                            values: va,
                        },
                        LoadItem::Discarded {
                            table: tb,
                            rows: rb,
                            values: vb,
                        },
                    ) => {
                        assert_eq!((ta, ra, va), (tb, rb, vb), "discard at item {i}");
                    }
                    (a, b) => panic!("item {i} variant mismatch: {a:?} vs {b:?}"),
                }
            }
        }
        (Err(a), Err(b)) => {
            // Both must fail; exact message text may differ between parsers.
            let _ = (a, b);
        }
        (tree, tape) => panic!("one path failed: tree={tree:?} tape={tape:?}"),
    }
}

// ---------- explicit lexical/structural cases ----------

#[test]
fn explicit_hazard_cases_are_equivalent() {
    let spec = StreamSpec::new("t");
    let cases: &[&[&str]] = &[
        // dup keys, escapes, unicode
        &[r#"{"dup":1,"a":2,"dup":3}"#],
        &[r#"{"esc":"a\"b\\c\nd","uni":"日本語","key with space":1}"#],
        // bare scalars and arrays as rows; NDJSON + top-level array flattening
        &["42\n\"text\"\ntrue", r#"[{"a":1},{"b":2}]"#],
        // nested children, scalar lists, empty lists, null items
        &[r#"{"id":1,"tags":[{"l":"a"},null,{"l":"b"}],"codes":[1,2],"empty":[]}"#],
        // cross-batch widening + new columns + child table appearing later
        &[
            r#"{"n":1}"#,
            r#"{"n":2.5,"extra":"x"}"#,
            r#"{"n":3,"kids":[{"k":1}]}"#,
        ],
        // 2^53 boundary, big u64, floats
        &[r#"{"big":9007199254740993,"huge":18446744073709551615,"f":0.1}"#],
        // shape conflict → Json column
        &[r#"{"x":{"a":1}}"#, r#"{"x":5}"#],
        // deep nesting via structs
        &[r#"{"p":{"q":{"r":{"s":1}}}}"#],
        // system-column-named source keys (the feature-002 review bug)
        &[r#"{"_rdlt_id":"upstream","_rdlt_load_id":"x","id":1}"#],
    ];
    for case in cases {
        let pushes: Vec<Vec<u8>> = case.iter().map(|s| s.as_bytes().to_vec()).collect();
        assert_equivalent(&pushes, &spec);
    }
}

#[test]
fn keyed_identity_is_equivalent() {
    let spec = StreamSpec::new("t").with_primary_key(["id".to_owned()]);
    let pushes = vec![
        br#"{"id":1,"v":"a"}"#.to_vec(),
        br#"{"id":2,"v":"b","w":1.5}"#.to_vec(),
    ];
    assert_equivalent(&pushes, &spec);
}

// ---------- generative ----------

fn arb_scalar() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        prop_oneof![any::<i64>(), Just(1i64 << 53), Just((1i64 << 53) + 1),].prop_map(Value::from),
        any::<f64>()
            .prop_filter("finite", |f| f.is_finite())
            .prop_map(Value::from),
        // Printable + escapes + unicode.
        "[ -~]{0,10}".prop_map(Value::from),
        Just(Value::from("line\nbreak \"quoted\" 日本語")),
    ]
}

fn arb_key() -> impl Strategy<Value = String> {
    prop_oneof![
        "[a-c]{1,2}",
        Just("id".to_owned()),
        Just("_rdlt_id".to_owned()),
        Just("User Name".to_owned()),
        Just("esc\"key".to_owned()),
    ]
}

fn arb_object(depth: u32) -> BoxedStrategy<Value> {
    let field = if depth == 0 {
        arb_scalar().boxed()
    } else {
        prop_oneof![
            4 => arb_scalar(),
            1 => prop::collection::vec(arb_scalar(), 0..3).prop_map(Value::Array),
            2 => prop::collection::vec(
                prop_oneof![3 => arb_object(depth - 1), 1 => Just(Value::Null)],
                0..3
            )
            .prop_map(Value::Array),
            1 => arb_object(depth - 1),
        ]
        .boxed()
    };
    prop::collection::btree_map(arb_key(), field, 1..5)
        .prop_map(|m| Value::Object(m.into_iter().collect()))
        .boxed()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn tree_and_tape_are_equivalent(
        batches in prop::collection::vec(prop::collection::vec(arb_object(3), 1..5), 1..3)
    ) {
        let spec = StreamSpec::new("t");
        let pushes: Vec<Vec<u8>> = batches
            .iter()
            .map(|rows| {
                let mut out = Vec::new();
                for row in rows {
                    out.extend_from_slice(serde_json::to_string(row).expect("ser").as_bytes());
                    out.push(b'\n');
                }
                out
            })
            .collect();
        assert_equivalent(&pushes, &spec);
    }
}
