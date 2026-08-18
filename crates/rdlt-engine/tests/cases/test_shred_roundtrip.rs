//! Shredder round-trip properties.
//!
//! 1. **Determinism**: shredding the same input twice produces byte-identical
//!    destination content (modulo `_rdlt_load_id`, which names the run).
//! 2. **Row conservation**: every input row lands exactly once in the root table.
//! 3. **Scalar fidelity**: homogeneous scalar columns survive untouched.

use proptest::prelude::*;
use rdlt_engine::config::Config;
use rdlt_engine::engine::Engine;
use rdlt_testkit::memory;
use serde_json::{Value, json};

use super::common::without_load_id;

/// Arbitrary JSON values, depth-limited, with object keys that exercise
/// normalization.
fn any_json(depth: u32) -> BoxedStrategy<Value> {
    let scalar = prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::from),
        any::<i64>().prop_map(Value::from),
        (-1.0e12f64..1.0e12).prop_map(Value::from),
        "[a-zA-Z0-9 _-]{0,12}".prop_map(Value::from),
    ];
    if depth == 0 {
        scalar.boxed()
    } else {
        prop_oneof![
            4 => scalar,
            1 => proptest::collection::vec(any_json(depth - 1), 0..4)
                .prop_map(Value::from),
            2 => proptest::collection::btree_map("[a-zA-Z][a-zA-Z0-9_ ]{0,8}", any_json(depth - 1), 0..5)
                .prop_map(|m| Value::Object(m.into_iter().collect())),
        ]
        .boxed()
    }
}

fn rows_strategy() -> impl Strategy<Value = Vec<Value>> {
    proptest::collection::vec(
        proptest::collection::btree_map("[a-zA-Z][a-zA-Z0-9_]{0,8}", any_json(2), 1..6)
            .prop_map(|m| Value::Object(m.into_iter().collect())),
        1..8,
    )
}

async fn run_once(rows: Vec<Value>) -> memory::Destination {
    let source = memory::Source::single_stream(rdlt_connector::source::StreamSpec::new("t"), rows);
    let dest = memory::Destination::new();
    Engine::new(Config::new("roundtrip"), source, dest.clone())
        .run()
        .await
        .expect("shredding arbitrary JSON objects must never fail");
    dest
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    #[test]
    fn shred_is_deterministic_and_conserves_rows(rows in rows_strategy()) {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("runtime");
        let row_count = rows.len();
        let (a, b) = runtime.block_on(async {
            let a = run_once(rows.clone()).await;
            let b = run_once(rows).await;
            (a, b)
        });
        // Determinism: identical input → identical destination content.
        prop_assert_eq!(without_load_id(&a), without_load_id(&b));
        // Conservation: every input row exactly once in the root table.
        prop_assert_eq!(a.committed_rows("t").len(), row_count);
    }
}

#[tokio::test]
async fn homogeneous_scalars_round_trip_exactly() {
    let rows = vec![
        json!({"i": 42, "f": 2.5, "s": "hello", "b": true, "n": null}),
        json!({"i": -7, "f": 0.125, "s": "", "b": false, "n": null}),
    ];
    let dest = run_once(rows.clone()).await;
    let out = dest.committed_rows("t");
    assert_eq!(out.len(), 2);
    for (input, output) in rows.iter().zip(&out) {
        for key in ["i", "f", "s", "b"] {
            assert_eq!(
                &output[key], &input[key],
                "column {key} must survive untouched"
            );
        }
    }
    // An all-null column carries no information yet: it materializes only once a
    // value arrives (same behavior as dlt).
    assert!(dest.schema("t").expect("schema").column("n").is_none());
}
