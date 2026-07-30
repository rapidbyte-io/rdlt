//! Byte-exact golden pin for every emitted `_rdlt_id` (feature 019 US6, PI4).
//!
//! `_rdlt_id` is PERSISTED. A destination that merges by identity matches
//! yesterday's rows against today's ids, so a shift does not corrupt loudly —
//! it silently stops matching, and every row looks new.
//!
//! Before this file the repository had **no oracle for that**. There was not a
//! single literal 64-hex identity checked in anywhere, and the closest thing —
//! the `shred_property` binary's referential-integrity check — provably cannot catch a
//! shift: child ids are derived from the root id via `child_row_id`, so when a
//! root id moves every child moves consistently and referential integrity
//! still holds. The whole suite stays green while every persisted id changes.
//!
//! So this listing is captured verbatim from the shipping build and compared
//! literally. It is not a behaviour description; it is the bytes.
//!
//! Regenerate deliberately, never reflexively — a diff here means ids that
//! already exist in users' warehouses no longer match what rdlt now emits:
//!
//! ```text
//! RDLT_REPIN=1 cargo nextest run -p rdlt-engine --test integration -E 'test(shred_identity_pin)'
//! ```
//!
//! # Why these cases
//!
//! Each entry pins a rule that is load-bearing and currently undefended.
//! Several were found by adversarially trying to break the planned changes,
//! and each one is a rule a plausible "cleanup" would violate:
//!
//! - **float rendering** — the keyed path renders floats with Rust's `Display`
//!   and the keyless path through serde_json. They disagree (`1.0` → `1` vs
//!   `1.0`; `1e16` → `10000000000000000` vs `1e16`). Swapping either for the
//!   other, or for `{:?}`, or for a faster float formatter, rewrites ids.
//! - **null / absent / object / array all collapse to one keyed value** and an
//!   empty string does NOT. Rendering an absent key as `""` rewrites every id
//!   for rows missing a key.
//! - **composite keys** — a shared scratch buffer that is cleared once per row
//!   instead of once per key field concatenates field two onto field one. Only
//!   a multi-field key catches it, and only when both fields render through
//!   the buffer.
//! - **key order in `primary_key` is load-bearing**, while the Merge-key
//!   validation in `runtime/validate.rs` compares order-insensitively — so nothing
//!   else in the engine defends it.
//! - **null slots in a child list consume positions** — `enumerate` runs over
//!   the full sequence and the null skip happens after it. Any
//!   `filter().enumerate()` renumbers every sibling.
//! - **two source keys that normalize to one child table** must stay separate
//!   entries in observation order, or their position sequences concatenate.
//!
//! The companion CROSS-VIEW property — that a document hashes identically
//! whether it arrived as a `serde_json::Value` or as bytes in the arena —
//! lives in `src/shred/table.rs`, because the arena view is crate-private.

use std::{collections::BTreeMap, fmt::Write as _, path::PathBuf};

use rdlt_connector::{DestinationCapabilities, StreamSpec};
use rdlt_core::schema::system_columns;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::{MemoryDestination, MemorySource};
use serde_json::{Value, json};

/// One corpus entry: what to shred, and under which stream shape.
struct Case {
    name: &'static str,
    /// `None` exercises the KEYLESS arm (content hash over canonical JSON);
    /// `Some` exercises the KEYED arm (per-field rendered text). They hash
    /// through different code and must both be pinned.
    primary_key: Option<Vec<&'static str>>,
    /// `false` shreds scalar lists into child tables with a `{"value": …}`
    /// wrapper; `true` keeps them as native lists.
    scalar_lists: bool,
    rows: Vec<Value>,
}

fn case(name: &'static str, rows: Vec<Value>) -> Case {
    Case {
        name,
        primary_key: None,
        scalar_lists: true,
        rows,
    }
}

fn keyed(name: &'static str, key: &[&'static str], rows: Vec<Value>) -> Case {
    Case {
        name,
        primary_key: Some(key.to_vec()),
        scalar_lists: true,
        rows,
    }
}

fn corpus() -> Vec<Case> {
    vec![
        // ---- the keyless arm: canonical JSON bytes ----
        case(
            "keyless_scalars",
            vec![
                json!({"v": 10.0}),
                json!({"v": 10}),
                json!({"v": "10"}),
                json!({"v": -0.0}),
                json!({"v": 0}),
                json!({"v": true}),
                json!({"v": null}),
                json!({"v": ""}),
            ],
        ),
        // Floats whose two renderings differ. If these ids ever equal their
        // integer siblings above, a float formatter was swapped.
        case(
            "keyless_float_edges",
            vec![
                json!({"v": 1e16}),
                json!({"v": 1e-6}),
                json!({"v": 1e300}),
                json!({"v": 1.5}),
                json!({"v": -1.5}),
            ],
        ),
        // Integers either side of 2^53 and above i64::MAX — the Int/UInt
        // reconstruction in canonical rendering.
        case(
            "keyless_int_boundaries",
            vec![
                json!({"v": 9007199254740993i64}),
                json!({"v": -9007199254740993i64}),
                json!({"v": 18446744073709551615u64}),
                json!({"v": i64::MIN}),
            ],
        ),
        // The canonical form sorts keys, so source order must not matter.
        // These two rows must share an id.
        case(
            "keyless_key_order_invariance",
            vec![json!({"b": 1, "a": 2}), json!({"a": 2, "b": 1})],
        ),
        // Last value wins, first position kept.
        case(
            "keyless_duplicate_keys",
            vec![json!({"dup": 1, "other": 2, "dup": 3})],
        ),
        // Non-object roots are wrapped as `{"value": …}` before hashing.
        case(
            "keyless_bare_roots",
            vec![json!(42), json!("str"), json!([1, 2]), json!([]), json!({})],
        ),
        case(
            "keyless_nested_objects",
            vec![
                json!({"a": {"b": {"c": 1}}}),
                json!({"a": {"b": {"c": 1}}, "d": null}),
                json!({"a": {}}),
            ],
        ),
        // ---- the keyed arm: per-field rendered text ----
        // Absent, explicit null, object and array all render as the SAME
        // keyed value; an empty string does not.
        keyed(
            "keyed_null_absent_and_empty",
            &["k"],
            vec![
                json!({"k": null, "tag": "explicit-null"}),
                json!({"tag": "absent"}),
                json!({"k": {}, "tag": "object"}),
                json!({"k": [], "tag": "array"}),
                json!({"k": "", "tag": "empty-string"}),
            ],
        ),
        // `1` and `1.0` share a keyed id (Rust `Display` renders 1.0 as "1")
        // but NOT a keyless one. Both facts are pinned.
        keyed(
            "keyed_float_edges",
            &["k"],
            vec![
                json!({"k": 10.0}),
                json!({"k": 10}),
                json!({"k": "10"}),
                json!({"k": -0.0}),
                json!({"k": 1e16}),
                json!({"k": 1e300}),
                json!({"k": true}),
            ],
        ),
        // Composite key: the ONLY shape that catches a scratch buffer cleared
        // once per row instead of once per field. Both fields must render
        // through the buffer, so both are numeric.
        keyed(
            "keyed_composite",
            &["a", "b"],
            vec![
                json!({"a": 1, "b": 2}),
                json!({"a": 12, "b": 3}),
                json!({"a": 123, "b": null}),
                json!({"a": 1, "b": "2"}),
            ],
        ),
        // Declared key ORDER changes the id. Same documents as above.
        keyed(
            "keyed_composite_reversed",
            &["b", "a"],
            vec![json!({"a": 1, "b": 2}), json!({"a": 12, "b": 3})],
        ),
        // A repeated key field is not deduplicated.
        keyed("keyed_duplicate_field", &["a", "a"], vec![json!({"a": 1})]),
        // ---- children ----
        // Null items still consume positions: the surviving children are at
        // 1 and 3, not 0 and 1.
        case(
            "child_null_slots_consume_positions",
            vec![json!({"tags": [null, {"x": 1}, null, {"y": 2}, null]})],
        ),
        // Two identical scalar items at different positions must differ,
        // because position feeds the child id.
        Case {
            name: "child_scalar_wrapper",
            primary_key: None,
            scalar_lists: false,
            rows: vec![json!({"tags": ["a", 1, true, null, "a"]})],
        },
        // Two source keys that NORMALIZE to the same child table must stay
        // separate observation entries: merging them would concatenate their
        // position sequences, so both children would not be at position 0.
        case(
            "child_normalized_collision",
            vec![json!({"a-b": [{"x": 1}], "a b": [{"x": 2}]})],
        ),
        // A nested child table and a flat one whose names alias.
        case(
            "child_cross_depth_alias",
            vec![json!({"a": [{"b": [{"z": 1}]}], "a__b": [{"z": 2}]})],
        ),
        // An empty list between two non-empty ones, in one push.
        case(
            "child_empty_between_nonempty",
            vec![
                json!({"tags": [{"x": 1}]}),
                json!({"tags": []}),
                json!({"tags": [{"x": 2}]}),
            ],
        ),
        // Empty list FIRST, so the child table is established by an empty one.
        case(
            "child_empty_first",
            vec![json!({"p": [], "q": [{"x": 1}]}), json!({"p": [{"y": 1}]})],
        ),
        case(
            "child_at_depth",
            vec![json!({"lvl1": [{"lvl2": [{"lvl3": [{"v": 1}]}]}]})],
        ),
        // The same two documents in both orders: each document's ids must not
        // depend on what was shredded before it in the same push.
        case(
            "child_order_independence_ab",
            vec![json!({"tags": [{"x": 1}]}), json!({"tags": [{"y": 2}]})],
        ),
        case(
            "child_order_independence_ba",
            vec![json!({"tags": [{"y": 2}]}), json!({"tags": [{"x": 1}]})],
        ),
        // A child list under a KEYED root: the root id comes from the key, the
        // children derive from it.
        keyed(
            "child_under_keyed_root",
            &["id"],
            vec![json!({"id": 7, "tags": [{"x": 1}, {"x": 2}]})],
        ),
        // Empty-string key and an escaped duplicate of a plain key.
        case(
            "child_pathological_keys",
            vec![json!({"": [{"x": 1}], "tags": [{"y": 1}]})],
        ),
    ]
}

async fn run(case: &Case) -> MemoryDestination {
    let mut spec = StreamSpec::new("s");
    if let Some(key) = &case.primary_key {
        spec = spec.with_primary_key(key.iter().copied());
    }
    let source = MemorySource::single_stream(spec, case.rows.clone());
    let dest = MemoryDestination::new().with_capabilities(DestinationCapabilities {
        merge: true,
        structs: true,
        scalar_lists: case.scalar_lists,
        json_type: true,
        decimal: true,
        ident_rules: Default::default(),
    });
    Engine::new(EngineConfig::new("identity-pin"), source, dest.clone())
        .run()
        .await
        .unwrap_or_else(|e| panic!("case `{}` must shred: {e}", case.name));
    dest
}

/// Render the identity-bearing system columns of every landed row.
///
/// `_rdlt_load_id` is deliberately excluded: it names the run and changes on
/// every execution. Everything else here is a function of the input alone.
fn render(case: &Case, dest: &MemoryDestination) -> String {
    let mut out = String::new();
    let key = match &case.primary_key {
        Some(k) => format!("primary_key={k:?}"),
        None => "primary_key=none".to_string(),
    };
    let _ = writeln!(
        out,
        "## {} [{key}, scalar_lists={}]",
        case.name, case.scalar_lists
    );

    let snapshot: BTreeMap<String, Vec<serde_json::Map<String, Value>>> = dest
        .snapshot()
        .into_iter()
        .map(|(table, rows)| (table.as_str().to_owned(), rows))
        .collect();

    for (table, rows) in snapshot {
        let _ = writeln!(out, "  table {table} ({} rows)", rows.len());
        for (idx, row) in rows.iter().enumerate() {
            let field = |name: &str| match row.get(name) {
                None | Some(Value::Null) => "-".to_string(),
                Some(Value::String(s)) => s.clone(),
                Some(other) => other.to_string(),
            };
            let _ = writeln!(
                out,
                "    [{idx}] id={} parent={} root={} pos={}",
                field(system_columns::ID),
                field(system_columns::PARENT_ID),
                field(system_columns::ROOT_ID),
                field(system_columns::POS),
            );
        }
    }
    out
}

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/shred_identities.txt")
}

#[tokio::test(flavor = "multi_thread")]
async fn emitted_identities_match_the_pin() {
    let mut actual = String::from(
        "# Emitted shred identities, verbatim.\n\
         # Generated by tests/shred_identity_pin.rs — read that file before editing.\n\
         # A diff here means ids already in users' warehouses no longer match.\n\n",
    );
    for case in corpus() {
        let dest = run(&case).await;
        actual.push_str(&render(&case, &dest));
        actual.push('\n');
    }

    let path = fixture();
    if std::env::var_os("RDLT_REPIN").is_some() {
        std::fs::create_dir_all(path.parent().expect("fixtures dir")).expect("create fixtures dir");
        std::fs::write(&path, &actual).expect("write pin");
        return;
    }

    let expected = std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "missing shred identity pin at {}: {e}\n\
             regenerate with RDLT_REPIN=1 and review every line",
            path.display()
        )
    });
    if actual != expected {
        let diff = expected
            .lines()
            .zip(actual.lines())
            .enumerate()
            .find(|(_, (e, a))| e != a)
            .map(|(n, (e, a))| format!("\n  line {n}\n  pinned: {e}\n  now:    {a}"))
            .unwrap_or_else(|| "\n  (line count differs)".to_string());
        panic!(
            "emitted identities changed — persisted `_rdlt_id` values are not what \
             was pinned:{diff}\n\
             if this is intended, re-pin with RDLT_REPIN=1 and say in the change \
             description why existing warehouses may stop matching"
        );
    }
}

/// The pin is only as good as its coverage. If a case is dropped from the
/// corpus the listing shrinks and the comparison still passes on the remaining
/// text — so the case list itself is pinned.
#[tokio::test(flavor = "multi_thread")]
async fn the_corpus_covers_every_named_hazard() {
    let names: Vec<&str> = corpus().iter().map(|c| c.name).collect();
    let expected = [
        "keyless_scalars",
        "keyless_float_edges",
        "keyless_int_boundaries",
        "keyless_key_order_invariance",
        "keyless_duplicate_keys",
        "keyless_bare_roots",
        "keyless_nested_objects",
        "keyed_null_absent_and_empty",
        "keyed_float_edges",
        "keyed_composite",
        "keyed_composite_reversed",
        "keyed_duplicate_field",
        "child_null_slots_consume_positions",
        "child_scalar_wrapper",
        "child_normalized_collision",
        "child_cross_depth_alias",
        "child_empty_between_nonempty",
        "child_empty_first",
        "child_at_depth",
        "child_order_independence_ab",
        "child_order_independence_ba",
        "child_under_keyed_root",
        "child_pathological_keys",
    ];
    assert_eq!(names, expected, "identity corpus coverage changed");
    // Both arms must be represented — they hash through different code.
    assert!(corpus().iter().any(|c| c.primary_key.is_some()));
    assert!(corpus().iter().any(|c| c.primary_key.is_none()));
    assert!(corpus().iter().any(|c| !c.scalar_lists));
}
