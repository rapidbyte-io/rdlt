//! Feature 013 US1 (T008): the 010 merge refinements on DuckDB — dedup_sort
//! ordered survivor selection and merge_key scope replacement, mirroring the
//! postgres cells' claims (contracts MR1–MR6 through the shared core).

mod common;

use common::{StructuredSource, batch, i64s, one_unit, strs};
use rdlt_connector::{StreamSpec, WriteMode};
use rdlt_connector_duckdb::dest::{DestOptions, DuckDb};
use rdlt_engine::{Engine, EngineConfig};
use rdlt_testkit::MemorySource;
use serde_json::json;

fn merge_config(name: &str, key: &[&str]) -> EngineConfig {
    let mut config = EngineConfig::new(name);
    config.write_mode = WriteMode::Merge {
        key: key.iter().map(|k| k.to_string()).collect(),
    };
    config
}

fn options(value: serde_json::Value) -> DestOptions {
    DestOptions::from_value(value).expect("valid options")
}

async fn run<S: rdlt_connector::Source + 'static>(
    config: EngineConfig,
    source: S,
    dest: DuckDb,
) -> Result<rdlt_engine::RunReport, rdlt_engine::RdltError> {
    Engine::new(config, source, dest).run().await
}

/// (k, seq, v) rows.
fn kseq(rows: &[(Option<i64>, Option<i64>, Option<&str>)]) -> StructuredSource {
    let ks: Vec<_> = rows.iter().map(|r| r.0).collect();
    let seqs: Vec<_> = rows.iter().map(|r| r.1).collect();
    let vs: Vec<_> = rows.iter().map(|r| r.2).collect();
    one_unit(
        "kv",
        &["k"],
        batch(&[("k", i64s(&ks)), ("seq", i64s(&seqs)), ("v", strs(&vs))]),
    )
}

/// (k, day, v) rows.
fn kday(rows: &[(Option<i64>, Option<i64>, Option<&str>)]) -> StructuredSource {
    let ks: Vec<_> = rows.iter().map(|r| r.0).collect();
    let days: Vec<_> = rows.iter().map(|r| r.1).collect();
    let vs: Vec<_> = rows.iter().map(|r| r.2).collect();
    one_unit(
        "kv",
        &["k"],
        batch(&[("k", i64s(&ks)), ("day", i64s(&days)), ("v", strs(&vs))]),
    )
}

/// MR1/MR2: dedup_sort picks the ordered survivor within one load — values
/// beat NULL, ties keep deterministic arrival last-wins.
#[tokio::test(flavor = "multi_thread")]
async fn dedup_sort_orders_survivors_values_beat_null() {
    let dir = tempfile::tempdir().unwrap();
    let dest = DuckDb::open(dir.path().join("dedup.duckdb"))
        .unwrap()
        .options(options(json!({
            "tables": {"kv": {"dedup_sort": {"column": "seq", "order": "desc"}}}
        })))
        .unwrap();
    let source = kseq(&[
        // k=1: greatest seq survives regardless of arrival order.
        (Some(1), Some(30), Some("winner")),
        (Some(1), Some(10), Some("early")),
        // k=2: the valued row beats the NULL row even arriving first.
        (Some(2), Some(5), Some("valued")),
        (Some(2), None, Some("null-late")),
        // k=3: all-NULL group → arrival last-wins.
        (Some(3), None, Some("first")),
        (Some(3), None, Some("last-wins")),
    ]);
    run(merge_config("dedup", &["k"]), source, dest.clone())
        .await
        .unwrap();

    assert_eq!(dest.count_rows("kv").unwrap(), 3);
    for (k, expect) in [(1, "winner"), (2, "valued"), (3, "last-wins")] {
        assert_eq!(
            dest.query_string(&format!("SELECT v FROM kv WHERE k = {k}"))
                .unwrap(),
            expect,
            "k={k}"
        );
    }
}

/// MR3/MR4: merge_key replaces DELIVERED scopes wholesale, leaves other
/// scopes untouched, and NULL is not a scope.
#[tokio::test(flavor = "multi_thread")]
async fn merge_key_scope_replacement() {
    let dir = tempfile::tempdir().unwrap();
    let dest = DuckDb::open(dir.path().join("scope.duckdb"))
        .unwrap()
        .options(options(json!({
            "tables": {"kv": {"merge_key": ["day"]}}
        })))
        .unwrap();

    let first = kday(&[
        (Some(1), Some(1), Some("d1-a")),
        (Some(2), Some(1), Some("d1-b")),
        (Some(3), Some(2), Some("d2-a")),
        (Some(4), None, Some("no-scope")),
    ]);
    run(merge_config("scope", &["k"]), first, dest.clone())
        .await
        .unwrap();
    assert_eq!(dest.count_rows("kv").unwrap(), 4);

    // Redeliver ONLY day 1, smaller: k=2 disappears (scope truth), day 2 and
    // the NULL-scope row stay.
    let second = kday(&[(Some(1), Some(1), Some("d1-a2"))]);
    run(merge_config("scope", &["k"]), second, dest.clone())
        .await
        .unwrap();

    assert_eq!(dest.count_rows("kv").unwrap(), 3);
    assert_eq!(
        dest.query_string("SELECT v FROM kv WHERE k = 1").unwrap(),
        "d1-a2"
    );
    assert_eq!(
        dest.query_string("SELECT count(*)::VARCHAR FROM kv WHERE k = 2")
            .unwrap(),
        "0",
        "undelivered row in the delivered scope disappears"
    );
    assert_eq!(
        dest.query_string("SELECT v FROM kv WHERE k = 3").unwrap(),
        "d2-a",
        "undelivered scope untouched"
    );
    assert_eq!(
        dest.query_string("SELECT v FROM kv WHERE k = 4").unwrap(),
        "no-scope",
        "NULL is not a scope (MR4)"
    );
}

/// MR5 (shared single-unit rule): a scoped table whose feed spans TWO commit
/// units is a typed error on the second unit — same message as postgres.
#[tokio::test(flavor = "multi_thread")]
async fn merge_key_split_feed_is_typed_on_the_second_unit() {
    let dir = tempfile::tempdir().unwrap();
    let dest = DuckDb::open(dir.path().join("unit.duckdb"))
        .unwrap()
        .options(options(json!({
            "tables": {"kv": {"merge_key": ["day"]}}
        })))
        .unwrap();
    // Two batches, each with its own checkpoint → two commit units, both
    // delivering kv rows.
    let source = StructuredSource {
        stream: "kv",
        key: &["k"],
        units: vec![
            batch(&[
                ("k", i64s(&[Some(1)])),
                ("day", i64s(&[Some(1)])),
                ("v", strs(&[Some("a")])),
            ]),
            batch(&[
                ("k", i64s(&[Some(2)])),
                ("day", i64s(&[Some(1)])),
                ("v", strs(&[Some("b")])),
            ]),
        ],
    };
    let err = run(merge_config("unit", &["k"]), source, dest)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("merge_key scope replacement")
            && err.contains("SINGLE commit unit")
            && err.contains("merge-refinements.md MR5"),
        "{err}"
    );
}

/// MR6 typed matrix (shared validator — identical wording to postgres):
/// keyless dedup, missing columns, key-constant dedup.
#[tokio::test(flavor = "multi_thread")]
async fn refinement_option_misuse_is_typed() {
    let dir = tempfile::tempdir().unwrap();

    // dedup_sort on a shredded (keyless) stream.
    let dest = DuckDb::open(dir.path().join("t1.duckdb"))
        .unwrap()
        .options(options(json!({
            "tables": {"docs": {"dedup_sort": {"column": "seq", "order": "desc"}}}
        })))
        .unwrap();
    let source = MemorySource::single_stream(
        StreamSpec::new("docs"),
        vec![json!({"seq": 1, "nested": {"a": 1}})],
    );
    let err = run(merge_config("t1", &["seq"]), source, dest)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("dedup_sort requires a KEYED"), "{err}");

    // merge_key column that is not a column of the table.
    let dest = DuckDb::open(dir.path().join("t2.duckdb"))
        .unwrap()
        .options(options(json!({
            "tables": {"kv": {"merge_key": ["ghost"]}}
        })))
        .unwrap();
    let source = kday(&[(Some(1), Some(1), Some("x"))]);
    let err = run(merge_config("t2", &["k"]), source, dest)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("merge_key column `ghost` is not a column"),
        "{err}"
    );

    // dedup_sort column inside the merge key: constant per group.
    let dest = DuckDb::open(dir.path().join("t3.duckdb"))
        .unwrap()
        .options(options(json!({
            "tables": {"kv": {"dedup_sort": {"column": "k", "order": "desc"}}}
        })))
        .unwrap();
    let source = kday(&[(Some(1), Some(1), Some("x"))]);
    let err = run(merge_config("t3", &["k"]), source, dest)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("part of the merge key"), "{err}");
}

/// M4 flag typing on DuckDB: a NON-boolean hard_delete column deletes on
/// IS NOT NULL (the postgres param-matrix twin).
#[tokio::test(flavor = "multi_thread")]
async fn non_bool_hard_delete_flag_uses_is_not_null() {
    let dir = tempfile::tempdir().unwrap();
    let dest = DuckDb::open(dir.path().join("flag.duckdb"))
        .unwrap()
        .options(options(json!({
            "tables": {"kv": {"hard_delete": "deleted_at"}}
        })))
        .unwrap();
    // (k, v, deleted_at) — deleted_at as text: NULL = keep, value = kill.
    let first = one_unit(
        "kv",
        &["k"],
        batch(&[
            ("k", i64s(&[Some(1), Some(2)])),
            ("v", strs(&[Some("keep"), Some("kill")])),
            ("deleted_at", strs(&[None, None])),
        ]),
    );
    run(merge_config("flag", &["k"]), first, dest.clone())
        .await
        .unwrap();

    let second = one_unit(
        "kv",
        &["k"],
        batch(&[
            ("k", i64s(&[Some(2)])),
            ("v", strs(&[Some("kill")])),
            ("deleted_at", strs(&[Some("2026-07-22")])),
        ]),
    );
    run(merge_config("flag", &["k"]), second, dest.clone())
        .await
        .unwrap();

    assert_eq!(dest.count_rows("kv").unwrap(), 1);
    assert_eq!(dest.query_string("SELECT v FROM kv").unwrap(), "keep");
}
