//! The merge strategies on DuckDB — delete_insert, upsert and scd2 — asserted
//! by their destination-visible outcomes rather than by the SQL that produced
//! them.
//!
//! The same options must yield the same behaviour and the same typed errors as
//! on postgres, because both destinations plan through one shared core; these
//! cells are how "same" is checked rather than assumed. Keyed STRUCTURED
//! streams are the shape strategies apply to, driven through the shared Arrow
//! helper in `tests/common`.

mod common;

use common::{Col, batch, bools, i64s, one_unit, strs};
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

fn kv(rows: &[(Option<i64>, Option<&str>)]) -> common::StructuredSource {
    let (ks, vs): (Vec<_>, Vec<_>) = rows.iter().cloned().unzip();
    one_unit("kv", &["k"], batch(&[("k", i64s(&ks)), ("v", strs(&vs))]))
}

fn kv_flag(rows: &[(Option<i64>, Option<&str>, Option<bool>)]) -> common::StructuredSource {
    let ks: Vec<_> = rows.iter().map(|r| r.0).collect();
    let vs: Vec<_> = rows.iter().map(|r| r.1).collect();
    let fs: Vec<_> = rows.iter().map(|r| r.2).collect();
    one_unit(
        "kv",
        &["k"],
        batch(&[("k", i64s(&ks)), ("v", strs(&vs)), ("gone", bools(&fs))]),
    )
}

async fn run<S: rdlt_connector::Source + 'static>(
    config: EngineConfig,
    source: S,
    dest: DuckDb,
) -> Result<rdlt_engine::RunReport, rdlt_engine::RdltError> {
    Engine::new(config, source, dest).run().await
}

/// M1: delete_insert (explicit) replaces matched keys; totals equal source
/// truth after redelivery-with-changes.
#[tokio::test(flavor = "multi_thread")]
async fn delete_insert_replaces_matched_keys() {
    let dir = tempfile::tempdir().unwrap();
    let dest = DuckDb::open(dir.path().join("di.duckdb"))
        .unwrap()
        .options(options(json!({"merge_strategy": "delete_insert"})))
        .unwrap();

    let first = kv(&[(Some(1), Some("one")), (Some(2), Some("two"))]);
    run(merge_config("di", &["k"]), first, dest.clone())
        .await
        .unwrap();

    let second = kv(&[(Some(2), Some("two-changed")), (Some(3), Some("three"))]);
    run(merge_config("di", &["k"]), second.at(1), dest.clone())
        .await
        .unwrap();

    assert_eq!(dest.count_rows("kv").unwrap(), 3);
    assert_eq!(
        dest.query_string("SELECT v FROM kv WHERE k = 2").unwrap(),
        "two-changed"
    );
}

/// M2/M4: upsert updates matched keys IN PLACE and composes with hard_delete
/// — flagged survivors delete their key, keep-side rows merge.
#[tokio::test(flavor = "multi_thread")]
async fn upsert_updates_in_place_and_composes_with_hard_delete() {
    let dir = tempfile::tempdir().unwrap();
    let dest = DuckDb::open(dir.path().join("ups.duckdb"))
        .unwrap()
        .options(options(json!({
            "merge_strategy": "upsert",
            "tables": {"kv": {"hard_delete": "gone"}}
        })))
        .unwrap();

    let first = kv_flag(&[
        (Some(1), Some("one"), Some(false)),
        (Some(2), Some("two"), Some(false)),
        (Some(3), Some("three"), Some(false)),
    ]);
    run(merge_config("ups", &["k"]), first, dest.clone())
        .await
        .unwrap();

    let second = kv_flag(&[
        (Some(1), Some("one-updated"), Some(false)),
        (Some(2), Some("ignored"), Some(true)), // flagged → key deleted
    ]);
    run(merge_config("ups", &["k"]), second.at(1), dest.clone())
        .await
        .unwrap();

    assert_eq!(dest.count_rows("kv").unwrap(), 2, "k=2 hard-deleted");
    assert_eq!(
        dest.query_string("SELECT v FROM kv WHERE k = 1").unwrap(),
        "one-updated",
        "matched key updates in place"
    );
    assert_eq!(
        dest.query_string("SELECT v FROM kv WHERE k = 3").unwrap(),
        "three",
        "untouched key keeps its row"
    );
}

/// M2/M7: upsert on a shredded (keyless) stream is a typed error — identical
/// wording to postgres (one shared validator serves both).
#[tokio::test(flavor = "multi_thread")]
async fn upsert_on_shredded_stream_is_typed() {
    let dir = tempfile::tempdir().unwrap();
    let dest = DuckDb::open(dir.path().join("shred.duckdb"))
        .unwrap()
        .options(options(json!({"merge_strategy": "upsert"})))
        .unwrap();
    // No primary key → shredded identity (_rdlt_id content hash).
    let source = MemorySource::single_stream(
        StreamSpec::new("docs"),
        vec![json!({"a": 1, "nested": {"b": 2}})],
    );
    let err = run(merge_config("shred", &["a"]), source, dest)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("upsert strategy requires a KEYED")
            && err.contains("shredded streams use delete_insert"),
        "{err}"
    );
}

/// 011 R5 on DuckDB: EXPLICIT merge_strategy under the append write mode is
/// a typed error; the unconfigured default never rejects.
#[tokio::test(flavor = "multi_thread")]
async fn explicit_strategy_under_append_is_typed() {
    let dir = tempfile::tempdir().unwrap();
    let dest = DuckDb::open(dir.path().join("append.duckdb"))
        .unwrap()
        .options(options(json!({"merge_strategy": "upsert"})))
        .unwrap();
    let source = kv(&[(Some(1), Some("x"))]);
    let err = run(EngineConfig::new("append"), source, dest)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("merge_strategy requires the merge write mode"),
        "{err}"
    );

    // The unconfigured default: append works untouched.
    let dest = DuckDb::open(dir.path().join("append-ok.duckdb")).unwrap();
    let source = kv(&[(Some(1), Some("x"))]);
    run(EngineConfig::new("append-ok"), source, dest.clone())
        .await
        .unwrap();
    assert_eq!(dest.count_rows("kv").unwrap(), 1);
}

/// S2/S3: scd2 closes the changed key's active version at the boundary and
/// opens a new one; unchanged keys are SKIPPED (no churn).
#[tokio::test(flavor = "multi_thread")]
async fn scd2_history_closes_and_opens() {
    let dir = tempfile::tempdir().unwrap();
    let dest = DuckDb::open(dir.path().join("scd2.duckdb"))
        .unwrap()
        .options(options(json!({
            "merge_strategy": "scd2",
            "tables": {"kv": {"scd2": {}}}
        })))
        .unwrap();

    let first = kv(&[(Some(1), Some("one")), (Some(2), Some("two"))]);
    run(merge_config("scd2", &["k"]), first, dest.clone())
        .await
        .unwrap();

    let second = kv(&[
        (Some(1), Some("one-v2")), // changed → close + open
        (Some(2), Some("two")),    // unchanged → skipped
    ]);
    run(merge_config("scd2", &["k"]), second.at(1), dest.clone())
        .await
        .unwrap();

    assert_eq!(dest.count_rows("kv").unwrap(), 3, "1 closed + 2 active");
    assert_eq!(
        dest.query_string("SELECT v FROM kv WHERE k = 1 AND _rdlt_valid_to IS NULL")
            .unwrap(),
        "one-v2",
        "the active version carries the new value"
    );
    assert_eq!(
        dest.query_string("SELECT v FROM kv WHERE k = 1 AND _rdlt_valid_to IS NOT NULL")
            .unwrap(),
        "one",
        "history keeps the closed version"
    );
    assert_eq!(
        dest.query_string("SELECT count(*)::VARCHAR FROM kv WHERE k = 2")
            .unwrap(),
        "1",
        "unchanged key has exactly its original row — no churn"
    );
}

/// S5 (the R5 probe carried into behavior): every scd2 row written in ONE
/// commit unit observes the SAME boundary instant.
#[tokio::test(flavor = "multi_thread")]
async fn scd2_boundary_is_one_instant_per_unit() {
    let dir = tempfile::tempdir().unwrap();
    let dest = DuckDb::open(dir.path().join("scd2b.duckdb"))
        .unwrap()
        .options(options(json!({
            "merge_strategy": "scd2",
            "tables": {"kv": {"scd2": {}}}
        })))
        .unwrap();
    let rows: Vec<(Option<i64>, Option<String>)> =
        (0..50).map(|i| (Some(i), Some(format!("v{i}")))).collect();
    let ks: Vec<_> = rows.iter().map(|r| r.0).collect();
    let vs: Vec<Option<&str>> = rows.iter().map(|r| r.1.as_deref()).collect();
    let source = one_unit(
        "kv",
        &["k"],
        batch(&[
            ("k", i64s(&ks)),
            (
                "v",
                Col::Str(vs.iter().map(|v| v.map(str::to_owned)).collect()),
            ),
        ]),
    );
    run(merge_config("scd2b", &["k"]), source, dest.clone())
        .await
        .unwrap();
    assert_eq!(
        dest.query_string("SELECT count(DISTINCT _rdlt_valid_from)::VARCHAR FROM kv")
            .unwrap(),
        "1",
        "one transaction-stable boundary per commit unit (S5)"
    );
}

/// S6: `absent: retire` retires active keys missing from a full feed.
#[tokio::test(flavor = "multi_thread")]
async fn scd2_absent_retire_full_feed() {
    let dir = tempfile::tempdir().unwrap();
    let dest = DuckDb::open(dir.path().join("retire.duckdb"))
        .unwrap()
        .options(options(json!({
            "merge_strategy": "scd2",
            "tables": {"kv": {"scd2": {"absent": "retire"}}}
        })))
        .unwrap();

    let first = kv(&[(Some(1), Some("one")), (Some(2), Some("two"))]);
    run(merge_config("retire", &["k"]), first, dest.clone())
        .await
        .unwrap();

    let second = kv(&[(Some(1), Some("one"))]); // k=2 absent
    run(merge_config("retire", &["k"]), second.at(1), dest.clone())
        .await
        .unwrap();

    assert_eq!(
        dest.query_string("SELECT count(*)::VARCHAR FROM kv WHERE _rdlt_valid_to IS NULL")
            .unwrap(),
        "1",
        "only the delivered key stays active"
    );
    assert_eq!(
        dest.query_string("SELECT k::VARCHAR FROM kv WHERE _rdlt_valid_to IS NOT NULL")
            .unwrap(),
        "2",
        "the absent key retired at the boundary"
    );
}

/// M3: upsert against a target with PRE-EXISTING duplicate keys is a typed
/// error naming the key columns (the unique index cannot be created).
#[tokio::test(flavor = "multi_thread")]
async fn upsert_over_preexisting_duplicates_is_typed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dups.duckdb");
    // Load duplicates via append first (no options).
    let dest = DuckDb::open(&path).unwrap();
    let source = kv(&[(Some(1), Some("a")), (Some(1), Some("b"))]);
    run(EngineConfig::new("seed-dups"), source, dest)
        .await
        .unwrap();

    // Now request upsert on the same table.
    let dest = DuckDb::open(&path)
        .unwrap()
        .options(options(json!({"merge_strategy": "upsert"})))
        .unwrap();
    let source = kv(&[(Some(2), Some("c"))]);
    let err = run(merge_config("dups", &["k"]), source, dest)
        .await
        .unwrap_err()
        .to_string();
    assert!(
        err.contains("cannot create the unique index") && err.contains("k"),
        "{err}"
    );
}

/// With a merge_scope, scd2 retirement is SCOPED —
/// absent keys retire only within delivered scopes; other scopes keep their
/// active versions untouched.
#[tokio::test(flavor = "multi_thread")]
async fn scd2_scoped_retirement_by_merge_scope() {
    let dir = tempfile::tempdir().unwrap();
    let dest = DuckDb::open(dir.path().join("scoped.duckdb"))
        .unwrap()
        .options(options(json!({
            "merge_strategy": "scd2",
            "tables": {"kv": {"scd2": {"absent": "retire"}, "merge_scope": ["day"]}}
        })))
        .unwrap();

    // day 1: k1,k2 · day 2: k3 — all active.
    let first = one_unit(
        "kv",
        &["k"],
        batch(&[
            ("k", i64s(&[Some(1), Some(2), Some(3)])),
            ("day", i64s(&[Some(1), Some(1), Some(2)])),
            ("v", strs(&[Some("k1"), Some("k2"), Some("k3")])),
        ]),
    );
    run(merge_config("scoped", &["k"]), first, dest.clone())
        .await
        .unwrap();

    // Redeliver ONLY day 1, without k2: k2 retires (absent in a delivered
    // scope); day 2's k3 stays ACTIVE (its scope was not delivered).
    let second = one_unit(
        "kv",
        &["k"],
        batch(&[
            ("k", i64s(&[Some(1)])),
            ("day", i64s(&[Some(1)])),
            ("v", strs(&[Some("k1")])),
        ]),
    );
    run(merge_config("scoped", &["k"]), second.at(1), dest.clone())
        .await
        .unwrap();

    assert_eq!(
        dest.query_string("SELECT k::VARCHAR FROM kv WHERE _rdlt_valid_to IS NOT NULL")
            .unwrap(),
        "2",
        "only the absent key IN the delivered scope retires"
    );
    assert_eq!(
        dest.query_string(
            "SELECT string_agg(k::VARCHAR, ',' ORDER BY k) FROM kv WHERE _rdlt_valid_to IS NULL"
        )
        .unwrap(),
        "1,3",
        "k3's undelivered scope keeps its active version — history intact"
    );
    // History preserved: no scope DELETE ran (scd2 never destroys rows).
    assert_eq!(dest.count_rows("kv").unwrap(), 3);
}

/// 013 G1 typed matrix: merge_scope with scd2 under `absent: keep` would be
/// silently inert — rejected naming the requirement.
#[tokio::test(flavor = "multi_thread")]
async fn scd2_merge_scope_requires_retire() {
    let err = DestOptions::from_value(json!({
        "merge_strategy": "scd2",
        "tables": {"kv": {"scd2": {}, "merge_scope": ["day"]}}
    }))
    .unwrap_err();
    assert!(
        err.contains("tables.kv.merge_scope") && err.contains("absent: retire"),
        "{err}"
    );
}

/// 013 G2: `active_record_timestamp` replaces NULL as the open marker, and
/// `boundary_timestamp` replaces the transaction timestamp.
#[tokio::test(flavor = "multi_thread")]
async fn scd2_active_marker_and_boundary_override() {
    let dir = tempfile::tempdir().unwrap();
    let dest = DuckDb::open(dir.path().join("marker.duckdb"))
        .unwrap()
        .options(options(json!({
            "merge_strategy": "scd2",
            "tables": {"kv": {"scd2": {
                "active_record_timestamp": "9999-12-31T00:00:00Z",
                "boundary_timestamp": "2026-07-22T00:00:00Z"
            }}}
        })))
        .unwrap();

    let first = kv(&[(Some(1), Some("one"))]);
    run(merge_config("marker", &["k"]), first, dest.clone())
        .await
        .unwrap();
    let second = kv(&[(Some(1), Some("one-v2"))]);
    run(merge_config("marker", &["k"]), second.at(1), dest.clone())
        .await
        .unwrap();

    // Active row carries the MARKER, not NULL.
    assert_eq!(
        dest.query_string("SELECT v FROM kv WHERE _rdlt_valid_to = TIMESTAMPTZ '9999-12-31'")
            .unwrap(),
        "one-v2"
    );
    assert_eq!(
        dest.query_string("SELECT count(*)::VARCHAR FROM kv WHERE _rdlt_valid_to IS NULL")
            .unwrap(),
        "0",
        "no NULL markers when active_record_timestamp is set"
    );
    // Closed row closed AT the caller-supplied boundary.
    assert_eq!(
        dest.query_string(
            "SELECT v FROM kv WHERE _rdlt_valid_to = TIMESTAMPTZ '2026-07-22T00:00:00Z'"
        )
        .unwrap(),
        "one"
    );
    // Typed: garbage timestamps never reach SQL.
    let err = DestOptions::from_value(json!({
        "tables": {"kv": {"merge_strategy": "scd2",
                           "scd2": {"boundary_timestamp": "'); DROP TABLE kv; --"}}}
    }))
    .unwrap_err();
    assert!(
        err.contains("boundary_timestamp") && err.contains("not a"),
        "{err}"
    );
}
