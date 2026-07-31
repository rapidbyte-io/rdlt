//! The cross-destination differential oracle: identical feeds run through the
//! postgres destination (a live container) and the DuckDB destination (a temp
//! file), and the two must agree on canonical per-table rows AND on typed-error
//! classes.
//!
//! This is the strongest check the suite has on the shared merge core. Each
//! destination's own cells can only prove it is self-consistent; only running
//! both against one feed can catch the two dialects quietly diverging.
//!
//! Type-affinity table — the ONLY sanctioned representation differences, which
//! the canonicalizer below normalizes exactly:
//!
//! | Logical    | postgres        | DuckDB          | canonical form        |
//! |---|---|---|---|
//! | Int64      | BIGINT          | BIGINT          | decimal string        |
//! | Utf8       | TEXT            | VARCHAR         | verbatim              |
//! | Bool       | BOOLEAN (t/f)   | BOOLEAN (true/false) | "true"/"false"   |
//! | NULL       | NULL            | NULL            | "∅"                   |
//!
//! Needs a container runtime; the same container-optional discipline as the
//! postgres live suites. Lives in THIS crate (not the facade) so the suite
//! runs under default workspace nextest — the facade's empty default
//! features would have silently skipped it (plan R7 amendment, recorded).

use std::sync::Arc;

use arrow_array::{ArrayRef, BooleanArray, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use async_trait::async_trait;
use rdlt_connector::WriteMode;
use rdlt_connector::core::Cursor;
use rdlt_connector::{ConnectorSpec, ReadRequest, Source, SourceError, StreamSpec};
use rdlt_connector_duckdb::dest::DuckDb;
use rdlt_connector_postgres::dest::Postgres;
use rdlt_connector_sqlcore::DestOptions;
use rdlt_engine::{Engine, EngineConfig};

// ---- shared structured feed ------------------------------------------------

#[derive(Clone)]
struct Feed {
    stream: &'static str,
    key: &'static [&'static str],
    units: Vec<RecordBatch>,
}

#[async_trait]
impl Source for Feed {
    fn spec(&self) -> ConnectorSpec {
        ConnectorSpec::new("differential-feed", "0.0.0")
    }
    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(vec![
            StreamSpec::new(self.stream)
                .with_structured()
                .with_primary_key(self.key.iter().copied()),
        ])
    }
    async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
        for (i, unit) in self.units.iter().enumerate() {
            let _ = req.out.arrow(unit.clone()).await;
            let _ = req.out.checkpoint(Cursor::new(i as u64 + 1)).await;
        }
        Ok(())
    }
}

fn batch(
    cols: &[(&str, Vec<Option<i64>>)],
    strs_: &[(&str, Vec<Option<&str>>)],
    bools_: &[(&str, Vec<Option<bool>>)],
) -> RecordBatch {
    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for (name, v) in cols {
        fields.push(Field::new(*name, DataType::Int64, true));
        arrays.push(Arc::new(Int64Array::from(v.clone())));
    }
    for (name, v) in strs_ {
        fields.push(Field::new(*name, DataType::Utf8, true));
        arrays.push(Arc::new(StringArray::from(v.clone())));
    }
    for (name, v) in bools_ {
        fields.push(Field::new(*name, DataType::Boolean, true));
        arrays.push(Arc::new(BooleanArray::from(v.clone())));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("batch")
}

// ---- both-destinations driver ----------------------------------------------

/// A canonical column: a bare identifier (quoted + normalized per engine) or
/// a raw SQL EXPRESSION (valid on both engines — used for derived facts like
/// history openness, 013 review finding 8).
fn is_bare_ident(c: &str) -> bool {
    !c.is_empty() && c.chars().all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

/// Canonical rows: every column cast to text, NULL → "∅", bools normalized,
/// ordered by the canonical text of the row itself (total order).
async fn canon_pg(conn: &str, schema: &str, table: &str, cols: &[&str]) -> Vec<Vec<String>> {
    let (client, connection) = tokio_postgres::connect(conn, tokio_postgres::NoTls)
        .await
        .expect("pg connect");
    tokio::spawn(async move { connection.await.ok() });
    let select = cols
        .iter()
        .map(|c| {
            if is_bare_ident(c) {
                format!(
                    "CASE WHEN \"{c}\" IS NULL THEN '∅' \
                     ELSE CASE pg_typeof(\"{c}\")::text WHEN 'boolean' \
                     THEN (CASE WHEN \"{c}\"::text = 't' THEN 'true' ELSE 'false' END) \
                     ELSE \"{c}\"::text END END"
                )
            } else {
                format!("coalesce(({c})::text, '∅')")
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    let rows = client
        .query(
            &format!("SELECT {select} FROM \"{schema}\".\"{table}\""),
            &[],
        )
        .await
        .expect("pg canon query");
    let mut out: Vec<Vec<String>> = rows
        .iter()
        .map(|r| (0..cols.len()).map(|i| r.get::<_, String>(i)).collect())
        .collect();
    out.sort();
    out
}

fn canon_duck(dest: &DuckDb, table: &str, cols: &[&str]) -> Vec<Vec<String>> {
    let select = cols
        .iter()
        .map(|c| {
            if is_bare_ident(c) {
                format!("coalesce(CAST(\"{c}\" AS VARCHAR), '∅')")
            } else {
                format!("coalesce(CAST(({c}) AS VARCHAR), '∅')")
            }
        })
        .collect::<Vec<_>>()
        .join(" || '\u{1}' || ");
    let joined = dest
        .query_string(&format!(
            "SELECT coalesce(string_agg(row, '\u{2}' ORDER BY row), '') FROM \
             (SELECT {select} AS row FROM \"{table}\") t"
        ))
        .expect("duck canon query");
    let mut out: Vec<Vec<String>> = joined
        .split('\u{2}')
        .filter(|r| !r.is_empty())
        .map(|r| r.split('\u{1}').map(str::to_owned).collect())
        .collect();
    out.sort();
    out
}

struct Outcome {
    /// Ok: canonical rows; Err: the typed error text.
    result: Result<Vec<Vec<String>>, String>,
}

/// Run the same feed + options + write mode against both destinations.
async fn both(
    name: &str,
    feeds: Vec<Feed>,
    options: serde_json::Value,
    mode: WriteMode,
    table: &str,
    cols: &[&str],
) -> (Outcome, Outcome) {
    // Callers guard on `runtime_available()` and skip before reaching here,
    // so a missing runtime at this point is unexpected — fail loudly.
    let pg = rdlt_connector_postgres::fixtures::PgFixture::start()
        .await
        .expect("postgres runtime (probed by the caller)");
    let conn = pg.conn.clone();
    let schema = format!("diff_{name}");
    let duck_dir = tempfile::tempdir().expect("tempdir");

    let mut pg_result: Result<(), String> = Ok(());
    let mut duck_result: Result<(), String> = Ok(());
    let duck = DuckDb::open(duck_dir.path().join("diff.duckdb"))
        .unwrap()
        .options(DestOptions::from_value(options.clone()).expect("options"))
        .unwrap();

    for feed in &feeds {
        // postgres side
        let dest = Postgres::connect(&conn)
            .dataset(&schema)
            .options(DestOptions::from_value(options.clone()).expect("options"))
            .unwrap();
        let mut config = EngineConfig::new(format!("diff-{name}"));
        config = config.with_write_mode(mode.clone());
        config = config.with_workdir(duck_dir.path().join("pg-work"));
        if let Err(e) = Engine::new(config, feed.clone(), dest).run().await {
            pg_result = Err(e.to_string());
            break;
        }
    }
    for feed in &feeds {
        let mut config = EngineConfig::new(format!("diff-{name}-duck"));
        config = config.with_write_mode(mode.clone());
        config = config.with_workdir(duck_dir.path().join("duck-work"));
        if let Err(e) = Engine::new(config, feed.clone(), duck.clone()).run().await {
            duck_result = Err(e.to_string());
            break;
        }
    }

    let pg_outcome = match pg_result {
        Ok(()) => Outcome {
            result: Ok(canon_pg(&conn, &schema, table, cols).await),
        },
        Err(e) => Outcome { result: Err(e) },
    };
    let duck_outcome = match duck_result {
        Ok(()) => Outcome {
            result: Ok(canon_duck(&duck, table, cols)),
        },
        Err(e) => Outcome { result: Err(e) },
    };
    (pg_outcome, duck_outcome)
}

fn assert_equivalent(name: &str, pg: Outcome, duck: Outcome) {
    match (pg.result, duck.result) {
        (Ok(p), Ok(d)) => assert_eq!(p, d, "{name}: canonical rows diverge"),
        (Err(p), Err(d)) => {
            // Typed-error-class parity: both must carry the shared message's
            // distinguishing phrase (the sqlcore text is shared verbatim, so
            // strict prefix-insensitive containment both ways).
            let key = |s: &str| {
                s.split("table `")
                    .nth(1)
                    .map(|t| t.to_owned())
                    .unwrap_or_else(|| s.to_owned())
            };
            assert_eq!(
                key(&p),
                key(&d),
                "{name}: typed errors diverge\npg: {p}\nduck: {d}"
            );
        }
        (Ok(_), Err(e)) => panic!("{name}: duck errored, pg succeeded: {e}"),
        (Err(e), Ok(_)) => panic!("{name}: pg errored, duck succeeded: {e}"),
    }
}

// ---- the differential matrix ------------------------------------------------

fn kv_feed(rows: &[(Option<i64>, Option<&str>)]) -> Feed {
    let (ks, vs): (Vec<_>, Vec<_>) = rows.iter().cloned().unzip();
    Feed {
        stream: "kv",
        key: &["k"],
        units: vec![batch(&[("k", ks)], &[("v", vs)], &[])],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn differential_delete_insert_redelivery() {
    if !rdlt_testkit::containers::runtime_available() {
        eprintln!("SKIP: no container runtime — differential cell not run");
        return;
    }
    let (pg, duck) = both(
        "di",
        vec![
            kv_feed(&[(Some(1), Some("one")), (Some(2), Some("two"))]),
            kv_feed(&[(Some(2), Some("two2")), (Some(3), Some("three"))]),
        ],
        serde_json::json!({}),
        WriteMode::Merge {
            key: vec!["k".into()],
        },
        "kv",
        &["k", "v"],
    )
    .await;
    assert_equivalent("delete_insert", pg, duck);
}

type UpsertRow<'a> = (Option<i64>, Option<i64>, Option<&'a str>, Option<bool>);

#[tokio::test(flavor = "multi_thread")]
async fn differential_upsert_with_hard_delete_and_dedup() {
    if !rdlt_testkit::containers::runtime_available() {
        eprintln!("SKIP: no container runtime — differential cell not run");
        return;
    }
    let feed = |rows: &[UpsertRow<'_>]| {
        let ks: Vec<_> = rows.iter().map(|r| r.0).collect();
        let seqs: Vec<_> = rows.iter().map(|r| r.1).collect();
        let vs: Vec<_> = rows.iter().map(|r| r.2).collect();
        let gone: Vec<_> = rows.iter().map(|r| r.3).collect();
        Feed {
            stream: "kv",
            key: &["k"],
            units: vec![batch(
                &[("k", ks), ("seq", seqs)],
                &[("v", vs)],
                &[("gone", gone)],
            )],
        }
    };
    let (pg, duck) = both(
        "ups",
        vec![
            feed(&[
                (Some(1), Some(1), Some("one"), Some(false)),
                (Some(2), Some(1), Some("two"), Some(false)),
            ]),
            feed(&[
                // in-load dupes: seq desc picks the survivor on BOTH dests
                (Some(1), Some(5), Some("one-v5"), Some(false)),
                (Some(1), Some(9), Some("one-v9"), Some(false)),
                (Some(2), Some(1), Some("two"), Some(true)), // hard delete
            ]),
        ],
        serde_json::json!({
            "merge_strategy": "upsert",
            "tables": {"kv": {"hard_delete": "gone",
                               "dedup_sort": {"column": "seq", "order": "desc"}}}
        }),
        WriteMode::Merge {
            key: vec!["k".into()],
        },
        "kv",
        &["k", "v"],
    )
    .await;
    assert_equivalent("upsert+hard_delete+dedup", pg, duck);
}

#[tokio::test(flavor = "multi_thread")]
async fn differential_scd2_history() {
    if !rdlt_testkit::containers::runtime_available() {
        eprintln!("SKIP: no container runtime — differential cell not run");
        return;
    }
    let (pg, duck) = both(
        "scd2",
        vec![
            kv_feed(&[(Some(1), Some("one")), (Some(2), Some("two"))]),
            kv_feed(&[(Some(1), Some("one-v2"))]), // change 1; 2 untouched (keep)
        ],
        serde_json::json!({
            "merge_strategy": "scd2",
            "tables": {"kv": {"scd2": {}}}
        }),
        WriteMode::Merge {
            key: vec!["k".into()],
        },
        "kv",
        // validity TIMES differ across engines by nature — compare the
        // HISTORY SHAPE: key, value, and whether the version is open.
        &["k", "v"],
    )
    .await;
    assert_equivalent("scd2", pg, duck);
}

#[tokio::test(flavor = "multi_thread")]
async fn differential_merge_scope_scope_and_null_scope() {
    if !rdlt_testkit::containers::runtime_available() {
        eprintln!("SKIP: no container runtime — differential cell not run");
        return;
    }
    let feed = |rows: &[(Option<i64>, Option<i64>, Option<&str>)]| {
        let ks: Vec<_> = rows.iter().map(|r| r.0).collect();
        let days: Vec<_> = rows.iter().map(|r| r.1).collect();
        let vs: Vec<_> = rows.iter().map(|r| r.2).collect();
        Feed {
            stream: "kv",
            key: &["k"],
            units: vec![batch(&[("k", ks), ("day", days)], &[("v", vs)], &[])],
        }
    };
    let (pg, duck) = both(
        "scope",
        vec![
            feed(&[
                (Some(1), Some(1), Some("d1-a")),
                (Some(2), Some(1), Some("d1-b")),
                (Some(3), Some(2), Some("d2-a")),
                (Some(4), None, Some("null-scope")),
            ]),
            feed(&[(Some(1), Some(1), Some("d1-a2"))]),
        ],
        serde_json::json!({"tables": {"kv": {"merge_scope": ["day"]}}}),
        WriteMode::Merge {
            key: vec!["k".into()],
        },
        "kv",
        &["k", "day", "v"],
    )
    .await;
    assert_equivalent("merge_scope", pg, duck);
}

#[tokio::test(flavor = "multi_thread")]
async fn differential_rejections_are_class_identical() {
    if !rdlt_testkit::containers::runtime_available() {
        eprintln!("SKIP: no container runtime — differential cell not run");
        return;
    }
    // Explicit strategy under append (011 R5): both destinations reject with
    // the SAME shared message.
    let (pg, duck) = both(
        "reject",
        vec![kv_feed(&[(Some(1), Some("x"))])],
        serde_json::json!({"merge_strategy": "upsert"}),
        WriteMode::Append,
        "kv",
        &["k", "v"],
    )
    .await;
    let (p, d) = (pg.result.unwrap_err(), duck.result.unwrap_err());
    for e in [&p, &d] {
        assert!(
            e.contains("merge_strategy requires the merge write mode"),
            "{e}"
        );
    }
    assert_eq!(
        p.split("table `").nth(1),
        d.split("table `").nth(1),
        "identical typed text after the destination prefix"
    );
}

/// 013 G1: scoped scd2 retirement produces IDENTICAL history shape on both
/// destinations (retire only within delivered scopes).
#[tokio::test(flavor = "multi_thread")]
async fn differential_scd2_scoped_retirement() {
    if !rdlt_testkit::containers::runtime_available() {
        eprintln!("SKIP: no container runtime — differential cell not run");
        return;
    }
    let feed = |rows: &[(Option<i64>, Option<i64>, Option<&str>)]| {
        let ks: Vec<_> = rows.iter().map(|r| r.0).collect();
        let days: Vec<_> = rows.iter().map(|r| r.1).collect();
        let vs: Vec<_> = rows.iter().map(|r| r.2).collect();
        Feed {
            stream: "kv",
            key: &["k"],
            units: vec![batch(&[("k", ks), ("day", days)], &[("v", vs)], &[])],
        }
    };
    let (pg, duck) = both(
        "scd2scope",
        vec![
            feed(&[
                (Some(1), Some(1), Some("k1")),
                (Some(2), Some(1), Some("k2")),
                (Some(3), Some(2), Some("k3")),
            ]),
            feed(&[(Some(1), Some(1), Some("k1"))]), // day 1 only, k2 absent
        ],
        serde_json::json!({
            "merge_strategy": "scd2",
            "tables": {"kv": {"scd2": {"absent": "retire",
                                        "active_record_timestamp": "9999-12-31T00:00:00Z"},
                               "merge_scope": ["day"]}}
        }),
        WriteMode::Merge {
            key: vec!["k".into()],
        },
        "kv",
        // history SHAPE: key, value, open-or-closed (marker makes "open"
        // comparable across engines without comparing wall-clock instants)
        &["k", "v"],
    )
    .await;
    assert_equivalent("scd2_scoped_retirement", pg, duck);
}
