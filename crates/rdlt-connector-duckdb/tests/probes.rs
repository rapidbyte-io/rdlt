//! Dialect-feasibility probes: each pins ONE assumption a DuckDB dialect arm
//! depends on, asserted against the bundled DuckDB itself rather than against
//! its documentation.
//!
//! A probe that fails converts its dependent arm into a TYPED capability gap —
//! never a silent approximation. That is the whole point of probing before
//! relying: an assumption that turns out false becomes a refusal the caller can
//! see, instead of SQL that runs and means something subtly different.

use duckdb::Connection;

fn conn() -> Connection {
    Connection::open_in_memory().expect("in-memory duckdb")
}

/// R4: postgres-style `DISTINCT ON` with multi-column ORDER BY — the shared
/// dedup/survivor shape. Survivor = first row per identity group
/// under (sort, arrival DESC).
#[test]
fn probe_distinct_on_ordering_semantics() {
    let c = conn();
    c.execute_batch(
        "CREATE TABLE stage (id BIGINT, seq BIGINT, v VARCHAR);
         INSERT INTO stage VALUES
           (1, 10, 'first'), (1, 30, 'winner'), (1, 20, 'middle'),
           (2, NULL, 'only-null'), (3, NULL, 'null-first'), (3, 5, 'valued');",
    )
    .unwrap();
    // seq DESC NULLS LAST: values beat NULL; rowid DESC is the tie-breaker.
    let rows: Vec<(i64, String)> = c
        .prepare(
            "SELECT id, v FROM (SELECT DISTINCT ON (id) * FROM stage \
             ORDER BY id, seq DESC NULLS LAST, rowid DESC) ORDER BY id",
        )
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        rows,
        vec![
            (1, "winner".to_string()),    // greatest seq survives
            (2, "only-null".to_string()), // all-NULL group keeps its row
            (3, "valued".to_string()),    // value beats NULL
        ],
        "PROBE FAILED: DuckDB DISTINCT ON does not honor the survivor shape"
    );
}

/// R4: `INSERT … ON CONFLICT (cols) DO UPDATE SET c = EXCLUDED.c` against a
/// plain `CREATE UNIQUE INDEX` target (the M5 auto-ensured merge-identity
/// index — NOT a declared constraint).
#[test]
fn probe_on_conflict_against_unique_index() {
    let c = conn();
    c.execute_batch(
        "CREATE TABLE t (id BIGINT, day BIGINT, v VARCHAR);
         CREATE UNIQUE INDEX ux ON t (id, day);
         INSERT INTO t VALUES (1, 1, 'old');",
    )
    .unwrap();
    c.execute_batch(
        "INSERT INTO t (id, day, v) VALUES (1, 1, 'new'), (2, 1, 'fresh')
         ON CONFLICT (id, day) DO UPDATE SET v = EXCLUDED.v",
    )
    .expect("PROBE FAILED: ON CONFLICT DO UPDATE against a unique index");
    let v: String = c
        .query_row("SELECT v FROM t WHERE id = 1", [], |r| r.get(0))
        .unwrap();
    assert_eq!(v, "new", "matched key must update in place");
    let n: i64 = c
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 2);
}

/// R5: `now()` is TRANSACTION-stable — two reads inside one transaction (with
/// real work between them) observe the same instant. The scd2 boundary rule
/// (S5) depends on this.
#[test]
fn probe_now_is_transaction_stable() {
    let mut c = conn();
    let tx = c.transaction().unwrap();
    let t1: String = tx
        .query_row("SELECT CAST(now() AS VARCHAR)", [], |r| r.get(0))
        .unwrap();
    tx.execute_batch("CREATE TABLE busy AS SELECT * FROM range(100000)")
        .unwrap();
    let t2: String = tx
        .query_row("SELECT CAST(now() AS VARCHAR)", [], |r| r.get(0))
        .unwrap();
    tx.commit().unwrap();
    assert_eq!(
        t1, t2,
        "PROBE FAILED: now() moved within one transaction — scd2 boundary unstable"
    );
}

/// S3 change detection depends on NULL-safe `IS DISTINCT FROM`.
#[test]
fn probe_is_distinct_from_null_semantics() {
    let c = conn();
    for (a, b, expect) in [
        ("1", "1", false),
        ("1", "2", true),
        ("NULL", "1", true),
        ("NULL", "NULL", false),
    ] {
        let got: bool = c
            .query_row(&format!("SELECT {a} IS DISTINCT FROM {b}"), [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(got, expect, "{a} IS DISTINCT FROM {b}");
    }
}

/// R6: the bundled build carries the JSON extension statically — native JSON
/// columns, VARCHAR→JSON cast, and json_extract all work without network.
#[test]
fn probe_bundled_json_extension() {
    let c = conn();
    c.execute_batch(
        "CREATE TABLE j (doc JSON);
         INSERT INTO j SELECT CAST('{\"a\": {\"b\": 7}}' AS JSON);",
    )
    .expect("PROBE FAILED: JSON type unavailable in the bundled build");
    let b: i64 = c
        .query_row(
            "SELECT CAST(json_extract(doc, '$.a.b') AS BIGINT) FROM j",
            [],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(b, 7, "json_extract over a native JSON column");
}

/// The scd2 arms also need correlated UPDATE … FROM and NOT EXISTS shapes.
#[test]
fn probe_update_from_and_not_exists() {
    let c = conn();
    c.execute_batch(
        "CREATE TABLE t (id BIGINT, v VARCHAR, closed TIMESTAMPTZ);
         CREATE TABLE d (id BIGINT, v VARCHAR);
         INSERT INTO t VALUES (1, 'old', NULL), (2, 'same', NULL);
         INSERT INTO d VALUES (1, 'new'), (2, 'same');",
    )
    .unwrap();
    c.execute_batch(
        "UPDATE t SET closed = now() FROM d \
         WHERE t.closed IS NULL AND t.id = d.id AND (t.v IS DISTINCT FROM d.v)",
    )
    .expect("PROBE FAILED: correlated UPDATE ... FROM");
    let closed: i64 = c
        .query_row("SELECT count(*) FROM t WHERE closed IS NOT NULL", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(closed, 1, "only the changed key closes");
    c.execute_batch(
        "INSERT INTO t (id, v, closed) SELECT id, v, NULL FROM d \
         WHERE NOT EXISTS (SELECT 1 FROM t x WHERE x.closed IS NULL AND x.id = d.id)",
    )
    .expect("PROBE FAILED: NOT EXISTS anti-join insert");
    let n: i64 = c
        .query_row("SELECT count(*) FROM t", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 3, "changed key re-opens; unchanged key skipped");
}

/// 013 G3: the settings/extension passthrough — applied, queryable, and
/// injection-shaped keys refused typed.
#[test]
fn g3_settings_and_extensions_passthrough() {
    use rdlt_connector_duckdb::dest::DuckDb;
    let dir = tempfile::tempdir().unwrap();
    let dest = DuckDb::open(dir.path().join("g3.duckdb"))
        .unwrap()
        .setting("threads", "2")
        .unwrap()
        .extension("json")
        .unwrap();
    // query_string runs on a CLONED (fresh-session) connection — passing
    // proves the replay-per-session fix (013 review finding 4), not just
    // the builder connection.
    assert_eq!(
        dest.query_string("SELECT current_setting('threads')::VARCHAR")
            .unwrap(),
        "2"
    );
    // Typed refusals: identifiers only — injection shapes never interpolate.
    let err = DuckDb::open(dir.path().join("g3b.duckdb"))
        .unwrap()
        .setting("threads='1'; DROP TABLE x; --", "2")
        .unwrap_err()
        .to_string();
    assert!(err.contains("bare identifiers"), "{err}");
    let err = DuckDb::open(dir.path().join("g3c.duckdb"))
        .unwrap()
        .extension("json; DROP TABLE x")
        .unwrap_err()
        .to_string();
    assert!(err.contains("bare identifiers"), "{err}");
    // Unknown settings/extensions surface DuckDB's own error, typed.
    let err = DuckDb::open(dir.path().join("g3d.duckdb"))
        .unwrap()
        .setting("no_such_setting_xyz", "1")
        .unwrap_err()
        .to_string();
    assert!(err.contains("no_such_setting_xyz"), "{err}");
}
