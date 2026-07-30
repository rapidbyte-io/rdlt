//! T018: incremental cursor semantics against real Postgres + DuckDB —
//! delta loads, closed-boundary dedup vs open opt-out, NULL policies,
//! regressing clocks, config windows, PK-less row-hash keys, and the
//! structured-stream Merge boundary (engine clause B4).

mod common;

use common::PgFixture;
use rdlt_connector_duckdb::dest::DuckDb;
use rdlt_connector_postgres::source::PostgresSource;
use rdlt_engine::{Engine, EngineConfig};

const BASE: &str = r#"
CREATE TABLE ev (id int8 PRIMARY KEY, v text, ts timestamptz);
INSERT INTO ev VALUES
  (1, 'a', '2026-01-01T00:00:00Z'),
  (2, 'b', '2026-01-02T00:00:00Z'),
  (3, 'c', '2026-01-02T00:00:00Z');
"#;

fn source(conn: &str, cursor_extra: &str) -> PostgresSource {
    PostgresSource::from_yaml(&format!(
        "conn: \"{conn}\"\nbatch_max_rows: 2\ntables:\n  - name: ev\n    cursor:\n      column: ts\n{cursor_extra}"
    ))
    .expect("config")
}

struct Rig {
    dest: DuckDb,
}

impl Rig {
    fn new() -> Self {
        let dir = tempfile::tempdir().expect("tempdir");
        let dest = DuckDb::open(dir.path().join("out.duckdb")).expect("open db");
        std::mem::forget(dir);
        Self { dest }
    }

    async fn run(&self, source: PostgresSource, pipeline: &str) -> u64 {
        let report = Engine::new(EngineConfig::new(pipeline), source, self.dest.clone())
            .run()
            .await
            .expect("run");
        report.total_rows()
    }

    fn count(&self) -> u64 {
        self.dest.count_rows("ev").expect("count")
    }

    fn distinct_ids(&self) -> String {
        self.dest
            .query_string("SELECT CAST(count(DISTINCT id) AS VARCHAR) FROM ev")
            .expect("distinct")
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn delta_loads_and_closed_boundary_dedup() {
    let Some(fixture) = PgFixture::start().await else {
        return;
    };
    fixture.seed(BASE).await;
    let rig = Rig::new();

    // Run 1: full load.
    assert_eq!(rig.run(source(&fixture.conn_url(), ""), "inc").await, 3);
    assert_eq!(rig.count(), 3);

    // New row AT the exact watermark (2026-01-02) plus one beyond it.
    fixture
        .seed(
            "INSERT INTO ev VALUES (4, 'd', '2026-01-02T00:00:00Z'), \
             (5, 'e', '2026-01-03T00:00:00Z');",
        )
        .await;
    // Run 2: closed boundary re-fetches watermark-equal rows; ids 2 and 3 are
    // deduped source-side via boundary keys — exactly the two new rows load.
    assert_eq!(rig.run(source(&fixture.conn_url(), ""), "inc").await, 2);
    assert_eq!(rig.count(), 5, "no duplicates");
    assert_eq!(rig.distinct_ids(), "5");

    // Run 3 with nothing new: zero rows move.
    assert_eq!(rig.run(source(&fixture.conn_url(), ""), "inc").await, 0);
    assert_eq!(rig.count(), 5);
}

#[tokio::test(flavor = "multi_thread")]
async fn open_boundary_skips_watermark_equal_rows() {
    let Some(fixture) = PgFixture::start().await else {
        return;
    };
    fixture.seed(BASE).await;
    let rig = Rig::new();
    let cfg = "      boundary: exclusive\n";

    assert_eq!(
        rig.run(source(&fixture.conn_url(), cfg), "inc-open").await,
        3
    );
    // A late row at the exact watermark: the open boundary (strict >) never
    // re-fetches it — the documented monotonic-cursor trade-off.
    fixture
        .seed("INSERT INTO ev VALUES (4, 'd', '2026-01-02T00:00:00Z');")
        .await;
    assert_eq!(
        rig.run(source(&fixture.conn_url(), cfg), "inc-open").await,
        0
    );
    assert_eq!(rig.count(), 3, "watermark-equal late row skipped by design");
}

#[tokio::test(flavor = "multi_thread")]
async fn null_cursor_policies() {
    let Some(fixture) = PgFixture::start().await else {
        return;
    };
    fixture.seed(BASE).await;
    fixture
        .seed("INSERT INTO ev VALUES (90, 'n1', NULL), (91, 'n2', NULL);")
        .await;

    // Default exclude: NULL-cursor rows never load.
    let rig = Rig::new();
    assert_eq!(rig.run(source(&fixture.conn_url(), ""), "inc-nx").await, 3);
    assert_eq!(rig.count(), 3);

    // Include: NULL-cursor rows load on EVERY run (they carry no watermark) —
    // the documented Append-mode consequence.
    let rig = Rig::new();
    let cfg = "      nulls: include\n";
    assert_eq!(rig.run(source(&fixture.conn_url(), cfg), "inc-ni").await, 5);
    assert_eq!(rig.run(source(&fixture.conn_url(), cfg), "inc-ni").await, 2);
    let null_copies = rig
        .dest
        .query_string("SELECT CAST(count(*) AS VARCHAR) FROM ev WHERE id = 90")
        .expect("null copies");
    assert_eq!(
        null_copies, "2",
        "null-cursor rows re-fetch every run under include"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn regressing_clock_never_moves_watermark_backward() {
    let Some(fixture) = PgFixture::start().await else {
        return;
    };
    fixture.seed(BASE).await;
    let rig = Rig::new();

    assert_eq!(rig.run(source(&fixture.conn_url(), ""), "inc-reg").await, 3);
    // Clock skew writes a row BELOW the committed watermark: invisible to
    // cursor incremental (the documented caveat), and the watermark must not
    // regress because of it.
    fixture
        .seed("INSERT INTO ev VALUES (6, 'skew', '2026-01-01T12:00:00Z');")
        .await;
    assert_eq!(rig.run(source(&fixture.conn_url(), ""), "inc-reg").await, 0);
    // A later row still loads exactly once — state stayed at the max.
    fixture
        .seed("INSERT INTO ev VALUES (7, 'later', '2026-01-04T00:00:00Z');")
        .await;
    assert_eq!(rig.run(source(&fixture.conn_url(), ""), "inc-reg").await, 1);
    assert_eq!(rig.count(), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn initial_and_end_value_window() {
    let Some(fixture) = PgFixture::start().await else {
        return;
    };
    fixture.seed(BASE).await;
    fixture
        .seed("INSERT INTO ev VALUES (8, 'h', '2026-01-05T00:00:00Z'), (9, 'i', '2026-01-06T00:00:00Z');")
        .await;
    let rig = Rig::new();
    let cfg = "      initial_value: \"2026-01-02T00:00:00Z\"\n      end_value: \"2026-01-06T00:00:00Z\"\n";
    // Closed start (>= 01-02), exclusive end (< 01-06): ids 2,3,8.
    assert_eq!(
        rig.run(source(&fixture.conn_url(), cfg), "inc-win").await,
        3
    );
    let ids = rig
        .dest
        .query_string(
            "SELECT CAST(string_agg(CAST(id AS VARCHAR), ',' ORDER BY id) AS VARCHAR) FROM ev",
        )
        .expect("ids");
    assert_eq!(ids, "2,3,8");
}

#[tokio::test(flavor = "multi_thread")]
async fn pkless_table_dedups_via_row_hash() {
    let Some(fixture) = PgFixture::start().await else {
        return;
    };
    fixture
        .seed(
            "CREATE TABLE ev (id int8, v text, ts timestamptz); \
             INSERT INTO ev VALUES (1, 'a', '2026-01-02T00:00:00Z'), (2, 'b', '2026-01-02T00:00:00Z');",
        )
        .await;
    let rig = Rig::new();
    assert_eq!(
        rig.run(source(&fixture.conn_url(), ""), "inc-hash").await,
        2
    );
    // New DISTINCT row at the watermark: row-hash dedup passes it, drops the
    // re-fetched identical ones.
    fixture
        .seed("INSERT INTO ev VALUES (3, 'c', '2026-01-02T00:00:00Z');")
        .await;
    assert_eq!(
        rig.run(source(&fixture.conn_url(), ""), "inc-hash").await,
        1
    );
    assert_eq!(rig.count(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn uuid_cursor_end_to_end() {
    // Pre-fix, a uuid cursor generated `"col" >= '...'::text`, which has no
    // uuid>=text operator — a guaranteed runtime error on the second run.
    let Some(fixture) = PgFixture::start().await else {
        return;
    };
    fixture
        .seed(
            "CREATE TABLE ev (id uuid PRIMARY KEY, v text); \
             INSERT INTO ev VALUES \
               ('00000000-0000-0000-0000-000000000001', 'a'), \
               ('00000000-0000-0000-0000-000000000002', 'b');",
        )
        .await;
    let rig = Rig::new();
    let src = |conn: &str| {
        PostgresSource::from_yaml(&format!(
            "conn: \"{conn}\"\ntables:\n  - name: ev\n    cursor:\n      column: id\n"
        ))
        .expect("config")
    };
    assert_eq!(rig.run(src(&fixture.conn_url()), "inc-uuid").await, 2);
    fixture
        .seed("INSERT INTO ev VALUES ('00000000-0000-0000-0000-000000000003', 'c');")
        .await;
    assert_eq!(rig.run(src(&fixture.conn_url()), "inc-uuid").await, 1);
    assert_eq!(rig.count(), 3);
}

#[tokio::test(flavor = "multi_thread")]
async fn text_cursor_mixed_case_byte_order() {
    // COLLATE "C" pins SQL ordering/filtering to the tracker's Rust byte
    // order: 'B' < 'a' in bytes, though most locales sort 'a' < 'B'.
    let Some(fixture) = PgFixture::start().await else {
        return;
    };
    fixture
        .seed(
            "CREATE TABLE ev (id int8 PRIMARY KEY, name text); \
             INSERT INTO ev VALUES (1, 'Alpha'), (2, 'beta'), (3, 'Gamma');",
        )
        .await;
    let rig = Rig::new();
    let src = |conn: &str| {
        PostgresSource::from_yaml(&format!(
            "conn: \"{conn}\"\ntables:\n  - name: ev\n    cursor:\n      column: name\n"
        ))
        .expect("config")
    };
    assert_eq!(rig.run(src(&fixture.conn_url()), "inc-text").await, 3);
    // 'Delta' < 'beta' in byte order (uppercase D), so under a locale sort it
    // would land inside the already-seen range; byte-order watermark 'beta'
    // means 'Delta' is BELOW the watermark — documented cursor semantics: a
    // non-monotonic text insert is invisible (same as the regressing clock).
    // 'zeta' is above in byte order and must load.
    fixture
        .seed("INSERT INTO ev VALUES (4, 'Delta'), (5, 'zeta');")
        .await;
    assert_eq!(
        rig.run(src(&fixture.conn_url()), "inc-text").await,
        1,
        "only the byte-order-greater row loads; ordering is consistent, no panic, no dupes"
    );
    assert_eq!(rig.count(), 4);
}

#[tokio::test(flavor = "multi_thread")]
async fn merge_by_declared_key_converges_and_keyless_is_rejected() {
    // Engine clause B4 as amended by feature 006 (merge-structured.md):
    // structured streams with a declared primary_key merge BY that key —
    // update-heavy re-runs converge to one row per key. Keyless structured
    // streams keep the original plan-time rejection.
    let Some(fixture) = PgFixture::start().await else {
        return;
    };
    fixture.seed(BASE).await;
    let rig = Rig::new();

    let merge_config = |pipeline: &str| {
        let mut config = EngineConfig::new(pipeline);
        config = config.with_write_mode(rdlt_connector::core::WriteMode::Merge {
            key: vec!["id".into()],
        });
        config
    };
    let run_merge = |src: PostgresSource| {
        let dest = rig.dest.clone();
        let config = merge_config("inc-merge");
        async move {
            Engine::new(config, src, dest)
                .run()
                .await
                .expect("keyed merge accepted")
                .total_rows()
        }
    };

    assert_eq!(run_merge(source(&fixture.conn_url(), "")).await, 3);
    assert_eq!(rig.count(), 3);

    // Update TWO existing rows past the watermark and add one new row: the
    // merge must overwrite in place, not append.
    fixture
        .seed(
            "UPDATE ev SET v = 'a2', ts = '2026-01-05T00:00:00Z' WHERE id = 1; \
             UPDATE ev SET v = 'b2', ts = '2026-01-05T00:00:00Z' WHERE id = 2; \
             INSERT INTO ev VALUES (4, 'd', '2026-01-04T00:00:00Z');",
        )
        .await;
    assert_eq!(run_merge(source(&fixture.conn_url(), "")).await, 3);
    assert_eq!(rig.count(), 4, "one row per key after update-heavy run");
    assert_eq!(rig.distinct_ids(), "4");
    let v1 = rig
        .dest
        .query_string("SELECT v FROM ev WHERE id = 1")
        .expect("v1");
    assert_eq!(v1, "a2", "merge took the updated value");

    // Idempotent re-run (nothing past the watermark): still one row per key.
    assert_eq!(run_merge(source(&fixture.conn_url(), "")).await, 0);
    assert_eq!(rig.count(), 4);

    // Keyless structured stream: B4 rejection stands, at plan time.
    fixture
        .seed("CREATE TABLE nokey (x int8); INSERT INTO nokey VALUES (1);")
        .await;
    let keyless = PostgresSource::from_yaml(&format!(
        "conn: \"{}\"\ntables:\n  - name: nokey\n",
        fixture.conn_url()
    ))
    .expect("config");
    let dir = tempfile::tempdir().expect("tempdir");
    let dest = DuckDb::open(dir.path().join("nokey.duckdb")).expect("open db");
    let mut config = merge_config("inc-merge-nokey");
    config = config.with_write_mode(rdlt_connector::core::WriteMode::Merge {
        key: vec!["x".into()],
    });
    let err = Engine::new(config, keyless, dest)
        .run()
        .await
        .expect_err("keyless structured merge must be rejected");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("merge") && msg.contains("primary_key"),
        "{msg}"
    );
}

// ---- Feature 007 US2: cursor lag (contract cursor-lag.md) ----

#[tokio::test(flavor = "multi_thread")]
async fn lag_captures_late_arrivals_with_exact_totals_under_merge() {
    let Some(fixture) = PgFixture::start().await else {
        return;
    };
    fixture.seed(BASE).await;
    let rig = Rig::new();

    let lag_source = |conn: &str| {
        PostgresSource::from_yaml(&format!(
            "conn: \"{conn}\"\ntables:\n  - name: ev\n    cursor:\n      column: ts\n      lag: \"5m\"\n"
        ))
        .expect("config")
    };
    let run_merge = |src: PostgresSource| {
        let dest = rig.dest.clone();
        async move {
            let mut config = EngineConfig::new("inc-lag");
            config = config.with_write_mode(rdlt_connector::core::WriteMode::Merge {
                key: vec!["id".into()],
            });
            Engine::new(config, src, dest)
                .run()
                .await
                .expect("lag run")
                .total_rows()
        }
    };

    assert_eq!(run_merge(lag_source(&fixture.conn_url())).await, 3);

    // A LATE commit: cursor value 3 minutes BEHIND the watermark (inside the
    // 5m window) — invisible without lag. Plus one far beyond the window.
    fixture
        .seed(
            "INSERT INTO ev VALUES (4, 'late', '2026-01-01T23:57:00Z'), \
             (5, 'too-old', '2026-01-01T12:00:00Z');",
        )
        .await;
    run_merge(lag_source(&fixture.conn_url())).await;
    assert_eq!(rig.count(), 4, "late row captured, beyond-window row not");
    assert_eq!(rig.distinct_ids(), "4");
    let missing = rig
        .dest
        .query_string("SELECT CAST(count(*) AS VARCHAR) FROM ev WHERE id = 5")
        .expect("count");
    assert_eq!(missing, "0", "L5: the window bounds the guarantee");

    // Idempotent window re-merge: three further runs, totals NEVER move —
    // window rows re-deliver and merge by key (SC-002 as amended by R4).
    for _ in 0..3 {
        run_merge(lag_source(&fixture.conn_url())).await;
        assert_eq!(rig.count(), 4, "destination totals stay exact");
        assert_eq!(rig.distinct_ids(), "4");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn lag_rejections_are_typed_and_early() {
    use rdlt_connector::Source as _;
    let Some(fixture) = PgFixture::start().await else {
        return;
    };
    fixture
        .seed(
            "CREATE TABLE ev (id int8 PRIMARY KEY, v text, ts timestamptz); \
             CREATE TABLE nokey (x int8, ts timestamptz); \
             CREATE TABLE dated (id int8 PRIMARY KEY, d date);",
        )
        .await;
    let src = |extra: &str| {
        PostgresSource::from_yaml(&format!("conn: \"{}\"\n{extra}", fixture.conn_url()))
            .expect("config")
    };

    // Text cursor: no defined subtraction — names column and type.
    let err = src("tables:\n  - name: ev\n    cursor:\n      column: v\n      lag: \"5m\"\n")
        .streams()
        .await
        .expect_err("lag on text cursor");
    let msg = err.to_string();
    assert!(msg.contains('v') && msg.contains("lag"), "{msg}");

    // Keyless stream: the merge dedup path must exist.
    let err = src("tables:\n  - name: nokey\n    cursor:\n      column: ts\n      lag: \"5m\"\n")
        .streams()
        .await
        .expect_err("lag without a primary key");
    assert!(err.to_string().contains("primary key"), "{err}");

    // Sub-day lag on a date cursor.
    let err = src("tables:\n  - name: dated\n    cursor:\n      column: d\n      lag: \"5m\"\n")
        .streams()
        .await
        .expect_err("sub-day lag on date");
    assert!(err.to_string().contains("whole-day"), "{err}");

    // Whole-day lag on date: accepted.
    src("tables:\n  - name: dated\n    cursor:\n      column: d\n      lag: \"2d\"\n")
        .streams()
        .await
        .expect("whole-day lag on date is fine");
}

// ---- Feature 007 US4: cursor edge policies (cursor-lag.md N1-N3, E1-E2) ----

#[tokio::test(flavor = "multi_thread")]
async fn null_cursor_error_policy_fails_typed_and_old_policies_unchanged() {
    let Some(fixture) = PgFixture::start().await else {
        return;
    };
    fixture
        .seed(
            "CREATE TABLE ev (id int8 PRIMARY KEY, v text, ts timestamptz); \
             INSERT INTO ev VALUES (1, 'a', '2026-01-01T00:00:00Z'), \
             (2, 'null-cursor', NULL), (3, 'c', '2026-01-02T00:00:00Z');",
        )
        .await;
    let src = |nulls: &str| {
        PostgresSource::from_yaml(&format!(
            "conn: \"{}\"\ntables:\n  - name: ev\n    cursor:\n      column: ts\n      nulls: {nulls}\n",
            fixture.conn_url()
        ))
        .expect("config")
    };

    // `error`: typed FATAL naming stream and column; nothing publishes.
    let rig = Rig::new();
    let err = Engine::new(
        EngineConfig::new("inc-nulls-err"),
        src("error"),
        rig.dest.clone(),
    )
    .run()
    .await
    .expect_err("NULL cursor under `error` must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("ev") && msg.contains("ts") && msg.contains("NULL"),
        "{msg}"
    );
    assert_eq!(
        rig.dest.count_rows("ev").unwrap_or(0),
        0,
        "N2: nothing published from the failed run"
    );

    // Fix the data, re-run same pipeline: clean load, no duplicates (N2).
    fixture
        .seed("UPDATE ev SET ts = '2026-01-03T00:00:00Z' WHERE id = 2;")
        .await;
    assert_eq!(
        Engine::new(
            EngineConfig::new("inc-nulls-err"),
            src("error"),
            rig.dest.clone()
        )
        .run()
        .await
        .expect("clean after fix")
        .total_rows(),
        3
    );
    assert_eq!(rig.count(), 3);
    assert_eq!(rig.distinct_ids(), "3");

    // N3: exclude (default) and include pins — byte-identical behavior.
    let Some(fixture2) = PgFixture::start().await else {
        return;
    };
    fixture2
        .seed(
            "CREATE TABLE ev (id int8 PRIMARY KEY, v text, ts timestamptz); \
             INSERT INTO ev VALUES (1, 'a', '2026-01-01T00:00:00Z'), (2, 'n', NULL);",
        )
        .await;
    let src2 = |nulls: &str| {
        PostgresSource::from_yaml(&format!(
            "conn: \"{}\"\ntables:\n  - name: ev\n    cursor:\n      column: ts\n      nulls: {nulls}\n",
            fixture2.conn_url()
        ))
        .expect("config")
    };
    let rig2 = Rig::new();
    assert_eq!(
        Engine::new(
            EngineConfig::new("inc-nulls-ex"),
            src2("exclude"),
            rig2.dest.clone()
        )
        .run()
        .await
        .expect("exclude")
        .total_rows(),
        1,
        "exclude filters the NULL row"
    );
    let rig3 = Rig::new();
    assert_eq!(
        Engine::new(
            EngineConfig::new("inc-nulls-in"),
            src2("include"),
            rig3.dest.clone()
        )
        .run()
        .await
        .expect("include")
        .total_rows(),
        2,
        "include loads the NULL row"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn inclusive_end_bound_loads_boundary_rows_exactly_once() {
    let Some(fixture) = PgFixture::start().await else {
        return;
    };
    fixture.seed(BASE).await; // ids 1..3, ts 01-01 .. 01-02
    let src = |end_bound: &str| {
        PostgresSource::from_yaml(&format!(
            "conn: \"{}\"\ntables:\n  - name: ev\n    cursor:\n      column: ts\n      end_value: \"2026-01-02T00:00:00Z\"\n      end_bound: {end_bound}\n",
            fixture.conn_url()
        ))
        .expect("config")
    };

    // Exclusive (default semantics): boundary rows (ids 2,3 at 01-02) do NOT load.
    let rig = Rig::new();
    assert_eq!(
        Engine::new(
            EngineConfig::new("inc-endx"),
            src("exclusive"),
            rig.dest.clone()
        )
        .run()
        .await
        .expect("exclusive")
        .total_rows(),
        1,
        "E1: exclusive stops before the bound"
    );

    // Inclusive: boundary rows load; re-run stays stable (E2).
    let rig = Rig::new();
    let run = || {
        let dest = rig.dest.clone();
        let source = src("inclusive");
        async move {
            Engine::new(EngineConfig::new("inc-endi"), source, dest)
                .run()
                .await
                .expect("inclusive")
                .total_rows()
        }
    };
    assert_eq!(run().await, 3, "E1: rows exactly AT the bound load");
    assert_eq!(run().await, 0, "E2: re-run moves nothing");
    assert_eq!(rig.count(), 3);
    assert_eq!(rig.distinct_ids(), "3");
}

// ---- Feature 011 (contract PM1/PM2): parameter-matrix gap cells ----

/// `direction: min` — descending cursors: the watermark is the MINIMUM
/// seen, and later runs load rows BELOW it.
#[tokio::test(flavor = "multi_thread")]
async fn direction_min_descends_and_resumes() {
    let Some(fixture) = PgFixture::start().await else {
        return;
    };
    fixture
        .seed(
            "CREATE TABLE ev (id int8 PRIMARY KEY, v text, ts timestamptz);\
             INSERT INTO ev VALUES (5, 'e', now()), (6, 'f', now());",
        )
        .await;
    let rig = Rig::new();
    let source = |conn: &str| {
        PostgresSource::from_yaml(&format!(
            "conn: \"{conn}\"\ntables:\n  - name: ev\n    cursor:\n      column: id\n      direction: min\n"
        ))
        .expect("config")
    };
    assert_eq!(rig.run(source(&fixture.conn_url()), "inc-min").await, 2);

    // Rows BELOW the min watermark arrive later (a descending feed).
    fixture
        .seed("INSERT INTO ev VALUES (3, 'c', now()), (4, 'd', now());")
        .await;
    assert_eq!(
        rig.run(source(&fixture.conn_url()), "inc-min").await,
        2,
        "descending resume loads exactly the rows below the watermark"
    );
    assert_eq!(rig.count(), 4);
}

/// `lag` magnitude family — integer cursors take plain magnitudes: a
/// resumed run re-scans `watermark - N` and captures late rows, with
/// exact totals under merge.
#[tokio::test(flavor = "multi_thread")]
async fn magnitude_lag_for_integer_cursors() {
    let Some(fixture) = PgFixture::start().await else {
        return;
    };
    fixture
        .seed(
            "CREATE TABLE ev (id int8 PRIMARY KEY, v text, ts timestamptz);\
             INSERT INTO ev VALUES (1,'a',now()),(2,'b',now()),(3,'c',now()),(7,'g',now());",
        )
        .await;
    let rig = Rig::new();
    let source = |conn: &str| {
        PostgresSource::from_yaml(&format!(
            "conn: \"{conn}\"\ntables:\n  - name: ev\n    cursor:\n      column: id\n      lag: \"2\"\n"
        ))
        .expect("config")
    };
    let merge = "int-lag";
    let run = |src| {
        let mut config = EngineConfig::new(merge);
        config = config.with_write_mode(rdlt_connector::WriteMode::Merge {
            key: vec!["id".into()],
        });
        Engine::new(config, src, rig.dest.clone())
    };
    run(source(&fixture.conn_url())).run().await.expect("run 1");
    // A LATE row lands inside the magnitude window (id 6 ≥ 7 - 2).
    fixture
        .seed("INSERT INTO ev VALUES (6, 'late', now());")
        .await;
    let report = run(source(&fixture.conn_url())).run().await.expect("run 2");
    assert_eq!(
        report.total_rows(),
        1,
        "the window re-reads [5,7], but boundary-key dedup drops the replayed \
         watermark row SOURCE-side — exactly the late row moves"
    );
    assert_eq!(
        rig.count(),
        5,
        "exact totals under merge (no dupes, late row present)"
    );
    assert_eq!(rig.distinct_ids(), "5");
}

/// `cursor.column` must survive the column selection — excluding it is a
/// typed error naming the column, before any data moves.
#[tokio::test(flavor = "multi_thread")]
async fn cursor_column_must_survive_selection() {
    let Some(fixture) = PgFixture::start().await else {
        return;
    };
    fixture
        .seed("CREATE TABLE ev (id int8 PRIMARY KEY, v text, ts timestamptz);")
        .await;
    let source = PostgresSource::from_yaml(&format!(
        "conn: \"{}\"\ntables:\n  - name: ev\n    excluded_columns: [ts]\n    cursor:\n      column: ts\n",
        fixture.conn_url()
    ))
    .expect("config parses — the check needs reflection");
    let rig = Rig::new();
    let config = EngineConfig::new("inc-cursor-sel");
    let err = Engine::new(config, source, rig.dest.clone())
        .run()
        .await
        .expect_err("excluded cursor column must fail typed")
        .to_string();
    assert!(err.contains("`ts`"), "{err}");
    assert!(err.contains("selected"), "{err}");
}

/// `batch_target_bytes` / `batch_max_rows` — the knobs OBSERVABLY cut
/// batches: tiny knobs produce many commit units, huge knobs one.
#[tokio::test(flavor = "multi_thread")]
async fn batch_knobs_cut_batches_observably() {
    let Some(fixture) = PgFixture::start().await else {
        return;
    };
    fixture
        .seed(
            "CREATE TABLE ev (id int8 PRIMARY KEY, v text, ts timestamptz);\
             INSERT INTO ev SELECT i, repeat('x', 500), now() FROM generate_series(1, 40) i;",
        )
        .await;
    let commits_with = |extra: &str| {
        let conn = fixture.conn_url();
        let extra = extra.to_string();
        async move {
            let rig = Rig::new();
            let source = PostgresSource::from_yaml(&format!(
                "conn: \"{conn}\"\n{extra}tables:\n  - name: ev\n    cursor:\n      column: id\n"
            ))
            .expect("config");
            let config = EngineConfig::new("inc-knobs");
            let report = Engine::new(config, source, rig.dest.clone())
                .run()
                .await
                .expect("run");
            report.commits
        }
    };
    let one = commits_with("").await;
    let by_rows = commits_with("batch_max_rows: 5\n").await;
    let by_bytes = commits_with("batch_target_bytes: 2048\n").await;
    // Incremental streams commit per checkpoint: one batch = one mid-stream
    // checkpoint + the final-state checkpoint = 2 commits at the default
    // knobs; the knobs multiply that observably.
    assert_eq!(
        one, 2,
        "default knobs: 40 small rows = one batch (+ final state)"
    );
    assert!(
        by_rows >= 8,
        "batch_max_rows=5 over 40 rows cuts many batches: {by_rows}"
    );
    assert!(
        by_bytes >= 4,
        "a 2 KiB byte target over ~20 KiB cuts many batches: {by_bytes}"
    );
}
