//! T018: incremental cursor semantics against real Postgres + DuckDB —
//! delta loads, closed-boundary dedup vs open opt-out, NULL policies,
//! regressing clocks, config windows, PK-less row-hash keys, and the
//! structured-stream Merge boundary (engine clause B4).

mod common;

use common::PgFixture;
use rdlt_dest_duckdb::DuckDb;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_source_postgres::PostgresSource;

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
    let fixture = PgFixture::start().await;
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
    let fixture = PgFixture::start().await;
    fixture.seed(BASE).await;
    let rig = Rig::new();
    let cfg = "      boundary: open\n";

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
    let fixture = PgFixture::start().await;
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
    let fixture = PgFixture::start().await;
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
    let fixture = PgFixture::start().await;
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
    let fixture = PgFixture::start().await;
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
    let fixture = PgFixture::start().await;
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
    let fixture = PgFixture::start().await;
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
async fn merge_mode_rejected_for_structured_streams() {
    // Engine clause B4 (feature 002): structured streams have no per-row
    // `_rdlt_id`, so Merge is rejected at plan time. Spec US2-AS5 cannot hold
    // for this source without an engine-contract change — recorded deviation
    // + backlog item (see tasks.md implementation notes).
    let fixture = PgFixture::start().await;
    fixture.seed(BASE).await;
    let dir = tempfile::tempdir().expect("tempdir");
    let dest = DuckDb::open(dir.path().join("out.duckdb")).expect("open db");
    let mut config = EngineConfig::new("inc-merge");
    config.write_mode = rdlt_connector::core::WriteMode::Merge {
        key: vec!["id".into()],
    };
    let err = Engine::new(config, source(&fixture.conn_url(), ""), dest)
        .run()
        .await
        .expect_err("merge must be rejected for structured streams");
    let msg = err.to_string().to_lowercase();
    assert!(
        msg.contains("merge") && (msg.contains("structured") || msg.contains("arrow")),
        "{msg}"
    );
}
