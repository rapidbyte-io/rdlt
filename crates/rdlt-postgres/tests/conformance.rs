//! T012: source conformance against real Postgres + DuckDB — the full
//! type-matrix round-trip (every contract row), selection modes, hostile
//! identifiers, empty tables, and reflect→read drift.

mod common;

use common::PgFixture;
use rdlt_dest_duckdb::DuckDb;
use rdlt_engine::{Engine, EngineConfig};
use rdlt_postgres::source::PostgresSource;

const TYPE_MATRIX_SEED: &str = r#"
CREATE TYPE mood AS ENUM ('happy', 'grumpy');
CREATE TABLE type_matrix (
    i8   int8 PRIMARY KEY,
    b    bool,
    i2   int2,
    i4   int4,
    f4   float4,
    f8   float8,
    num  numeric(10,2),
    numu numeric,
    numb numeric(50,5),
    t    text,
    vc   varchar(12),
    ch   char(5),
    byt  bytea,
    ts   timestamp,
    tstz timestamptz,
    d    date,
    tm   time,
    u    uuid,
    j    json,
    jb   jsonb,
    md   mood,
    tags text[],
    iv   interval,
    mn   money,
    ip   inet
);
INSERT INTO type_matrix VALUES (
    1, true, 32000, 2000000000, 1.5, 2.25,
    1.25, 1.23456789, 123456789012345678901234567890123456789012345.67891,
    'plain', 'varchar', 'abc', '\xdeadbeef',
    '2026-01-02 03:04:05.123456', '2026-01-02 03:04:05.123456+00',
    '2026-01-02', '03:04:05.123456',
    '550e8400-e29b-41d4-a716-446655440000',
    '{"k": 1}', '{"k": 2}',
    'happy', ARRAY['a','b'], interval '1 day 2 hours', 100.00, '10.0.0.1'
);
INSERT INTO type_matrix (i8) VALUES (2); -- all-NULL row (except PK)
INSERT INTO type_matrix (i8, tstz, d) VALUES (3, 'infinity', '-infinity');
"#;

fn source_for(conn: &str, extra: &str) -> PostgresSource {
    PostgresSource::from_yaml(&format!("conn: \"{conn}\"\n{extra}")).expect("config")
}

async fn run_to_duckdb(source: PostgresSource, pipeline: &str) -> (DuckDb, rdlt_engine::RunReport) {
    let db = tempfile::tempdir().expect("tempdir");
    let dest = DuckDb::open(db.path().join("out.duckdb")).expect("open db");
    // Leak the tempdir so the db outlives this helper (tests read from it).
    std::mem::forget(db);
    let report = Engine::new(EngineConfig::new(pipeline), source, dest.clone())
        .run()
        .await
        .expect("pipeline run");
    (dest, report)
}

#[tokio::test(flavor = "multi_thread")]
async fn type_matrix_round_trip() {
    let fixture = PgFixture::start().await;
    fixture.seed(TYPE_MATRIX_SEED).await;
    let (dest, report) = run_to_duckdb(
        source_for(&fixture.conn_url(), "tables:\n  - name: type_matrix\n"),
        "conf-matrix",
    )
    .await;
    assert_eq!(report.total_rows(), 3);
    assert_eq!(dest.count_rows("type_matrix").expect("count"), 3);

    let val = |col: &str| {
        // query_string reads a single VARCHAR — cast every probe to text.
        dest.query_string(&format!(
            "SELECT CAST(({col}) AS VARCHAR) FROM type_matrix WHERE i8 = 1"
        ))
        .expect(col)
    };
    // Lossless scalar rows (contract table 1).
    assert_eq!(val("b"), "true");
    assert_eq!(val("i2"), "32000");
    assert_eq!(val("i4"), "2000000000");
    assert_eq!(val("f4"), "1.5");
    assert_eq!(val("f8"), "2.25");
    assert_eq!(val("num"), "1.25");
    assert_eq!(val("t"), "plain");
    assert_eq!(val("vc"), "varchar");
    assert_eq!(val("ch"), "abc  ", "char(5) keeps pad spaces as stored");
    assert_eq!(val("hex(byt)"), "DEADBEEF");
    assert!(
        val("ts").starts_with("2026-01-02 03:04:05.123456"),
        "{}",
        val("ts")
    );
    assert!(
        val("tstz").starts_with("2026-01-02 03:04:05.123456"),
        "{}",
        val("tstz")
    );
    assert_eq!(val("d"), "2026-01-02");
    assert_eq!(val("tm"), "03:04:05.123456");
    assert_eq!(val("u"), "550e8400-e29b-41d4-a716-446655440000");
    // Policy rows (contract table 2): canonical text, values survive.
    assert_eq!(
        val("numu"),
        "1.23456789",
        "unconstrained numeric → lossless text"
    );
    assert_eq!(
        val("numb"),
        "123456789012345678901234567890123456789012345.67891",
        "numeric(50,5) beyond decimal128 → lossless text"
    );
    assert_eq!(val("j"), r#"{"k": 1}"#);
    assert_eq!(val("jb"), r#"{"k": 2}"#, "jsonb version byte stripped");
    assert_eq!(val("md"), "happy", "enum label text");
    assert_eq!(val("tags"), r#"["a", "b"]"#, "array → canonical JSON text");
    assert_eq!(val("iv"), "1 day 02:00:00", "interval canonical text");
    assert_eq!(val("mn"), "$100.00", "money canonical text");
    assert_eq!(
        val("ip"),
        "10.0.0.1/32",
        "inet canonical text (PG includes the netmask)"
    );

    // All-NULL row survives as NULLs.
    let nulls = dest
        .query_string("SELECT CAST(count(*) AS VARCHAR) FROM type_matrix WHERE i8 = 2 AND b IS NULL AND jb IS NULL AND mn IS NULL")
        .expect("null row");
    assert_eq!(nulls, "1");

    // ±infinity saturates to extremes — visible, sortable, never NULL.
    let inf = dest
        .query_string("SELECT CAST(count(*) AS VARCHAR) FROM type_matrix WHERE i8 = 3 AND tstz IS NOT NULL AND d IS NOT NULL")
        .expect("infinity row");
    assert_eq!(inf, "1");
    let max_is_inf_row = dest
        .query_string("SELECT CAST(i8 AS VARCHAR) FROM type_matrix WHERE tstz IS NOT NULL ORDER BY tstz DESC LIMIT 1")
        .expect("max tstz");
    assert_eq!(
        max_is_inf_row, "3",
        "+infinity sorts above every real timestamp"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn type_hints_end_to_end() {
    // 006 US2: hinted text→timestamptz lands typed; unconstrained numeric
    // regains decimality via a hint; a failing cast is a typed copy error
    // naming the column.
    let fixture = PgFixture::start().await;
    fixture
        .seed(
            "CREATE TABLE h (id int8 PRIMARY KEY, raw_ts text, amount numeric); \
             INSERT INTO h VALUES (1, '2026-01-02T03:04:05Z', 12.3456);",
        )
        .await;
    let (dest, _) = run_to_duckdb(
        source_for(
            &fixture.conn_url(),
            "tables:\n  - name: h\n    type_hints:\n      raw_ts: timestamp_tz\n      amount: decimal(12,4)\n",
        ),
        "conf-hints",
    )
    .await;
    let ts_type = dest
        .query_string("SELECT CAST(typeof(raw_ts) AS VARCHAR) FROM h")
        .expect("typeof");
    assert!(
        ts_type.contains("TIMESTAMP"),
        "hinted column lands typed: {ts_type}"
    );
    let amount = dest
        .query_string("SELECT CAST(amount AS VARCHAR) FROM h")
        .expect("amount");
    assert_eq!(amount, "12.3456", "decimal hint restores decimality");

    // Cast failure: text that is not a timestamp → typed copy-phase error.
    let fixture2 = PgFixture::start().await;
    fixture2
        .seed(
            "CREATE TABLE h (id int8 PRIMARY KEY, raw_ts text); \
             INSERT INTO h VALUES (1, 'not-a-timestamp');",
        )
        .await;
    let source = source_for(
        &fixture2.conn_url(),
        "tables:\n  - name: h\n    type_hints:\n      raw_ts: timestamp_tz\n",
    );
    let db = tempfile::tempdir().expect("tempdir");
    let dest = DuckDb::open(db.path().join("out.duckdb")).expect("open db");
    let err = Engine::new(EngineConfig::new("conf-hint-fail"), source, dest)
        .run()
        .await
        .expect_err("failing cast must be typed, never silent");
    assert!(err.to_string().contains("copy phase"), "{err}");
}

#[tokio::test(flavor = "multi_thread")]
async fn partitioned_tables_load_once_via_parent() {
    // 005 review finding: without a relispartition filter, schema-wide
    // discovery streamed BOTH the partitioned parent and every leaf,
    // double-loading every row.
    let fixture = PgFixture::start().await;
    fixture
        .seed(
            "CREATE TABLE metrics (day date NOT NULL, v int8) PARTITION BY RANGE (day); \
             CREATE TABLE metrics_jan PARTITION OF metrics \
                 FOR VALUES FROM ('2026-01-01') TO ('2026-02-01'); \
             CREATE TABLE metrics_feb PARTITION OF metrics \
                 FOR VALUES FROM ('2026-02-01') TO ('2026-03-01'); \
             INSERT INTO metrics VALUES ('2026-01-05', 1), ('2026-02-05', 2), ('2026-02-06', 3);",
        )
        .await;
    let (dest, report) = run_to_duckdb(source_for(&fixture.conn_url(), ""), "conf-part").await;
    assert_eq!(
        report.total_rows(),
        3,
        "each partitioned row loads exactly once"
    );
    assert_eq!(dest.count_rows("metrics").expect("parent stream"), 3);
    assert!(
        dest.count_rows("metrics_jan").is_err() && dest.count_rows("metrics_feb").is_err(),
        "leaf partitions must not become their own streams"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_wide_discovery_and_views() {
    let fixture = PgFixture::start().await;
    fixture
        .seed(
            r#"
CREATE TABLE alpha (id int8 PRIMARY KEY, v text);
CREATE TABLE beta (id int8 PRIMARY KEY);
INSERT INTO alpha VALUES (1, 'x');
INSERT INTO beta VALUES (7);
CREATE VIEW gamma AS SELECT id FROM alpha;
"#,
        )
        .await;

    // No table list ⇒ every table in the schema; views excluded by default.
    let (dest, _) = run_to_duckdb(source_for(&fixture.conn_url(), ""), "conf-discovery").await;
    assert_eq!(dest.count_rows("alpha").expect("alpha"), 1);
    assert_eq!(dest.count_rows("beta").expect("beta"), 1);
    assert!(
        dest.count_rows("gamma").is_err(),
        "views excluded by default"
    );

    // include_views: the view becomes a stream.
    let (dest, _) = run_to_duckdb(
        source_for(&fixture.conn_url(), "include_views: true\n"),
        "conf-views",
    )
    .await;
    assert_eq!(dest.count_rows("gamma").expect("gamma"), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn hostile_identifiers_and_column_selection() {
    let fixture = PgFixture::start().await;
    fixture
        .seed(
            r#"
CREATE TABLE "Order ""Items""" (
    "Id" int8 PRIMARY KEY,
    "select" text,
    secret text
);
INSERT INTO "Order ""Items""" VALUES (1, 'kw', 'hidden');
"#,
        )
        .await;
    let (dest, _) = run_to_duckdb(
        source_for(
            &fixture.conn_url(),
            "tables:\n  - name: 'Order \"Items\"'\n    excluded_columns: [secret]\n",
        ),
        "conf-idents",
    )
    .await;
    // The DESTINATION normalizes identifiers (engine ident rules):
    // `Order "Items"` lands as `order__items_`. The source's job was reading
    // the hostile names faithfully; the rename is downstream policy.
    assert_eq!(dest.count_rows("order__items_").expect("count"), 1);
    let v = dest
        .query_string(r#"SELECT CAST("select" AS VARCHAR) FROM order__items_"#)
        .expect("keyword column");
    assert_eq!(v, "kw");
    let secret_cols = dest
        .query_string(
            "SELECT CAST(count(*) AS VARCHAR) FROM information_schema.columns \
             WHERE table_name = 'order__items_' AND column_name = 'secret'",
        )
        .expect("column probe");
    assert_eq!(
        secret_cols, "0",
        "excluded column must not exist downstream"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn empty_table_materializes_with_schema() {
    let fixture = PgFixture::start().await;
    fixture
        .seed("CREATE TABLE hollow (id int8 PRIMARY KEY, v numeric(6,3));")
        .await;
    let (dest, report) = run_to_duckdb(
        source_for(&fixture.conn_url(), "tables:\n  - name: hollow\n"),
        "conf-empty",
    )
    .await;
    assert_eq!(report.total_rows(), 0);
    assert_eq!(
        dest.count_rows("hollow")
            .expect("table exists with declared schema"),
        0
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn drift_column_added_dropped_retyped() {
    use rdlt_connector::Source as _;

    // ADDED between reflect and read: this run projects the reflected
    // columns only (discovery is once per run); the NEXT run evolves.
    let fixture = PgFixture::start().await;
    fixture
        .seed("CREATE TABLE d (id int8 PRIMARY KEY, v text); INSERT INTO d VALUES (1, 'x');")
        .await;
    let source = source_for(&fixture.conn_url(), "tables:\n  - name: d\n");
    source.streams().await.expect("reflect");
    fixture
        .seed("ALTER TABLE d ADD COLUMN extra int4; UPDATE d SET extra = 7;")
        .await;
    let db = tempfile::tempdir().expect("tempdir");
    let dest = DuckDb::open(db.path().join("out.duckdb")).expect("open db");
    std::mem::forget(db);
    Engine::new(EngineConfig::new("drift-add"), source, dest.clone())
        .run()
        .await
        .expect("added column is invisible this run, never misaligned");
    assert!(
        dest.query_string("SELECT CAST(extra AS VARCHAR) FROM d")
            .is_err(),
        "column added after reflection must not appear this run"
    );
    // Next run (fresh reflection) picks it up via schema evolution.
    let source = source_for(&fixture.conn_url(), "tables:\n  - name: d\n");
    Engine::new(EngineConfig::new("drift-add"), source, dest.clone())
        .run()
        .await
        .expect("second run evolves");
    assert_eq!(
        // Append-mode snapshot re-runs duplicate rows by design (run 1's
        // copy has NULL in the evolved column) — aggregate over the copies.
        dest.query_string("SELECT CAST(max(extra) AS VARCHAR) FROM d")
            .expect("evolved column"),
        "7"
    );

    // DROPPED between reflect and read: typed error, never misaligned data.
    let fixture = PgFixture::start().await;
    fixture
        .seed("CREATE TABLE d (id int8 PRIMARY KEY, v text); INSERT INTO d VALUES (1, 'x');")
        .await;
    let source = source_for(&fixture.conn_url(), "tables:\n  - name: d\n");
    source.streams().await.expect("reflect");
    fixture.seed("ALTER TABLE d DROP COLUMN v;").await;
    let db = tempfile::tempdir().expect("tempdir");
    let dest = DuckDb::open(db.path().join("out.duckdb")).expect("open db");
    let err = Engine::new(EngineConfig::new("drift-drop"), source, dest)
        .run()
        .await
        .expect_err("dropped column must fail loudly");
    assert!(err.to_string().contains("copy phase"), "{err}");

    // RETYPED between reflect and read: the wire shape no longer matches the
    // reflected decode plan — a typed DECODE error, never silent coercion.
    let fixture = PgFixture::start().await;
    fixture
        .seed("CREATE TABLE d (id int8 PRIMARY KEY, n int4); INSERT INTO d VALUES (1, 5);")
        .await;
    let source = source_for(&fixture.conn_url(), "tables:\n  - name: d\n");
    source.streams().await.expect("reflect");
    fixture
        .seed("ALTER TABLE d ALTER COLUMN n TYPE int8;")
        .await;
    let db = tempfile::tempdir().expect("tempdir");
    let dest = DuckDb::open(db.path().join("out.duckdb")).expect("open db");
    let err = Engine::new(EngineConfig::new("drift-retype"), source, dest)
        .run()
        .await
        .expect_err("retyped column must fail loudly");
    let msg = err.to_string();
    assert!(
        msg.contains("decode") || msg.contains("copy phase"),
        "{msg}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn drift_table_dropped_between_reflect_and_read() {
    let fixture = PgFixture::start().await;
    fixture
        .seed("CREATE TABLE doomed (id int8 PRIMARY KEY); INSERT INTO doomed VALUES (1);")
        .await;
    let source = source_for(&fixture.conn_url(), "tables:\n  - name: doomed\n");
    // Prime reflection (as the engine's stream discovery would)…
    use rdlt_connector::Source as _;
    let specs = source.streams().await.expect("streams");
    assert_eq!(specs.len(), 1);
    // …then drift: the table vanishes before read.
    fixture.seed("DROP TABLE doomed;").await;

    let db = tempfile::tempdir().expect("tempdir");
    let dest = DuckDb::open(db.path().join("out.duckdb")).expect("open db");
    let err = Engine::new(EngineConfig::new("conf-drift"), source, dest)
        .run()
        .await
        .expect_err("dropped table must fail loudly");
    let msg = err.to_string();
    assert!(
        msg.contains("copy phase") && msg.contains("doomed"),
        "typed error names phase + table: {msg}"
    );
}
