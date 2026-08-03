//! The live cells: one Oracle Free container, the whole read path.

use rdlt_connector_oracle::source::cursor::OracleCursor;
use rdlt_connector_sdk::spi::core::StreamName;
use rdlt_connector_sdk::spi::{Source, StreamSpec};
use rdlt_testkit::{assert_conformant, verify_source};

use super::common;
use super::common::{Flavor, OracleFixture, incremental, stream};

/// Collect one stream's rows through the SPI, returning the JSON
/// objects and the final cursor.
async fn read_all(
    shell: &rdlt_connector_oracle::source::Shell,
    name: &str,
    since: Option<rdlt_connector_sdk::spi::core::Cursor>,
) -> (
    Vec<serde_json::Value>,
    Option<rdlt_connector_sdk::spi::core::Cursor>,
) {
    use rdlt_connector_sdk::spi::PushPayload;

    let (out, mut incoming) = rdlt_connector_sdk::spi::records_channel(64 << 20);
    let spec = StreamSpec::new(name);
    let reader = shell.read(rdlt_connector_sdk::spi::ReadRequest {
        stream: spec,
        since,
        out,
    });
    let collect = async {
        let mut rows = Vec::new();
        let mut cursor = None;
        while let Some(push) = incoming.recv().await {
            match push.payload {
                PushPayload::RawJson(bytes) => {
                    for line in bytes.split(|b| *b == b'\n') {
                        if line.is_empty() {
                            continue;
                        }
                        rows.push(serde_json::from_slice(line).expect("ndjson line"));
                    }
                }
                PushPayload::Checkpoint(c) => cursor = Some(c),
                _ => {}
            }
        }
        (rows, cursor)
    };
    let (result, collected) = tokio::join!(reader, collect);
    result.expect("the read settles");
    collected
}

/// The whole shape at once: types survive the round trip, a large
/// row count streams past the driver's old 100-row ceiling, LOBs
/// arrive whole at megabyte scale, and the watermark resumes.
#[tokio::test(flavor = "multi_thread")]
async fn the_read_path_holds_against_a_live_database() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };

    // A table exercising the type rulebook's interesting rows.
    fixture
        .seed(&[
            "CREATE TABLE TYPES_T (
                ID NUMBER(10) PRIMARY KEY,
                SMALL_N NUMBER(8,2),
                BIG_N NUMBER,
                TXT VARCHAR2(100),
                D DATE,
                TS TIMESTAMP,
                TSTZ TIMESTAMP WITH TIME ZONE,
                B RAW(16)
            )",
            "INSERT INTO TYPES_T VALUES (1, 12.34, \
             12345678901234567890123456789012345678, 'hello', \
             DATE '2026-01-02', TIMESTAMP '2026-01-02 03:04:05.678', \
             TIMESTAMP '2026-01-02 03:04:05.678 +02:00', HEXTORAW('DEADBEEF'))",
        ])
        .await;

    let shell = fixture.shell(&[stream("types", "TYPES_T")]);
    let (rows, _) = read_all(&shell, "types", None).await;
    assert_eq!(rows.len(), 1);
    let row = &rows[0];
    assert_eq!(row["id"], serde_json::json!(1));
    assert_eq!(row["txt"], serde_json::json!("hello"));
    assert_eq!(
        row["big_n"],
        serde_json::json!("12345678901234567890123456789012345678"),
        "bare NUMBER keeps all 38 digits as exact text"
    );
    assert!(
        row["d"]
            .as_str()
            .expect("date")
            .starts_with("2026-01-02T00:00:00"),
        "Oracle DATE carries time-of-day: {}",
        row["d"]
    );
    assert!(
        row["tstz"].as_str().expect("tstz").contains("+02:00"),
        "{}",
        row["tstz"]
    );
    assert_eq!(row["b"], serde_json::json!("deadbeef"));

    // Volume: far past the old 100-row ceiling, exact count.
    fixture
        .seed(&[
            "CREATE TABLE BULK_T (ID NUMBER(8) PRIMARY KEY, V VARCHAR2(40))",
            "INSERT INTO BULK_T SELECT LEVEL, 'row-'||LEVEL FROM DUAL CONNECT BY LEVEL <= 5000",
        ])
        .await;
    let shell = fixture.shell(&[stream("bulk", "BULK_T")]);
    let (rows, _) = read_all(&shell, "bulk", None).await;
    assert_eq!(
        rows.len(),
        5000,
        "every row crosses, not just the first page"
    );

    // LOBs at megabyte scale, whole.
    fixture
        .seed(&[
            "CREATE TABLE LOB_T (ID NUMBER(4) PRIMARY KEY, DOC CLOB, BIN BLOB)",
            "DECLARE c CLOB; b BLOB; chunk VARCHAR2(1024) := RPAD('x', 1024, 'x'); \
             braw RAW(1024) := UTL_RAW.CAST_TO_RAW(RPAD('y', 1024, 'y')); \
             BEGIN INSERT INTO LOB_T VALUES (1, EMPTY_CLOB(), EMPTY_BLOB()) RETURNING DOC, BIN INTO c, b; \
             FOR i IN 1..2048 LOOP DBMS_LOB.WRITEAPPEND(c, 1024, chunk); \
             DBMS_LOB.WRITEAPPEND(b, 1024, braw); END LOOP; COMMIT; END;",
        ])
        .await;
    let shell = fixture.shell(&[stream("lobs", "LOB_T")]);
    let (rows, _) = read_all(&shell, "lobs", None).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]["doc"].as_str().expect("clob").len(),
        2 * 1024 * 1024,
        "a 2 MiB CLOB arrives whole"
    );
    assert_eq!(
        rows[0]["bin"].as_str().expect("blob").len(),
        2 * 1024 * 1024 * 2,
        "a 2 MiB BLOB arrives whole (hex-rendered)"
    );

    // Incremental: the watermark resumes and never re-reads.
    fixture
        .seed(&[
            "CREATE TABLE INC_T (ID NUMBER(8) PRIMARY KEY, V VARCHAR2(20))",
            "INSERT INTO INC_T SELECT LEVEL, 'v' FROM DUAL CONNECT BY LEVEL <= 150",
        ])
        .await;
    let shell = fixture.shell(&[incremental("inc", "INC_T", "ID")]);
    let (first, cursor) = read_all(&shell, "inc", None).await;
    assert_eq!(first.len(), 150);
    let cursor = cursor.expect("a checkpoint landed");
    let decoded = OracleCursor::decode(Some(&cursor)).expect("decodes");
    assert_eq!(decoded.streams["inc"].watermark, "150");

    fixture
        .seed(&["INSERT INTO INC_T VALUES (151, 'new')"])
        .await;
    let (second, _) = read_all(&shell, "inc", Some(cursor)).await;
    assert_eq!(second.len(), 1, "only the new row");
    assert_eq!(second[0]["id"], serde_json::json!(151));
}

/// Resume must hold when PHYSICAL order disagrees with cursor order.
///
/// Rows are paged by `(cursor, ROWID)` precisely so a checkpoint is a
/// true low-water boundary. Seeding rows whose ROWID order is the
/// REVERSE of their ID order makes a ROWID-ordered read checkpoint a
/// watermark it has not reached — the defect this shape pins.
#[tokio::test(flavor = "multi_thread")]
async fn resume_holds_when_physical_order_disagrees_with_the_cursor() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE SHUFFLE_T (ID NUMBER(8) PRIMARY KEY, V VARCHAR2(20))",
            // Descending inserts: physical order is the reverse of ID order.
            "INSERT INTO SHUFFLE_T SELECT 400 - LEVEL, 'v' FROM DUAL CONNECT BY LEVEL <= 300",
        ])
        .await;
    let shell = fixture.shell(&[incremental("shuffled", "SHUFFLE_T", "ID")]);
    let (rows, cursor) = read_all(&shell, "shuffled", None).await;
    assert_eq!(rows.len(), 300);
    let cursor = cursor.expect("a checkpoint landed");

    // Everything at or below the watermark was delivered, so a resume
    // reads nothing — and adding a row above it reads exactly that one.
    let (again, _) = read_all(&shell, "shuffled", Some(cursor.clone())).await;
    assert!(
        again.is_empty(),
        "a completed stream re-reads nothing: {}",
        again.len()
    );
    fixture
        .seed(&["INSERT INTO SHUFFLE_T VALUES (9999, 'new')"])
        .await;
    let (delta, _) = read_all(&shell, "shuffled", Some(cursor)).await;
    assert_eq!(delta.len(), 1);
    assert_eq!(delta[0]["id"], serde_json::json!(9999));
}

/// A cursor column that does not exist is refused — silently
/// re-reading the whole table on every run is not an option.
#[tokio::test(flavor = "multi_thread")]
async fn a_missing_cursor_column_is_refused() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&["CREATE TABLE GHOST_T (ID NUMBER(8) PRIMARY KEY)"])
        .await;
    let shell = fixture.shell(&[incremental("ghost", "GHOST_T", "CREATED_ON")]);
    let (out, _keep) = rdlt_connector_sdk::spi::records_channel(1 << 20);
    let err = shell
        .read(rdlt_connector_sdk::spi::ReadRequest {
            stream: StreamSpec::new("ghost"),
            since: None,
            out,
        })
        .await
        .expect_err("refused")
        .to_string();
    assert!(
        err.contains("cursor column `CREATED_ON` is not a column")
            && err.contains("silently re-read everything"),
        "{err}"
    );
}

/// The sdk conformance kit certifies the Shell against the live
/// database — "certified = passes conformance".
#[tokio::test(flavor = "multi_thread")]
async fn the_source_is_conformant() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE CONF_T (ID NUMBER(8) PRIMARY KEY, V VARCHAR2(20))",
            "INSERT INTO CONF_T SELECT LEVEL, 'c'||LEVEL FROM DUAL CONNECT BY LEVEL <= 7",
        ])
        .await;
    let shell = fixture.shell(&[incremental("conf_stream", "CONF_T", "ID")]);
    assert_conformant(verify_source(&shell).await);
    let _ = StreamName::new("conf_stream");
}

/// A NULLABLE cursor column is refused BEFORE any row moves.
///
/// The timing is the whole point. Refusing at the row was worse than
/// useless: Oracle sorts NULLs LAST, so the failure landed on the
/// final page of the FIRST run, after most of the table had already
/// been delivered and checkpointed. Every later run resumed with
/// `WHERE c > :watermark`, which never matches NULL, so it went green
/// and those rows were silently absent forever — the loud refusal was
/// unreachable exactly when it mattered.
#[tokio::test(flavor = "multi_thread")]
async fn a_null_cursor_value_is_refused() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE NULLC_T (ID NUMBER(8) PRIMARY KEY, TS DATE)",
            "INSERT INTO NULLC_T SELECT LEVEL, DATE '2026-01-01' FROM DUAL CONNECT BY LEVEL <= 5",
            "INSERT INTO NULLC_T VALUES (99, NULL)",
        ])
        .await;
    let shell = fixture.shell(&[incremental("nullc", "NULLC_T", "TS")]);
    let (out, _keep) = rdlt_connector_sdk::spi::records_channel(1 << 20);
    let err = shell
        .read(rdlt_connector_sdk::spi::ReadRequest {
            stream: StreamSpec::new("nullc"),
            since: None,
            out,
        })
        .await
        .expect_err("refused")
        .to_string();
    assert!(
        err.contains("cursor column `TS` admits NULLs") && err.contains("NOT NULL"),
        "{err}"
    );
    // And it is refused on a table where no row is NULL yet — the
    // COLUMN's nullability is the fact, not today's contents.
    fixture
        .seed(&["CREATE TABLE NULLC_EMPTY (ID NUMBER(8) PRIMARY KEY, TS DATE)"])
        .await;
    let shell = fixture.shell(&[incremental("e", "NULLC_EMPTY", "TS")]);
    let (out, _keep) = rdlt_connector_sdk::spi::records_channel(1 << 20);
    let err = shell
        .read(rdlt_connector_sdk::spi::ReadRequest {
            stream: StreamSpec::new("e"),
            since: None,
            out,
        })
        .await
        .expect_err("refused before a single row")
        .to_string();
    assert!(err.contains("admits NULLs"), "{err}");
}

/// A DATE cursor resumes — the rendering and the SQL format model
/// have to agree exactly, which they did not before this was pinned.
#[tokio::test(flavor = "multi_thread")]
async fn a_date_cursor_resumes_across_runs() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE DATEC_T (ID NUMBER(8) PRIMARY KEY, TS DATE NOT NULL)",
            "INSERT INTO DATEC_T SELECT LEVEL, DATE '2026-01-01' + LEVEL FROM DUAL \
             CONNECT BY LEVEL <= 40",
        ])
        .await;
    let shell = fixture.shell(&[incremental("datec", "DATEC_T", "TS")]);
    let (first, cursor) = read_all(&shell, "datec", None).await;
    assert_eq!(first.len(), 40);
    let cursor = cursor.expect("a checkpoint landed");

    fixture
        .seed(&["INSERT INTO DATEC_T VALUES (999, DATE '2027-06-01')"])
        .await;
    let (delta, _) = read_all(&shell, "datec", Some(cursor)).await;
    assert_eq!(delta.len(), 1, "a DATE watermark resumes exactly");
    assert_eq!(delta[0]["id"], serde_json::json!(999));
}

/// The SAME read path against Oracle XE **21c**.
///
/// The connector claims to work across Oracle generations, and a
/// claim no cell exercises is a wish. What this actually exercises is
/// the EXECUTE path's capability negotiation, the older server's type
/// and ROWID behaviour, and a cursor resume — all on a wire that
/// omits fields 23ai sends.
///
/// It does NOT certify the vendored FETCH patch, whatever an earlier
/// version of this comment claimed: `FetchMessage` is built at one
/// site, `fetch_more`, and this connector never continues a cursor,
/// so that capability read is unreachable from here.
///
/// Deliberately one cell, not the whole suite: 21c costs another
/// ~75 s of gate time, and what differs across generations is the
/// wire and the types, both of which this exercises.
#[tokio::test(flavor = "multi_thread")]
async fn the_read_path_holds_against_oracle_21c() {
    let Some(fixture) = OracleFixture::start_on(Flavor::Xe21).await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE XE_T (\
               ID NUMBER(8) PRIMARY KEY, \
               NAME VARCHAR2(64), \
               AMOUNT NUMBER(12,2), \
               TS TIMESTAMP WITH TIME ZONE NOT NULL, \
               BODY CLOB, \
               RAWS BLOB)",
            "INSERT INTO XE_T VALUES (1, 'first', 10.25, \
             TIMESTAMP '2026-01-01 10:00:00 +00:00', 'hello', \
             UTL_RAW.CAST_TO_RAW('bytes'))",
            "INSERT INTO XE_T VALUES (2, 'sécond', -3.5, \
             TIMESTAMP '2026-01-02 10:00:00 +00:00', NULL, NULL)",
            "INSERT INTO XE_T SELECT LEVEL + 10, 'bulk', LEVEL, \
             TIMESTAMP '2026-02-01 10:00:00 +00:00' + NUMTODSINTERVAL(LEVEL, 'SECOND'), \
             NULL, NULL FROM DUAL CONNECT BY LEVEL <= 600",
        ])
        .await;

    let shell = fixture.shell(&[incremental("xe", "XE_T", "TS")]);
    let (rows, cursor) = read_all(&shell, "xe", None).await;
    assert_eq!(rows.len(), 602, "every row crosses the 21c wire");
    assert_eq!(rows[0]["name"], serde_json::json!("first"));
    // Exact Decimal crosses as TEXT by design — a JSON number is an
    // IEEE double and cannot carry NUMBER(12,2) losslessly.
    assert_eq!(rows[0]["amount"], serde_json::json!("10.25"));
    assert_eq!(rows[1]["amount"], serde_json::json!("-3.5"));
    assert_eq!(rows[0]["body"], serde_json::json!("hello"));
    assert_eq!(rows[1]["name"], serde_json::json!("sécond"), "utf-8 intact");
    assert!(rows[1]["body"].is_null(), "a NULL LOB stays null");

    // And it resumes: the older server's ROWID and timestamp
    // renderings have to survive a round trip through the cursor.
    let cursor = cursor.expect("a checkpoint landed");
    fixture
        .seed(&[
            "INSERT INTO XE_T VALUES (9999, 'late', 1, \
             TIMESTAMP '2027-01-01 10:00:00 +00:00', NULL, NULL)",
        ])
        .await;
    let (delta, _) = read_all(&shell, "xe", Some(cursor)).await;
    assert_eq!(delta.len(), 1, "21c resumes exactly");
    assert_eq!(delta[0]["id"], serde_json::json!(9999));
}

/// NVARCHAR2 and NCHAR survive the wire.
///
/// They did not. The driver advertises UTF-16 for the national
/// character set at connect but decoded every character column as
/// UTF-8, so `N'zażółć'` arrived as `"\0z\0a\u{1}|..."` — destroyed,
/// unrecoverable, with no error anywhere, while the VARCHAR2 twin in
/// the same row was fine. Both research.md and plan.md claimed these
/// types were lossless and no cell covered them.
#[tokio::test(flavor = "multi_thread")]
async fn national_character_types_survive_the_wire() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE NCHAR_T (ID NUMBER(4) PRIMARY KEY, NV NVARCHAR2(50), \
             NC NCHAR(6), V VARCHAR2(50))",
            "INSERT INTO NCHAR_T VALUES (1, N'zażółć', N'abc', 'zażółć')",
        ])
        .await;
    let shell = fixture.shell(&[stream("n", "NCHAR_T")]);
    let (rows, _) = read_all(&shell, "n", None).await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["nv"], serde_json::json!("zażółć"), "NVARCHAR2");
    assert_eq!(rows[0]["nc"], serde_json::json!("abc   "), "NCHAR is padded");
    assert_eq!(rows[0]["v"], serde_json::json!("zażółć"), "VARCHAR2 twin");
}

/// BINARY_FLOAT and BINARY_DOUBLE decode as numbers, and the
/// non-finite values Oracle legally stores do not kill the table.
///
/// The driver had no arm for either type, so the catch-all applied a
/// lossy UTF-8 decode to raw IEEE bytes and the connector kept the
/// mojibake. It surfaced only because Postgres refused an embedded
/// NUL.
#[tokio::test(flavor = "multi_thread")]
async fn binary_float_and_double_decode_as_numbers() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE BINF_T (ID NUMBER(4) PRIMARY KEY, F BINARY_FLOAT, D BINARY_DOUBLE)",
            "INSERT INTO BINF_T VALUES (1, 1.5F, 2.25D)",
            "INSERT INTO BINF_T VALUES (2, -0.5F, -1234.5D)",
            "INSERT INTO BINF_T VALUES (3, BINARY_FLOAT_INFINITY, BINARY_DOUBLE_NAN)",
        ])
        .await;
    let shell = fixture.shell(&[stream("b", "BINF_T")]);
    let (rows, _) = read_all(&shell, "b", None).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["f"], serde_json::json!(1.5));
    assert_eq!(rows[0]["d"], serde_json::json!(2.25));
    assert_eq!(rows[1]["f"], serde_json::json!(-0.5), "negatives invert");
    assert_eq!(rows[1]["d"], serde_json::json!(-1234.5));
    // JSON has no literal for these; null beats making one sensor
    // reading render the whole table unreadable.
    assert!(rows[2]["f"].is_null(), "infinity crosses as null");
    assert!(rows[2]["d"].is_null(), "NaN crosses as null");
}

/// A table is reachable by its SCHEMA-qualified name, and a
/// case-sensitive name is reachable by quoting it.
///
/// `HR.EMPLOYEES` — an app schema read by a separate user, the
/// ordinary Oracle estate shape — was quoted as ONE identifier,
/// `"HR.EMPLOYEES"`, which no table has ever been called.
#[tokio::test(flavor = "multi_thread")]
async fn qualified_and_case_sensitive_tables_are_reachable() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE QUAL_T (ID NUMBER(4) PRIMARY KEY)",
            "INSERT INTO QUAL_T VALUES (7)",
            "CREATE TABLE \"lower_t\" (ID NUMBER(4) PRIMARY KEY)",
            "INSERT INTO \"lower_t\" VALUES (9)",
        ])
        .await;
    let shell = fixture.shell(&[
        stream("qualified", &format!("{}.QUAL_T", common::APP_USER)),
        stream("lower", "\"lower_t\""),
    ]);
    let (qualified, _) = read_all(&shell, "qualified", None).await;
    assert_eq!(qualified.len(), 1);
    assert_eq!(qualified[0]["id"], serde_json::json!(7));

    let (lower, _) = read_all(&shell, "lower", None).await;
    assert_eq!(lower.len(), 1, "a quoted lower-case name reaches its table");
    assert_eq!(lower[0]["id"], serde_json::json!(9));
}

/// Two columns differing only in case are refused, not silently
/// collapsed — 030's duplicate-CSV-header defect in a new costume.
#[tokio::test(flavor = "multi_thread")]
async fn columns_differing_only_in_case_are_refused() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&["CREATE TABLE DUPC_T (\"id\" NUMBER(4), \"ID\" NUMBER(4))"])
        .await;
    let shell = fixture.shell(&[stream("d", "DUPC_T")]);
    let (out, _keep) = rdlt_connector_sdk::spi::records_channel(1 << 20);
    let err = shell
        .read(rdlt_connector_sdk::spi::ReadRequest {
            stream: StreamSpec::new("d"),
            since: None,
            out,
        })
        .await
        .expect_err("refused")
        .to_string();
    assert!(err.contains("differ only in case"), "{err}");
}

/// A row too wide for one packet fails BY NAME, and FATALLY.
///
/// It used to derive a one-row page anyway, hand the impossible read
/// to the wire, and surface as a buffer underflow — which classified
/// TRANSIENT, so the engine retried a permanently impossible read
/// until its budget ran out, giving the operator protocol noise
/// instead of the remedy.
#[tokio::test(flavor = "multi_thread")]
async fn a_row_wider_than_the_packet_is_refused_by_name() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE WIDE_T (ID NUMBER(4), A VARCHAR2(4000), B VARCHAR2(4000))",
            "INSERT INTO WIDE_T VALUES (1, RPAD('a',4000,'a'), RPAD('b',4000,'b'))",
        ])
        .await;
    let shell = fixture.shell(&[stream("w", "WIDE_T")]);
    let (out, _keep) = rdlt_connector_sdk::spi::records_channel(1 << 20);
    let err = shell
        .read(rdlt_connector_sdk::spi::ReadRequest {
            stream: StreamSpec::new("w"),
            since: None,
            out,
        })
        .await
        .expect_err("refused");
    let text = err.to_string();
    assert!(
        text.contains("wider than one session data unit"),
        "names the cause: {text}"
    );
    assert!(
        !matches!(err, rdlt_connector_sdk::spi::SourceError::Transient(_)),
        "a permanently impossible read must not be retried: {text}"
    );
}

/// The connector DECLARES the types it derived, so a decimal does not
/// land as TEXT downstream.
///
/// Every column's exact `LogicalType` was computed from the server's
/// describe and then thrown away — nothing reached the engine, and
/// the config's own `type_hints` escape hatch had zero readers. An
/// operator's hint overrides the derivation, which is what the hatch
/// is for.
#[tokio::test(flavor = "multi_thread")]
async fn derived_types_are_declared_and_hints_override_them() {
    use rdlt_connector_sdk::spi::core::LogicalType;

    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE DECL_T (ID NUMBER(8) PRIMARY KEY, AMOUNT NUMBER(12,2), \
             RAWS RAW(16), TS TIMESTAMP WITH TIME ZONE, BIG NUMBER)",
        ])
        .await;
    let declared: rdlt_connector_oracle::source::Stream = serde_json::from_value(serde_json::json!({
        "name": "d", "table": "DECL_T",
        // Bare NUMBER is derived conservatively as text; the operator
        // knows this one's real domain.
        "type_hints": {"big": "int64"}
    }))
    .expect("stream document");
    let shell = fixture.shell(&[declared]);
    let specs = shell.streams().await.expect("discovery");
    let hints = &specs[0].type_hints;

    assert_eq!(hints.get("id"), Some(&LogicalType::Int64));
    assert_eq!(
        hints.get("amount"),
        Some(&LogicalType::Decimal {
            precision: 12,
            scale: 2
        }),
        "a decimal is declared exactly, not left to land as TEXT"
    );
    assert_eq!(hints.get("raws"), Some(&LogicalType::Binary));
    assert_eq!(hints.get("ts"), Some(&LogicalType::TimestampTz));
    assert_eq!(
        hints.get("big"),
        Some(&LogicalType::Int64),
        "the operator's hint wins over the conservative derivation"
    );
}

/// A read of many pages survives the server's open-cursor limit.
///
/// It did not. Every page is a distinct SQL text, the driver's
/// statement cache holds a server cursor per text, and nothing ever
/// closes one — so a read died at ~297 pages whatever the page size,
/// which for an ordinary table is somewhere around 60,000 rows.
/// MEASURED twice independently, and confirmed by raising the
/// server's own `open_cursors` to 20,000, at which the identical read
/// completed.
///
/// 1,000 pages here is four connection recycles.
#[tokio::test(flavor = "multi_thread")]
async fn a_read_survives_more_pages_than_the_server_allows_cursors() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE MANYP_T (ID NUMBER(8) PRIMARY KEY, V VARCHAR2(16))",
            "INSERT INTO MANYP_T SELECT LEVEL, 'v' FROM DUAL CONNECT BY LEVEL <= 5000",
        ])
        .await;
    let shell = fixture.shell_tuned(&[stream("m", "MANYP_T")], serde_json::json!({"page_rows": 5}));
    let (rows, _) = read_all(&shell, "m", None).await;
    assert_eq!(
        rows.len(),
        5000,
        "1000 pages on a server that allows 300 open cursors"
    );
}

/// A bare `NUMBER` works as a cursor.
///
/// It did not, and that is how Oracle estates spell a
/// sequence-backed surrogate key — the single most common
/// incremental cursor there is. Because Oracle accepts any magnitude
/// in a bare NUMBER it maps to text (exact digits), and the cursor
/// gate refused it with a message saying the column was not numeric,
/// about a column that is numeric and monotonic. The operator's only
/// recourse was redeclaring the table.
#[tokio::test(flavor = "multi_thread")]
async fn a_bare_number_works_as_a_cursor() {
    let Some(fixture) = OracleFixture::start().await else {
        return;
    };
    fixture
        .seed(&[
            "CREATE TABLE BAREN_T (ID NUMBER NOT NULL PRIMARY KEY, V VARCHAR2(16))",
            "INSERT INTO BAREN_T SELECT LEVEL, 'v' FROM DUAL CONNECT BY LEVEL <= 150",
        ])
        .await;
    let shell = fixture.shell(&[incremental("bare", "BAREN_T", "ID")]);
    let (rows, cursor) = read_all(&shell, "bare", None).await;
    assert_eq!(rows.len(), 150);

    fixture
        .seed(&["INSERT INTO BAREN_T VALUES (99999, 'late')"])
        .await;
    let (delta, _) = read_all(&shell, "bare", Some(cursor.expect("checkpoint"))).await;
    assert_eq!(delta.len(), 1, "and it resumes numerically, not lexically");
    // Bare NUMBER crosses as exact TEXT — Oracle accepts 38 digits
    // in it and a JSON number is an IEEE double.
    assert_eq!(delta[0]["id"], serde_json::json!("99999"));
}
