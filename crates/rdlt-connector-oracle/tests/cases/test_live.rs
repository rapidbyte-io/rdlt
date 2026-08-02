//! The live cells: one Oracle Free container, the whole read path.

use rdlt_connector_oracle::source::cursor::OracleCursor;
use rdlt_connector_sdk::spi::core::StreamName;
use rdlt_connector_sdk::spi::{Source, StreamSpec};
use rdlt_testkit::{assert_conformant, verify_source};

use super::common::{OracleFixture, incremental, stream};

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
