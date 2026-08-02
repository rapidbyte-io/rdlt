//! The document gate: the schema and the parser agree, and every
//! refusal names its field.

use rdlt_connector_oracle::source::{self, Config, Shell};
use rdlt_connector_sdk::config::Document;

fn base() -> serde_json::Value {
    serde_json::json!({
        "host": "db.example", "service": "FREEPDB1",
        "user": "rdlt", "password": "pw",
        "streams": [{"name": "events", "table": "EVENTS"}]
    })
}

/// The generated schema accepts what the parser accepts, and refuses
/// what it refuses — they come from the same structs.
#[test]
fn the_schema_and_parser_agree() {
    let schema = jsonschema::validator_for(&source::config_schema()).expect("compiles");
    let good = base();
    assert!(schema.is_valid(&good));
    assert!(Shell::from_value(good).is_ok());

    let mut typo = base();
    typo["strems"] = serde_json::json!([]);
    assert!(
        !schema.is_valid(&typo),
        "deny_unknown_fields reaches the schema"
    );
    assert!(Shell::from_value(typo).is_err());
}

/// The port defaults to Oracle's registered listener port, and the
/// tuning defaults are the JDBC-equivalent values an Oracle operator
/// expects.
#[test]
fn the_defaults_match_the_jdbc_equivalents() {
    let config = Config::from_value(base()).expect("valid");
    assert_eq!(config.port, 1521);
    assert_eq!(
        config.tuning.lob_chunk_bytes, 1_048_576,
        "defaultLobPrefetchSize"
    );
    assert_eq!(
        config.tuning.connect_timeout_ms, 60_000,
        "oracle.net.CONNECT_TIMEOUT"
    );
    assert_eq!(
        config.tuning.read_timeout_ms, 600_000,
        "oracle.jdbc.ReadTimeout"
    );
    assert_eq!(config.tuning.sdu_bytes, 8192);
    assert!(
        config.tuning.page_rows.is_none(),
        "derived from widths unless set"
    );
}

/// A legacy instance is named by SID; an operator's known-good JDBC
/// tuning translates field for field.
#[test]
fn a_legacy_sid_instance_with_jdbc_tuning_parses() {
    let mut document = base();
    document["service"] = serde_json::Value::Null;
    document.as_object_mut().expect("object").remove("service");
    document["sid"] = "ORCL".into();
    document["tuning"] = serde_json::json!({
        "page_rows": 25,                 // defaultRowPrefetch=25
        "lob_chunk_bytes": 1_048_576,    // oracle.jdbc.defaultLobPrefetchSize
        "read_timeout_ms": 600_000,      // oracle.jdbc.ReadTimeout
        "connect_timeout_ms": 60_000,    // oracle.net.CONNECT_TIMEOUT
    });
    let config = Config::from_value(document).expect("valid legacy document");
    assert_eq!(config.sid.as_deref(), Some("ORCL"));
    assert_eq!(config.tuning.page_rows, Some(25));
}

/// Every refusal names its field; duplicates are refused at the gate
/// (the reader resolves streams by name and would shadow the twin).
#[test]
fn refusals_name_their_field() {
    let cases: Vec<(serde_json::Value, &str)> = vec![
        (
            {
                let mut v = base();
                v["host"] = "".into();
                v
            },
            "`host` must not be empty",
        ),
        (
            {
                let mut v = base();
                v["service"] = "".into();
                v
            },
            "one of `service` (modern) or `sid` (legacy) is required",
        ),
        (
            {
                let mut v = base();
                v["sid"] = "ORCL".into();
                v
            },
            "`service` and `sid` are two ways to name one instance",
        ),
        (
            {
                let mut v = base();
                v["tuning"] = serde_json::json!({"page_rows": 0});
                v
            },
            "`tuning.page_rows` is 0",
        ),
        (
            {
                let mut v = base();
                v["tuning"] = serde_json::json!({"sdu_bytes": 128});
                v
            },
            "Oracle's minimum session data unit is 512",
        ),
        (
            {
                let mut v = base();
                v["streams"] = serde_json::json!([]);
                v
            },
            "at least one stream is required",
        ),
        (
            {
                let mut v = base();
                v["streams"] = serde_json::json!([
                    {"name": "a", "table": "T"},
                    {"name": "a", "table": "U"}
                ]);
                v
            },
            "duplicate stream name `a`",
        ),
        (
            {
                let mut v = base();
                v["streams"] = serde_json::json!([{"name": "a", "table": ""}]);
                v
            },
            "stream `a`: `table` must not be empty",
        ),
    ];
    for (document, needle) in cases {
        let err = Config::from_value(document)
            .expect_err("refused")
            .to_string();
        assert!(
            err.starts_with("invalid oracle source config: ") && err.contains(needle),
            "{err}"
        );
    }
}

/// The password never renders.
#[test]
fn the_password_is_grep_proof() {
    let config = Config::from_value(base()).expect("valid");
    assert!(!format!("{config:?}").contains("pw"));
}
