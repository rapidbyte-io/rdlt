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

/// The port defaults to Oracle's registered listener port.
#[test]
fn the_port_defaults_to_1521() {
    let config = Config::from_value(base()).expect("valid");
    assert_eq!(config.port, 1521);
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
            "`service` must not be empty",
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
