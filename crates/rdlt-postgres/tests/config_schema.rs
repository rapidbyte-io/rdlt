//! T019: the declared config schema is GENERATED from the config structs —
//! examples validate, unknown fields fail schema AND parser identically.

use jsonschema::validator_for;
use rdlt_postgres::source::{PostgresConfig, config_schema};
use serde_json::json;

fn example() -> serde_json::Value {
    json!({
        "conn": "postgres://app@db.internal/app",
        "schema": "public",
        "tls": {"mode": "verify_full", "root_cert": "/etc/rdlt/ca.pem"},
        "tables": [
            {
                "name": "orders",
                "cursor": {"column": "updated_at"},
                "type_hints": {"total": "decimal(12,4)", "created": "timestamp_tz"}
            }
        ],
        "queries": [
            {
                "name": "order_totals",
                "sql": "SELECT id, sum(x) AS total FROM t GROUP BY id",
                "primary_key": ["id"]
            }
        ]
    })
}

#[test]
fn documented_example_validates_and_parses() {
    let validator = validator_for(&config_schema()).expect("generated schema compiles");
    let example = example();
    assert!(
        validator.is_valid(&example),
        "example must validate: {:?}",
        validator.iter_errors(&example).next()
    );
    PostgresConfig::from_value(example).expect("schema-valid example parses");
}

#[test]
fn unknown_fields_fail_schema_and_parser_identically() {
    let validator = validator_for(&config_schema()).expect("schema compiles");
    let mut bad = example();
    bad["ssl"] = json!("on"); // plausible typo for `tls`
    assert!(
        !validator.is_valid(&bad),
        "unknown field must fail the schema"
    );
    assert!(
        PostgresConfig::from_value(bad).is_err(),
        "and the parser agrees (deny_unknown_fields)"
    );
}

#[test]
fn schema_valid_corpus_parses() {
    // schema-valid ⇒ parses: the declared surface never over-promises.
    let validator = validator_for(&config_schema()).expect("schema compiles");
    let corpus = [
        json!({"conn": "host=localhost user=a dbname=d"}),
        json!({"conn": "postgres://u@h/d", "include_views": true, "batch_max_rows": 7}),
        json!({"conn": "postgres://u@h/d", "tls": {"mode": "require"}}),
        json!({"conn": "postgres://u@h/d",
               "queries": [{"name": "q", "sql": "SELECT 1 AS x"}]}),
    ];
    for config in corpus {
        assert!(
            validator.is_valid(&config),
            "corpus entry invalid: {config}"
        );
        PostgresConfig::from_value(config.clone())
            .unwrap_or_else(|e| panic!("schema-valid config failed to parse: {config}: {e}"));
    }
    // Bad hint strings are stopped by the schema's pattern, same as FromStr.
    let bad_hint = json!({"conn": "postgres://u@h/d",
        "tables": [{"name": "t", "type_hints": {"c": "decimal(banana)"}}]});
    assert!(!validator.is_valid(&bad_hint));
    assert!(PostgresConfig::from_value(bad_hint).is_err());
}
