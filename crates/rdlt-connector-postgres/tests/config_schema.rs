//! T019: the declared config schema is GENERATED from the config structs —
//! examples validate, unknown fields fail schema AND parser identically.

use jsonschema::validator_for;
use rdlt_connector_postgres::source::{PostgresConfig, config_schema};
use serde_json::json;

fn example() -> serde_json::Value {
    json!({
        "conn": "postgres://app@db.internal/app",
        "schema": "public",
        "tls": {"mode": "verify_full", "root_cert": "/etc/rdlt/ca.pem",
                "client_cert": "/etc/rdlt/client.pem", "client_key": "/etc/rdlt/client.key"},
        "tables": [
            {
                "name": "orders",
                "cursor": {"column": "updated_at", "lag": "5m",
                           "end_bound": "inclusive", "nulls": "error"},
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
    // Feature 007: bad lag strings likewise (pattern mirrors FromStr)…
    let bad_lag = json!({"conn": "postgres://u@h/d",
        "tables": [{"name": "t", "cursor": {"column": "ts", "lag": "soon"}}]});
    assert!(!validator.is_valid(&bad_lag));
    assert!(PostgresConfig::from_value(bad_lag).is_err());
    // …and the new field corpus is schema-valid AND parses.
    let new_fields = json!({"conn": "postgres://u@h/d",
        "tls": {"mode": "require", "client_cert": "/c.pem", "client_key": "/k.pem"},
        "tables": [{"name": "t",
                    "cursor": {"column": "ts", "lag": "1d",
                               "end_value": "2026-01-01T00:00:00Z",
                               "end_bound": "inclusive", "nulls": "error"}}]});
    assert!(validator.is_valid(&new_fields), "new fields validate");
    PostgresConfig::from_value(new_fields).expect("new fields parse");
}

// ---- Feature 009: the cdc block (SC-007) ----

#[test]
fn cdc_block_round_trips_the_schema() {
    let validator = validator_for(&config_schema()).expect("schema compiles");
    // The documented example (quickstart shape) validates AND parses.
    let example = json!({"conn": "postgres://u@h/d",
        "cdc": {"slot": "my_slot", "publication": "my_pub",
                "create_if_missing": true, "mode": "tail",
                "idle_wait": "5s", "flag_column": "_rdlt_deleted", "ack": "auto"},
        "tables": [{"name": "orders"}]});
    assert!(
        validator.is_valid(&example),
        "cdc example must validate: {:?}",
        validator.iter_errors(&example).next()
    );
    PostgresConfig::from_value(example).expect("cdc example parses");
    // Unknown fields fail BOTH layers (C4).
    let unknown = json!({"conn": "postgres://u@h/d",
        "cdc": {"slot": "s", "publication": "p", "drop_slot": true}});
    assert!(!validator.is_valid(&unknown));
    assert!(PostgresConfig::from_value(unknown).is_err());
    // idle_wait keeps the duration vocabulary at the schema layer too (C4):
    // a magnitude fails the pattern, same as FromStr.
    let bad_wait = json!({"conn": "postgres://u@h/d",
        "cdc": {"slot": "s", "publication": "p", "idle_wait": "5"}});
    assert!(!validator.is_valid(&bad_wait));
    assert!(PostgresConfig::from_value(bad_wait).is_err());
    // cdc + cursor exclusivity (C1) is a VALIDATION rule: schema-valid by
    // shape, rejected by the parser naming the table.
    let exclusive = json!({"conn": "postgres://u@h/d",
        "cdc": {"slot": "s", "publication": "p"},
        "tables": [{"name": "t", "cursor": {"column": "id"}}]});
    assert!(validator.is_valid(&exclusive), "shape-valid");
    let err = PostgresConfig::from_value(exclusive)
        .expect_err("C1")
        .to_string();
    assert!(
        err.contains("`t`") && err.contains("mutually exclusive"),
        "{err}"
    );
}

// ---- Feature 008: destination options schema (SC-008) ----

mod dest_options {
    use jsonschema::validator_for;
    use rdlt_connector_postgres::dest::PgDestOptions;
    use serde_json::json;

    fn schema() -> serde_json::Value {
        serde_json::to_value(schemars::schema_for!(PgDestOptions)).expect("schema serializes")
    }

    #[test]
    fn documented_example_validates_and_parses() {
        let validator = validator_for(&schema()).expect("generated schema compiles");
        let example = json!({
            "merge_strategy": "upsert",
            "tables": {
                "customers": {"merge_strategy": "scd2",
                               "scd2": {"absent": "retire",
                                        "valid_from": "_rdlt_valid_from",
                                        "valid_to": "_rdlt_valid_to"}},
                "orders": {"hard_delete": "is_deleted",
                            "dedup_sort": {"column": "seq", "order": "desc"},
                            "merge_key": ["day", "tenant"]}
            }
        });
        assert!(
            validator.is_valid(&example),
            "example must validate: {:?}",
            validator.iter_errors(&example).next()
        );
        PgDestOptions::from_value(example).expect("schema-valid example parses");
    }

    #[test]
    fn refinement_options_round_trip_the_schema() {
        // Feature 010 (MR7): both options in the generated schema; bad
        // `order` tokens and unknown sub-fields fail schema AND parser.
        let validator = validator_for(&schema()).expect("schema compiles");
        for bad in [
            json!({"tables": {"t": {"dedup_sort": {"column": "seq", "order": "downwards"}}}}),
            json!({"tables": {"t": {"dedup_sort": {"column": "seq"}}}}),
            json!({"tables": {"t": {"dedup_sort": {"column": "seq", "order": "desc",
                                                     "nulls": "first"}}}}),
            json!({"tables": {"t": {"merge_key": "day"}}}),
        ] {
            assert!(!validator.is_valid(&bad), "schema must reject: {bad}");
            assert!(
                PgDestOptions::from_value(bad.clone()).is_err(),
                "parser agrees: {bad}"
            );
        }
    }

    #[test]
    fn unknown_fields_and_contradictions_fail_both_layers() {
        let validator = validator_for(&schema()).expect("schema compiles");
        // Unknown field: schema AND parser agree.
        let bad = json!({"merge_stratgy": "upsert"});
        assert!(!validator.is_valid(&bad));
        assert!(PgDestOptions::from_value(bad).is_err());
        // Unknown strategy value.
        let bad = json!({"merge_strategy": "replace"});
        assert!(!validator.is_valid(&bad));
        assert!(PgDestOptions::from_value(bad).is_err());
        // Schema-valid but semantically contradictory (S8): the VALIDATOR
        // accepts the shape; the parser's validate() names the field.
        let contradiction = json!({
            "tables": {"t": {"merge_strategy": "scd2", "hard_delete": "gone"}}
        });
        assert!(validator.is_valid(&contradiction), "shape is legal");
        let err = PgDestOptions::from_value(contradiction).unwrap_err();
        assert!(err.contains("tables.t.hard_delete"), "{err}");
    }
}
