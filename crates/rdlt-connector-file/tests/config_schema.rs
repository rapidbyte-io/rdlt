//! T019: generated config schema — examples validate, unknown fields fail
//! schema AND parser identically.

use jsonschema::validator_for;
use rdlt_connector_file::config::FileConfig;
use rdlt_connector_file::config_schema;
use serde_json::json;

fn example() -> serde_json::Value {
    json!({
        "streams": [
            {
                "name": "events",
                "format": "jsonl",
                "path": "data/events-*.jsonl",
                "primary_key": ["id"],
                "type_hints": {"ts": "timestamp_tz"}
            },
            {"name": "metrics", "format": "parquet", "path": "data/metrics.parquet"}
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
    FileConfig::from_value(example).expect("schema-valid example parses");
}

#[test]
fn unknown_fields_fail_schema_and_parser_identically() {
    let validator = validator_for(&config_schema()).expect("schema compiles");
    let mut bad = example();
    bad["streams"][0]["glob"] = json!("*.jsonl"); // typo for `path`
    assert!(!validator.is_valid(&bad));
    assert!(FileConfig::from_value(bad).is_err());
}

#[test]
fn schema_valid_corpus_parses() {
    let validator = validator_for(&config_schema()).expect("schema compiles");
    let corpus = [
        json!({"streams": [{"name": "s", "format": "jsonl", "path": "f.jsonl"}]}),
        json!({"streams": [{"name": "s", "format": "jsonl", "path": "f.jsonl",
                            "validate": false}]}),
    ];
    // The `validate()` minimum is IN the schema: zero streams fails both.
    let empty = json!({"streams": []});
    assert!(!validator.is_valid(&empty), "minItems mirrors validate()");
    assert!(FileConfig::from_value(empty).is_err());
    for config in corpus {
        assert!(
            validator.is_valid(&config),
            "corpus entry invalid: {config}"
        );
        FileConfig::from_value(config.clone())
            .unwrap_or_else(|e| panic!("schema-valid config failed to parse: {config}: {e}"));
    }
}
