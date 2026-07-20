//! T019: generated config schema — examples validate, unknown fields fail
//! schema AND parser identically.

use jsonschema::validator_for;
use rdlt_source_rest::{RestConfig, config_schema};
use serde_json::json;

fn example() -> serde_json::Value {
    json!({
        "base_url": "https://api.example.com",
        "auth": {"bearer": {"token": "secret"}},
        "streams": [
            {
                "name": "issues",
                "path": "/issues",
                "pagination": {"type": "page", "start": 1},
                "cursor_field": "updated_at",
                "cursor_param": "since",
                "primary_key": ["id"],
                "type_hints": {"updated_at": "timestamp_tz"}
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
    RestConfig::from_value(example).expect("schema-valid example parses");
}

#[test]
fn unknown_fields_fail_schema_and_parser_identically() {
    let validator = validator_for(&config_schema()).expect("schema compiles");
    let mut bad = example();
    bad["streams"][0]["cursor"] = json!("updated_at"); // typo for cursor_field
    assert!(!validator.is_valid(&bad));
    assert!(RestConfig::from_value(bad).is_err());
}

#[test]
fn schema_valid_corpus_parses() {
    let validator = validator_for(&config_schema()).expect("schema compiles");
    let corpus = [
        json!({"base_url": "https://x", "streams": [{"name": "s", "path": "/s"}]}),
        json!({"base_url": "https://x", "auth": "none",
               "streams": [{"name": "s", "path": "/s",
                            "pagination": {"type": "offset", "page_size": 50}}]}),
    ];
    for config in corpus {
        assert!(
            validator.is_valid(&config),
            "corpus entry invalid: {config}"
        );
        RestConfig::from_value(config.clone())
            .unwrap_or_else(|e| panic!("schema-valid config failed to parse: {config}: {e}"));
    }
}
