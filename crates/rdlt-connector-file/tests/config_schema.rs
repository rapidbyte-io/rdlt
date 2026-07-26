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

/// 015: the DEST config document round-trips its generated schema.
#[test]
fn dest_schema_valid_corpus_parses() {
    use rdlt_connector_file::dest::{FileDestConfig, dest_config_schema};
    let validator = jsonschema::validator_for(&dest_config_schema()).expect("schema compiles");
    let corpus = [
        serde_json::json!({"path": "out"}),
        serde_json::json!({"path": "warehouse/events", "format": "jsonl",
                            "partition_by": "day",
                            "location": {"s3": {"endpoint": "http://127.0.0.1:9000",
                                                  "bucket": "lake",
                                                  "access_key": "k", "secret_key": "s"}}}),
    ];
    for config in corpus {
        assert!(
            validator.is_valid(&config),
            "corpus entry invalid: {config}"
        );
        let parsed: FileDestConfig =
            serde_json::from_value(config.clone()).expect("schema-valid config parses");
        parsed.validate("file destination").expect("validates");
    }
}

/// T133 — the hand-mirrored `DestSpec::File` parity guard.
///
/// `rdlt::pipeline_spec::DestSpec::File` restates this config field by field so
/// a pipeline document can express it. That mirror is maintained BY HAND, and
/// the dangerous direction is silent: add a field here and forget the mirror,
/// and the field compiles, ships, and is simply unreachable from any pipeline
/// YAML — unconfigurable with no error anywhere.
///
/// The assertion cannot live next to `DestSpec` (this crate is a dependency of
/// the facade, so it cannot see it) and it cannot import it here for the same
/// reason. So it pins the field set on THIS side and names the required action
/// in the failure message. It is derived from the schema rather than written as
/// a struct literal deliberately: US10 may embed `FileDestConfig` into
/// `DestSpec::File` instead of mirroring it, and a field-set assertion survives
/// that refactor where a per-field destructure would not.
#[test]
fn file_dest_config_field_set_is_mirrored_in_dest_spec() {
    let schema = schemars::schema_for!(rdlt_connector_file::dest::FileDestConfig);
    let value = serde_json::to_value(&schema).expect("schema serializes");
    let mut fields: Vec<String> = value["properties"]
        .as_object()
        .expect("FileDestConfig is an object schema")
        .keys()
        .cloned()
        .collect();
    fields.sort();

    let expected = ["format", "location", "parquet", "partition_by", "path"];
    assert_eq!(
        fields, expected,
        "FileDestConfig's fields changed. `rdlt::pipeline_spec::DestSpec::File` \
         is a hand-maintained mirror of this struct: add the same field there \
         (and to the destructure that rebuilds this config from it), then update \
         this expected list. Skipping the mirror leaves the new field \
         unreachable from pipeline YAML with no error at all."
    );
}
