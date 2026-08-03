//! The examples are LOAD-BEARING: every `examples/*/pipeline.yaml`
//! must parse through the real Spec gate and build a pipeline, and
//! each connector's designated reference example must mention EVERY
//! field of that connector's config schema — active or commented — so
//! "the full configuration is visible in examples/" is a property the
//! gate holds, not a promise that rots.
//!
//! Feature-gated to the full connector set: the workspace gate builds
//! with the union of features (the CLI enables them all), which is
//! where these run.
#![cfg(all(
    feature = "rest",
    feature = "postgres",
    feature = "oracle",
    feature = "file",
    feature = "duckdb",
    feature = "snowflake",
    feature = "iceberg"
))]

use std::path::{Path, PathBuf};

/// Every example, with the schemas its pipeline is the REFERENCE for.
/// A connector may appear in several examples; the full-vocabulary
/// claim is checked only against its designated one.
fn reference_map() -> Vec<(&'static str, Vec<serde_json::Value>)> {
    use rdlt::connector;
    vec![
        (
            "pokemon-to-jsonl",
            vec![connector::rest::source::config_schema()],
        ),
        (
            "oracle-to-jsonl",
            vec![connector::oracle::source::config_schema()],
        ),
        (
            "postgres-to-parquet",
            vec![
                connector::postgres::source::config_schema(),
                connector::file::destination::config_schema(),
            ],
        ),
        (
            "jsonl-to-postgres",
            vec![
                connector::file::source::config_schema(),
                connector::postgres::destination::config_schema(),
            ],
        ),
        (
            "csv-to-duckdb",
            vec![connector::duckdb::destination::config_schema()],
        ),
        (
            "postgres-to-iceberg",
            vec![connector::iceberg::destination::config_schema()],
        ),
        (
            "jsonl-to-snowflake",
            vec![connector::snowflake::destination::config_schema()],
        ),
    ]
}

fn examples_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples")
}

/// Every pipeline parses AND builds. Building constructs the real
/// connector shells, which run each document's validation gate — so a
/// typo in an ACTIVE line of any example fails here, not in a user's
/// first run.
#[test]
fn every_example_pipeline_parses_and_builds() {
    // Building constructs the real shells, and the duckdb shell OPENS
    // its database file — an IO side effect that must land in a
    // throwaway working directory, not in `examples/`. Safe to chdir
    // because nextest runs each test in its own process.
    let scratch = tempfile::tempdir().expect("tempdir");
    let mut seen = 0;
    for (name, _) in reference_map() {
        let path = examples_dir().join(name).join("pipeline.yaml");
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{name}/pipeline.yaml must exist: {e}"));
        std::fs::create_dir_all(scratch.path().join("examples").join(name)).expect("mkdir");
        std::env::set_current_dir(scratch.path()).expect("chdir");
        let spec: rdlt::pipeline_spec::Spec = serde_yaml::from_str(&text)
            .unwrap_or_else(|e| panic!("{name}/pipeline.yaml must parse: {e}"));
        rdlt::pipeline_spec::build_pipeline(&spec)
            .unwrap_or_else(|e| panic!("{name}/pipeline.yaml must build: {e}"));
        seen += 1;
    }
    assert_eq!(seen, 7, "the reference map names every example");
}

/// The reference map IS the example set: an example directory this
/// test does not know is either missing its designated-schema entry
/// or is dead weight — both are findings.
#[test]
fn the_reference_map_matches_the_example_directories() {
    let mut on_disk: Vec<String> = std::fs::read_dir(examples_dir())
        .expect("examples/ exists")
        .filter_map(|entry| {
            let entry = entry.expect("entry");
            entry
                .path()
                .join("pipeline.yaml")
                .is_file()
                .then(|| entry.file_name().to_string_lossy().into_owned())
        })
        .collect();
    on_disk.sort();
    let mut mapped: Vec<String> = reference_map()
        .into_iter()
        .map(|(name, _)| name.to_owned())
        .collect();
    mapped.sort();
    assert_eq!(on_disk, mapped);
}

/// Collect every property name reachable in a config schema.
fn property_names(schema: &serde_json::Value, out: &mut std::collections::BTreeSet<String>) {
    if let Some(props) = schema.get("properties").and_then(|p| p.as_object()) {
        for (name, sub) in props {
            out.insert(name.clone());
            property_names(sub, out);
        }
    }
    for key in ["items", "additionalProperties"] {
        if let Some(sub) = schema.get(key) {
            property_names(sub, out);
        }
    }
    for key in ["anyOf", "oneOf", "allOf"] {
        if let Some(list) = schema.get(key).and_then(|l| l.as_array()) {
            for sub in list {
                property_names(sub, out);
            }
        }
    }
    if let Some(defs) = schema.get("$defs").and_then(|d| d.as_object()) {
        for sub in defs.values() {
            property_names(sub, out);
        }
    }
}

/// THE FULL-CONFIGURATION GUARANTEE: each designated example mentions
/// every field of its connector's schema, in an active line or a
/// commented one. A field added to a config struct without a home in
/// its reference example fails HERE — which is the entire point of
/// this file.
#[test]
fn each_reference_example_mentions_every_schema_field() {
    for (name, schemas) in reference_map() {
        let text = std::fs::read_to_string(examples_dir().join(name).join("pipeline.yaml"))
            .expect("read example");
        let mut fields = std::collections::BTreeSet::new();
        for schema in &schemas {
            property_names(schema, &mut fields);
        }
        let missing: Vec<&String> = fields
            .iter()
            .filter(|field| !text.contains(field.as_str()))
            .collect();
        assert!(
            missing.is_empty(),
            "{name}/pipeline.yaml is the reference for its connector but never mentions: \
             {missing:?} — show each field, active or commented"
        );
    }
}
