//! The examples are LOAD-BEARING: every `examples/*/pipeline.yaml`
//! must parse through the real Spec gate, every connector reference in
//! it — rich spelling or `connector:` — must desugar to a known
//! reverse-DNS id, and every resolved config must pass that connector's
//! own Document gate. No spawn in the default gate: whether the
//! documents RUN against real binaries is the spawn suites' business;
//! this holds the document language itself against the examples.

use std::collections::HashSet;
use std::path::{Path, PathBuf};

use rdlt::pipeline_spec::{ConfigSource, ConnectorRef};
use rdlt_connector_sdk::config::Document;

/// Every rich spelling the pipeline document exposes. The expected ids
/// are derived through the production desugar table rather than copied
/// into this test.
const CONNECTOR_SPELLINGS: &[&str] = &[
    "rest",
    "oracle",
    "file",
    "postgres",
    "duckdb",
    "iceberg",
    "snowflake",
];

fn examples_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples")
}

/// Push one resolved config through the named connector's own Document
/// gate, exactly as the connector does when a spawned run handshakes.
fn validate_config(example: &str, role: &str, reference: &ConnectorRef) {
    let ConfigSource::Inline(config) = &reference.config else {
        panic!("{example}: the {role} config must resolve to an inline document");
    };
    let config = config.clone();
    let outcome = match (reference.id.as_str(), role) {
        ("io.rapidbyte.rest", "source") => rdlt_connector_rest::source::Config::from_value(config)
            .map(|_| ())
            .map_err(|error| error.to_string()),
        ("io.rapidbyte.oracle", "source") => {
            rdlt_connector_oracle::source::Config::from_value(config)
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
        ("io.rapidbyte.file", "source") => rdlt_connector_file::source::Config::from_value(config)
            .map(|_| ())
            .map_err(|error| error.to_string()),
        ("io.rapidbyte.postgres", "source") => {
            rdlt_connector_postgres::source::Config::from_value(config)
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
        ("io.rapidbyte.duckdb", "destination") => {
            rdlt_connector_duckdb::destination::Config::from_value(config)
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
        ("io.rapidbyte.postgres", "destination") => {
            rdlt_connector_postgres::destination::Config::from_value(config)
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
        ("io.rapidbyte.file", "destination") => {
            rdlt_connector_file::destination::Config::from_value(config)
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
        ("io.rapidbyte.iceberg", "destination") => {
            rdlt_connector_iceberg::destination::Config::from_value(config)
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
        ("io.rapidbyte.snowflake", "destination") => {
            rdlt_connector_snowflake::destination::Config::from_value(config)
                .map(|_| ())
                .map_err(|error| error.to_string())
        }
        other => panic!("{example}: no connector config gate is wired for {other:?}"),
    };
    if let Err(error) = outcome {
        panic!(
            "{example}: the {role} config fails {}'s own document gate: {error}",
            reference.id
        );
    }
}

/// Every example directory carries a pipeline.yaml that parses, and
/// both sides desugar to a shipped connector id and pass that
/// connector's config gate. The count is pinned so a deleted or
/// unreadable example fails rather than shrinking the property.
#[test]
fn every_example_pipeline_parses_desugars_and_passes_connector_gates() {
    let known_ids: HashSet<_> = CONNECTOR_SPELLINGS
        .iter()
        .map(|spelling| {
            rdlt::pipeline_spec::connector_id(spelling)
                .unwrap_or_else(|| panic!("`{spelling}` must have a desugar-table row"))
        })
        .collect();
    let mut seen = 0;
    for entry in std::fs::read_dir(examples_dir()).expect("examples/ exists") {
        let entry = entry.expect("entry");
        let dir = entry.path();
        let path = dir.join("pipeline.yaml");
        if !path.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("{name}/pipeline.yaml must read: {e}"));
        let spec: rdlt::pipeline_spec::Spec = serde_yaml::from_str(&text)
            .unwrap_or_else(|e| panic!("{name}/pipeline.yaml must parse: {e}"));
        // The example's own directory is the base, so a path-form
        // config resolves relative to the files beside it.
        let source = spec
            .source
            .desugar(&dir)
            .unwrap_or_else(|e| panic!("{name}: the source must desugar: {e}"));
        let destination = spec
            .destination
            .desugar(&dir)
            .unwrap_or_else(|e| panic!("{name}: the destination must desugar: {e}"));
        for (role, reference) in [("source", &source), ("destination", &destination)] {
            assert!(
                known_ids.contains(reference.id.as_str()),
                "{name}: `{}` is not a shipped connector's id",
                reference.id
            );
            validate_config(&name, role, reference);
        }
        seen += 1;
    }
    assert_eq!(seen, 7, "every example directory carries a pipeline.yaml");
}
