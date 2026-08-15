//! Every rich spelling desugars to exactly its ConnectorRef: the
//! reverse-DNS id from the one table, the config document verbatim,
//! no version pin, no path override. Plus the `config` path form's
//! resolution rules, and the `parquet:` spelling's death (D-043-1:
//! neither the alias nor the shorthand survives the swap).

use std::path::Path;

use rdlt::pipeline_spec::{ConfigSource, Spec, SpecError};

fn spec(yaml: &str) -> Spec {
    serde_yaml_ng::from_str(yaml).expect("spec parses")
}

/// The four source spellings and the id each must desugar to.
const SOURCE_CASES: &[(&str, &str)] = &[
    ("rest", "io.rapidbyte.rest"),
    ("oracle", "io.rapidbyte.oracle"),
    ("file", "io.rapidbyte.file"),
    ("postgres", "io.rapidbyte.postgres"),
];

/// The five destination spellings and the id each must desugar to.
const DEST_CASES: &[(&str, &str)] = &[
    ("duckdb", "io.rapidbyte.duckdb"),
    ("postgres", "io.rapidbyte.postgres"),
    ("file", "io.rapidbyte.file"),
    ("iceberg", "io.rapidbyte.iceberg"),
    ("snowflake", "io.rapidbyte.snowflake"),
];

/// Assert one desugared reference: the table id, nothing invented, and
/// the distinctive config key carried through verbatim.
fn assert_desugared(reference: &rdlt::pipeline_spec::ConnectorRef, id: &str, spelling: &str) {
    assert_eq!(reference.id, id, "{spelling}: the desugar table's id");
    assert_eq!(
        reference.version, None,
        "{spelling}: no version pin is invented"
    );
    assert_eq!(
        reference.path, None,
        "{spelling}: no path override is invented"
    );
    let ConfigSource::Inline(config) = &reference.config else {
        panic!("{spelling}: a desugared reference carries its config inline");
    };
    assert_eq!(
        config[format!("marker_{spelling}")],
        format!("value-{spelling}"),
        "{spelling}: the config document rides through verbatim"
    );
}

#[test]
fn every_rich_source_spelling_desugars_to_its_connector_ref() {
    for (spelling, id) in SOURCE_CASES {
        let text = format!(
            "pipeline: p\n\
             source:\n\
            \x20 {spelling}:\n\
            \x20   marker_{spelling}: value-{spelling}\n\
             destination:\n\
            \x20 connector: {{id: io.rapidbyte.duckdb, config: {{}}}}\n"
        );
        let parsed = spec(&text);
        let reference = parsed
            .source
            .desugar(Path::new(""))
            .unwrap_or_else(|e| panic!("{spelling}: the source arm desugars: {e}"));
        assert_desugared(&reference, id, spelling);
    }
}

#[test]
fn every_rich_destination_spelling_desugars_to_its_connector_ref() {
    for (spelling, id) in DEST_CASES {
        let text = format!(
            "pipeline: p\n\
             source:\n\
            \x20 connector: {{id: io.rapidbyte.file, config: {{}}}}\n\
             destination:\n\
            \x20 {spelling}:\n\
            \x20   marker_{spelling}: value-{spelling}\n"
        );
        let parsed = spec(&text);
        let reference = parsed
            .destination
            .desugar(Path::new(""))
            .unwrap_or_else(|e| panic!("{spelling}: the destination arm desugars: {e}"));
        assert_desugared(&reference, id, spelling);
    }
}

/// The path form: a rich arm whose value is a string reads that file
/// at desugar time, relative to the caller's base (the spec's own
/// directory for a file-loaded spec).
#[test]
fn a_string_config_resolves_as_a_path_relative_to_the_base() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("x.yaml"),
        "marker_postgres: value-postgres\n",
    )
    .expect("config file writes");
    let parsed = spec(
        "pipeline: p\n\
         source:\n\
        \x20 postgres: ./x.yaml\n\
         destination:\n\
        \x20 connector: {id: io.rapidbyte.duckdb, config: {}}\n",
    );
    let reference = parsed
        .source
        .desugar(dir.path())
        .expect("the path form resolves against the base");
    assert_desugared(&reference, "io.rapidbyte.postgres", "postgres");
}

/// A missing config file refuses at desugar with a typed Resolve error
/// naming the full path it looked for.
#[test]
fn a_missing_config_path_is_a_resolve_error_naming_the_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let parsed = spec(
        "pipeline: p\n\
         source:\n\
        \x20 postgres: absent.yaml\n\
         destination:\n\
        \x20 connector: {id: io.rapidbyte.duckdb, config: {}}\n",
    );
    match parsed.source.desugar(dir.path()) {
        Err(SpecError::Resolve(message)) => {
            let looked_for = dir.path().join("absent.yaml");
            assert!(
                message.starts_with(&format!("reading {}:", looked_for.display())),
                "the refusal names the full path it looked for: {message}"
            );
        }
        Err(other) => panic!("expected a Resolve error, got: {other}"),
        Ok(_) => panic!("a missing config file must not desugar"),
    }
}

/// The `connector:` arm's `config:` accepts the SAME path form as the
/// rich spellings — one resolution rule for every arm.
#[test]
fn the_connector_arm_accepts_the_config_path_form_identically() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("x.yaml"), "marker_file: value-file\n")
        .expect("config file writes");
    let parsed = spec(
        "pipeline: p\n\
         source:\n\
        \x20 connector:\n\
        \x20   id: io.rapidbyte.file\n\
        \x20   config: ./x.yaml\n\
         destination:\n\
        \x20 connector: {id: io.rapidbyte.duckdb, config: {}}\n",
    );
    let reference = parsed
        .source
        .desugar(dir.path())
        .expect("the connector arm's path form resolves against the base");
    assert_desugared(&reference, "io.rapidbyte.file", "file");
}

/// D-043-1: `parquet:` is gone — not an alias, not a shorthand. The
/// spelling is an unknown field at parse, never quietly rerouted.
#[test]
fn the_parquet_spelling_is_refused_at_parse() {
    let parsed: Result<Spec, _> = serde_yaml_ng::from_str(
        "pipeline: p\n\
         source:\n\
        \x20 connector: {id: io.rapidbyte.file, config: {}}\n\
         destination:\n\
        \x20 parquet: {path: out}\n",
    );
    let error = parsed.expect_err("the parquet spelling must not parse");
    assert!(
        error.to_string().contains("parquet"),
        "the refusal names the unknown spelling: {error}"
    );
}
