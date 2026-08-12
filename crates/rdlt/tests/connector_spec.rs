//! 039: the `connector:` requirement in the pipeline document — the
//! parse round-trip for BOTH spec enums, the ref-level unknown-key
//! refusal, and the provider's frozen NotFound spelling surfacing
//! through `build_pipeline` verbatim.
//!
//! This is the explicit form every rich spelling desugars to — the
//! one arm whose id, version pin and path override are spelled out.

use rdlt::pipeline_spec::{
    ConfigSource, ConnectorRef, DestSpec, SourceSpec, Spec, SpecError, build_pipeline,
};

/// The full vocabulary round-trips on both sides: id, the optional
/// version pin and path override, and the opaque config document.
#[test]
fn the_connector_requirement_parses_in_both_enums() {
    let text = r#"
pipeline: p
source:
  connector:
    id: io.rapidbyte.file
    version: "0.3.0"
    path: /explicit/bin
    config:
      streams:
        - name: events
destination:
  connector:
    id: io.rapidbyte.duckdb
    config:
      path: out.db
"#;
    let spec: Spec = serde_yaml::from_str(text).expect("the connector vocabulary parses");

    let SourceSpec::Connector(source) = &spec.source else {
        panic!("source parses as the connector variant");
    };
    assert_eq!(source.id, "io.rapidbyte.file");
    assert_eq!(source.version.as_deref(), Some("0.3.0"));
    assert_eq!(
        source.path.as_deref(),
        Some(std::path::Path::new("/explicit/bin"))
    );
    // The config is the connector's OWN document, carried opaquely —
    // whatever YAML was written arrives as the equivalent JSON.
    let ConfigSource::Inline(config) = &source.config else {
        panic!("an inline mapping parses as the inline config form");
    };
    assert_eq!(config["streams"][0]["name"], "events");

    let DestSpec::Connector(dest) = &spec.destination else {
        panic!("destination parses as the connector variant");
    };
    assert_eq!(dest.id, "io.rapidbyte.duckdb");
    assert_eq!(dest.version, None, "version is optional");
    assert_eq!(dest.path, None, "path is optional");
    let ConfigSource::Inline(config) = &dest.config else {
        panic!("an inline mapping parses as the inline config form");
    };
    assert_eq!(config["path"], "out.db");
}

/// An unknown key inside `connector:` is refused at PARSE — the ref
/// level denies unknown fields, so a typo cannot ride silently beside
/// the opaque config block.
#[test]
fn an_unknown_key_inside_the_connector_ref_is_refused() {
    let text = r#"
pipeline: p
source:
  connector:
    id: io.rapidbyte.file
    binary: /oops
    config: {}
destination:
  connector:
    id: io.rapidbyte.duckdb
    config: {}
"#;
    let parsed: Result<Spec, _> = serde_yaml::from_str(text);
    assert!(
        parsed.is_err(),
        "`binary:` is not part of the connector vocabulary and must refuse"
    );
}

/// `config` is part of the frozen vocabulary, not optional: a
/// requirement without one fails to parse rather than inventing an
/// empty document the connector never agreed to.
#[test]
fn a_connector_ref_without_a_config_is_refused() {
    let text = r#"
pipeline: p
source:
  connector:
    id: io.rapidbyte.file
destination:
  connector:
    id: io.rapidbyte.duckdb
    config: {}
"#;
    let parsed: Result<Spec, _> = serde_yaml::from_str(text);
    assert!(parsed.is_err(), "a missing config block must refuse");
}

/// The ref's Debug ELIDES the config: that document is the connector's
/// own vocabulary and routinely carries credentials, so a derived Debug
/// would print them into any `{:?}` of a `Spec` or a test failure
/// message. The marker below must never surface through Debug.
#[test]
fn a_debug_render_of_a_connector_ref_elides_the_config() {
    let reference = ConnectorRef {
        id: "io.rapidbyte.file".to_owned(),
        version: None,
        path: None,
        config: ConfigSource::Inline(serde_json::json!({ "password": "SECRET-MARKER-7f3a" })),
    };
    let rendered = format!("{reference:?}");
    assert!(
        !rendered.contains("SECRET-MARKER-7f3a"),
        "the config document must not reach a Debug render: {rendered}"
    );
    assert!(
        rendered.contains("io.rapidbyte.file") && rendered.contains("<elided>"),
        "the other fields still render, and the config says it was elided: {rendered}"
    );
}

/// A requirement whose binary exists nowhere refuses through
/// `build_pipeline` with the provider's frozen NotFound spelling,
/// VERBATIM — a typed Resolve error (the CLI's exit-2 class), never a
/// facade paraphrase.
#[tokio::test]
async fn a_missing_connector_binary_refuses_with_the_frozen_notfound_spelling() {
    let text = r#"
pipeline: p
source:
  connector:
    id: io.rdlt.test.absent
    config: {}
destination:
  connector:
    id: io.rdlt.test.absent
    config: {}
"#;
    let spec: Spec = serde_yaml::from_str(text).expect("parses");
    match build_pipeline(&spec, std::path::Path::new("")).await {
        Err(SpecError::Resolve(message)) => assert_eq!(
            message,
            "connector `io.rdlt.test.absent`: no binary `rdlt-connector-absent` on PATH \
             and no explicit path was given — install it (e.g. cargo install \
             rdlt-connector-absent) or set path: in the connector requirement"
        ),
        Err(other) => panic!("expected a Resolve error, got: {other}"),
        Ok(_) => panic!("a missing connector binary must not build"),
    }
}

/// `build_pipeline` resolves a relative path-form config against the
/// `base` the caller passes — the include rule, NOT the working
/// directory. The config file exists only beside the (imaginary) spec,
/// so getting past resolution to the frozen NotFound refusal proves the
/// base was honored; a cwd-based resolution would refuse at `reading`.
#[tokio::test]
async fn a_relative_config_path_resolves_against_the_base_not_the_cwd() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("cfg.yaml"), "k: v\n").expect("config writes");
    let text = r#"
pipeline: p
source:
  connector:
    id: io.rdlt.test.absent
    config: ./cfg.yaml
destination:
  connector:
    id: io.rdlt.test.absent
    config: {}
"#;
    let spec: Spec = serde_yaml::from_str(text).expect("parses");
    match build_pipeline(&spec, dir.path()).await {
        Err(SpecError::Resolve(message)) => assert!(
            message.contains("no binary `rdlt-connector-absent`"),
            "resolution must SUCCEED (the failure is the absent binary, \
             after it): {message}"
        ),
        Err(other) => panic!("expected a Resolve error, got: {other}"),
        Ok(_) => panic!("a missing connector binary must not build"),
    }
}
