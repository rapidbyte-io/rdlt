//! 039: the `connector:` requirement in the pipeline document — the
//! parse round-trip for BOTH spec enums, the ref-level unknown-key
//! refusal, and the provider's frozen NotFound spelling surfacing
//! through `build_pipeline` verbatim.
//!
//! These tests need NO connector feature: the `connector` variant is
//! deliberately always present (it names an out-of-process connector,
//! not a compiled-in one).

use rdlt::pipeline_spec::{DestSpec, SourceSpec, Spec, SpecError, build_pipeline};

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
    assert_eq!(source.config["streams"][0]["name"], "events");

    let DestSpec::Connector(dest) = &spec.destination else {
        panic!("destination parses as the connector variant");
    };
    assert_eq!(dest.id, "io.rapidbyte.duckdb");
    assert_eq!(dest.version, None, "version is optional");
    assert_eq!(dest.path, None, "path is optional");
    assert_eq!(dest.config["path"], "out.db");
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
    match build_pipeline(&spec).await {
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
