//! The pipeline document end to end: every rich spelling parses to
//! exactly its connector requirement (the reverse-DNS id from the one
//! table, the config verbatim, no version pin, no path override) in the
//! role it fills; the config forms and their resolution rules; the
//! write-mode forms; the arm and document-level refusals, each naming
//! the spelling and the accepted set; the `connector:` vocabulary
//! round-trip; construction failures as typed Resolve errors — including
//! the provider's frozen NotFound spelling surfacing through `build`
//! verbatim; and the `parquet:` spelling's death (neither alias nor
//! shorthand survives).

use std::path::Path;

use rdlt::document::connector::Connector;
use rdlt::document::{Config, Document, Error, WriteMode, build};

use super::support::document;

// ---- the arms ---------------------------------------------------------

/// The four source spellings and the id each must parse to.
const SOURCE_CASES: &[(&str, &str)] = &[
    ("rest", "io.rapidbyte.rest"),
    ("oracle", "io.rapidbyte.oracle"),
    ("file", "io.rapidbyte.file"),
    ("postgres", "io.rapidbyte.postgres"),
];

/// The five destination spellings and the id each must parse to.
const DEST_CASES: &[(&str, &str)] = &[
    ("duckdb", "io.rapidbyte.duckdb"),
    ("postgres", "io.rapidbyte.postgres"),
    ("file", "io.rapidbyte.file"),
    ("iceberg", "io.rapidbyte.iceberg"),
    ("snowflake", "io.rapidbyte.snowflake"),
];

/// Assert one desugared requirement: the table id, nothing invented, and
/// the distinctive config key carried through verbatim.
fn assert_desugared(reference: &Connector, id: &str, spelling: &str) {
    assert_eq!(reference.id, id, "{spelling}: the desugar table's id");
    assert_eq!(
        reference.version, None,
        "{spelling}: no version pin is invented"
    );
    assert_eq!(
        reference.path, None,
        "{spelling}: no path override is invented"
    );
    let Config::Inline(config) = &reference.config else {
        panic!("{spelling}: an inline mapping parses as the inline config form");
    };
    assert_eq!(
        config[format!("marker_{spelling}")],
        format!("value-{spelling}"),
        "{spelling}: the config document rides through verbatim"
    );
}

/// Each rich source spelling parses to exactly its connector requirement,
/// inline and by path — one pin per spelling, minimal on purpose (the
/// config block is opaque here; its vocabulary belongs to the connector).
#[test]
fn every_rich_source_spelling_parses_to_its_connector() {
    for (spelling, id) in SOURCE_CASES {
        let inline = document(&format!(
            "pipeline: p\n\
             source:\n\
            \x20 {spelling}:\n\
            \x20   marker_{spelling}: value-{spelling}\n\
             destination:\n\
            \x20 connector: {{id: io.rapidbyte.duckdb, config: {{}}}}\n"
        ));
        assert_desugared(&inline.source, id, spelling);

        // The path form parses as a PATH, not as a one-key document.
        let by_path = document(&format!(
            "pipeline: p\nsource:\n  {spelling}: cfg.yaml\ndestination:\n  duckdb: {{path: out.db}}\n"
        ));
        assert_eq!(by_path.source.id, *id, "{spelling}: path form");
        assert!(
            matches!(by_path.source.config, Config::Path(ref p) if p == "cfg.yaml"),
            "{spelling}: a string value is a config path"
        );
    }
}

/// Each rich destination spelling parses to exactly its connector
/// requirement — the destination twin of the source pin above.
#[test]
fn every_rich_destination_spelling_parses_to_its_connector() {
    for (spelling, id) in DEST_CASES {
        let parsed = document(&format!(
            "pipeline: p\n\
             source:\n\
            \x20 connector: {{id: io.rapidbyte.file, config: {{}}}}\n\
             destination:\n\
            \x20 {spelling}:\n\
            \x20   marker_{spelling}: value-{spelling}\n"
        ));
        assert_desugared(&parsed.destination, id, spelling);
    }
}

/// Every pipeline-document form parses — write_mode's three shapes,
/// workdir default vs custom, and the policies' presence.
#[test]
fn pipeline_document_forms_parse() {
    for (mode, want_merge) in [
        ("write_mode: append\n", false),
        ("write_mode: replace\n", false),
        ("write_mode: {merge: {key: [id]}}\n", true),
        ("", false), // absent = append default
    ] {
        let parsed = document(&format!(
            "pipeline: p\n{mode}source:\n  postgres: s.yaml\n\
             destination:\n  duckdb: {{path: out.db}}\n"
        ));
        assert_eq!(
            matches!(parsed.write_mode, Some(WriteMode::Merge { .. })),
            want_merge,
            "{mode}"
        );
    }
    let custom = document(
        "pipeline: p\nworkdir: /tmp/x\nsource:\n  postgres: s.yaml\n\
         destination:\n  file: {path: out}\n",
    );
    assert_eq!(custom.workdir.as_deref(), Some(Path::new("/tmp/x")));
    let bare = document(
        "pipeline: p\nsource:\n  postgres: s.yaml\n\
         destination:\n  duckdb: {path: out.db}\n",
    );
    assert!(
        bare.workdir.is_none(),
        "workdir defaults downstream to .rdlt/<pipeline> beside the document"
    );
    let with_policies = document(
        "pipeline: p\nbatch_policy: {every_rows: 50000}\n\
         commit_policy: {every_bytes: 104857600}\n\
         source:\n  postgres: s.yaml\ndestination:\n  duckdb: {path: out.db}\n",
    );
    assert!(with_policies.batch_policy.is_some());
    assert!(with_policies.commit_policy.is_some());
}

/// A typoed top-level key and an unknown source kind each refuse at
/// parse — deny_unknown_fields on the document, and the arm's own
/// refusal naming the spelling and the accepted set.
#[test]
fn unknown_spellings_are_refused_at_parse() {
    let bad_kind: Result<Document, _> = serde_yaml_ng::from_str(
        "pipeline: p\nsource:\n  mongodb: {conn: x}\ndestination:\n  duckdb: {path: out.db}\n",
    );
    let error = bad_kind
        .expect_err("an unknown source kind must not parse")
        .to_string();
    assert!(
        error.contains(
            "unknown spelling `mongodb`: a source arm names `connector:` or one of \
             `rest`, `oracle`, `file`, `postgres`"
        ),
        "the refusal names the spelling and the accepted set: {error}"
    );

    let bad_key: Result<Document, _> = serde_yaml_ng::from_str(
        "pipeline: p\nworkdirr: /tmp/x\nsource:\n  postgres: s.yaml\n\
         destination:\n  duckdb: {path: out.db}\n",
    );
    assert!(bad_key.is_err(), "a typoed top-level key must not parse");

    // A typo INSIDE the merge block must refuse too: a silently
    // ignored `kye` would mean a merge with no key slipping toward the
    // plan-time refusal instead of failing at the typo.
    let bad_merge_key: Result<Document, _> = serde_yaml_ng::from_str(
        "pipeline: p\nwrite_mode: {merge: {kye: [id]}}\nsource:\n  postgres: s.yaml\n\
         destination:\n  duckdb: {path: out.db}\n",
    );
    assert!(
        bad_merge_key.is_err(),
        "a typoed merge-block key must not parse"
    );
}

/// A spelling from the table used in the role it does not fill refuses
/// at parse, naming the spelling, the role, and that role's accepted
/// set — `duckdb:` is not a source, `rest:` is not a destination.
#[test]
fn a_spelling_in_the_wrong_role_is_refused_at_parse() {
    let dest_as_source: Result<Document, _> = serde_yaml_ng::from_str(
        "pipeline: p\nsource:\n  duckdb: {path: in.db}\ndestination:\n  duckdb: {path: out.db}\n",
    );
    let error = dest_as_source
        .expect_err("a destination-only spelling must not fill the source arm")
        .to_string();
    assert!(
        error.contains(
            "`duckdb` is not a source: a source arm names `connector:` or one of \
             `rest`, `oracle`, `file`, `postgres`"
        ),
        "{error}"
    );

    let source_as_dest: Result<Document, _> = serde_yaml_ng::from_str(
        "pipeline: p\nsource:\n  postgres: s.yaml\ndestination:\n  rest: {base_url: x}\n",
    );
    let error = source_as_dest
        .expect_err("a source-only spelling must not fill the destination arm")
        .to_string();
    assert!(
        error.contains(
            "`rest` is not a destination: a destination arm names `connector:` or one of \
             `file`, `postgres`, `duckdb`, `iceberg`, `snowflake`"
        ),
        "{error}"
    );
}

/// An arm is a single-key map: two spellings in one arm, or none,
/// refuse at parse naming what was found and the accepted set.
#[test]
fn an_arm_naming_two_connectors_or_none_is_refused_at_parse() {
    let two: Result<Document, _> = serde_yaml_ng::from_str(
        "pipeline: p\nsource:\n  postgres: s.yaml\n  file: {path: in}\n\
         destination:\n  duckdb: {path: out.db}\n",
    );
    let error = two
        .expect_err("two connectors in one arm must not parse")
        .to_string();
    assert!(
        error.contains(
            "two connectors, `postgres` and `file`: a source arm is a single-key map, \
             `connector:` or one of `rest`, `oracle`, `file`, `postgres`"
        ),
        "{error}"
    );

    let none: Result<Document, _> = serde_yaml_ng::from_str(
        "pipeline: p\nsource: {}\ndestination:\n  duckdb: {path: out.db}\n",
    );
    let error = none.expect_err("an empty arm must not parse").to_string();
    assert!(
        error.contains(
            "no connector: a source arm is a single-key map, `connector:` or one of \
             `rest`, `oracle`, `file`, `postgres`"
        ),
        "{error}"
    );
}

/// `parquet:` is gone — not an alias, not a shorthand. The spelling is
/// an unknown field at parse, never quietly rerouted.
#[test]
fn the_parquet_spelling_is_refused_at_parse() {
    let parsed: Result<Document, _> = serde_yaml_ng::from_str(
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

// ---- config resolution ------------------------------------------------

/// The path form: a rich arm whose value is a string names a file that
/// resolves at build time, relative to the caller's base (the document's
/// own directory for a file-loaded document).
#[test]
fn a_string_config_resolves_as_a_path_relative_to_the_base() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dir.path().join("x.yaml"),
        "marker_postgres: value-postgres\n",
    )
    .expect("config file writes");
    let parsed = document(
        "pipeline: p\n\
         source:\n\
        \x20 postgres: ./x.yaml\n\
         destination:\n\
        \x20 connector: {id: io.rapidbyte.duckdb, config: {}}\n",
    );
    assert_eq!(parsed.source.id, "io.rapidbyte.postgres");
    let config = parsed
        .source
        .config
        .resolve(dir.path())
        .expect("the path form resolves against the base");
    assert_eq!(
        config["marker_postgres"], "value-postgres",
        "the config document rides through verbatim"
    );
}

/// A missing config file refuses at resolve with a typed Resolve error
/// naming the full path it looked for.
#[test]
fn a_missing_config_path_is_a_resolve_error_naming_the_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let parsed = document(
        "pipeline: p\n\
         source:\n\
        \x20 postgres: absent.yaml\n\
         destination:\n\
        \x20 connector: {id: io.rapidbyte.duckdb, config: {}}\n",
    );
    match parsed.source.config.resolve(dir.path()) {
        Err(Error::Resolve(message)) => {
            let looked_for = dir.path().join("absent.yaml");
            assert!(
                message.starts_with(&format!("reading {}:", looked_for.display())),
                "the refusal names the full path it looked for: {message}"
            );
        }
        Err(other) => panic!("expected a Resolve error, got: {other}"),
        Ok(_) => panic!("a missing config file must not resolve"),
    }
}

/// The `connector:` arm's `config:` accepts the SAME path form as the
/// rich spellings — one resolution rule for every arm.
#[test]
fn the_connector_arm_accepts_the_config_path_form_identically() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("x.yaml"), "marker_file: value-file\n")
        .expect("config file writes");
    let parsed = document(
        "pipeline: p\n\
         source:\n\
        \x20 connector:\n\
        \x20   id: io.rapidbyte.file\n\
        \x20   config: ./x.yaml\n\
         destination:\n\
        \x20 connector: {id: io.rapidbyte.duckdb, config: {}}\n",
    );
    assert_eq!(parsed.source.id, "io.rapidbyte.file");
    assert_eq!(parsed.source.version, None);
    assert_eq!(parsed.source.path, None);
    let config = parsed
        .source
        .config
        .resolve(dir.path())
        .expect("the connector arm's path form resolves against the base");
    assert_eq!(config["marker_file"], "value-file");
}

// ---- the `connector:` vocabulary --------------------------------------

/// The full vocabulary round-trips on both sides: id, the optional
/// version pin and path override, and the opaque config document.
#[test]
fn the_connector_requirement_parses_in_both_arms() {
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
    let doc: Document = serde_yaml_ng::from_str(text).expect("the connector vocabulary parses");

    let source = &doc.source;
    assert_eq!(source.id, "io.rapidbyte.file");
    assert_eq!(source.version.as_deref(), Some("0.3.0"));
    assert_eq!(source.path.as_deref(), Some(Path::new("/explicit/bin")));
    // The config is the connector's OWN document, carried opaquely —
    // whatever YAML was written arrives as the equivalent JSON.
    let Config::Inline(config) = &source.config else {
        panic!("an inline mapping parses as the inline config form");
    };
    assert_eq!(config["streams"][0]["name"], "events");

    let dest = &doc.destination;
    assert_eq!(dest.id, "io.rapidbyte.duckdb");
    assert_eq!(dest.version, None, "version is optional");
    assert_eq!(dest.path, None, "path is optional");
    let Config::Inline(config) = &dest.config else {
        panic!("an inline mapping parses as the inline config form");
    };
    assert_eq!(config["path"], "out.db");
}

/// An unknown key inside `connector:` is refused at PARSE — the
/// requirement level denies unknown fields, so a typo cannot ride
/// silently beside the opaque config block.
#[test]
fn an_unknown_key_inside_the_connector_requirement_is_refused() {
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
    let parsed: Result<Document, _> = serde_yaml_ng::from_str(text);
    assert!(
        parsed.is_err(),
        "`binary:` is not part of the connector vocabulary and must refuse"
    );
}

/// `config` is part of the frozen vocabulary, not optional: a
/// requirement without one fails to parse rather than inventing an
/// empty document the connector never agreed to.
#[test]
fn a_connector_requirement_without_a_config_is_refused() {
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
    let parsed: Result<Document, _> = serde_yaml_ng::from_str(text);
    assert!(parsed.is_err(), "a missing config block must refuse");
}

/// The requirement's Debug ELIDES the config: that document is the
/// connector's own vocabulary and routinely carries credentials, so a
/// derived Debug would print them into any `{:?}` of a `Document` or a
/// test failure message. The marker below must never surface through
/// Debug.
#[test]
fn a_debug_render_of_a_connector_requirement_elides_the_config() {
    let reference = Connector {
        id: "io.rapidbyte.file".to_owned(),
        version: None,
        path: None,
        config: Config::Inline(serde_json::json!({ "password": "SECRET-MARKER-7f3a" })),
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

// ---- construction -----------------------------------------------------

/// A missing path-form config file refuses at resolve — before any
/// connector binary is looked for — as a typed Resolve error naming the
/// problem, and must not panic or misclassify.
#[tokio::test]
async fn missing_config_file_is_a_resolve_error() {
    let yaml = "pipeline: p\nsource:\n  rest: /no/such/file.yaml\n\
                destination:\n  file:\n    path: ./out\n";
    let doc: Document = serde_yaml_ng::from_str(yaml).expect("parses");
    match build(&doc, Path::new("")).await {
        Err(Error::Resolve(message)) => {
            assert!(message.contains("/no/such/file.yaml"), "{message}");
        }
        Err(other) => panic!("expected a Resolve error, got: {other}"),
        Ok(_) => panic!("a missing config file must not build"),
    }
}

/// A requirement whose binary exists nowhere refuses through `build`
/// with the provider's frozen NotFound spelling, VERBATIM — a typed
/// Resolve error (the CLI's exit-2 class), never a facade paraphrase.
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
    let doc: Document = serde_yaml_ng::from_str(text).expect("parses");
    match build(&doc, Path::new("")).await {
        Err(Error::Resolve(message)) => assert_eq!(
            message,
            "connector `io.rdlt.test.absent`: no binary `rdlt-connector-absent` on PATH \
             and no explicit path was given — install it (e.g. cargo install \
             rdlt-connector-absent) or set path: in the connector requirement"
        ),
        Err(other) => panic!("expected a Resolve error, got: {other}"),
        Ok(_) => panic!("a missing connector binary must not build"),
    }
}

/// `build` resolves a relative path-form config against the `base` the
/// caller passes — the include rule, NOT the working directory. The
/// config file exists only beside the (imaginary) document, so getting
/// past resolution to the frozen NotFound refusal proves the base was
/// honored; a cwd-based resolution would refuse at `reading`.
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
    let doc: Document = serde_yaml_ng::from_str(text).expect("parses");
    match build(&doc, dir.path()).await {
        Err(Error::Resolve(message)) => assert!(
            message.contains("no binary `rdlt-connector-absent`"),
            "resolution must SUCCEED (the failure is the absent binary, \
             after it): {message}"
        ),
        Err(other) => panic!("expected a Resolve error, got: {other}"),
        Ok(_) => panic!("a missing connector binary must not build"),
    }
}
