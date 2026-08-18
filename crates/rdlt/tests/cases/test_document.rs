//! The pipeline document end to end: the ONE arm form (`connector:`)
//! round-trips on both sides; every other arm shape — a first-party
//! short name, a stray key, two keys, none — refuses at parse naming the
//! one accepted form; the config forms and their resolution rules; the
//! write-mode forms; the document-level refusals; construction failures
//! as typed `Error::Config` — including the provider's frozen NotFound
//! spelling surfacing through `Pipeline::from_document` verbatim — and
//! the file/text doors (`from_file`, `from_text`) up to the spawn.

use std::path::Path;

use rdlt::document::connector::Connector;
use rdlt::document::{Config, Document, MAX_DOCUMENT_BYTES, WriteMode};
use rdlt::error::Error;
use rdlt::pipeline::Pipeline;

use super::support::document;

/// The one accepted form, as every arm refusal spells it.
const FORM: &str = "an arm is `connector: {id, config, …}` (version and path optional)";

/// A minimal well-formed destination arm for cells whose subject is the
/// source side.
const DEST: &str = "destination:\n  connector: {id: io.example.dst, config: {}}\n";

// ---- parse shape ------------------------------------------------------

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
            "pipeline: p\n{mode}source:\n  connector: {{id: io.example.src, config: s.yaml}}\n{DEST}"
        ));
        assert_eq!(
            matches!(parsed.write_mode, Some(WriteMode::Merge { .. })),
            want_merge,
            "{mode}"
        );
    }
    let custom = document(&format!(
        "pipeline: p\nworkdir: /tmp/x\nsource:\n  connector: {{id: io.example.src, config: s.yaml}}\n{DEST}"
    ));
    assert_eq!(custom.workdir.as_deref(), Some(Path::new("/tmp/x")));
    let bare = document(&format!(
        "pipeline: p\nsource:\n  connector: {{id: io.example.src, config: s.yaml}}\n{DEST}"
    ));
    assert!(
        bare.workdir.is_none(),
        "workdir defaults downstream to .rdlt/<pipeline> beside the document"
    );
    let with_policies = document(&format!(
        "pipeline: p\nbatch_policy: {{every_rows: 50000}}\n\
         commit_policy: {{every_bytes: 104857600}}\n\
         source:\n  connector: {{id: io.example.src, config: s.yaml}}\n{DEST}"
    ));
    assert!(with_policies.batch_policy.is_some());
    assert!(with_policies.commit_policy.is_some());
}

/// A typoed top-level key refuses at parse (deny_unknown_fields on the
/// document), and so does a typo inside the merge block.
#[test]
fn unknown_document_keys_are_refused_at_parse() {
    let bad_key: Result<Document, _> = serde_yaml_ng::from_str(&format!(
        "pipeline: p\nworkdirr: /tmp/x\nsource:\n  connector: {{id: io.example.src, config: {{}}}}\n{DEST}"
    ));
    assert!(bad_key.is_err(), "a typoed top-level key must not parse");

    // A typo INSIDE the merge block must refuse too: a silently
    // ignored `kye` would mean a merge with no key slipping toward the
    // plan-time refusal instead of failing at the typo.
    let bad_merge_key: Result<Document, _> = serde_yaml_ng::from_str(&format!(
        "pipeline: p\nwrite_mode: {{merge: {{kye: [id]}}}}\n\
         source:\n  connector: {{id: io.example.src, config: {{}}}}\n{DEST}"
    ));
    assert!(
        bad_merge_key.is_err(),
        "a typoed merge-block key must not parse"
    );
}

/// The facade knows no connector by name: a first-party short name
/// (`postgres:`), a name it never had (`mongodb:`), and the long-dead
/// `parquet:` all refuse at parse the same way — an unknown key, the
/// refusal naming it and the one accepted form. Neither alias nor
/// shorthand exists.
#[test]
fn a_short_name_in_an_arm_is_refused_at_parse_naming_the_one_form() {
    for (spelling, arm) in [
        ("postgres", "source:\n  postgres: {conn: x}\n"),
        ("mongodb", "source:\n  mongodb: {conn: x}\n"),
        ("parquet", "source:\n  parquet: {path: out}\n"),
    ] {
        let parsed: Result<Document, _> =
            serde_yaml_ng::from_str(&format!("pipeline: p\n{arm}{DEST}"));
        let error = parsed
            .expect_err("a short name is not an arm form and must not parse")
            .to_string();
        assert!(
            error.contains(&format!("unknown key `{spelling}`: {FORM}")),
            "{spelling}: the refusal names the key and the one form: {error}"
        );
    }
    // The destination side refuses identically.
    let parsed: Result<Document, _> = serde_yaml_ng::from_str(
        "pipeline: p\nsource:\n  connector: {id: io.example.src, config: {}}\n\
         destination:\n  duckdb: {path: out.db}\n",
    );
    let error = parsed
        .expect_err("a destination short name must not parse")
        .to_string();
    assert!(
        error.contains(&format!("unknown key `duckdb`: {FORM}")),
        "{error}"
    );
}

/// An arm is exactly one `connector:` key: a second key beside it, or no
/// key at all, refuses at parse naming what was found and the one form.
#[test]
fn an_arm_with_two_keys_or_none_is_refused_at_parse() {
    let two: Result<Document, _> = serde_yaml_ng::from_str(&format!(
        "pipeline: p\nsource:\n  connector: {{id: io.example.src, config: {{}}}}\n  path: /x\n{DEST}"
    ));
    let error = two
        .expect_err("two keys in one arm must not parse")
        .to_string();
    assert!(
        error.contains(&format!("two keys, `connector` and `path`: {FORM}")),
        "{error}"
    );

    let none: Result<Document, _> =
        serde_yaml_ng::from_str(&format!("pipeline: p\nsource: {{}}\n{DEST}"));
    let error = none.expect_err("an empty arm must not parse").to_string();
    assert!(error.contains(&format!("no connector: {FORM}")), "{error}");
}

// ---- config resolution ------------------------------------------------

/// The path form: a `config:` whose value is a string names a file that
/// resolves at build time, relative to the caller's base (the document's
/// own directory for a file-loaded document); the inline form is the
/// document itself, carried verbatim.
#[test]
fn a_string_config_resolves_as_a_path_relative_to_the_base() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("x.yaml"), "marker: value\n").expect("config file writes");
    let parsed = document(&format!(
        "pipeline: p\n\
         source:\n\
        \x20 connector:\n\
        \x20   id: io.example.src\n\
        \x20   config: ./x.yaml\n\
         {DEST}"
    ));
    assert_eq!(parsed.source.id, "io.example.src");
    assert!(
        matches!(parsed.source.config, Config::Path(ref p) if p == "./x.yaml"),
        "a string value is a config path"
    );
    let config = parsed
        .source
        .config
        .resolve(dir.path())
        .expect("the path form resolves against the base");
    assert_eq!(
        config["marker"], "value",
        "the config document rides through verbatim"
    );

    let inline = document(&format!(
        "pipeline: p\nsource:\n  connector: {{id: io.example.src, config: {{marker: value}}}}\n{DEST}"
    ));
    let Config::Inline(config) = &inline.source.config else {
        panic!("an inline mapping parses as the inline config form");
    };
    assert_eq!(config["marker"], "value");
}

/// A missing config file refuses at resolve with a typed Config error
/// naming the full path it looked for.
#[test]
fn a_missing_config_path_is_a_config_error_naming_the_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let parsed = document(&format!(
        "pipeline: p\nsource:\n  connector: {{id: io.example.src, config: absent.yaml}}\n{DEST}"
    ));
    match parsed.source.config.resolve(dir.path()) {
        Err(Error::Config { message }) => {
            let looked_for = dir.path().join("absent.yaml");
            assert!(
                message.starts_with(&format!("reading {}:", looked_for.display())),
                "the refusal names the full path it looked for: {message}"
            );
        }
        Err(other) => panic!("expected a Config error, got: {other}"),
        Ok(_) => panic!("a missing config file must not resolve"),
    }
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
/// connector binary is looked for — as a typed Config error naming the
/// problem, and must not panic or misclassify.
#[tokio::test]
async fn missing_config_file_is_a_config_error() {
    let yaml = format!(
        "pipeline: p\nsource:\n  connector: {{id: io.example.src, config: /no/such/file.yaml}}\n{DEST}"
    );
    let doc: Document = serde_yaml_ng::from_str(&yaml).expect("parses");
    match Pipeline::from_document(&doc, Path::new("")).await {
        Err(Error::Config { message }) => {
            assert!(message.contains("/no/such/file.yaml"), "{message}");
        }
        Err(other) => panic!("expected a Config error, got: {other}"),
        Ok(_) => panic!("a missing config file must not build"),
    }
}

/// A requirement whose binary exists nowhere refuses through
/// `from_document` with the provider's frozen NotFound spelling,
/// VERBATIM — a typed Config error (the CLI's exit-2 class), never a
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
    let doc: Document = serde_yaml_ng::from_str(text).expect("parses");
    match Pipeline::from_document(&doc, Path::new("")).await {
        Err(Error::Config { message }) => assert_eq!(
            message,
            "connector `io.rdlt.test.absent`: no binary `rdlt-connector-absent` on PATH \
             and no explicit path was given — install it (e.g. cargo install \
             rdlt-connector-absent) or set path: in the connector requirement"
        ),
        Err(other) => panic!("expected a Config error, got: {other}"),
        Ok(_) => panic!("a missing connector binary must not build"),
    }
}

/// `from_document` resolves a relative path-form config against the
/// `base` the caller passes — the include rule, NOT the working
/// directory. The config file exists only beside the (imaginary)
/// document, so getting past resolution to the frozen NotFound refusal
/// proves the base was honored; a cwd-based resolution would refuse at
/// `reading`.
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
    match Pipeline::from_document(&doc, dir.path()).await {
        Err(Error::Config { message }) => assert!(
            message.contains("no binary `rdlt-connector-absent`"),
            "resolution must SUCCEED (the failure is the absent binary, \
             after it): {message}"
        ),
        Err(other) => panic!("expected a Config error, got: {other}"),
        Ok(_) => panic!("a missing connector binary must not build"),
    }
}

// ---- the file and text doors ------------------------------------------

/// `from_file` on a path that does not exist refuses as a Config error
/// naming the path — the same `reading <path>:` spelling `document::read`
/// gives the CLI, typed into the engine's taxonomy.
#[tokio::test]
async fn from_file_on_a_missing_file_is_a_config_error_naming_the_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let missing = dir.path().join("definitely-missing.yaml");
    match Pipeline::from_file(&missing).await {
        Err(Error::Config { message }) => assert!(
            message.starts_with(&format!("reading {}:", missing.display())),
            "the refusal names the path it looked for: {message}"
        ),
        Err(other) => panic!("expected a Config error, got: {other}"),
        Ok(_) => panic!("a missing file must not build"),
    }
}

/// `from_file` reads through the document ceiling: a file over
/// `MAX_DOCUMENT_BYTES` refuses typed, naming the floor it measured
/// ("at least N bytes"), before any parse — a sparse fixture proves the
/// bounded read itself owns the cap.
#[tokio::test]
async fn from_file_on_an_oversized_file_refuses_at_the_document_cap() {
    let dir = tempfile::tempdir().expect("tempdir");
    let big = dir.path().join("big.yaml");
    std::fs::File::create(&big)
        .expect("fixture file")
        .set_len(MAX_DOCUMENT_BYTES + 1)
        .expect("sparse size");
    match Pipeline::from_file(&big).await {
        Err(Error::Config { message }) => assert!(
            message.contains(&format!("at least {} bytes", MAX_DOCUMENT_BYTES + 1))
                && message.contains(&format!("{MAX_DOCUMENT_BYTES}-byte")),
            "the refusal names the floor and the cap: {message}"
        ),
        Err(other) => panic!("expected a Config error, got: {other}"),
        Ok(_) => panic!("an oversized file must not build"),
    }
}

/// `from_file` on a file that does not parse refuses as a Config error
/// naming the file (`parsing <path>: …`) — the parse failure is a
/// document problem like the read failure, not an engine one.
#[tokio::test]
async fn from_file_on_a_malformed_document_is_a_config_error_naming_the_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let bad = dir.path().join("bad.yaml");
    std::fs::write(&bad, "pipeline: p\nsource: {}\n").expect("write");
    match Pipeline::from_file(&bad).await {
        Err(Error::Config { message }) => assert!(
            message.starts_with(&format!("parsing {}:", bad.display()))
                && message.contains(&format!("no connector: {FORM}")),
            "{message}"
        ),
        Err(other) => panic!("expected a Config error, got: {other}"),
        Ok(_) => panic!("a malformed document must not build"),
    }
}

/// `from_file` infers the base — the file's own directory: a relative
/// path-form config beside the document resolves (the file exists only
/// there, so reaching the frozen NotFound refusal proves it), and a
/// missing one refuses naming the path joined onto that directory.
#[tokio::test]
async fn from_file_resolves_relative_configs_beside_the_document() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("cfg.yaml"), "k: v\n").expect("config writes");
    let doc = dir.path().join("pipeline.yaml");
    std::fs::write(
        &doc,
        format!(
            "pipeline: p\nsource:\n  connector: {{id: io.rdlt.test.absent, config: ./cfg.yaml}}\n{DEST}"
        ),
    )
    .expect("document writes");
    match Pipeline::from_file(&doc).await {
        Err(Error::Config { message }) => assert!(
            message.contains("no binary `rdlt-connector-absent`"),
            "the config beside the document resolved; the absent binary is next: {message}"
        ),
        Err(other) => panic!("expected a Config error, got: {other}"),
        Ok(_) => panic!("a missing connector binary must not build"),
    }

    std::fs::write(
        &doc,
        format!(
            "pipeline: p\nsource:\n  connector: {{id: io.rdlt.test.absent, config: gone.yaml}}\n{DEST}"
        ),
    )
    .expect("document rewrites");
    match Pipeline::from_file(&doc).await {
        Err(Error::Config { message }) => assert!(
            message.starts_with(&format!(
                "reading {}:",
                dir.path().join("gone.yaml").display()
            )),
            "the missing config is looked for beside the document: {message}"
        ),
        Err(other) => panic!("expected a Config error, got: {other}"),
        Ok(_) => panic!("a missing config file must not build"),
    }
}

/// `from_text` takes YAML or JSON — JSON is valid YAML, so one parse
/// serves both. Both spellings of the same document reach the same
/// resolve stage: the JSON one carries a path-form config that resolves
/// against the passed base and then meets the frozen NotFound refusal;
/// the YAML one names a config that is not there and refuses at
/// `reading`, against the same base. A text that is neither refuses as
/// a Config error at parse.
#[tokio::test]
async fn from_text_takes_yaml_or_json_and_resolves_against_the_base() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(dir.path().join("cfg.yaml"), "k: v\n").expect("config writes");
    let json = r#"{"pipeline":"p","source":{"connector":{"id":"io.rdlt.test.absent","config":"cfg.yaml"}},"destination":{"connector":{"id":"io.rdlt.test.absent","config":{}}}}"#;
    match Pipeline::from_text(json, dir.path()).await {
        Err(Error::Config { message }) => assert!(
            message.contains("no binary `rdlt-connector-absent`"),
            "the JSON document parsed and its path-form config resolved: {message}"
        ),
        Err(other) => panic!("expected a Config error, got: {other}"),
        Ok(_) => panic!("a missing connector binary must not build"),
    }

    let yaml = format!(
        "pipeline: p\nsource:\n  connector: {{id: io.rdlt.test.absent, config: gone.yaml}}\n{DEST}"
    );
    match Pipeline::from_text(&yaml, dir.path()).await {
        Err(Error::Config { message }) => assert!(
            message.starts_with(&format!(
                "reading {}:",
                dir.path().join("gone.yaml").display()
            )),
            "the YAML document parsed and resolved against the base: {message}"
        ),
        Err(other) => panic!("expected a Config error, got: {other}"),
        Ok(_) => panic!("a missing config file must not build"),
    }

    match Pipeline::from_text("pipeline: p\nsource: {}\n", dir.path()).await {
        Err(Error::Config { message }) => assert!(
            message.starts_with("parsing document:")
                && message.contains(&format!("no connector: {FORM}")),
            "{message}"
        ),
        Err(other) => panic!("expected a Config error, got: {other}"),
        Ok(_) => panic!("a malformed document must not build"),
    }
}
