//! Parse-shape pins for the shared `rdlt::pipeline_spec` model, from
//! the CLI's side of the seam: every rich spelling parses to its arm
//! (sugar for `connector:` — the desugar pins live in the facade's own
//! `tests/desugar.rs`), the config forms, the write-mode forms, and
//! the document-level refusals.

use rdlt::pipeline_spec::{ConfigSource, DestSpec, SourceSpec, Spec, WriteModeSpec};

fn spec(yaml: &str) -> Spec {
    serde_yaml::from_str(yaml).expect("spec parses")
}

/// Each rich source spelling parses to its own arm, inline and by
/// path — one pin per spelling, minimal on purpose (the config block
/// is opaque here; its vocabulary belongs to the connector).
#[test]
fn every_rich_source_spelling_parses_to_its_arm() {
    type Check = fn(&SourceSpec) -> bool;
    let cases: [(&str, Check); 4] = [
        ("rest", |s| matches!(s, SourceSpec::Rest(_))),
        ("oracle", |s| matches!(s, SourceSpec::Oracle(_))),
        ("file", |s| matches!(s, SourceSpec::File(_))),
        ("postgres", |s| matches!(s, SourceSpec::Postgres(_))),
    ];
    for (spelling, want) in cases {
        let inline = spec(&format!(
            "pipeline: p\nsource:\n  {spelling}: {{k: v}}\ndestination:\n  duckdb: {{path: out.db}}\n"
        ));
        assert!(want(&inline.source), "{spelling}: inline mapping");

        let by_path = spec(&format!(
            "pipeline: p\nsource:\n  {spelling}: cfg.yaml\ndestination:\n  duckdb: {{path: out.db}}\n"
        ));
        assert!(want(&by_path.source), "{spelling}: path form");
    }
    // The path form parses as a PATH, not as a one-key document.
    let by_path = spec(
        "pipeline: p\nsource:\n  postgres: cfg.yaml\ndestination:\n  duckdb: {path: out.db}\n",
    );
    assert!(matches!(
        by_path.source,
        SourceSpec::Postgres(ConfigSource::Path(_))
    ));
}

/// Each rich destination spelling parses to its own arm — the
/// destination twin of the source pin above.
#[test]
fn every_rich_destination_spelling_parses_to_its_arm() {
    type Check = fn(&DestSpec) -> bool;
    let cases: [(&str, Check); 5] = [
        ("duckdb", |d| matches!(d, DestSpec::Duckdb(_))),
        ("postgres", |d| matches!(d, DestSpec::Postgres(_))),
        ("file", |d| matches!(d, DestSpec::File(_))),
        ("iceberg", |d| matches!(d, DestSpec::Iceberg(_))),
        ("snowflake", |d| matches!(d, DestSpec::Snowflake(_))),
    ];
    for (spelling, want) in cases {
        let parsed = spec(&format!(
            "pipeline: p\nsource:\n  file: {{k: v}}\ndestination:\n  {spelling}: {{k: v}}\n"
        ));
        assert!(want(&parsed.destination), "{spelling}");
    }
}

/// Every pipeline-spec form parses — write_mode's three shapes,
/// workdir default vs custom, and the policies' presence.
#[test]
fn pipeline_spec_forms_parse() {
    for (mode, want_merge) in [
        ("write_mode: append\n", false),
        ("write_mode: replace\n", false),
        ("write_mode: {merge: {key: [id]}}\n", true),
        ("", false), // absent = append default
    ] {
        let parsed = spec(&format!(
            "pipeline: p\n{mode}source:\n  postgres: s.yaml\n\
             destination:\n  duckdb: {{path: out.db}}\n"
        ));
        assert_eq!(
            matches!(parsed.write_mode, Some(WriteModeSpec::Merge { .. })),
            want_merge,
            "{mode}"
        );
    }
    let custom = spec(
        "pipeline: p\nworkdir: /tmp/x\nsource:\n  postgres: s.yaml\n\
         destination:\n  file: {path: out}\n",
    );
    assert_eq!(
        custom.workdir.as_deref(),
        Some(std::path::Path::new("/tmp/x"))
    );
    let bare = spec(
        "pipeline: p\nsource:\n  postgres: s.yaml\n\
         destination:\n  duckdb: {path: out.db}\n",
    );
    assert!(
        bare.workdir.is_none(),
        "workdir defaults downstream to .rdlt/<pipeline> beside the document"
    );
    let with_policies = spec(
        "pipeline: p\nbatch_policy: {every_rows: 50000}\n\
         commit_policy: {every_bytes: 104857600}\n\
         source:\n  postgres: s.yaml\ndestination:\n  duckdb: {path: out.db}\n",
    );
    assert!(with_policies.batch_policy.is_some());
    assert!(with_policies.commit_policy.is_some());
}

/// A typoed top-level key and an unknown source kind each refuse at
/// parse — deny_unknown_fields on the document AND on both arm enums.
#[test]
fn unknown_spellings_are_refused_at_parse() {
    let bad_kind: Result<Spec, _> = serde_yaml::from_str(
        "pipeline: p\nsource:\n  mongodb: {conn: x}\ndestination:\n  duckdb: {path: out.db}\n",
    );
    assert!(bad_kind.is_err(), "an unknown source kind must not parse");

    let bad_key: Result<Spec, _> = serde_yaml::from_str(
        "pipeline: p\nworkdirr: /tmp/x\nsource:\n  postgres: s.yaml\n\
         destination:\n  duckdb: {path: out.db}\n",
    );
    assert!(bad_key.is_err(), "a typoed top-level key must not parse");
}
