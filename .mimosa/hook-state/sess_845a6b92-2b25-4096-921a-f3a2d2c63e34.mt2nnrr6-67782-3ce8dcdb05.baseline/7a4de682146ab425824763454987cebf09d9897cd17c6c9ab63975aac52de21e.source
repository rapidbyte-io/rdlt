//! THE SPAWN ACCEPTANCE: a pipeline document written in the frozen
//! `connector:` vocabulary — an id and an inline config, no `path:`
//! override anywhere — resolves by DISCOVERY (D-039-1's last-segment
//! convention over the provider search path), spawns the reference
//! connector on both sides and RUNS end to end. Since 044 the spawn
//! subject is the reference connector: the seven first-party
//! connectors move to their own repository, and their bins can no
//! longer anchor an engine gate — while the desugar TABLE keeps all
//! seven rich spellings, pinned offline (`tests/desugar.rs` holds the
//! full table; one assertion here keeps a live sample beside the
//! spawn path).
//!
//! Gated exactly like rdlt-runtime's spawn suites: behind the
//! `spawn-bins` feature, with `RDLT_BUILD_CONNECTOR_BINS` telling the
//! shared helper to (re)build the bin itself — the Makefile's
//! spawn-bins line sets both.

#![cfg(feature = "spawn-bins")]

use std::path::PathBuf;

use rdlt::pipeline_spec::{ConfigSource, Spec, build_pipeline_with};
use rdlt::runtime::LocalBinaryConnectorProvider;

/// The directory holding the reference bin this suite spawns, through
/// the testkit's ONE spawn scaffold — building it under
/// `RDLT_BUILD_CONNECTOR_BINS`, refusing a relative `CARGO_TARGET_DIR`,
/// failing loudly on a missing bin — rather than a local copy of those
/// mechanics (the 042 lesson: copies diverge, and a diverged copy
/// certifies a stale binary).
fn bins_dir() -> PathBuf {
    rdlt_testkit::spawn::built_connector_bin(env!("CARGO_MANIFEST_DIR"), "rdlt-connector-reference")
        .parent()
        .expect("a built bin has a parent directory")
        .to_path_buf()
}

/// `connector:` refs only, both sides, no `path:` override — the full
/// id → discovery → spawn route over the search path, then rows landing
/// exactly-once and the cursor surviving into a second session.
#[tokio::test(flavor = "multi_thread")]
async fn a_connector_document_runs_over_discovered_spawned_binaries() {
    const ROWS: u64 = 200;
    let bins = bins_dir();
    let dir = tempfile::tempdir().expect("tempdir");
    let src_dir = dir.path().join("src");
    std::fs::create_dir_all(&src_dir).expect("fixture dir");
    let mut text = String::new();
    for id in 0..ROWS {
        text.push_str(&format!("{{\"id\":{id},\"name\":\"row-{id}\"}}\n"));
    }
    std::fs::write(src_dir.join("events.jsonl"), text).expect("fixture writes");

    let yaml = format!(
        "pipeline: spawn-acceptance\n\
         workdir: {work}\n\
         source:\n\
        \x20 connector:\n\
        \x20   id: io.rapidbyte.reference\n\
        \x20   config:\n\
        \x20     path: \"{fixture}\"\n\
         destination:\n\
        \x20 connector:\n\
        \x20   id: io.rapidbyte.reference\n\
        \x20   config:\n\
        \x20     path: \"{out}\"\n",
        work = dir.path().join("work").display(),
        fixture = src_dir.join("events.jsonl").display(),
        out = dir.path().join("out").display(),
    );
    let spec: Spec = serde_yaml_ng::from_str(&yaml).expect("the connector document parses");

    // No `path:` overrides anywhere in the document, so this exercises
    // the full id → discovery route: the provider's search path stands
    // in for PATH, pointing at the built bin.
    let provider = LocalBinaryConnectorProvider::new().with_search_path(bins);
    let report = build_pipeline_with(&spec, std::path::Path::new(""), &provider)
        .await
        .expect("both connector arms resolve to spawned connectors")
        .run()
        .await
        .expect("the run over two spawned connectors succeeds");
    assert_eq!(report.total_rows(), ROWS, "every fixture row committed");

    let out = dir.path().join("out");
    assert!(
        std::fs::read_dir(&out)
            .expect("the destination materialized its output directory")
            .filter_map(|entry| entry.ok())
            .any(|entry| {
                let name = entry.file_name().to_string_lossy().into_owned();
                name.starts_with("events-") && name.ends_with(".jsonl")
            }),
        "the reference destination published at least one events part"
    );

    // The cursor round-tripped over the wire: a second build (fresh
    // spawns) reads nothing new — exactly-once across sessions.
    let report = build_pipeline_with(&spec, std::path::Path::new(""), &provider)
        .await
        .expect("fresh spawns for the second run")
        .run()
        .await
        .expect("the second run succeeds");
    assert_eq!(
        report.total_rows(),
        0,
        "the committed cursor crossed the wire back: nothing re-reads"
    );
}

/// The sugar surface, sampled live beside the spawn path: one rich
/// spelling still desugars to exactly its table id with the config
/// verbatim — stopping AT the desugar table, no spawn, so this pin
/// outlives the connectors' move out of this repo. The full
/// seven-spelling table is tests/desugar.rs's.
#[test]
fn a_rich_spelling_still_desugars_to_its_table_id_without_spawning() {
    let spec: Spec = serde_yaml_ng::from_str(
        "pipeline: p\n\
         source:\n\
        \x20 file:\n\
        \x20   marker: value\n\
         destination:\n\
        \x20 connector: {id: io.rapidbyte.reference, config: {path: out}}\n",
    )
    .expect("the rich-spelling document parses");
    let reference = spec
        .source
        .desugar(std::path::Path::new(""))
        .expect("the rich source arm desugars");
    assert_eq!(reference.id, "io.rapidbyte.file", "the desugar table's id");
    let ConfigSource::Inline(config) = &reference.config else {
        panic!("a desugared reference carries its config inline");
    };
    assert_eq!(
        config["marker"], "value",
        "the config document rides through verbatim"
    );
}
