//! THE SPAWN ACCEPTANCE: a pipeline document in the one `connector:`
//! form — an id and an inline config, no `path:` override anywhere —
//! resolves by DISCOVERY (the id's last segment over the provider search
//! path), spawns the reference connector on both sides and RUNS end to
//! end. The spawn subject is the reference connector: the first-party
//! connectors live in their own repository, so their bins cannot anchor
//! an engine gate.
//!
//! Gated exactly like rdlt-runtime's spawn suites: behind the
//! `spawn-bins` feature, with `RDLT_BUILD_CONNECTOR_BINS` telling the
//! shared helper to (re)build the bin itself — the Makefile's
//! spawn-bins line sets both.

#![cfg(feature = "spawn-bins")]

use std::path::PathBuf;

use rdlt::document::Document;
use rdlt::pipeline::Pipeline;
use rdlt::runtime::local::Local;

/// The directory holding the reference bin this suite spawns, through
/// the testkit's ONE spawn scaffold — building it under
/// `RDLT_BUILD_CONNECTOR_BINS`, refusing a relative `CARGO_TARGET_DIR`,
/// failing loudly on a missing bin — rather than a local copy of those
/// mechanics (copies diverge, and a diverged copy certifies a stale
/// binary).
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
         schema_policy: evolve\n\
         resources:\n\
        \x20 byte_budget: 8388608\n\
        \x20 max_concurrent_streams: 4\n\
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
    let doc: Document = serde_yaml_ng::from_str(&yaml).expect("the connector document parses");

    // No `path:` overrides anywhere in the document, so this exercises
    // the full id → discovery route: the provider's search path stands
    // in for PATH, pointing at the built bin.
    let provider = Local::new().with_search_path(bins);
    let report = Pipeline::from_document_with(&doc, std::path::Path::new(""), &provider)
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
    let report = Pipeline::from_document_with(&doc, std::path::Path::new(""), &provider)
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
