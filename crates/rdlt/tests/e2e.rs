//! The facade's load-bearing e2e — the binary is NAMED `e2e` so the
//! Makefile's `TARGET=e2e` filter (`-E 'binary(e2e)'`) selects it by
//! name: seed a jsonl file, run reference → reference through
//! `Pipeline::from_file` — the embedder's exact door, default provider,
//! base inferred from the file — and prove rows landed; then a SECOND
//! construction (`from_document`, the parsed form) + run of the same
//! document reads ZERO rows, the persisted-cursor-across-sessions
//! claim that makes this cell load-bearing rather than a smoke. A
//! second cell runs the same pipeline from a JSON text through
//! `from_text`.
//!
//! The `connector:` arms carry explicit `path:` overrides to the
//! testkit-built reference bin (the bins live in target/, not on
//! PATH); the id → DISCOVERY route is spawned_pipeline.rs's subject.
//! Gated under `spawn-bins` with the `RDLT_BUILD_CONNECTOR_BINS`
//! discipline, exactly like spawned_pipeline — the Makefile's
//! spawn-bins line sets both.

#![cfg(feature = "spawn-bins")]

use std::path::Path;

use rdlt::document;
use rdlt::pipeline::Pipeline;

/// Count the rows in every published `events-…jsonl` part — the
/// reference destination's visibility contract (underscore-prefixed
/// names are bookkeeping and staging temporaries, never data).
fn published_rows(out_dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(out_dir) else {
        return 0;
    };
    let mut rows = 0;
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('_') || !name.ends_with(".jsonl") {
            continue;
        }
        let text = std::fs::read_to_string(entry.path()).expect("a published part reads");
        rows += text.lines().count() as u64;
    }
    rows
}

/// Seed `ROWS` jsonl rows under `dir` and hand back the fixture path
/// and the output directory the destination will publish into.
fn seed(dir: &Path, rows: u64) -> (std::path::PathBuf, std::path::PathBuf) {
    let fixture = dir.join("events.jsonl");
    let mut text = String::new();
    for id in 0..rows {
        text.push_str(&format!("{{\"id\":{id},\"name\":\"row-{id}\"}}\n"));
    }
    std::fs::write(&fixture, text).expect("the fixture file writes");
    (fixture, dir.join("out"))
}

#[tokio::test(flavor = "multi_thread")]
async fn a_reference_pipeline_lands_rows_once_and_a_second_session_reads_zero() {
    const ROWS: u64 = 300;
    let bin = rdlt_testkit::spawn::built_connector_bin(
        env!("CARGO_MANIFEST_DIR"),
        "rdlt-connector-reference",
    );
    let dir = tempfile::tempdir().expect("tempdir");
    let (fixture, out_dir) = seed(dir.path(), ROWS);

    // The destination arm's config is the PATH form, spelled relative:
    // it lives beside the pipeline document, and `from_file` infers that
    // directory as the base — the same include rule the CLI follows.
    std::fs::write(
        dir.path().join("dest.yaml"),
        format!("path: \"{}\"\n", out_dir.display()),
    )
    .expect("the destination config writes");
    let yaml = format!(
        "pipeline: e2e-reference\n\
         workdir: {work}\n\
         source:\n\
        \x20 connector:\n\
        \x20   id: io.rapidbyte.reference\n\
        \x20   path: {bin}\n\
        \x20   config:\n\
        \x20     path: \"{fixture}\"\n\
         destination:\n\
        \x20 connector:\n\
        \x20   id: io.rapidbyte.reference\n\
        \x20   path: {bin}\n\
        \x20   config: dest.yaml\n",
        work = dir.path().join("work").display(),
        bin = bin.display(),
        fixture = fixture.display(),
    );
    let document_path = dir.path().join("pipeline.yaml");
    std::fs::write(&document_path, &yaml).expect("the pipeline document writes");

    let report = Pipeline::from_file(&document_path)
        .await
        .expect("both connector arms spawn and handshake")
        .run()
        .await
        .expect("the run over two spawned reference connectors succeeds");
    assert_eq!(report.total_rows(), ROWS, "every fixture row committed");
    assert_eq!(
        published_rows(&out_dir),
        ROWS,
        "every committed row is reader-visible at the destination"
    );

    // The second session: fresh spawns over the same document, this
    // time from its parsed form with the base passed explicitly. The
    // source's byte cursor persisted through the destination's state
    // document, so the unchanged file reads ZERO rows — and the
    // published data is untouched.
    let doc = document::parse(&yaml).expect("the connector document parses");
    let report = Pipeline::from_document(&doc, dir.path())
        .await
        .expect("fresh spawns for the second session")
        .run()
        .await
        .expect("the second session succeeds");
    assert_eq!(
        report.total_rows(),
        0,
        "the persisted cursor crossed sessions: the unchanged file re-reads nothing"
    );
    assert_eq!(
        published_rows(&out_dir),
        ROWS,
        "the second session added nothing — still exactly-once"
    );
}

/// The same pipeline as a JSON text through `from_text`: JSON is valid
/// YAML, so the one parse takes it, and the built pipeline runs end to
/// end over the spawned reference connectors.
#[tokio::test(flavor = "multi_thread")]
async fn a_json_document_builds_through_from_text_and_runs() {
    const ROWS: u64 = 50;
    let bin = rdlt_testkit::spawn::built_connector_bin(
        env!("CARGO_MANIFEST_DIR"),
        "rdlt-connector-reference",
    );
    let dir = tempfile::tempdir().expect("tempdir");
    let (fixture, out_dir) = seed(dir.path(), ROWS);
    let json = serde_json::json!({
        "pipeline": "e2e-reference-json",
        "workdir": dir.path().join("work"),
        "source": {"connector": {"id": "io.rapidbyte.reference", "path": bin,
                                 "config": {"path": fixture}}},
        "destination": {"connector": {"id": "io.rapidbyte.reference", "path": bin,
                                      "config": {"path": out_dir}}},
    })
    .to_string();

    let report = Pipeline::from_text(&json, dir.path())
        .await
        .expect("a JSON document parses and both arms spawn")
        .run()
        .await
        .expect("the run over two spawned reference connectors succeeds");
    assert_eq!(report.total_rows(), ROWS, "every fixture row committed");
    assert_eq!(
        published_rows(&out_dir),
        ROWS,
        "every committed row is visible"
    );
}
