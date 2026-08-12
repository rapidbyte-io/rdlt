//! The facade's load-bearing e2e — the binary is NAMED `e2e` so the
//! Makefile's `TARGET=e2e` filter (`-E 'binary(/e2e/)'`) selects it by
//! name: seed a jsonl file, run reference → reference through
//! `build_pipeline` — the embedder's exact door, default provider —
//! and prove rows landed; then a SECOND build + run of the same
//! document reads ZERO rows, the persisted-cursor-across-sessions
//! claim that makes this cell load-bearing rather than a smoke.
//!
//! The `connector:` arms carry explicit `path:` overrides to the
//! testkit-built reference bin (the bins live in target/, not on
//! PATH); the id → DISCOVERY route is spawned_pipeline.rs's subject.
//! Gated under `spawn-bins` with the `RDLT_BUILD_CONNECTOR_BINS`
//! discipline, exactly like spawned_pipeline — the Makefile's
//! spawn-bins line sets both.

#![cfg(feature = "spawn-bins")]

use std::path::Path;

use rdlt::pipeline_spec::{Spec, build_pipeline};

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

#[tokio::test(flavor = "multi_thread")]
async fn a_reference_pipeline_lands_rows_once_and_a_second_session_reads_zero() {
    const ROWS: u64 = 300;
    let bin = rdlt_testkit::spawn::built_connector_bin(
        env!("CARGO_MANIFEST_DIR"),
        "rdlt-connector-reference",
    );
    let dir = tempfile::tempdir().expect("tempdir");
    let fixture = dir.path().join("events.jsonl");
    let mut text = String::new();
    for id in 0..ROWS {
        text.push_str(&format!("{{\"id\":{id},\"name\":\"row-{id}\"}}\n"));
    }
    std::fs::write(&fixture, text).expect("the fixture file writes");
    let out_dir = dir.path().join("out");

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
        \x20   config:\n\
        \x20     path: \"{out}\"\n",
        work = dir.path().join("work").display(),
        bin = bin.display(),
        fixture = fixture.display(),
        out = out_dir.display(),
    );
    let spec: Spec = serde_yaml::from_str(&yaml).expect("the connector document parses");

    let report = build_pipeline(&spec, Path::new(""))
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

    // The second session: fresh spawns over the same document. The
    // source's byte cursor persisted through the destination's state
    // document, so the unchanged file reads ZERO rows — and the
    // published data is untouched.
    let report = build_pipeline(&spec, Path::new(""))
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
