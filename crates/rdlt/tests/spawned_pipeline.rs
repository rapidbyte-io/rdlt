//! THE D1 SWAP'S LIVE ACCEPTANCE (043 T1): a pipeline document written
//! entirely in RICH spellings — `file:` source, `duckdb:` destination,
//! no `connector:` block anywhere — desugars, discovers, spawns and
//! RUNS end to end. This is the proof that the sugar reaches real
//! binaries, not just the desugar table (`tests/desugar.rs` holds that
//! half offline).
//!
//! Gated exactly like rdlt-runtime's spawn suites: behind the
//! `spawn-bins` feature, with `RDLT_BUILD_CONNECTOR_BINS` telling the
//! shared helper to (re)build the bins itself — the Makefile's
//! spawn-bins line sets both.

#![cfg(feature = "spawn-bins")]

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use rdlt::pipeline_spec::{Spec, build_pipeline_with};
use rdlt::runtime::LocalBinaryConnectorProvider;

/// The workspace root: two levels above this crate's own manifest.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/rdlt sits two levels below the workspace root")
        .to_path_buf()
}

/// Where debug binaries land — `CARGO_TARGET_DIR` honored (absolute
/// used as-is, relative resolved against the repo root, exactly as
/// cargo treats it), the same rule as rdlt-runtime's spawn suites.
fn target_debug_dir() -> PathBuf {
    match std::env::var_os("CARGO_TARGET_DIR") {
        Some(target) => {
            let target = PathBuf::from(target);
            if target.is_absolute() {
                target.join("debug")
            } else {
                workspace_root().join(target).join("debug")
            }
        }
        None => workspace_root().join("target/debug"),
    }
}

/// Build the two bins this suite spawns (once per test process) when
/// `RDLT_BUILD_CONNECTOR_BINS` is set; without it, require them to
/// already exist and fail loudly — never build behind the runner's
/// back, never skip silently.
fn ensure_bins() {
    static BUILT: OnceLock<()> = OnceLock::new();
    BUILT.get_or_init(|| {
        if std::env::var_os("RDLT_BUILD_CONNECTOR_BINS").is_none() {
            // Opt-in rebuild, same residue as the runtime suites: a
            // stale bin passes green here, so say so out loud.
            eprintln!(
                "note: RDLT_BUILD_CONNECTOR_BINS is unset — spawning the connector \
                 binaries already on disk WITHOUT rebuilding. Whatever this suite \
                 proves is about those binaries, not necessarily the current source. \
                 The Makefile's spawn-bins lines set the var."
            );
            return;
        }
        let cargo = std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into());
        let status = std::process::Command::new(&cargo)
            .current_dir(workspace_root())
            .args([
                "build",
                "-p",
                "rdlt-connector-file",
                "-p",
                "rdlt-connector-duckdb",
                "--features",
                "bin-serve",
                "--bin",
                "rdlt-connector-file",
                "--bin",
                "rdlt-connector-duckdb",
            ])
            .status()
            .expect("cargo build for the connector bins spawns");
        assert!(status.success(), "building the connector bins failed");
    });
    for name in ["rdlt-connector-file", "rdlt-connector-duckdb"] {
        let path = target_debug_dir().join(name);
        assert!(
            path.is_file(),
            "connector binary `{}` is not built — run the Makefile's spawn-bins \
             line (it sets RDLT_BUILD_CONNECTOR_BINS=1) or `cargo build -p {name} \
             --features bin-serve` first",
            path.display()
        );
    }
}

/// Rich spellings only, both sides — the exact document shape the D1
/// swap re-routes from compiled-in machinery to spawned binaries.
#[tokio::test(flavor = "multi_thread")]
async fn a_rich_spelling_document_runs_over_spawned_binaries() {
    const ROWS: u64 = 200;
    ensure_bins();
    let dir = tempfile::tempdir().expect("tempdir");
    let src_dir = dir.path().join("src");
    std::fs::create_dir_all(&src_dir).expect("fixture dir");
    let mut text = String::new();
    for id in 0..ROWS {
        text.push_str(&format!("{{\"id\":{id},\"name\":\"row-{id}\"}}\n"));
    }
    std::fs::write(src_dir.join("rows.jsonl"), text).expect("fixture writes");

    let yaml = format!(
        "pipeline: d1-swap-acceptance\n\
         workdir: {work}\n\
         source:\n\
        \x20 file:\n\
        \x20   streams:\n\
        \x20     - name: events\n\
        \x20       format: jsonl\n\
        \x20       path: \"{glob}\"\n\
         destination:\n\
        \x20 duckdb:\n\
        \x20   path: {db}\n",
        work = dir.path().join("work").display(),
        glob = src_dir.join("*.jsonl").display(),
        db = dir.path().join("out.duckdb").display(),
    );
    let spec: Spec = serde_yaml::from_str(&yaml).expect("the rich-spelling document parses");

    // No `path:` overrides anywhere in the document, so this exercises
    // the full desugar → discovery route: the provider's search path
    // stands in for PATH, pointing at the built bins.
    let provider = LocalBinaryConnectorProvider::new().with_search_path(target_debug_dir());
    let report = build_pipeline_with(&spec, std::path::Path::new(""), &provider)
        .await
        .expect("both rich arms resolve to spawned connectors")
        .run()
        .await
        .expect("the run over two spawned connectors succeeds");
    assert_eq!(report.total_rows(), ROWS, "every fixture row committed");

    let db = dir.path().join("out.duckdb");
    assert!(
        db.is_file() && db.metadata().expect("metadata").len() > 0,
        "the duckdb destination materialized its database file"
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
