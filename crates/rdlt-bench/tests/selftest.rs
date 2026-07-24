//! US1 independent test: the self-test cell runs the full protocol end to end
//! under nextest — declare → select → warmup+runs → medians + artifact shape —
//! with zero containers and zero release-CLI dependency.

use std::collections::BTreeMap;

use rdlt_bench::runner::Paths;
use rdlt_bench::{artifact, cells, fixtures, runner};

fn repo_paths() -> Paths {
    let repo = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("crates/rdlt-bench sits two levels below the repo root")
        .to_path_buf();
    Paths {
        cells_dir: repo.join("benches/cells"),
        fixtures_toml: repo.join("benches/fixtures/fixtures.toml"),
        bars_toml: repo.join("benches/bars.toml"),
        results: repo.join("benches/results"),
        cli: repo.join("target/release/rdlt"),
        benches: repo.join("benches"),
        repo,
    }
}

#[test]
fn selftest_cell_runs_the_full_protocol() {
    let paths = repo_paths();

    // The checked-in registry must load and cross-validate as a whole.
    let all_cells = cells::load_cells(&paths.cells_dir).expect("cells load");
    let bars = if paths.bars_toml.is_file() {
        cells::load_bars(&paths.bars_toml).expect("bars load")
    } else {
        Vec::new()
    };
    cells::cross_validate(&all_cells, &bars).expect("gated↔bar bijection");

    let cell = all_cells
        .iter()
        .find(|c| c.id == "selftest-protocol")
        .expect("selftest cell declared in benches/cells/selftest.toml");
    assert_eq!(cell.suite, "selftest");
    assert_eq!(cell.runs, 3);

    let defs = fixtures::load_fixtures(&paths.fixtures_toml).expect("fixtures load");
    let def = defs
        .iter()
        .find(|f| f.id == cell.fixture)
        .expect("fixture registered");
    let started = fixtures::start(def, &BTreeMap::new()).expect("none fixture starts");

    let result = runner::run_cell(
        cell,
        &paths,
        &started,
        BTreeMap::new(),
        BTreeMap::new(),
        None,
    )
    .expect("protocol runs end to end");

    assert_eq!(result.cell_id, "selftest-protocol");
    assert_eq!(
        result.rdlt.runs_ms.len(),
        3,
        "3 counted runs (warmup uncounted)"
    );
    assert!(
        result.rdlt.median_ms >= 15.0,
        "sleep 0.02 medians ~20ms+, got {}",
        result.rdlt.median_ms
    );
    assert_eq!(result.rdlt.p95_ms, {
        let mut sorted = result.rdlt.runs_ms.clone();
        sorted.sort_by(f64::total_cmp);
        sorted[2]
    });
    // No pipeline report for a custom command — throughput honestly absent.
    assert!(result.rdlt.rows.is_none());
    assert!(result.rdlt.rows_per_s.is_none());
    // Fingerprint captured the live machine + the workload knobs rode along.
    assert!(!result.fingerprint.kernel.is_empty());
    assert_eq!(
        result.workload.get("purpose").and_then(|v| v.as_str()),
        Some("harness protocol wiring")
    );

    // Artifact round-trips through the real results dir layout (floats via
    // JSON shortest-repr can wobble in the last ulp — compare semantically).
    let tmp = tempfile::tempdir().unwrap();
    artifact::write(tmp.path(), &result).unwrap();
    let back = artifact::read(tmp.path(), "selftest-protocol").unwrap();
    assert_eq!(back.cell_id, result.cell_id);
    assert_eq!(back.fingerprint, result.fingerprint);
    assert_eq!(back.rdlt.runs_ms.len(), result.rdlt.runs_ms.len());
    assert!((back.rdlt.median_ms - result.rdlt.median_ms).abs() < 1e-6);
}
