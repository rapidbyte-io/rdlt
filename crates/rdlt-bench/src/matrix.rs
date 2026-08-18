//! The four commands over the loaded registry: `list` the matrix, `run` the
//! selected cells (fixtures shared across the invocation, competitors
//! baseline-first, then the product, artifact written, history appended,
//! summary printed), `gate` the bars against the recorded ledger, `report`
//! the RESULTS.md regeneration. Progress goes to stderr; each command's
//! product goes to stdout.

use std::collections::BTreeMap;

use crate::artifact::{self, Artifact, CompetitorSide};
use crate::bar::{self, Bar};
use crate::cell::{self, Cell, Selection};
use crate::competitor::{self, variant::Variant};
use crate::error::{Error, Result};
use crate::measure::{self, Quiet};
use crate::paths::Paths;
use crate::{fixture, history, product, report};

/// Everything loaded once before the per-cell loop.
#[derive(Debug)]
pub(crate) struct Context {
    pub(crate) cells: Vec<Cell>,
    pub(crate) bars: Vec<Bar>,
    pub(crate) fixtures: Vec<fixture::Def>,
    pub(crate) variants: Vec<Variant>,
}

/// The cells and their bars, cross-validated — what every command needs.
fn registry(paths: &Paths) -> Result<(Vec<Cell>, Vec<Bar>)> {
    let cells = cell::load(&paths.cells_dir)?;
    let bars = if paths.bars_toml.is_file() {
        bar::load(&paths.bars_toml)?
    } else {
        Vec::new()
    };
    bar::cross_validate(&cells, &bars)?;
    Ok((cells, bars))
}

/// Load the cell matrix, bars, fixture definitions, and competitor variants —
/// the optional files (fixtures/variants) resolve to empty when absent.
pub(crate) fn prepare(paths: &Paths) -> Result<Context> {
    let (cells, bars) = registry(paths)?;
    let fixtures = if paths.fixtures_toml.is_file() {
        fixture::load(&paths.fixtures_toml)?
    } else {
        Vec::new()
    };
    let variants = competitor::variant::discover(&paths.competitors_dir)?;
    Ok(Context {
        cells,
        bars,
        fixtures,
        variants,
    })
}

/// Show the cell matrix: fixtures, run count, competitor arms, whether a bar
/// governs the cell.
pub fn list(paths: &Paths, selection: &Selection) -> Result<()> {
    let (cells, bars) = registry(paths)?;
    println!("{:<34} {:<16} {:<5} competitors", "CELL", "FIXTURE", "RUNS");
    for cell in cells.iter().filter(|c| selection.selects(c)) {
        let competitors: Vec<&str> = cell
            .competitors
            .iter()
            .map(|c| c.variant.as_str())
            .collect();
        let barred = bars.iter().any(|b| b.cell == cell.id);
        println!(
            "{:<34} {:<16} {:<5} {}{}",
            cell.id,
            cell.fixtures.join(","),
            cell.runs,
            competitors.join(","),
            if barred { "  [bar]" } else { "" },
        );
    }
    Ok(())
}

/// The cell's record: the product measurement beside its competitor arms,
/// under the fingerprint of the machine, the fixtures' dataset identities and
/// the competitor pins, with every competitor→rdlt ratio filled in.
pub fn assemble(
    cell: &Cell,
    measured: product::Measurement,
    fixtures: &[&fixture::Live],
    competitor_pins: BTreeMap<String, String>,
    competitors: BTreeMap<String, CompetitorSide>,
    quiet_note: Option<String>,
    forced: bool,
) -> Artifact {
    // Keys are prefixed with the fixture id so a cross-store cell records
    // both stores' identities without collision.
    let mut dataset_hashes = BTreeMap::new();
    for fixture in fixtures {
        for (key, value) in &fixture.hashes {
            dataset_hashes.insert(format!("{}:{key}", fixture.def.id), value.clone());
        }
    }
    let mut artifact = Artifact {
        format_version: artifact::FORMAT_VERSION,
        cell_id: cell.id.clone(),
        recorded_at: artifact::recorded_at(),
        fingerprint: artifact::fingerprint(dataset_hashes, competitor_pins, quiet_note),
        workload: cell.workload.clone(),
        rdlt: measured.rdlt,
        competitors,
        verify: measured.verify,
        forced,
        extra: serde_json::Map::new(),
    };
    let rdlt_median = artifact.rdlt.median_ms;
    for side in artifact.competitors.values_mut() {
        if let CompetitorSide::Ok {
            median_ms,
            ratio_vs_rdlt,
            ..
        } = side
        {
            *ratio_vs_rdlt = Some(*median_ms / rdlt_median);
        }
    }
    artifact
}

/// One cell end to end: preconditions, its fixtures up (shared across the
/// invocation, collected primary-first), the quiet guard, competitors
/// baseline-first, then the product; the artifact written and announced,
/// the history appended, the artifact returned for the run summary.
pub(crate) fn run_cell(
    cell: &Cell,
    paths: &Paths,
    ctx: &Context,
    started: &mut BTreeMap<String, fixture::Live>,
) -> Result<Artifact> {
    eprintln!("== {} ({} runs) ==", cell.id, cell.runs);
    // The release CLI and every connector binary the pipeline names are
    // checked before a container starts, a row is generated, or a competitor
    // baseline runs.
    product::preconditions(cell, paths)?;
    let base_subs: BTreeMap<String, String> = BTreeMap::from([
        ("repo".to_owned(), paths.repo.display().to_string()),
        ("benches".to_owned(), paths.benches.display().to_string()),
        ("cli".to_owned(), paths.cli.display().to_string()),
    ]);
    for id in &cell.fixtures {
        if !started.contains_key(id) {
            let def = ctx
                .fixtures
                .iter()
                .find(|f| &f.id == id)
                .ok_or_else(|| Error(format!("cell `{}`: unknown fixture `{id}`", cell.id)))?;
            started.insert(id.clone(), fixture::start(def, &base_subs)?);
        }
    }
    let cell_fixtures: Vec<&fixture::Live> = cell.fixtures.iter().map(|id| &started[id]).collect();
    let subs = product::substitutions(paths, cell_fixtures[0]);

    // Quiet guard BEFORE anything is measured — the baselines feed the
    // recorded ratios, so they need the quiet machine as much as the product.
    let quiet_note = match measure::guard()? {
        Quiet::Settled => None,
        Quiet::Annotated(note) => {
            eprintln!("[{}] {note}", cell.id);
            Some(note)
        }
    };
    let forced = measure::forced();

    // Baseline FIRST — competitors run before the product side.
    let mut competitor_sides = BTreeMap::new();
    let mut competitor_pins: BTreeMap<String, String> = BTreeMap::new();
    for reference in &cell.competitors {
        let variant = ctx
            .variants
            .iter()
            .find(|v| v.id == reference.variant)
            .ok_or_else(|| {
                Error(format!(
                    "cell `{}`: unknown competitor variant `{}`",
                    cell.id, reference.variant
                ))
            })?;
        competitor_pins.insert(variant.id.clone(), variant.pin.clone());
        eprintln!("   baseline {} ...", variant.id);
        let mut side = competitor::run(variant, reference, cell.runs, &subs, &cell_fixtures);
        if let CompetitorSide::Ok { artifact_bytes, .. } = &mut side {
            *artifact_bytes = product::measure_artifact_bytes(
                &variant.id,
                reference.artifact_bytes_sh.as_ref(),
                &subs,
            );
        }
        // A baseline that could not run is a loud `Missing{reason}`, never a
        // silent skip: enforcement is measurement-first (bars gate a
        // recorded session, not the live run), so the run proceeds and the
        // matrix shows the gap.
        if let CompetitorSide::Missing { reason } = &side {
            eprintln!("   baseline {} MISSING: {reason}", variant.id);
        }
        competitor_sides.insert(variant.id.clone(), side);
    }

    let measured = product::run(cell, paths, &cell_fixtures, &subs)?;
    let result = assemble(
        cell,
        measured,
        &cell_fixtures,
        competitor_pins,
        competitor_sides,
        quiet_note,
        forced,
    );
    // Selftest output is harness machinery, never a recording: its artifact
    // goes to the gitignored `raw/` dir and it never touches the history
    // feed — otherwise every gate run would append machinery lines to (and
    // dirty) the committed feed.
    let is_selftest = cell::is_selftest(&cell.id);
    let artifact_dir = if is_selftest {
        paths.results.join("raw")
    } else {
        paths.results.clone()
    };
    let path = artifact::write(&artifact_dir, &result)?;
    if !is_selftest {
        history::append(&paths.history, &result)?;
    }
    eprintln!(
        "   median {:.1} ms  (artifact: {})",
        result.rdlt.median_ms,
        path.strip_prefix(&paths.repo).unwrap_or(&path).display()
    );
    Ok(result)
}

/// The invocation's product to stdout: the same table renderer RESULTS.md
/// uses, plus immediate bar verdicts for the gated cells just measured.
/// Returns whether every bar held.
fn summary(measured: &[Artifact], bars: &[Bar]) -> bool {
    println!(
        "\n## run summary ({} cell{})\n",
        measured.len(),
        if measured.len() == 1 { "" } else { "s" }
    );
    let refs: Vec<&Artifact> = measured.iter().collect();
    print!("{}", report::summary_table(&refs, bars));
    let mut all_pass = true;
    let mut any_bar = false;
    for artifact in measured {
        if let Some(note) = &artifact.fingerprint.quiet_note {
            println!("[WARN] {}: {note}", artifact.cell_id);
        }
        for bar in bars.iter().filter(|b| b.cell == artifact.cell_id) {
            any_bar = true;
            let verdict = bar::evaluate(bar, artifact);
            println!("{verdict}");
            all_pass &= verdict.passed();
        }
    }
    if any_bar {
        println!(
            "{}",
            if all_pass {
                "bars: all met for this run"
            } else {
                "bars: VIOLATIONS above — artifact written, exit 1"
            }
        );
    }
    println!(
        "artifacts: benches/results/  ·  full gate: `rdlt-bench gate`  ·  tables: `rdlt-bench report`"
    );
    all_pass
}

/// Run every selected cell, fixtures shared across the invocation, and print
/// the run summary. Returns whether every bar over the measured cells held —
/// a fresh gated measurement below its bar exits nonzero.
pub fn run(paths: &Paths, selection: &Selection) -> Result<bool> {
    let ctx = prepare(paths)?;
    let selected: Vec<&Cell> = ctx.cells.iter().filter(|c| selection.selects(c)).collect();
    if selected.is_empty() {
        return Err(Error("no cells match the selection".into()));
    }
    let mut started: BTreeMap<String, fixture::Live> = BTreeMap::new();
    let mut measured: Vec<Artifact> = Vec::new();
    for cell in selected {
        measured.push(run_cell(cell, paths, &ctx, &mut started)?);
    }
    Ok(summary(&measured, &ctx.bars))
}

/// Judge every bar against the recorded ledger and print the verdicts.
/// Returns whether all bars held. An absent ledger refuses with instructions
/// BEFORE the per-bar loop — otherwise every bar fails "no artifact" and the
/// real problem (no recorded session in this checkout) hides behind bar
/// noise; the artifacts half only, since gate never reads the history feed.
pub fn gate(paths: &Paths) -> Result<bool> {
    let (_cells, bars) = registry(paths)?;
    if bars.is_empty() {
        return Err(Error(
            "bars.toml is missing or empty — nothing to gate".into(),
        ));
    }
    paths.require_recorded_artifacts()?;
    let verdicts = bar::run(paths, &bars);
    for verdict in &verdicts {
        println!("{verdict}");
    }
    let all_pass = bar::passed(&verdicts);
    println!(
        "{}",
        if all_pass {
            "gate: all bars met"
        } else {
            "gate: VIOLATIONS above"
        }
    );
    Ok(all_pass)
}

/// Regenerate RESULTS.md's tables from the recorded ledger — the same
/// artifacts and feed the gate's bars bind against. A checkout without a
/// recorded session refuses rather than splicing a header-only matrix and
/// empty trends over the recorded tables the bars cite.
pub fn report(paths: &Paths) -> Result<()> {
    let (cells, bars) = registry(paths)?;
    paths.require_recorded_ledger()?;
    let updated = report::regenerate(paths, &cells, &bars)?;
    println!("regenerated sections: {}", updated.join(", "));
    Ok(())
}
