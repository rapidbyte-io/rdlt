//! `rdlt-bench` — the four subcommands: list / run / gate / report.

use std::collections::BTreeMap;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use rdlt_bench::cells::Cell;
use rdlt_bench::paths::Paths;
use rdlt_bench::protocol::QuietVerdict;
use rdlt_bench::{
    BenchError, artifact, cells, competitors, fixtures, gate, protocol, report, runner,
};

#[derive(Debug, Parser)]
#[command(
    name = "rdlt-bench",
    about = "rdlt declarative benchmark harness: cells as data -> run -> gate -> report",
    after_help = "Run from the repo root; measured runs need `make release` and a quiet machine.\n\
                  Cells: benches/harness/cells/*.toml · bars: benches/bars.toml · artifacts: benches/results/"
)]
struct Cli {
    #[command(subcommand)]
    command: Cmd,
}

#[derive(Debug, Subcommand)]
enum Cmd {
    /// Show the cell matrix (fixture, competitors, bars)
    List {
        #[command(flatten)]
        selection: SelectionArgs,
    },
    /// Run cells and write artifacts to benches/results/
    Run {
        #[command(flatten)]
        selection: SelectionArgs,
    },
    /// Evaluate benches/bars.toml against the latest artifacts (exit 1 on violation)
    Gate,
    /// Regenerate RESULTS.md tables from artifacts (narrative preserved)
    Report,
}

#[derive(Debug, clap::Args)]
struct SelectionArgs {
    /// A single cell id
    cell: Option<String>,
    /// Glob over cell ids (`*` wildcards, e.g. 'pg-*')
    #[arg(long)]
    filter: Option<String>,
}

impl SelectionArgs {
    fn selects(&self, cell: &Cell) -> bool {
        self.cell.as_deref().is_none_or(|id| id == cell.id)
            && self
                .filter
                .as_deref()
                .is_none_or(|g| glob_match(g, &cell.id))
    }
}

/// `*`-only glob (the ids are kebab-case; anything fancier is YAGNI).
fn glob_match(pattern: &str, id: &str) -> bool {
    fn inner(p: &[u8], s: &[u8]) -> bool {
        match (p.first(), s.first()) {
            (None, None) => true,
            (Some(b'*'), _) => inner(&p[1..], s) || (!s.is_empty() && inner(p, &s[1..])),
            (Some(c), Some(d)) if c == d => inner(&p[1..], &s[1..]),
            _ => false,
        }
    }
    inner(pattern.as_bytes(), id.as_bytes())
}

fn load_all(paths: &Paths) -> rdlt_bench::Result<(Vec<Cell>, Vec<cells::Bar>)> {
    let all_cells = cells::load_cells(&paths.cells_dir)?;
    let bars = if paths.bars_toml.is_file() {
        cells::load_bars(&paths.bars_toml)?
    } else {
        Vec::new()
    };
    cells::cross_validate(&all_cells, &bars)?;
    Ok((all_cells, bars))
}

fn cmd_list(paths: &Paths, selection: &SelectionArgs) -> rdlt_bench::Result<()> {
    let (all_cells, bars) = load_all(paths)?;
    println!("{:<34} {:<16} {:<5} competitors", "CELL", "FIXTURE", "RUNS");
    for cell in all_cells.iter().filter(|c| selection.selects(c)) {
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

/// Everything loaded once before the per-cell loop.
struct RunContext {
    all_cells: Vec<Cell>,
    bars: Vec<cells::Bar>,
    fixture_defs: Vec<fixtures::FixtureDef>,
    variants: Vec<competitors::Variant>,
}

/// Load the cell matrix, bars, fixture definitions, and competitor variants —
/// the optional files (fixtures/variants) resolve to empty when absent.
fn prepare(paths: &Paths) -> rdlt_bench::Result<RunContext> {
    let (all_cells, bars) = load_all(paths)?;
    let fixture_defs = if paths.fixtures_toml.is_file() {
        fixtures::load_fixtures(&paths.fixtures_toml)?
    } else {
        Vec::new()
    };
    let variants = competitors::discover_variants(&paths.benches.join("competitors"))?;
    Ok(RunContext {
        all_cells,
        bars,
        fixture_defs,
        variants,
    })
}

/// One cell end to end: bring up its fixture (shared within the invocation),
/// run competitors baseline-first, then the rdlt side; write and announce the
/// artifact and return it for the run summary.
fn run_one_cell(
    cell: &Cell,
    paths: &Paths,
    ctx: &RunContext,
    base_subs: &BTreeMap<String, String>,
    started: &mut BTreeMap<String, fixtures::Started>,
) -> rdlt_bench::Result<artifact::Artifact> {
    eprintln!("== {} ({} runs) ==", cell.id, cell.runs);
    // Check the release CLI and every connector binary named by the
    // pipeline before a container starts, a row is generated, or a
    // competitor baseline runs.
    runner::preconditions(cell, paths)?;
    // Bring up every store the cell uses (shared across the invocation), then
    // collect them primary-first — the order the cell declared them in.
    for id in &cell.fixtures {
        if !started.contains_key(id) {
            let def = ctx
                .fixture_defs
                .iter()
                .find(|f| &f.id == id)
                .ok_or_else(|| BenchError(format!("cell `{}`: unknown fixture `{id}`", cell.id)))?;
            started.insert(id.clone(), fixtures::start(def, base_subs)?);
        }
    }
    let cell_fixtures: Vec<&fixtures::Started> =
        cell.fixtures.iter().map(|id| &started[id]).collect();
    let primary = cell_fixtures[0];

    // Quiet guard BEFORE anything is measured — the baselines feed the
    // recorded ratios, so they need the quiet machine as much as the rdlt side.
    let quiet_note = match protocol::quiet_guard_now()? {
        QuietVerdict::Quiet => None,
        QuietVerdict::Annotated(note) => {
            eprintln!("[{}] {note}", cell.id);
            Some(note)
        }
    };
    let forced = protocol::forced();

    // Baseline FIRST — competitors run before the rdlt side.
    let mut competitor_sides = BTreeMap::new();
    let mut competitor_pins: BTreeMap<String, String> = BTreeMap::new();
    for reference in &cell.competitors {
        let variant = ctx
            .variants
            .iter()
            .find(|v| v.id == reference.variant)
            .ok_or_else(|| {
                BenchError(format!(
                    "cell `{}`: unknown competitor variant `{}`",
                    cell.id, reference.variant
                ))
            })?;
        competitor_pins.insert(variant.id.clone(), variant.pin.clone());
        let mut subs = base_subs.clone();
        subs.insert("data".into(), primary.data_dir.path().display().to_string());
        if let Some(conn) = primary.conn() {
            subs.insert("conn".into(), conn.to_owned());
        }
        if let Some(port) = primary.def.port {
            subs.insert("port".into(), port.to_string());
        }
        eprintln!("   baseline {} ...", variant.id);
        let mut side =
            competitors::run_competitor(variant, reference, cell.runs, &subs, &cell_fixtures);
        // Measured here rather than inside the runner because each arm writes
        // to its own place and only this loop knows which arm just ran.
        if let artifact::CompetitorSide::Ok { artifact_bytes, .. } = &mut side {
            *artifact_bytes = runner::measure_artifact_bytes(
                &variant.id,
                reference.artifact_bytes_sh.as_ref(),
                &subs,
            );
        }
        // A baseline that could not run is recorded as a loud `Missing{reason}`,
        // never a silent skip. Enforcement is measurement-first (bars gate a
        // recorded session, not the live run), so the run proceeds and the
        // matrix shows the gap.
        if let artifact::CompetitorSide::Missing { reason } = &side {
            eprintln!("   baseline {} MISSING: {reason}", variant.id);
        }
        competitor_sides.insert(variant.id.clone(), side);
    }

    let result = runner::run_cell(
        cell,
        paths,
        &cell_fixtures,
        competitor_pins,
        competitor_sides,
        quiet_note,
        forced,
    )?;
    let path = artifact::write(&paths.results, &result)?;
    report::append_history(&paths.benches.join("history.jsonl"), &result)?;
    eprintln!(
        "   median {:.1} ms  (artifact: {})",
        result.rdlt.median_ms,
        path.strip_prefix(&paths.repo).unwrap_or(&path).display()
    );
    Ok(result)
}

/// The invocation's PRODUCT to stdout (progress stayed on stderr): the same
/// table renderer RESULTS.md uses — one rendering path, no drift — plus
/// immediate bar verdicts for the gated cells just measured. Returns whether
/// every bar held; a fresh gated measurement below its bar exits nonzero.
fn print_run_summary(measured: &[artifact::Artifact], bars: &[cells::Bar]) -> bool {
    println!(
        "\n## run summary ({} cell{})\n",
        measured.len(),
        if measured.len() == 1 { "" } else { "s" }
    );
    let refs: Vec<&artifact::Artifact> = measured.iter().collect();
    print!("{}", report::summary_table(&refs, bars));
    let mut all_pass = true;
    let mut any_bar = false;
    for artifact in measured {
        if let Some(note) = &artifact.fingerprint.quiet_note {
            println!("[WARN] {}: {note}", artifact.cell_id);
        }
        for bar in bars.iter().filter(|b| b.cell == artifact.cell_id) {
            any_bar = true;
            all_pass &= gate::print_verdict(&gate::evaluate(bar, artifact));
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

fn cmd_run(paths: &Paths, selection: &SelectionArgs) -> rdlt_bench::Result<bool> {
    let ctx = prepare(paths)?;
    let selected: Vec<&Cell> = ctx
        .all_cells
        .iter()
        .filter(|c| selection.selects(c))
        .collect();
    if selected.is_empty() {
        return Err(BenchError("no cells match the selection".into()));
    }
    let base_subs: BTreeMap<String, String> = BTreeMap::from([
        ("repo".to_owned(), paths.repo.display().to_string()),
        ("benches".to_owned(), paths.benches.display().to_string()),
        ("cli".to_owned(), paths.cli.display().to_string()),
    ]);
    // Fixtures shared across cells within this invocation.
    let mut started: BTreeMap<String, fixtures::Started> = BTreeMap::new();
    let mut measured: Vec<artifact::Artifact> = Vec::new();
    for cell in selected {
        measured.push(run_one_cell(cell, paths, &ctx, &base_subs, &mut started)?);
    }
    Ok(print_run_summary(&measured, &ctx.bars))
}

fn cmd_gate(paths: &Paths) -> rdlt_bench::Result<bool> {
    let (_cells, bars) = load_all(paths)?;
    if bars.is_empty() {
        return Err(BenchError(
            "bars.toml is missing or empty — nothing to gate".into(),
        ));
    }
    let (verdicts, all_pass) = gate::run_gate(&bars, &paths.results)?;
    for verdict in &verdicts {
        gate::print_verdict(verdict);
    }
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

fn cmd_report(paths: &Paths) -> rdlt_bench::Result<()> {
    let (all_cells, bars) = load_all(paths)?;
    let results_md = paths.benches.join("RESULTS.md");
    let updated = report::regenerate(&results_md, &all_cells, &bars, &paths.results)?;
    if updated.is_empty() {
        println!("no artifacts found — nothing regenerated");
    } else {
        println!("regenerated sections: {}", updated.join(", "));
    }
    Ok(())
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let paths = match Paths::resolve() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    };
    let outcome = match &cli.command {
        Cmd::List { selection } => cmd_list(&paths, selection).map(|()| true),
        Cmd::Run { selection } => cmd_run(&paths, selection),
        Cmd::Gate => cmd_gate(&paths),
        Cmd::Report => cmd_report(&paths).map(|()| true),
    };
    match outcome {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::from(1),
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::from(2)
        }
    }
}
