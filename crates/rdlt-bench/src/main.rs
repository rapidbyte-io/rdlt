//! `rdlt-bench` — list / run / gate / report (quickstart.md).
//!
//! House-style arg parsing (no clap): four subcommands, two filters.

use std::collections::BTreeMap;
use std::process::ExitCode;

use rdlt_bench::cells::{Cell, Class};
use rdlt_bench::runner::Paths;
use rdlt_bench::{BenchError, artifact, cells, competitors, fixtures, gate, report, runner};

fn usage() -> ExitCode {
    eprintln!(
        "usage: rdlt-bench <command> [options]\n\
         \n\
         commands:\n\
         \x20 list   [--class gated|scoreboard] [--filter GLOB]   show the cell matrix\n\
         \x20 run    [CELL-ID] [--class ...] [--filter GLOB]      run cells, write artifacts\n\
         \x20 gate                                                 evaluate bars.toml, exit 1 on violation\n\
         \x20 report                                               regenerate RESULTS.md tables\n\
         \n\
         Run from the repo root; gated runs need `make release` and a quiet machine."
    );
    ExitCode::from(64)
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

#[derive(Debug, Default)]
struct Selection {
    id: Option<String>,
    class: Option<Class>,
    filter: Option<String>,
}

impl Selection {
    fn parse(args: &[String]) -> Result<Self, String> {
        let mut selection = Selection::default();
        let mut it = args.iter();
        while let Some(arg) = it.next() {
            match arg.as_str() {
                "--class" => {
                    selection.class = Some(match it.next().map(String::as_str) {
                        Some("gated") => Class::Gated,
                        Some("scoreboard") => Class::Scoreboard,
                        other => return Err(format!("--class gated|scoreboard, got {other:?}")),
                    })
                }
                "--filter" => {
                    selection.filter =
                        Some(it.next().ok_or("--filter needs a glob")?.to_owned());
                }
                id if !id.starts_with('-') && selection.id.is_none() => {
                    selection.id = Some(id.to_owned());
                }
                other => return Err(format!("unknown argument `{other}`")),
            }
        }
        Ok(selection)
    }

    fn selects(&self, cell: &Cell) -> bool {
        self.id.as_deref().is_none_or(|id| id == cell.id)
            && self.class.is_none_or(|c| c == cell.class)
            && self.filter.as_deref().is_none_or(|g| glob_match(g, &cell.id))
    }
}

fn load_all(paths: &Paths) -> rdlt_bench::Result<(Vec<Cell>, Vec<cells::Bar>)> {
    let all_cells = cells::load_cells(&paths.cells_dir)?;
    let bars =
        if paths.bars_toml.is_file() { cells::load_bars(&paths.bars_toml)? } else { Vec::new() };
    cells::cross_validate(&all_cells, &bars)?;
    Ok((all_cells, bars))
}

fn cmd_list(paths: &Paths, selection: &Selection) -> rdlt_bench::Result<()> {
    let (all_cells, bars) = load_all(paths)?;
    println!(
        "{:<28} {:<10} {:<11} {:<7} {:<16} {:<5} competitors",
        "CELL", "CLASS", "MODE", "SUITE", "FIXTURE", "RUNS"
    );
    for cell in all_cells.iter().filter(|c| selection.selects(c)) {
        let competitors: Vec<&str> =
            cell.competitors.iter().map(|c| c.variant.as_str()).collect();
        let barred = bars.iter().any(|b| b.cell == cell.id);
        println!(
            "{:<28} {:<10} {:<11} {:<7} {:<16} {:<5} {}{}",
            cell.id,
            cell.class.to_string(),
            format!("{:?}", cell.mode).to_lowercase(),
            cell.suite,
            cell.fixture,
            cell.runs,
            competitors.join(","),
            if barred { "  [bar]" } else { "" },
        );
    }
    Ok(())
}

fn cmd_run(paths: &Paths, selection: &Selection) -> rdlt_bench::Result<()> {
    let (all_cells, _bars) = load_all(paths)?;
    let selected: Vec<&Cell> = all_cells.iter().filter(|c| selection.selects(c)).collect();
    if selected.is_empty() {
        return Err(BenchError("no cells match the selection".into()));
    }
    let fixture_defs = if paths.fixtures_toml.is_file() {
        fixtures::load_fixtures(&paths.fixtures_toml)?
    } else {
        Vec::new()
    };
    let variants = {
        let path = paths.benches.join("competitors/dlt/variants.toml");
        if path.is_file() { competitors::load_variants(&path)? } else { Vec::new() }
    };

    // Fixtures shared across cells within this invocation (BH2).
    let mut started: BTreeMap<String, fixtures::Started> = BTreeMap::new();
    let base_subs: BTreeMap<String, String> = BTreeMap::from([
        ("repo".to_owned(), paths.repo.display().to_string()),
        ("benches".to_owned(), paths.benches.display().to_string()),
        ("cli".to_owned(), paths.cli.display().to_string()),
    ]);

    for cell in selected {
        eprintln!("== {} ({} {:?}, {} runs) ==", cell.id, cell.class, cell.mode, cell.runs);
        if !started.contains_key(&cell.fixture) {
            let def = fixture_defs
                .iter()
                .find(|f| f.id == cell.fixture)
                .ok_or_else(|| {
                    BenchError(format!("cell `{}`: unknown fixture `{}`", cell.id, cell.fixture))
                })?;
            started.insert(cell.fixture.clone(), fixtures::start(def, &base_subs)?);
        }
        let fixture = &started[&cell.fixture];

        // Baseline FIRST (R12) — competitors run before the rdlt side.
        let mut competitor_sides = BTreeMap::new();
        let mut pin = None;
        for reference in &cell.competitors {
            let variant = variants
                .iter()
                .find(|v| v.id == reference.variant)
                .ok_or_else(|| {
                    BenchError(format!(
                        "cell `{}`: unknown competitor variant `{}`",
                        cell.id, reference.variant
                    ))
                })?;
            pin.get_or_insert_with(|| variant.pin.clone());
            let mut subs = base_subs.clone();
            subs.insert("data".into(), fixture.data.path().display().to_string());
            if let Some(conn) = fixture.conn() {
                subs.insert("conn".into(), conn.to_owned());
            }
            eprintln!("   baseline {} ...", variant.id);
            let side = competitors::run_competitor(variant, reference, cell.runs, &subs);
            if let artifact::CompetitorSide::Missing { reason } = &side {
                eprintln!("   baseline {} MISSING: {reason}", variant.id);
            }
            competitor_sides.insert(variant.id.clone(), side);
        }

        let result = runner::run_cell(cell, paths, fixture, pin, competitor_sides)?;
        let path = artifact::write(&paths.results, &result)?;
        eprintln!(
            "   median {:.1} ms  (artifact: {})",
            result.rdlt.median_ms,
            path.strip_prefix(&paths.repo).unwrap_or(&path).display()
        );
    }
    Ok(())
}

fn cmd_gate(paths: &Paths) -> rdlt_bench::Result<bool> {
    let (_cells, bars) = load_all(paths)?;
    if bars.is_empty() {
        return Err(BenchError("bars.toml is missing or empty — nothing to gate".into()));
    }
    let (verdicts, all_pass) = gate::run_gate(&bars, &paths.results)?;
    for verdict in &verdicts {
        let tag = if verdict.passed() { "PASS" } else { "FAIL" };
        println!("[{tag}] {}", verdict.detail());
    }
    println!("{}", if all_pass { "gate: all bars met" } else { "gate: VIOLATIONS above" });
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
    let args: Vec<String> = std::env::args().skip(1).collect();
    let Some((command, rest)) = args.split_first() else { return usage() };
    let selection = match Selection::parse(rest) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: {e}");
            return usage();
        }
    };
    let paths = match Paths::resolve() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("error: {e}");
            return ExitCode::from(2);
        }
    };
    let outcome = match command.as_str() {
        "list" => cmd_list(&paths, &selection).map(|()| true),
        "run" => cmd_run(&paths, &selection).map(|()| true),
        "gate" => cmd_gate(&paths),
        "report" => cmd_report(&paths).map(|()| true),
        _ => return usage(),
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
