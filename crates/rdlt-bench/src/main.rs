//! `rdlt-bench` — the four subcommands: list / run / gate / report. Each arm
//! is one call into the library; exit 0 on success, 1 on a bar violation, 2
//! on an error.

use std::process::ExitCode;

use clap::{Parser, Subcommand};

use rdlt_bench::cell::Selection;
use rdlt_bench::matrix;
use rdlt_bench::paths::Paths;

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
        selection: Selection,
    },
    /// Run cells and write artifacts to benches/results/
    Run {
        #[command(flatten)]
        selection: Selection,
    },
    /// Evaluate benches/bars.toml against the recorded artifacts (exit 1 on violation)
    Gate,
    /// Regenerate RESULTS.md tables from artifacts (narrative preserved)
    Report,
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
        Cmd::List { selection } => matrix::list(&paths, selection).map(|()| true),
        Cmd::Run { selection } => matrix::run(&paths, selection),
        Cmd::Gate => matrix::gate(&paths),
        Cmd::Report => matrix::report(&paths).map(|()| true),
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
