//! Repository automation for rdlt.

mod coverage;
mod deps;
mod lexer;
mod lint;
mod rules;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

/// Repository automation for rdlt.
#[derive(Debug, Parser)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Check comments, structure and error style in every Rust file.
    Lint,
    /// Check that workspace crates depend only on what the architecture allows.
    Deps,
    /// Fail when an llvm-cov JSON export is below the coverage thresholds.
    CoverageGate {
        /// Path to the export written by `cargo llvm-cov --json --summary-only`.
        export: PathBuf,
        /// Minimum line coverage, in percent.
        #[arg(long)]
        lines: f64,
        /// Minimum branch coverage, in percent.
        #[arg(long)]
        branches: f64,
    },
}

fn main() -> anyhow::Result<ExitCode> {
    let root = workspace_root();
    match Cli::parse().command {
        Command::Lint => lint::run(&root),
        Command::Deps => deps::run(&root),
        Command::CoverageGate {
            export,
            lines,
            branches,
        } => coverage::run(&export, lines, branches),
    }
}

/// The repository root, one level above this crate's manifest.
fn workspace_root() -> PathBuf {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .expect("xtask sits one level below the repository root")
        .to_path_buf()
}
