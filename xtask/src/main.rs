//! Repository automation for rdlt.

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
}

fn main() -> anyhow::Result<ExitCode> {
    let root = workspace_root();
    match Cli::parse().command {
        Command::Lint => lint::run(&root),
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
