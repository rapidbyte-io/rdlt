//! The argument surface, as data. Only flags that DO something are
//! declared: a flag whose feature has not landed yet would be
//! accepted-and-ignored, the defect class this workspace refuses.

use std::path::PathBuf;

use clap::{ArgAction, Parser, Subcommand};

/// rdlt — move data with exactly-once commits.
#[derive(Debug, Parser)]
#[command(name = "rdlt", version, about, disable_help_subcommand = true)]
pub struct Cli {
    /// Quieter: suppress the per-event feed; errors and the report
    /// still appear. Overrides -v.
    #[arg(short, long, global = true)]
    pub quiet: bool,

    /// Louder: -v adds read/commit/part detail lines.
    #[arg(short, long, global = true, action = ArgAction::Count)]
    pub verbose: u8,

    /// Never draw the live progress display; log a line per event
    /// instead (the automatic behaviour when stderr is not a
    /// terminal).
    #[arg(long, global = true)]
    pub no_progress: bool,

    /// When to color output. `auto` follows the terminal and NO_COLOR.
    #[arg(long, global = true, value_enum, default_value_t = ColorChoice::Auto)]
    pub color: ColorChoice,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Run a pipeline to completion (resumable; exactly-once).
    Run {
        /// The pipeline document.
        #[arg(value_name = "pipeline.yaml")]
        spec: PathBuf,

        /// Write the JSON run report here instead of stdout.
        #[arg(long, value_name = "path")]
        report: Option<PathBuf>,

        /// Also write every pipeline event as NDJSON — to a file, or
        /// to stdout with `-` (which then requires `--report`, so the
        /// two machine outputs never interleave).
        #[arg(long, value_name = "path|-")]
        events: Option<PathBuf>,
    },
    /// Parse and build a pipeline through the real gates, run nothing.
    ///
    /// Every refusal a run would hit at build time surfaces here:
    /// document typos, contradictory options, unsupported write modes.
    /// One caveat: destinations are constructed for real, so the
    /// duckdb destination creates its (empty) database file exactly as
    /// a run would.
    Validate {
        /// The pipeline document.
        #[arg(value_name = "pipeline.yaml")]
        spec: PathBuf,
    },
    /// Print a connector's configuration JSON Schema to stdout.
    Schema {
        /// Which connector's document to describe: a short name (rest,
        /// oracle, file, postgres, duckdb, iceberg, snowflake — the
        /// same table the pipeline document's rich spellings resolve
        /// through), a reverse-DNS id (io.rapidbyte.file, found as
        /// rdlt-connector-file on PATH), or an explicit binary path.
        /// The named binary is spawned and asked for its schema.
        #[arg(value_name = "connector")]
        connector: String,

        /// Which half of the connector to ask. Without it the
        /// connector is probed source-first (a dual-role connector
        /// answers with its source schema).
        #[arg(long, value_enum)]
        role: Option<SchemaRole>,
    },
}

/// The two halves a spawned connector can serve its schema as —
/// `schema --role`'s vocabulary, mapped onto the runtime's `Role` at
/// the dispatch site.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum SchemaRole {
    Source,
    Destination,
}

/// The color ladder. `console` already honours NO_COLOR under
/// `auto`; the explicit forms override both it and the TTY check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ColorChoice {
    Auto,
    Always,
    Never,
}

/// The verbosity ladder the renderers read, resolved from the flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Verbosity {
    /// Errors and the report only.
    Quiet,
    /// The lifecycle feed (streams, batches, commits, discards).
    Normal,
    /// Plus read/commit-start/part lines.
    Verbose,
}

impl Cli {
    pub fn verbosity(&self) -> Verbosity {
        if self.quiet {
            Verbosity::Quiet
        } else if self.verbose > 0 {
            Verbosity::Verbose
        } else {
            Verbosity::Normal
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The pre-clap spelling parses unchanged — the compatibility
    /// contract's argument half.
    #[test]
    fn the_frozen_run_spelling_parses() {
        let cli = Cli::try_parse_from(["rdlt", "run", "p.yaml"]).expect("parses");
        let Command::Run { spec, report, .. } = cli.command else {
            panic!("run parses as Run");
        };
        assert_eq!(spec, PathBuf::from("p.yaml"));
        assert_eq!(report, None);

        let cli =
            Cli::try_parse_from(["rdlt", "run", "p.yaml", "--report", "r.json"]).expect("parses");
        let Command::Run { report, .. } = cli.command else {
            panic!("run parses as Run");
        };
        assert_eq!(report, Some(PathBuf::from("r.json")));
    }

    /// Quiet beats verbose; counting works.
    #[test]
    fn the_verbosity_ladder_resolves() {
        let quiet = Cli::try_parse_from(["rdlt", "-q", "-v", "run", "p.yaml"]).expect("parses");
        assert_eq!(quiet.verbosity(), Verbosity::Quiet);
        let loud = Cli::try_parse_from(["rdlt", "-v", "run", "p.yaml"]).expect("parses");
        assert_eq!(loud.verbosity(), Verbosity::Verbose);
        let normal = Cli::try_parse_from(["rdlt", "run", "p.yaml"]).expect("parses");
        assert_eq!(normal.verbosity(), Verbosity::Normal);
    }
}
