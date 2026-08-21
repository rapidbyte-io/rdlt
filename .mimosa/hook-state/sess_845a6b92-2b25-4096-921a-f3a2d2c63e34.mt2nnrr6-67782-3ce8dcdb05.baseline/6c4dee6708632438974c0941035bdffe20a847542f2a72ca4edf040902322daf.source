//! The argument surface, as data. Only flags that DO something are
//! declared: a flag whose feature has not landed yet would be
//! accepted-and-ignored, the defect class this workspace refuses.

use std::path::PathBuf;

use clap::{ArgAction, Parser, Subcommand};

/// rdlt — move data with exactly-once commits.
#[derive(Debug, Parser)]
#[command(name = "rdlt", version, about, disable_help_subcommand = true)]
pub(crate) struct Cli {
    /// Quieter: suppress the per-event feed; errors and the report
    /// still appear. Overrides -v.
    #[arg(short, long, global = true)]
    pub(crate) quiet: bool,

    /// Louder: -v adds read/commit/part detail lines.
    #[arg(short, long, global = true, action = ArgAction::Count)]
    pub(crate) verbose: u8,

    /// Never draw the live progress display; log a line per event
    /// instead (the automatic behaviour when stderr is not a
    /// terminal).
    #[arg(long, global = true)]
    pub(crate) no_progress: bool,

    /// When to color output. `auto` follows the terminal and NO_COLOR.
    #[arg(long, global = true, value_enum, default_value_t = ColorChoice::Auto)]
    pub(crate) color: ColorChoice,

    /// Machine/plain output. `auto` follows the terminal; `plain` logs a
    /// line per event even on a terminal; `json` silences the feed and
    /// prints the report JSON to stdout even on a terminal.
    #[arg(long, global = true, value_enum, default_value_t = Output::Auto)]
    pub(crate) output: Output,

    #[command(subcommand)]
    pub(crate) command: Command,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Command {
    /// Run a pipeline to completion (resumable; exactly-once).
    Run {
        /// The pipeline document.
        #[arg(value_name = "pipeline.yaml")]
        spec: PathBuf,

        /// Write the JSON run report here instead of stdout.
        ///
        /// The report includes each stream's final cursor; cursor
        /// values are source-defined and may embed sensitive resume
        /// material, so treat the file like the state store itself.
        #[arg(long, value_name = "path")]
        report: Option<PathBuf>,

        /// Also write every pipeline event as NDJSON — to a file, or
        /// to stdout with `-` (which then requires `--report`, so the
        /// two machine outputs never interleave).
        ///
        /// `committed` events carry each commit's cursors; cursor
        /// values are source-defined and may embed sensitive resume
        /// material, so treat the event log like the state store
        /// itself.
        #[arg(long, value_name = "path|-")]
        events: Option<PathBuf>,
    },
    /// Connectivity, discovery and plan checks, without running.
    ///
    /// The build gates run for real (spawn + handshake, so every
    /// refusal a run would hit at build time surfaces here: document
    /// typos, contradictory options, a config the connector's own gate
    /// refuses), then both connectors' reachability probes, stream
    /// discovery, and the run's plan validation. The engine creates
    /// nothing — no workdir, no WAL, no load session. One caveat: what
    /// a connector's config gate does during the handshake is that
    /// connector's behavior — a destination that materializes its
    /// target at construction (the duckdb destination creates its
    /// empty database file) does so here exactly as a run would.
    Check {
        /// The pipeline document.
        #[arg(value_name = "pipeline.yaml")]
        spec: PathBuf,
    },
    /// Print a connector's configuration JSON Schema to stdout.
    Schema {
        /// Which connector's document to describe: the FULL reverse-DNS
        /// connector id (io.rapidbyte.reference, found as
        /// rdlt-connector-reference on PATH by its last segment; a
        /// shorthand discovers the binary and is then refused as an
        /// identity mismatch — the same rule a document's `id` follows)
        /// or an explicit binary path. The named binary is spawned and
        /// asked for its schema.
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
pub(crate) enum SchemaRole {
    Source,
    Destination,
}

/// The color ladder. `console` already honours NO_COLOR under
/// `auto`; the explicit forms override both it and the TTY check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum ColorChoice {
    Auto,
    Always,
    Never,
}

/// The output ladder. `auto` lets the terminal decide between the live
/// display and a line per event; the explicit forms are for scripts
/// and CI, where the terminal's answer must not matter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Output {
    Auto,
    Plain,
    Json,
}

/// The verbosity ladder the renderers read, resolved from the flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Verbosity {
    /// Errors and the report only.
    Quiet,
    /// The lifecycle feed (streams, batches, commits, discards).
    Normal,
    /// Plus read/commit-start/part lines.
    Verbose,
}

impl Cli {
    pub(crate) fn verbosity(&self) -> Verbosity {
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

    /// `--output` defaults to `auto`, takes its three values, and is
    /// global — accepted after the subcommand too.
    #[test]
    fn the_output_flag_parses() {
        let auto = Cli::try_parse_from(["rdlt", "run", "p.yaml"]).expect("parses");
        assert_eq!(auto.output, Output::Auto);
        let json =
            Cli::try_parse_from(["rdlt", "--output", "json", "run", "p.yaml"]).expect("parses");
        assert_eq!(json.output, Output::Json);
        let plain =
            Cli::try_parse_from(["rdlt", "run", "p.yaml", "--output", "plain"]).expect("parses");
        assert_eq!(plain.output, Output::Plain);
        assert!(Cli::try_parse_from(["rdlt", "--output", "yaml", "run", "p.yaml"]).is_err());
    }
}
