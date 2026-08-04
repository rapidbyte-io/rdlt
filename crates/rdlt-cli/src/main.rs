//! # rdlt — the pipeline CLI
//!
//! `rdlt run <pipeline.yaml> [--report <path>]`
//!
//! ONE YAML document describes the whole pipeline: pipeline-wide settings,
//! the source (inline, or `config: path` to a reusable document), and the
//! destination — one file, one format, end to end. The document model and its
//! construction into a pipeline are the shared [`rdlt::pipeline_spec`]; this
//! binary parses the file, renders the event feed, and emits the report.
//!
//! Everything the CLI does, the library does — the CLI adds zero engine
//! capability. Events stream to stderr (human-readable); the
//! `RunReport` JSON goes to stdout or `--report`.
//!
//! Exit codes mirror `RdltError` variants (stable, scriptable):
//! 0 success · 2 config · 3 schema contract · 4 source · 5 destination · 6 WAL/disk ·
//! 7 cancelled · 64 usage · 70 internal defect · 74 file I/O.
//!
//! 70 is also the code for any variant this build does not know: `RdltError` is
//! `#[non_exhaustive]`, and "this binary cannot classify what happened" is a bug
//! to report, never an instruction to go and edit the pipeline configuration.

mod args;
mod cdc;
mod run;
mod schema;
mod ui;

use std::process::ExitCode;

use clap::Parser as _;
use rdlt::prelude::*;

/// Bound glibc's allocator retention: data movement churns
/// large short-lived buffers (slabs, arenas, arrow builds), and glibc retains
/// them as RSS long after free.
///
/// What each call actually does, measured as a 2x2 factorial (7 interleaved
/// runs per arm, two cells, quiet machine):
///
/// - `M_TRIM_THRESHOLD` is set to 128 KiB, which IS glibc's default — so the
///   value changes nothing. The call's real effect is its documented side
///   effect: it DISABLES glibc's dynamic growth of the mmap/trim thresholds.
///   Without it, glibc raises those thresholds as it sees large frees, so more
///   big allocations come from the retained heap. That is the knob doing the
///   memory work: dropping it costs +29% peak RSS on a 1M-row relational copy
///   and +32% on the nested-JSONL cell.
/// - `M_ARENA_MAX = 2` bounds per-thread arena growth. Its effect is small and
///   NOT consistent: it improved both wall and RSS on the relational copy and
///   cost ~4% wall on the JSONL cell, buying no RSS there.
///
/// Wall-clock effects of the pair are within a few percent in BOTH directions
/// depending on the cell, so neither "free" nor "costly" is an honest summary;
/// the memory reduction is the reason it is here, and it is large.
///
/// CLI-only: library embedders own their allocator policy. The workspace denies
/// unsafe; this single libc FFI call (no pointers, no invariants — two integer
/// knobs) is the deliberate exception.
#[allow(unsafe_code)]
fn bound_allocator_retention() {
    #[cfg(target_env = "gnu")]
    // SAFETY: mallopt takes two ints, touches no memory we own, and is called
    // before any pipeline threads exist.
    unsafe {
        libc::mallopt(libc::M_ARENA_MAX, 2);
        libc::mallopt(libc::M_TRIM_THRESHOLD, 128 * 1024);
    }
}

fn main() -> ExitCode {
    bound_allocator_retention();

    let cli = match args::Cli::try_parse() {
        Ok(cli) => cli,
        Err(e) => {
            // clap renders its own message (help/version exit 0; a bad
            // invocation exits 64, the historical usage code).
            let is_help = matches!(
                e.kind(),
                clap::error::ErrorKind::DisplayHelp | clap::error::ErrorKind::DisplayVersion
            );
            let _ = e.print();
            return if is_help {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(64)
            };
        }
    };

    let runtime = match tokio::runtime::Runtime::new() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: starting runtime: {e}");
            return ExitCode::from(2);
        }
    };
    match cli.color {
        args::ColorChoice::Auto => {}
        args::ColorChoice::Always => console::set_colors_enabled_stderr(true),
        args::ColorChoice::Never => console::set_colors_enabled_stderr(false),
    }
    let renderer = ui::select(
        cli.quiet,
        cli.verbose > 0,
        cli.no_progress,
        console::Term::stderr().is_term(),
    );
    let verbosity = cli.verbosity();
    let outcome = match cli.command {
        args::Command::Run {
            spec,
            report,
            events,
        } => runtime.block_on(run::run(spec, report, events, verbosity, renderer)),
        args::Command::Validate { spec } => runtime.block_on(run::validate(spec, verbosity)),
        args::Command::Schema { connector } => schema::print(connector),
    };
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => e.render_and_exit_code(),
    }
}

/// The CLI's error taxonomy, and the STABLE exit-code contract:
/// 0 success · 2 config · 3 schema contract · 4 source · 5 destination ·
/// 6 WAL/disk · 7 cancelled · 64 usage · 70 internal defect · 74 file I/O.
pub(crate) enum CliError {
    Usage(String),
    /// A file the CLI itself could not read or write. Distinct from `Usage`:
    /// the invocation was well-formed, the filesystem refused.
    Io(String),
    Run(RdltError),
}

impl CliError {
    fn render_and_exit_code(self) -> ExitCode {
        match self {
            CliError::Io(message) => {
                eprintln!("error: {message}");
                ExitCode::from(74) // EX_IOERR: the invocation was fine, the filesystem was not
            }
            CliError::Usage(message) => {
                eprintln!("error: {message}");
                ExitCode::from(2)
            }
            CliError::Run(error) => {
                eprintln!("error: {error}");
                ExitCode::from(exit_code_for(&error))
            }
        }
    }
}

/// The `RdltError` half of the contract, alone so a pin can hold it.
pub(crate) fn exit_code_for(error: &RdltError) -> u8 {
    match error {
        RdltError::Config { .. } => 2,
        RdltError::Schema(_) => 3,
        RdltError::Source { .. } => 4,
        RdltError::Destination { .. } => 5,
        RdltError::Wal { .. } => 6,
        RdltError::Cancelled => 7,
        RdltError::Internal { .. } => 70,
        // NOT 2. Falling back to the config code tells a scripting
        // caller to fix their YAML for something the engine could not
        // classify, and a future variant would silently join it.
        _ => 70,
    }
}

impl From<RdltError> for CliError {
    fn from(e: RdltError) -> Self {
        CliError::Run(e)
    }
}

impl From<rdlt::pipeline_spec::SpecError> for CliError {
    fn from(e: rdlt::pipeline_spec::SpecError) -> Self {
        match e {
            // A spec-resolution problem is a config error (exit 2), the same
            // taxonomy the loud parse/IO paths use.
            rdlt::pipeline_spec::SpecError::Resolve(message) => CliError::Usage(message),
            // The builder's own typed error keeps its exit-code mapping.
            rdlt::pipeline_spec::SpecError::Build(error) => CliError::Run(error),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The exit-code contract, pinned variant by variant — scripts
    /// dispatch on these numbers.
    #[test]
    fn the_exit_code_contract_holds() {
        assert_eq!(exit_code_for(&RdltError::config("x")), 2);
        assert_eq!(exit_code_for(&RdltError::Cancelled), 7);
        assert_eq!(
            exit_code_for(&RdltError::source(rdlt::prelude::StreamName::new("s"), "x")),
            4
        );
    }
}
