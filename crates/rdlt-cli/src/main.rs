//! # rdlt — the pipeline CLI
//!
//! `rdlt run <pipeline.yaml> [--report <path>] [--events <path|->]`,
//! `rdlt validate <pipeline.yaml>`, `rdlt schema <connector> [--role ..]`.
//! ONE YAML document describes the whole pipeline; its model and its
//! construction into a pipeline are the shared [`rdlt::pipeline_spec`],
//! so this binary adds zero engine capability — it parses arguments,
//! renders the event feed on stderr, and emits the run report's JSON on
//! stdout (or to `--report`). Every number it shows the library computed.
//!
//! Exit codes, stable and scriptable: 0 success · 2 config · 3 schema
//! contract · 4 source · 5 destination · 6 WAL/disk · 7 cancelled ·
//! 64 usage · 70 internal/unknown · 74 file I/O; `--help`/`--version`
//! exit 0. The table lives once in [`exit`]; the whole
//! stdout/stderr/exit contract is pinned against the built binary by
//! `tests/cases/test_contract.rs` and the gated `test_spawned.rs`.
//!
//! The modules are the map: [`args`] the flags as data, [`command`] the
//! three subcommands, [`render`] the stderr renderers and their mode
//! ladder, [`events`] the `--events` NDJSON sink, [`exit`] the error
//! taxonomy and its codes, [`alloc`] the allocator knob.

mod alloc;
mod args;
mod command;
mod events;
mod exit;
mod render;

use std::process::ExitCode;

use clap::Parser as _;

fn main() -> ExitCode {
    alloc::bound_retention();

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
            render::stderr::line(&format!("error: starting runtime: {e}"));
            return ExitCode::from(2);
        }
    };
    match cli.color {
        args::ColorChoice::Auto => {}
        args::ColorChoice::Always => console::set_colors_enabled_stderr(true),
        args::ColorChoice::Never => console::set_colors_enabled_stderr(false),
    }
    let mode = render::mode::select(
        cli.quiet,
        cli.verbose > 0,
        cli.no_progress,
        cli.output,
        console::Term::stderr().is_term(),
    );
    let verbosity = cli.verbosity();
    let outcome = match cli.command {
        args::Command::Run {
            spec,
            report,
            events,
        } => runtime.block_on(command::run::run(
            spec,
            report,
            events,
            console::Term::stdout().is_term(),
            verbosity,
            mode,
            cli.output,
        )),
        args::Command::Validate { spec } => {
            runtime.block_on(command::validate::validate(spec, verbosity))
        }
        args::Command::Schema { connector, role } => {
            runtime.block_on(command::schema::print(&connector, role))
        }
    };
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => e.exit(),
    }
}
