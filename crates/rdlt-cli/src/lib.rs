//! The `rdlt` command line: write, check, run, inspect and certify pipelines and connectors, over
//! the `rdlt` facade with every feature.
//!
//! Machine output goes to standard output and human output to standard error; with
//! `--output json` every result and every error is JSON on standard output. The exit codes are
//! [`Exit::TABLE`].

#![forbid(unsafe_code)]

#[cfg(panic = "abort")]
compile_error!("rdlt-cli contains a panic at its boundary, so it needs a build that unwinds");

mod args;
mod certify;
mod connector;
mod exit;
mod host;
mod output;
mod reference;
mod stopping;
mod verbs;

pub use exit::Exit;
pub use reference::reference;

/// Runs the command line on the process's arguments, and answers the code it exits with.
pub fn main() -> std::process::ExitCode {
    let cli = match <args::Cli as clap::Parser>::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            let exit = if error.use_stderr() {
                Exit::Usage
            } else {
                Exit::Success
            };
            error.print().ok();
            return exit.into();
        }
    };
    verbs::dispatch(cli).into()
}
