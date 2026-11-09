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

/// Whether the command line, which did not parse, names `--output json`: its failure is then
/// JSON on standard output too.
fn asked_for_json() -> bool {
    let args: Vec<std::ffi::OsString> = std::env::args_os().collect();
    args.iter().any(|arg| arg == "--output=json")
        || args
            .windows(2)
            .any(|pair| pair[0] == "--output" && pair[1] == "json")
}

/// Runs the command line on the process's arguments, and answers the code it exits with.
pub fn main() -> std::process::ExitCode {
    certify::panics::contain();
    let cli = match <args::Cli as clap::Parser>::try_parse() {
        Ok(cli) => cli,
        Err(error) => {
            if error.use_stderr() && asked_for_json() {
                let said = error.render().to_string();
                let lines: Vec<&str> = said
                    .lines()
                    .map(str::trim)
                    .filter(|line| !line.is_empty())
                    .collect();
                let cut = lines.iter().position(|line| line.starts_with("Usage:"));
                let said = lines[..cut.unwrap_or(lines.len())].join(" ");
                let message = said.strip_prefix("error: ").unwrap_or(&said);
                output::failed_at_start(output::Mode::Json, &output::Failure::usage(message));
                return Exit::Usage.into();
            }
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
