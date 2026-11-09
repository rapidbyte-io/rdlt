//! `rdlt`: the command line over the rdlt facade.

#![forbid(unsafe_code)]

fn main() -> std::process::ExitCode {
    rdlt_cli::main()
}
