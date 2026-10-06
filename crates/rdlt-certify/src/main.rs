//! `rdlt-certify`: certifies a connector binary, or a connector listening at an endpoint, against
//! the protocol's conformance clauses.

#![forbid(unsafe_code)]

mod cli;

use std::process::ExitCode;

fn main() -> ExitCode {
    cli::main()
}
