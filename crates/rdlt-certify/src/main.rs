//! `rdlt-certify`: certifies a connector binary, or a connector listening at an endpoint, against
//! the protocol's conformance clauses (§20.8).

#![forbid(unsafe_code)]

mod cli;

use std::process::ExitCode;

fn main() -> ExitCode {
    cli::main()
}
