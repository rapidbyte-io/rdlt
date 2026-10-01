//! The offset-log reference source, served to a host that spawns it: the kill matrix reads a log
//! that forgets what it committed from it in a process of its own.

#![forbid(unsafe_code)]

use std::process::ExitCode;

fn main() -> ExitCode {
    rdlt_connector::serve::<rdlt_connector_reference::LogSource>()
}
