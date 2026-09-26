//! The SQLite destination, served to a host that spawns it.

#![forbid(unsafe_code)]

use std::process::ExitCode;

fn main() -> ExitCode {
    rdlt_connector::serve::<rdlt_connector_reference::SqliteDestination>()
}
