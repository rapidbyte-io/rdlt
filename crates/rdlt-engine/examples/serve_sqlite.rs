//! The SQLite reference destination, served to a host that spawns it: the engine's integration
//! suite runs against it in a process of its own.

use std::process::ExitCode;

fn main() -> ExitCode {
    rdlt_connector::serve::<rdlt_connector_reference::SqliteDestination>()
}
