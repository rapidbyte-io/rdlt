//! The change reference source, served to a host that spawns it: the engine's integration suite
//! reads change streams, their phases included, from it in a process of its own.

use std::process::ExitCode;

fn main() -> ExitCode {
    rdlt_connector::serve::<rdlt_connector_reference::ChangesSource>()
}
