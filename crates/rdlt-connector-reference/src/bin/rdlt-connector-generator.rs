//! The generator source, served to a host that spawns it.

use std::process::ExitCode;

fn main() -> ExitCode {
    rdlt_connector::serve::<rdlt_connector_reference::GeneratorSource>()
}
