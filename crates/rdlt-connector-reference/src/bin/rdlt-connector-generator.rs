//! The generator source, served to a host that spawns it: a test connector, built only with the
//! `test-connectors` feature.

#![forbid(unsafe_code)]

use std::process::ExitCode;

fn main() -> ExitCode {
    rdlt_connector::serve::<rdlt_connector_reference::GeneratorSource>()
}
