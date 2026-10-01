//! The reference generator, served as a connector binary that serves the source role alone.

#![forbid(unsafe_code)]

use std::process::ExitCode;

use rdlt_connector::serve::Served;
use rdlt_connector::source_factory;
use rdlt_connector_reference::GeneratorSource;

fn main() -> ExitCode {
    Served::new()
        .with_source(source_factory::<GeneratorSource>())
        .serve()
}
