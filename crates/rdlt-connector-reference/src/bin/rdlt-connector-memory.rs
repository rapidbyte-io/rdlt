//! The memory source and destination, served to a host that spawns them: test connectors, built
//! only with the `test-connectors` feature.

#![forbid(unsafe_code)]

use std::process::ExitCode;

use rdlt_connector::serve::Served;
use rdlt_connector::{destination_factory, source_factory};
use rdlt_connector_reference::{MemoryDestination, MemorySource};

fn main() -> ExitCode {
    Served::new()
        .with_source(source_factory::<MemorySource>())
        .with_destination(destination_factory::<MemoryDestination>())
        .serve()
}
