//! The files reference source and destination, served to a host that spawns them: the engine's
//! integration suite runs against them in a process of their own.

use std::process::ExitCode;

use rdlt_connector::serve::Served;
use rdlt_connector::{destination_factory, source_factory};
use rdlt_connector_reference::{FilesDestination, FilesSource};

fn main() -> ExitCode {
    Served::new()
        .with_source(source_factory::<FilesSource>())
        .with_destination(destination_factory::<FilesDestination>())
        .serve()
}
