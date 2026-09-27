//! The files source and destination, served to a host that spawns them.

#![forbid(unsafe_code)]

use std::process::ExitCode;

use rdlt_connector::serve::Served;
use rdlt_connector::{readable_destination_factory, source_factory};
use rdlt_connector_reference::{FilesDestination, FilesSource};

fn main() -> ExitCode {
    Served::new()
        .with_source(source_factory::<FilesSource>())
        .with_destination(readable_destination_factory::<FilesDestination>())
        .serve()
}
