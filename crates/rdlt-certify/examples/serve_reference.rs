//! The reference memory source and SQLite destination, served as one connector binary, for the
//! tests that certify a spawned or listening connector: the SQLite store outlives each process.

use std::process::ExitCode;

use rdlt_connector::serve::Served;
use rdlt_connector::{destination_factory, source_factory};
use rdlt_connector_reference::{MemorySource, SqliteDestination};

fn main() -> ExitCode {
    Served::new()
        .with_source(source_factory::<MemorySource>())
        .with_destination(destination_factory::<SqliteDestination>())
        .serve()
}
