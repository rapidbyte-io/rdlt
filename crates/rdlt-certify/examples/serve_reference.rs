//! The reference memory source and SQLite destination, served as one connector binary, for the
//! tests that certify a spawned or listening connector: the SQLite store outlives each process,
//! and the destination reads back what it published.

#![forbid(unsafe_code)]

use std::process::ExitCode;

use rdlt_connector::serve::Served;
use rdlt_connector::{readable_destination_factory, source_factory};
use rdlt_connector_reference::{MemorySource, SqliteDestination};

fn main() -> ExitCode {
    Served::new()
        .with_source(source_factory::<MemorySource>())
        .with_destination(readable_destination_factory::<SqliteDestination>())
        .serve()
}
