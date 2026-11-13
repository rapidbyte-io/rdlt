#![forbid(unsafe_code)]
//! rdlt's quick start: seeded offset logs loaded into SQLite, exactly once.

use rdlt::{Connector, Host, Pipeline, ReadMode, Side, Stream};
use rdlt_connector_reference::{LogSource, SqliteDestination};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "demo.db".to_owned());
    // Compiled in and trusted: both connectors run in this process.
    let host = Host::new()
        .source::<LogSource>()
        .destination::<SqliteDestination>();
    // Two partitions of 500 seeded messages each, read up to their heads.
    let source = Side::new(Connector::new("io.rapidbyte.log")).config(rdlt::json!({
        "seed": 7,
        "streams": [{ "name": "events", "partitions": 2, "messages": 500, "bounded": true }]
    }));
    let destination =
        Side::new(Connector::new("io.rapidbyte.sqlite")).config(rdlt::json!({ "path": database }));
    // One stream, read incrementally: each run starts where the last committed one ended.
    let pipeline = Pipeline::builder("events-demo", source, destination)
        .stream(Stream::new("events").read(ReadMode::Incremental))
        .build(&host)
        .await?;
    let outcome = pipeline.start().await;
    if let Some(error) = outcome.error {
        return Err(error.into());
    }
    println!(
        "{:?}: committed {} rows",
        outcome.report.status, outcome.report.rows
    );
    Ok(())
}
