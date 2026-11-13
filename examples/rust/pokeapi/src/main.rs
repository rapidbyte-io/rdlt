#![forbid(unsafe_code)]
//! Loads [PokeAPI](https://pokeapi.co)'s pokemon and types into SQLite, each run resuming where
//! the last ended.

use pokeapi::PokeApi;
use rdlt::{Connector, Host, Pipeline, ReadMode, Side, Stream, WriteMode};
use rdlt_connector_reference::SqliteDestination;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let database = args.next().unwrap_or_else(|| "pokeapi.db".to_owned());
    // The connector's own root, PokeAPI's, unless another is given.
    let config = args
        .next()
        .map_or(rdlt::json!({}), |url| rdlt::json!({ "base_url": url }));
    // Compiled in and trusted: both connectors run in this process.
    let host = Host::new()
        .source::<PokeApi>()
        .destination::<SqliteDestination>();
    let source = Side::new(Connector::new("io.example.pokeapi")).config(config);
    let destination =
        Side::new(Connector::new("io.rapidbyte.sqlite")).config(rdlt::json!({ "path": database }));
    // Each stream read on from where the last run ended, and merged by its key, so a page read
    // again replaces its rows rather than adding to them.
    let merged = |name: &str| {
        Stream::new(name)
            .read(ReadMode::Incremental)
            .write(WriteMode::merge(["id"]))
    };
    let pipeline = Pipeline::builder("pokeapi", source, destination)
        .stream(merged("pokemon"))
        .stream(merged("types"))
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
