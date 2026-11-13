#![forbid(unsafe_code)]
//! A source connector for [PokeAPI](https://pokeapi.co): its `pokemon` and `types` streams, read
//! a page at a time, each resuming after the last page a run committed.
//!
//! ```no_run
//! use pokeapi::PokeApi;
//!
//! // Compiled in and trusted, the connector runs in the host's own process.
//! let host = rdlt::Host::new().source::<PokeApi>();
//! ```

mod client;

use rdlt_connector::prelude::*;
use serde::{Deserialize, Serialize};

use crate::client::{Client, Entry, Page};

/// The configuration the connector accepts; its JSON Schema is published.
#[derive(Debug, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// The API's root.
    base_url: String,
    /// How many entries each list request asks for, from 1 to 100.
    #[schemars(range(min = 1, max = 100))]
    page_size: u64,
    /// The most rows each stream reads in all its runs together, or null for every one: a
    /// stream that has read them reads nothing more.
    limit: Option<u64>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            base_url: "https://pokeapi.co/api/v2".to_owned(),
            page_size: 50,
            limit: Some(151),
        }
    }
}

/// The connector, connected: the API's client, and how much a read asks of it.
#[derive(Debug)]
pub struct PokeApi {
    client: Client,
    page_size: u64,
    limit: u64,
}

#[source(id = "io.example.pokeapi")]
impl SourceConnector for PokeApi {
    type Config = Config;

    async fn connect(config: Config, _context: &ConnectContext) -> Result<Self> {
        if !(1..=100).contains(&config.page_size) {
            return Err(ConnectorError::config("page_size is not from 1 to 100"));
        }
        Ok(Self {
            client: Client::new(&config.base_url)?,
            page_size: config.page_size,
            limit: config.limit.unwrap_or(u64::MAX),
        })
    }

    async fn check(&self) -> Result<()> {
        self.client.page("pokemon", 0, 1).await.map(drop)
    }

    fn streams(&self) -> Streams<Self> {
        Streams::new().with(Pokemon).with(Types)
    }
}

/// Where a stream resumes: the list offset its next page starts at.
#[derive(Debug, Default, Serialize, Deserialize)]
struct Position {
    offset: u64,
}

/// `pokemon`: each entry of `/pokemon`, read in full from `/pokemon/{id}`.
struct Pokemon;

/// A pokemon as it lands: its own fields, and its types, abilities and stats as nested JSON.
#[derive(Deserialize, Serialize)]
struct Detail {
    id: u64,
    name: String,
    height: u64,
    weight: u64,
    base_experience: Option<u64>,
    types: serde_json::Value,
    abilities: serde_json::Value,
    stats: serde_json::Value,
}

impl ReadStream<PokeApi> for Pokemon {
    type Cursor = Position;

    fn spec(&self) -> StreamSpec {
        keyed("pokemon")
    }

    async fn read(
        &self,
        source: &PokeApi,
        _partition: &Partition,
        cursor: Position,
        out: &mut Emitter<Position>,
    ) -> Result<()> {
        let mut pages = Pages::new(source, "pokemon", cursor);
        while let Some(entries) = pages.next().await? {
            let mut details = Vec::with_capacity(entries.len());
            for entry in &entries {
                let path = format!("pokemon/{}", entry.id()?);
                details.push(source.client.get::<Detail>(&path).await?);
            }
            out.rows(&details).await?;
            out.checkpoint(&pages.after).await?;
        }
        Ok(())
    }
}

/// `types`: each entry of `/type`, its id read from its URL.
struct Types;

/// A type as it lands.
#[derive(Serialize)]
struct Type<'a> {
    id: u64,
    name: &'a str,
}

impl ReadStream<PokeApi> for Types {
    type Cursor = Position;

    fn spec(&self) -> StreamSpec {
        keyed("types")
    }

    async fn read(
        &self,
        source: &PokeApi,
        _partition: &Partition,
        cursor: Position,
        out: &mut Emitter<Position>,
    ) -> Result<()> {
        let mut pages = Pages::new(source, "type", cursor);
        while let Some(entries) = pages.next().await? {
            let types = entries.iter().map(|entry| {
                let name = &entry.name;
                Ok(Type {
                    id: entry.id()?,
                    name,
                })
            });
            out.rows(&types.collect::<Result<Vec<_>>>()?).await?;
            out.checkpoint(&pages.after).await?;
        }
        Ok(())
    }
}

/// A stream keyed by `id` that reads in full or from the last committed page on.
fn keyed(name: &str) -> StreamSpec {
    StreamSpec::new(StreamName::new(name).expect("a stream's name is valid"))
        .with_primary_key(["id"])
        .with_read_modes([ReadMode::Full, ReadMode::Incremental])
}

/// The pages of a list from a cursor on, up to the configured limit.
struct Pages<'a> {
    source: &'a PokeApi,
    resource: &'static str,
    /// Where the page after those read starts.
    after: Position,
    last: bool,
}

impl<'a> Pages<'a> {
    fn new(source: &'a PokeApi, resource: &'static str, after: Position) -> Self {
        Self {
            source,
            resource,
            after,
            last: false,
        }
    }

    /// The next page's entries, or none once the list or the limit ends.
    async fn next(&mut self) -> Result<Option<Vec<Entry>>> {
        let left = self.source.limit.saturating_sub(self.after.offset);
        if self.last || left == 0 {
            return Ok(None);
        }
        let asked = left.min(self.source.page_size);
        let client = &self.source.client;
        let Page { next, results } = client.page(self.resource, self.after.offset, asked).await?;
        self.last = next.is_none() || results.is_empty();
        let read = u64::try_from(results.len()).internal("counting a page's entries")?;
        self.after.offset += read;
        Ok((!results.is_empty()).then_some(results))
    }
}
