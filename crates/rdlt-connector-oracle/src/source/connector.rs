//! The source connector: config in, streams out.

use async_trait::async_trait;
use rdlt_connector_sdk::source::{Feed, SourceConnector};
use rdlt_connector_sdk::spi::core::{Cursor, StreamName};
use rdlt_connector_sdk::spi::{SourceError, StreamSpec};

use super::client::Client;
use super::config::{self, Config, Stream};
use super::cursor::OracleCursor;
use super::read::read_stream;

/// The crash points this source arms — exported so the sweep
/// iterates exactly this list. These spellings are frozen.
pub const FAIL_POINTS: &[&str] = &["ora.query", "ora.checkpoint"];

/// The Oracle source.
#[derive(Debug, Clone)]
pub struct Oracle {
    config: Config,
}

impl Oracle {
    fn stream_config(&self, name: &StreamName) -> Option<&Stream> {
        self.config.streams.iter().find(|s| s.name == name.as_str())
    }
}

#[async_trait]
impl SourceConnector for Oracle {
    const NAME: &'static str = "oracle";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");

    type Config = Config;

    fn assemble(config: Config) -> Result<Self, config::ConfigError> {
        Ok(Self { config })
    }

    fn config_schema() -> Option<serde_json::Value> {
        Some(config::config_schema())
    }

    /// A cheap connectivity probe — connect and let it go.
    async fn check(&self) -> Result<(), SourceError> {
        Client::connect(&self.config).await.map(|_| ())
    }

    /// Declare each stream — INCLUDING its column types.
    ///
    /// The rows themselves cross as raw JSON, where a decimal is a
    /// string and a BLOB is hex, so `type_hints` is the ONLY channel
    /// that tells the engine what those strings mean. Without it every
    /// `NUMBER(12,2)` landed as TEXT in the destination, every RAW as
    /// text, every DATE as text — the connector derived the exact type
    /// from the server's own describe and then threw it away.
    ///
    /// This costs one describe round trip per stream, which is why it
    /// lives in discovery rather than the read path.
    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        let mut client = Client::connect(&self.config).await?;
        let mut specs = Vec::with_capacity(self.config.streams.len());
        for stream in &self.config.streams {
            let mut spec = StreamSpec::new(stream.name.as_str());
            if let Some(key) = &stream.primary_key {
                spec = spec.with_primary_key(key.iter().cloned());
            }
            if let Some(cursor) = &stream.cursor {
                spec = spec.with_cursor_field(cursor.clone());
            }
            let (returned, described) = client
                .query(
                    &format!("describing `{}`", stream.name),
                    &format!(
                        "SELECT t.* FROM {} t WHERE 1 = 0",
                        super::client::quote_table(&stream.table)
                    ),
                    &[],
                )
                .await?;
            client = returned;
            for column in &described.columns {
                let derived = super::schema::logical_type(column).map_err(SourceError::fatal)?;
                spec = spec.with_type_hint(column.name.to_lowercase(), derived);
            }
            // The operator's own hints WIN — that is what the escape
            // hatch is for, chiefly bare `NUMBER`, which is derived
            // conservatively as text because Oracle accepts any
            // magnitude in it.
            for (column, hint) in &stream.type_hints {
                spec = spec.with_type_hint(column.to_lowercase(), (*hint).into());
            }
            specs.push(spec);
        }
        Ok(specs)
    }

    async fn read_stream(
        &self,
        stream: &StreamSpec,
        since: Option<Cursor>,
        feed: &mut Feed,
    ) -> Result<(), SourceError> {
        let Some(config) = self.stream_config(&stream.name) else {
            return Err(SourceError::fatal(format!(
                "unknown stream {}",
                stream.name
            )));
        };
        let mut cursor = OracleCursor::decode(since.as_ref())?;
        // A fresh connection per stream: the boundary's poison rule
        // means a connection that errored is gone, and a stream that
        // starts clean can retry independently.
        let client = Client::connect(&self.config).await?;
        read_stream(
            client,
            &self.config,
            config,
            &self.config.tuning,
            &mut cursor,
            feed,
        )
        .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The registry carries its frozen spellings.
    #[test]
    fn the_registry_is_the_frozen_pair() {
        assert_eq!(FAIL_POINTS, &["ora.query", "ora.checkpoint"]);
    }
}
