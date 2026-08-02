//! The source connector: config in, streams out.

use async_trait::async_trait;
use rdlt_connector_sdk::source::{Feed, SourceConnector};
use rdlt_connector_sdk::spi::core::{Cursor, StreamName, crash_point};
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

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(self
            .config
            .streams
            .iter()
            .map(|stream| {
                let mut spec = StreamSpec::new(stream.name.as_str());
                if let Some(key) = &stream.primary_key {
                    spec = spec.with_primary_key(key.iter().cloned());
                }
                spec
            })
            .collect())
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
        crash_point!(
            "ora.query",
            Err(SourceError::fatal("injected crash at ora.query"))
        );
        // A fresh connection per stream: the boundary's poison rule
        // means a connection that errored is gone, and a stream that
        // starts clean can retry independently.
        let client = Client::connect(&self.config).await?;
        read_stream(client, config, &self.config.tuning, &mut cursor, feed).await?;
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
