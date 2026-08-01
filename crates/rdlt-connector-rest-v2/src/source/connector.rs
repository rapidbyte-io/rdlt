//! The SPI face: [`Rest`] implements `Source` over a validated [`Config`]
//! and one shared [`http::Client`].

use async_trait::async_trait;
use rdlt_connector::{
    ConnectorSpec, ReadRequest, Source, SourceError, StreamSpec, core::StreamName,
};

use super::config::{self, Config, Stream};
use super::http::{Client, Credentials};
use super::read;

/// The declarative REST source: one validated config document, one HTTP
/// client whose read deadline covers every request the source makes.
#[derive(Debug)]
pub struct Rest {
    config: Config,
    client: Client,
}

impl Rest {
    pub fn from_yaml(yaml: &str) -> Result<Self, config::ConfigError> {
        Self::assemble(Config::from_yaml(yaml)?)
    }

    pub fn from_json(json: &str) -> Result<Self, config::ConfigError> {
        Self::assemble(Config::from_json(json)?)
    }

    /// Embedder entry point (see [`Config::from_value`]).
    pub fn from_value(value: serde_json::Value) -> Result<Self, config::ConfigError> {
        Self::assemble(Config::from_value(value)?)
    }

    /// Construct from an already-parsed [`Config`], validating it first —
    /// the canonical entry when a caller holds a config value rather than
    /// serialized text. Validation makes the read path's invariants real
    /// (selectors parse, `max_concurrency >= 1`, parent-stream
    /// membership), so nothing downstream has to re-check or paper over
    /// them.
    pub fn new(config: Config) -> Result<Self, config::ConfigError> {
        config.validate()?;
        Self::assemble(config)
    }

    /// Wire up the HTTP client for a config already known valid (the
    /// `from_*` text constructors validate as they parse; [`Self::new`]
    /// validates too).
    fn assemble(config: Config) -> Result<Self, config::ConfigError> {
        // ONE reqwest client for the whole source, so the deadline covers
        // the token fetch as well as the data requests. Its construction is
        // fallible — `Client::new()` PANICS when the TLS backend cannot
        // initialise, which is an environment problem an embedder should
        // receive as an error.
        //
        // `read_timeout`, NOT `timeout`. The bound exists to catch a server
        // that accepts a connection and then stalls, and `read_timeout`
        // resets after each successful read — so it bounds every wait
        // without capping a transfer that is making continuous progress. A
        // total deadline would kill a large page mid-download, and since
        // that failure is transient the engine would restart and hit the
        // same wall on every attempt.
        let http = reqwest::Client::builder()
            .read_timeout(std::time::Duration::from_secs(config.request_timeout_secs))
            .build()
            .map_err(|e| config::ConfigError::Invalid(format!("building the HTTP client: {e}")))?;
        let client = Client::new(
            Credentials::new(config.auth.clone(), http.clone()),
            http,
            config
                .headers
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            config
                .params
                .iter()
                .map(|(name, value)| (name.clone(), value.clone()))
                .collect(),
            config.min_request_interval_ms,
            config.retry_after_cap_secs,
        );
        Ok(Self { config, client })
    }

    fn stream_config(&self, name: &StreamName) -> Option<&Stream> {
        self.config.streams.iter().find(|s| s.name == name.as_str())
    }
}

#[async_trait]
impl Source for Rest {
    fn spec(&self) -> ConnectorSpec {
        let mut spec = ConnectorSpec::new("rest", env!("CARGO_PKG_VERSION"));
        spec.config_schema = Some(config::config_schema());
        spec
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
                if let Some(incremental) = stream.effective_incremental() {
                    spec = spec.with_cursor_field(incremental.cursor_field);
                }
                for (column, hint) in &stream.type_hints {
                    spec = spec.with_type_hint(column.clone(), (*hint).into());
                }
                spec
            })
            .collect())
    }

    async fn read(&self, mut request: ReadRequest) -> Result<(), SourceError> {
        let stream = self
            .stream_config(&request.stream.name)
            .ok_or_else(|| SourceError::fatal(format!("unknown stream {}", request.stream.name)))?;
        read::deliver(
            &self.config,
            &self.client,
            stream,
            request.since.as_ref(),
            &mut request.out,
        )
        .await
    }
}
