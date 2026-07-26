//! # Declarative REST source
//!
//! YAML-configured pagination (7 families), auth (incl. OAuth2
//! client-credentials), JSONPath-subset record selection, response actions,
//! incremental cursors, and parent-child endpoint composition. Depends on
//! the SPI only. Error classification is the source's whole retry story:
//! 429 → `RateLimited` (Retry-After honored, bounded), 5xx/network →
//! `Transient`, other 4xx → `Fatal` unless a DECLARED response action says
//! otherwise; the ENGINE retries — there are no free retry loops here.
//!
//! Module layout (family convention): [`config`] the document, [`client`]
//! HTTP execution (auth/classify/pacing), [`read`] the paginated read loop.

pub mod client;
pub mod config;
pub mod read;

use async_trait::async_trait;
use rdlt_connector::{
    ConnectorSpec, ReadRequest, Source, SourceError, StreamSpec, core::StreamName,
};
use serde_json::Value;

pub use client::RestClient;
pub use config::{Auth, Pagination, RestConfig, RestStream};
pub use read::paginate::{PageContext, PageDecision, Paginator, PaginatorError};

/// Fail-point registry: the read path's protocol boundaries.
#[cfg(feature = "failpoints")]
#[doc(hidden)]
pub const FAIL_POINTS: &[&str] = &["rest.request", "rest.decode", "rest.checkpoint"];

#[derive(Debug)]
pub struct RestSource {
    config: RestConfig,
    client: RestClient,
}

impl RestSource {
    pub fn from_yaml(yaml: &str) -> Result<Self, config::ConfigError> {
        Self::build(RestConfig::from_yaml(yaml)?)
    }

    pub fn from_json(json: &str) -> Result<Self, config::ConfigError> {
        Self::build(RestConfig::from_json(json)?)
    }

    /// Embedder entry point (see [`RestConfig::from_value`]).
    pub fn from_value(value: serde_json::Value) -> Result<Self, config::ConfigError> {
        Self::build(RestConfig::from_value(value)?)
    }

    /// Construct from an already-parsed [`RestConfig`], validating it first —
    /// the canonical entry when a caller holds a config value rather than
    /// serialized text. Validation makes the read path's invariants real (the
    /// selector parses, `max_concurrency >= 1`, parent-stream membership), so
    /// nothing downstream has to re-check or paper over them.
    pub fn new(config: RestConfig) -> Result<Self, config::ConfigError> {
        config.validate()?;
        Self::build(config)
    }

    /// Wire up the HTTP client for a config already known valid (the `from_*`
    /// text constructors validate as they parse; [`Self::new`] validates too).
    fn build(config: RestConfig) -> Result<Self, config::ConfigError> {
        // ONE client for the whole source, so the deadline covers the token
        // fetch as well as the data requests. Its construction is fallible —
        // `Client::new()` PANICS when the TLS backend cannot initialise, which
        // is an environment problem an embedder should receive as an error.
        // `read_timeout`, NOT `timeout`. The bound exists to catch a server that
        // accepts a connection and then stalls, and `read_timeout` resets after
        // each successful read — so it bounds every wait without capping a
        // transfer that is making continuous progress. A total deadline would
        // kill a large page mid-download, and since that failure is transient the
        // engine would restart and hit the same wall on every attempt.
        let http = reqwest::Client::builder()
            .read_timeout(std::time::Duration::from_secs(config.request_timeout_secs))
            .build()
            .map_err(|e| config::ConfigError::Invalid(format!("building the HTTP client: {e}")))?;
        let client = RestClient::new(
            client::AuthProvider::new(config.auth.clone(), http.clone()),
            http,
            config
                .headers
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            config
                .params
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
            config.min_request_interval_ms,
            config.retry_after_cap_secs,
        );
        Ok(Self { config, client })
    }

    fn stream_config(&self, name: &StreamName) -> Option<&RestStream> {
        self.config.streams.iter().find(|s| s.name == name.as_str())
    }
}

#[async_trait]
impl Source for RestSource {
    fn spec(&self) -> ConnectorSpec {
        let mut spec = ConnectorSpec::new("rest", env!("CARGO_PKG_VERSION"));
        spec.config_schema = Some(config_schema());
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
                if let Some(inc) = stream.effective_incremental() {
                    spec = spec.with_cursor_field(inc.cursor_field);
                }
                for (column, hint) in &stream.type_hints {
                    spec = spec.with_type_hint(column.clone(), (*hint).into());
                }
                spec
            })
            .collect())
    }

    async fn read(&self, mut req: ReadRequest) -> Result<(), SourceError> {
        let stream = self
            .stream_config(&req.stream.name)
            .ok_or_else(|| SourceError::fatal(format!("unknown stream {}", req.stream.name)))?;
        read::read_stream(
            &self.config,
            &self.client,
            stream,
            req.since.as_ref(),
            &mut req.out,
        )
        .await
    }
}

/// JSON Schema GENERATED from the config structs — the declared schema and the
/// parser cannot drift.
pub fn config_schema() -> Value {
    serde_json::to_value(schemars::schema_for!(RestConfig)).expect("schema serializes")
}
