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
pub use read::paginate::{PageContext, PageDecision, Paginator};

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
        Ok(Self::new(RestConfig::from_yaml(yaml)?))
    }

    pub fn from_json(json: &str) -> Result<Self, config::ConfigError> {
        Ok(Self::new(RestConfig::from_json(json)?))
    }

    /// Embedder entry point (see [`RestConfig::from_value`]).
    pub fn from_value(value: serde_json::Value) -> Result<Self, config::ConfigError> {
        Ok(Self::new(RestConfig::from_value(value)?))
    }

    pub fn new(config: RestConfig) -> Self {
        let client = RestClient::new(
            client::AuthProvider::new(config.auth.clone()),
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
        Self { config, client }
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
