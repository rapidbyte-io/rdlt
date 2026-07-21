//! # rdlt-connector-rest — declarative REST source
//!
//! YAML-configured pagination, auth, and incremental cursors. Depends on the SPI
//! only. Error classification is the source's whole retry story: 429 → `RateLimited`
//! (Retry-After honored), 5xx/network → `Transient`, other 4xx → `Fatal`; the ENGINE
//! retries (contract clause S3 — no retry loops here).

pub mod config;

use async_trait::async_trait;
use bytes::Bytes;
use rdlt_connector::{
    ConnectorSpec, Cursor, ReadRequest, Source, SourceError, StreamSpec, core::StreamName,
};
use serde_json::Value;

pub use config::{Auth, Pagination, RestConfig, RestStream};

#[derive(Debug)]
pub struct RestSource {
    config: RestConfig,
    client: reqwest::Client,
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
        Self {
            config,
            client: reqwest::Client::new(),
        }
    }

    fn stream_config(&self, name: &StreamName) -> Option<&RestStream> {
        self.config.streams.iter().find(|s| s.name == name.as_str())
    }

    fn apply_auth(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.config.auth {
            Auth::None => request,
            Auth::Bearer { token } => request.bearer_auth(token),
            Auth::Header { name, value } => request.header(name, value),
            Auth::Basic { username, password } => request.basic_auth(username, Some(password)),
        }
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
                if let Some(field) = &stream.cursor_field {
                    spec = spec.with_cursor_field(field.clone());
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

        let url = format!(
            "{}/{}",
            self.config.base_url.trim_end_matches('/'),
            stream.path.trim_start_matches('/')
        );
        let since_value: Option<String> = req
            .since
            .as_ref()
            .and_then(|c| cursor_to_param(c.as_value()));
        // Max observed cursor value; starts at the resume point so an empty page can
        // never move the cursor backwards.
        let mut max_cursor: Option<String> = since_value.clone();

        let mut page = match &stream.pagination {
            Pagination::Page { start, .. } => *start,
            _ => 0,
        };
        let mut offset: u64 = 0;

        loop {
            let mut request = self.client.get(&url);
            for (k, v) in &stream.params {
                request = request.query(&[(k, v)]);
            }
            if let (Some(param), Some(value)) = (&stream.cursor_param, &since_value) {
                request = request.query(&[(param, value)]);
            }
            match &stream.pagination {
                Pagination::None => {}
                Pagination::Page { page_param, .. } => {
                    request = request.query(&[(page_param, &page.to_string())]);
                }
                Pagination::Offset {
                    offset_param,
                    limit_param,
                    page_size,
                } => {
                    request = request
                        .query(&[(offset_param, &offset.to_string())])
                        .query(&[(limit_param, &page_size.to_string())]);
                }
            }
            let response = self
                .apply_auth(request)
                .send()
                .await
                .map_err(classify_reqwest)?;
            let status = response.status();
            if !status.is_success() {
                return Err(classify_status(status, &response));
            }
            let body = response.bytes().await.map_err(classify_reqwest)?;

            let (records, count) = extract_records(&body, stream)?;
            if count == 0 {
                break;
            }

            // Update the cursor from this page BEFORE pushing, so the checkpoint that
            // follows the rows covers exactly them (clause S2).
            if let Some(field) = &stream.cursor_field {
                update_max_cursor(&records, field, &mut max_cursor);
            }
            if req.out.raw_json(records).await.is_err() {
                return Ok(()); // clause S4: closed channel = cancellation
            }
            if stream.cursor_field.is_some()
                && let Some(max) = &max_cursor
                && req.out.checkpoint(Cursor::new(max.clone())).await.is_err()
            {
                return Ok(());
            }

            match &stream.pagination {
                Pagination::None => break,
                Pagination::Page { .. } => page += 1,
                Pagination::Offset { page_size, .. } => {
                    if count < *page_size as usize {
                        break; // short page = last page
                    }
                    offset += *page_size;
                }
            }
        }
        Ok(())
    }
}

/// Extract the records array as bytes plus a count. Without `records_path` the body
/// streams through untouched (the perf path); with it, one parse + reserialize.
fn extract_records(body: &Bytes, stream: &RestStream) -> Result<(Bytes, usize), SourceError> {
    match &stream.records_path {
        None => {
            let value: Value = serde_json::from_slice(body)
                .map_err(|e| SourceError::fatal(format!("response is not valid JSON: {e}")))?;
            match value {
                Value::Array(items) => Ok((body.clone(), items.len())),
                _ => Err(SourceError::fatal(
                    "response body is not a JSON array; set records_path",
                )),
            }
        }
        Some(path) => {
            let value: Value = serde_json::from_slice(body)
                .map_err(|e| SourceError::fatal(format!("response is not valid JSON: {e}")))?;
            let mut node = &value;
            for part in path.split('.') {
                node = node.get(part).ok_or_else(|| {
                    SourceError::fatal(format!("records_path `{path}` missing in response"))
                })?;
            }
            match node {
                Value::Array(items) => {
                    let bytes =
                        serde_json::to_vec(node).map_err(|e| SourceError::fatal(e.to_string()))?;
                    Ok((bytes.into(), items.len()))
                }
                _ => Err(SourceError::fatal(format!(
                    "records_path `{path}` is not an array"
                ))),
            }
        }
    }
}

fn update_max_cursor(records: &Bytes, field: &str, max: &mut Option<String>) {
    let Ok(Value::Array(items)) = serde_json::from_slice::<Value>(records) else {
        return;
    };
    for item in items {
        let Some(value) = item.get(field) else {
            continue;
        };
        let rendered = match value {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            _ => continue,
        };
        // ISO-8601 timestamps and equal-width numerics order lexicographically —
        // the documented constraint on REST cursor fields.
        if max.as_ref().is_none_or(|current| rendered > *current) {
            *max = Some(rendered);
        }
    }
}

fn cursor_to_param(value: &Value) -> Option<String> {
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

fn classify_reqwest(error: reqwest::Error) -> SourceError {
    // Connection-level problems are the textbook transient failure.
    SourceError::transient(error)
}

fn classify_status(status: reqwest::StatusCode, response: &reqwest::Response) -> SourceError {
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        let retry_after = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.parse::<u64>().ok())
            .map(std::time::Duration::from_secs);
        return SourceError::RateLimited {
            retry_after,
            source: "HTTP 429 from API".into(),
        };
    }
    if status.is_server_error() {
        return SourceError::transient(format!("HTTP {status}"));
    }
    SourceError::fatal(format!("HTTP {status}"))
}

/// JSON Schema GENERATED from the config structs (feature 006, US4) — the
/// declared schema and the parser cannot drift.
pub fn config_schema() -> Value {
    serde_json::to_value(schemars::schema_for!(RestConfig)).expect("schema serializes")
}
