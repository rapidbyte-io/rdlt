//! The echo connector: the smallest `SourceConnector` that still
//! exercises every path `serve::source` has to forward — a normal read
//! (N rows, one checkpoint) and an induced failure (a terminal
//! `SourceError`) — without pulling in a real system.

use async_trait::async_trait;
use rdlt_connector::{Cursor, SourceError, StreamSpec};
use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::source::{Feed, SourceConnector};

#[derive(Debug, serde::Deserialize)]
pub struct EchoConfig {
    pub rows: u64,
    #[serde(default)]
    pub fail_read: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum EchoError {
    #[error("echo yaml: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("echo json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("echo: {0}")]
    Invalid(String),
}

impl Document for EchoConfig {
    type Error = EchoError;
    fn validate(&self) -> Result<(), Self::Error> {
        if self.rows == 0 {
            return Err(EchoError::Invalid("rows must be > 0".to_string()));
        }
        Ok(())
    }
}

/// Pushes `rows` numbered objects `{"n": i}` (`i` from `0`) one at a
/// time on its one stream `numbers`, then a single checkpoint `{"n":
/// last}` — or, with `fail_read` set, pushes nothing and returns a
/// fatal error instead, so a caller can tell the two shapes apart on
/// the wire without a partial read muddying the count.
pub struct EchoSource {
    rows: u64,
    fail_read: bool,
}

#[async_trait]
impl SourceConnector for EchoSource {
    const NAME: &'static str = "echo-source";
    const VERSION: &'static str = "0.0.0";
    type Config = EchoConfig;

    fn assemble(config: EchoConfig) -> Result<Self, EchoError> {
        Ok(Self {
            rows: config.rows,
            fail_read: config.fail_read,
        })
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(vec![StreamSpec::new("numbers")])
    }

    async fn read_stream(
        &self,
        _stream: &StreamSpec,
        _since: Option<Cursor>,
        feed: &mut Feed,
    ) -> Result<(), SourceError> {
        if self.fail_read {
            return Err(SourceError::fatal("echo: induced read failure"));
        }
        let mut last = 0;
        for n in 0..self.rows {
            last = n;
            if feed.rows([serde_json::json!({"n": n})]).await.is_break() {
                return Ok(());
            }
        }
        let _ = feed
            .checkpoint(Cursor::new(serde_json::json!({"n": last})))
            .await;
        Ok(())
    }
}
