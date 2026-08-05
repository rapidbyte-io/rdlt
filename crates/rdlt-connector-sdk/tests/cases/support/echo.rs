//! The echo connector: the smallest `SourceConnector` that still
//! exercises every path `serve::source` has to forward — a normal read
//! (N rows, one checkpoint) and an induced failure (a terminal
//! `SourceError`) — without pulling in a real system.

use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use rdlt_connector::{Cursor, SourceError, StreamSpec};
use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::source::{Feed, SourceConnector};

/// Flips true the moment a `read_stream` push observes `ControlFlow::Break`
/// — the signal a dropped response stream (or any other closed SPI
/// channel) produces. Process-global rather than per-instance: nextest
/// runs each test as its own OS process, so one test's read can never
/// share this flag with another's, and there is no `EchoSource` handle
/// left after `serve_on` moves it into the shell for the cancellation
/// test to poll instead.
static BREAK_OBSERVED: AtomicBool = AtomicBool::new(false);

/// Whether the most recent `EchoSource::read_stream` call observed
/// cancellation — polled by the `serve::source` cancellation-chain test.
pub fn break_observed() -> bool {
    BREAK_OBSERVED.load(Ordering::SeqCst)
}

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
                BREAK_OBSERVED.store(true, Ordering::SeqCst);
                return Ok(());
            }
        }
        let _ = feed
            .checkpoint(Cursor::new(serde_json::json!({"n": last})))
            .await;
        Ok(())
    }
}
