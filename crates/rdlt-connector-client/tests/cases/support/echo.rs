//! This crate's own echo pair: the smallest sdk connectors a client
//! test can dial. Written fresh for THIS crate (no cross-crate
//! test-support imports — the sdk's echo file is idiom, not substrate):
//! [`EchoSource`] answers the handshake/streams/read shapes, and
//! [`EchoDestination`]'s `Backend` logs every call so a wire test can
//! pin what the client actually drove.
//!
//! The destination's two config knobs exist for the client's session
//! tests (Task 4 consumes them over the wire):
//!
//! - `fail_publish` induces a transient `publish` failure;
//! - `replay_seq: Some(n)` makes `existing_receipt` answer a receipt at
//!   commit_seq `n` (instead of `None`), so a test can drive the D3
//!   `ExistingReceipt` → `Some` → `Replay` leg.

use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use rdlt_connector::core::{
    CommitMeta, CommitReceipt, LoadId, PipelineId, StateDoc, TableName, TableSchema, WriteMode,
};
use rdlt_connector::{
    Cursor, DestinationCapabilities, DestinationError, OpenContext, RecordBatch, SourceError,
    StreamSpec,
};
use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::destination::{Backend, DestinationConnector};
use rdlt_connector_sdk::source::{Feed, SourceConnector};

/// The one config error both echo halves report through. `Display` is
/// what a refused handshake carries verbatim over the wire — the
/// client-side test pins `echo: rows must be > 0` against it.
#[derive(Debug, thiserror::Error)]
pub enum EchoError {
    #[error("echo yaml: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("echo json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("echo: {0}")]
    Refused(String),
}

#[derive(Debug, serde::Deserialize)]
pub struct EchoSourceConfig {
    pub rows: u64,
    #[serde(default)]
    pub fail_read: bool,
}

impl Document for EchoSourceConfig {
    type Error = EchoError;
    fn validate(&self) -> Result<(), Self::Error> {
        if self.rows == 0 {
            return Err(EchoError::Refused("rows must be > 0".to_string()));
        }
        Ok(())
    }
}

/// One stream (`numbers`) of `rows` objects `{"n": i}` plus a single
/// trailing checkpoint — or, with `fail_read`, no rows and a fatal
/// error, so the wire carries exactly one of the two shapes.
pub struct EchoSource {
    rows: u64,
    fail_read: bool,
}

#[async_trait]
impl SourceConnector for EchoSource {
    const NAME: &'static str = "echo-source";
    const VERSION: &'static str = "0.0.0";
    type Config = EchoSourceConfig;

    fn assemble(config: EchoSourceConfig) -> Result<Self, EchoError> {
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
        for n in 0..self.rows {
            if feed.rows([serde_json::json!({"n": n})]).await.is_break() {
                return Ok(());
            }
        }
        let _ = feed
            .checkpoint(Cursor::new(serde_json::json!({"n": self.rows - 1})))
            .await;
        Ok(())
    }
}

/// The call log every [`EchoBackend`] in this process appends to.
/// Process-global of necessity: a served destination builds its
/// connector from the handshake's config bytes, so the driving test
/// holds no other handle to observe calls through (nextest's
/// process-per-test keeps logs from crossing tests).
static CALLS: OnceLock<Arc<Mutex<Vec<String>>>> = OnceLock::new();

fn calls() -> Arc<Mutex<Vec<String>>> {
    CALLS
        .get_or_init(|| Arc::new(Mutex::new(Vec::new())))
        .clone()
}

/// The calls logged so far, in arrival order.
#[allow(dead_code)] // Task 4's session tests read this; Task 2's handshake tests do not.
pub fn calls_snapshot() -> Vec<String> {
    calls().lock().expect("call log lock").clone()
}

/// Empty the log — a test that reads [`calls_snapshot`] clears first.
#[allow(dead_code)] // Task 4's session tests use this; Task 2's handshake tests do not.
pub fn clear_calls() {
    calls().lock().expect("call log lock").clear();
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct EchoDestinationConfig {
    /// Induces a transient `publish` failure.
    #[serde(default)]
    pub fail_publish: bool,
    /// `Some(n)`: `existing_receipt` answers a receipt pinned at
    /// commit_seq `n` instead of `None` — the knob Task 4's
    /// D3-over-the-wire test drives the Replay leg with.
    #[serde(default)]
    pub replay_seq: Option<u64>,
}

impl Document for EchoDestinationConfig {
    type Error = EchoError;
    fn validate(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// The smallest destination whose every `Backend` call is observable:
/// each one logs its name, and the two config knobs above steer
/// `publish`/`existing_receipt`.
pub struct EchoDestination {
    fail_publish: bool,
    replay_seq: Option<u64>,
}

#[async_trait]
impl DestinationConnector for EchoDestination {
    const NAME: &'static str = "echo-destination";
    const VERSION: &'static str = "0.0.0";
    type Config = EchoDestinationConfig;
    type Backend = EchoBackend;

    fn assemble(config: EchoDestinationConfig) -> Result<Self, EchoError> {
        Ok(Self {
            fail_publish: config.fail_publish,
            replay_seq: config.replay_seq,
        })
    }

    fn capabilities(&self) -> DestinationCapabilities {
        // Deliberately non-default: a handshake test comparing against
        // an all-false `Default` could pass with the capabilities
        // payload silently dropped on the wire.
        DestinationCapabilities::default()
            .with_merge(true)
            .with_structs(true)
    }

    async fn connect(&self, _context: &OpenContext) -> Result<EchoBackend, DestinationError> {
        Ok(EchoBackend {
            log: calls(),
            fail_publish: self.fail_publish,
            replay_seq: self.replay_seq,
        })
    }
}

pub struct EchoBackend {
    log: Arc<Mutex<Vec<String>>>,
    fail_publish: bool,
    replay_seq: Option<u64>,
}

impl EchoBackend {
    fn log(&self, call: &str) {
        self.log
            .lock()
            .expect("call log lock")
            .push(call.to_string());
    }
}

#[async_trait]
impl Backend for EchoBackend {
    async fn ensure_table(
        &mut self,
        _schema: &TableSchema,
        _mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        self.log("ensure_table");
        Ok(())
    }

    async fn write(
        &mut self,
        _table: &TableName,
        _batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        self.log("write");
        Ok(())
    }

    async fn existing_receipt(
        &mut self,
        load_id: &LoadId,
        _commit_seq: u64,
    ) -> Result<Option<CommitReceipt>, DestinationError> {
        self.log("existing_receipt");
        Ok(self.replay_seq.map(|commit_seq| CommitReceipt {
            load_id: load_id.clone(),
            commit_seq,
        }))
    }

    async fn replay(
        &mut self,
        _meta: &CommitMeta,
        _receipt: &CommitReceipt,
    ) -> Result<(), DestinationError> {
        self.log("replay");
        Ok(())
    }

    async fn publish(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        self.log("publish");
        if self.fail_publish {
            return Err(DestinationError::transient("echo: induced publish failure"));
        }
        Ok(CommitReceipt {
            load_id: meta.load_id,
            commit_seq: meta.commit_seq,
        })
    }

    async fn read_state(
        &mut self,
        _pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        self.log("read_state");
        Ok(None)
    }

    async fn close(&mut self) -> Result<(), DestinationError> {
        self.log("close");
        Ok(())
    }
}
