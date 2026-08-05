//! The echo connectors: the smallest `SourceConnector`/
//! `DestinationConnector` pair that still exercises every path
//! `serve::source`/`serve::destination` has to forward — for the
//! source, a normal read (N rows, one checkpoint) and an induced
//! failure (a terminal `SourceError`); for the destination, the full
//! session choreography plus an induced publish failure — without
//! pulling in a real system either way.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use rdlt_connector::core::{
    CommitMeta, CommitReceipt, LoadId, PipelineId, StateDoc, TableName, TableSchema, WriteMode,
};
use rdlt_connector::{
    Cursor, DestinationCapabilities, DestinationError, OpenContext, PartCloseReason, PartClosed,
    RecordBatch, SourceError, StreamSpec,
};
use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::destination::{Backend, DestinationConnector};
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

/// The table name `EchoDestination` reports its one `PartClosed` event
/// against — arbitrary, but fixed so a driving test can assert on it.
pub const ECHOED_TABLE: &str = "echoed";

/// The shared call log every `EchoBackend` instance in THIS PROCESS
/// writes to — read by the `serve::destination` tests to pin the SDK
/// session choreography's exact call order. Process-global for the same
/// reason `BREAK_OBSERVED` is above: the `EchoDestination` a wire test
/// drives lives entirely inside the served connector, built fresh from
/// the handshake's config bytes, so there is no other handle a test
/// could hold onto.
static CALL_LOG: OnceLock<Arc<Mutex<Vec<String>>>> = OnceLock::new();

fn call_log() -> Arc<Mutex<Vec<String>>> {
    CALL_LOG
        .get_or_init(|| Arc::new(Mutex::new(Vec::new())))
        .clone()
}

/// The current call log, for a test's assertions. Does not clear it —
/// call [`clear_call_log`] first if a prior test in the same process
/// (nextest gives each test its own process, but this stays defensive
/// rather than relying on that alone) may have left entries behind.
pub fn call_log_snapshot() -> Vec<String> {
    call_log().lock().expect("call log lock").clone()
}

/// Reset the call log — call at the start of a test that reads it.
pub fn clear_call_log() {
    call_log().lock().expect("call log lock").clear();
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct EchoDestinationConfig {
    /// Induces `DestinationError::transient` from `publish` — the
    /// `serve::destination` failure-path test's trigger.
    #[serde(default)]
    pub fail_publish: bool,
    /// Makes `existing_receipt` answer `Some` (a fixed receipt) instead
    /// of `None` — the 038 T5 review F1 knob: a wire test can drive the
    /// `ExistingReceipt` → `Some` → `Replay` leg (never reached by the
    /// `None` → `Publish` leg the default choreography test exercises)
    /// and assert `replay` shows in the call log WITHOUT `publish`.
    #[serde(default)]
    pub receipt_exists: bool,
}

impl Document for EchoDestinationConfig {
    type Error = EchoError;
    fn validate(&self) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// The smallest `DestinationConnector` that still exercises the sdk's
/// full session choreography over the wire: every `Backend` call is
/// logged (in arrival order, to [`CALL_LOG`]), `publish` emits exactly
/// one `PartClosed` (table [`ECHOED_TABLE`], 64 bytes, reason `Commit`)
/// through the `OpenContext` callback BEFORE returning its receipt, and
/// `fail_publish` induces a transient publish failure instead.
pub struct EchoDestination {
    fail_publish: bool,
    receipt_exists: bool,
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
            receipt_exists: config.receipt_exists,
        })
    }

    fn capabilities(&self) -> DestinationCapabilities {
        // Non-default on purpose: a `capabilities_json` round-trip test
        // that happened to compare against an all-`false` default would
        // pass even if the wire plumbing silently dropped the payload.
        DestinationCapabilities::default()
            .with_merge(true)
            .with_structs(true)
    }

    async fn connect(&self, context: &OpenContext) -> Result<EchoBackend, DestinationError> {
        Ok(EchoBackend {
            log: call_log(),
            fail_publish: self.fail_publish,
            receipt_exists: self.receipt_exists,
            part_events: context.part_events.clone(),
        })
    }
}

pub struct EchoBackend {
    log: Arc<Mutex<Vec<String>>>,
    fail_publish: bool,
    receipt_exists: bool,
    part_events: Option<rdlt_connector::PartEventFn>,
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
        commit_seq: u64,
    ) -> Result<Option<CommitReceipt>, DestinationError> {
        self.log("existing_receipt");
        Ok(self.receipt_exists.then(|| CommitReceipt {
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
        if let Some(part_events) = &self.part_events {
            part_events(PartClosed::new(
                TableName::new(ECHOED_TABLE),
                64,
                PartCloseReason::Commit,
            ));
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
