//! This crate's own echo pair: the smallest sdk connectors a client
//! test can dial. Written fresh for THIS crate (no cross-crate
//! test-support imports — the sdk's echo file is idiom, not substrate):
//! [`EchoSource`] answers the handshake/streams/read shapes, and
//! [`EchoDestination`]'s `Backend` logs every call so a wire test can
//! pin what the client actually drove.
//!
//! The source's failure knobs exist for the client's read-seam tests,
//! which consume them over the wire: `fail_read` induces a fatal
//! read failure, `fail_check` a transient check failure — each carrying
//! a pinned `echo:`-prefixed cause.
//!
//! The destination's config knobs exist for the client's session
//! tests, which consume them over the wire:
//!
//! - `fail_publish` induces a transient `publish` failure;
//! - `replay_seq: Some(_)` makes `existing_receipt` answer `Some`
//!   (with the REQUESTED identity — the client refuses a receipt
//!   naming any other), so a test can drive the choreography's
//!   `ExistingReceipt` → `Some` → `Replay` leg;
//! - `fail_connect` induces a transient `connect` failure, so a test
//!   can drive the Open frame's `ErrorFrame` reply;
//! - `emit_parts: n` makes `publish` report `n` closed parts through
//!   the session's `OpenContext::part_events` listener BEFORE
//!   returning — the knob the part-event interleaving test drives.

use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use rdlt_connector::arrow::RecordBatch;
use rdlt_connector::core::commit::{CommitMeta, CommitReceipt, WriteMode};
use rdlt_connector::core::cursor::Cursor;
use rdlt_connector::core::id::{LoadId, PipelineId, TableName};
use rdlt_connector::core::schema::TableSchema;
use rdlt_connector::core::state::StateDoc;
use rdlt_connector::destination::{
    Capabilities, OpenContext, PartCloseReason, PartClosed, PartEventFn,
};
use rdlt_connector::error::{DestinationError, SourceError};
use rdlt_connector::source::StreamSpec;
use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::destination::{Backend, DestinationConnector};
use rdlt_connector_sdk::source::{Feed, SourceConnector};

/// The one config error both echo halves report through. The serving
/// boundary redacts any config value repeated in its `Display` text.
#[derive(Debug, thiserror::Error)]
pub enum EchoError {
    #[error("echo yaml: {0}")]
    Yaml(#[from] serde_yaml_ng::Error),
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
    /// Induces a transient `check` failure — the knob the client's
    /// failing-check round-trip drives.
    #[serde(default)]
    pub fail_check: bool,
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
    fail_check: bool,
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
            fail_check: config.fail_check,
        })
    }

    async fn check(&self) -> Result<(), SourceError> {
        if self.fail_check {
            return Err(SourceError::transient("echo: induced check failure"));
        }
        Ok(())
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
pub fn calls_snapshot() -> Vec<String> {
    calls().lock().expect("call log lock").clone()
}

/// Empty the log — a test that reads [`calls_snapshot`] clears first.
pub fn clear_calls() {
    calls().lock().expect("call log lock").clear();
}

#[derive(Debug, Default, serde::Deserialize)]
pub struct EchoDestinationConfig {
    /// Induces a transient `publish` failure.
    #[serde(default)]
    pub fail_publish: bool,
    /// `Some(_)`: `existing_receipt` answers `Some` instead of `None`
    /// — with the requested identity, the only shape the client
    /// accepts. The knob the replay-leg tests drive.
    #[serde(default)]
    pub replay_seq: Option<u64>,
    /// Induces a transient `connect` failure — the Open frame's
    /// `ErrorFrame` reply, seen from the client as a failed `open`.
    #[serde(default)]
    pub fail_connect: bool,
    /// How many closed parts `publish` reports through the session's
    /// part-event listener before returning its receipt.
    #[serde(default)]
    pub emit_parts: u64,
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
    fail_connect: bool,
    emit_parts: u64,
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
            fail_connect: config.fail_connect,
            emit_parts: config.emit_parts,
        })
    }

    fn capabilities(&self) -> Capabilities {
        // Deliberately non-default: a handshake test comparing against
        // an all-false `Default` could pass with the capabilities
        // payload silently dropped on the wire.
        Capabilities::default().with_merge(true).with_structs(true)
    }

    async fn connect(&self, context: &OpenContext) -> Result<EchoBackend, DestinationError> {
        if self.fail_connect {
            return Err(DestinationError::transient("echo: induced connect failure"));
        }
        Ok(EchoBackend {
            log: calls(),
            fail_publish: self.fail_publish,
            replay_seq: self.replay_seq,
            emit_parts: self.emit_parts,
            // The listener the serving layer wired into the context —
            // held so `publish` can report parts the way a real
            // file-writing backend would.
            part_events: context.part_events.clone(),
        })
    }
}

pub struct EchoBackend {
    log: Arc<Mutex<Vec<String>>>,
    fail_publish: bool,
    replay_seq: Option<u64>,
    emit_parts: u64,
    part_events: Option<PartEventFn>,
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
        // A CONFORMING receipt: the identity the caller asked about.
        // (The client refuses any other, and the echo exists to model
        // a well-behaved destination — the knob chooses WHETHER a
        // receipt answers, never WHOSE.)
        Ok(self.replay_seq.is_some().then(|| CommitReceipt {
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
        // Report the configured parts SYNCHRONOUSLY, inside the publish
        // call — the serving layer's ordering promise (every part queued
        // when a Backend call returns precedes that call's own reply) is
        // exactly what the client-side interleaving test pins.
        if let Some(listener) = &self.part_events {
            for index in 0..self.emit_parts {
                listener(PartClosed::new(
                    TableName::new("numbers"),
                    512 + index,
                    PartCloseReason::Commit,
                ));
            }
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
