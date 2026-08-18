//! The echo connectors: the smallest `SourceConnector`/
//! `DestinationConnector` pair that still exercises every path
//! `serve::source`/`serve::destination` has to forward — for the
//! source, a normal read (N rows, one checkpoint) and an induced
//! failure (a terminal `SourceError`); for the destination, the full
//! session choreography plus an induced publish failure — without
//! pulling in a real system either way.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use async_trait::async_trait;
use rdlt_connector::arrow::RecordBatch;
use rdlt_connector::core::commit::{CommitMeta, CommitReceipt, WriteMode};
use rdlt_connector::core::cursor::Cursor;
use rdlt_connector::core::id::{LoadId, PipelineId, TableName};
use rdlt_connector::core::schema::TableSchema;
use rdlt_connector::core::state::StateDoc;
use rdlt_connector::destination::{Capabilities, OpenContext, PartCloseReason, PartClosed};
use rdlt_connector::error::{DestinationError, SourceError};
use rdlt_connector::source::StreamSpec;
use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::destination::{Backend, DestinationConnector};
use rdlt_connector_sdk::source::{Feed, SourceConnector};

static READ_TASK_DROPPED: AtomicBool = AtomicBool::new(false);

/// Whether the most recent `EchoSource::read_stream` task was dropped.
pub fn read_task_dropped() -> bool {
    READ_TASK_DROPPED.load(Ordering::SeqCst)
}

/// Bytes this process's `read_stream` pushes have been ADMITTED for
/// under the [`EchoConfig::push_bytes`] knob (the unsized default path
/// leaves this at zero — its rows are too small for a byte bound to
/// say anything about). Incremented only once a push RETURNS, so a
/// push parked on backpressure is deliberately not counted: that makes
/// this counter a direct readout of how much a stalled reader lets the
/// serve layer buffer, which is exactly what the frame channel's budget
/// bounds. Process-global for the same reason [`READ_TASK_DROPPED`] is —
/// the connector lives inside the served process with no handle a test
/// could hold.
static PUSHED_BYTES: AtomicU64 = AtomicU64::new(0);

/// Bytes admitted so far — see [`PUSHED_BYTES`].
pub fn pushed_bytes() -> u64 {
    PUSHED_BYTES.load(Ordering::SeqCst)
}

#[derive(Debug, serde::Deserialize)]
pub struct EchoConfig {
    pub rows: u64,
    #[serde(default)]
    pub fail_read: bool,
    /// When set, every push is one `raw_json` document of EXACTLY this
    /// many bytes instead of the tiny `{"n": i}` row — the knob the
    /// frame-channel byte-bound test needs, since a bound measured in
    /// BYTES only shows itself against frames big enough for a
    /// frame-COUNT bound to price wrongly.
    #[serde(default)]
    pub push_bytes: usize,
    #[serde(default)]
    pub park_after_first: bool,
}

#[derive(Debug, thiserror::Error)]
pub enum EchoError {
    #[error("echo yaml: {0}")]
    Yaml(#[from] serde_yaml_ng::Error),
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
    push_bytes: usize,
    park_after_first: bool,
}

/// One NDJSON document of exactly `bytes` bytes carrying `n` — a real
/// row padded out, so a frame under test is both well-formed and
/// precisely sized. Under the padding's own minimum length the document
/// is simply as short as it can be; the tests that care pass sizes far
/// above it.
fn padded_row(n: u64, bytes: usize) -> bytes::Bytes {
    let head = format!("{{\"n\":{n},\"pad\":\"");
    let tail = "\"}\n";
    let pad = bytes.saturating_sub(head.len() + tail.len());
    let mut row = String::with_capacity(head.len() + pad + tail.len());
    row.push_str(&head);
    row.extend(std::iter::repeat_n('x', pad));
    row.push_str(tail);
    bytes::Bytes::from(row.into_bytes())
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
            push_bytes: config.push_bytes,
            park_after_first: config.park_after_first,
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
        struct DropSignal;
        impl Drop for DropSignal {
            fn drop(&mut self) {
                READ_TASK_DROPPED.store(true, Ordering::SeqCst);
            }
        }
        let _drop_signal = DropSignal;
        if self.fail_read {
            return Err(SourceError::fatal("echo: induced read failure"));
        }
        let mut last = 0;
        for n in 0..self.rows {
            last = n;
            let (flow, bytes) = if self.push_bytes > 0 {
                let row = padded_row(n, self.push_bytes);
                let bytes = row.len() as u64;
                (feed.raw_json(row).await, bytes)
            } else {
                (feed.rows([serde_json::json!({"n": n})]).await, 0)
            };
            if flow.is_break() {
                return Ok(());
            }
            PUSHED_BYTES.fetch_add(bytes, Ordering::SeqCst);
            if self.park_after_first {
                std::future::pending::<()>().await;
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
/// reason `READ_TASK_DROPPED` is above: the `EchoDestination` a wire test
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
    /// of `None`, so a wire test can drive the
    /// `ExistingReceipt` → `Some` → `Replay` leg (never reached by the
    /// `None` → `Publish` leg the default choreography test exercises)
    /// and assert `replay` shows in the call log WITHOUT `publish`.
    #[serde(default)]
    pub receipt_exists: bool,
    /// Makes the FIRST `connect` call fail transient — every call after
    /// that (on the SAME `EchoDestination` instance, i.e. the same
    /// served process) succeeds normally, so a wire test drives Open
    /// (→ ErrorFrame TRANSIENT), then Open again on the SAME stream,
    /// and asserts the retry
    /// succeeds — proving a failed connect does not poison the guard.
    #[serde(default)]
    pub fail_connect: bool,
    /// Makes `validate()` fail — the destination-role twin of
    /// `EchoConfig::rows == 0` on the source side, and the handshake
    /// refusal matrix's "config failing validate" row: none of the knobs
    /// above fail validation (they only change post-handshake `Backend`
    /// behavior), so this field exists solely to give that row something
    /// to fail on.
    #[serde(default)]
    pub invalid: bool,
    /// Makes `write` PANIC — a connector defect, the shape the serve
    /// layer's panic belt must contain: `close` still runs and the
    /// client sees a typed error, never a silently dead stream.
    #[serde(default)]
    pub panic_on_write: bool,
}

impl Document for EchoDestinationConfig {
    type Error = EchoError;
    fn validate(&self) -> Result<(), Self::Error> {
        if self.invalid {
            return Err(EchoError::Invalid(
                "destination config marked invalid".to_string(),
            ));
        }
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
    panic_on_write: bool,
    /// `Some(true)` induces ONE transient `connect` failure, consumed
    /// (flipped to `Some(false)`) the first time `connect` runs — see
    /// [`EchoDestinationConfig::fail_connect`]. Interior mutability
    /// because `connect` takes `&self`, not `&mut self` (the SPI's own
    /// shape — a connector is shared across however many sessions a
    /// process serves).
    fail_connect_once: AtomicBool,
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
            panic_on_write: config.panic_on_write,
            fail_connect_once: AtomicBool::new(config.fail_connect),
        })
    }

    fn capabilities(&self) -> Capabilities {
        // Non-default on purpose: a `capabilities_json` round-trip test
        // that happened to compare against an all-`false` default would
        // pass even if the wire plumbing silently dropped the payload.
        Capabilities::default().with_merge(true).with_structs(true)
    }

    async fn connect(&self, context: &OpenContext) -> Result<EchoBackend, DestinationError> {
        // `swap(false, ..)` both reads whether a failure was still owed
        // AND consumes it in one atomic step — a retried `connect` after
        // firing this once always succeeds.
        if self.fail_connect_once.swap(false, Ordering::SeqCst) {
            return Err(DestinationError::transient("echo: induced connect failure"));
        }
        Ok(EchoBackend {
            log: call_log(),
            fail_publish: self.fail_publish,
            receipt_exists: self.receipt_exists,
            panic_on_write: self.panic_on_write,
            part_events: context.part_events.clone(),
        })
    }
}

pub struct EchoBackend {
    log: Arc<Mutex<Vec<String>>>,
    fail_publish: bool,
    receipt_exists: bool,
    panic_on_write: bool,
    part_events: Option<rdlt_connector::destination::PartEventFn>,
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
        assert!(!self.panic_on_write, "echo: induced write panic");
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
