//! The reference destination: jsonl parts plus commit receipts, in ONE
//! output directory.
//!
//! Staging is in-memory (a crashed session's staging simply vanishes —
//! the open contract by construction); publish writes each table's
//! staged rows to `<table>-<load_id>-<part>.jsonl`, persists the state
//! document, and appends a receipt line LAST. The part number IS the
//! commit sequence, deliberately: a crash after the parts but before
//! the receipt leaves no receipt, so the retried commit re-publishes —
//! and deterministic names make that re-publish overwrite its own
//! files instead of duplicating them.

use std::collections::BTreeMap;
use std::io::Write as _;
use std::path::PathBuf;

use async_trait::async_trait;
use rdlt_connector_sdk::config::{self, Document};
use rdlt_connector_sdk::destination::{Backend, DestinationConnector};
use rdlt_connector_sdk::spi::core::{
    CommitMeta, CommitReceipt, LoadId, PipelineId, StateDoc, TableName, TableSchema, WriteMode,
};
use rdlt_connector_sdk::spi::{
    DestinationCapabilities, DestinationError, OpenContext, RecordBatch,
};

/// The append-only receipt log: one json line per published commit,
/// `{"load_id":<string>,"commit_seq":<u64>}` — what `existing_receipt`
/// answers the sdk's replay choreography from.
const RECEIPTS_FILE: &str = "_reference_receipts.json";

/// The latest committed state document, written atomically (write to a
/// temporary, then rename) by every publish, BEFORE its receipt.
const STATE_FILE: &str = "_reference_state.json";

/// The reference destination document: ONE output directory.
/// `{ "path": "out/dir" }`
#[derive(Debug, serde::Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Config {
    /// The directory the parts, receipts, and state land in; created at
    /// the first connect.
    pub path: String,
}

/// The destination's configuration error — parser framings plus the
/// config gate's own refusals, every spelling owned here.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// YAML did not parse as the config document.
    #[error("invalid reference destination YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    /// JSON did not parse as the config document.
    #[error("invalid reference destination JSON: {0}")]
    Json(#[from] serde_json::Error),
    /// The document parsed but violates an invariant.
    #[error("invalid reference destination config: {0}")]
    Invalid(String),
}

impl Document for Config {
    type Error = Error;

    fn validate(&self) -> Result<(), Error> {
        if self.path.is_empty() {
            return Err(Error::Invalid(
                "`path` is empty — one output directory is required".into(),
            ));
        }
        Ok(())
    }
}

/// The connector: one directory.
#[derive(Debug)]
pub struct Reference {
    dir: PathBuf,
}

#[async_trait]
impl DestinationConnector for Reference {
    const NAME: &'static str = "io.rapidbyte.reference";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    type Config = Config;
    type Backend = Writer;

    fn assemble(config: Config) -> Result<Self, Error> {
        Ok(Self {
            dir: PathBuf::from(config.path),
        })
    }

    fn config_schema() -> Option<serde_json::Value> {
        Some(config::schema_of::<Config>())
    }

    fn capabilities(&self) -> DestinationCapabilities {
        // Truthful: arrow's json writer renders structs, lists, json
        // and decimals into the parts. Merge stays undeclared — jsonl
        // parts are append-only files with no upsert machinery.
        DestinationCapabilities::default()
            .with_structs(true)
            .with_scalar_lists(true)
            .with_json_type(true)
            .with_decimal(true)
    }

    async fn connect(&self, context: &OpenContext) -> Result<Writer, DestinationError> {
        // The open contract — a crashed predecessor's staging invisible
        // and reclaimable — holds by construction: staging lives in the
        // dead session's memory, and nothing staged ever touches disk.
        std::fs::create_dir_all(&self.dir).map_err(|error| {
            DestinationError::transient(format!(
                "reference destination: create {}: {error}",
                self.dir.display()
            ))
        })?;
        Ok(Writer {
            dir: self.dir.clone(),
            load_id: context.load_id.clone(),
            staged: Vec::new(),
        })
    }
}

/// One session's system IO: staged batches in memory, published files,
/// receipts and state on disk.
#[derive(Debug)]
pub struct Writer {
    dir: PathBuf,
    load_id: LoadId,
    staged: Vec<(TableName, RecordBatch)>,
}

#[async_trait]
impl Backend for Writer {
    async fn ensure_table(
        &mut self,
        _schema: &TableSchema,
        _mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        // A jsonl part carries its own column names on every row —
        // there is no DDL to run, and re-ensuring is trivially
        // idempotent.
        Ok(())
    }

    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        self.staged.push((table.clone(), batch));
        Ok(())
    }

    async fn existing_receipt(
        &mut self,
        load_id: &LoadId,
        commit_seq: u64,
    ) -> Result<Option<CommitReceipt>, DestinationError> {
        let path = self.dir.join(RECEIPTS_FILE);
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(DestinationError::transient(format!(
                    "reference destination: read {}: {error}",
                    path.display()
                )));
            }
        };
        for line in text.lines().filter(|line| !line.trim().is_empty()) {
            let receipt: CommitReceipt = serde_json::from_str(line).map_err(|error| {
                DestinationError::fatal(format!(
                    "reference destination: {} carries a corrupt receipt line `{line}`: {error}",
                    path.display()
                ))
            })?;
            if receipt.load_id == *load_id && receipt.commit_seq == commit_seq {
                return Ok(Some(receipt));
            }
        }
        Ok(None)
    }

    async fn replay(
        &mut self,
        _meta: &CommitMeta,
        _receipt: &CommitReceipt,
    ) -> Result<(), DestinationError> {
        // The redelivered unit was already published under this receipt;
        // dropping its staging is what keeps a LATER commit from
        // publishing it a second time.
        self.staged.clear();
        Ok(())
    }

    async fn publish(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        let mut tables: BTreeMap<TableName, Vec<RecordBatch>> = BTreeMap::new();
        for (table, batch) in self.staged.drain(..) {
            tables.entry(table).or_default().push(batch);
        }
        for (table, batches) in &tables {
            let mut encoded = Vec::new();
            let mut writer = arrow::json::LineDelimitedWriter::new(&mut encoded);
            for batch in batches {
                writer.write(batch).map_err(|error| {
                    DestinationError::fatal(format!(
                        "reference destination: encode `{table}` as jsonl: {error}"
                    ))
                })?;
            }
            writer.finish().map_err(|error| {
                DestinationError::fatal(format!(
                    "reference destination: encode `{table}` as jsonl: {error}"
                ))
            })?;
            let part = format!("{table}-{}-{}.jsonl", self.load_id, meta.commit_seq);
            self.persist(&part, &encoded)?;
        }

        // State BEFORE receipt: a crash between the two re-publishes the
        // whole commit (no receipt yet), overwriting both parts and
        // state with identical content — convergent, never duplicated.
        let state = serde_json::to_vec(&meta.state).map_err(|error| {
            DestinationError::fatal(format!(
                "reference destination: encode the state document: {error}"
            ))
        })?;
        self.persist(STATE_FILE, &state)?;

        let receipt = CommitReceipt {
            load_id: meta.load_id.clone(),
            commit_seq: meta.commit_seq,
        };
        let line = serde_json::to_string(&receipt).map_err(|error| {
            DestinationError::fatal(format!(
                "reference destination: encode the receipt: {error}"
            ))
        })?;
        let path = self.dir.join(RECEIPTS_FILE);
        let mut log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|error| {
                DestinationError::transient(format!(
                    "reference destination: open {}: {error}",
                    path.display()
                ))
            })?;
        writeln!(log, "{line}").map_err(|error| {
            DestinationError::transient(format!(
                "reference destination: append to {}: {error}",
                path.display()
            ))
        })?;
        Ok(receipt)
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        let path = self.dir.join(STATE_FILE);
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(DestinationError::transient(format!(
                    "reference destination: read {}: {error}",
                    path.display()
                )));
            }
        };
        let state: StateDoc = serde_json::from_str(&text).map_err(|error| {
            DestinationError::fatal(format!(
                "reference destination: {} carries a corrupt state document: {error}",
                path.display()
            ))
        })?;
        // ONE state slot, latest-writer-wins: the document names its
        // pipeline, so another pipeline's read answers None (fresh)
        // rather than someone else's cursors.
        Ok((state.pipeline == *pipeline).then_some(state))
    }
}

impl Writer {
    /// Write `bytes` to `name` atomically: to an underscore-prefixed
    /// temporary first (invisible to any table-prefix reader), then a
    /// same-directory rename.
    fn persist(&self, name: &str, bytes: &[u8]) -> Result<(), DestinationError> {
        let temp = self.dir.join(format!("_staged-{name}"));
        let target = self.dir.join(name);
        let framed = |verb: &str, path: &PathBuf, error: std::io::Error| {
            DestinationError::transient(format!(
                "reference destination: {verb} {}: {error}",
                path.display()
            ))
        };
        std::fs::write(&temp, bytes).map_err(|error| framed("write", &temp, error))?;
        std::fs::rename(&temp, &target).map_err(|error| framed("publish", &target, error))?;
        Ok(())
    }
}

/// The canonical face: `Shell::from_yaml(text)?` / `Shell::new(config)?`
/// is a running SPI destination in one call.
pub type Shell = rdlt_connector_sdk::destination::Shell<Reference>;
