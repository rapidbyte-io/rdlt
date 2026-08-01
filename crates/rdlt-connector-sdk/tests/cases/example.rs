//! The example connector: a complete source + destination authored on
//! the framework, in memory.
//!
//! This is the SDK's proof discipline, not a toy: the conformance kits
//! certify these against the SAME clauses every shipping connector
//! answers to, so "the framework satisfies the SPI contract" is a test
//! result rather than a claim. It is also the reference an author reads
//! — everything a framework connector must write is here, and nothing
//! else is.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rdlt_connector::core::{
    CommitMeta, CommitReceipt, Cursor, LoadId, PipelineId, StateDoc, TableName, TableSchema,
    WriteMode,
};
use rdlt_connector::{
    DestinationCapabilities, DestinationError, OpenContext, RecordBatch, SourceError, StreamSpec,
};
use rdlt_connector_sdk::config::Document;
use rdlt_connector_sdk::destination::{Backend, DestinationConnector};
use rdlt_connector_sdk::source::{Feed, SourceConnector};

// ---- shared config document ------------------------------------------------

#[derive(Debug, serde::Deserialize)]
pub struct TickerConfig {
    /// How many ticks the stream carries.
    pub count: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum TickerError {
    #[error("invalid ticker YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("invalid ticker JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid ticker config: {0}")]
    Invalid(String),
}

impl Document for TickerConfig {
    type Error = TickerError;
    fn validate(&self) -> Result<(), Self::Error> {
        if self.count < 1 {
            return Err(TickerError::Invalid(format!(
                "`count` is {} — at least one tick is required",
                self.count
            )));
        }
        Ok(())
    }
}

// ---- the source ------------------------------------------------------------

/// Ticks 1..=count on one stream, checkpointing after every row —
/// deterministic and resumable, which is exactly what the source kit
/// certifies (S1: full read == covered-by(c) ++ read(since = c)).
pub struct Ticker {
    count: i64,
}

#[async_trait]
impl SourceConnector for Ticker {
    const NAME: &'static str = "ticker";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    type Config = TickerConfig;

    fn assemble(config: TickerConfig) -> Result<Self, TickerError> {
        Ok(Self {
            count: config.count,
        })
    }

    async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
        Ok(vec![StreamSpec::new("ticks").with_cursor_field("n")])
    }

    async fn read_stream(
        &self,
        stream: &StreamSpec,
        since: Option<Cursor>,
        feed: &mut Feed,
    ) -> Result<(), SourceError> {
        if stream.name.as_str() != "ticks" {
            return Err(SourceError::fatal(format!(
                "unknown stream {}",
                stream.name
            )));
        }
        // Resume strictly after the committed cursor; a cursor this
        // source never produced is a harness defect, not a
        // start-from-zero.
        let resume_after = match &since {
            None => 0,
            Some(cursor) => cursor
                .as_value()
                .as_i64()
                .ok_or_else(|| SourceError::fatal("unknown resume cursor: not a tick number"))?,
        };
        for n in (resume_after + 1)..=self.count {
            if feed.rows([serde_json::json!({"n": n})]).await.is_break() {
                return Ok(());
            }
            if feed
                .checkpoint(Cursor::new(serde_json::json!(n)))
                .await
                .is_break()
            {
                return Ok(());
            }
        }
        Ok(())
    }
}

// ---- the destination -------------------------------------------------------

/// The shared "warehouse": staged rows invisible until publish,
/// committed rows visible, receipts keyed by `(load_id, commit_seq)`,
/// one state slot — all under one mutex so publish is atomic with
/// state, and shared across sessions so a re-opened session sees the
/// same storage (which is what lets the kit's crashed-session and
/// re-commit clauses mean anything).
#[derive(Debug, Default)]
pub struct Store {
    staged: Vec<(TableName, RecordBatch)>,
    committed: BTreeMap<TableName, Vec<RecordBatch>>,
    receipts: BTreeMap<(String, u64), CommitReceipt>,
    state: Option<StateDoc>,
}

pub type SharedStore = Arc<Mutex<Store>>;

/// Rows a reader would see in `table` — the kit's `TableProbe` answers
/// from here.
pub fn visible_rows(store: &SharedStore, table: &str) -> u64 {
    let store = store.lock().expect("store lock");
    store
        .committed
        .get(&TableName::new(table))
        .map(|batches| batches.iter().map(|b| b.num_rows() as u64).sum())
        .unwrap_or(0)
}

pub struct Sink {
    store: SharedStore,
}

impl Sink {
    pub fn with_store(store: SharedStore) -> Self {
        Self { store }
    }
}

pub struct SinkBackend {
    store: SharedStore,
}

#[async_trait]
impl DestinationConnector for Sink {
    const NAME: &'static str = "sink";
    const VERSION: &'static str = env!("CARGO_PKG_VERSION");
    type Config = TickerConfig;
    type Backend = SinkBackend;

    fn assemble(_config: TickerConfig) -> Result<Self, TickerError> {
        Ok(Self {
            store: SharedStore::default(),
        })
    }

    fn capabilities(&self) -> DestinationCapabilities {
        // Deliberately merge-less: the example certifies the append
        // choreography; merge semantics are a backend concern the
        // framework does not choreograph.
        DestinationCapabilities::default()
            .with_structs(true)
            .with_scalar_lists(true)
            .with_json_type(true)
            .with_decimal(true)
    }

    async fn connect(&self, _context: &OpenContext) -> Result<SinkBackend, DestinationError> {
        // The open contract: a crashed predecessor's staging becomes
        // invisible and reclaimable — here, dropped.
        self.store.lock().expect("store lock").staged.clear();
        Ok(SinkBackend {
            store: Arc::clone(&self.store),
        })
    }
}

#[async_trait]
impl Backend for SinkBackend {
    async fn ensure_table(
        &mut self,
        _schema: &TableSchema,
        _mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        Ok(())
    }

    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        self.store
            .lock()
            .expect("store lock")
            .staged
            .push((table.clone(), batch));
        Ok(())
    }

    async fn existing_receipt(
        &mut self,
        load_id: &LoadId,
        commit_seq: u64,
    ) -> Result<Option<CommitReceipt>, DestinationError> {
        Ok(self
            .store
            .lock()
            .expect("store lock")
            .receipts
            .get(&(load_id.as_str().to_owned(), commit_seq))
            .cloned())
    }

    async fn publish(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        let mut store = self.store.lock().expect("store lock");
        let staged = std::mem::take(&mut store.staged);
        for (table, batch) in staged {
            store.committed.entry(table).or_default().push(batch);
        }
        store.state = Some(meta.state.clone());
        let receipt = CommitReceipt {
            load_id: meta.load_id.clone(),
            commit_seq: meta.commit_seq,
        };
        store.receipts.insert(
            (meta.load_id.as_str().to_owned(), meta.commit_seq),
            receipt.clone(),
        );
        Ok(receipt)
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        Ok(self
            .store
            .lock()
            .expect("store lock")
            .state
            .clone()
            .filter(|state| &state.pipeline == pipeline))
    }
}
