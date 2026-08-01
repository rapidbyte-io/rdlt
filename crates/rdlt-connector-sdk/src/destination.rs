//! The destination framework: implement [`DestinationConnector`] and a
//! [`Backend`], get the SPI — with the session choreography's
//! conformance clauses enforced by construction.
//!
//! The split follows what a decade of connector frameworks converged on:
//! the FRAMEWORK owns the protocol state machine, the connector owns the
//! system IO. Concretely, the SDK session refuses a write to a table
//! this session never ensured (the clause-E1 contract violation every
//! destination had to police itself), asks the backend for an existing
//! `(load_id, commit_seq)` receipt BEFORE publishing (the clause-D3
//! replay, so an at-least-once re-commit returns the prior receipt), and
//! reads state through the backend untouched.
//!
//! What the choreography deliberately does NOT own: atomicity and
//! durability. Staging invisibility (D1), crashed-session reclamation
//! (D4), and atomic publish-with-state (D2) are properties of the
//! backend's storage that no wrapper can add from outside — the
//! [`Backend`] contract states them, the conformance suites verify
//! them, and a transactional backend should keep its own internal
//! receipt guard as defense in depth (the framework's replay check is
//! the protocol fast path, not a substitute for a transactional one).

use async_trait::async_trait;
use rdlt_connector::core::{
    CommitMeta, CommitReceipt, LoadId, PipelineId, StateDoc, TableName, TableSchema, WriteMode,
};
use rdlt_connector::{
    ConnectorSpec, Destination, DestinationCapabilities, DestinationError, LoadSession,
    OpenContext, RecordBatch,
};

use crate::config::Document;

/// A destination authored on the framework.
#[async_trait]
pub trait DestinationConnector: Send + Sync + 'static {
    /// The connector's stable identifier (`postgres`, `iceberg`).
    const NAME: &'static str;
    /// The connector's own version — spell it
    /// `env!("CARGO_PKG_VERSION")` in the connector crate.
    const VERSION: &'static str;

    /// The validated configuration document this connector is built
    /// from.
    type Config: Document;

    /// The per-session system IO this connector opens.
    type Backend: Backend;

    /// Build the runtime from an already-validated config.
    fn assemble(config: Self::Config) -> Result<Self, <Self::Config as Document>::Error>
    where
        Self: Sized;

    /// The config document's generated JSON schema, when provided.
    fn config_schema() -> Option<serde_json::Value> {
        None
    }

    /// Truthful capability declaration — the host plans from this.
    fn capabilities(&self) -> DestinationCapabilities;

    /// A cheap connectivity probe — the SPI's `check` contract verbatim.
    async fn check(&self) -> Result<(), DestinationError> {
        Ok(())
    }

    /// Open the system IO for one load session. The backend MUST make a
    /// crashed predecessor's staging invisible and reclaimable (the
    /// SPI's open contract — a storage property the framework cannot
    /// add).
    async fn connect(&self, context: &OpenContext) -> Result<Self::Backend, DestinationError>;
}

/// One session's system IO — what the author writes instead of a
/// [`LoadSession`].
///
/// The framework calls these in the choreography's order; the backend
/// owns storage semantics. Two contracts the conformance suites verify
/// and no wrapper can supply: staged writes stay invisible until
/// `publish`, and `publish` is atomic with the state it persists.
#[async_trait]
pub trait Backend: Send {
    /// Create or migrate the physical table — the SPI's `ensure_table`
    /// contract verbatim (idempotent; records the write disposition).
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError>;

    /// Stage one batch, invisibly.
    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError>;

    /// The receipt this `(load_id, commit_seq)` already published under,
    /// if it did — the crash-recovery replay the framework consults
    /// BEFORE publishing. Backends whose receipts live in the same
    /// transaction as their publish keep their internal guard too; this
    /// is the protocol fast path.
    async fn existing_receipt(
        &mut self,
        load_id: &LoadId,
        commit_seq: u64,
    ) -> Result<Option<CommitReceipt>, DestinationError>;

    /// Atomically publish everything staged since the last commit AND
    /// persist `meta.state`, returning the new receipt.
    async fn publish(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError>;

    /// The state persisted by the latest successful commit, or `None`
    /// for a fresh pipeline.
    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError>;
}

/// The SPI shell around a [`DestinationConnector`] — what [`shell`]
/// returns.
#[derive(Debug)]
pub struct DestinationShell<C> {
    connector: C,
}

/// Wrap a framework connector as an SPI [`Destination`].
pub fn shell<C: DestinationConnector>(connector: C) -> DestinationShell<C> {
    DestinationShell { connector }
}

#[async_trait]
impl<C: DestinationConnector> Destination for DestinationShell<C> {
    fn spec(&self) -> ConnectorSpec {
        let mut spec = ConnectorSpec::new(C::NAME, C::VERSION);
        spec.config_schema = C::config_schema();
        spec
    }

    async fn check(&self) -> Result<(), DestinationError> {
        self.connector.check().await
    }

    fn capabilities(&self) -> DestinationCapabilities {
        self.connector.capabilities()
    }

    async fn open(&self, context: OpenContext) -> Result<Box<dyn LoadSession>, DestinationError> {
        let backend = self.connector.connect(&context).await?;
        Ok(Box::new(Session {
            backend,
            ensured: std::collections::BTreeSet::new(),
        }))
    }
}

/// The framework-owned session: choreography over a [`Backend`].
struct Session<B> {
    backend: B,
    /// Tables ensured BY THIS SESSION — the host's contract says every
    /// write is preceded by an ensure at the current schema version, and
    /// the session refuses violations instead of trusting them.
    ensured: std::collections::BTreeSet<TableName>,
}

#[async_trait]
impl<B: Backend> LoadSession for Session<B> {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        self.backend.ensure_table(schema, mode).await?;
        self.ensured.insert(schema.table.clone());
        Ok(())
    }

    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        if !self.ensured.contains(table) {
            return Err(DestinationError::fatal(format!(
                "write before ensure_table for `{table}` on this session — \
                 the host contract guarantees an ensure precedes the first \
                 write, so this is a harness or host defect, not data"
            )));
        }
        self.backend.write(table, batch).await
    }

    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        // Clause D3, as choreography: an at-least-once re-commit of the
        // same (load_id, commit_seq) returns the receipt it already
        // earned; nothing republishes.
        if let Some(receipt) = self
            .backend
            .existing_receipt(&meta.load_id, meta.commit_seq)
            .await?
        {
            return Ok(receipt);
        }
        self.backend.publish(meta).await
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        self.backend.read_state(pipeline).await
    }
}
