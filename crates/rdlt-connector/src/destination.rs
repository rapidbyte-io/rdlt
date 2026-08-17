//! The destination half of the SPI: [`Destination`], its capability
//! declaration, its per-run [`LoadSession`], and the context a session
//! opens under.

use arrow_array::RecordBatch;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::core::naming::IdentRules;
use crate::core::{
    CommitMeta, CommitReceipt, LoadId, PipelineId, StateDoc, TableName, TableSchema, WriteMode,
};
use crate::error::DestinationError;
use crate::spec::ConnectorSpec;

/// A data destination: capability declaration + session factory.
#[async_trait]
pub trait Destination: Send + Sync + 'static {
    /// Name, version, config JSON-schema.
    fn spec(&self) -> ConnectorSpec;

    /// A cheap connectivity probe: can this destination reach what it
    /// writes to, with the credentials it holds?
    ///
    /// Distinct from [`Destination::open`] on purpose — an operator wants
    /// "are the credentials right" answered in seconds, without staging
    /// anything. Failures classify exactly as a session would classify
    /// them.
    ///
    /// The default body returns `Ok(())` WITHOUT probing anything, so a
    /// destination that has not implemented it reports success trivially —
    /// a host needing a real answer must know which connectors implement
    /// the probe. Implementations replace it wholesale.
    async fn check(&self) -> Result<(), DestinationError> {
        Ok(())
    }

    /// Truthful capability declaration — the host plans from this and
    /// does not re-verify at runtime.
    fn capabilities(&self) -> Capabilities;

    /// Open a load session. MUST make uncommitted staged data from any
    /// previous (crashed) session invisible and reclaimable.
    async fn open(&self, context: OpenContext) -> Result<Box<dyn LoadSession>, DestinationError>;
}

/// What a destination can do natively — data, not traits.
///
/// The host plans from this at BUILD time (lowering strategy, merge
/// validation, identifier normalization) and does not re-verify at
/// runtime, so an untruthful declaration is not caught by a nicer error
/// later — it is caught by the destination failing mid-load, or worse,
/// by data arriving in a shape nobody intended. Declaring truthfully is
/// a conformance obligation.
///
/// `Default` is the most conservative destination possible — everything
/// false, default identifier rules — which the host can always lower for.
/// `#[non_exhaustive]`, so construction is `Default` plus the `with_*`
/// declarations: a future capability then arrives as one new method with
/// a conservative default, not as a breaking change for every
/// out-of-tree destination.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Capabilities {
    /// Supports keyed merge (upsert + subtree replacement by
    /// `_rdlt_root_id`).
    pub merge: bool,
    /// Native struct/nested columns; if false the host flattens
    /// collision-safely.
    pub structs: bool,
    /// Native list-of-scalars columns; if false those become child
    /// tables.
    pub scalar_lists: bool,
    /// Has a native JSON/JSONB type.
    ///
    /// Unlike `structs` and `decimal`, this flag does NOT drive host
    /// lowering — the host never rewrites a `Json` column. A destination
    /// declaring `false` states that it maps `Json` itself, in its own
    /// schema translation, and owns doing so (a lakehouse format without
    /// a JSON type maps it to string; a file format writes its own
    /// encoding).
    pub json_type: bool,
    /// Native fixed-point decimal; if false decimals lower to text.
    pub decimal: bool,
    /// The destination's publish is NOT atomic across tables, and its
    /// crash convergence recognises prior work only under the SAME run
    /// identity. The engine must refuse to run it without a WAL workdir:
    /// a mid-publish failure would otherwise restart under a fresh load
    /// id and re-append rows the first attempt already committed.
    #[serde(default)]
    pub requires_durable_identity: bool,
    /// Identifier limits and case-folding, used to normalize table and
    /// column names before any DDL is generated. Wrong rules produce
    /// names the destination silently truncates or folds into
    /// collisions.
    pub ident_rules: IdentRules,
}

impl Capabilities {
    /// Declare keyed-merge support.
    #[must_use]
    pub fn with_merge(mut self, merge: bool) -> Self {
        self.merge = merge;
        self
    }

    /// Declare native struct/nested-column support.
    #[must_use]
    pub fn with_structs(mut self, structs: bool) -> Self {
        self.structs = structs;
        self
    }

    /// Declare native list-of-scalars support.
    #[must_use]
    pub fn with_scalar_lists(mut self, scalar_lists: bool) -> Self {
        self.scalar_lists = scalar_lists;
        self
    }

    /// Declare a native JSON/JSONB type (see the field's caveat: this
    /// flag does not drive host lowering).
    #[must_use]
    pub fn with_json_type(mut self, json_type: bool) -> Self {
        self.json_type = json_type;
        self
    }

    /// Declare native fixed-point decimal support.
    #[must_use]
    pub fn with_decimal(mut self, decimal: bool) -> Self {
        self.decimal = decimal;
        self
    }

    /// Declare that the destination requires durable identity
    /// (non-atomic-publish destinations that demand a WAL workdir).
    #[must_use]
    pub fn with_requires_durable_identity(mut self, requires_durable_identity: bool) -> Self {
        self.requires_durable_identity = requires_durable_identity;
        self
    }

    /// Declare the destination's identifier limits and case-folding.
    #[must_use]
    pub fn with_ident_rules(mut self, ident_rules: IdentRules) -> Self {
        self.ident_rules = ident_rules;
        self
    }
}

/// What a load session opens under.
///
/// `#[non_exhaustive]`: out-of-crate struct-literal construction is
/// closed, so wire-era fields can arrive without a breaking change.
/// [`OpenContext::new`] is the constructor — every consumer in the tree
/// builds through it — and the fields stay `pub` for read access.
#[derive(Clone)]
#[non_exhaustive]
pub struct OpenContext {
    /// The pipeline whose state this session reads and republishes — the
    /// key `StateDoc` persists under.
    pub pipeline: PipelineId,
    /// THIS run's identity. Half of the `(load_id, commit_seq)` pair that
    /// makes a re-commit idempotent, and what distinguishes this
    /// session's staging from a crashed predecessor's.
    pub load_id: LoadId,
    /// Where a file-writing destination reports each output part it
    /// closes. ADVISORY, like every telemetry signal: absent means
    /// nobody is listening and the destination must behave identically.
    /// SQL destinations never call it.
    pub part_events: Option<PartEventFn>,
}

impl OpenContext {
    /// Build a context. Present so a later field addition is not a
    /// breaking change for callers constructing through here rather than
    /// with a struct literal.
    pub fn new(pipeline: PipelineId, load_id: LoadId) -> Self {
        Self {
            pipeline,
            load_id,
            part_events: None,
        }
    }

    /// Attach a part-event listener.
    #[must_use]
    pub fn with_part_events(mut self, listener: PartEventFn) -> Self {
        self.part_events = Some(listener);
        self
    }
}

impl std::fmt::Debug for OpenContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The listener is a function with no useful rendering; whether
        // one is attached is the fact worth printing.
        f.debug_struct("OpenContext")
            .field("pipeline", &self.pipeline)
            .field("load_id", &self.load_id)
            .field("part_events", &self.part_events.is_some())
            .finish()
    }
}

/// A closed-part report: the file-writing destinations' telemetry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct PartClosed {
    /// The table the part belongs to.
    pub table: TableName,
    /// The part's encoded, on-the-wire size.
    pub encoded_bytes: u64,
    /// What closed it.
    pub reason: PartCloseReason,
}

impl PartClosed {
    /// Build a report. The hedge for later field additions, like
    /// [`OpenContext::new`].
    pub fn new(table: TableName, encoded_bytes: u64, reason: PartCloseReason) -> Self {
        Self {
            table,
            encoded_bytes,
            reason,
        }
    }
}

/// Why a part closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum PartCloseReason {
    /// It reached its size target.
    Target,
    /// It was open longer than the configured time bound.
    Time,
    /// The memory ceiling across open parts closed it early.
    Budget,
    /// A commit closed it — no part spans a commit.
    Commit,
    /// A schema change closed it — a file holds one schema.
    Schema,
}

/// The listener shape: a plain callback, so the SPI carries no channel
/// or runtime dependency for what is a fire-and-forget signal. `Arc`d
/// because sessions and contexts are moved around freely.
pub type PartEventFn = std::sync::Arc<dyn Fn(PartClosed) + Send + Sync>;

/// One destination load session. Writes are staged and invisible until
/// `commit`; publication is atomic with pipeline state and idempotent per
/// `(load_id, commit_seq)`.
///
/// Send-only deliberately: a remote client owns each session in one
/// task; no speculative Sync.
#[async_trait]
pub trait LoadSession: Send {
    /// Create or migrate the physical table for this schema version, and
    /// record the table's write disposition (`mode` is the root stream's
    /// mode for child tables — merge needs it at commit time).
    /// Idempotent; always precedes the first write at each schema
    /// version.
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError>;

    /// Write one batch. The host guarantees `batch` conforms exactly to
    /// the last ensured schema for `table` and that per-table batches
    /// arrive in order. Delivery is at-least-once — safe because staged
    /// writes stay invisible until commit and a crashed session's staging
    /// is reclaimed on the next open.
    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError>;

    /// Atomically publish all writes since the last commit AND persist
    /// `meta.state`. Re-committing the same `(load_id, commit_seq)`
    /// returns the prior receipt without re-publishing.
    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError>;

    /// Recover the state persisted by the latest successful commit, or
    /// `None` for a fresh pipeline.
    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError>;

    /// Called exactly once whenever the session ends. After the run's
    /// last successful commit its error PROPAGATES (a cleanup failure
    /// is a real destination error); on failure/cancellation paths the
    /// engine invokes it best-effort and ignores its error — the
    /// session must tolerate being closed after arbitrary partial
    /// work. Default: nothing to do.
    async fn close(&mut self) -> Result<(), DestinationError> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Closeless;

    #[async_trait]
    impl LoadSession for Closeless {
        async fn ensure_table(
            &mut self,
            _schema: &TableSchema,
            _mode: &WriteMode,
        ) -> Result<(), DestinationError> {
            Ok(())
        }
        async fn write(
            &mut self,
            _table: &TableName,
            _batch: RecordBatch,
        ) -> Result<(), DestinationError> {
            Ok(())
        }
        async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
            Ok(CommitReceipt {
                load_id: meta.load_id,
                commit_seq: meta.commit_seq,
            })
        }
        async fn read_state(
            &mut self,
            _pipeline: &PipelineId,
        ) -> Result<Option<StateDoc>, DestinationError> {
            Ok(None)
        }
    }

    /// The default `close` is a trivial success BY CONTRACT: a session
    /// with nothing to release on close need not implement it at all.
    #[tokio::test]
    async fn the_default_close_reports_success_without_doing_anything() {
        assert!(Closeless.close().await.is_ok());
    }

    #[test]
    fn part_events_serialize_with_the_core_twin_spelling() {
        let event = PartClosed::new(TableName::new("t"), 512, PartCloseReason::Budget);
        let wire = serde_json::to_value(&event).expect("serializes");
        assert_eq!(
            wire["reason"], "budget",
            "snake_case, matching rdlt_core::PartClose"
        );
        let back: PartClosed = serde_json::from_value(wire).expect("round-trips");
        assert_eq!(back, event);
    }

    /// `Default` is the shape the host can always lower for, and each
    /// builder moves exactly one declaration.
    #[test]
    fn default_is_conservative_and_each_builder_declares_one_thing() {
        let conservative = Capabilities::default();
        assert!(!conservative.merge);
        assert!(!conservative.structs);
        assert!(!conservative.scalar_lists);
        assert!(!conservative.json_type);
        assert!(!conservative.decimal);
        assert!(!conservative.requires_durable_identity);

        let declared = Capabilities::default()
            .with_merge(true)
            .with_structs(true)
            .with_scalar_lists(true)
            .with_json_type(true)
            .with_decimal(true)
            .with_requires_durable_identity(true);
        assert!(
            declared.merge
                && declared.structs
                && declared.scalar_lists
                && declared.json_type
                && declared.decimal
                && declared.requires_durable_identity
        );
        assert_eq!(declared.ident_rules, IdentRules::default());
    }

    /// The declaration is data a platform can store and ship.
    #[test]
    fn the_declaration_round_trips_through_serde() {
        let declared = Capabilities::default()
            .with_merge(true)
            .with_decimal(true)
            .with_requires_durable_identity(true);
        let wire = serde_json::to_value(declared).expect("serializes");
        assert_eq!(wire["merge"], true);
        assert_eq!(wire["structs"], false);
        assert_eq!(wire["requires_durable_identity"], true);
        let back: Capabilities = serde_json::from_value(wire).expect("round-trips");
        assert_eq!(back, declared);
    }
}
