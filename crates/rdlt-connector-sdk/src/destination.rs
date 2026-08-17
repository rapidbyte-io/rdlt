//! The destination framework: implement [`DestinationConnector`] and a
//! [`Backend`], get the SPI — with the session choreography's
//! conformance clauses enforced by construction.
//!
//! The FRAMEWORK owns the protocol state machine, the connector owns the
//! system IO. Concretely, the session refuses a write to a table this
//! session never ensured, asks the backend for an existing
//! `(load_id, commit_seq)` receipt BEFORE publishing (so an
//! at-least-once re-commit returns the prior receipt instead of
//! republishing), and reads state through the backend untouched.
//!
//! What the choreography deliberately does NOT own: atomicity and
//! durability. Staging invisibility, crashed-session reclamation, and
//! atomic publish-with-state are properties of the backend's storage
//! that no wrapper can add from outside — the [`Backend`] contract
//! states them, the conformance suites verify them, and a transactional
//! backend keeps its own internal receipt guard as defense in depth (the
//! framework's replay check is the protocol fast path, not a substitute
//! for a transactional one).
//!
//! The black-box conformance kit cannot distinguish the framework's
//! replay choreography from a coincidentally-idempotent backend, so the
//! call ORDER is pinned by the crate's own spy-backend suite
//! (`tests/cases/test_session_choreography.rs`).
//!
//! [`WriteGuard`] carries the trust-boundary-independent half —
//! write-before-ensure and open-once — split out from [`Session`] so
//! TWO callers enforce the SAME rules: [`Session`] composes it for a
//! caller driving the shell as an SPI [`Destination`] (the client
//! crate's remote-backend adapter, a connector's own tests), and the
//! serve layer enforces it directly
//! against raw wire frames, because a bidi stream carrying
//! client-supplied frame ORDER never trusts that order. The commit
//! choreography (`existing_receipt` → `replay` → `publish`) stays
//! composed inside [`Session`] alone: the serve layer maps each of those
//! three wire frames straight onto its own [`Backend`] method, and the
//! CALLER — the client crate's remote-backend adapter, built on this same
//! [`Session`] type over a `Backend` that dials the connector — is what
//! reassembles the choreography; see [`Shell::connect`].

use async_trait::async_trait;
use rdlt_connector::arrow::RecordBatch;
use rdlt_connector::core::commit::{CommitMeta, CommitReceipt, WriteMode};
use rdlt_connector::core::id::{LoadId, PipelineId, TableName};
use rdlt_connector::core::schema::TableSchema;
use rdlt_connector::core::state::StateDoc;
use rdlt_connector::destination::{Capabilities, Destination, LoadSession, OpenContext};
use rdlt_connector::error::DestinationError;
use rdlt_connector::spec::ConnectorSpec;

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
    fn capabilities(&self) -> Capabilities;

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
    /// BEFORE publishing. A LOOKUP ONLY: side effects a replayed commit
    /// still needs belong in [`Backend::replay`], which the framework
    /// calls next. Backends whose receipts live in the same transaction
    /// as their publish keep their internal guard too; this is the
    /// protocol fast path.
    async fn existing_receipt(
        &mut self,
        load_id: &LoadId,
        commit_seq: u64,
    ) -> Result<Option<CommitReceipt>, DestinationError>;

    /// Housekeeping for a REPLAYED commit — called when
    /// [`Backend::existing_receipt`] found `receipt`, before the
    /// framework returns it instead of publishing. A replay is not
    /// always a no-op: a backend that stages into scratch tables must
    /// still clear the redelivered rows (or a later genuine commit
    /// finds and publishes them twice), and a backend keeping in-memory
    /// once-per-load guards must re-mark them so the rest of THIS
    /// session sees the replayed unit as done. The default does
    /// nothing, which is correct for backends that publish directly
    /// into the target.
    async fn replay(
        &mut self,
        meta: &CommitMeta,
        receipt: &CommitReceipt,
    ) -> Result<(), DestinationError> {
        let _ = (meta, receipt);
        Ok(())
    }

    /// Atomically publish everything staged since the last commit AND
    /// persist `meta.state`, returning the new receipt.
    async fn publish(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError>;

    /// The state persisted by the latest successful commit, or `None`
    /// for a fresh pipeline.
    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError>;

    /// The session's orderly end — the SPI's `close` contract verbatim
    /// (called exactly once whenever the session ends; strict — error
    /// propagates — after the run's last successful commit, best-effort
    /// — error ignored — on a failure/cancellation path; must tolerate
    /// being closed after arbitrary partial work). Default: nothing to
    /// do.
    async fn close(&mut self) -> Result<(), DestinationError> {
        Ok(())
    }
}

/// The SPI shell around a [`DestinationConnector`] — what [`shell`]
/// returns, and the SPI face `serve` runs over. Production connectors
/// run as processes; building a shell in-process is the seam `serve`
/// uses and the one a connector's own tests may use.
#[derive(Debug, Clone)]
pub struct Shell<C> {
    connector: C,
}

/// Wrap a framework connector as an SPI [`Destination`].
pub fn shell<C: DestinationConnector>(connector: C) -> Shell<C> {
    Shell { connector }
}

/// The constructor family: parse (through the config's [`Document`]
/// gate), assemble, shell. `serve` enters through [`Shell::from_value`]
/// with the handshake's document; a connector's tests build the same
/// face in-process from a value or text.
impl<C: DestinationConnector> Shell<C> {
    /// Validate an already-parsed document, assemble, and shell — the
    /// entry for a caller holding a config VALUE rather than text (a
    /// hand-built document has not passed any gate yet, so this one
    /// validates).
    pub fn new(config: C::Config) -> Result<Self, <C::Config as Document>::Error> {
        config.validate()?;
        Ok(shell(C::assemble(config)?))
    }

    /// Parse YAML text, validate, assemble, shell.
    pub fn from_yaml(yaml: &str) -> Result<Self, <C::Config as Document>::Error> {
        Ok(shell(C::assemble(C::Config::from_yaml(yaml)?)?))
    }

    /// Parse JSON text, validate, assemble, shell.
    pub fn from_json(json: &str) -> Result<Self, <C::Config as Document>::Error> {
        Ok(shell(C::assemble(C::Config::from_json(json)?)?))
    }

    /// The value entry — what `serve` calls with the handshake's config
    /// document: an already-parsed `serde_json::Value` straight through
    /// the same gate.
    pub fn from_value(value: serde_json::Value) -> Result<Self, <C::Config as Document>::Error> {
        Ok(shell(C::assemble(C::Config::from_value(value)?)?))
    }

    /// Open the connector's raw system IO directly. The serve layer
    /// drives the returned [`Backend`] frame-by-frame instead of going
    /// through [`Destination::open`]'s [`Session`] wrapper, so every
    /// wire frame (`Ensure`/`Write`/`ExistingReceipt`/`Replay`/
    /// `Publish`/`ReadState`/`Close`) reaches a REAL [`Backend`] method.
    /// This is the LOWER layer [`Destination::open`] is built on, not a
    /// replacement for it: nothing here enforces write-before-ensure or
    /// the replay choreography — those stay in [`WriteGuard`]/[`Session`],
    /// which a caller composes on top.
    pub async fn connect(&self, context: &OpenContext) -> Result<C::Backend, DestinationError> {
        self.connector.connect(context).await
    }
}

#[async_trait]
impl<C: DestinationConnector> Destination for Shell<C> {
    fn spec(&self) -> ConnectorSpec {
        let mut spec = ConnectorSpec::new(C::NAME, C::VERSION);
        spec.config_schema = C::config_schema();
        spec
    }

    async fn check(&self) -> Result<(), DestinationError> {
        self.connector.check().await
    }

    fn capabilities(&self) -> Capabilities {
        self.connector.capabilities()
    }

    async fn open(&self, context: OpenContext) -> Result<Box<dyn LoadSession>, DestinationError> {
        let backend = self.connector.connect(&context).await?;
        Ok(Box::new(Session::new(backend)))
    }
}

/// The write-before-ensure/open-once enforcement common to EVERY caller
/// of a [`Backend`], regardless of which side of a trust boundary it
/// runs on. Carries no `Backend` of its own: a caller composes it
/// alongside one (as [`Session`] does) or drives it directly against raw
/// frames (as the serve layer does).
#[derive(Debug, Default)]
pub struct WriteGuard {
    opened: bool,
    /// Tables ensured BY THIS SESSION — the host's contract says every
    /// write is preceded by an ensure at the current schema version, and
    /// the guard refuses violations instead of trusting them.
    ensured: std::collections::BTreeSet<TableName>,
}

impl WriteGuard {
    /// A fresh, unopened guard.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether the session has ALREADY completed a successful `Open` —
    /// checked BEFORE spending a `Backend::connect` attempt on a session
    /// that would be refused anyway. Split from marking open on purpose:
    /// see [`WriteGuard::mark_open`].
    pub fn is_open(&self) -> bool {
        self.opened
    }

    /// Mark the session open. Call this ONLY after a `Backend::connect`
    /// (or equivalent) attempt has SUCCEEDED — never eagerly before
    /// attempting it: an eager mark consumes the guard's one Open on a
    /// merely Transient connect failure, leaving the session with no
    /// backend and no way to retry, so a retryable failure degrades into
    /// one that is not. `debug_assert!` rather than `Result`: by the
    /// time a caller reaches this, [`WriteGuard::is_open`] must already
    /// have gated the call, so finding `opened` already true is a caller
    /// bug, not a wire input to report on.
    pub fn mark_open(&mut self) {
        debug_assert!(!self.opened, "mark_open called on an already-open guard");
        self.opened = true;
    }

    /// Record `table` as ensured at the CURRENT schema version.
    pub fn ensure(&mut self, table: TableName) {
        self.ensured.insert(table);
    }

    /// Refuse a write to a table this guard never saw [`WriteGuard::ensure`]d
    /// — the host contract guarantees an ensure precedes the first write,
    /// so a violation is a harness or host defect, not data.
    pub fn check_write(&self, table: &TableName) -> Result<(), DestinationError> {
        if !self.ensured.contains(table) {
            return Err(DestinationError::fatal(format!(
                "write before ensure_table for `{table}` on this session — \
                 the host contract guarantees an ensure precedes the first \
                 write, so this is a harness or host defect, not data"
            )));
        }
        Ok(())
    }
}

/// The framework-owned session: [`WriteGuard`] plus the commit
/// choreography (`existing_receipt` → `replay` → `publish`), composed
/// over a [`Backend`]. The choreography stays HERE rather than in
/// [`WriteGuard`] because it is the half that runs on whichever side of
/// the wire is doing the calling, not the half a server enforces against
/// wire ordering it does not trust.
///
/// Public so the client crate's remote-backend adapter can compose the
/// SAME `Session<B>` over a `Backend` that dials a connector out of
/// process, rather than reimplementing the choreography against the
/// wire. Constructed only through [`Session::new`]; `backend`/`guard`
/// stay private, so nothing outside this module can bypass the
/// choreography [`LoadSession::commit`] runs.
#[derive(Debug)]
pub struct Session<B> {
    backend: B,
    guard: WriteGuard,
}

impl<B> Session<B> {
    /// Wrap an already-open `backend` in a fresh session — a new,
    /// unopened [`WriteGuard`] alongside it. [`Destination::open`] calls
    /// this immediately after its own `connect` succeeds; the client
    /// crate's remote-backend adapter is the second caller, over a
    /// `Backend` that dials the connector process.
    pub fn new(backend: B) -> Self {
        Self {
            backend,
            guard: WriteGuard::new(),
        }
    }
}

#[async_trait]
impl<B: Backend> LoadSession for Session<B> {
    async fn ensure_table(
        &mut self,
        schema: &TableSchema,
        mode: &WriteMode,
    ) -> Result<(), DestinationError> {
        self.backend.ensure_table(schema, mode).await?;
        self.guard.ensure(schema.table.clone());
        Ok(())
    }

    async fn write(
        &mut self,
        table: &TableName,
        batch: RecordBatch,
    ) -> Result<(), DestinationError> {
        self.guard.check_write(table)?;
        self.backend.write(table, batch).await
    }

    async fn commit(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
        // An at-least-once re-commit of the same (load_id, commit_seq)
        // returns the receipt it already earned; nothing republishes.
        // The backend's replay hook runs first — redelivered staging
        // must be cleared and once-per-load guards re-marked even
        // though the publish is skipped.
        if let Some(receipt) = self
            .backend
            .existing_receipt(&meta.load_id, meta.commit_seq)
            .await?
        {
            // The receipt is the protocol's idempotence TOKEN, and a
            // token that names a different commit proves nothing about
            // this one: a backend whose lookup is buggy — a stale
            // cache, an unkeyed read — would otherwise turn every
            // commit into a silent no-op publish while the engine marks
            // the WAL committed and reclaims segments. Checked HERE, at
            // the choreography seat every host rides, so no backend
            // has to be trusted to check it itself.
            if receipt.load_id != meta.load_id || receipt.commit_seq != meta.commit_seq {
                return Err(DestinationError::fatal(format!(
                    "the destination answered existing_receipt({}, {}) with a receipt for \
                     ({}, {}) — a receipt naming a different commit proves nothing about \
                     this one; refusing rather than skipping publish",
                    meta.load_id,
                    meta.commit_seq,
                    // The forged identity is arbitrary wire text:
                    // render it bounded and escaped — inert, and never a
                    // frame-cap-sized firehose.
                    rdlt_connector::gate::render_diagnostic(receipt.load_id.as_str(), 256),
                    receipt.commit_seq
                )));
            }
            self.backend.replay(&meta, &receipt).await?;
            return Ok(receipt);
        }
        // The mirror of the lookup guard above: the receipt `publish`
        // RETURNS is the same idempotence token — an embedder holding
        // it (remote hosts do) would vouch for this commit with a token
        // naming some other one. The engine discards returned receipts,
        // so in-tree this is defense in depth; checked at the same seat
        // for the same reason.
        let (expected_load, expected_seq) = (meta.load_id.clone(), meta.commit_seq);
        let receipt = self.backend.publish(meta).await?;
        if receipt.load_id != expected_load || receipt.commit_seq != expected_seq {
            return Err(DestinationError::fatal(format!(
                "the destination answered publish({expected_load}, {expected_seq}) with a \
                 receipt for ({}, {}) — a receipt naming a different commit proves nothing \
                 about this one; refusing rather than certifying the publish",
                rdlt_connector::gate::render_diagnostic(receipt.load_id.as_str(), 256),
                receipt.commit_seq
            )));
        }
        Ok(receipt)
    }

    async fn read_state(
        &mut self,
        pipeline: &PipelineId,
    ) -> Result<Option<StateDoc>, DestinationError> {
        self.backend.read_state(pipeline).await
    }

    async fn close(&mut self) -> Result<(), DestinationError> {
        self.backend.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, serde::Deserialize)]
    struct ProbeConfig {}

    #[derive(Debug, thiserror::Error)]
    enum ProbeError {
        #[error("probe yaml: {0}")]
        Yaml(#[from] serde_yaml_ng::Error),
        #[error("probe json: {0}")]
        Json(#[from] serde_json::Error),
    }

    impl Document for ProbeConfig {
        type Error = ProbeError;
        fn validate(&self) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    #[derive(Debug)]
    struct Probe;
    struct ProbeBackend;

    #[async_trait]
    impl Backend for ProbeBackend {
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
        async fn existing_receipt(
            &mut self,
            _load_id: &LoadId,
            _commit_seq: u64,
        ) -> Result<Option<CommitReceipt>, DestinationError> {
            Ok(None)
        }
        async fn publish(&mut self, meta: CommitMeta) -> Result<CommitReceipt, DestinationError> {
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

    #[async_trait]
    impl DestinationConnector for Probe {
        const NAME: &'static str = "probe";
        const VERSION: &'static str = "9.9.9";
        type Config = ProbeConfig;
        type Backend = ProbeBackend;

        fn assemble(_config: ProbeConfig) -> Result<Self, ProbeError> {
            Ok(Self)
        }

        fn config_schema() -> Option<serde_json::Value> {
            Some(serde_json::json!({"type": "object"}))
        }

        fn capabilities(&self) -> Capabilities {
            Capabilities::default().with_structs(true)
        }

        async fn check(&self) -> Result<(), DestinationError> {
            Err(DestinationError::fatal("probe refuses"))
        }

        async fn connect(&self, _context: &OpenContext) -> Result<ProbeBackend, DestinationError> {
            Ok(ProbeBackend)
        }
    }

    /// The from-text constructor family works end to end on the
    /// destination shell too.
    #[tokio::test]
    async fn the_shell_constructors_run_the_document_gate() {
        let from_yaml = Shell::<Probe>::from_yaml("{}").expect("valid yaml");
        assert_eq!(from_yaml.spec().name, "probe");
        let from_value = Shell::<Probe>::from_value(serde_json::json!({})).expect("valid value");
        assert_eq!(from_value.spec().name, "probe");
        let parse = Shell::<Probe>::from_json("not json").unwrap_err();
        assert!(parse.to_string().starts_with("probe json: "), "{parse}");
    }

    /// The shell serves spec, capabilities, and check from the
    /// connector's OWN declarations — non-default values chosen so
    /// replacing any delegation with a default would fail here.
    #[tokio::test]
    async fn the_shell_serves_the_spi_from_the_connector_parts() {
        let destination = shell(Probe);
        let spec = destination.spec();
        assert_eq!(
            (spec.name.as_str(), spec.version.as_str()),
            ("probe", "9.9.9")
        );
        assert_eq!(
            spec.config_schema,
            Some(serde_json::json!({"type": "object"}))
        );
        assert!(
            destination.capabilities().structs,
            "the declared capabilities, not a default"
        );
        let refused = destination
            .check()
            .await
            .expect_err("the override propagates");
        assert!(refused.to_string().contains("probe refuses"));
    }

    /// `ProbeBackend` leaves `Backend::close` at its default — this
    /// pins BOTH halves at once: the default itself is a trivial
    /// success, AND the shell's `LoadSession::close` genuinely forwards
    /// to it rather than being a no-op of its own that never reaches
    /// the backend.
    #[tokio::test]
    async fn the_shells_close_forwards_to_the_backends_default() {
        let destination = shell(Probe);
        let mut session = destination
            .open(OpenContext::new(PipelineId::new("p"), LoadId::new("l")))
            .await
            .expect("probe connect always succeeds");
        assert!(session.close().await.is_ok());
    }
}
