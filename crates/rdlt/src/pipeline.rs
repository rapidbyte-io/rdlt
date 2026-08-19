//! The engine's boundary: a typestate builder that takes a source VALUE
//! and a destination VALUE and hands back a runnable [`Pipeline`] — and
//! the document constructors ([`Pipeline::from_file`],
//! [`Pipeline::from_text`], [`Pipeline::from_document`],
//! [`Pipeline::from_document_with`]) that feed it from a pipeline
//! document.
//!
//! Missing source or destination is a **compile** error; configuration
//! errors die in [`Builder::build`], before any I/O. In production the
//! values are the runtime's process adapters (what the `from_*`
//! constructors supply); hand-rolled SPI implementations are test
//! doubles.

use std::path::{Path, PathBuf};

use rdlt_runtime::local::Local;
use rdlt_runtime::provider::Provider;

use crate::commit::{BatchPolicy, CommitPolicy, WriteMode};
use crate::document::{self, Document};
use crate::error::Error;
use crate::event::EventStream;
use crate::id::{PipelineId, StreamName};
use crate::policy::SchemaPolicy;
use crate::report;
use crate::sdk::spi::destination::Destination;
use crate::sdk::spi::source::Source;
use rdlt_engine::config::Config;
use rdlt_engine::engine::Engine;

/// Typestate marker: no source/destination provided yet.
#[derive(Debug)]
pub struct Missing;

/// Builder for [`Pipeline`]. `S`/`D` are typestate: `build` exists only once both a
/// source and a destination are set.
///
/// ```compile_fail
/// // This must NOT compile — no destination was provided.
/// let _ = rdlt::pipeline::Pipeline::builder("p")
///     .source(rdlt_testkit::memory::Source::default())
///     .build();
/// ```
#[derive(Debug)]
pub struct Builder<S, D> {
    config: Config,
    source: S,
    destination: D,
}

impl Builder<Missing, Missing> {
    pub(crate) fn new(pipeline: impl Into<PipelineId>) -> Self {
        Self {
            config: Config::new(pipeline),
            source: Missing,
            destination: Missing,
        }
    }
}

impl<S, D> Builder<S, D> {
    /// Set the source. Changes the builder's type, which is how a pipeline
    /// missing a source fails to compile rather than failing at run time.
    pub fn source<NS: Source>(self, source: NS) -> Builder<NS, D> {
        Builder {
            config: self.config,
            source,
            destination: self.destination,
        }
    }

    /// Set the destination. Changes the builder's type, so `build()` is only
    /// callable once both halves are present.
    pub fn destination<ND: Destination>(self, destination: ND) -> Builder<S, ND> {
        Builder {
            config: self.config,
            source: self.source,
            destination,
        }
    }

    /// Default write disposition for every stream (default: `Append`).
    pub fn write_mode(mut self, mode: WriteMode) -> Self {
        self.config = self.config.with_write_mode(mode);
        self
    }

    /// Per-stream override.
    pub fn write_mode_for(mut self, stream: impl Into<StreamName>, mode: WriteMode) -> Self {
        self.config = self.config.with_write_mode_for(stream.into(), mode);
        self
    }

    /// Schema-change policy (default: evolve).
    pub fn schema_policy(mut self, policy: SchemaPolicy) -> Self {
        self.config = self.config.with_schema_policy(policy);
        self
    }

    /// How much to accumulate before each destination WRITE
    /// (default: write each source batch straight through).
    pub fn batch_policy(mut self, policy: BatchPolicy) -> Self {
        self.config = self.config.with_batch_policy(policy);
        self
    }

    /// Commit grouping policy (default: every checkpoint).
    pub fn commit_policy(mut self, policy: CommitPolicy) -> Self {
        self.config = self.config.with_commit_policy(policy);
        self
    }

    /// Local work directory (holds the WAL). Unset means NO WAL:
    /// recovery still works, degrading to re-extraction from the last
    /// committed cursor — slower, never wrong. The document constructors
    /// ([`Pipeline::from_document`] and kin) default it to
    /// `.rdlt/<pipeline>` under their base; embedders building here
    /// choose their own.
    /// One workdir belongs to ONE pipeline — the engine refuses a
    /// directory whose WAL another pipeline wrote.
    pub fn workdir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.config = self.config.with_workdir(dir.into());
        self
    }

    /// Cap on in-flight bytes per stage channel — the memory bound.
    pub fn byte_budget(mut self, bytes: usize) -> Self {
        self.config = self.config.with_byte_budget(bytes);
        self
    }

    /// Cap on `columns × rows` in one assembled output batch: the memory
    /// bound for the assembly step, which materializes every null-filled
    /// absent column. Wide-and-large honest tables that cross the default
    /// raise it here — the same knob the assembly seat's refusal names.
    pub fn max_batch_cells(mut self, cells: usize) -> Self {
        self.config = self.config.with_max_batch_cells(cells);
        self
    }

    /// Cap on streams one source may declare: every declared stream costs
    /// plan-time validation and its own share of the run's in-flight
    /// budget. An honestly larger discovery raises it here — the same
    /// knob the plan-time refusal names.
    pub fn max_streams_per_source(mut self, streams: usize) -> Self {
        self.config = self.config.with_max_streams_per_source(streams);
        self
    }

    /// Cap on streams READING at once (default 16). Discovery may
    /// declare more; the rest wait for a read slot, holding no rows
    /// while they wait. Zero clamps to one.
    pub fn max_concurrent_streams(mut self, streams: usize) -> Self {
        self.config = self.config.with_max_concurrent_streams(streams);
        self
    }

    /// How transient failures are retried (default:
    /// [`RetryPolicy::default`](crate::policy::RetryPolicy) — 5 total
    /// attempts, 100 ms base doubling to a 30 s cap, full jitter).
    pub fn retry_policy(mut self, policy: crate::policy::RetryPolicy) -> Self {
        self.config = self.config.with_retry_policy(policy);
        self
    }
}

impl<S: Source, D: Destination> Builder<S, D> {
    /// Validate configuration against destination capabilities and construct the
    /// pipeline. No network or destination I/O happens here; the checks are purely
    /// against the declared destination capabilities.
    pub fn build(self) -> Result<Pipeline, Error> {
        let caps = self.destination.capabilities();
        // Which streams request Merge: the default write mode, and any
        // named per-stream overrides.
        let merge_default = matches!(self.config.write_mode(), WriteMode::Merge { .. });
        let merge_overrides: Vec<&StreamName> = self
            .config
            .write_modes()
            .iter()
            .filter(|(_, mode)| matches!(mode, WriteMode::Merge { .. }))
            .map(|(stream, _)| stream)
            .collect();
        if !caps.merge && (merge_default || !merge_overrides.is_empty()) {
            return Err(Error::config(format!(
                "destination `{}` does not support Merge (requested {})",
                self.destination.spec().name,
                if merge_default {
                    "as the default write mode".to_owned()
                } else {
                    format!(
                        "for streams: {}",
                        merge_overrides
                            .iter()
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
            )));
        }
        // Merge-key precedence: `WriteMode::Merge { key }` and a source's
        // `StreamSpec::primary_key` are NOT independent — for a structured
        // stream the two must AGREE (name the same columns, as a set), and the
        // engine enforces that agreement at plan time in `validate_streams`
        // once the source's streams are known. `build` runs before any stream
        // exists, so it can only enforce the stream-agnostic half: a Merge
        // write mode must carry at least one key column.
        for (stream, mode) in self.config.write_modes() {
            if let WriteMode::Merge { key } = mode
                && key.is_empty()
            {
                return Err(Error::config(format!(
                    "stream `{stream}`: Merge requires at least one key column"
                )));
            }
        }
        if let WriteMode::Merge { key } = self.config.write_mode()
            && key.is_empty()
        {
            return Err(Error::config("Merge requires at least one key column"));
        }
        // The document path refuses a threshold-less commit policy at
        // build; the builder path makes the same refusal — such a policy
        // would hold the whole run in one crash window, the exact shape
        // the type refuses everywhere else.
        if let Err(reason) = self.config.commit_policy().check() {
            return Err(Error::config(reason.to_string()));
        }

        Ok(Pipeline {
            engine: Engine::new(self.config, self.source, self.destination),
        })
    }
}

/// The engine byte budget a spawned connector's dial derives its
/// flow-control windows from: the document's `resources.byte_budget`
/// where spelled — the SAME number `apply_tuning` threads into the
/// engine channel, with the builder's own floor clamp, so the wire can
/// never hold more in flight than the engine would buffer — and the
/// engine channel's own default constant otherwise. Deliberately NOT
/// the document's `batch_policy.every_bytes`: that knob paces
/// DESTINATION writes, and the facade never resizes the engine channel
/// from it, so deriving the wire window from it would silently
/// throttle the connector wire to a write-cadence number while the
/// engine kept buffering at its default.
fn engine_budget_bytes(document: &Document) -> u64 {
    match document.resources.as_ref().and_then(|r| r.byte_budget) {
        Some(bytes) => bytes.max(1) as u64,
        None => rdlt_engine::config::Config::DEFAULT_BYTE_BUDGET as u64,
    }
}

/// Thread the document's engine-tuning nodes — `schema_policy` and
/// `resources` — onto the builder. Infallible by construction: each
/// knob rides its builder method's own clamp (a spelled `0` becomes
/// the smallest enforceable value, never "off"), and the scalar policy
/// maps onto the engine type's run-wide default.
fn apply_tuning<S, D>(builder: Builder<S, D>, document: &Document) -> Builder<S, D> {
    let builder = match &document.schema_policy {
        Some(mode) => {
            let action = match mode {
                document::SchemaPolicy::Evolve => rdlt_engine::policy::PolicyAction::Evolve,
                document::SchemaPolicy::Freeze => rdlt_engine::policy::PolicyAction::Freeze,
                document::SchemaPolicy::DiscardRow => rdlt_engine::policy::PolicyAction::DiscardRow,
                document::SchemaPolicy::DiscardValue => {
                    rdlt_engine::policy::PolicyAction::DiscardValue
                }
            };
            builder.schema_policy(SchemaPolicy::with_default(action))
        }
        None => builder,
    };
    let Some(resources) = &document.resources else {
        return builder;
    };
    let builder = match resources.byte_budget {
        Some(bytes) => builder.byte_budget(bytes),
        None => builder,
    };
    let builder = match resources.max_batch_cells {
        Some(cells) => builder.max_batch_cells(cells),
        None => builder,
    };
    let builder = match resources.max_streams_per_source {
        Some(streams) => builder.max_streams_per_source(streams),
        None => builder,
    };
    match resources.max_concurrent_streams {
        Some(streams) => builder.max_concurrent_streams(streams),
        None => builder,
    }
}

/// The workdir a document runs under: the spelled path — relative
/// spellings resolved against `base`, the document's own directory,
/// exactly like path-form configs — or, when the document names none,
/// `.rdlt/<pipeline>` under `base`.
///
/// Per pipeline, deliberately: the engine replays and clears whatever
/// WAL its workdir holds, and it refuses a directory occupied by
/// another pipeline — the per-pipeline default keeps that refusal from
/// ever firing on defaulted documents. Resolving against `base` (not
/// the invoking shell's working directory) means one document owns ONE
/// workdir and one lock from wherever it is run; a document built from a
/// string gets whatever base its embedder passes, so a bare `Path::new("")`
/// base stays working-directory-relative. The pipeline name is
/// path-sanitized through the same normalization destination
/// identifiers use, so a name carrying separators or `..` cannot
/// escape `.rdlt/`.
fn resolved_workdir(document: &Document, base: &Path) -> PathBuf {
    match &document.workdir {
        Some(dir) => base.join(dir),
        None => base
            .join(".rdlt")
            .join(rdlt_engine::naming::normalize_ident(
                &document.pipeline,
                rdlt_core::schema::IdentRules::default(),
            )),
    }
}

/// What a clean [`Pipeline::check`] found — the engine's summary under
/// its facade name. At least the discovered stream count.
pub use rdlt_engine::check::Summary as CheckSummary;

/// A configured, runnable pipeline.
#[derive(Debug)]
pub struct Pipeline {
    engine: Engine,
}

impl Pipeline {
    /// Start building a pipeline with this name.
    ///
    /// The returned builder is missing both connectors, and its type says so:
    /// `build()` does not exist until a source and a destination are set.
    pub fn builder(name: impl Into<PipelineId>) -> Builder<Missing, Missing> {
        Builder::new(name)
    }

    /// Read a pipeline document file, parse it, and construct the
    /// pipeline over the default local provider — [`Pipeline::from_document`]
    /// with the base inferred: the file's own directory (`""`, the working
    /// directory, for a bare file name), so a `config: ./creds.yaml` and
    /// the default `workdir` resolve beside the document that names them.
    /// The read is capped at [`document::MAX_DOCUMENT_BYTES`]; a missing
    /// or oversized file, and a document that does not parse, refuse as
    /// [`Error::Config`] naming the path.
    pub async fn from_file(path: impl AsRef<Path>) -> Result<Pipeline, Error> {
        let path = path.as_ref();
        let text = document::read(path).map_err(Error::config)?;
        let doc = document::parse(&text)
            .map_err(|e| Error::config(format!("parsing {}: {e}", path.display())))?;
        let base = path.parent().unwrap_or(Path::new(""));
        Pipeline::from_document(&doc, base).await
    }

    /// Parse a pipeline document from text — YAML or JSON (JSON is
    /// valid YAML) — and construct the pipeline over the default local
    /// provider. `base` is the directory relative `config:` paths and the
    /// default `workdir` resolve against; a text has no directory of its
    /// own, so the embedder names the one it means (`""` for the working
    /// directory). A document that does not parse refuses as
    /// [`Error::Config`].
    pub async fn from_text(text: &str, base: impl AsRef<Path>) -> Result<Pipeline, Error> {
        let doc =
            document::parse(text).map_err(|e| Error::config(format!("parsing document: {e}")))?;
        Pipeline::from_document(&doc, base).await
    }

    /// Construct the pipeline a parsed [`Document`] describes over the
    /// default local provider (spawn from `PATH`, or the `path:` an arm
    /// names). The document's `resources.byte_budget` sizes BOTH the
    /// engine channel and this provider's dial windows — one number for
    /// the two sides; with [`Pipeline::from_document_with`] the
    /// caller's provider carries its own budget. Construction only — no data moves — but BOTH arms'
    /// connector requirements are resolved: spawn, dial, handshake (where
    /// the CONNECTOR validates its own config), wrap; reading a path-form
    /// config file is the one other I/O. `base` is the directory relative
    /// `config:` paths and the default `workdir` resolve against — the
    /// document's own directory when it came from a file. Data paths
    /// INSIDE a config document are the connector process's to resolve,
    /// against its working directory. Document problems — an unreadable
    /// or malformed config file, a binary that cannot be found or spawned,
    /// a config the connector's gate refuses, a threshold-less commit
    /// policy — are [`Error::Config`]; the builder's own refusals pass
    /// through as themselves.
    pub async fn from_document(doc: &Document, base: impl AsRef<Path>) -> Result<Pipeline, Error> {
        let provider = Local::default().with_budget_bytes(engine_budget_bytes(doc));
        Pipeline::from_document_with(doc, base, &provider).await
    }

    /// [`Pipeline::from_document`] with the caller's own [`Provider`]
    /// deciding how connector requirements become processes (or pool
    /// members, or anything else) — the engine never learns which. `base`
    /// as there: the directory relative `config:` paths and the default
    /// `workdir` resolve against. Under a caller-supplied provider the
    /// document's `resources.byte_budget` resizes the ENGINE channel
    /// only; the wire windows stay the provider's own budget (the
    /// default-provider path sizes both from the one number).
    pub async fn from_document_with(
        doc: &Document,
        base: impl AsRef<Path>,
        provider: &dyn Provider,
    ) -> Result<Pipeline, Error> {
        let base = base.as_ref();
        let builder = Pipeline::builder(doc.pipeline.as_str());
        let builder = match &doc.write_mode {
            None | Some(document::WriteMode::Append) => builder.write_mode(WriteMode::Append),
            Some(document::WriteMode::Replace) => builder.write_mode(WriteMode::Replace),
            Some(document::WriteMode::Merge { key }) => {
                builder.write_mode(WriteMode::Merge { key: key.clone() })
            }
        };
        let builder = builder.workdir(resolved_workdir(doc, base));
        let builder = apply_tuning(builder, doc);
        let builder = match &doc.batch_policy {
            Some(policy) => builder.batch_policy(*policy),
            None => builder,
        };
        let builder = match &doc.commit_policy {
            // Refused here rather than honoured: a policy with no
            // threshold never fires, so the run would hold everything
            // uncommitted until it ended.
            Some(policy) => {
                policy.check().map_err(Error::config)?;
                builder.commit_policy(*policy)
            }
            None => builder,
        };

        // The provider's typed errors render verbatim — the frozen
        // NotFound spelling, the handshake's identity/config refusals —
        // never a facade paraphrase on top.
        let source_config = doc.source.config.resolve(base)?;
        let source = provider
            .source(&doc.source.requirement(), &source_config)
            .await
            .map_err(|e| Error::config(e.to_string()))?;
        let dest_config = doc.destination.config.resolve(base)?;
        let dest = provider
            .destination(&doc.destination.requirement(), &dest_config)
            .await
            .map_err(|e| Error::config(e.to_string()))?;
        builder.source(source).destination(dest).build()
    }

    /// Token for cancelling a running pipeline; safe at any instant (cancellation is
    /// recovered exactly like a crash).
    pub fn cancellation_token(&self) -> tokio_util::sync::CancellationToken {
        self.engine.cancellation_token()
    }

    /// Typed event stream (StreamStarted, BatchLoaded, SchemaEvolved, Committed, etc.).
    /// Subscribe before calling [`Pipeline::run`], which consumes the pipeline.
    pub fn events(&self) -> EventStream {
        self.engine.events()
    }

    /// Connectivity, discovery and plan checks without a load session:
    /// the source's and destination's `check` probes, stream discovery
    /// under the stream cap, then the same plan validation a run
    /// performs. No workdir, no WAL, no session, no reads — nothing is
    /// created or written anywhere. CONSUMES the pipeline like
    /// [`Pipeline::run`]; build the same pipeline again to run after a
    /// check.
    pub async fn check(self) -> Result<CheckSummary, Error> {
        self.engine.check().await
    }

    /// Run to completion, CONSUMING the pipeline. Resumable: after a crash or
    /// cancellation, build the same pipeline again and call `run` to continue from
    /// committed state. Taking `self` by value makes "run the same pipeline twice"
    /// a compile error instead of a runtime one — the engine is single-shot.
    ///
    /// ```compile_fail
    /// # async fn demo() {
    /// use rdlt::pipeline::Pipeline;
    /// use rdlt_testkit::memory;
    /// let pipeline = Pipeline::builder("p")
    ///     .source(memory::Source::default())
    ///     .destination(memory::Destination::new())
    ///     .build()
    ///     .unwrap();
    /// let _ = pipeline.run().await;
    /// let _ = pipeline.run().await; // moved by the first run — must NOT compile
    /// # }
    /// ```
    pub async fn run(self) -> Result<report::Run, Error> {
        self.engine.run().await
    }
}

#[cfg(test)]
mod workdir_tests {
    use super::*;

    fn document_from(yaml: &str) -> Document {
        document::parse(yaml).expect("the fixture document parses")
    }

    const BODY: &str = "source:\n  connector: {id: io.example.src, config: s.yaml}\n\
                        destination:\n  connector: {id: io.example.dst, config: {path: out.db}}\n";

    /// The default workdir is PER PIPELINE and lives beside the document:
    /// two defaulted documents in one directory get two workdirs, so
    /// neither can ever scan (or clear) the other's WAL.
    #[test]
    fn the_default_workdir_is_per_pipeline_under_the_document_dir() {
        let orders = document_from(&format!("pipeline: orders\n{BODY}"));
        let customers = document_from(&format!("pipeline: customers\n{BODY}"));
        let base = Path::new("/srv/specs");
        assert_eq!(
            resolved_workdir(&orders, base),
            Path::new("/srv/specs/.rdlt/orders")
        );
        assert_eq!(
            resolved_workdir(&customers, base),
            Path::new("/srv/specs/.rdlt/customers")
        );
    }

    /// A pipeline name is a free-form string; the workdir leaf derived
    /// from it is normalized, so separators and dot-segments cannot
    /// escape `.rdlt/`.
    #[test]
    fn the_default_workdir_leaf_is_path_sanitized() {
        let document = document_from(&format!("pipeline: \"../Orders / prod\"\n{BODY}"));
        let dir = resolved_workdir(&document, Path::new("/srv/specs"));
        let leaf = dir.file_name().expect("a leaf exists").to_string_lossy();
        assert_eq!(dir.parent(), Some(Path::new("/srv/specs/.rdlt")));
        assert!(
            !leaf.contains('/') && !leaf.contains(".."),
            "the leaf must not traverse: {leaf}"
        );
    }

    /// An explicit relative `workdir:` resolves against the document's
    /// directory — the include rule config paths already follow — so one
    /// document owns ONE workdir and one lock from any invoking CWD. An
    /// absolute spelling is taken as given.
    #[test]
    fn an_explicit_workdir_resolves_against_the_document_dir() {
        let relative = document_from(&format!("pipeline: p\nworkdir: state/wd\n{BODY}"));
        assert_eq!(
            resolved_workdir(&relative, Path::new("/srv/specs")),
            Path::new("/srv/specs/state/wd")
        );
        let absolute = document_from(&format!("pipeline: p\nworkdir: /var/lib/rdlt\n{BODY}"));
        assert_eq!(
            resolved_workdir(&absolute, Path::new("/srv/specs")),
            Path::new("/var/lib/rdlt")
        );
    }

    /// An empty base — a document built from a string with
    /// `Path::new("")` — stays working-directory-relative, exactly as
    /// documented for embedders.
    #[test]
    fn an_empty_base_keeps_the_default_cwd_relative() {
        let document = document_from(&format!("pipeline: p\n{BODY}"));
        assert_eq!(
            resolved_workdir(&document, Path::new("")),
            Path::new(".rdlt/p")
        );
    }
}

#[cfg(test)]
mod budget_tests {
    use super::*;

    /// The dial budget is the ENGINE CHANNEL's, never the batch policy's:
    /// `every_bytes` is destination-write cadence, and a document tuning
    /// its dest-flush size must not silently shrink the connector wire's
    /// h2 windows.
    #[test]
    fn the_dial_budget_ignores_batch_policy() {
        let document: Document = serde_yaml_ng::from_str(
            "pipeline: p\nbatch_policy: {every_bytes: 1048576}\n\
             source:\n  connector: {id: io.example.src, config: s.yaml}\n\
             destination:\n  connector: {id: io.example.dst, config: {path: out.db}}\n",
        )
        .expect("the fixture document parses");
        assert_eq!(
            engine_budget_bytes(&document),
            rdlt_engine::config::Config::DEFAULT_BYTE_BUDGET as u64,
            "a 1 MiB dest-write threshold must leave the wire budget at the engine default"
        );
    }

    /// `resources.byte_budget` IS the dial budget where spelled — the
    /// one number both the engine channel and the provider's wire
    /// windows ride — with the builder's own floor clamp (a spelled 0
    /// becomes 1, never "off"), and the engine default where absent.
    #[test]
    fn the_dial_budget_follows_the_documents_byte_budget() {
        let spelled: Document = serde_yaml_ng::from_str(
            "pipeline: p\nresources: {byte_budget: 1048576}\n\
             source:\n  connector: {id: io.example.src, config: s.yaml}\n\
             destination:\n  connector: {id: io.example.dst, config: {path: out.db}}\n",
        )
        .expect("the fixture document parses");
        assert_eq!(engine_budget_bytes(&spelled), 1_048_576);

        let zero: Document = serde_yaml_ng::from_str(
            "pipeline: p\nresources: {byte_budget: 0}\n\
             source:\n  connector: {id: io.example.src, config: s.yaml}\n\
             destination:\n  connector: {id: io.example.dst, config: {path: out.db}}\n",
        )
        .expect("the fixture document parses");
        assert_eq!(
            engine_budget_bytes(&zero),
            1,
            "zero clamps to the smallest enforceable window, like the builder"
        );
    }
}

#[cfg(test)]
mod tuning_tests {
    //! `apply_tuning` observed at the engine, not inspected: the
    //! document's knobs go through the REAL builder methods into a run
    //! whose behavior only the threaded value can explain.

    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use async_trait::async_trait;
    use rdlt_testkit::memory;
    use serde_json::json;

    use super::*;
    use crate::sdk::spi::error::SourceError;
    use crate::sdk::spi::source::{ReadRequest, Source, StreamSpec};
    use crate::sdk::spi::spec::ConnectorSpec;

    const BODY: &str = "source:\n  connector: {id: io.example.src, config: s.yaml}\n\
                        destination:\n  connector: {id: io.example.dst, config: {path: out.db}}\n";

    fn document_from(yaml: &str) -> Document {
        document::parse(yaml).expect("the fixture document parses")
    }

    /// Three streams whose reads overlap unless serialized, counting the
    /// concurrency they observe.
    struct Gauged {
        current: Arc<AtomicUsize>,
        peak: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl Source for Gauged {
        fn spec(&self) -> ConnectorSpec {
            ConnectorSpec::new("gauged", "0.0.0")
        }
        async fn check(&self) -> Result<(), SourceError> {
            Ok(())
        }
        async fn streams(&self) -> Result<Vec<StreamSpec>, SourceError> {
            Ok(vec![
                StreamSpec::new("a"),
                StreamSpec::new("b"),
                StreamSpec::new("c"),
            ])
        }
        async fn read(&self, _request: ReadRequest) -> Result<(), SourceError> {
            let now = self.current.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(now, Ordering::SeqCst);
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            self.current.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// End to end through the YAML: a document spelling
    /// `max_concurrent_streams: 1` serializes a three-stream read —
    /// only the threaded knob can produce a peak of exactly 1.
    #[tokio::test]
    async fn a_document_concurrency_knob_reaches_the_engine() {
        let document = document_from(&format!(
            "pipeline: p\nresources:\n  max_concurrent_streams: 1\n{BODY}"
        ));
        let peak = Arc::new(AtomicUsize::new(0));
        let pipeline = apply_tuning(Pipeline::builder("serialized"), &document)
            .source(Gauged {
                current: Arc::new(AtomicUsize::new(0)),
                peak: Arc::clone(&peak),
            })
            .destination(memory::Destination::new())
            .build()
            .expect("valid config");
        pipeline.run().await.expect("the run completes");
        assert_eq!(peak.load(Ordering::SeqCst), 1, "reads never overlapped");
    }

    /// `schema_policy: freeze` reaches the engine: a within-run drift
    /// the default (evolve) absorbs is refused typed under the
    /// document's scalar.
    #[tokio::test]
    async fn a_document_schema_policy_reaches_the_engine() {
        // Two BATCHES, the second wider: drift is judged across
        // batches, so one batch holding both rows would just infer the
        // union and never exercise the policy.
        let drifting = || {
            memory::Source::new(vec![memory::Stream::new(
                StreamSpec::new("events"),
                vec![
                    memory::Batch::new(vec![json!({"id": 1})]),
                    memory::Batch::new(vec![json!({"id": 2, "grown": true})]),
                ],
            )])
        };

        let frozen = document_from(&format!("pipeline: p\nschema_policy: freeze\n{BODY}"));
        let pipeline = apply_tuning(Pipeline::builder("frozen"), &frozen)
            .source(drifting())
            .destination(memory::Destination::new())
            .build()
            .expect("valid config");
        let error = pipeline.run().await.expect_err("freeze refuses the drift");
        assert!(
            matches!(error, Error::Schema(_)),
            "a frozen schema drift is the typed contract violation: {error:?}"
        );

        let evolving = document_from(&format!("pipeline: p\n{BODY}"));
        let pipeline = apply_tuning(Pipeline::builder("evolving"), &evolving)
            .source(drifting())
            .destination(memory::Destination::new())
            .build()
            .expect("valid config");
        pipeline
            .run()
            .await
            .expect("the absent scalar keeps the evolve default");
    }
}
