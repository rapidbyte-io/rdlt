//! The engine's boundary: a typestate builder that takes a source VALUE
//! and a destination VALUE and hands back a runnable [`Pipeline`].
//!
//! Missing source or destination is a **compile** error; configuration
//! errors die in [`Builder::build`], before any I/O. In production the
//! values are the runtime's process adapters (what [`crate::document::build`]
//! supplies); hand-rolled SPI implementations are test doubles.

use std::path::PathBuf;

use crate::commit::{BatchPolicy, CommitPolicy, WriteMode};
use crate::error::Error;
use crate::event::EventStream;
use crate::id::{PipelineId, StreamName};
use crate::policy::SchemaPolicy;
use crate::report;
use crate::sdk::spi::destination::Destination;
use crate::sdk::spi::source::Source;
use rdlt_engine::{Engine, EngineConfig};

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
    config: EngineConfig,
    source: S,
    destination: D,
}

impl Builder<Missing, Missing> {
    pub(crate) fn new(pipeline: impl Into<PipelineId>) -> Self {
        Self {
            config: EngineConfig::new(pipeline),
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
    /// committed cursor — slower, never wrong. The pipeline-document
    /// path ([`crate::document`]) defaults it to `.rdlt/<pipeline>`
    /// beside the document; embedders building here choose their own.
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
