//! The typestate pipeline builder.
//!
//! Missing source or destination is a **compile** error; configuration errors die in
//! [`PipelineBuilder::build`], before any I/O.

use std::collections::BTreeMap;
use std::path::PathBuf;

use rdlt_connector::{Destination, Source, WriteMode};
use rdlt_core::{CommitPolicy, RdltError, RunReport, SchemaPolicy, StreamName};
use rdlt_engine::{Engine, EngineConfig};

/// Typestate marker: no source/destination provided yet.
#[derive(Debug)]
pub struct Missing;

/// Builder for [`Pipeline`]. `S`/`D` are typestate: `build` exists only once both a
/// source and a destination are set.
///
/// ```compile_fail
/// // B1: this must NOT compile — no destination was provided.
/// let _ = rdlt::Pipeline::builder("p")
///     .source(rdlt_testkit::MemorySource::default())
///     .build();
/// ```
#[derive(Debug)]
pub struct PipelineBuilder<S, D> {
    config: EngineConfig,
    source: S,
    destination: D,
}

impl PipelineBuilder<Missing, Missing> {
    pub(crate) fn new(pipeline: impl Into<rdlt_core::PipelineId>) -> Self {
        Self {
            config: EngineConfig::new(pipeline),
            source: Missing,
            destination: Missing,
        }
    }
}

impl<S, D> PipelineBuilder<S, D> {
    /// Set the source. Changes the builder's type, which is how a pipeline
    /// missing a source fails to compile rather than failing at run time.
    pub fn source<NS: Source>(self, source: NS) -> PipelineBuilder<NS, D> {
        PipelineBuilder {
            config: self.config,
            source,
            destination: self.destination,
        }
    }

    /// Set the destination. Changes the builder's type, so `build()` is only
    /// callable once both halves are present.
    pub fn destination<ND: Destination>(self, destination: ND) -> PipelineBuilder<S, ND> {
        PipelineBuilder {
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
    pub fn batch_policy(mut self, policy: rdlt_core::BatchPolicy) -> Self {
        self.config = self.config.with_batch_policy(policy);
        self
    }

    /// Commit grouping policy (default: every checkpoint).
    pub fn commit_policy(mut self, policy: CommitPolicy) -> Self {
        self.config = self.config.with_commit_policy(policy);
        self
    }

    /// Local work directory (holds the WAL). Default: `.rdlt`.
    pub fn workdir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.config = self.config.with_workdir(dir.into());
        self
    }

    /// Cap on in-flight bytes per stage channel — the memory bound.
    pub fn byte_budget(mut self, bytes: usize) -> Self {
        self.config = self.config.with_byte_budget(bytes);
        self
    }
}

impl<S: Source, D: Destination> PipelineBuilder<S, D> {
    /// Validate configuration against destination capabilities and construct the
    /// pipeline. No network or destination I/O happens here; the checks are purely
    /// against the declared destination capabilities.
    pub fn build(self) -> Result<Pipeline, RdltError> {
        let caps = self.destination.capabilities();
        let merge = merge_streams(self.config.write_mode(), self.config.write_modes());
        if !caps.merge && merge.any() {
            return Err(RdltError::config(format!(
                "destination `{}` does not support Merge (requested {})",
                self.destination.spec().name,
                if merge.default {
                    "as the default write mode".to_owned()
                } else {
                    format!(
                        "for streams: {}",
                        merge
                            .streams
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
                return Err(RdltError::config(format!(
                    "stream `{stream}`: Merge requires at least one key column"
                )));
            }
        }
        if let WriteMode::Merge { key } = self.config.write_mode()
            && key.is_empty()
        {
            return Err(RdltError::config("Merge requires at least one key column"));
        }
        Ok(Pipeline {
            engine: Engine::new(self.config, self.source, self.destination),
        })
    }
}

/// Which streams request Merge: the default write mode (`default`) and any
/// named per-stream overrides (`streams`). Replaces an earlier
/// `Vec<Option<StreamName>>` whose `None` element was an easy-to-miss sentinel
/// for "the default mode".
struct MergeRequests {
    default: bool,
    streams: Vec<StreamName>,
}

impl MergeRequests {
    /// Any Merge requested at all — default mode or a named stream.
    fn any(&self) -> bool {
        self.default || !self.streams.is_empty()
    }
}

fn merge_streams(
    default: &WriteMode,
    overrides: &BTreeMap<StreamName, WriteMode>,
) -> MergeRequests {
    MergeRequests {
        default: matches!(default, WriteMode::Merge { .. }),
        streams: overrides
            .iter()
            .filter(|(_, mode)| matches!(mode, WriteMode::Merge { .. }))
            .map(|(stream, _)| stream.clone())
            .collect(),
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
    pub fn builder(name: impl Into<rdlt_core::PipelineId>) -> PipelineBuilder<Missing, Missing> {
        PipelineBuilder::new(name)
    }

    /// Token for cancelling a running pipeline; safe at any instant (cancellation is
    /// recovered exactly like a crash).
    pub fn cancellation_token(&self) -> tokio_util::sync::CancellationToken {
        self.engine.cancellation_token()
    }

    /// Typed event stream (StreamStarted, BatchLoaded, SchemaEvolved, Committed, etc.).
    /// Subscribe before calling [`Pipeline::run`], which consumes the pipeline.
    pub fn events(&self) -> rdlt_engine::EventStream {
        self.engine.events()
    }

    /// Run to completion, CONSUMING the pipeline. Resumable: after a crash or
    /// cancellation, build the same pipeline again and call `run` to continue from
    /// committed state. Taking `self` by value makes "run the same pipeline twice"
    /// a compile error instead of a runtime one — the engine is single-shot.
    ///
    /// ```compile_fail
    /// # async fn demo() {
    /// use rdlt::Pipeline;
    /// use rdlt_testkit::{MemoryDestination, MemorySource};
    /// let pipeline = Pipeline::builder("p")
    ///     .source(MemorySource::default())
    ///     .destination(MemoryDestination::new())
    ///     .build()
    ///     .unwrap();
    /// let _ = pipeline.run().await;
    /// let _ = pipeline.run().await; // moved by the first run — must NOT compile
    /// # }
    /// ```
    pub async fn run(self) -> Result<RunReport, RdltError> {
        self.engine.run().await
    }
}
