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
    pub fn source<NS: Source>(self, source: NS) -> PipelineBuilder<NS, D> {
        PipelineBuilder {
            config: self.config,
            source,
            destination: self.destination,
        }
    }

    pub fn destination<ND: Destination>(self, destination: ND) -> PipelineBuilder<S, ND> {
        PipelineBuilder {
            config: self.config,
            source: self.source,
            destination,
        }
    }

    /// Default write disposition for every stream (default: `Append`).
    pub fn write_mode(mut self, mode: WriteMode) -> Self {
        self.config.write_mode = mode;
        self
    }

    /// Per-stream override.
    pub fn write_mode_for(mut self, stream: impl Into<StreamName>, mode: WriteMode) -> Self {
        self.config.write_modes.insert(stream.into(), mode);
        self
    }

    /// Schema-change policy (default: evolve).
    pub fn schema_policy(mut self, policy: SchemaPolicy) -> Self {
        self.config.schema_policy = policy;
        self
    }

    /// Commit grouping policy (default: every checkpoint).
    pub fn commit_policy(mut self, policy: CommitPolicy) -> Self {
        self.config.commit_policy = policy;
        self
    }

    /// Local work directory (holds the WAL). Default: `.rdlt`.
    pub fn workdir(mut self, dir: impl Into<PathBuf>) -> Self {
        self.config.workdir = Some(dir.into());
        self
    }

    /// Cap on in-flight bytes per stage channel — the memory bound.
    pub fn byte_budget(mut self, bytes: usize) -> Self {
        self.config.byte_budget = bytes;
        self
    }
}

impl<S: Source, D: Destination> PipelineBuilder<S, D> {
    /// Validate configuration against destination capabilities and construct the
    /// pipeline. No network or destination I/O happens here; the checks are purely
    /// against the declared destination capabilities.
    pub fn build(self) -> Result<Pipeline, RdltError> {
        let caps = self.destination.capabilities();
        let merge_requested = merge_streams(&self.config.write_mode, &self.config.write_modes);
        if !caps.merge && !merge_requested.is_empty() {
            return Err(RdltError::config(format!(
                "destination `{}` does not support Merge (requested {})",
                self.destination.spec().name,
                if merge_requested.contains(&None) {
                    "as the default write mode".to_owned()
                } else {
                    format!(
                        "for streams: {}",
                        merge_requested
                            .iter()
                            .flatten()
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
            )));
        }
        for (stream, mode) in &self.config.write_modes {
            if let WriteMode::Merge { key } = mode
                && key.is_empty()
            {
                return Err(RdltError::config(format!(
                    "stream `{stream}`: Merge requires at least one key column"
                )));
            }
        }
        if let WriteMode::Merge { key } = &self.config.write_mode
            && key.is_empty()
        {
            return Err(RdltError::config("Merge requires at least one key column"));
        }
        Ok(Pipeline {
            engine: Some(Engine::new(self.config, self.source, self.destination)),
        })
    }
}

/// Which streams (None = the default mode) request Merge.
fn merge_streams(
    default: &WriteMode,
    overrides: &BTreeMap<StreamName, WriteMode>,
) -> Vec<Option<StreamName>> {
    let mut requesting = Vec::new();
    if matches!(default, WriteMode::Merge { .. }) {
        requesting.push(None);
    }
    for (stream, mode) in overrides {
        if matches!(mode, WriteMode::Merge { .. }) {
            requesting.push(Some(stream.clone()));
        }
    }
    requesting
}

/// A configured, runnable pipeline.
#[derive(Debug)]
pub struct Pipeline {
    engine: Option<Engine>,
}

impl Pipeline {
    pub fn builder(name: impl Into<rdlt_core::PipelineId>) -> PipelineBuilder<Missing, Missing> {
        PipelineBuilder::new(name)
    }

    /// Token for cancelling a running pipeline; safe at any instant (cancellation is
    /// recovered exactly like a crash).
    pub fn cancellation_token(&self) -> Option<tokio_util::sync::CancellationToken> {
        self.engine.as_ref().map(Engine::cancellation_token)
    }

    /// Typed event stream (StreamStarted, BatchLoaded, SchemaEvolved, Committed, etc.).
    /// Subscribe before calling [`Pipeline::run`]; `None` once the pipeline ran.
    pub fn events(&self) -> Option<rdlt_engine::EventStream> {
        self.engine.as_ref().map(Engine::events)
    }

    /// Run to completion. Resumable: after a crash or cancellation, constructing the
    /// same pipeline again and calling `run` continues from committed state.
    pub async fn run(&mut self) -> Result<RunReport, RdltError> {
        let engine = self
            .engine
            .take()
            .ok_or_else(|| RdltError::config("this pipeline was already run; build a new one"))?;
        engine.run().await
    }
}
