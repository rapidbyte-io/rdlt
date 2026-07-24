//! # rdlt-engine — the deep module
//!
//! Shred → schema → WAL → load, orchestrated over byte-bounded channels. Everything
//! below `lib.rs` is `pub(crate)`: unit-tested privately, free to churn without semver
//! cost. The public surface is [`Engine`]/[`EngineConfig`] plus the vocabulary
//! re-exported from [`rdlt_core`].

#[doc(hidden)]
pub mod fuzzing;
mod load;
mod runtime;
mod schema;
mod shred;
mod wal;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use rdlt_connector::{Destination, Source};
use rdlt_core::{CommitPolicy, PipelineId, SchemaPolicy, StreamName, WriteMode};
pub use rdlt_core::{PipelineEvent, RdltError, RunReport};
use tokio_util::sync::CancellationToken;

/// Engine configuration. The facade (`rdlt` crate) fronts this with a typestate
/// builder; embedders using the engine directly fill it in plainly.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub pipeline: PipelineId,
    /// Default write disposition for every stream.
    pub write_mode: WriteMode,
    /// Per-stream overrides.
    pub write_modes: BTreeMap<StreamName, WriteMode>,
    pub schema_policy: SchemaPolicy,
    pub commit_policy: CommitPolicy,
    /// Working directory that holds the WAL. `None` disables durable recovery.
    pub workdir: Option<PathBuf>,
    /// Bound on in-flight bytes per stage channel — this is the RSS cap.
    pub byte_budget: usize,
}

impl EngineConfig {
    pub fn new(pipeline: impl Into<PipelineId>) -> Self {
        Self {
            pipeline: pipeline.into(),
            write_mode: WriteMode::Append,
            write_modes: BTreeMap::new(),
            schema_policy: SchemaPolicy::evolve(),
            commit_policy: CommitPolicy::default(),
            workdir: None,
            byte_budget: 64 << 20,
        }
    }

    pub(crate) fn mode_for(&self, stream: &StreamName) -> WriteMode {
        self.write_modes
            .get(stream)
            .cloned()
            .unwrap_or_else(|| self.write_mode.clone())
    }
}

/// One pipeline execution: consumes itself on [`Engine::run`].
pub struct Engine {
    config: EngineConfig,
    source: Arc<dyn Source>,
    destination: Arc<dyn Destination>,
    cancel: CancellationToken,
    events: tokio::sync::broadcast::Sender<PipelineEvent>,
}

/// Ring-buffer depth of the observability broadcast channel. Large enough that a
/// briefly-stalled subscriber rarely lags out; a subscriber slower than this loses
/// oldest events (by design — observability never backpressures data movement).
const EVENT_CHANNEL_CAPACITY: usize = 4096;

/// Typed observability stream (embedder-api.md O1): every [`PipelineEvent`] in causal
/// order. Slow consumers lose oldest events rather than backpressuring the pipeline —
/// observability must never slow data movement.
#[derive(Debug)]
pub struct EventStream {
    rx: tokio::sync::broadcast::Receiver<PipelineEvent>,
}

impl EventStream {
    /// `None` once the run finished and all events were consumed.
    pub async fn recv(&mut self) -> Option<PipelineEvent> {
        use tokio::sync::broadcast::error::RecvError;
        loop {
            match self.rx.recv().await {
                Ok(event) => return Some(event),
                Err(RecvError::Lagged(_)) => continue,
                Err(RecvError::Closed) => return None,
            }
        }
    }
}

impl std::fmt::Debug for Engine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Engine")
            .field("pipeline", &self.config.pipeline)
            .finish_non_exhaustive()
    }
}

impl Engine {
    pub fn new(config: EngineConfig, source: impl Source, destination: impl Destination) -> Self {
        Self {
            config,
            source: Arc::new(source),
            destination: Arc::new(destination),
            cancel: CancellationToken::new(),
            events: tokio::sync::broadcast::channel(EVENT_CHANNEL_CAPACITY).0,
        }
    }

    /// Subscribe to the typed event stream. Call before [`Engine::run`]; multiple
    /// subscribers each see every event.
    pub fn events(&self) -> EventStream {
        EventStream {
            rx: self.events.subscribe(),
        }
    }

    /// Cancelling is safe at any instant and equivalent to a crash: the next run
    /// recovers identically.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancel.clone()
    }

    /// Run to completion (resumable, cancel-safe). Consumes the engine — a run is not
    /// restartable in place; construct a new one to run again.
    pub async fn run(self) -> Result<RunReport, RdltError> {
        runtime::graph::run(
            self.config,
            self.source,
            self.destination,
            self.cancel,
            self.events,
        )
        .await
    }
}
