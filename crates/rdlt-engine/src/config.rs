use std::collections::BTreeMap;
use std::path::PathBuf;

use rdlt_core::{CommitPolicy, PipelineId, SchemaPolicy, StreamName, WriteMode};

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
