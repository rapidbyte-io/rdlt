use std::{collections::BTreeMap, path::PathBuf};

use rdlt_core::{CommitPolicy, PipelineId, SchemaPolicy, StreamName, WriteMode};

/// Default bound on in-flight bytes per stage channel: 64 MiB. This is the engine's
/// resident-memory cap; a producer that would exceed it parks until the consumer
/// drains.
///
/// Public because it is THE engine default other layers must agree with:
/// the facade threads it into the connector provider's dial windows when a
/// pipeline document sets no byte budget of its own — one constant, never a
/// second literal that could drift.
pub const DEFAULT_BYTE_BUDGET: usize = 64 << 20;

/// The default bound on batch-assembly CELLS — `columns × rows` per
/// outgoing batch (5M3, made configurable): assembly null-fills absent
/// columns and stamps lineage per cell, so the product bounds the
/// engine-side expansion transient independently of the input's encoded
/// size. 2²⁸ cells ≈ 1 GiB of null-fill worst case. Honest maximal
/// pipelines (≤ ~260 columns at the 1M-row cap) sit under it; wider
/// shapes raise it here.
pub const DEFAULT_MAX_BATCH_CELLS: usize = 1 << 28;

/// The default bound on streams one source may declare for a run (5L9,
/// made configurable): every stream costs plan-time validation and its
/// own shred state, and discovery's list length is the one axis a source
/// controls directly. 1,024 is far past every honest discovery; a
/// pipeline that genuinely reads more raises it here.
pub const DEFAULT_MAX_STREAMS_PER_SOURCE: usize = 1024;

/// Configuration for an [`Engine`](crate::Engine).
///
/// [`EngineConfig::new`] provides working defaults for everything except the pipeline
/// id; the `with_*` methods override them. The facade (`rdlt` crate) fronts this with
/// a typestate builder; embedders using the engine directly construct it here.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub(crate) pipeline: PipelineId,
    pub(crate) write_mode: WriteMode,
    pub(crate) write_modes: BTreeMap<StreamName, WriteMode>,
    pub(crate) schema_policy: SchemaPolicy,
    pub(crate) commit_policy: CommitPolicy,
    pub(crate) batch_policy: rdlt_core::BatchPolicy,
    pub(crate) workdir: Option<PathBuf>,
    pub(crate) byte_budget: usize,
    pub(crate) max_batch_cells: usize,
    pub(crate) max_streams_per_source: usize,
}

impl EngineConfig {
    pub fn new(pipeline: impl Into<PipelineId>) -> Self {
        Self {
            pipeline: pipeline.into(),
            write_mode: WriteMode::Append,
            write_modes: BTreeMap::new(),
            schema_policy: SchemaPolicy::evolve(),
            commit_policy: CommitPolicy::default(),
            batch_policy: rdlt_core::BatchPolicy::default(),
            workdir: None,
            byte_budget: DEFAULT_BYTE_BUDGET,
            max_batch_cells: DEFAULT_MAX_BATCH_CELLS,
            max_streams_per_source: DEFAULT_MAX_STREAMS_PER_SOURCE,
        }
    }

    /// Sets the default write disposition for every stream.
    ///
    /// Streams named in [`EngineConfig::with_write_mode_for`] keep their own mode.
    pub fn with_write_mode(mut self, mode: WriteMode) -> Self {
        self.write_mode = mode;
        self
    }

    /// Overrides the write disposition for one stream.
    pub fn with_write_mode_for(mut self, stream: impl Into<StreamName>, mode: WriteMode) -> Self {
        self.write_modes.insert(stream.into(), mode);
        self
    }

    /// Sets the policy applied when a stream's observed schema drifts from the
    /// registered one.
    pub fn with_schema_policy(mut self, policy: SchemaPolicy) -> Self {
        self.schema_policy = policy;
        self
    }

    /// Sets when the engine commits: per checkpoint batch, or at run end.
    /// How much to accumulate before each destination write.
    ///
    /// Destination-agnostic: the engine does the accumulating, so
    /// every connector benefits without re-solving it. The default
    /// writes each source batch straight through.
    pub fn with_batch_policy(mut self, policy: rdlt_core::BatchPolicy) -> Self {
        self.batch_policy = policy;
        self
    }

    pub fn with_commit_policy(mut self, policy: CommitPolicy) -> Self {
        self.commit_policy = policy;
        self
    }

    /// The configured commit policy (7L10): the facade's `build()`
    /// runs `CommitPolicy::check` on it — the same refusal the YAML
    /// parse makes — so a threshold-less policy cannot reach a run
    /// through the builder either.
    pub fn commit_policy(&self) -> &CommitPolicy {
        &self.commit_policy
    }

    /// Sets the working directory that holds the write-ahead log.
    ///
    /// Without a workdir the engine runs without durable recovery: a crash loses
    /// staged-but-uncommitted work instead of replaying it.
    pub fn with_workdir(mut self, workdir: impl Into<PathBuf>) -> Self {
        self.workdir = Some(workdir.into());
        self
    }

    /// Sets the bound on in-flight bytes per stage channel — the resident-memory cap.
    pub fn with_byte_budget(mut self, bytes: usize) -> Self {
        // Zero used to disable every semaphore-backed byte bound. A caller
        // asking for no buffering instead receives the smallest enforceable
        // window; the memory ceiling is never silently switched off.
        self.byte_budget = bytes.max(1);
        self
    }

    /// Sets the batch-assembly cell budget (`columns × rows`, see
    /// [`DEFAULT_MAX_BATCH_CELLS`]). Raise it for honestly wide × large
    /// batches; lowering tightens the engine-side expansion ceiling. Zero
    /// clamps to one cell, never off — the budget is what refuses the
    /// 16 GiB null-fill amplification.
    pub fn with_max_batch_cells(mut self, cells: usize) -> Self {
        self.max_batch_cells = cells.max(1);
        self
    }

    /// Sets the most streams one source may declare for a run (see
    /// [`DEFAULT_MAX_STREAMS_PER_SOURCE`]). Raise it for genuinely huge
    /// discoveries; the bound exists because the stream list is the one
    /// discovery axis a source controls directly.
    pub fn with_max_streams_per_source(mut self, streams: usize) -> Self {
        self.max_streams_per_source = streams.max(1);
        self
    }

    /// Returns the default write disposition.
    pub fn write_mode(&self) -> &WriteMode {
        &self.write_mode
    }

    /// Returns the per-stream write-mode overrides.
    pub fn write_modes(&self) -> &BTreeMap<StreamName, WriteMode> {
        &self.write_modes
    }

    pub(crate) fn write_mode_for(&self, stream: &StreamName) -> WriteMode {
        self.write_modes
            .get(stream)
            .cloned()
            .unwrap_or_else(|| self.write_mode.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_zero_byte_budget_clamps_to_the_smallest_enforceable_window() {
        assert_eq!(EngineConfig::new("p").with_byte_budget(0).byte_budget, 1);
    }
}
