//! What every bench workload loads through: a runtime of the workers its cores suggest, and an
//! engine within those cores loading one stream.

#[cfg(test)]
mod tests;

use std::num::NonZeroU64;
use std::sync::Arc;

use rdlt_connector::{Destination, PipelineId, Source, StreamName};
use tokio::runtime::Runtime;

use crate::{
    CommitPolicy, ComputePoolError, Cores, Engine, EngineConfig, PipelinePlan, Report, RetryPolicy,
    RunOutcome, StreamPlan, SystemEnv, WalStore,
};

/// A runtime, and an engine on it with the plan it loads.
#[derive(Debug)]
pub(super) struct Runner {
    runtime: Runtime,
    engine: Engine,
    plan: PipelinePlan,
}

impl Runner {
    /// A runtime of `cores`' workers, and an engine within `cores` of a budget of 1 GiB that
    /// loads `stream` as the pipeline `pipeline`, committing every `commit` rows or once at the
    /// end.
    ///
    /// # Errors
    ///
    /// When the compute pool's threads do not start.
    ///
    /// # Panics
    ///
    /// Panics where the runtime does not start or `pipeline` is no valid id.
    pub(super) fn try_new(
        cores: Cores,
        pipeline: &str,
        stream: StreamPlan,
        commit: Option<NonZeroU64>,
    ) -> Result<Self, ComputePoolError> {
        Self::build(cores, pipeline, stream, commit, None)
    }

    /// A runner as [`Runner::try_new`] makes it whose loads are each logged ahead into `store`,
    /// one attempt a run, so a run whose commit fails leaves its log.
    pub(super) fn logging(
        cores: Cores,
        pipeline: &str,
        stream: StreamPlan,
        commit: Option<NonZeroU64>,
        store: Arc<dyn WalStore>,
    ) -> Result<Self, ComputePoolError> {
        Self::build(cores, pipeline, stream, commit, Some(store))
    }

    fn build(
        cores: Cores,
        pipeline: &str,
        stream: StreamPlan,
        commit: Option<NonZeroU64>,
        log: Option<Arc<dyn WalStore>>,
    ) -> Result<Self, ComputePoolError> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(cores.workers().get())
            .enable_all()
            .build()
            .expect("a runtime starts");
        let policy = match commit {
            Some(rows) => CommitPolicy::new(None, Some(rows.get()), None),
            None => CommitPolicy::new(None, None, Some(1 << 40)),
        };
        let mut builder = EngineConfig::builder()
            .memory(1 << 30)
            .commit(policy.expect("a valid policy"));
        if log.is_some() {
            builder = builder.retry(RetryPolicy::default().max_attempts(1));
        }
        let config = builder.build().expect("a valid configuration");
        let pipeline = PipelineId::parse(pipeline).expect("a valid id");
        let mut env = SystemEnv::try_new(cores)?;
        let mut plan = PipelinePlan::new(pipeline, [stream]).expect("a valid plan");
        if let Some(store) = log {
            env = env.with_wal(store);
            plan = plan.with_wal(true);
        }
        Ok(Self {
            runtime,
            engine: Engine::new(config, Arc::new(env)),
            plan,
        })
    }

    /// Runs `future` on the runtime, where connectors served from this process run.
    pub(super) fn block_on<F: Future>(&self, future: F) -> F::Output {
        self.runtime.block_on(future)
    }

    /// Runs the engine from `source` into `destination`; its report.
    ///
    /// # Panics
    ///
    /// Panics where the run fails.
    pub(super) fn load(
        &self,
        source: Arc<dyn Source>,
        destination: Arc<dyn Destination>,
    ) -> Report {
        let outcome = self.outcome(source, destination);
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        outcome.report
    }

    /// Runs the engine from `source` into `destination`; its outcome, failed or not.
    pub(super) fn outcome(
        &self,
        source: Arc<dyn Source>,
        destination: Arc<dyn Destination>,
    ) -> RunOutcome {
        self.runtime
            .block_on(self.engine.run(self.plan.clone(), source, destination))
    }
}

/// The stream, `events`, every bench source pushes.
pub(super) fn stream() -> StreamPlan {
    StreamPlan::new(StreamName::new("events").expect("a valid name"))
}
