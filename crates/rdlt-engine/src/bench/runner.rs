//! What every bench workload loads through: a runtime of the workers its cores suggest, and an
//! engine within those cores loading its streams.

#[cfg(test)]
mod tests;

use std::num::{NonZeroU64, NonZeroUsize};
use std::sync::Arc;

use rdlt_connector::{Destination, PipelineId, Source};
use tokio::runtime::Runtime;

use crate::{
    CommitPolicy, ComputePoolError, Cores, Engine, EngineConfig, GrowthLimits, PipelinePlan,
    Report, RetryPolicy, RunOutcome, StreamPlan, SystemEnv, WalStore,
};

/// Where a runner's loads keep their logs, holding at most `bytes` on disk, and whether every
/// load keeps one, or only one with a stream whose source cannot read again.
struct Log {
    store: Arc<dyn WalStore>,
    bytes: NonZeroU64,
    always: bool,
}

/// A runtime, and an engine on it with the plan it loads.
#[derive(Debug)]
pub(super) struct Runner {
    runtime: Runtime,
    engine: Engine,
    plan: PipelinePlan,
}

impl Runner {
    /// Bytes: the memory budget a runner has unless it is given one.
    pub(super) const MEMORY: u64 = 1 << 30;

    /// A runtime of `cores`' workers, and an engine within `cores` of a budget of
    /// [`Runner::MEMORY`] that loads `stream` as the pipeline `pipeline`, committing every
    /// `commit` rows or once at the end.
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
        Self::build(cores, pipeline, vec![stream], commit, Self::MEMORY, None)
    }

    /// A runner as [`Runner::try_new`] makes it whose loads are each logged ahead into `store`,
    /// holding at most `log_bytes` on disk, one attempt a run, so a run whose commit fails leaves
    /// its log.
    pub(super) fn logging(
        cores: Cores,
        pipeline: &str,
        stream: StreamPlan,
        commit: Option<NonZeroU64>,
        (store, log_bytes): (Arc<dyn WalStore>, NonZeroU64),
    ) -> Result<Self, ComputePoolError> {
        let log = Log {
            store,
            bytes: log_bytes,
            always: true,
        };
        Self::build(
            cores,
            pipeline,
            vec![stream],
            commit,
            Self::MEMORY,
            Some(log),
        )
    }

    /// A runner as [`Runner::within`] makes it whose loads are logged as [`Runner::logging`]'s
    /// are, a load logging ahead only where a stream's source cannot read again.
    pub(super) fn mixed(
        cores: Cores,
        (pipeline, streams): (&str, Vec<StreamPlan>),
        commit: Option<NonZeroU64>,
        memory: u64,
        (store, log_bytes): (Arc<dyn WalStore>, NonZeroU64),
    ) -> Result<Self, ComputePoolError> {
        let log = Log {
            store,
            bytes: log_bytes,
            always: false,
        };
        Self::build(cores, pipeline, streams, commit, memory, Some(log))
    }

    /// A runtime and an engine as [`Runner::try_new`] makes them that load `streams`, within a
    /// budget of `memory` bytes.
    ///
    /// # Errors
    ///
    /// When the compute pool's threads do not start.
    ///
    /// # Panics
    ///
    /// Panics where the runtime does not start or `pipeline` is no valid id.
    pub(super) fn within(
        cores: Cores,
        pipeline: &str,
        streams: Vec<StreamPlan>,
        commit: Option<NonZeroU64>,
        memory: u64,
    ) -> Result<Self, ComputePoolError> {
        Self::build(cores, pipeline, streams, commit, memory, None)
    }

    /// A runtime of `cores`' workers, and an engine within `cores` configured as `config` that
    /// loads `plan`.
    ///
    /// # Errors
    ///
    /// When the compute pool's threads do not start.
    ///
    /// # Panics
    ///
    /// Panics where the runtime does not start.
    pub(super) fn configured(
        cores: Cores,
        config: EngineConfig,
        plan: PipelinePlan,
    ) -> Result<Self, ComputePoolError> {
        let env = SystemEnv::try_new(cores)?;
        Ok(Self::assemble(cores, config, env, plan))
    }

    fn build(
        cores: Cores,
        pipeline: &str,
        streams: Vec<StreamPlan>,
        commit: Option<NonZeroU64>,
        memory: u64,
        log: Option<Log>,
    ) -> Result<Self, ComputePoolError> {
        let policy = match commit {
            Some(rows) => CommitPolicy::new(None, Some(rows.get()), None),
            None => CommitPolicy::new(None, None, Some(1 << 40)),
        };
        let mut builder = EngineConfig::builder()
            .memory(memory)
            .commit(policy.expect("a valid policy"));
        if let Some(log) = &log {
            let growth = GrowthLimits::default().with_log_bytes(log.bytes.get());
            builder = builder
                .retry(RetryPolicy::default().max_attempts(1))
                .growth(growth.expect("a limit of more than zero"));
        }
        let config = builder.build().expect("a valid configuration");
        let pipeline = PipelineId::parse(pipeline).expect("a valid id");
        let mut env = SystemEnv::try_new(cores)?;
        let mut plan = PipelinePlan::new(pipeline, streams).expect("a valid plan");
        if let Some(log) = log {
            env = env.with_wal(log.store);
            plan = plan.with_wal(log.always);
        }
        Ok(Self::assemble(cores, config, env, plan))
    }

    /// A runtime of `cores`' workers, and an engine in `env` configured as `config` that loads
    /// `plan`.
    fn assemble(cores: Cores, config: EngineConfig, env: SystemEnv, plan: PipelinePlan) -> Self {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(cores.runtime_workers().get())
            .enable_all()
            .build()
            .expect("a runtime starts");
        Self {
            runtime,
            engine: Engine::new(config, Arc::new(env)),
            plan,
        }
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

    /// Runs the engine from `source` into `destination` while `beside` runs beside it, both
    /// started at once; the run's report and what `beside` returned.
    ///
    /// # Panics
    ///
    /// Panics where the run fails.
    pub(super) fn load_beside<F: Future>(
        &self,
        source: Arc<dyn Source>,
        destination: Arc<dyn Destination>,
        beside: F,
    ) -> (Report, F::Output) {
        let run = self.engine.run(self.plan.clone(), source, destination);
        let (outcome, beside) = self.runtime.block_on(async { tokio::join!(run, beside) });
        assert!(outcome.error.is_none(), "{:?}", outcome.error);
        (outcome.report, beside)
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

/// The stream, `events`, every bench source pushes first.
pub(super) fn stream() -> StreamPlan {
    StreamPlan::new(super::stream_name(0))
}

/// The `count` streams a replay of that many streams pushes: `events`, then `events_1` and on.
pub(super) fn streams(count: NonZeroUsize) -> Vec<StreamPlan> {
    (0..count.get())
        .map(|index| StreamPlan::new(super::stream_name(index)))
        .collect()
}
