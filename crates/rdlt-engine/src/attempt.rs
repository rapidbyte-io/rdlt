//! One attempt of a run: open the destination, plan the streams, and load until done.

mod streams;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use parking_lot::Mutex;
use rdlt_connector::{
    Cursor, Destination, Epoch, GenerationId, LoadId, OpenContext, OpenedSession, Partition,
    PipelineState, Source, StreamName,
};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::budget::MemoryBudget;
use crate::config::EngineConfig;
use crate::coordinator::{Coordinator, CoordinatorParts, PartitionRun, StreamRun};
use crate::env::Env;
use crate::error::{Error, ErrorKind, Side};
use crate::lane::Lanes;
use crate::naming::Naming;
use crate::partition::{self, PartitionContext, PartitionJob};
use crate::plan::PipelinePlan;
use crate::report::{AttemptEnd, AttemptLog};
use crate::scope::TaskScope;
use crate::table::{SharedSession, Tables};

use streams::Planning;

/// Everything a run's attempts share.
pub(crate) struct RunContext {
    pub(crate) env: Arc<dyn Env>,
    pub(crate) config: Arc<EngineConfig>,
    pub(crate) plan: Arc<PipelinePlan>,
    pub(crate) source: Arc<dyn Source>,
    pub(crate) destination: Arc<dyn Destination>,
    pub(crate) budget: MemoryBudget,
    /// Fires when the run is asked to stop after committing.
    pub(crate) stop: CancellationToken,
    /// The generation of each full read this run started or resumed, so a retry after the read
    /// completed does not read the stream again.
    pub(crate) cycles: Mutex<BTreeMap<StreamName, GenerationId>>,
}

/// A stream ready to load, and the partitions to read.
struct Planned {
    stream: StreamRun,
    on_demand: bool,
    partitions: Vec<(Partition, Option<Cursor>)>,
}

/// An open session and the state it returned.
struct Opened {
    session: Arc<SharedSession>,
    epoch: Epoch,
    state: PipelineState,
}

/// Runs one attempt under `load_id`, recording its commits in `log` as they land.
pub(crate) async fn run(
    context: &RunContext,
    load_id: LoadId,
    log: Arc<Mutex<AttemptLog>>,
) -> Result<AttemptEnd, Error> {
    let opened = open(context, load_id).await?;
    log.lock().opened = opened
        .state
        .last_receipt
        .as_ref()
        .map(|receipt| (receipt.load_id, receipt.commit_seq));
    let catalog = context
        .source
        .discover()
        .await
        .map_err(|error| Error::connector(Side::Source, "discovering the catalog", error))?;
    let capabilities = Arc::new(context.destination.capabilities().clone());
    let mut planning = Planning {
        context,
        catalog: &catalog,
        state: &opened.state,
        naming: Naming::new(capabilities.identifiers.clone()),
        capabilities,
    };
    let tables = Tables::new(Arc::clone(&opened.session)).committed(&opened.state);
    let mut planned = Vec::with_capacity(context.plan.streams().len());
    for plan in context.plan.streams() {
        planned.push(planning.stream(plan, &tables).await?);
    }
    launch(
        context,
        load_id,
        opened,
        planned,
        Arc::new(tables),
        Arc::clone(&log),
    )
    .await?;
    let end = log.lock().end;
    end.ok_or_else(|| Error::internal("the attempt ended without its coordinator finishing"))
}

/// Opens the destination, which fences older sessions, and decodes the committed state.
///
/// The epoch comes from the open itself: destinations keep their live epoch outside the state
/// records.
async fn open(context: &RunContext, load_id: LoadId) -> Result<Opened, Error> {
    let open = OpenContext {
        pipeline: context.plan.pipeline().clone(),
        load_id,
    };
    let OpenedSession {
        session,
        epoch,
        state,
    } =
        context.destination.open(&open).await.map_err(|error| {
            Error::connector(Side::Destination, "opening the destination", error)
        })?;
    let state = PipelineState::from_records(&state).map_err(|error| {
        Error::new(
            ErrorKind::Destination,
            format!("reading pipeline state: {error}"),
        )
        .with_code("state_invalid")
    })?;
    Ok(Opened {
        session: SharedSession::new(session),
        epoch,
        state,
    })
}

/// Runs the lanes, the partitions and the coordinator in one scope until all of them end.
async fn launch(
    context: &RunContext,
    load_id: LoadId,
    opened: Opened,
    planned: Vec<Planned>,
    tables: Arc<Tables>,
    log: Arc<Mutex<AttemptLog>>,
) -> Result<(), Error> {
    let count = lane_count(&context.config, context.destination.as_ref());
    let (lanes, lane_tasks) = Lanes::new(count, &tables, context.config.lane_window());
    let mut scope = TaskScope::new(&CancellationToken::new());
    let cancel = scope.token().clone();
    for lane in lane_tasks {
        scope.spawn(lane.run(cancel.clone()));
    }
    let (progress, progress_feed) = mpsc::unbounded_channel();
    let (barrier, barrier_feed) = watch::channel(0);
    let stop_reads = CancellationToken::new();
    let partition_context = PartitionContext {
        source: Arc::clone(&context.source),
        lanes: lanes.clone(),
        tables: Arc::clone(&tables),
        budget: context.budget.clone(),
        progress,
        barrier: barrier_feed,
        stop: stop_reads.clone(),
        cancel: cancel.clone(),
        slots: Arc::new(Semaphore::new(context.config.partitions().get())),
        segments: Arc::new(AtomicU64::new(1)),
        buffer: context.config.partition_buffer(),
        load_id,
        loaded_at: context.env.now(),
        env: Arc::clone(&context.env),
        batch: *context.config.batch(),
    };
    // Only the partitions may keep the progress channel open, so the coordinator sees them end.
    let (streams, partitions) = spawn_partitions(&mut scope, planned, partition_context);
    let coordinator = Coordinator::new(CoordinatorParts {
        env: Arc::clone(&context.env),
        policy: *context.config.commit(),
        barrier_wait: context.config.barrier_wait(),
        tables,
        source: Arc::clone(&context.source),
        lanes,
        load_id,
        epoch: opened.epoch,
        streams,
        partitions,
        progress: progress_feed,
        barrier,
        stop_reads,
        stop: context.stop.clone(),
        cancel,
        log,
    });
    scope.spawn(coordinator.run());
    scope.join().await
}

/// Starts a task per partition to read, and returns the streams and partitions as the
/// coordinator tracks them.
fn spawn_partitions(
    scope: &mut TaskScope<Error>,
    planned: Vec<Planned>,
    context: PartitionContext,
) -> (Vec<StreamRun>, Vec<PartitionRun>) {
    let mut streams = Vec::with_capacity(planned.len());
    let mut partitions = Vec::new();
    for (index, stream) in planned.into_iter().enumerate() {
        for (partition, cursor) in stream.partitions {
            let id = partition.id().clone();
            let job = PartitionJob {
                index: partitions.len(),
                stream: stream.stream.name.clone(),
                table: stream.stream.table,
                partition,
                cursor,
                on_demand: stream.on_demand,
            };
            partitions.push(PartitionRun::new(index, id, stream.on_demand));
            scope.spawn(partition::run(job, context.clone()));
        }
        streams.push(stream.stream);
    }
    // The context's progress sender must not outlive the partitions.
    drop(context);
    (streams, partitions)
}

/// The configured lanes, or one per core, never more than the destination's writers.
fn lane_count(config: &EngineConfig, destination: &dyn Destination) -> NonZeroUsize {
    let limit = usize::from(destination.capabilities().max_parallel_writers.get());
    let lanes = config.lanes().map_or_else(
        || std::thread::available_parallelism().map_or(1, NonZeroUsize::get),
        |lanes| usize::from(lanes.get()),
    );
    NonZeroUsize::new(lanes.min(limit)).unwrap_or(NonZeroUsize::MIN)
}
