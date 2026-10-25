//! One attempt of a run: open the destination, plan the streams, and load until done.

mod check;
mod lanes;
mod sequences;
mod streams;
#[cfg(test)]
mod tests;
mod together;

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use parking_lot::Mutex;
use rdlt_connector::{
    Cursor, Destination, Epoch, GenerationId, LoadId, OpenContext, OpenedSession, Partition,
    PipelineState, Source, StreamName,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::budget::MemoryBudget;
use crate::compute::Pool;
use crate::config::EngineConfig;
use crate::coordinator::{
    Coordinator, CoordinatorParts, DoneMarkers, PartitionRun, StreamRun, launcher,
};
use crate::env::Env;
use crate::error::{Error, ErrorKind, Side, codes};
use crate::lane::Lanes;
use crate::limits::LEAST_CALLS;
use crate::naming::{Naming, recorded};
use crate::partition::{
    self, ChangeMode, Latest, LoadClock, PartitionContext, PartitionJob, Progress, Slots,
};
use crate::plan::PipelinePlan;
use crate::report::{AttemptEnd, AttemptLog, Tally};
use crate::scope::TaskScope;
use crate::stored::{StateLimits, Stored};
use crate::table::{SharedSession, Tables};
use crate::watch;

use lanes::start_lanes;
pub(crate) use sequences::Keying;
use streams::Planning;

/// Everything a run's attempts share.
pub(crate) struct RunContext {
    pub(crate) env: Arc<dyn Env>,
    pub(crate) config: Arc<EngineConfig>,
    pub(crate) plan: Arc<PipelinePlan>,
    pub(crate) source: Arc<dyn Source>,
    pub(crate) destination: Arc<dyn Destination>,
    pub(crate) budget: MemoryBudget,
    /// Where the run's work counts what it waits for and spends its time on.
    pub(crate) tally: Arc<Tally>,
    /// Fires when the run is asked to stop after committing.
    pub(crate) stop: CancellationToken,
    /// The generation of each full read this run started or resumed, so a retry after the read
    /// completed does not read the stream again, with the epoch of the stream's last reset when
    /// the read began: a reset since ends the read, and a retry starts a new one.
    pub(crate) cycles: Mutex<BTreeMap<StreamName, (GenerationId, Option<Epoch>)>>,
}

/// A stream ready to load, and the partitions to read.
struct Planned {
    stream: StreamRun,
    on_demand: bool,
    /// How a change stream's pushes load; `None` for other streams.
    changes: Option<ChangeMode>,
    /// Whether its reads follow their unbounded partitions: in a following run, for a stream it
    /// plans again as it reads.
    follow: bool,
    /// Whether its partitions read again from their source's earliest after a retention loss.
    reset_retention: bool,
    partitions: Vec<(Partition, Option<Cursor>)>,
}

/// An open session and the state it returned.
struct Opened {
    session: Arc<SharedSession>,
    epoch: Epoch,
    state: PipelineState,
    /// What the state's records take as an open's answer carries them.
    stored: Stored,
}

/// Runs one attempt under `load_id`, recording its commits in `log` as they land.
pub(crate) async fn run(
    context: &RunContext,
    load_id: LoadId,
    log: Arc<Mutex<AttemptLog>>,
) -> Result<AttemptEnd, Error> {
    within_calls(context)?;
    let opened = open(context, load_id).await?;
    let session = Arc::clone(&opened.session);
    let ran = opened_run(context, load_id, opened, &log).await;
    if ran.is_err() {
        // A failed attempt's session releases what it holds now; the failure matters more than
        // any error from closing, and a session the coordinator closed stays closed.
        closed_within(context, session.close()).await;
    }
    ran
}

/// Refuses an attempt whose source or destination carries too few calls at once for a read or a
/// writer beside the engine's other call into it, before it opens the destination.
///
/// # Errors
///
/// A `Config` error coded `calls_too_few`, naming the connector, the calls it carries and those a
/// run needs.
fn within_calls(context: &RunContext) -> Result<(), Error> {
    let sides = [
        ("source", context.source.max_calls(), "a read"),
        ("destination", context.destination.max_calls(), "a writer"),
    ];
    for (side, calls, held) in sides {
        let Some(calls) = calls.filter(|calls| calls.get() < LEAST_CALLS) else {
            continue;
        };
        return Err(Error::config(format!(
            "the {side} carries {calls} at once, fewer calls than the {LEAST_CALLS} a run needs: \
             {held} and the engine's other call beside it"
        ))
        .with_code(codes::CALLS_TOO_FEW));
    }
    Ok(())
}

/// Waits for `closing`, a close after a failure, no longer than the close wait: the failure is
/// what the run reports, and the next attempt should not wait on a destination that is stuck.
async fn closed_within<T>(context: &RunContext, closing: impl Future<Output = T>) {
    tokio::select! {
        biased;
        _ = closing => {}
        () = context.env.sleep(context.config.close_wait()) => {}
    }
}

/// Runs the attempt as [`run`] does, on the session it opened.
async fn opened_run(
    context: &RunContext,
    load_id: LoadId,
    opened: Opened,
    log: &Arc<Mutex<AttemptLog>>,
) -> Result<AttemptEnd, Error> {
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
    let naming = Naming::checked(&capabilities.identifiers)?;
    recorded::check(&naming, &opened.state)?;
    let mut planning = Planning {
        context,
        catalog: &catalog,
        state: &opened.state,
        naming,
        capabilities,
    };
    let tables = Tables::new(Arc::clone(&opened.session))
        .salted(load_id)
        .growing(context.config.child_table_limit())
        .committed(&opened.state)?;
    let planned = planning.streams(context.plan.streams(), &tables).await?;
    let partitions = planned.iter().map(|stream| stream.partitions.len()).sum();
    within_partition_limit(partitions)?;
    launch(
        context,
        load_id,
        opened,
        planned,
        Arc::new(tables),
        Arc::clone(log),
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
    let stored = Stored::of(&state, StateLimits::of(&context.config));
    let state = match PipelineState::from_records(&state) {
        Ok(state) => state,
        Err(error) => {
            closed_within(context, session.close()).await;
            return Err(Error::new(
                ErrorKind::Destination,
                format!("reading pipeline state: {error}"),
            )
            .with_code(codes::STATE_INVALID));
        }
    };
    Ok(Opened {
        session: SharedSession::new(session),
        epoch,
        state,
        stored,
    })
}

/// Refuses an attempt reading more than `partitions` at once: each is a task and bookkeeping,
/// whichever stream it reads, so the limit on a plan's partitions holds for all of them.
///
/// # Errors
///
/// `plan_invalid`, a Source error no retry mends, beyond the limit.
pub(crate) fn within_partition_limit(partitions: usize) -> Result<(), Error> {
    use rdlt_connector::limits::MAX_PLAN_PARTITIONS;
    if partitions <= MAX_PLAN_PARTITIONS {
        return Ok(());
    }
    Err(Error::new(
        ErrorKind::Source,
        format!(
            "the streams read {partitions} partitions at once, beyond the limit of \
             {MAX_PLAN_PARTITIONS}"
        ),
    )
    .with_code(codes::PLAN_INVALID))
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
    let mut scope = TaskScope::new(&CancellationToken::new());
    let lanes = start_lanes(context, &tables, &mut scope);
    let ((progress, progress_feed), (barrier, barrier_feed)) =
        (mpsc::unbounded_channel(), watch::channel(0));
    let (stop_reads, latest) = (CancellationToken::new(), Arc::new(Latest::default()));
    let ends = Ends {
        progress,
        latest: Arc::clone(&latest),
        barrier: barrier_feed,
        stop: stop_reads.clone(),
        cancel: scope.token().clone(),
    };
    let partition_context =
        shared_by_partitions(context, (load_id, opened.epoch), (&lanes, &tables), ends);
    let (tasks, spawned) = mpsc::unbounded_channel();
    let (streams, partitions) = spawn_partitions(&mut scope, planned, &partition_context);
    let coordinator = Coordinator::new(CoordinatorParts {
        env: Arc::clone(&context.env),
        policy: context.config.commit_for(context.plan.commits_as_stream()),
        barrier_wait: context.config.barrier_wait(),
        tables,
        source: Arc::clone(&context.source),
        lanes,
        load_id,
        epoch: opened.epoch,
        streams,
        partitions,
        progress: progress_feed,
        latest,
        budget: context.budget.clone(),
        barrier,
        stop_reads,
        stop: context.stop.clone(),
        cancel: scope.token().clone(),
        log,
        launcher: launcher(partition_context, tasks),
        done: DoneMarkers::from(&opened.state),
        stored: opened.stored,
        follow: context.plan.until().follows(),
        replan: context.config.replan(),
        tally: Arc::clone(&context.tally),
    });
    scope.spawn(coordinator.run());
    scope.join_spawning(spawned).await
}

/// The partitions' ends of what ties them to the attempt's coordinator.
struct Ends {
    progress: mpsc::UnboundedSender<Progress>,
    latest: Arc<Latest>,
    barrier: watch::Receiver<u64>,
    stop: CancellationToken,
    cancel: CancellationToken,
}

/// What the attempt's partitions share: its load and its session's epoch, its `lanes` and
/// `tables`, and their `ends`.
fn shared_by_partitions(
    context: &RunContext,
    (load_id, epoch): (LoadId, Epoch),
    (lanes, tables): (&Lanes, &Arc<Tables>),
    ends: Ends,
) -> Arc<PartitionContext> {
    Arc::new(PartitionContext {
        source: Arc::clone(&context.source),
        lanes: lanes.clone(),
        tables: Arc::clone(tables),
        budget: context.budget.clone(),
        rendering: Arc::new(crate::cost::rendering(context.destination.capabilities())),
        progress: ends.progress,
        latest: ends.latest,
        barrier: ends.barrier,
        stop: ends.stop,
        cancel: ends.cancel,
        slots: Slots::new(context.config.partitions(), context.source.max_calls()),
        segments: Arc::new(AtomicU64::new(1)),
        buffer: context.config.partition_buffer(),
        load_id,
        epoch,
        clock: Arc::new(LoadClock::new(context.env.now())),
        env: Arc::clone(&context.env),
        pool: Pool::new(Arc::clone(&context.env), Arc::clone(&context.tally)),
        batch: *context.config.batch(),
        stop_wait: context.config.stop_wait(),
    })
}

/// Starts a task per partition to read, and returns the streams and partitions as the
/// coordinator tracks them.
///
/// The context goes on to the launcher, so only the partitions and the launcher keep the lanes
/// and the progress channel open.
fn spawn_partitions(
    scope: &mut TaskScope<Error>,
    planned: Vec<Planned>,
    context: &Arc<PartitionContext>,
) -> (Vec<StreamRun>, Vec<PartitionRun>) {
    let mut streams = Vec::with_capacity(planned.len());
    let mut partitions = Vec::new();
    for (index, mut stream) in planned.into_iter().enumerate() {
        for (partition, cursor) in stream.partitions {
            let id = partition.id().clone();
            let job = PartitionJob {
                index: partitions.len(),
                stream: stream.stream.name.clone(),
                table: stream.stream.table,
                partition,
                cursor,
                on_demand: stream.on_demand,
                changes: stream.changes,
                stop: context.stop.child_token(),
                follow: stream.follow,
                reset_retention: stream.reset_retention,
            };
            if let Some(phases) = stream.stream.phases.as_mut() {
                phases.reading.insert(id.clone(), partitions.len());
            }
            let stop = job.stop.clone();
            let tracked = PartitionRun::new(index, id, stream.on_demand, stop);
            partitions.push(tracked.starting(job.cursor.as_ref()));
            scope.spawn(partition::run(job, Arc::clone(context)));
        }
        streams.push(stream.stream);
    }
    (streams, partitions)
}
