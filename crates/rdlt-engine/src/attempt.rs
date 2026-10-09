//! One attempt of a run: open the destination, plan the streams, and load until done.

mod check;
mod replay;
mod sequences;
mod streams;
#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use parking_lot::Mutex;
use rdlt_connector::{
    Cursor, Destination, Epoch, GenerationId, LoadId, OpenContext, OpenedSession, Partition,
    PipelineState, Source, StreamName,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::budget::{Denied, MemoryBudget, Reservation};
use crate::compute::Pool;
use crate::config::EngineConfig;
use crate::coordinator::{Coordinator, CoordinatorParts, PartitionRun, StreamRun, launcher};
use crate::env::Env;
use crate::error::{Error, ErrorKind, Side};
use crate::lane::Lanes;
use crate::limits::{LOG_COPY_BYTES, WAL_STAGING_EXCEEDS_BUDGET};
use crate::naming::{Naming, recorded};
use crate::partition::{
    self, ChangeMode, Latest, LoadClock, PartitionContext, PartitionJob, Progress, Slots,
};
use crate::plan::PipelinePlan;
use crate::report::{AttemptEnd, AttemptLog, Tally};
use crate::scope::TaskScope;
use crate::stored::{StateLimits, Stored};
use crate::table::{SharedSession, Tables};
use crate::wal::{LoadLog, Owner, Positions, WalStore, staged_at_most};
use crate::watch;

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
    /// Where the engine keeps write-ahead logs, each request counted into the tally; none
    /// where it keeps none.
    pub(crate) wal: Option<Arc<dyn WalStore>>,
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
    if context.plan.logs_ahead() && context.wal.is_none() {
        return Err(Error::config(format!(
            "pipeline {} keeps a write-ahead log, and the engine has nowhere to keep one",
            context.plan.pipeline()
        ))
        .with_code("wal_store_missing"));
    }
    // The load's log opens before any other is read, so a replay that starts later lists it
    // and fences it before it reads the pipeline's logs itself.
    let Some(store) = context.wal.clone() else {
        return logged_run(context, load_id, &log).await;
    };
    let pipeline = context.plan.pipeline();
    let identity = crate::wal::taken::open_own(store.as_ref(), pipeline, load_id).await?;
    log.lock().store = Some(identity);
    let ran = logged_run(context, load_id, &log).await;
    // A failed attempt that published nothing leaves no log behind: nothing of it is replayed.
    // The failure matters more than one of removing it, which a later replay retries.
    if ran.is_err()
        && matches!(store.chunks(pipeline, load_id).await, Ok(chunks) if chunks.is_empty())
    {
        drop(store.remove_log(pipeline, load_id).await);
    }
    ran
}

/// Runs the attempt as [`run`] does, its log opened where the engine keeps one.
async fn logged_run(
    context: &RunContext,
    load_id: LoadId,
    log: &Arc<Mutex<AttemptLog>>,
) -> Result<AttemptEnd, Error> {
    // What earlier loads logged and never saw committed lands before this one plans.
    replay::replay(context, load_id, log).await?;
    let opened = open(context, load_id).await?;
    let session = Arc::clone(&opened.session);
    let ran = opened_run(context, load_id, opened, log).await;
    if ran.is_err() {
        // A failed attempt's session releases what it holds now; the failure matters more than
        // any error from closing, and a session the coordinator closed stays closed.
        closed_within(context, session.close()).await;
    }
    ran
}

/// Waits for `closing`, a close after a failure, no longer than the close wait: the failure is
/// what the run reports, and the next attempt should not wait on a destination that is stuck.
pub(super) async fn closed_within<T>(context: &RunContext, closing: impl Future<Output = T>) {
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
    {
        let mut log = log.lock();
        log.opened = opened
            .state
            .last_receipt
            .as_ref()
            .map(|receipt| (receipt.load_id, receipt.commit_seq));
        log.origin = opened.state.origin;
        log.log_store = opened.state.log_store;
        crate::wal::taken::one_store(log.store, opened.state.log_store)?;
    }
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
        .growing(context.config.child_table_limit())
        .committed(&opened.state)?;
    let mut planned = Vec::with_capacity(context.plan.streams().len());
    for plan in context.plan.streams() {
        planned.push(planning.stream(plan, &tables).await?);
    }
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
            .with_code("state_invalid"));
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
    .with_code("plan_invalid"))
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
    let wal = start_log(context, load_id, &opened, &planned, &mut scope).await?;
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
    let partition_context = shared_by_partitions(context, load_id, (&lanes, &tables, &wal), ends);
    let (tasks, spawned) = mpsc::unbounded_channel();
    let (streams, partitions) = spawn_partitions(&mut scope, planned, &partition_context);
    let coordinator = Coordinator::new(CoordinatorParts {
        env: Arc::clone(&context.env),
        policy: context.config.commit_for(context.plan.commits_as_stream()),
        barrier_wait: context.config.barrier_wait(),
        tables,
        source: Arc::clone(&context.source),
        pipeline: context.plan.pipeline().clone(),
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
        wal,
        positions: Positions::of(&opened.state),
        stored: opened.stored,
        follow: context.plan.until().follows(),
        replan: context.config.replan(),
        tally: Arc::clone(&context.tally),
        store: context.wal.clone(),
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

/// What the attempt's partitions share: its `lanes`, `tables` and `wal`, and their `ends`.
fn shared_by_partitions(
    context: &RunContext,
    load_id: LoadId,
    (lanes, tables, wal): (&Lanes, &Arc<Tables>, &Option<LoadLog>),
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
        slots: Slots::new(context.config.partitions()),
        segments: Arc::new(AtomicU64::new(1)),
        buffer: context.config.partition_buffer(),
        load_id,
        clock: Arc::new(LoadClock::new(context.env.now())),
        env: Arc::clone(&context.env),
        pool: Pool::new(Arc::clone(&context.env), Arc::clone(&context.tally)),
        batch: *context.config.batch(),
        stop_wait: context.config.stop_wait(),
        wal: wal.clone(),
    })
}

/// The attempt's lanes, their tasks started in `scope`.
fn start_lanes(context: &RunContext, tables: &Arc<Tables>, scope: &mut TaskScope<Error>) -> Lanes {
    let count = lane_count(
        &context.config,
        context.env.cores(),
        context.destination.as_ref(),
    );
    let writers = context.config.growth().writers();
    let (lanes, tasks) = Lanes::new(
        (count, writers),
        tables,
        context.config.lane_window(),
        &context.budget,
        (&context.env, &context.tally),
    );
    let cancel = scope.token().clone();
    // A change of a table's schema reserves what its commit records, until the attempt ends.
    tables.charge(context.budget.clone(), cancel.clone());
    for lane in tasks {
        scope.spawn(lane.run(cancel.clone()));
    }
    lanes
}

/// The load's write-ahead log, started in `scope`, where the pipeline asks for one or a stream's
/// source cannot read again what it acknowledged; `opened` is the session the attempt opened.
///
/// The log opened for a load that needs none is removed in `scope`.
///
/// # Errors
///
/// `wal_staging_exceeds_budget` where the store stages more in memory than `staged_at_most`
/// leaves of the log's share beside a commit's frame, a carry's read and a frame's head.
async fn start_log(
    context: &RunContext,
    load_id: LoadId,
    opened: &Opened,
    planned: &[Planned],
    scope: &mut TaskScope<Error>,
) -> Result<Option<LoadLog>, Error> {
    let needed =
        context.plan.logs_ahead() || planned.iter().any(|stream| !stream.stream.replayable);
    let Some(store) = context.wal.clone() else {
        return Ok(None);
    };
    if !needed {
        let pipeline = context.plan.pipeline().clone();
        scope.spawn(async move {
            store
                .remove_log(&pipeline, load_id)
                .await
                .map_err(Error::from_wal)
        });
        return Ok(None);
    }
    let staging = staged(&context.budget, store.staging_bytes()).await?;
    let owner = Owner {
        pipeline: context.plan.pipeline().clone(),
        load: load_id,
        epoch: opened.epoch,
        opened: opened
            .state
            .last_receipt
            .as_ref()
            .map(|receipt| (receipt.load_id, receipt.commit_seq)),
        origin: opened.state.origin.unwrap_or(load_id),
    };
    // A store's largest chunk bounds the log too: a chunk holds at most what the log does.
    let log_bytes = context.config.growth().log_bytes();
    let bound = store
        .chunk_bytes()
        .map_or(log_bytes, |most| most.min(log_bytes));
    let tally = Arc::clone(&context.tally);
    let (log, task) = LoadLog::start(store, owner, bound, (Some(staging), tally));
    scope.spawn(task);
    Ok(Some(log))
}

/// What a log holds in memory beside its frames, `bytes` its store's stagings hold and what a
/// carry reads back at once, reserved from `budget`'s share for the log for as long as the log
/// is written.
///
/// # Errors
///
/// `wal_staging_exceeds_budget` for stagings that leave the share too little for a commit's
/// frame recording a full share of cursors ([`staged_at_most`]).
async fn staged(budget: &MemoryBudget, bytes: u64) -> Result<Reservation, Error> {
    let most = staged_at_most(budget.shares());
    if bytes > most {
        return Err(Error::config(format!(
            "the write-ahead log's store stages {bytes} bytes in memory, more than the {most} \
             the memory budget's share for the log leaves beside a commit recording every \
             cursor it holds: a larger budget, or smaller parts, let it run"
        ))
        .with_code(WAL_STAGING_EXCEEDS_BUDGET));
    }
    // What a carry reads back at once is held beside the staging, in the log's share.
    budget
        .acquire_log(bytes.saturating_add(LOG_COPY_BYTES))
        .await
        .map_err(|denied| match denied {
            Denied::Exhausted(exhausted) => Error::memory(exhausted),
            Denied::TooLarge(large) => Error::config(format!(
                "the write-ahead log's store stages {bytes} bytes in memory, more than the memory \
                 budget's share for the log holds beside what a carry reads at once"
            ))
            .with_code(WAL_STAGING_EXCEEDS_BUDGET)
            .with_source(large),
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

/// The configured lanes, or one per core of `cores`, never more than the destination's parallel
/// writers, nor than the writers an attempt holds open: each lane holds one at least.
fn lane_count(
    config: &EngineConfig,
    cores: NonZeroUsize,
    destination: &dyn Destination,
) -> NonZeroUsize {
    let limit = usize::from(destination.capabilities().max_parallel_writers.get());
    let lanes = config
        .lanes()
        .map_or(cores.get(), |lanes| usize::from(lanes.get()));
    let lanes = lanes.min(limit).min(config.growth().writers().get());
    NonZeroUsize::new(lanes).unwrap_or(NonZeroUsize::MIN)
}
