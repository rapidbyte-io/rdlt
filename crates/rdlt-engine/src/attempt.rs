//! One attempt of a run: open the destination, plan the streams, and load until done.

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use arrow_schema::SchemaRef;
use parking_lot::Mutex;
use rdlt_connector::{
    Catalog, Checkpointing, Cursor, Destination, DestinationSession, DestinationWriter, Epoch,
    GenerationId, LoadId, OpenContext, OpenedSession, Partition, PartitionState, PipelineState,
    ReadMode, SchemaVersion, Source, StreamName, StreamState, TableChange, TablePath, TableRef,
    TableSchema,
};
use tokio::sync::{Semaphore, mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::budget::MemoryBudget;
use crate::config::EngineConfig;
use crate::coordinator::{Coordinator, CoordinatorParts, Cycle, PartitionRun, StreamRun};
use crate::env::Env;
use crate::error::{Error, ErrorKind, Side};
use crate::lane::Lanes;
use crate::partition::{self, PartitionContext, PartitionJob};
use crate::plan::{PipelinePlan, StreamPlan, WriteMode};
use crate::report::{AttemptEnd, AttemptLog};
use crate::scope::TaskScope;

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
    arrow: SchemaRef,
    on_demand: bool,
    partitions: Vec<(Partition, Option<Cursor>)>,
}

/// An open session and the state it returned.
struct Opened {
    session: Box<dyn DestinationSession>,
    epoch: Epoch,
    state: PipelineState,
}

/// Runs one attempt under `load_id`, recording its commits in `log` as they land.
pub(crate) async fn run(
    context: &RunContext,
    load_id: LoadId,
    log: Arc<Mutex<AttemptLog>>,
) -> Result<AttemptEnd, Error> {
    let mut opened = open(context, load_id).await?;
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
    let mut planned = Vec::with_capacity(context.plan.streams().len());
    for plan in context.plan.streams() {
        let stream = plan_stream(
            context,
            plan,
            &catalog,
            &opened.state,
            opened.session.as_mut(),
        );
        planned.push(stream.await?);
    }
    let writers = writers(context, opened.session.as_mut(), &planned).await?;
    launch(context, load_id, opened, planned, writers, Arc::clone(&log)).await?;
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
        session,
        epoch,
        state,
    })
}

/// One writer per table for each lane.
async fn writers(
    context: &RunContext,
    session: &mut dyn DestinationSession,
    planned: &[Planned],
) -> Result<Vec<Vec<Box<dyn DestinationWriter>>>, Error> {
    let lanes = lane_count(&context.config, context.destination.as_ref());
    let mut writers = Vec::with_capacity(lanes.get());
    for _ in 0..lanes.get() {
        let mut lane = Vec::with_capacity(planned.len());
        for stream in planned {
            let writer = session
                .writer(&stream.stream.table)
                .await
                .map_err(|error| {
                    Error::connector(Side::Destination, "creating a writer", error)
                        .with_stream(&stream.stream.name)
                })?;
            lane.push(writer);
        }
        writers.push(lane);
    }
    Ok(writers)
}

/// Runs the lanes, the partitions and the coordinator in one scope until all of them end.
async fn launch(
    context: &RunContext,
    load_id: LoadId,
    opened: Opened,
    planned: Vec<Planned>,
    writers: Vec<Vec<Box<dyn DestinationWriter>>>,
    log: Arc<Mutex<AttemptLog>>,
) -> Result<(), Error> {
    let (lanes, lane_tasks) = Lanes::new(writers, context.config.lane_window());
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
        budget: context.budget.clone(),
        progress,
        barrier: barrier_feed,
        stop: stop_reads.clone(),
        cancel: cancel.clone(),
        slots: Arc::new(Semaphore::new(context.config.partitions().get())),
        segments: Arc::new(AtomicU64::new(1)),
        buffer: context.config.partition_buffer(),
    };
    // Only the partitions may keep the progress channel open, so the coordinator sees them end.
    let (streams, partitions) = spawn_partitions(&mut scope, planned, partition_context);
    let coordinator = Coordinator::new(CoordinatorParts {
        env: Arc::clone(&context.env),
        policy: *context.config.commit(),
        barrier_wait: context.config.barrier_wait(),
        session: opened.session,
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
    for (table, stream) in planned.into_iter().enumerate() {
        for (partition, cursor) in stream.partitions {
            let id = partition.id().clone();
            let job = PartitionJob {
                index: partitions.len(),
                stream: stream.stream.name.clone(),
                table,
                schema: Arc::clone(&stream.arrow),
                partition,
                cursor,
                on_demand: stream.on_demand,
            };
            partitions.push(PartitionRun::new(table, id, stream.on_demand));
            scope.spawn(partition::run(job, context.clone()));
        }
        streams.push(stream.stream);
    }
    // The context's progress sender must not outlive the partitions.
    drop(context);
    (streams, partitions)
}

/// Checks `plan` against the catalog, the destination and committed state, creates its table
/// when state has no schema for it, and plans its partitions.
async fn plan_stream(
    context: &RunContext,
    plan: &StreamPlan,
    catalog: &Catalog,
    state: &PipelineState,
    session: &mut dyn DestinationSession,
) -> Result<Planned, Error> {
    let name = plan.name();
    let (schema, on_demand) = check_stream(context, plan, catalog)?;
    let path = TablePath::new([name.to_string()]).map_err(|error| {
        Error::internal(format!("stream {name} has no valid table path: {error}"))
    })?;
    let record_schema = needs_schema(state, &path, &schema, name)?;
    let read = read(context, plan, state.streams.get(name));
    let generation = match (plan.write_mode(), &read) {
        (WriteMode::Replace, Read::Cycle(cycle, _)) => Some(cycle.generation),
        _ => None,
    };
    let table = TableRef {
        path,
        name: Arc::from(name.to_string()),
        version: SchemaVersion(1),
        generation,
    };
    if record_schema {
        let create = TableChange::Create {
            table: table.clone(),
            schema: schema.clone(),
        };
        session.apply_schema(&create).await.map_err(|error| {
            let context = format!("creating the table of stream {name}");
            Error::connector(Side::Destination, context, error).with_stream(name)
        })?;
    }
    let (cycle, partitions) = match read {
        Read::Incremental(state) => (None, partitions(context, name, &state).await?),
        Read::Cycle(cycle, state) => (Some(cycle), partitions(context, name, &state).await?),
        Read::Completed => (None, Vec::new()),
    };
    Ok(Planned {
        arrow: Arc::new(schema.to_arrow()),
        stream: StreamRun {
            name: name.clone(),
            write: plan.write_mode(),
            table,
            schema,
            record_schema,
            cycle,
            remaining: partitions.len(),
            stopped: false,
        },
        on_demand,
        partitions,
    })
}

/// The stream's declared schema and whether it checkpoints on demand, once the source can read it
/// as planned and the destination can write it as planned.
fn check_stream(
    context: &RunContext,
    plan: &StreamPlan,
    catalog: &Catalog,
) -> Result<(TableSchema, bool), Error> {
    let name = plan.name();
    let refuse = |code: &str, detail: &str| {
        Error::config(format!("stream {name}: {detail}"))
            .with_code(code)
            .with_stream(name)
    };
    let spec = catalog.get(name).ok_or_else(|| {
        refuse(
            "stream_not_found",
            "the source's catalog has no such stream",
        )
    })?;
    let schema = spec.schema().cloned().ok_or_else(|| {
        let detail = "the stream declares no schema, which the engine needs until it can infer one";
        refuse("schema_required", detail)
    })?;
    if !spec.supports(plan.read_mode()) {
        let detail = format!("the source cannot read it as {:?}", plan.read_mode());
        return Err(refuse("read_mode_unsupported", &detail));
    }
    let modes = context.destination.capabilities().write_modes;
    let writable = match plan.write_mode() {
        WriteMode::Append => modes.append,
        WriteMode::Replace => modes.replace,
    };
    if !writable {
        let detail = format!("the destination cannot write {:?}", plan.write_mode());
        return Err(refuse("write_mode_unsupported", &detail));
    }
    Ok((schema, spec.checkpointing() == Checkpointing::OnDemand))
}

/// Whether the table's schema must still be recorded; a committed schema must equal `schema`.
fn needs_schema(
    state: &PipelineState,
    path: &TablePath,
    schema: &TableSchema,
    name: &StreamName,
) -> Result<bool, Error> {
    match state
        .tables
        .get(path)
        .and_then(|table| table.schema.as_ref())
    {
        None => Ok(true),
        Some((_, committed)) if committed == schema => Ok(false),
        Some(_) => Err(Error::schema(format!(
            "stream {name}: the declared schema differs from the committed one, and schema \
             changes are not supported yet"
        ))
        .with_code("schema_changed")
        .with_stream(name)),
    }
}

/// How an attempt reads a stream.
enum Read {
    /// From its committed partitions.
    Incremental(StreamState),
    /// As one full read, planned from the given state.
    Cycle(Cycle, StreamState),
    /// Not at all: this run already completed the stream's full read.
    Completed,
}

/// How an attempt reads `plan`, given its committed state.
///
/// A full read resumes the read state records as in progress, whichever run started it. Without
/// one, it starts a new read from the beginning, whose first commit deletes the previous read's
/// partition entries, unless this run already completed a full read of the stream.
fn read(context: &RunContext, plan: &StreamPlan, committed: Option<&StreamState>) -> Read {
    let committed = committed.cloned().unwrap_or_default();
    if plan.read_mode() != ReadMode::Full {
        return Read::Incremental(committed);
    }
    let mut cycles = context.cycles.lock();
    if let Some(generation) = committed.generation {
        cycles.insert(plan.name().clone(), generation);
        let cycle = Cycle {
            generation,
            recorded: true,
            stale: Vec::new(),
            finished: false,
            completed: committed.completed.clone(),
        };
        return Read::Cycle(cycle, committed);
    }
    let generation = *cycles
        .entry(plan.name().clone())
        .or_insert_with(|| GenerationId(context.env.random()));
    if committed.completed.contains(&generation) {
        return Read::Completed;
    }
    let cycle = Cycle {
        generation,
        recorded: false,
        stale: committed.partitions.keys().cloned().collect(),
        finished: false,
        completed: committed.completed.clone(),
    };
    Read::Cycle(cycle, StreamState::default())
}

/// The partitions of `name` still to read, with their committed cursors.
async fn partitions(
    context: &RunContext,
    name: &StreamName,
    state: &StreamState,
) -> Result<Vec<(Partition, Option<Cursor>)>, Error> {
    let planned = context.source.plan(name, state).await.map_err(|error| {
        Error::connector(Side::Source, format!("planning stream {name}"), error).with_stream(name)
    })?;
    Ok(planned
        .into_iter()
        .filter_map(|partition| match state.partitions.get(partition.id()) {
            Some(PartitionState::Done) => None,
            Some(PartitionState::Cursor(cursor)) => Some((partition, Some(cursor.clone()))),
            None => Some((partition, None)),
        })
        .collect())
}

/// The configured lanes, or one per core up to the destination's limit.
fn lane_count(config: &EngineConfig, destination: &dyn Destination) -> NonZeroUsize {
    let limit = usize::from(destination.capabilities().max_parallel_writers.get());
    let lanes = config.lanes().map_or_else(
        || {
            std::thread::available_parallelism()
                .map_or(1, NonZeroUsize::get)
                .min(limit)
        },
        |lanes| usize::from(lanes.get()),
    );
    NonZeroUsize::new(lanes).unwrap_or(NonZeroUsize::MIN)
}
