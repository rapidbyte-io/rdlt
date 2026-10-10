//! How an attempt reads each of its streams, and the partitions its source plans for it.

use std::collections::{BTreeMap, BTreeSet};

use rdlt_connector::{
    Cursor, Epoch, GenerationId, Partition, PartitionId, PartitionState, ReadMode, StreamState,
};

use crate::attempt::RunContext;
use crate::coordinator::{Begun, Cycle, plan_of};
use crate::error::{Error, ErrorKind};
use crate::plan::StreamPlan;

/// How an attempt reads a stream.
pub(super) enum Read {
    /// From its committed partitions.
    Incremental(StreamState),
    /// As one full read, planned from the given state.
    Cycle(Cycle, StreamState),
    /// Not at all: this run already completed the stream's full read.
    Completed,
}

/// How an attempt reads `plan`, given its committed state and the epoch of its last reset.
///
/// A full read resumes the read state records as in progress, whichever run started it. Without
/// one, it starts a new read from the beginning, whose first commit deletes the previous read's
/// partition entries, unless this run already completed a full read of the stream. A read this run
/// began before the stream was reset is over: what it published of its generation was the
/// stream's before the reset, so the next read fills a generation of its own.
pub(super) fn read(
    context: &RunContext,
    plan: &StreamPlan,
    committed: Option<&StreamState>,
    reset: Option<Epoch>,
) -> Read {
    let committed = committed.cloned().unwrap_or_default();
    if plan.read_mode() != ReadMode::Full {
        return Read::Incremental(committed);
    }
    let mut cycles = context.cycles.lock();
    if let Some(generation) = committed.generation {
        cycles.insert(plan.name().clone(), (generation, reset));
        let cycle = Cycle {
            generation,
            recorded: true,
            stale: Vec::new(),
            finished: false,
            completed: committed.completed.clone(),
        };
        return Read::Cycle(cycle, committed);
    }
    let generation = cycles
        .get(plan.name())
        .filter(|(_, began)| *began == reset)
        .map_or_else(
            || GenerationId(context.env.random()),
            |(generation, _)| *generation,
        );
    cycles.insert(plan.name().clone(), (generation, reset));
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

/// The partitions of a stream still to read, and its phase.
#[derive(Debug, Default)]
pub(super) struct Partitioned {
    /// The phase they belong to.
    pub(super) phase: u16,
    /// The committed positions of the phase's partitions.
    pub(super) committed: BTreeMap<PartitionId, PartitionState>,
    /// When the plan begins a new phase, the entries of the phases before it and where its
    /// partitions start.
    pub(super) begun: Option<Begun>,
    /// The partitions to read, each from its committed position or its start.
    pub(super) partitions: Vec<(Partition, Option<Cursor>)>,
    /// Every partition the plan names, those it leaves unread among them.
    pub(super) named: BTreeSet<PartitionId>,
}

/// A stream's read, and the partitions its source planned for it.
pub(super) struct Sourced {
    /// The full read it is, where it is one.
    pub(super) cycle: Option<Cycle>,
    /// The partitions to read, or why they cannot be planned.
    pub(super) partitioned: Result<Partitioned, Error>,
}

/// The partitions the source plans for `plan`'s stream, read as `read` says; none for a read
/// already completed.
pub(super) async fn sourced(context: &RunContext, plan: &StreamPlan, read: Read) -> Sourced {
    match read {
        Read::Incremental(state) => Sourced {
            cycle: None,
            partitioned: partitions(context, plan, &state).await,
        },
        Read::Cycle(cycle, state) => Sourced {
            partitioned: partitions(context, plan, &state).await,
            cycle: Some(cycle),
        },
        Read::Completed => Sourced {
            cycle: None,
            partitioned: Ok(Partitioned::default()),
        },
    }
}

/// The partitions of `plan`'s stream still to read, with their committed cursors; a plan
/// beginning a new phase starts its partitions where it says.
///
/// Only a change stream reads in phases: a plan beginning one for any other stream is
/// `phase_unexpected`, a Source error, since the stream would start over from its plan every run.
async fn partitions(
    context: &RunContext,
    plan: &StreamPlan,
    state: &StreamState,
) -> Result<Partitioned, Error> {
    let name = plan.name();
    let planned = plan_of(context.source.as_ref(), name, state).await?;
    let named = planned
        .partitions
        .iter()
        .map(|partition| partition.id().clone())
        .collect();
    if let Some(phase) = planned.phase.filter(|phase| *phase != state.phase) {
        if plan.read_mode() != ReadMode::Cdc {
            return Err(Error::new(
                ErrorKind::Source,
                format!("stream {name}: the source planned phase {phase} of a stream not read as changes"),
            )
            .with_code("phase_unexpected")
            .with_stream(name));
        }
        let begun = Begun {
            stale: state.partitions.keys().cloned().collect(),
            starts: planned.starts.clone(),
        };
        let partitions = planned
            .partitions
            .into_iter()
            .map(|partition| {
                let start = planned.starts.get(partition.id()).cloned();
                (partition, start)
            })
            .collect();
        return Ok(Partitioned {
            phase,
            committed: BTreeMap::new(),
            begun: Some(begun),
            partitions,
            named,
        });
    }
    let partitions = planned
        .partitions
        .into_iter()
        .filter_map(|partition| match state.partitions.get(partition.id()) {
            Some(PartitionState::Done) => None,
            Some(PartitionState::Cursor(cursor)) => Some((partition, Some(cursor.clone()))),
            None => Some((partition, None)),
        })
        .collect();
    Ok(Partitioned {
        phase: state.phase,
        committed: state.partitions.clone(),
        begun: None,
        partitions,
        named,
    })
}
