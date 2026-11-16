//! How an attempt reads each of its streams, and the partitions its source plans for it.

use std::collections::{BTreeMap, BTreeSet};

use rdlt_connector::{
    Cursor, Fills, GenerationId, Partition, PartitionId, PartitionState, PipelineState, ReadMode,
    StreamState,
};

use crate::attempt::RunContext;
use crate::coordinator::{Begun, Cycle, FullRead, plan_of};
use crate::error::{Error, ErrorKind, codes};
use crate::partition::{read_named, reset_remedy, resumed};
use crate::plan::StreamPlan;

/// How an attempt reads a stream.
pub(super) enum Read {
    /// From its committed partitions.
    Incremental(StreamState),
    /// From the committed partitions of the full read state records, which filled the table and
    /// which the read ends with its first commit.
    Ending(StreamState),
    /// As one full read, planned from the given state.
    Cycle(Cycle, StreamState),
    /// Not at all: this run already completed the stream's full read.
    Completed,
    /// Not at all: the read is refused before the source plans it or its table changes.
    Refused(Error),
}

/// How an attempt reads `plan`, given the pipeline's committed `state`.
///
/// A full read state records holds positions in the place it fills: the stream's table, or the
/// generation a replace stream swaps in once the read completes. A full read resumes it,
/// whichever run started it, where it fills the same place. Otherwise, or without one, it starts
/// a new read from the beginning, whose first commit deletes the previous read's partition
/// entries, unless this run already completed a full read of the stream. A new read draws a
/// generation of its own, never one this run remembers, which may hold the rows of a read that
/// is over: a reset's, or one a changed plan ended. A read that is not full resumes the positions
/// of a full read that filled the table and ends it, and is refused where the full read fills a
/// generation.
pub(super) fn read(context: &RunContext, plan: &StreamPlan, state: &PipelineState) -> Read {
    let name = plan.name();
    let committed = state.streams.get(name).cloned().unwrap_or_default();
    let recorded = committed.generation.zip(state.fills.get(name).copied());
    if plan.read_mode() != ReadMode::Full {
        return match recorded {
            Some((_, Fills::Generation)) => Read::Refused(unfinished(plan)),
            Some((_, Fills::Table)) => Read::Ending(committed),
            None => Read::Incremental(committed),
        };
    }
    let mut cycles = context.cycles.lock();
    let fills = plan.write_mode().fills();
    if let Some((generation, _)) = recorded.filter(|(_, filled)| *filled == fills) {
        cycles.insert(name.clone(), generation);
        let cycle = Cycle {
            generation,
            recorded: true,
            stale: Vec::new(),
            finished: false,
            completed: committed.completed.clone(),
        };
        return Read::Cycle(cycle, committed);
    }
    if cycles
        .get(name)
        .is_some_and(|generation| committed.completed.contains(generation))
    {
        return Read::Completed;
    }
    let generation = GenerationId(context.env.random());
    cycles.insert(name.clone(), generation);
    let cycle = Cycle {
        generation,
        recorded: false,
        stale: committed.partitions.keys().cloned().collect(),
        finished: false,
        completed: committed.completed.clone(),
    };
    Read::Cycle(cycle, StreamState::default())
}

/// The refusal of a read that is not full of `plan`'s stream, whose full read in progress fills a
/// generation not yet swapped in: its positions say nothing of the table, whose own positions went
/// with the read's first commit, so neither resuming from them nor reading from the beginning
/// lands each row once.
fn unfinished(plan: &StreamPlan) -> Error {
    let name = plan.name();
    Error::config(format!(
        "stream {name}: a full read replacing its table is part way, its rows in a generation not \
         yet swapped in, so {} has no position in the table to resume from; read the stream in \
         full, replacing its table, to complete it, or {}",
        read_named(plan.read_mode()),
        reset_remedy(plan.read_mode())
    ))
    .with_code(codes::CURSOR_MISSING)
    .with_stream(name)
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
    /// What it does with a full read: none, the full read it is, or ending what state records.
    pub(super) full: FullRead,
    /// The partitions to read, or why they cannot be planned.
    pub(super) partitioned: Result<Partitioned, Error>,
}

/// The partitions the source plans for `plan`'s stream, read as `read` says; none for a read
/// already completed, and a refused read's refusal.
pub(super) async fn sourced(context: &RunContext, plan: &StreamPlan, read: Read) -> Sourced {
    let (full, partitioned) = match read {
        Read::Incremental(state) => (FullRead::None, partitions(context, plan, &state).await),
        Read::Ending(state) => (FullRead::Ends, partitions(context, plan, &state).await),
        Read::Cycle(cycle, state) => {
            let partitioned = partitions(context, plan, &state).await;
            (FullRead::Reads(cycle), partitioned)
        }
        Read::Completed => (FullRead::None, Ok(Partitioned::default())),
        Read::Refused(error) => (FullRead::None, Err(error)),
    };
    Sourced { full, partitioned }
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
            .with_code(codes::PHASE_UNEXPECTED)
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
    let mut partitions = Vec::new();
    for partition in planned.partitions {
        let position = state.partitions.get(partition.id());
        partitions.extend(resumed(name, partition, position, plan.read_mode())?);
    }
    Ok(Partitioned {
        phase: state.phase,
        committed: state.partitions.clone(),
        begun: None,
        partitions,
        named,
    })
}
