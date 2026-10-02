//! Streams read in phases, as a CDC stream reads its snapshot and then its changes (spec §9.4):
//! once every partition of a phase has ended and its end is committed, the stream is planned
//! again, and a plan naming a new phase starts that phase's partitions.

use std::collections::BTreeMap;
use std::sync::Arc;

use rdlt_connector::{
    BoxFuture, Cursor, Partition, PartitionId, PartitionPlan, PartitionState, Source, StateChange,
    StateEntry, StateKey, StreamName, StreamState,
};
use tokio::sync::mpsc;

use super::{Coordinator, PartitionRun};
use crate::error::{Error, ErrorKind, Side};
use crate::partition::{self, ChangeMode, PartitionContext, PartitionJob};
use crate::wal::frame::BegunPhase;

/// A phased stream's place in its phases.
#[derive(Debug)]
pub(crate) struct Phases {
    /// The phase the stream reads.
    pub(crate) phase: u16,
    /// The partitions the stream reads in its phase, by id, each with its index in the attempt.
    pub(crate) reading: BTreeMap<PartitionId, usize>,
    /// The committed position of each partition of the phase, as state records it.
    pub(crate) committed: BTreeMap<PartitionId, PartitionState>,
    /// The phase this attempt began, which the next commit records; `None` once a commit has
    /// taken it.
    pub(crate) begun: Option<Begun>,
    /// Whether planning named no new phase: the stream reads nothing more this attempt.
    pub(crate) settled: bool,
    /// How the stream's partitions read.
    pub(crate) template: Template,
}

/// A phase an attempt began: the entries of the phases before it, and where its partitions
/// start.
#[derive(Debug, Default)]
pub(crate) struct Begun {
    pub(crate) stale: Vec<PartitionId>,
    pub(crate) starts: BTreeMap<PartitionId, Cursor>,
}

/// What every partition of a stream is read with.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Template {
    pub(crate) table: usize,
    pub(crate) on_demand: bool,
    pub(crate) changes: Option<ChangeMode>,
    /// Whether its partitions read again from their source's earliest after a retention loss.
    pub(crate) reset_retention: bool,
}

/// What `source` plans for the stream `name` from `state`, checked as
/// [`PartitionPlan::validate`] checks it.
///
/// # Errors
///
/// The source's error, or `plan_invalid`, a Source error no retry mends, for a plan of too many
/// partitions, a partition named twice, or a start of one it does not name.
pub(crate) async fn plan_of(
    source: &dyn Source,
    name: &StreamName,
    state: &StreamState,
) -> Result<PartitionPlan, Error> {
    let planned = source.plan(name, state).await.map_err(|error| {
        Error::connector(Side::Source, format!("planning stream {name}"), error).with_stream(name)
    })?;
    planned.validate().map_err(|invalid| {
        Error::new(ErrorKind::Source, format!("stream {name}: {invalid}"))
            .with_code("plan_invalid")
            .with_stream(name)
    })?;
    Ok(planned)
}

/// Starts the partitions the coordinator plans.
pub(crate) type Launcher = Box<dyn Fn(PartitionJob) -> Result<(), Error> + Send + Sync>;

/// A launcher whose partitions read with `context`, their tasks sent to `tasks` to join the
/// attempt's scope.
pub(crate) fn launcher(
    context: Arc<PartitionContext>,
    tasks: mpsc::UnboundedSender<BoxFuture<'static, Result<(), Error>>>,
) -> Launcher {
    Box::new(move |job| {
        let task = partition::run(job, Arc::clone(&context));
        tasks
            .send(Box::pin(task))
            .map_err(|_| Error::cancelled("the attempt's scope ended"))
    })
}

impl Coordinator {
    /// Plans again each phased stream whose phase has ended, starting the partitions of the new
    /// phase a plan names.
    ///
    /// A stopping attempt starts nothing more. A stop the coordinator has yet to see lets a phase
    /// begin: the load that follows sees it, stops the phase's partitions and ends the attempt
    /// stopped, as a stream with more to read is.
    pub(super) async fn advance_phases(&mut self) -> Result<(), Error> {
        if self.stopping {
            return Ok(());
        }
        for stream in 0..self.parts.streams.len() {
            if self.phase_ended(stream) {
                self.plan_again(stream).await?;
            }
        }
        Ok(())
    }

    /// Whether `stream` is phased, and every partition it reads in its phase has ended with its
    /// end committed.
    ///
    /// Phases advance only after a commit, which takes every seal the coordinator has seen, and a
    /// partition seals its end before it reports it has ended: an ended partition's end is
    /// committed.
    pub(super) fn phase_ended(&self, stream: usize) -> bool {
        let run = &self.parts.streams[stream];
        let Some(phases) = &run.phases else {
            return false;
        };
        if phases.settled {
            return false;
        }
        phases
            .reading
            .values()
            .all(|index| self.parts.partitions[*index].ended)
    }

    /// Plans `stream` again from its committed state; a plan naming a new phase starts the
    /// phase's partitions, and any other settles the stream.
    async fn plan_again(&mut self, stream: usize) -> Result<(), Error> {
        let planned = self.plan_stream(stream).await?;
        let Some(phases) = self.parts.streams[stream].phases.as_mut() else {
            return Ok(());
        };
        match planned.phase {
            Some(next) if next != phases.phase => self.begin(stream, next, planned),
            _ => {
                phases.settled = true;
                Ok(())
            }
        }
    }

    /// What the source plans for `stream` from the state of its phase the destination holds.
    pub(super) async fn plan_stream(&self, stream: usize) -> Result<PartitionPlan, Error> {
        let name = &self.parts.streams[stream].name;
        let state = self.parts.streams[stream]
            .phases
            .as_ref()
            .map(|phases| StreamState {
                phase: phases.phase,
                partitions: phases.committed.clone(),
                ..StreamState::default()
            })
            .unwrap_or_default();
        plan_of(self.parts.source.as_ref(), name, &state).await
    }

    /// Begins `stream`'s phase `next`, starting its partitions where `planned` says.
    ///
    /// Only a change stream reads in phases: `phase_unexpected` for any other.
    pub(super) fn begin(
        &mut self,
        stream: usize,
        next: u16,
        planned: PartitionPlan,
    ) -> Result<(), Error> {
        let run = &mut self.parts.streams[stream];
        let Some(phases) = run.phases.as_mut() else {
            return Ok(());
        };
        if phases.template.changes.is_none() {
            return Err(Error::new(
                ErrorKind::Source,
                format!(
                    "stream {}: the source planned phase {next} of a stream not read as changes",
                    run.name
                ),
            )
            .with_code("phase_unexpected")
            .with_stream(&run.name));
        }
        let mut begun = phases.begun.take().unwrap_or_default();
        begun
            .stale
            .extend(std::mem::take(&mut phases.committed).into_keys());
        begun.starts.clone_from(&planned.starts);
        phases.begun = Some(begun);
        phases.phase = next;
        // The phase's partitions have ended, their ends committed: their places are free.
        self.retired
            .extend(std::mem::take(&mut phases.reading).into_values());
        let template = phases.template;
        self.forget_lag(stream, None);
        for partition in planned.partitions {
            let cursor = planned.starts.get(partition.id()).cloned();
            self.launch(stream, template, partition, cursor)?;
        }
        Ok(())
    }

    /// Starts reading `partition` of `stream`, in its phase, from `cursor`.
    ///
    /// A partition read again takes the place its ended read had, and a new one the place of a
    /// partition no stream reads any more, so a run that reads for ever tracks no more
    /// partitions than its plans name at once.
    pub(super) fn launch(
        &mut self,
        stream: usize,
        template: Template,
        partition: Partition,
        cursor: Option<Cursor>,
    ) -> Result<(), Error> {
        let id = partition.id().clone();
        let stop = self.parts.stop_reads.child_token();
        let tracked = PartitionRun::new(stream, id.clone(), template.on_demand, stop.clone())
            .starting(cursor.as_ref());
        let index = self.place(stream, &id);
        match self.parts.partitions.get_mut(index) {
            Some(place) => *place = tracked,
            None => self.parts.partitions.push(tracked),
        }
        self.unended += 1;
        let run = &mut self.parts.streams[stream];
        if let Some(phases) = run.phases.as_mut() {
            phases.reading.insert(id, index);
        }
        run.remaining += 1;
        let job = PartitionJob {
            index,
            stream: run.name.clone(),
            table: template.table,
            partition,
            cursor,
            on_demand: template.on_demand,
            changes: template.changes,
            stop,
            follow: self.parts.follow,
            reset_retention: template.reset_retention,
        };
        (self.parts.launcher)(job)
    }

    /// The index `stream`'s partition `id` takes: the place of its ended read, else the place
    /// of a partition no stream reads whose seals were all committed, else a new one.
    fn place(&mut self, stream: usize, id: &PartitionId) -> usize {
        let partitions = &self.parts.partitions;
        let ended = self.parts.streams[stream]
            .phases
            .as_ref()
            .and_then(|phases| phases.reading.get(id).copied())
            .filter(|index| partitions[*index].ended);
        if let Some(index) = ended {
            return index;
        }
        let free = self
            .retired
            .iter()
            .copied()
            .find(|index| !self.sealing.contains(index));
        match free {
            Some(index) => {
                self.retired.remove(&index);
                index
            }
            None => self.parts.partitions.len(),
        }
    }

    /// Records, for each phased stream, the committed positions of its phase's partitions.
    pub(super) fn record_positions(&mut self, positions: &BTreeMap<usize, PartitionState>) {
        for (index, state) in positions {
            let partition = &self.parts.partitions[*index];
            if let Some(phases) = self.parts.streams[partition.stream].phases.as_mut()
                && phases.reading.get(&partition.id) == Some(index)
            {
                phases.committed.insert(partition.id.clone(), state.clone());
            }
        }
    }

    /// The phases the next commit begins, each with the state changes recording it: the stream's
    /// stale partition entries deleted, where its partitions start, then the phase.
    ///
    /// A commit that fails ends the attempt, so the commit that takes a phase's changes records
    /// it. A partition that begins its phase is recorded at its start, so an attempt that ends
    /// before the partition's first checkpoint resumes it there.
    pub(super) fn phase_delta(&mut self) -> Vec<BegunPhase> {
        let mut begun_phases = Vec::new();
        for stream in &mut self.parts.streams {
            let Some(phases) = stream.phases.as_mut() else {
                continue;
            };
            let Some(begun) = phases.begun.take() else {
                continue;
            };
            let mut changes = Vec::new();
            for partition in begun.stale {
                let key = StateKey::Partition(stream.name.clone(), partition);
                changes.push(StateChange::Delete(key.encode()));
            }
            for (partition, start) in begun.starts {
                let state = PartitionState::Cursor(start);
                phases.committed.insert(partition.clone(), state.clone());
                let entry = StateEntry::Partition {
                    stream: stream.name.clone(),
                    partition,
                    state,
                };
                changes.push(StateChange::Put(entry.to_record()));
            }
            let entry = StateEntry::Phase {
                stream: stream.name.clone(),
                phase: phases.phase,
            };
            changes.push(StateChange::Put(entry.to_record()));
            begun_phases.push(BegunPhase {
                stream: stream.name.clone(),
                phase: phases.phase,
                changes,
            });
        }
        begun_phases
    }
}
