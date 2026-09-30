//! Planning a following run's streams again as it reads: partitions the source adds start, ones
//! that ended and are not done read again from where they were committed, and ones the source no
//! longer names stop.

use std::collections::BTreeSet;

use rdlt_connector::{Partition, PartitionId, PartitionPlan, PartitionState};

use super::Coordinator;
use crate::error::Error;

impl Coordinator {
    /// Plans each stream that tracks its partitions again, unless the attempt is stopping.
    pub(super) async fn replan(&mut self) -> Result<(), Error> {
        if self.stopping {
            return Ok(());
        }
        for stream in 0..self.parts.streams.len() {
            if self.parts.streams[stream].phases.is_some() {
                self.replan_stream(stream).await?;
            }
        }
        Ok(())
    }

    /// Plans `stream` again: a plan of the same phase is read as it now says, and one naming a new
    /// phase waits for the next commit to begin it.
    ///
    /// Phases begin only after a commit, which takes every seal the coordinator has seen: a phase
    /// begun here could leave a seal of the last one to commit after it, recording a position of
    /// the phase before.
    pub(super) async fn replan_stream(&mut self, stream: usize) -> Result<(), Error> {
        let planned = self.plan_stream(stream).await?;
        let Some(phases) = self.parts.streams[stream].phases.as_mut() else {
            return Ok(());
        };
        phases.settled = false;
        match planned.phase {
            Some(next) if next != phases.phase => Ok(()),
            _ => self.rebalance(stream, &planned),
        }
    }

    /// Reads `stream`'s phase as `planned` says: running partitions it no longer names stop, and
    /// each it names that is neither running nor done starts from its committed position, or
    /// from the beginning, as initial planning starts it: a plan's starts place only a new
    /// phase's partitions.
    ///
    /// A partition whose end is not yet committed waits for the next plan: read again from its
    /// committed position, it would read again what its end seals.
    fn rebalance(&mut self, stream: usize, planned: &PartitionPlan) -> Result<(), Error> {
        let Some(phases) = &self.parts.streams[stream].phases else {
            return Ok(());
        };
        let named: BTreeSet<&PartitionId> = planned.partitions.iter().map(Partition::id).collect();
        let reading = phases.reading.clone();
        let template = phases.template;
        let committed = phases.committed.clone();
        let mut dropped = Vec::new();
        for index in &reading {
            let run = &self.parts.partitions[*index];
            if named.contains(&run.id) {
                continue;
            }
            // Ended or not, a partition the plan no longer names lags no more.
            dropped.push(run.id.clone());
            if !run.ended {
                run.stop.cancel();
            }
        }
        let unsettled: BTreeSet<&PartitionId> = reading
            .iter()
            .map(|index| &self.parts.partitions[*index])
            .filter(|run| !run.ended)
            .map(|run| &run.id)
            .chain(
                self.sealed
                    .iter()
                    .map(|seal| &self.parts.partitions[seal.partition].id),
            )
            .collect();
        let starts: Vec<_> = planned
            .partitions
            .iter()
            .filter(|partition| !unsettled.contains(partition.id()))
            .filter_map(|partition| match committed.get(partition.id()) {
                Some(PartitionState::Done) => None,
                Some(PartitionState::Cursor(cursor)) => {
                    Some((partition.clone(), Some(cursor.clone())))
                }
                None => Some((partition.clone(), None)),
            })
            .collect();
        for (partition, cursor) in starts {
            self.launch(stream, template, partition, cursor)?;
        }
        self.forget_lag(stream, Some(&dropped));
        Ok(())
    }
}
