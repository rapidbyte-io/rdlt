//! What the coordinator notes for its run's status as it goes: where each partition stands, and
//! which step of a commit is under way.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use rdlt_connector::StreamName;

use super::{Coordinator, PartitionRun};
use crate::partition::Progress;
use crate::status::{Phase, Place, Step, stamp};

impl PartitionRun {
    /// The place the partition holds, for the run's status: its stream's name is `stream`.
    pub(super) fn place(&self, stream: &StreamName) -> Place {
        Place {
            stream: stream.clone(),
            id: self.id.clone(),
            stamp: Arc::clone(&self.stamp),
        }
    }
}

impl Coordinator {
    /// Notes that the next commit is at `step` from `at`, or that the attempt is between commits
    /// where `None`: at its commit step, it is on its first try.
    pub(super) fn step(&self, step: Option<Step>, at: Instant) {
        let phase = match step {
            Some(step) => Phase::Committing {
                seq: self.key.seq,
                step,
                tries: u32::from(step == Step::Commit),
            },
            None if self.stopping => Phase::Stopping,
            None => Phase::Reading,
        };
        self.parts.live.phase(phase, at);
    }

    /// Notes that the next commit is sent again, on its `tries`th try, from `at`.
    pub(super) fn trying(&self, tries: u32, at: Instant) {
        let phase = Phase::Committing {
            seq: self.key.seq,
            step: Step::Commit,
            tries,
        };
        self.parts.live.phase(phase, at);
    }

    /// Stamps the partition `progress` tells of with now, or as ended: but for written rows, which
    /// a partition tells as often as it writes.
    pub(super) fn stamp(&self, progress: &Progress) {
        if matches!(progress, Progress::Written { .. }) {
            return;
        }
        let Some(run) = self.parts.partitions.get(progress.partition()) else {
            return;
        };
        let stamped = match progress {
            Progress::Ended { .. } => Place::ENDED,
            // A partition's queued signal may follow its end.
            _ if run.ended => return,
            _ => stamp(self.clock.began(), self.parts.env.instant()),
        };
        run.stamp.store(stamped, Ordering::Relaxed);
    }
}
