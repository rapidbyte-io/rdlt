//! Acknowledging committed positions to the source, the cursors owed to it where a report
//! failed, and telling a commit that moves the load on from one that only records again what
//! state already holds.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroU32;

use rdlt_connector::{
    ConnectorError, Cursor, POSITION_UNSENT, PartitionId, PartitionState, StateChange, StateKey,
};

use super::delta::Collected;
use super::{Coordinator, cancelled};
use crate::error::{Error, Side};
use crate::report::Trigger;

/// Where a commit falls among its attempt's commits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Turn {
    /// More commits may follow.
    Within,
    /// The attempt's last: every partition has ended, and no phase follows.
    Last,
}

/// The cursors each stream's source has yet to be told are committed, by stream and partition:
/// a landed commit's, until a report carries them, a newer one takes their place, or they are
/// given up.
#[derive(Debug, Default)]
pub(super) struct Owed(BTreeMap<usize, BTreeMap<PartitionId, Owing>>);

/// A cursor owed to a partition's source.
#[derive(Debug)]
struct Owing {
    cursor: Cursor,
    /// Whether the commit that took the cursor recorded the partition done, which no later
    /// attempt reads.
    done: bool,
    /// The reports carrying the partition that failed in a row.
    failures: u32,
}

/// What a report that failed leaves of the cursors it carried.
struct Failed {
    /// The partitions of the report's stream still owed their cursors.
    owed: BTreeMap<PartitionId, Owing>,
    /// Cursors of partitions recorded done given up.
    dropped: u64,
    /// Whether a partition at a cursor is owed beyond what the attempt allows.
    ends: bool,
}

impl Owed {
    /// Owes `partition` of `stream` the cursor `cursor` of a commit that records it at `state`,
    /// in place of an older one.
    fn owe(
        &mut self,
        stream: usize,
        partition: PartitionId,
        cursor: Cursor,
        state: &PartitionState,
    ) {
        let partitions = self.0.entry(stream).or_default();
        let failures = partitions.get(&partition).map_or(0, |owed| owed.failures);
        let owing = Owing {
            cursor,
            done: *state == PartitionState::Done,
            failures,
        };
        partitions.insert(partition, owing);
    }
}

/// What a report of `owed` that failed with `error`, at a commit that falls at `turn`, leaves of
/// its cursors, where `attempts` failed reports in a row exhaust a partition's.
///
/// A cursor of a partition recorded done is given up at the attempt's last commit, once its
/// reports are exhausted, or where the source never sent it: no later attempt reads the
/// partition to report it. A partition at a cursor ends the attempt then instead, and the next
/// tells it from where its read starts.
fn failed(
    mut owed: BTreeMap<PartitionId, Owing>,
    error: &ConnectorError,
    turn: Turn,
    attempts: NonZeroU32,
) -> Failed {
    let unsent = error.code() == Some(POSITION_UNSENT);
    let (mut dropped, mut ends) = (0, false);
    owed.retain(|_, owing| {
        owing.failures = owing.failures.saturating_add(1);
        let spent = turn == Turn::Last || owing.failures >= attempts.get();
        if owing.done && (spent || unsent) {
            dropped += 1;
            return false;
        }
        ends |= spent && !owing.done;
        true
    });
    Failed {
        owed,
        dropped,
        ends,
    }
}

impl Coordinator {
    /// Whether the commit of `collected`, whose state changes before its receipt are `delta`,
    /// is progress: it publishes a row, or records anything but a partition where it stood.
    ///
    /// A partition whose read was sent nothing is sealed where it started, and recorded there
    /// again. Positions are compared byte for byte: a source that encodes an equal position
    /// anew has moved, as far as the engine can tell.
    pub(super) fn progresses(&self, collected: &Collected, delta: &[StateChange]) -> bool {
        let restated: BTreeSet<String> = collected
            .positions
            .iter()
            .filter(|(partition, state)| {
                self.parts.partitions[**partition].stands.as_ref() == Some(*state)
            })
            .map(|(partition, _)| {
                let partition = &self.parts.partitions[*partition];
                let stream = self.parts.streams[partition.stream].name.clone();
                StateKey::Partition(stream, partition.id.clone()).encode()
            })
            .collect();
        let moves = |change: &StateChange| match change {
            StateChange::Put(record) => !restated.contains(&record.key),
            StateChange::Delete(_) => true,
        };
        !collected.segments.is_empty() || delta.iter().any(moves)
    }

    /// Records a commit that landed: each partition stands at its position in `positions`, and
    /// the attempt progressed where the commit did.
    pub(super) fn landed(&mut self, positions: &BTreeMap<usize, PartitionState>, progressed: bool) {
        for (partition, state) in positions {
            self.parts.partitions[*partition].stands = Some(state.clone());
        }
        self.parts.log.lock().progressed |= progressed;
    }

    /// Tells each stream's source, in one report, the cursors it is owed: those in `reported`,
    /// which a commit that landed with `positions` holds, and those of earlier reports that
    /// failed; at a commit that falls at `turn`, which may have published nothing.
    ///
    /// Every partition a commit covers is told, moved or not, so a report that failed or was
    /// lost is made again by the next commit, else by the next attempt.
    ///
    /// # Errors
    ///
    /// The source's error where a partition at a cursor is still owed at the attempt's last
    /// commit, or after as many failed reports in a row as the run makes attempts; every stream
    /// is told first. A cancelled error where the attempt is cancelled while a report is made.
    pub(super) async fn acknowledge(
        &mut self,
        reported: BTreeMap<usize, Cursor>,
        positions: &BTreeMap<usize, PartitionState>,
        turn: Turn,
    ) -> Result<(), Error> {
        for (place, cursor) in reported {
            let partition = &self.parts.partitions[place];
            let state = positions
                .get(&place)
                .expect("a partition a commit reports is one it records");
            self.owed
                .owe(partition.stream, partition.id.clone(), cursor, state);
        }
        let mut ended = None;
        for (stream, owed) in std::mem::take(&mut self.owed.0) {
            let cursors: Vec<(PartitionId, Cursor)> = owed
                .iter()
                .map(|(partition, owing)| (partition.clone(), owing.cursor.clone()))
                .collect();
            let name = &self.parts.streams[stream].name;
            let told = tokio::select! {
                biased;
                // An answer and the cancel together: the answer wins.
                told = self.parts.source.committed(name, &cursors) => told,
                () = self.parts.cancel.cancelled() => return Err(cancelled()),
            };
            let Err(error) = told else {
                continue;
            };
            let failed = failed(owed, &error, turn, self.parts.retry.attempts());
            self.parts.tally.add(|counters| {
                counters.acks.failed = counters.acks.failed.saturating_add(1);
                counters.acks.dropped = counters.acks.dropped.saturating_add(failed.dropped);
            });
            if !failed.owed.is_empty() {
                self.owed.0.insert(stream, failed.owed);
            }
            if failed.ends && ended.is_none() {
                let error =
                    Error::connector(Side::Source, format!("acknowledging stream {name}"), error)
                        .with_stream(name);
                ended = Some(error);
            }
        }
        ended.map_or(Ok(()), Err)
    }

    /// Where a commit `trigger` made due falls: a commit made once every partition has ended is
    /// the attempt's last, unless a phase that ended may start another.
    pub(super) fn turn(&self, trigger: Trigger) -> Turn {
        let phase_ended = (0..self.parts.streams.len()).any(|stream| self.phase_ended(stream));
        if trigger == Trigger::End && (self.stopping || !phase_ended) {
            Turn::Last
        } else {
            Turn::Within
        }
    }

    /// Makes the attempt's last report of the cursors still owed, once the commit of a phase's
    /// end was followed by no other phase.
    ///
    /// # Errors
    ///
    /// As [`acknowledge`](Self::acknowledge) at the attempt's last commit.
    pub(super) async fn settle(&mut self) -> Result<(), Error> {
        if self.owed.0.is_empty() {
            return Ok(());
        }
        self.acknowledge(BTreeMap::new(), &BTreeMap::new(), Turn::Last)
            .await
    }
}
