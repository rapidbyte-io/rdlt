//! Acknowledging committed positions to the source, and telling a commit that moves the load on
//! from one that only records again what state already holds.

use std::collections::BTreeMap;

use rdlt_connector::{CommitMeta, Cursor, PartitionId, PartitionState, StateChange};

use super::Coordinator;
use super::delta::Collected;
use crate::crash::crash_point;
use crate::error::{Error, Side};
use crate::wal::Sealed;
use crate::wal::frame::BegunPhase;

impl Coordinator {
    /// Where the load keeps a log, logs the commit `meta` of `sealed`, beginning the phases
    /// `begun`: whether it did.
    ///
    /// What the commit records of tables, `prepaid` bytes, was reserved as each table changed.
    pub(super) async fn log_commit(
        &self,
        meta: &CommitMeta,
        prepaid: u64,
        sealed: Vec<Sealed>,
        begun: Vec<BegunPhase>,
    ) -> Result<bool, Error> {
        let Some(log) = &self.parts.wal else {
            return Ok(false);
        };
        // Every batch of the commit's segments was queued for the log before its partition
        // sealed it: the commit's frame, queued now, follows them all.
        log.commit(&self.parts.budget, sealed, begun, meta, prepaid)
            .await?;
        crash_point!("engine.ack.early");
        Ok(true)
    }

    /// Whether the commit of `collected`, whose state changes before its receipt are `delta`,
    /// is progress: it publishes a row, or records anything but a partition where it stood.
    ///
    /// A partition whose read was sent nothing is sealed where it started, and recorded there
    /// again. Positions are compared byte for byte: a source that encodes an equal position
    /// anew has moved, as far as the engine can tell.
    pub(super) fn progresses(&self, collected: &Collected, delta: &[StateChange]) -> bool {
        let restated: Vec<StateChange> = collected
            .positions
            .iter()
            .filter(|(partition, state)| {
                self.parts.partitions[**partition].stands.as_ref() == Some(*state)
            })
            .map(|(partition, state)| self.position(*partition, state))
            .collect();
        !collected.segments.is_empty() || delta.iter().any(|change| !restated.contains(change))
    }

    /// Records a commit that landed: each partition stands at its position in `positions`, and
    /// the attempt progressed where the commit did.
    pub(super) fn landed(&mut self, positions: &BTreeMap<usize, PartitionState>, progressed: bool) {
        for (partition, state) in positions {
            self.parts.partitions[*partition].stands = Some(state.clone());
        }
        self.parts.log.lock().progressed |= progressed;
    }

    /// Tells the source the cursors in `reported` are committed, per stream, for the streams
    /// that can read again what they acknowledged where `replayable`, the others otherwise.
    ///
    /// Every partition a commit covers is told, moved or not: a report that failed or was lost
    /// is so made again by the next attempt, though its partition is sent nothing more. A source
    /// that cannot read again always has a log, which is where it is told before the commit.
    pub(super) async fn acknowledge(
        &mut self,
        reported: &BTreeMap<usize, Cursor>,
        replayable: bool,
    ) -> Result<(), Error> {
        let mut cursors: BTreeMap<usize, Vec<(PartitionId, Cursor)>> = BTreeMap::new();
        for (partition, cursor) in reported {
            let partition = &self.parts.partitions[*partition];
            if self.parts.streams[partition.stream].replayable != replayable {
                continue;
            }
            cursors
                .entry(partition.stream)
                .or_default()
                .push((partition.id.clone(), cursor.clone()));
        }
        for (stream, cursors) in cursors {
            let name = &self.parts.streams[stream].name;
            self.parts
                .source
                .committed(name, &cursors)
                .await
                .map_err(|error| {
                    Error::connector(Side::Source, format!("acknowledging stream {name}"), error)
                        .with_stream(name)
                })?;
        }
        Ok(())
    }
}
