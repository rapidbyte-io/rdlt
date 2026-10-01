//! Acknowledging committed positions to the source.

use std::collections::BTreeMap;

use rdlt_connector::{Cursor, PartitionId, PartitionState};

use super::Coordinator;
use super::delta::Collected;
use crate::error::{Error, Side};

/// What a commit moves on.
pub(super) struct Moved {
    /// The positions that move their partition on from where the destination holds it.
    pub(super) advanced: BTreeMap<usize, PartitionState>,
    /// Whether the commit is progress: it publishes a row, moves a partition, or is not `quiet`.
    pub(super) progressed: bool,
}

impl Coordinator {
    /// What the commit of `collected` moves on; it is `quiet` where it begins and completes no
    /// phase and records no table's change.
    ///
    /// A partition whose read was sent nothing is sealed where it started. The source has
    /// nothing to hear of it: it was told that position when it was first committed, or will be
    /// told the next the partition reaches.
    pub(super) fn moved(&self, collected: &Collected, quiet: bool) -> Moved {
        let advanced: BTreeMap<usize, PartitionState> = collected
            .positions
            .iter()
            .filter(|(partition, state)| {
                self.parts.partitions[**partition].stands.as_ref() != Some(*state)
            })
            .map(|(partition, state)| (*partition, state.clone()))
            .collect();
        let progressed = !(quiet && collected.segments.is_empty() && advanced.is_empty());
        Moved {
            advanced,
            progressed,
        }
    }

    /// Records a commit that landed: the destination holds each partition at its position in
    /// `positions`, and the attempt progressed where the commit did.
    pub(super) fn landed(&mut self, positions: &BTreeMap<usize, PartitionState>, progressed: bool) {
        for (partition, state) in positions {
            self.parts.partitions[*partition].stands = Some(state.clone());
        }
        self.parts.log.lock().progressed |= progressed;
    }

    /// Tells the source which of `positions` are committed, per stream, for the streams that can
    /// read again what they acknowledged where `replayable`, the others otherwise.
    pub(super) async fn acknowledge(
        &mut self,
        positions: &BTreeMap<usize, PartitionState>,
        replayable: bool,
    ) -> Result<(), Error> {
        let mut cursors: BTreeMap<usize, Vec<(PartitionId, Cursor)>> = BTreeMap::new();
        for (partition, state) in positions {
            let partition = &self.parts.partitions[*partition];
            if self.parts.streams[partition.stream].replayable != replayable {
                continue;
            }
            if let PartitionState::Cursor(cursor) = state {
                let cursor = cursor.clone();
                cursors
                    .entry(partition.stream)
                    .or_default()
                    .push((partition.id.clone(), cursor));
            }
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
