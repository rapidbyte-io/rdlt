//! Acknowledging committed positions to the source.

use std::collections::BTreeMap;

use rdlt_connector::{Cursor, PartitionId, PartitionState};

use super::Coordinator;
use crate::error::{Error, Side};

impl Coordinator {
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
