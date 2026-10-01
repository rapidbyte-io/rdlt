//! What a change stream's configuration and the partitions it is asked for are checked against.

use rdlt_connector::limits::MAX_BATCH_ROWS;
use rdlt_connector::{ConnectorError, PartitionId};

use super::{CHANGES_PARTITION, ChangedStream};
use crate::limits::{MAX_CHANGES, MAX_PARTITIONS, MAX_SNAPSHOT_KEYS, MAX_TRUNCATES, within};

impl ChangedStream {
    /// Checks the stream against what a source holds: the partitions a plan lists, the rows of a
    /// batch, the table and the changes a snapshot read builds, and the positions a row carries.
    pub(super) fn bounded(&self) -> Result<(), ConnectorError> {
        if self.snapshot_partitions == 0 || self.batch_rows == 0 {
            return Err(ConnectorError::config(format!(
                "stream {}: snapshot_partitions and batch_rows must be at least 1",
                self.name
            )));
        }
        let truncates = u64::try_from(self.truncates.len()).unwrap_or(u64::MAX);
        let checked = [
            (
                "snapshot_partitions",
                self.snapshot_partitions,
                MAX_PARTITIONS,
            ),
            ("batch_rows", self.batch_rows, MAX_BATCH_ROWS),
            ("keys", self.keys, MAX_SNAPSHOT_KEYS),
            ("captured", self.captured, MAX_SNAPSHOT_KEYS),
            ("changes", self.changes, MAX_CHANGES),
            ("truncates", truncates, MAX_TRUNCATES),
        ];
        checked
            .into_iter()
            .try_for_each(|(name, actual, limit)| within(&self.name, name, actual, limit))
    }

    /// The index of the snapshot partition `id` names, written the way the stream plans it; none
    /// where the stream has no such partition.
    pub(super) fn snapshot_index(&self, id: &str) -> Option<u64> {
        id.strip_prefix("snapshot-")
            .and_then(|index| index.parse::<u64>().ok())
            .filter(|index| *index < self.snapshot_partitions)
            .filter(|index| id == format!("snapshot-{index}"))
    }

    /// Checks that `partition` is one the stream has: the changes, or a partition of its snapshot.
    pub(super) fn member(&self, partition: &PartitionId) -> Result<(), ConnectorError> {
        let id = partition.as_str();
        if id == CHANGES_PARTITION || self.snapshot_index(id).is_some() {
            return Ok(());
        }
        Err(self.no_partition(id))
    }

    /// The error of a partition the stream does not have.
    pub(super) fn no_partition(&self, id: &str) -> ConnectorError {
        ConnectorError::data(format!("stream {} has no partition {id}", self.name))
    }
}
