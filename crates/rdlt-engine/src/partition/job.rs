//! One partition to read, as an attempt asks for it.

use rdlt_connector::{Cursor, Partition, StreamName};
use tokio_util::sync::CancellationToken;

/// One partition to read.
#[derive(Clone, Debug)]
pub(crate) struct PartitionJob {
    /// The partition's index in the attempt.
    pub(crate) index: usize,
    /// The stream.
    pub(crate) stream: StreamName,
    /// The stream's table index.
    pub(crate) table: usize,
    /// The partition.
    pub(crate) partition: Partition,
    /// Where to resume.
    pub(crate) cursor: Option<Cursor>,
    /// Whether the partition checkpoints when asked, so barriers are forwarded to it.
    pub(crate) on_demand: bool,
    /// How a change stream's pushes load; `None` for a stream not read as changes.
    pub(crate) changes: Option<ChangeMode>,
    /// Fires when this partition must stop reading: as every partition does when the attempt
    /// stops, or alone when a plan no longer names it.
    pub(crate) stop: CancellationToken,
    /// Whether a read of the unbounded partition follows it once caught up: in a following run,
    /// for a stream the run plans again as it reads; a full read ends at its head.
    pub(crate) follow: bool,
    /// Whether the partition reads again from its source's earliest where the source's retention
    /// dropped where it would resume, rather than failing.
    pub(crate) reset_retention: bool,
}

impl PartitionJob {
    /// Whether the read follows its unbounded partition for as long as the run.
    pub(crate) fn followed(&self) -> bool {
        self.follow && self.partition.is_unbounded()
    }
}

/// How a change stream's pushes load.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ChangeMode {
    /// Whether its table merges by key; otherwise it is a log of every change.
    pub(crate) merge: bool,
    pub(crate) deletes: crate::plan::DeleteMode,
    pub(crate) truncates: crate::plan::OnTruncate,
    /// Whether the destination keeps a column's value an update flags unchanged.
    pub(crate) partial_updates: bool,
}
