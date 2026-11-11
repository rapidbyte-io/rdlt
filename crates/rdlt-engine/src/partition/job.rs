//! One partition to read, as an attempt asks for it.

use std::sync::Arc;

use rdlt_connector::{Cursor, Exclusion, Partition, ReadMode, StreamName};
use tokio_util::sync::CancellationToken;

use crate::transform::stage::Stage;

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
    /// How the stream is read, which decides what lands of the rows after the read's last
    /// checkpoint.
    pub(crate) read: ReadMode,
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
    /// The columns the stream excludes: its read may leave them out, and they are dropped from
    /// whatever it sends.
    pub(crate) exclude: Exclusion,
    /// What the stream's include and transforms make of each batch, if it lists either.
    pub(crate) stage: Option<Arc<Stage>>,
}

impl PartitionJob {
    /// Whether the read follows its unbounded partition for as long as the run.
    pub(crate) fn followed(&self) -> bool {
        self.follow && self.partition.is_unbounded()
    }

    /// Whether the rows the read pushes after its last checkpoint are left to the partition's
    /// next read rather than sealed with its end: an unbounded partition's, in a read that is
    /// not full.
    pub(crate) fn reads_again(&self) -> bool {
        self.partition.is_unbounded() && self.read != ReadMode::Full
    }

    /// Whether a read that pushes rows and no checkpoint after any fails, since none of them
    /// could land: an unbounded partition's, read incrementally.
    ///
    /// A change read is not held to it, as a change source may read ahead of its round.
    pub(crate) fn must_checkpoint(&self) -> bool {
        self.partition.is_unbounded() && self.read == ReadMode::Incremental
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
