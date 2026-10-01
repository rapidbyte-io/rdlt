//! What a partition tells the commit coordinator as it reads.

use rdlt_connector::{PartitionState, SegmentId};

/// What a partition tells the commit coordinator.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Progress {
    /// The partition started reading.
    Started {
        /// The partition's index in the attempt.
        partition: usize,
    },
    /// Rows were queued for staging.
    Written {
        /// Rows queued.
        rows: u64,
        /// Their bytes in memory.
        bytes: u64,
    },
    /// A segment was sealed.
    Sealed(Seal),
    /// The stream's source said its partitions changed.
    Replan {
        /// The partition's index in the attempt.
        partition: usize,
    },
    /// The partition's read is `records` behind its source's newest.
    Behind {
        /// The partition's index in the attempt.
        partition: usize,
        records: u64,
    },
    /// The partition's source had dropped where its read would resume, and it read again from
    /// its earliest.
    RetentionReset {
        /// The partition's index in the attempt.
        partition: usize,
    },
    /// Rows were staged to a segment no commit will take: no checkpoint sealed them.
    Abandoned {
        /// Rows abandoned.
        rows: u64,
        /// Their bytes in memory.
        bytes: u64,
    },
    /// The partition stopped reading; a partition that was not stopped sealed its end first.
    Ended {
        /// The partition's index in the attempt.
        partition: usize,
        /// Whether the read ended because the engine asked it to stop.
        stopped: bool,
    },
}

/// A sealed segment: every row written to it, and where to resume after it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Seal {
    /// The partition's index in the attempt.
    pub(crate) partition: usize,
    /// The segment.
    pub(crate) segment: SegmentId,
    /// Rows written to the segment.
    pub(crate) rows: u64,
    /// Their bytes in memory.
    pub(crate) bytes: u64,
    /// Where the partition resumes once the segment is committed.
    pub(crate) state: PartitionState,
    /// The barrier this seal answers.
    pub(crate) answers: Option<u64>,
    /// Rows the schema policy dropped from the segment.
    pub(crate) discarded_rows: u64,
    /// Values the schema policy nulled in the segment.
    pub(crate) discarded_values: u64,
    /// Deletes the stream ignores, dropped from the segment.
    pub(crate) deletes_ignored: u64,
    /// Truncates the stream ignores, dropped from the segment.
    pub(crate) truncates_ignored: u64,
}
