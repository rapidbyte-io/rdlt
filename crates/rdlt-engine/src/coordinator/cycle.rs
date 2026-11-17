//! A stream's full read, a cycle, and what an attempt's read of the stream does with one.

use rdlt_connector::{GenerationId, PartitionId};

/// What a stream's read in an attempt has to do with a full read.
#[derive(Debug)]
pub(crate) enum FullRead {
    /// Nothing: the read is not full and ends no full read, or is a full read this run already
    /// completed.
    None,
    /// The read is this full read.
    Reads(Cycle),
    /// The read is not full, and ends with the attempt's next commit the full read state records,
    /// which filled the stream's table.
    Ends,
}

impl FullRead {
    /// The full read the read is, where it is one.
    pub(crate) fn cycle(&self) -> Option<&Cycle> {
        match self {
            Self::Reads(cycle) => Some(cycle),
            Self::None | Self::Ends => None,
        }
    }

    /// The full read the read is, where it is one, to change.
    pub(super) fn cycle_mut(&mut self) -> Option<&mut Cycle> {
        match self {
            Self::Reads(cycle) => Some(cycle),
            Self::None | Self::Ends => None,
        }
    }
}

/// A full read of a stream, from its first partition to its last; a replace stream fills the
/// cycle's generation.
#[derive(Debug)]
pub(crate) struct Cycle {
    pub(crate) generation: GenerationId,
    /// Whether state records the cycle.
    pub(crate) recorded: bool,
    /// Partition entries of the previous cycle, deleted when this cycle is first recorded.
    pub(crate) stale: Vec<PartitionId>,
    /// Whether a commit has already ended the cycle.
    pub(crate) finished: bool,
    /// The stream's recently completed reads, which this one joins when it completes.
    pub(crate) completed: Vec<GenerationId>,
}

/// How many completed reads state keeps per stream: enough that a run's retry still finds its own
/// read among them after other runs of the pipeline completed theirs.
pub(super) const KEPT_COMPLETIONS: usize = 16;
