//! The partitions a source plans for a stream, checked as the engine takes them.

use std::collections::{BTreeMap, BTreeSet};

use crate::cursor::Cursor;
use crate::id::PartitionId;
use crate::limits::MAX_PLAN_PARTITIONS;

/// A slice of a stream that is read, and checkpointed, on its own.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Partition {
    id: PartitionId,
    unbounded: bool,
}

impl Partition {
    /// A partition with `id`; the source gives the id its meaning.
    pub fn new(id: PartitionId) -> Self {
        Self {
            id,
            unbounded: false,
        }
    }

    /// The partition, as one that never ends, as a change stream's changes or a log's records
    /// do: a read of it that ends only pauses it.
    ///
    /// Such a partition is never done: its next read resumes from its last checkpoint, so rows a
    /// read pushed after that checkpoint are read again, not committed without a position.
    #[must_use]
    pub fn unbounded(mut self) -> Self {
        self.unbounded = true;
        self
    }

    /// Whether the partition never ends.
    pub fn is_unbounded(&self) -> bool {
        self.unbounded
    }

    /// The partition of a stream that is not split, with id `whole`.
    pub fn single() -> Self {
        Self::new(PartitionId::whole())
    }

    /// The partition's id.
    pub fn id(&self) -> &PartitionId {
        &self.id
    }
}

/// The partitions a source plans for a stream, and the phase they belong to.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PartitionPlan {
    /// The stream's phase the partitions belong to; `None` keeps the phase state records.
    ///
    /// A phase other than the recorded one begins the phase: the commit that first records it
    /// forgets the previous phase's partitions.
    pub phase: Option<u16>,
    /// The partitions, with distinct ids.
    pub partitions: Vec<Partition>,
    /// Where partitions of a new phase start, as a CDC stream's changes start from the position
    /// its snapshot captured.
    ///
    /// A partition without a start starts from the beginning. A plan in the recorded phase
    /// resumes each partition from its committed position instead.
    pub starts: BTreeMap<PartitionId, Cursor>,
}

impl PartitionPlan {
    /// `partitions`, in the phase state records.
    pub fn new(partitions: Vec<Partition>) -> Self {
        Self {
            phase: None,
            partitions,
            starts: BTreeMap::new(),
        }
    }

    /// The plan with its partitions in `phase`.
    #[must_use]
    pub fn phase(mut self, phase: u16) -> Self {
        self.phase = Some(phase);
        self
    }

    /// The plan with `partition`, of a new phase, starting at `cursor`.
    #[must_use]
    pub fn start(mut self, partition: PartitionId, cursor: Cursor) -> Self {
        self.starts.insert(partition, cursor);
        self
    }
}

impl From<Vec<Partition>> for PartitionPlan {
    fn from(partitions: Vec<Partition>) -> Self {
        Self::new(partitions)
    }
}

/// Why a plan is refused.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum InvalidPlan {
    /// The plan names more partitions than a plan may.
    #[error("the plan names {count} partitions, beyond the limit of {limit}")]
    TooMany {
        /// The partitions named.
        count: usize,
        /// The most a plan names.
        limit: usize,
    },
    /// The plan names a partition more than once.
    #[error("the plan names partition {0} more than once")]
    Repeated(PartitionId),
    /// The plan says where a partition it does not name starts.
    #[error("the plan starts partition {0}, which it does not name")]
    Unplanned(PartitionId),
}

impl PartitionPlan {
    /// Checks the plan: at most [`MAX_PLAN_PARTITIONS`] partitions, with distinct ids, and a
    /// start only for a partition it names.
    ///
    /// # Errors
    ///
    /// The first [`InvalidPlan`] the plan is.
    pub fn validate(&self) -> Result<(), InvalidPlan> {
        if self.partitions.len() > MAX_PLAN_PARTITIONS {
            return Err(InvalidPlan::TooMany {
                count: self.partitions.len(),
                limit: MAX_PLAN_PARTITIONS,
            });
        }
        let mut named = BTreeSet::new();
        for partition in &self.partitions {
            if !named.insert(partition.id()) {
                return Err(InvalidPlan::Repeated(partition.id().clone()));
            }
        }
        match self.starts.keys().find(|id| !named.contains(id)) {
            Some(stray) => Err(InvalidPlan::Unplanned(stray.clone())),
            None => Ok(()),
        }
    }
}
