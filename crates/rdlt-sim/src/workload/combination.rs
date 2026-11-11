//! The ways a stream is read and written that a long sweep must reach: every combination of how
//! its first phase reads it, whether the next reads it the other way, how its partitions end, what
//! its first reads end on and how it is written, but those no stream may be.
//!
//! Each is drawn on its own (`SimStream::generate`). Those never drawn, and why:
//! - a stream that replaces its table read incrementally in either phase: replace needs a full
//!   read, and the engine refuses the plan as `plan_mode_invalid`;
//! - a stream whose rows arrive as runs follow it read in full in either phase: a full read ends
//!   at the rows that have arrived, where the model holds each cycle to every row of its phase.

use std::fmt;

use rdlt_connector::ReadMode;
use rdlt_engine::WriteMode;

use super::{PHASES, SimStream};
use crate::reached::Reached;

/// How many seeds a sweep runs at least for [`unreached`] to hold it to every combination:
/// those of the simulation's recipe and of every shard of the pull request's and the nightly's.
pub const SWEPT: usize = 1000;

/// How a stream is read and written, as a sweep reaches it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Combination {
    /// How its first phase reads it.
    pub read: ReadMode,
    /// Whether the next phase reads it the other way.
    pub changed: bool,
    /// How its partitions end.
    pub ends: Ends,
    /// What its first phase's reads end on.
    pub end: End,
    /// How it is written.
    pub write: WriteMode,
}

/// How a stream's partitions end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Ends {
    /// Each has an end, which a read reaches.
    Bounded,
    /// None ends, and each run reads on from its cursor, as a table polled for new rows is.
    Unbounded,
    /// None ends, and its rows arrive as runs follow it, as a log's do.
    Followed,
}

/// What a read ends on.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum End {
    /// A checkpoint after its last row.
    Checkpoint,
    /// Rows after its last checkpoint, or no checkpoint at all.
    Rows,
}

impl Combination {
    /// Every combination a stream may be read and written in, in a fixed order.
    pub fn listed() -> Vec<Self> {
        let mut listed = Vec::new();
        for read in [ReadMode::Full, ReadMode::Incremental] {
            for changed in [false, true] {
                for ends in [Ends::Bounded, Ends::Unbounded, Ends::Followed] {
                    for end in [End::Checkpoint, End::Rows] {
                        for write in [WriteMode::Append, WriteMode::Replace, WriteMode::Merge] {
                            let combination = Self {
                                read,
                                changed,
                                ends,
                                end,
                                write,
                            };
                            if combination.drawn() {
                                listed.push(combination);
                            }
                        }
                    }
                }
            }
        }
        listed
    }

    /// Whether a stream may be so, as the module's list of those never drawn says.
    fn drawn(self) -> bool {
        let full = self.read == ReadMode::Full && !self.changed;
        let incremental = self.read == ReadMode::Incremental && !self.changed;
        (self.write != WriteMode::Replace || full) && (self.ends != Ends::Followed || incremental)
    }

    /// This combination's place among those [`listed`](Self::listed).
    fn index(self) -> Option<usize> {
        Self::listed().iter().position(|listed| *listed == self)
    }

    /// The bit of a set of combinations that holds this one.
    pub(crate) fn bit(self) -> u64 {
        self.index().map_or(0, |index| 1 << index)
    }
}

impl fmt::Display for Combination {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let read = |read: ReadMode| match read {
            ReadMode::Full => "full",
            ReadMode::Incremental => "incremental",
            ReadMode::Cdc => "cdc",
        };
        let next = match (self.changed, self.read) {
            (false, _) => String::new(),
            (true, ReadMode::Full) => " then incremental".to_owned(),
            (true, _) => " then full".to_owned(),
        };
        write!(
            formatter,
            "{}{next}, {:?}, ending on {:?}, {:?}",
            read(self.read),
            self.ends,
            self.end,
            self.write
        )
    }
}

impl SimStream {
    /// The combination the stream is read and written in, where its first phase reads rows.
    pub fn combination(&self) -> Option<Combination> {
        let read = (0..self.partitions.len()).any(|partition| !self.read(partition, 0).is_empty());
        let ends = match (self.unbounded, self.follows) {
            (false, _) => Ends::Bounded,
            (true, false) => Ends::Unbounded,
            (true, true) => Ends::Followed,
        };
        read.then(|| Combination {
            read: self.reads[0],
            changed: self.reads[1..PHASES]
                .iter()
                .any(|next| *next != self.reads[0]),
            ends,
            end: if self.final_checkpoint {
                End::Checkpoint
            } else {
                End::Rows
            },
            write: self.write,
        })
    }
}

/// The combinations none of `reached` reached, in the order [`Combination::listed`] lists them.
pub fn unreached(reached: impl IntoIterator<Item = Reached>) -> Vec<Combination> {
    let every: Vec<Reached> = reached.into_iter().collect();
    Combination::listed()
        .into_iter()
        .filter(|combination| {
            !every
                .iter()
                .any(|reached| reached.has_combination(*combination))
        })
        .collect()
}

#[cfg(test)]
mod tests;
