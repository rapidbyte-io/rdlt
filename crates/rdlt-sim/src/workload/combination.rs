//! The ways a stream is read and written that a long sweep must reach: every combination of how
//! its first phase reads it, whether the next reads it the other way, how its partitions end, what
//! its first reads end on and how it is written, but those no stream may be.
//!
//! Each is drawn on its own (`SimStream::generate`), but what a read that does not end on a
//! checkpoint ends on, which the source notes as each read ends: rows after its last checkpoint,
//! or no checkpoint at all, as its batches fall, the first counted only where the engine did not
//! refuse the read (`world::ends`). The draws are tied only where the engine refuses a
//! combination or the model cannot hold it:
//! - a stream that replaces its table is read in full in every phase: replace needs a full read,
//!   and the engine refuses the plan as `plan_mode_invalid`;
//! - a stream whose rows arrive as runs follow it is read incrementally in every phase, and where
//!   its partitions end, its reads end on a checkpoint: a full read, or a read finishing a
//!   partition on rows, would end at the rows that had arrived, where the model holds each to every
//!   row of its phase;
//! - an incremental read of a partition that never ends, of a stream that checkpoints on demand,
//!   never ends on rows after a checkpoint, but checkpoints nowhere and is refused. Where its last
//!   checkpoint falls is the engine's to ask, so which rows the next read reads again is not the
//!   model's to know: it would need the source to tell the model where each read checkpointed,
//!   and the model to follow every run's reads. The same read of a stream that checkpoints
//!   naturally ends on rows after a checkpoint, which the model places;
//! - each partition of a stream whose first incremental reads leave the rows after their last
//!   checkpoint to the next read holds two or more whole checkpoint groups, so the rows left are
//!   one group, read again in the batches of the phase that first held them (a batch is typed as
//!   its first row's phase); and a bounded partition a first read ending on rows finished does not
//!   grow, as it is done.

use std::fmt;

use rdlt_connector::ReadMode;
use rdlt_engine::WriteMode;

use super::{PHASES, SimStream};
use crate::reached::Reached;

/// How many seeds a sweep runs at least for [`unreached`] to hold it to every combination: a
/// pull request's shard, and so each of the nightly's and `just ci`'s sweep too.
///
/// A shorter sweep, as the simulation recipe's and the coverage run's of a thousand seeds, is held
/// to none: the rarest combination is reached about six times in a thousand seeds, so a change to
/// the draws would miss one about once in 135, where 2500 seeds miss one about once in six hundred
/// thousand.
pub const SWEPT: usize = 2500;

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
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum End {
    /// A checkpoint after its last row.
    Checkpoint,
    /// Rows after its last checkpoint.
    Tail,
    /// Rows, with no checkpoint before any.
    Unmarked,
}

impl fmt::Display for End {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Checkpoint => "ending on a checkpoint",
            Self::Tail => "ending on rows after a checkpoint",
            Self::Unmarked => "never checkpointing",
        })
    }
}

impl Combination {
    /// Every combination a stream may be read and written in, in a fixed order.
    pub fn listed() -> Vec<Self> {
        let mut listed = Vec::new();
        for read in [ReadMode::Full, ReadMode::Incremental] {
            for changed in [false, true] {
                for ends in [Ends::Bounded, Ends::Unbounded, Ends::Followed] {
                    for end in [End::Checkpoint, End::Tail, End::Unmarked] {
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

    /// Whether a stream may be so, as the ties the module lists allow.
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
            "{}{next}, {:?}, {}, {:?}",
            read(self.read),
            self.ends,
            self.end,
            self.write
        )
    }
}

impl SimStream {
    /// The combination the stream is read and written in, where a read of its first phase ended
    /// on `end`.
    pub fn combination(&self, end: End) -> Combination {
        let ends = match (self.unbounded, self.follows) {
            (false, _) => Ends::Bounded,
            (true, false) => Ends::Unbounded,
            (true, true) => Ends::Followed,
        };
        Combination {
            read: self.reads[0],
            changed: self.reads[1..PHASES]
                .iter()
                .any(|next| *next != self.reads[0]),
            ends,
            end,
            write: self.write,
        }
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
