//! What a pipeline's tables and state may grow to, across pushes and runs.

use std::num::{NonZeroU64, NonZeroUsize};

use crate::error::Error;

/// Limits on what a pipeline's tables and state grow to across pushes and runs, each refused,
/// typed, where it would be passed.
///
/// A table's columns, nested fields counted, are held to the schema columns of
/// [`EngineConfig::limits`](super::EngineConfig::limits): a table is never wider than a schema a
/// connector may send.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GrowthLimits {
    child_tables: NonZeroUsize,
    writers: NonZeroUsize,
    state_bytes: NonZeroU64,
}

impl GrowthLimits {
    /// Limits of `child_tables` child tables a normalized stream adds below its table, recorded
    /// and new together, of `writers` destination writers an attempt holds open at once, and of
    /// `state_bytes` bytes a message carrying the pipeline's state may take; none may be zero.
    pub fn new(child_tables: usize, writers: usize, state_bytes: u64) -> Result<Self, Error> {
        let invalid = |name: &str| {
            Error::config(format!("growth limits: {name} must be more than zero"))
                .with_code("growth_limits_invalid")
        };
        Ok(Self {
            child_tables: NonZeroUsize::new(child_tables).ok_or_else(|| invalid("child_tables"))?,
            writers: NonZeroUsize::new(writers).ok_or_else(|| invalid("writers"))?,
            state_bytes: NonZeroU64::new(state_bytes).ok_or_else(|| invalid("state_bytes"))?,
        })
    }

    /// Tables: the most child tables a normalized stream adds below its table, those state
    /// records and those an attempt adds together.
    pub fn child_tables(&self) -> NonZeroUsize {
        self.child_tables
    }

    /// Writers: the most destination writers an attempt holds open at once, across its lanes,
    /// each lane an equal share of them and one at least.
    ///
    /// A served destination's connection carries each open writer as a call, among at most 200,
    /// so the default leaves room for every other call of the session.
    pub fn writers(&self) -> NonZeroUsize {
        self.writers
    }

    /// Bytes: the most one message carrying the pipeline's state may take, as the protocol
    /// bounds it: an open's answer, a commit's request, a plan's request and a report of
    /// committed positions.
    ///
    /// A commit that would leave state, or make a request, beyond it is refused before it is
    /// logged or the source hears of it, so no stored state is one an open cannot carry.
    pub fn state_bytes(&self) -> NonZeroU64 {
        self.state_bytes
    }
}

impl Default for GrowthLimits {
    /// 1024 child tables a stream, 128 writers an attempt, and 16 MiB of state a message.
    fn default() -> Self {
        Self {
            child_tables: NonZeroUsize::new(1024).unwrap_or(NonZeroUsize::MIN),
            writers: NonZeroUsize::new(128).unwrap_or(NonZeroUsize::MIN),
            state_bytes: NonZeroU64::new(16 << 20).unwrap_or(NonZeroU64::MIN),
        }
    }
}
