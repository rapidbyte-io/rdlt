//! What a pipeline's tables and state may grow to, across pushes and runs.

use std::num::NonZeroUsize;

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
}

impl GrowthLimits {
    /// Limits of `child_tables` child tables a normalized stream adds below its table, recorded
    /// and new together, and of `writers` destination writers an attempt holds open at once;
    /// none may be zero.
    pub fn new(child_tables: usize, writers: usize) -> Result<Self, Error> {
        let invalid = |name: &str| {
            Error::config(format!("growth limits: {name} must be more than zero"))
                .with_code("growth_limits_invalid")
        };
        Ok(Self {
            child_tables: NonZeroUsize::new(child_tables).ok_or_else(|| invalid("child_tables"))?,
            writers: NonZeroUsize::new(writers).ok_or_else(|| invalid("writers"))?,
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
}

impl Default for GrowthLimits {
    /// 1,024 child tables a stream, 128 writers an attempt.
    fn default() -> Self {
        Self {
            child_tables: NonZeroUsize::new(1024).unwrap_or(NonZeroUsize::MIN),
            writers: NonZeroUsize::new(128).unwrap_or(NonZeroUsize::MIN),
        }
    }
}
