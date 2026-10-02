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
}

impl GrowthLimits {
    /// Limits of `child_tables` child tables a normalized stream adds below its table, recorded
    /// and new together; none may be zero.
    pub fn new(child_tables: usize) -> Result<Self, Error> {
        let invalid = |name: &str| {
            Error::config(format!("growth limits: {name} must be more than zero"))
                .with_code("growth_limits_invalid")
        };
        Ok(Self {
            child_tables: NonZeroUsize::new(child_tables).ok_or_else(|| invalid("child_tables"))?,
        })
    }

    /// Tables: the most child tables a normalized stream adds below its table, those state
    /// records and those an attempt adds together.
    pub fn child_tables(&self) -> NonZeroUsize {
        self.child_tables
    }
}

impl Default for GrowthLimits {
    /// 1,024 child tables a stream.
    fn default() -> Self {
        Self {
            child_tables: NonZeroUsize::new(1024).unwrap_or(NonZeroUsize::MIN),
        }
    }
}
