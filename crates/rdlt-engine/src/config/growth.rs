//! What a pipeline's tables and state may grow to, across pushes and runs.

use std::num::NonZeroUsize;

use crate::error::{Error, codes};

/// Limits on what a pipeline's tables and state grow to across pushes and runs, each refused,
/// typed, where it would be passed.
///
/// A table's columns, nested fields counted, are held to the schema columns of
/// [`EngineConfig::limits`](super::EngineConfig::limits): a table is never wider than a schema a
/// connector may send.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GrowthLimits {
    child_tables: NonZeroUsize,
    table_writers: NonZeroUsize,
}

impl GrowthLimits {
    /// Limits of `child_tables` child tables a normalized stream adds below its table, recorded
    /// and new together, and of `table_writers` table writers an attempt holds open at once;
    /// neither may be zero.
    ///
    /// The pipeline's stored state is held to [`Limits::state_bytes`](rdlt_wire::Limits) of
    /// [`EngineConfig::limits`](super::EngineConfig::limits), what an open's answer may hold.
    pub fn new(child_tables: usize, table_writers: usize) -> Result<Self, Error> {
        let invalid = |name: &str| {
            Error::config(format!("growth limits: {name} must be more than zero"))
                .with_code(codes::GROWTH_LIMITS_INVALID)
        };
        Ok(Self {
            child_tables: NonZeroUsize::new(child_tables).ok_or_else(|| invalid("child_tables"))?,
            table_writers: NonZeroUsize::new(table_writers)
                .ok_or_else(|| invalid("table_writers"))?,
        })
    }

    /// Tables: the most child tables a normalized stream adds below its table, those state
    /// records and those an attempt adds together.
    pub fn child_tables(&self) -> NonZeroUsize {
        self.child_tables
    }

    /// Table writers: the most an attempt holds open at once, across its writers, each writer an
    /// equal share of them and one at least.
    ///
    /// A destination whose calls are bounded
    /// ([`Destination::max_calls`](rdlt_connector::Destination::max_calls)) holds no more open
    /// than its calls less one, where those are fewer: a served destination, 198.
    pub fn table_writers(&self) -> NonZeroUsize {
        self.table_writers
    }
}

impl Default for GrowthLimits {
    /// 1024 child tables a stream and 128 table writers an attempt.
    fn default() -> Self {
        Self {
            child_tables: NonZeroUsize::new(1024).unwrap_or(NonZeroUsize::MIN),
            table_writers: NonZeroUsize::new(128).unwrap_or(NonZeroUsize::MIN),
        }
    }
}
