//! What a pipeline's tables and state may grow to, across pushes and runs.

use std::num::{NonZeroU64, NonZeroUsize};

use crate::error::Error;
use crate::limits::LOG_BYTES;

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
    log_bytes: NonZeroU64,
}

impl GrowthLimits {
    /// Limits of `child_tables` child tables a normalized stream adds below its table, recorded
    /// and new together, and of `writers` destination writers an attempt holds open at once;
    /// neither may be zero.
    ///
    /// The pipeline's stored state is held to [`Limits::state_bytes`](rdlt_wire::Limits) of
    /// [`EngineConfig::limits`](super::EngineConfig::limits), what an open's answer may hold.
    pub fn new(child_tables: usize, writers: usize) -> Result<Self, Error> {
        let invalid = |name: &str| {
            Error::config(format!("growth limits: {name} must be more than zero"))
                .with_code("growth_limits_invalid")
        };
        Ok(Self {
            child_tables: NonZeroUsize::new(child_tables).ok_or_else(|| invalid("child_tables"))?,
            writers: NonZeroUsize::new(writers).ok_or_else(|| invalid("writers"))?,
            log_bytes: Self::default().log_bytes,
        })
    }

    /// These limits, a load's write-ahead log holding at most `bytes` on disk; never zero.
    ///
    /// # Errors
    ///
    /// `growth_limits_invalid` for zero bytes.
    pub fn with_log_bytes(mut self, bytes: u64) -> Result<Self, Error> {
        self.log_bytes = NonZeroU64::new(bytes).ok_or_else(|| {
            Error::config("growth limits: log_bytes must be more than zero")
                .with_code("growth_limits_invalid")
        })?;
        Ok(self)
    }

    /// Bytes: the most a load's write-ahead log holds on disk, its chunks published and the chunk
    /// it stages together.
    ///
    /// Only a commit lets a chunk go, so a source that does not checkpoint grows its load's log
    /// by what it sends. Once the log holds half of this, a commit is due, and again at each
    /// eighth more; a batch whose frame would take the log past it fails its write with
    /// `log_bytes_exceeded`, before the source is told anything of it.
    pub fn log_bytes(&self) -> NonZeroU64 {
        self.log_bytes
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
    /// 1024 child tables a stream, 128 writers an attempt, and 4 GiB of a load's log.
    fn default() -> Self {
        Self {
            child_tables: NonZeroUsize::new(1024).unwrap_or(NonZeroUsize::MIN),
            writers: NonZeroUsize::new(128).unwrap_or(NonZeroUsize::MIN),
            log_bytes: NonZeroU64::new(LOG_BYTES).unwrap_or(NonZeroU64::MIN),
        }
    }
}
