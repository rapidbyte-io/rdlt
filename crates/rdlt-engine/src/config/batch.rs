//! How pushes are coalesced into batches, and how JSON is cut up to shred in parallel.

use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use crate::error::{Error, codes};

/// How a partition coalesces pushes into batches and cuts JSON into chunks to shred in parallel.
///
/// Pushes are held until they reach `target_bytes` or `max_rows`, or the first has waited
/// `max_latency`; a checkpoint and the end of the read flush them too. A JSON push's rows are
/// only known once it is shredded, so JSON pushes count toward `target_bytes` alone.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchPolicy {
    target_bytes: NonZeroU64,
    max_rows: NonZeroU64,
    max_latency: Duration,
    chunk_bytes: NonZeroUsize,
}

impl BatchPolicy {
    /// A policy with these thresholds; none may be zero.
    pub fn new(
        target_bytes: u64,
        max_rows: u64,
        max_latency: Duration,
        chunk_bytes: usize,
    ) -> Result<Self, Error> {
        let invalid = |name: &str| {
            Error::config(format!("batch policy: {name} must be more than zero"))
                .with_code(codes::BATCH_POLICY_INVALID)
        };
        if max_latency.is_zero() {
            return Err(invalid("max_latency"));
        }
        Ok(Self {
            target_bytes: NonZeroU64::new(target_bytes).ok_or_else(|| invalid("target_bytes"))?,
            max_rows: NonZeroU64::new(max_rows).ok_or_else(|| invalid("max_rows"))?,
            max_latency,
            chunk_bytes: NonZeroUsize::new(chunk_bytes).ok_or_else(|| invalid("chunk_bytes"))?,
        })
    }

    /// Bytes of pushes a batch is coalesced up to.
    pub fn target_bytes(&self) -> NonZeroU64 {
        self.target_bytes
    }

    /// Rows of Arrow pushes a batch is coalesced up to.
    pub fn max_rows(&self) -> NonZeroU64 {
        self.max_rows
    }

    /// How long the first push of a batch waits for more.
    pub fn max_latency(&self) -> Duration {
        self.max_latency
    }

    /// Bytes: the most of a flush's JSON one shredding job takes, whitespace between records
    /// included, before its chunk ends at the next record end.
    ///
    /// A flush is cut into smaller chunks where a thousand of its records and an even share of two
    /// jobs for each of the engine's cores both take fewer bytes: the larger of the two.
    pub fn chunk_bytes(&self) -> NonZeroUsize {
        self.chunk_bytes
    }
}

impl Default for BatchPolicy {
    /// 8 MiB or 1 Mi rows within one second, shredded in jobs of at most 2 MiB.
    fn default() -> Self {
        Self {
            target_bytes: NonZeroU64::new(8 << 20).unwrap_or(NonZeroU64::MIN),
            max_rows: NonZeroU64::new(1 << 20).unwrap_or(NonZeroU64::MIN),
            max_latency: Duration::from_secs(1),
            chunk_bytes: NonZeroUsize::new(2 << 20).unwrap_or(NonZeroUsize::MIN),
        }
    }
}
